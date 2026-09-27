package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	// cleanupLockKey is the Redis key every replica contends for. Exactly one
	// instance may run a sweep at a time.
	cleanupLockKey = "lock:session-cleanup"

	// cleanupLockTTL bounds how long a crashed holder can block the sweep. It
	// is far longer than a pass over the keyspace needs; the holder releases
	// the lock itself as soon as the pass finishes.
	cleanupLockTTL = 5 * time.Minute

	// DefaultCleanupInterval is the sweep period used when no interval is
	// configured.
	DefaultCleanupInterval = 5 * time.Minute

	// DefaultCleanupJitter is the upper bound of the random delay applied
	// before each sweep attempt.
	DefaultCleanupJitter = 30 * time.Second

	// cleanupScanBatch is the COUNT hint passed to SCAN. Redis treats it as a
	// hint only; the cursor loop is what guarantees full coverage.
	cleanupScanBatch = 200

	// userSessionsPattern matches the per-user session index sets. Individual
	// session keys are never scanned: they expire on their own via TTL, and the
	// sweep only needs to notice that they are gone.
	userSessionsPattern = "user:sessions:*"
)

// SessionStore reclaims expired rows from the legacy sessions table (migration
// 012). That table carries an expires_at index that only exists to serve a
// reaper, so the scheduled job owns it. It may be nil in deployments with no
// session rows, in which case only Redis state is swept.
type SessionStore interface {
	DeleteExpiredSessions(ctx context.Context) (int64, error)
}

// CleanupStats reports what a single sweep reclaimed.
type CleanupStats struct {
	// SetsScanned is the number of user:sessions:<id> index sets inspected.
	SetsScanned int `json:"setsScanned"`
	// DanglingMembers is the number of set members removed because their
	// session key had already expired.
	DanglingMembers int64 `json:"danglingMembers"`
	// EmptySetsRemoved is the number of index sets deleted once every member
	// turned out to be dangling.
	EmptySetsRemoved int64 `json:"emptySetsRemoved"`
	// RowsDeleted is the number of expired rows removed from the sessions table.
	RowsDeleted int64 `json:"rowsDeleted"`
}

// Cleaner is the single scheduled owner of expired-session reclamation.
//
// Session keys in Redis expire by themselves, but the per-user index sets they
// are tracked in do not: a Redis set has no per-member TTL, so a refresh-token
// hash stays in user:sessions:<id> forever after its session key TTLs out. Every
// session-listing request walks those sets, and the indexes grow without bound
// for any active user. Reconciling them is background work, so it lives here
// rather than in a request path.
//
// Every API server replica runs this job, so the schedule is jittered and the
// actual pass is guarded by a Redis lock: replicas wake at different instants
// and the first to claim the lock sweeps while the rest skip the tick.
type Cleaner struct {
	redis    *redis.Client
	store    SessionStore
	interval time.Duration
	jitter   time.Duration

	// jitterFn returns the delay to apply before the next attempt. It is a
	// field so tests can make the schedule deterministic.
	jitterFn func(time.Duration) time.Duration

	mu       sync.Mutex
	started  bool
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewCleaner builds the session cleanup job. Non-positive interval or jitter
// falls back to the package defaults; a nil store disables the table sweep.
func NewCleaner(rdb *redis.Client, store SessionStore, interval, jitter time.Duration) *Cleaner {
	if interval <= 0 {
		interval = DefaultCleanupInterval
	}
	if jitter < 0 {
		jitter = 0
	}
	return &Cleaner{
		redis:    rdb,
		store:    store,
		interval: interval,
		jitter:   jitter,
		jitterFn: randomJitter,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Start launches the sweep loop. The loop ends when ctx is cancelled or Stop is
// called.
func (c *Cleaner) Start(ctx context.Context) {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return
	}
	c.started = true
	c.mu.Unlock()

	go c.run(ctx)
}

// Stop ends the sweep loop after the in-flight pass finishes. It is safe to
// call on a cleaner that was never started.
func (c *Cleaner) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })

	c.mu.Lock()
	started := c.started
	c.mu.Unlock()
	if !started {
		return
	}

	select {
	case <-c.doneCh:
	case <-time.After(c.interval):
		log.Warn().Msg("session cleanup worker did not stop within one interval")
	}
}

func (c *Cleaner) run(ctx context.Context) {
	defer close(c.doneCh)
	log.Info().
		Dur("interval", c.interval).
		Dur("jitter", c.jitter).
		Msg("session cleanup worker started")

	for {
		// Jitter before contending for the lock. Without it every replica
		// would fire on the same tick, all but one would lose the SETNX, and
		// the fleet would spend a round trip per replica per interval to
		// discover the same thing.
		if delay := c.jitterFn(c.jitter); delay > 0 {
			if !c.sleep(ctx, delay) {
				return
			}
		}

		stats, held, err := c.RunOnce(ctx)
		switch {
		case err != nil:
			log.Error().Err(err).Msg("session cleanup sweep failed")
		case !held:
			log.Debug().Msg("session cleanup skipped: another replica holds the lock")
		default:
			log.Info().
				Int("sets_scanned", stats.SetsScanned).
				Int64("dangling_members", stats.DanglingMembers).
				Int64("empty_sets_removed", stats.EmptySetsRemoved).
				Int64("rows_deleted", stats.RowsDeleted).
				Msg("session cleanup sweep complete")
		}

		if !c.sleep(ctx, c.interval) {
			return
		}
	}
}

// RunOnce performs a single sweep. The bool reports whether this instance won
// the cleanup lock: false with a nil error means another replica owns the sweep
// and this call did no work.
func (c *Cleaner) RunOnce(ctx context.Context) (CleanupStats, bool, error) {
	var stats CleanupStats

	token, err := newLockToken()
	if err != nil {
		return stats, false, err
	}

	acquired, err := c.redis.SetNX(ctx, cleanupLockKey, token, cleanupLockTTL).Result()
	if err != nil {
		return stats, false, fmt.Errorf("acquiring session cleanup lock: %w", err)
	}
	if !acquired {
		return stats, false, nil
	}
	defer c.release(token)

	stats, err = c.sweep(ctx)
	if err != nil {
		return stats, true, err
	}
	return stats, true, nil
}

// sweep reclaims Redis session state first, then the sessions table. Redis work
// comes first because a Redis outage should not block the table sweep.
func (c *Cleaner) sweep(ctx context.Context) (CleanupStats, error) {
	var stats CleanupStats

	// Collect the whole keyspace view before deleting anything. SCAN may skip
	// keys that are added or removed while it iterates, so pruning during the
	// scan could leave an index set unreconciled until the next pass.
	var cursor uint64
	var sets []string
	for {
		keys, next, err := c.redis.Scan(ctx, cursor, userSessionsPattern, cleanupScanBatch).Result()
		if err != nil {
			return stats, fmt.Errorf("scanning session index sets: %w", err)
		}
		sets = append(sets, keys...)
		cursor = next
		if cursor == 0 {
			break
		}
	}

	for _, key := range sets {
		removed, emptied, err := c.pruneSet(ctx, key)
		if err != nil {
			return stats, err
		}
		stats.SetsScanned++
		stats.DanglingMembers += removed
		if emptied {
			stats.EmptySetsRemoved++
		}
	}

	if c.store == nil {
		return stats, nil
	}
	deleted, err := c.store.DeleteExpiredSessions(ctx)
	if err != nil {
		return stats, err
	}
	stats.RowsDeleted = deleted
	return stats, nil
}

// pruneSet drops the members of one user:sessions:<id> set whose session key
// has already expired, then deletes the set if nothing live is left in it.
//
// A missing session key means the refresh token is permanently gone, so the
// member is safe to drop: Create writes the session key and the set member in a
// single MULTI/EXEC, so a live token is never briefly invisible.
func (c *Cleaner) pruneSet(ctx context.Context, key string) (removed int64, emptied bool, err error) {
	hashes, err := c.redis.SMembers(ctx, key).Result()
	if err != nil {
		return 0, false, fmt.Errorf("listing members of %s: %w", key, err)
	}
	if len(hashes) == 0 {
		// A set with zero members cannot exist in Redis, so the key disappeared
		// between the scan and now. Nothing left to reclaim.
		return 0, true, nil
	}

	pipe := c.redis.Pipeline()
	exists := make([]*redis.IntCmd, len(hashes))
	for i, hash := range hashes {
		exists[i] = pipe.Exists(ctx, sessionKeyFor(hash))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return 0, false, fmt.Errorf("checking session keys for %s: %w", key, err)
	}

	var dangling []interface{}
	for i, cmd := range exists {
		if cmd.Val() == 0 {
			dangling = append(dangling, hashes[i])
		}
	}
	if len(dangling) == 0 {
		return 0, false, nil
	}

	if err := c.redis.SRem(ctx, key, dangling...).Err(); err != nil {
		return 0, false, fmt.Errorf("removing expired members from %s: %w", key, err)
	}

	size, err := c.redis.SCard(ctx, key).Result()
	if err != nil {
		return int64(len(dangling)), false, fmt.Errorf("sizing %s: %w", key, err)
	}
	if size > 0 {
		return int64(len(dangling)), false, nil
	}
	if err := c.redis.Del(ctx, key).Err(); err != nil {
		return int64(len(dangling)), false, fmt.Errorf("deleting empty session set %s: %w", key, err)
	}
	return int64(len(dangling)), true, nil
}

// releaseCleanupLock deletes the lock only when it is still the one this
// instance took, so a holder whose lease already expired can never release a
// successor's lock.
var releaseCleanupLock = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

func (c *Cleaner) release(token string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := releaseCleanupLock.Run(ctx, c.redis, []string{cleanupLockKey}, token).Result(); err != nil {
		log.Warn().Err(err).Msg("releasing session cleanup lock")
	}
}

// sleep waits for d and reports whether the loop should keep running. It
// returns false once the context is cancelled or Stop was called.
func (c *Cleaner) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		case <-c.stopCh:
			return false
		default:
			return true
		}
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	case <-c.stopCh:
		return false
	}
}

func newLockToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generating cleanup lock token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// randomJitter returns a uniform delay in [0, max). A zero max disables jitter.
func randomJitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return max / 2
	}
	return time.Duration(n.Int64())
}

func sessionKeyFor(hash string) string {
	return "session:" + hash
}
