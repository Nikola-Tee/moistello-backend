package session

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubStore records how many rows a sweep deleted.
type stubStore struct {
	mu      sync.Mutex
	deleted int64
	err     error
}

func (s *stubStore) DeleteExpiredSessions(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleted, s.err
}

func newTestClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

// seedSession writes a session key and its index-set member the same way
// service.Create does, so a live session is indistinguishable from a real one.
func seedSession(t *testing.T, client *redis.Client, userID, hash string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, client.Set(ctx, sessionKeyFor(hash), userID+"|device|0|user", time.Hour).Err())
	require.NoError(t, client.SAdd(ctx, "user:sessions:"+userID, hash).Err())
}

// seedExpiredSession leaves only the index-set member behind, which is the
// state the sweep exists to reclaim.
func seedExpiredSession(t *testing.T, client *redis.Client, userID, hash string) {
	t.Helper()
	require.NoError(t, client.SAdd(context.Background(), "user:sessions:"+userID, hash).Err())
}

func TestCleanerSweepPrunesExpiredIndexMembers(t *testing.T) {
	_, client := newTestClient(t)
	seedSession(t, client, "user-live", "livehash")
	seedExpiredSession(t, client, "user-live", "deadhash")
	seedExpiredSession(t, client, "user-dead", "onlyhash")

	store := &stubStore{deleted: 3}
	c := NewCleaner(client, store, time.Minute, 0)

	stats, held, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	assert.True(t, held, "the only caller must win the cleanup lock")
	assert.Equal(t, 2, stats.SetsScanned)
	assert.Equal(t, int64(2), stats.DanglingMembers)
	assert.Equal(t, int64(1), stats.EmptySetsRemoved, "the all-dead index set should be deleted")
	assert.Equal(t, int64(3), stats.RowsDeleted)

	// The live member survives; both dead members are gone.
	assert.Equal(t, []string{"livehash"}, client.SMembers(context.Background(), "user:sessions:user-live").Val())
	assert.False(t, client.Exists(context.Background(), "user:sessions:user-dead").Val() > 0)
	assert.True(t, client.Exists(context.Background(), sessionKeyFor("livehash")).Val() > 0,
		"the sweep must never touch a live session key")
}

func TestCleanerSweepIsIdempotent(t *testing.T) {
	_, client := newTestClient(t)
	seedExpiredSession(t, client, "user-a", "hash1")
	seedExpiredSession(t, client, "user-a", "hash2")

	c := NewCleaner(client, &stubStore{}, time.Minute, 0)

	first, _, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(2), first.DanglingMembers)

	second, _, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, second.SetsScanned, "a second pass has nothing left to reconcile")
	assert.Equal(t, int64(0), second.DanglingMembers)
}

func TestCleanerReleasesLockAfterSweep(t *testing.T) {
	_, client := newTestClient(t)
	seedExpiredSession(t, client, "user-a", "hash1")

	c := NewCleaner(client, &stubStore{}, time.Minute, 0)
	_, held, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	require.True(t, held)

	exists, err := client.Exists(context.Background(), cleanupLockKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists, "the lock must not outlive the sweep")
}

// TestCleanerOnlyOneReplicaSweeps is the cross-replica guarantee: N independent
// jobs on the same Redis, all firing on the same tick, produce exactly one sweep.
func TestCleanerOnlyOneReplicaSweeps(t *testing.T) {
	const replicas = 8

	_, client := newTestClient(t)
	for i := 0; i < 25; i++ {
		seedExpiredSession(t, client, fmt.Sprintf("user-%d", i), fmt.Sprintf("hash-%d", i))
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]bool, replicas)
	stats := make([]CleanupStats, replicas)

	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Each replica gets its own store so we can attribute the sweep.
			store := &stubStore{deleted: int64(i + 1)}
			c := NewCleaner(client, store, time.Minute, 0)
			<-start
			s, held, err := c.RunOnce(context.Background())
			assert.NoError(t, err)
			results[i] = held
			stats[i] = s
		}(i)
	}

	close(start)
	wg.Wait()

	winners := 0
	var winner int
	for i, held := range results {
		if held {
			winners++
			winner = i
		}
	}

	assert.Equal(t, 1, winners, "exactly one replica may run the sweep per tick")
	assert.Equal(t, 25, stats[winner].SetsScanned)
	assert.Equal(t, int64(25), stats[winner].DanglingMembers)

	// The losers reported no work at all, not a partial sweep.
	for i, held := range results {
		if held {
			continue
		}
		assert.Zero(t, stats[i].DanglingMembers, "a losing replica must not touch the keyspace")
		assert.Equal(t, 0, stats[i].SetsScanned)
	}
}

// TestCleanerJitterSpreadsReplicas guards the point of jitter: no two replicas
// should draw the same delay often enough to defeat the lock's purpose.
func TestCleanerJitterSpreadsReplicas(t *testing.T) {
	const replicas = 20

	seen := make(map[time.Duration]int, replicas)
	for i := 0; i < replicas; i++ {
		d := randomJitter(30 * time.Second)
		assert.GreaterOrEqual(t, d, time.Duration(0))
		assert.Less(t, d, 30*time.Second)
		seen[d]++
	}
	assert.Greater(t, len(seen), 1, "jitter must not collapse to a single delay")

	assert.Equal(t, time.Duration(0), randomJitter(0), "zero jitter means sweep immediately")
}

func TestCleanerDefaultsApplied(t *testing.T) {
	_, client := newTestClient(t)
	c := NewCleaner(client, nil, 0, -1)
	assert.Equal(t, DefaultCleanupInterval, c.interval)
	assert.Equal(t, time.Duration(0), c.jitter)
	assert.Nil(t, c.store, "a nil store must disable the table sweep, not panic")
}

func TestCleanerWithoutStoreStillSweepsRedis(t *testing.T) {
	_, client := newTestClient(t)
	seedExpiredSession(t, client, "user-a", "hash1")

	c := NewCleaner(client, nil, time.Minute, 0)
	stats, held, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	assert.True(t, held)
	assert.Equal(t, int64(1), stats.DanglingMembers)
	assert.Equal(t, int64(0), stats.RowsDeleted)
}

func TestCleanerStoreErrorSurfaces(t *testing.T) {
	_, client := newTestClient(t)
	seedExpiredSession(t, client, "user-a", "hash1")

	c := NewCleaner(client, &stubStore{err: assert.AnError}, time.Minute, 0)
	_, held, err := c.RunOnce(context.Background())
	assert.Error(t, err)
	assert.True(t, held, "the lock was still held when the sweep failed")
	assert.False(t, client.Exists(context.Background(), cleanupLockKey).Val() > 0,
		"a failed sweep must still release the lock")
}

func TestCleanerStartStopIsClean(t *testing.T) {
	_, client := newTestClient(t)
	c := NewCleaner(client, nil, 10*time.Millisecond, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	c.Start(ctx) // duplicate Start must not spawn a second loop

	time.Sleep(30 * time.Millisecond)
	c.Stop()
	c.Stop() // duplicate Stop must not panic on a closed channel
}

func TestCleanerStopWithoutStartDoesNotBlock(t *testing.T) {
	_, client := newTestClient(t)
	c := NewCleaner(client, nil, time.Hour, 0)

	done := make(chan struct{})
	go func() {
		c.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop on a never-started cleaner must return immediately")
	}
}

func TestCleanerStopsOnContextCancel(t *testing.T) {
	_, client := newTestClient(t)
	c := NewCleaner(client, nil, 10*time.Millisecond, 0)

	ctx, cancel := context.WithCancel(context.Background())
	c.Start(ctx)
	cancel()

	select {
	case <-c.doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelling the context must end the sweep loop")
	}
}

// TestNoCleanupLogicInRequestPaths encodes the grep check from #374: expired
// session reclamation belongs to the scheduled job, so no handler or request-
// facing service may reference the sweep's entry points.
func TestNoCleanupLogicInRequestPaths(t *testing.T) {
	banned := []string{"RunOnce", "CleanupStats", "DeleteExpiredSessions", "pruneSet", "sessionCleaner"}

	// The job's own package is the one place these identifiers may appear, so
	// the scan covers every request-facing package plus the rest of auth.
	roots := []string{
		filepath.Join("..", "..", "..", "api", "handler"),
		filepath.Join("..", "..", "..", "api", "middleware"),
		filepath.Join("..", "..", "..", "domain", "auth"),
	}
	selfDir, err := filepath.Abs(".")
	require.NoError(t, err)

	for _, root := range roots {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				if abs, aErr := filepath.Abs(path); aErr == nil && abs == selfDir {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") {
				return nil
			}

			src, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			// Only flag identifiers the parser sees as code, so the words
			// cannot be laundered through a comment.
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, src, 0)
			if err != nil {
				return err
			}
			var hits []string
			ast.Inspect(file, func(node ast.Node) bool {
				ident, ok := node.(*ast.Ident)
				if !ok {
					return true
				}
				for _, name := range banned {
					if ident.Name == name {
						hits = append(hits, path+": "+name)
					}
				}
				return true
			})
			assert.Empty(t, hits, "cleanup entry point referenced outside the scheduled job: %v", hits)
			return nil
		})
		require.NoError(t, err)
	}
}
