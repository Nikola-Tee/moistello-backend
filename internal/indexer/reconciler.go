package indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// Reconciler detects gaps in processed events by comparing the cursor position
// with the current chain height, and replays any missed ledgers.
type Reconciler struct {
	cursor             *CursorTracker
	poller             *Poller
	processor          *EventProcessor
	dedup              *Deduplicator
	deadLetters        DeadLetterStore
	interval           time.Duration
	batchSize          int
	maxLedgersPerCycle int
}

// NewReconciler creates a Reconciler with the given dependencies.
func NewReconciler(
	cursor *CursorTracker,
	poller *Poller,
	processor *EventProcessor,
	dedup *Deduplicator,
) *Reconciler {
	return &Reconciler{
		cursor:             cursor,
		poller:             poller,
		processor:          processor,
		dedup:              dedup,
		interval:           5 * time.Minute,
		batchSize:          50,
		maxLedgersPerCycle: 500,
	}
}

// WithDeadLetters wires the dead-letter store used to record events that
// could not be processed (#349).
func (r *Reconciler) WithDeadLetters(store DeadLetterStore) *Reconciler {
	r.deadLetters = store
	return r
}

// WithBatchSize sets the number of ledgers fetched in each individual Horizon call.
func (r *Reconciler) WithBatchSize(size int) *Reconciler {
	if size > 0 {
		r.batchSize = size
	}
	return r
}

// WithMaxLedgersPerCycle sets the maximum number of missed ledgers processed per reconciliation cycle.
func (r *Reconciler) WithMaxLedgersPerCycle(max int) *Reconciler {
	if max > 0 {
		r.maxLedgersPerCycle = max
	}
	return r
}

// StartReconciliation runs reconciliation on a periodic ticker until the
// context is cancelled.
func (r *Reconciler) StartReconciliation(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = r.interval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Info().Dur("interval", interval).Msg("reconciler started")

	for {
		select {
		case <-ticker.C:
			if err := r.Reconcile(ctx); err != nil {
				log.Error().Err(err).Msg("reconciliation failed")
			}
		case <-ctx.Done():
			log.Info().Msg("reconciler stopped")
			return
		}
	}
}

// recordDeadLetter stores a failed event so it can be replayed after the
// underlying fault is fixed (#349). It is a no-op when no store is wired, so
// the reconciler stays usable in tests and dry runs.
func (r *Reconciler) recordDeadLetter(ctx context.Context, txn *Transaction, ledger int64, cause error) {
	if r.deadLetters == nil {
		return
	}

	payload, err := MarshalPayload(txn)
	if err != nil {
		// A payload we cannot serialise must not stop the failure being
		// recorded; the hash and error are what matter for triage.
		log.Warn().Err(err).Str("hash", txn.Hash).Msg("serializing dead letter payload")
	}

	if _, err := r.deadLetters.Record(ctx, &DeadLetterEntry{
		Chain:    "stellar",
		TxHash:   txn.Hash,
		Ledger:   ledger,
		Error:    cause.Error(),
		Attempts: 1,
		Payload:  payload,
		Status:   "dead_letter",
	}); err != nil {
		// Bookkeeping must never turn a recoverable per-event failure into a
		// failed reconciliation run.
		log.Error().Err(err).Str("hash", txn.Hash).Msg("recording dead letter failed")
	}
}

// Reconcile checks for gaps between the stored cursor and the latest chain
// ledger, and replays any missed ledgers in batches up to maxLedgersPerCycle.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	cursor, err := r.cursor.GetCurrent(ctx)
	if err != nil {
		return fmt.Errorf("reading cursor: %w", err)
	}

	// Fetch the latest ledger to determine the current chain height
	ledgers, err := r.poller.FetchLedgers(ctx, cursor.LastLedger, 1)
	if err != nil {
		return fmt.Errorf("fetching latest ledger: %w", err)
	}
	if len(ledgers) == 0 {
		return nil
	}

	latestChain := ledgers[0].Sequence + int64(len(ledgers)) - 1
	gap := latestChain - cursor.LastLedger

	if gap > 1000 {
		log.Warn().
			Int64("gap", gap).
			Int64("cursor", cursor.LastLedger).
			Int64("chain", latestChain).
			Msg("large gap detected — indexer may be behind")
	}

	// Nothing to catch up
	if gap <= 0 {
		return nil
	}

	batchSize := r.batchSize
	if batchSize <= 0 {
		batchSize = 50
	}
	maxPerCycle := r.maxLedgersPerCycle
	if maxPerCycle <= 0 {
		maxPerCycle = 500
	}

	totalLedgersFetched := 0
	processed := 0
	skipped := 0
	currentCursor := cursor.LastLedger

	for totalLedgersFetched < maxPerCycle {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		limit := batchSize
		if remaining := maxPerCycle - totalLedgersFetched; remaining < limit {
			limit = remaining
		}

		missedLedgers, err := r.poller.FetchLedgers(ctx, currentCursor, limit)
		if err != nil {
			return fmt.Errorf("fetching missed ledgers: %w", err)
		}
		if len(missedLedgers) == 0 {
			break
		}

		totalLedgersFetched += len(missedLedgers)

		for _, ledger := range missedLedgers {
			currentCursor = ledger.Sequence

			txns, err := r.poller.FetchTransactions(ctx, ledger.Sequence)
			if err != nil {
				log.Warn().Err(err).Int64("ledger", ledger.Sequence).Msg("skipping ledger during reconciliation")
				if err := r.cursor.Update(ctx, ledger.Sequence); err != nil {
					return fmt.Errorf("updating cursor during reconciliation: %w", err)
				}
				continue
			}

			filtered := r.poller.FilterByContract(txns)
			for _, txn := range filtered {
				if r.dedup.Has(txn.Hash) {
					skipped++
					continue
				}
				r.dedup.Add(txn.Hash)

				if err := r.processor.ProcessTransaction(ctx, &txn); err != nil {
					log.Warn().Err(err).
						Str("hash", txn.Hash).
						Msg("reconciler process error")
					// Record the failure so it can be replayed later. Without
					// this the reconciliation advances the cursor past an event
					// that was never processed, losing it silently (#349).
					r.recordDeadLetter(ctx, &txn, ledger.Sequence, err)
					continue
				}
				processed++
			}

			// Update the cursor after each successfully processed ledger (parallel-safe)
			if err := r.cursor.Update(ctx, ledger.Sequence); err != nil {
				return fmt.Errorf("updating cursor during reconciliation: %w", err)
			}
		}

		if len(missedLedgers) < limit {
			break
		}
	}

	if processed > 0 || skipped > 0 {
		log.Info().
			Int64("gap", gap).
			Int("replayed", processed).
			Int("skipped", skipped).
			Int("ledgersReconciled", totalLedgersFetched).
			Msg("reconciliation complete")
	} else if gap < 100 {
		log.Debug().Int64("gap", gap).Msg("reconciliation — no new events")
	}

	return nil
}
