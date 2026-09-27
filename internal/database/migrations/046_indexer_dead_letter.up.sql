-- Dead-letter queue for indexer events that could not be processed (#349).
--
-- Without this, a transaction that fails to process is logged and skipped: the
-- cursor advances past its ledger, the deduplicator already has its hash, and
-- the event is lost with no durable record that it was ever seen. This table
-- makes the failure recoverable and auditable.
CREATE TABLE IF NOT EXISTS indexer_dead_letter (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    chain        VARCHAR(50) NOT NULL DEFAULT 'stellar',
    tx_hash      TEXT        NOT NULL,
    ledger       BIGINT      NOT NULL,
    error        TEXT        NOT NULL,
    -- How many times processing this event has been attempted, so an operator
    -- can distinguish a one-off failure from a poison event.
    attempts     INT         NOT NULL DEFAULT 1,
    -- The raw Horizon transaction, so a failed event can be replayed after the
    -- underlying bug is fixed without re-fetching history from Horizon.
    payload      JSONB,
    status       VARCHAR(20) NOT NULL DEFAULT 'dead_letter', -- dead_letter, resolved
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at  TIMESTAMPTZ
);

-- A transaction hash identifies exactly one event, so a repeated failure
-- updates the existing row rather than piling up duplicates. The index also
-- serves the admin listing, which reads the unresolved backlog.
CREATE UNIQUE INDEX IF NOT EXISTS idx_indexer_dlq_tx_hash
    ON indexer_dead_letter (tx_hash);

CREATE INDEX IF NOT EXISTS idx_indexer_dlq_status_created
    ON indexer_dead_letter (status, created_at DESC);

-- Supports the "what is stuck around this ledger" question during incident
-- triage, which is the usual reason an operator opens this table.
CREATE INDEX IF NOT EXISTS idx_indexer_dlq_ledger
    ON indexer_dead_letter (ledger);