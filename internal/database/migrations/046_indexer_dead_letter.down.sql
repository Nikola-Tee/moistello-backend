DROP INDEX IF EXISTS idx_indexer_dlq_ledger;
DROP INDEX IF EXISTS idx_indexer_dlq_status_created;
DROP INDEX IF EXISTS idx_indexer_dlq_tx_hash;

DROP TABLE IF EXISTS indexer_dead_letter;