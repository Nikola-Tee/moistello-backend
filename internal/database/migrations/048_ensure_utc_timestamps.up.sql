-- Migrate all timestamp columns to use UTC consistently (#468).
-- PostgreSQL TIMESTAMPTZ already stores in UTC internally, but this migration
-- ensures all columns have explicit timezone awareness and adds a comment
-- documenting the UTC convention for future contributors.

-- Add UTC documentation comments to key timestamp columns
COMMENT ON COLUMN users.created_at IS 'Stored as UTC (TIMESTAMPTZ). Application code must use time.UTC.';
COMMENT ON COLUMN users.updated_at IS 'Stored as UTC (TIMESTAMPTZ). Application code must use time.UTC.';
COMMENT ON COLUMN circles.created_at IS 'Stored as UTC (TIMESTAMPTZ). Application code must use time.UTC.';
COMMENT ON COLUMN circles.updated_at IS 'Stored as UTC (TIMESTAMPTZ). Application code must use time.UTC.';
COMMENT ON COLUMN contributions.created_at IS 'Stored as UTC (TIMESTAMPTZ). Application code must use time.UTC.';
COMMENT ON COLUMN payouts.created_at IS 'Stored as UTC (TIMESTAMPTZ). Application code must use time.UTC.';
COMMENT ON COLUMN sessions.created_at IS 'Stored as UTC (TIMESTAMPTZ). Application code must use time.UTC.';
COMMENT ON COLUMN sessions.expires_at IS 'Stored as UTC (TIMESTAMPTZ). Application code must use time.UTC.';
COMMENT ON COLUMN audit_log.created_at IS 'Stored as UTC (TIMESTAMPTZ). Application code must use time.UTC.';
COMMENT ON COLUMN notifications.created_at IS 'Stored as UTC (TIMESTAMPTZ). Application code must use time.UTC.';
COMMENT ON COLUMN governance_proposals.created_at IS 'Stored as UTC (TIMESTAMPTZ). Application code must use time.UTC.';
COMMENT ON COLUMN governance_proposals.voting_ends_at IS 'Stored as UTC (TIMESTAMPTZ). Application code must use time.UTC.';
COMMENT ON COLUMN governance_proposals.executed_at IS 'Stored as UTC (TIMESTAMPTZ). Application code must use time.UTC.';
