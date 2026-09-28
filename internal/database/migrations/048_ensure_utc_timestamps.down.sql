-- Revert UTC timestamp documentation comments (#468).
COMMENT ON COLUMN users.created_at IS NULL;
COMMENT ON COLUMN users.updated_at IS NULL;
COMMENT ON COLUMN circles.created_at IS NULL;
COMMENT ON COLUMN circles.updated_at IS NULL;
COMMENT ON COLUMN contributions.created_at IS NULL;
COMMENT ON COLUMN payouts.created_at IS NULL;
COMMENT ON COLUMN sessions.created_at IS NULL;
COMMENT ON COLUMN sessions.expires_at IS NULL;
COMMENT ON COLUMN audit_log.created_at IS NULL;
COMMENT ON COLUMN notifications.created_at IS NULL;
COMMENT ON COLUMN governance_proposals.created_at IS NULL;
COMMENT ON COLUMN governance_proposals.voting_ends_at IS NULL;
COMMENT ON COLUMN governance_proposals.executed_at IS NULL;
