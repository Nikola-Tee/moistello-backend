package session

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// pgSessionStore reclaims rows in the sessions table created by migration 012.
type pgSessionStore struct {
	db *sqlx.DB
}

// NewSessionStore returns a SessionStore backed by the sessions table.
func NewSessionStore(db *sqlx.DB) SessionStore {
	return &pgSessionStore{db: db}
}

func (s *pgSessionStore) DeleteExpiredSessions(ctx context.Context) (int64, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE expires_at < NOW()`)
	if err != nil {
		return 0, fmt.Errorf("deleting expired sessions: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("counting deleted sessions: %w", err)
	}
	return deleted, nil
}
