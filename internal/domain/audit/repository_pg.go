package audit

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

type pgRepo struct {
	db *sqlx.DB
}

func NewRepository(db *sqlx.DB) Repository {
	return &pgRepo{db: db}
}

func (r *pgRepo) Log(ctx context.Context, entry *AuditEntry) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `INSERT INTO audit_log (id, actor_id, action, resource_type, resource_id, details, ip_address, user_agent, created_at)
		VALUES (:id, :actor_id, :action, :resource_type, :resource_id, :details, :ip_address, :user_agent, :created_at)`
	_, err := r.db.NamedExecContext(ctx, query, entry)
	if err != nil {
		return fmt.Errorf("logging audit entry: %w", err)
	}
	return nil
}

// List returns one page of audit entries matching filter, newest first, along
// with the total number of entries matching that same filter so callers can
// paginate without losing the filtered count.
func (r *pgRepo) List(ctx context.Context, filter ListFilter, page, limit int) ([]AuditEntry, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	page, limit = normalizePage(page, limit)
	offset := (page - 1) * limit

	where, args := filter.buildWhere()

	var total int
	countQuery := `SELECT COUNT(*) FROM audit_log` + where
	if err := r.db.GetContext(ctx, &total, countQuery, args...); err != nil {
		return nil, 0, fmt.Errorf("counting audit entries: %w", err)
	}

	// LIMIT/OFFSET are appended after the filter args so their placeholders keep
	// matching the order the conditions were built in.
	pageArgs := append(append([]any{}, args...), limit, offset)
	query := `SELECT id, actor_id, action, resource_type, resource_id, details, ip_address, user_agent, created_at
		FROM audit_log` + where + ` ORDER BY created_at DESC, id DESC LIMIT $` + fmt.Sprint(len(args)+1) +
		` OFFSET $` + fmt.Sprint(len(args)+2)

	var entries []AuditEntry
	if err := r.db.SelectContext(ctx, &entries, query, pageArgs...); err != nil {
		return nil, 0, fmt.Errorf("listing audit entries: %w", err)
	}
	return entries, total, nil
}

func normalizePage(page, limit int) (int, int) {
	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 200 {
		limit = 20
	}
	return page, limit
}

// buildWhere turns the filter into a WHERE clause and its bound arguments.
//
// Each clause is appended with an explicit placeholder rather than interpolated,
// so filter values can never be read as SQL. The order is fixed, which keeps the
// placeholder numbering and the argument slice in step.
func (f ListFilter) buildWhere() (string, []any) {
	var clauses []string
	var args []any

	add := func(format string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(format, len(args)))
	}

	if f.ResourceType != "" {
		add("resource_type = $%d", f.ResourceType)
	}
	if f.Action != "" {
		add("action = $%d", f.Action)
	}
	if f.ActorID != nil {
		add("actor_id = $%d", f.ActorID.String())
	}
	if f.From != nil {
		add("created_at >= $%d", *f.From)
	}
	if f.To != nil {
		add("created_at <= $%d", *f.To)
	}
	if len(clauses) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}
