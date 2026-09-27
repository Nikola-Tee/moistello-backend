package audit

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const entryColumns = "id, actor_id, action, resource_type, resource_id, details, ip_address, user_agent, created_at"

func newMockRepo(t *testing.T) (Repository, sqlmock.Sqlmock, func()) {
	t.Helper()
	sqlDB, dbm, err := sqlmock.New()
	require.NoError(t, err)
	return NewRepository(sqlx.NewDb(sqlDB, "postgres")), dbm, func() { _ = sqlDB.Close() }
}

func mustTime(t *testing.T, value string) *time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	require.NoError(t, err)
	return &parsed
}

func auditRows(n int) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{
		"id", "actor_id", "action", "resource_type", "resource_id", "details", "ip_address", "user_agent", "created_at",
	})
	for i := 0; i < n; i++ {
		rows.AddRow(
			uuid.New(), uuid.New(), "circle.inspected", "circle", uuid.New(),
			[]byte(`{"reason":"review"}`), "10.0.0.1", "curl/8", time.Now().UTC(),
		)
	}
	return rows
}

// TestList_NoFilterQueriesEverything checks the unfiltered path keeps working
// and still paginates.
func TestList_NoFilterQueriesEverything(t *testing.T) {
	repo, dbm, closeFn := newMockRepo(t)
	defer closeFn()

	dbm.ExpectQuery(`SELECT COUNT\(\*\) FROM audit_log`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(7))
	dbm.ExpectQuery(`SELECT `+entryColumns+` FROM audit_log ORDER BY created_at DESC, id DESC LIMIT \$1 OFFSET \$2`).
		WithArgs(20, 0).
		WillReturnRows(auditRows(2))

	entries, total, err := repo.List(context.Background(), ListFilter{}, 1, 20)
	require.NoError(t, err)
	assert.Equal(t, 7, total)
	assert.Len(t, entries, 2)
	assert.NoError(t, dbm.ExpectationsWereMet())
}

// TestList_FilterByResourceType pins the type filter down to the column it
// claims, with the value bound rather than interpolated.
func TestList_FilterByResourceType(t *testing.T) {
	repo, dbm, closeFn := newMockRepo(t)
	defer closeFn()

	dbm.ExpectQuery(`SELECT COUNT\(\*\) FROM audit_log WHERE resource_type = \$1`).
		WithArgs("circle").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	dbm.ExpectQuery(`FROM audit_log WHERE resource_type = \$1 ORDER BY`).
		WithArgs("circle", 20, 0).
		WillReturnRows(auditRows(1))

	_, total, err := repo.List(context.Background(), ListFilter{ResourceType: "circle"}, 1, 20)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.NoError(t, dbm.ExpectationsWereMet())
}

func TestList_FilterByAction(t *testing.T) {
	repo, dbm, closeFn := newMockRepo(t)
	defer closeFn()

	dbm.ExpectQuery(`FROM audit_log WHERE action = \$1`).
		WithArgs("circle.inspected").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	dbm.ExpectQuery(`FROM audit_log WHERE action = \$1 ORDER BY`).
		WithArgs("circle.inspected", 20, 0).
		WillReturnRows(auditRows(1))

	_, _, err := repo.List(context.Background(), ListFilter{Action: "circle.inspected"}, 1, 20)
	require.NoError(t, err)
	assert.NoError(t, dbm.ExpectationsWereMet())
}

func TestList_FilterByActor(t *testing.T) {
	repo, dbm, closeFn := newMockRepo(t)
	defer closeFn()
	actorID := uuid.New()

	dbm.ExpectQuery(`FROM audit_log WHERE actor_id = \$1`).
		WithArgs(actorID.String()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	dbm.ExpectQuery(`FROM audit_log WHERE actor_id = \$1 ORDER BY`).
		WithArgs(actorID.String(), 20, 0).
		WillReturnRows(auditRows(1))

	_, _, err := repo.List(context.Background(), ListFilter{ActorID: &actorID}, 1, 20)
	require.NoError(t, err)
	assert.NoError(t, dbm.ExpectationsWereMet())
}

// TestList_FilterByTimeRange checks the bounds are inclusive and land on
// created_at.
func TestList_FilterByTimeRange(t *testing.T) {
	repo, dbm, closeFn := newMockRepo(t)
	defer closeFn()

	from := mustTime(t, "2026-01-01T00:00:00Z")
	to := mustTime(t, "2026-01-31T23:59:59Z")

	dbm.ExpectQuery(`FROM audit_log WHERE created_at >= \$1 AND created_at <= \$2`).
		WithArgs(*from, *to).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	dbm.ExpectQuery(`FROM audit_log WHERE created_at >= \$1 AND created_at <= \$2 ORDER BY`).
		WithArgs(*from, *to, 20, 0).
		WillReturnRows(auditRows(1))

	_, _, err := repo.List(context.Background(), ListFilter{From: from, To: to}, 1, 20)
	require.NoError(t, err)
	assert.NoError(t, dbm.ExpectationsWereMet())
}

// TestList_AllFiltersCombined checks the placeholder numbering survives a filter
// with every field set, which is the case most likely to bind an argument in the
// wrong position.
func TestList_AllFiltersCombined(t *testing.T) {
	repo, dbm, closeFn := newMockRepo(t)
	defer closeFn()

	actorID := uuid.New()
	from := mustTime(t, "2026-01-01T00:00:00Z")
	to := mustTime(t, "2026-02-01T00:00:00Z")
	filter := ListFilter{
		ResourceType: "circle",
		Action:       "circle.inspected",
		ActorID:      &actorID,
		From:         from,
		To:           to,
	}

	where := `WHERE resource_type = \$1 AND action = \$2 AND actor_id = \$3 AND created_at >= \$4 AND created_at <= \$5`
	dbm.ExpectQuery(`SELECT COUNT\(\*\) FROM audit_log `+where).
		WithArgs("circle", "circle.inspected", actorID.String(), *from, *to).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	dbm.ExpectQuery(`FROM audit_log `+where+` ORDER BY created_at DESC, id DESC LIMIT \$6 OFFSET \$7`).
		WithArgs("circle", "circle.inspected", actorID.String(), *from, *to, 20, 0).
		WillReturnRows(auditRows(1))

	_, _, err := repo.List(context.Background(), filter, 1, 20)
	require.NoError(t, err)
	assert.NoError(t, dbm.ExpectationsWereMet())
}

// TestList_FilterValuesAreBoundNotInterpolated is the injection guard: a filter
// value carrying SQL must arrive as an argument, never as part of the statement.
func TestList_FilterValuesAreBoundNotInterpolated(t *testing.T) {
	repo, dbm, closeFn := newMockRepo(t)
	defer closeFn()

	const injected = "circle'; DROP TABLE audit_log; --"
	dbm.ExpectQuery(`FROM audit_log WHERE resource_type = \$1`).
		WithArgs(injected).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	dbm.ExpectQuery(`FROM audit_log WHERE resource_type = \$1 ORDER BY`).
		WithArgs(injected, 20, 0).
		WillReturnRows(auditRows(0))

	entries, total, err := repo.List(context.Background(), ListFilter{ResourceType: injected}, 1, 20)
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, entries)
	assert.NoError(t, dbm.ExpectationsWereMet(),
		"the injected value must be matched as a bound argument, not as SQL")
}

// TestList_CountUsesTheSameFilter guards the pagination contract: the total has
// to describe the filtered set, not the whole table.
func TestList_CountUsesTheSameFilter(t *testing.T) {
	repo, dbm, closeFn := newMockRepo(t)
	defer closeFn()

	// The unfiltered count is never asked for.
	dbm.ExpectQuery(`SELECT COUNT\(\*\) FROM audit_log WHERE resource_type = \$1`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	dbm.ExpectQuery(`FROM audit_log WHERE resource_type = \$1 ORDER BY`).
		WithArgs("circle", 10, 20).
		WillReturnRows(auditRows(1))

	_, total, err := repo.List(context.Background(), ListFilter{ResourceType: "circle"}, 3, 10)
	require.NoError(t, err)
	assert.Equal(t, 3, total, "total must count the filtered set, not every row")
	assert.NoError(t, dbm.ExpectationsWereMet())
}

// TestList_OrderingIsStable pins newest-first with a tiebreaker, so paging
// through entries written in the same instant cannot repeat or skip a row.
func TestList_OrderingIsStable(t *testing.T) {
	repo, dbm, closeFn := newMockRepo(t)
	defer closeFn()

	dbm.ExpectQuery(`SELECT COUNT\(\*\) FROM audit_log`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	dbm.ExpectQuery(`ORDER BY created_at DESC, id DESC LIMIT \$1 OFFSET \$2`).
		WithArgs(20, 0).
		WillReturnRows(auditRows(1))

	_, _, err := repo.List(context.Background(), ListFilter{}, 1, 20)
	require.NoError(t, err)
	assert.NoError(t, dbm.ExpectationsWereMet())
}

func TestList_ClampsOutOfRangePaging(t *testing.T) {
	for _, tc := range []struct {
		name       string
		page, lim  int
		wantOffset int
		wantLimit  int
	}{
		{"zero page and limit fall back to the first page", 0, 0, 0, 20},
		{"negative page falls back to the first page", -5, 20, 0, 20},
		{"absurd limit is capped", 1, 5000, 0, 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, dbm, closeFn := newMockRepo(t)
			defer closeFn()

			dbm.ExpectQuery(`SELECT COUNT`).
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			dbm.ExpectQuery(`LIMIT \$1 OFFSET \$2`).
				WithArgs(tc.wantLimit, tc.wantOffset).
				WillReturnRows(auditRows(0))

			entries, total, err := repo.List(context.Background(), ListFilter{}, tc.page, tc.lim)
			require.NoError(t, err)
			assert.Equal(t, 0, total)
			assert.Empty(t, entries)
			assert.NoError(t, dbm.ExpectationsWereMet())
		})
	}
}

func TestList_CountFailureIsReported(t *testing.T) {
	repo, dbm, closeFn := newMockRepo(t)
	defer closeFn()

	dbm.ExpectQuery(`SELECT COUNT`).
		WillReturnError(assertAnError{})

	_, _, err := repo.List(context.Background(), ListFilter{ResourceType: "circle"}, 1, 20)
	require.Error(t, err)
}

type assertAnError struct{}

func (assertAnError) Error() string { return "boom" }

// ---------------------------------------------------------------------------
// Filter value semantics
// ---------------------------------------------------------------------------

func TestListFilter_IsZero(t *testing.T) {
	assert.True(t, ListFilter{}.IsZero())
	assert.False(t, ListFilter{ResourceType: "circle"}.IsZero())
	assert.False(t, ListFilter{ActorID: &uuid.UUID{}}.IsZero())
	assert.False(t, ListFilter{From: mustTime(t, "2026-01-01T00:00:00Z")}.IsZero())
}

func TestListFilter_Inverted(t *testing.T) {
	from := mustTime(t, "2026-02-01T00:00:00Z")
	to := mustTime(t, "2026-01-01T00:00:00Z")
	same := mustTime(t, "2026-01-01T00:00:00Z")

	assert.True(t, ListFilter{From: from, To: to}.Inverted())
	assert.False(t, ListFilter{From: same, To: same}.Inverted(), "an instant is not inverted")
	assert.False(t, ListFilter{From: to, To: from}.Inverted())
	assert.False(t, ListFilter{From: from}.Inverted(), "an open-ended range is not inverted")
	assert.False(t, ListFilter{}.Inverted())
}

func TestListFilter_BuildWhere(t *testing.T) {
	t.Run("empty filter has no where clause", func(t *testing.T) {
		where, args := ListFilter{}.buildWhere()
		assert.Empty(t, where)
		assert.Empty(t, args)
	})

	t.Run("each filter adds one bound argument", func(t *testing.T) {
		id := uuid.New()
		ts := mustTime(t, "2026-01-01T00:00:00Z")
		where, args := ListFilter{ResourceType: "circle", ActorID: &id, From: ts}.buildWhere()
		assert.Equal(t, " WHERE resource_type = $1 AND actor_id = $2 AND created_at >= $3", where)
		assert.Equal(t, []any{"circle", id.String(), *ts}, args)
	})
}
