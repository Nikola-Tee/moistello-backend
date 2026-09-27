package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/moistello/backend/internal/api/handler"
	"github.com/moistello/backend/internal/api/middleware"
	"github.com/moistello/backend/internal/domain/audit"
	"github.com/moistello/backend/pkg/response"
)

// stubAuditRepo records the filter it was asked for so the tests can assert on
// what the endpoint actually queries.
type stubAuditRepo struct {
	gotFilter audit.ListFilter
	gotPage   int
	gotLimit  int
	entries   []audit.AuditEntry
	total     int
	err       error
}

func (s *stubAuditRepo) Log(context.Context, *audit.AuditEntry) error { return nil }

func (s *stubAuditRepo) List(_ context.Context, filter audit.ListFilter, page, limit int) ([]audit.AuditEntry, int, error) {
	s.gotFilter, s.gotPage, s.gotLimit = filter, page, limit
	return s.entries, s.total, s.err
}

// auditRouter wires the endpoint the way the real router does: behind
// authentication, then behind the admin check.
func auditRouter(role string, repo audit.Repository) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := handler.NewAdminHandler(nil, nil, nil, repo, nil, nil, nil)

	group := r.Group("/admin")
	group.Use(func(c *gin.Context) {
		if role == "" {
			// Stands in for AuthMiddleware rejecting an anonymous caller.
			response.Unauthorized(c, "authentication required")
			c.Abort()
			return
		}
		c.Set("role", role)
		c.Next()
	})
	group.Use(middleware.AdminMiddleware())
	group.GET("/audit-log", h.GetAuditLog)
	return r
}

func getAuditLog(t *testing.T, r *gin.Engine, query string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/admin/audit-log"+query, nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

// ---------------------------------------------------------------------------
// Admin-only access
// ---------------------------------------------------------------------------

// TestGetAuditLog_RequiresAdmin is the access-control check: the audit log
// exposes who did what to whom, so it must not be readable by anyone else.
func TestGetAuditLog_RequiresAdmin(t *testing.T) {
	t.Run("anonymous caller is rejected", func(t *testing.T) {
		rec := getAuditLog(t, auditRouter("", &stubAuditRepo{}), "")
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("authenticated non-admin is forbidden", func(t *testing.T) {
		rec := getAuditLog(t, auditRouter("user", &stubAuditRepo{}), "")
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), "admin access required")
	})

	t.Run("admin is allowed", func(t *testing.T) {
		repo := &stubAuditRepo{entries: []audit.AuditEntry{}, total: 0}
		rec := getAuditLog(t, auditRouter("admin", repo), "")
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

// TestGetAuditLog_ForbiddenCallersNeverReachTheRepository proves the rejection
// happens before the query runs, not merely that the response says 403.
func TestGetAuditLog_ForbiddenCallersNeverReachTheRepository(t *testing.T) {
	for _, role := range []string{"", "user", "member", "moderator"} {
		repo := &stubAuditRepo{}
		rec := getAuditLog(t, auditRouter(role, repo), "?resource_type=circle")
		require.NotEqual(t, http.StatusOK, rec.Code, "role %q must not read the audit log", role)
		assert.Equal(t, 0, repo.gotLimit, "role %q must not have reached the repository", role)
	}
}

// ---------------------------------------------------------------------------
// Queryability
// ---------------------------------------------------------------------------

// TestGetAuditLog_FiltersArePassedThrough is the main queryability check: each
// documented filter has to reach the repository as a real constraint, and an
// unfiltered request must not invent one.
func TestGetAuditLog_FiltersArePassedThrough(t *testing.T) {
	actorID := uuid.New()
	from := "2026-01-01T00:00:00Z"
	to := "2026-01-31T23:59:59Z"

	t.Run("no filters", func(t *testing.T) {
		repo := &stubAuditRepo{}
		getAuditLog(t, auditRouter("admin", repo), "")
		assert.True(t, repo.gotFilter.IsZero(), "an unfiltered request must not constrain the query")
	})

	t.Run("resource type", func(t *testing.T) {
		repo := &stubAuditRepo{}
		getAuditLog(t, auditRouter("admin", repo), "?resource_type=circle")
		assert.Equal(t, "circle", repo.gotFilter.ResourceType)
	})

	t.Run("action", func(t *testing.T) {
		repo := &stubAuditRepo{}
		getAuditLog(t, auditRouter("admin", repo), "?action=circle.inspected")
		assert.Equal(t, "circle.inspected", repo.gotFilter.Action)
	})

	t.Run("actor", func(t *testing.T) {
		repo := &stubAuditRepo{}
		getAuditLog(t, auditRouter("admin", repo), "?actor_id="+actorID.String())
		require.NotNil(t, repo.gotFilter.ActorID)
		assert.Equal(t, actorID, *repo.gotFilter.ActorID)
	})

	t.Run("time range", func(t *testing.T) {
		repo := &stubAuditRepo{}
		getAuditLog(t, auditRouter("admin", repo), "?from="+from+"&to="+to)
		require.NotNil(t, repo.gotFilter.From)
		require.NotNil(t, repo.gotFilter.To)
		assert.Equal(t, from, repo.gotFilter.From.Format(time.RFC3339))
		assert.Equal(t, to, repo.gotFilter.To.Format(time.RFC3339))
	})

	t.Run("every filter at once", func(t *testing.T) {
		repo := &stubAuditRepo{}
		getAuditLog(t, auditRouter("admin", repo),
			"?resource_type=circle&action=circle.inspected&actor_id="+actorID.String()+"&from="+from+"&to="+to)
		assert.Equal(t, "circle", repo.gotFilter.ResourceType)
		assert.Equal(t, "circle.inspected", repo.gotFilter.Action)
		require.NotNil(t, repo.gotFilter.ActorID)
		assert.Equal(t, actorID, *repo.gotFilter.ActorID)
		require.NotNil(t, repo.gotFilter.From)
		require.NotNil(t, repo.gotFilter.To)
	})

	t.Run("surrounding whitespace is trimmed", func(t *testing.T) {
		repo := &stubAuditRepo{}
		getAuditLog(t, auditRouter("admin", repo), "?resource_type=%20circle%20")
		assert.Equal(t, "circle", repo.gotFilter.ResourceType)
	})
}

// TestGetAuditLog_RejectsMalformedFilters keeps a typo from silently becoming an
// unfiltered query over the entire audit log.
func TestGetAuditLog_RejectsMalformedFilters(t *testing.T) {
	for _, tc := range []struct{ name, query, expect string }{
		{"actor id is not a uuid", "?actor_id=not-a-uuid", "actor_id"},
		{"actor id is truncated", "?actor_id=1234", "actor_id"},
		{"from is unparseable", "?from=yesterday", "from"},
		{"to is unparseable", "?to=2026-13-45", "to"},
		{"range is inverted", "?from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z", "from must not be after to"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &stubAuditRepo{}
			rec := getAuditLog(t, auditRouter("admin", repo), tc.query)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), tc.expect)
			assert.Equal(t, 0, repo.gotLimit, "a malformed filter must not reach the repository")
		})
	}
}

// TestGetAuditLog_AcceptsDateOnlyBounds covers the plain-date form an admin
// console would send, and pins that it brackets the whole day.
func TestGetAuditLog_AcceptsDateOnlyBounds(t *testing.T) {
	repo := &stubAuditRepo{}
	rec := getAuditLog(t, auditRouter("admin", repo), "?from=2026-01-01&to=2026-01-31")
	require.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, repo.gotFilter.From)
	require.NotNil(t, repo.gotFilter.To)
	assert.Equal(t, "2026-01-01T00:00:00Z", repo.gotFilter.From.UTC().Format(time.RFC3339))
	assert.Equal(t, "2026-01-31T00:00:00Z", repo.gotFilter.To.UTC().Format(time.RFC3339))
}

// TestGetAuditLog_ResponseShape covers the success envelope, including the empty
// case, which must serialize as an empty array rather than null.
func TestGetAuditLog_ResponseShape(t *testing.T) {
	now := time.Now().UTC()

	t.Run("entries and pagination meta", func(t *testing.T) {
		repo := &stubAuditRepo{
			entries: []audit.AuditEntry{{
				ID: uuid.New(), Action: "circle.inspected", ResourceType: "circle", CreatedAt: now,
			}},
			total: 42,
		}
		rec := getAuditLog(t, auditRouter("admin", repo), "?page=2&limit=10")
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, 2, repo.gotPage)
		assert.Equal(t, 10, repo.gotLimit)

		var body struct {
			Success bool `json:"success"`
			Data    struct {
				Entries []audit.AuditEntry `json:"entries"`
			} `json:"data"`
			Meta response.PaginationMeta `json:"meta"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.True(t, body.Success)
		require.Len(t, body.Data.Entries, 1)
		assert.Equal(t, "circle.inspected", body.Data.Entries[0].Action)
		assert.Equal(t, 42, body.Meta.TotalItems)
		assert.Equal(t, 2, body.Meta.Page)
		assert.Equal(t, 10, body.Meta.Limit)
		assert.Equal(t, 5, body.Meta.TotalPages)
		assert.True(t, body.Meta.HasMore)
	})

	t.Run("no results is an empty array", func(t *testing.T) {
		rec := getAuditLog(t, auditRouter("admin", &stubAuditRepo{}), "")
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"entries":[]`)
	})
}

func TestGetAuditLog_RepositoryFailureIsNotLeaked(t *testing.T) {
	repo := &stubAuditRepo{err: assert.AnError}
	rec := getAuditLog(t, auditRouter("admin", repo), "")
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.NotContains(t, rec.Body.String(), assert.AnError.Error(),
		"internal errors must not be echoed to the client")
}
