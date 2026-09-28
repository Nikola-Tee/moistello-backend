package handler

import (
	"errors"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/moistello/backend/internal/indexer"
	"github.com/moistello/backend/pkg/response"
)

// AdminIndexerHandler exposes the indexer dead-letter queue for inspection and
// triage (#349). Without it, an event the indexer failed to process would be
// invisible: the poll loop logs the error, advances the cursor, and moves on.
type AdminIndexerHandler struct {
	deadLetters indexer.DeadLetterStore
}

func NewAdminIndexerHandler(deadLetters indexer.DeadLetterStore) *AdminIndexerHandler {
	return &AdminIndexerHandler{deadLetters: deadLetters}
}

// GetDeadLetterEvents lists indexer events that could not be processed.
//
// @Summary [Admin] List dead-lettered indexer events
// @Description Lists Stellar events the indexer failed to process, newest first, with the failure reason and attempt count. Admin only.
// @Tags Admin
// @Produce json
// @Security BearerAuth
// @Param limit query int false "Maximum entries to return" default(100)
// @Success 200 {object} response.Envelope
// @Failure 500 {object} response.Envelope
// @Router /admin/indexer/dead-letter [get]
func (h *AdminIndexerHandler) GetDeadLetterEvents(c *gin.Context) {
	if h.deadLetters == nil {
		response.OK(c, gin.H{"dead_letter_events": []any{}, "total": 0})
		return
	}

	limit := 100
	if raw := c.Query("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	entries, err := h.deadLetters.List(c.Request.Context(), limit)
	if err != nil {
		response.InternalError(c, "failed to fetch dead letter events")
		return
	}
	if entries == nil {
		entries = []indexer.DeadLetterEntry{}
	}

	total, err := h.deadLetters.Count(c.Request.Context())
	if err != nil {
		response.InternalError(c, "failed to count dead letter events")
		return
	}

	response.OK(c, gin.H{"dead_letter_events": entries, "total": total})
}

// ResolveDeadLetterEvent marks a dead-lettered event as handled, clearing it
// from the backlog once the underlying fault has been fixed.
//
// @Summary [Admin] Resolve a dead-lettered indexer event
// @Description Marks a dead-lettered indexer event as resolved and removes it from the backlog. The record is retained for audit. Admin only.
// @Tags Admin
// @Produce json
// @Security BearerAuth
// @Param id path string true "Dead-letter entry ID"
// @Success 200 {object} response.Envelope
// @Failure 404 {object} response.Envelope
// @Router /admin/indexer/dead-letter/{id}/resolve [post]
func (h *AdminIndexerHandler) ResolveDeadLetterEvent(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.BadRequest(c, "dead letter entry ID is required")
		return
	}

	if h.deadLetters == nil {
		response.OK(c, gin.H{"id": id, "resolved": true})
		return
	}

	if err := h.deadLetters.Resolve(c.Request.Context(), id); err != nil {
		if errors.Is(err, indexer.ErrDeadLetterNotFound) {
			response.NotFound(c, "dead letter entry not found")
			return
		}
		response.InternalError(c, "failed to resolve dead letter event")
		return
	}

	response.OK(c, gin.H{"id": id, "resolved": true})
}
