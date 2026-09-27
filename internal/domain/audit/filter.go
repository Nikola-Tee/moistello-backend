package audit

import (
	"time"

	"github.com/google/uuid"
)

// ListFilter narrows an audit log query. A zero-valued field is not filtered on,
// so an empty filter lists everything.
//
// The fields are pointers where "unset" and "set to the zero value" mean
// different things: an explicitly empty actor or a zero timestamp is not a
// meaningful filter, so only a non-nil pointer contributes a condition.
type ListFilter struct {
	// ResourceType matches audit_log.resource_type, e.g. "circle".
	ResourceType string
	// Action matches audit_log.action, e.g. "circle.inspected".
	Action string
	// ActorID matches audit_log.actor_id.
	ActorID *uuid.UUID
	// From is an inclusive lower bound on audit_log.created_at.
	From *time.Time
	// To is an inclusive upper bound on audit_log.created_at.
	To *time.Time
}

// IsZero reports whether the filter selects every entry.
func (f ListFilter) IsZero() bool {
	return f.ResourceType == "" && f.Action == "" && f.ActorID == nil && f.From == nil && f.To == nil
}

// Inverted reports whether the range bounds cannot both be satisfied, which
// callers should reject rather than silently return nothing.
func (f ListFilter) Inverted() bool {
	return f.From != nil && f.To != nil && f.From.After(*f.To)
}
