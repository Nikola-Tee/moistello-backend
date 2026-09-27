package user

import (
	"context"

	"github.com/google/uuid"
)

type UserFilter struct {
	Search string
	Page   int
	Limit  int
}

type Repository interface {
	FindByID(ctx context.Context, id uuid.UUID) (*User, error)
	FindByWalletAddress(ctx context.Context, walletAddress string) (*User, error)
	FindByEmail(ctx context.Context, email string) (*User, error)
	FindByPasskeyCredentialID(ctx context.Context, credentialID string) (*User, error)
	Create(ctx context.Context, u *User) error
	Delete(ctx context.Context, id uuid.UUID) error
	// Restore clears deleted_at, returning a soft-deleted user to the active
	// set (#347). It reports ErrUserNotFound when no user with that id exists
	// and ErrUserNotDeleted when the user exists but was never soft-deleted,
	// so a restore aimed at the wrong id is not silently reported as success.
	Restore(ctx context.Context, id uuid.UUID) error
	Update(ctx context.Context, u *User) error
	UpdateMoiScore(ctx context.Context, id uuid.UUID, score int) error
	List(ctx context.Context, filter UserFilter) ([]User, error)
	Count(ctx context.Context, filter UserFilter) (int, error)
	ClaimNextName(ctx context.Context) (int64, error)

	// The following bypass the deleted_at IS NULL filter and exist only for
	// administrative tooling. Nothing on the auth or request path may use
	// them: they are how an admin inspects and lists deleted accounts
	// without re-admitting them (#347).
	FindByIDIncludingDeleted(ctx context.Context, id uuid.UUID) (*User, error)
	ListDeleted(ctx context.Context, filter UserFilter) ([]User, error)
	CountDeleted(ctx context.Context, filter UserFilter) (int, error)
}
