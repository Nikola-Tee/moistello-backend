package user

import "errors"

var (
	ErrUserNotFound = errors.New("user not found")
	ErrInvalidUUID  = errors.New("invalid user ID format")
	// ErrUserNotDeleted is returned by Restore when the user exists but is
	// already active, so a repeated restore is not reported as a no-op success.
	ErrUserNotDeleted = errors.New("user is not deleted")
)
