package models

import "context"

// The acting user travels on the context because the audit row is written deep
// in internal/repository, and the only layer that knows who is calling is the
// HTTP middleware at the top. Threading an actor parameter through every service
// signature would mean changing functions that have no other interest in it —
// including the ones a worker calls, where there is no user at all.
//
// It lives in models rather than in a handler package because both ends of that
// journey have to agree on the key, and internal/services must not import Gin.

// actorKey is an unexported struct type so nothing outside this package can
// collide with it. A string literal key is settable — and readable — by any
// package that guesses the same string.
type actorKey struct{}

// WithActor tags ctx with the user responsible for whatever happens next.
func WithActor(ctx context.Context, userID uint64) context.Context {
	if userID == 0 {
		return ctx
	}
	return context.WithValue(ctx, actorKey{}, userID)
}

// ActorFromContext returns the acting user id, or nil when the change was made
// by the system rather than by a person.
//
// nil is a legitimate answer, not a missing value: the reaper expiring an
// abandoned checkout and the worker settling a charge are both nobody, which is
// why audit_logs.actor_user_id is nullable.
func ActorFromContext(ctx context.Context) *uint64 {
	id, ok := ctx.Value(actorKey{}).(uint64)
	if !ok || id == 0 {
		return nil
	}
	return &id
}
