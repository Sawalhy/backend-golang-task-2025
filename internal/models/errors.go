package models

import "errors"

// Domain outcomes are typed errors, never string matching on a message. A
// handler deciding between 409 and 500 by looking for "insufficient" in a string
// breaks the first time someone rewords the message.
var (
	// ErrInsufficientStock means the conditional UPDATE matched zero rows: at the
	// instant we tried, available < qty. This is a legitimate business outcome
	// (409 Conflict), not a failure to retry.
	ErrInsufficientStock = errors.New("insufficient stock")

	// ErrLostRace means a CAS found the row in a different state than expected —
	// RowsAffected == 0. Somebody else moved it first. The caller decides whether
	// that is fine (idempotent replay) or a conflict.
	ErrLostRace = errors.New("lost race: row was not in the expected state")

	// ErrIllegalTransition means the requested edge is not in the state machine.
	// This is a programming error, not a race.
	ErrIllegalTransition = errors.New("illegal state transition")

	ErrNotFound       = errors.New("not found")
	ErrAlreadyExists  = errors.New("already exists")
	ErrForbidden      = errors.New("forbidden")
	ErrUnauthorized   = errors.New("unauthorized")
	ErrInvalidInput   = errors.New("invalid input")
	ErrOrderNotOpen   = errors.New("order is no longer open")
	ErrPaymentPending = errors.New("a payment intent is already live for this order")
)
