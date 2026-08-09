package database

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

// Error classification decides whether a failure is retried or branched on, so
// getting it wrong is how a lost race turns into an infinite retry loop — or how
// a genuine deadlock turns into a 500.

func pgErr(code string) error { return &pgconn.PgError{Code: code} }

func TestPGErrorCodeUnwraps(t *testing.T) {
	assert.Equal(t, CodeDeadlockDetected, PGErrorCode(pgErr(CodeDeadlockDetected)))

	// Errors are wrapped with context all through this codebase, so the code has
	// to survive several layers of %w.
	wrapped := fmt.Errorf("charging order 1: %w",
		fmt.Errorf("reserving stock: %w", pgErr(CodeUniqueViolation)))
	assert.Equal(t, CodeUniqueViolation, PGErrorCode(wrapped))

	assert.Empty(t, PGErrorCode(errors.New("not from postgres")))
	assert.Empty(t, PGErrorCode(nil))
}

// The distinction that matters: a serialization conflict can be replayed, but a
// constraint violation means an invariant HELD and we lost a race. Replaying
// that produces the identical result, so retrying it forever is a hang.
func TestOnlySerializationConflictsAreRetryable(t *testing.T) {
	retryable := []string{CodeDeadlockDetected, CodeSerializationFailure}
	for _, code := range retryable {
		assert.True(t, IsRetryable(pgErr(code)), "%s should be retryable", code)
	}

	notRetryable := []string{CodeUniqueViolation, CodeCheckViolation, "23503", "42P01"}
	for _, code := range notRetryable {
		assert.False(t, IsRetryable(pgErr(code)),
			"%s means an invariant held; replaying it changes nothing", code)
	}

	assert.False(t, IsRetryable(errors.New("some other failure")))
	assert.False(t, IsRetryable(nil))
}

func TestConstraintClassifiers(t *testing.T) {
	assert.True(t, IsUniqueViolation(pgErr(CodeUniqueViolation)))
	assert.False(t, IsUniqueViolation(pgErr(CodeCheckViolation)))

	// CHECK (available >= 0) rejecting an oversell.
	assert.True(t, IsCheckViolation(pgErr(CodeCheckViolation)))
	assert.False(t, IsCheckViolation(pgErr(CodeUniqueViolation)))

	assert.False(t, IsUniqueViolation(errors.New("nope")))
	assert.False(t, IsCheckViolation(nil))
}

// Wrapped constraint errors must still classify, since the repository layer
// wraps everything before returning it.
func TestClassifiersSeeThroughWrapping(t *testing.T) {
	wrapped := fmt.Errorf("opening intent for order 7: %w", pgErr(CodeUniqueViolation))
	assert.True(t, IsUniqueViolation(wrapped))

	deep := fmt.Errorf("a: %w", fmt.Errorf("b: %w", pgErr(CodeDeadlockDetected)))
	assert.True(t, IsRetryable(deep))
}
