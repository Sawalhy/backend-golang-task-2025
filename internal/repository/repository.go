// Package repository owns SQL and nothing else. Business rules live in
// internal/services; this layer executes statements and reports what the
// database did.
//
// Two conventions run through every file here:
//
//   - context.Context is the first parameter of anything doing I/O, and it is
//     threaded, never re-created with context.Background() mid-stack.
//   - Every state change is a compare-and-swap: the expected state goes in the
//     WHERE clause and the caller checks RowsAffected. RowsAffected == 0 means
//     you lost a race, which is an outcome with its own branch — not an error.
package repository

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/pkg/database"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/metrics"
)

// Store is the handle every repository hangs off. It carries the pool and the
// retry budget; it holds no per-request state, so it is safe for concurrent use.
type Store struct {
	db         *gorm.DB
	maxRetries int
}

func New(db *gorm.DB, maxRetries int) *Store {
	if maxRetries < 0 {
		maxRetries = 0
	}
	return &Store{db: db, maxRetries: maxRetries}
}

// DB exposes the pool for reads that need no transaction.
func (s *Store) DB() *gorm.DB { return s.db }

// TxFunc runs inside a transaction. It must not perform network I/O — not the
// payment provider, not the broker, not email. A row lock held across a 2.4s
// HTTP call costs roughly 500x throughput on a contended product and burns a
// pool connection for the duration.
type TxFunc func(ctx context.Context, tx *gorm.DB) error

// InTx runs fn in a transaction, retrying on serialization conflicts with
// bounded, jittered backoff.
//
// Deadlocks (40P01) are already prevented by sorting line items on product_id
// before touching inventory — a total order on resources makes a wait cycle
// impossible. This retry covers the residue: paths that touch rows the ordering
// rule does not cover, and serialization failures from concurrent updates.
//
// The jitter matters. Two transactions that deadlock and both retry after
// exactly 50ms will deadlock again, in lockstep, until the budget runs out.
func (s *Store) InTx(ctx context.Context, fn TxFunc) error {
	var lastErr error

	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		if attempt > 0 {
			// 25ms, 50ms, 100ms ... each with up to 100% jitter on top.
			base := time.Duration(25<<(attempt-1)) * time.Millisecond
			delay := base + time.Duration(rand.Int63n(int64(base)))

			select {
			case <-ctx.Done():
				return fmt.Errorf("retrying transaction: %w", ctx.Err())
			case <-time.After(delay):
			}
		}

		err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return fn(ctx, tx)
		})
		if err == nil {
			return nil
		}
		if !database.IsRetryable(err) {
			return err
		}
		// Counted, not just retried. Deadlocks are supposed to be impossible here
		// — line items are sorted on product_id before inventory is touched — so a
		// rising rate is the signal that some write path bypassed the sort
		// (failure mode G), which is invisible from the outside otherwise.
		metrics.DeadlockRetries.Inc()
		lastErr = err
	}

	return fmt.Errorf("transaction still conflicting after %d retries: %w", s.maxRetries, lastErr)
}

// txOrDB lets a repository method participate in a caller's transaction when
// given one, or run standalone when given nil. Without this every method would
// need a WithTx twin.
func (s *Store) txOrDB(tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return tx
	}
	return s.db
}

// notFound maps GORM's sentinel onto ours so no layer above this one has to
// import GORM to ask "did that exist?".
func notFound(err error) bool { return errors.Is(err, gorm.ErrRecordNotFound) }
