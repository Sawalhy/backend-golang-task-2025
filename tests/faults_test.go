package tests

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/database"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

// faultInjector makes the REAL database fail on demand.
//
// GORM implements its own features as a chain of named callbacks per operation
// — gorm:begin_transaction, gorm:create, gorm:query, gorm:commit_or_rollback
// and so on. That chain is an extension point: a plugin registers its own
// callback into it, which is the same position EF Core's DbCommandInterceptor
// occupies, or middleware in an HTTP pipeline.
//
// Why this rather than mocking a repository:
//
//   - A mock replaces the repository, so everything below the seam disappears —
//     the SQL, GORM, the driver, the transaction. You assert against a stub.
//   - A callback leaves all of that in place and makes the real thing fail. The
//     error then travels the production path: the repository wraps it,
//     database.IsRetryable classifies it, InTx decides replay-or-return, the
//     service branches, the handler maps a status code.
//
// And it requires no production change. This plugin is installed on the TEST
// connection only; database.Open never registers it. Adding repository
// interfaces to allow mocking would change every service signature and, worse,
// would make it possible to write the oversell test against a mock — where it
// passes and proves nothing.
//
// The honest limit: the error is synthetic. It exercises OUR handling, not
// Postgres's behaviour. For a failure that is genuinely real — a connection
// dying mid-transaction while holding locks — see the pg_terminate_backend test
// in failure_paths_test.go.
type faultInjector struct {
	mu    sync.Mutex
	rules []*faultRule
}

type faultRule struct {
	match     string // matched against the SQL text and the target table; "" matches everything
	remaining int    // used when nth == 0: fail the next `remaining` matches
	nth        int   // when > 0, fail only the nth match and let the rest through
	seen       int
	err       error
	fired      int
}

func newFaultInjector() *faultInjector { return &faultInjector{} }

func (f *faultInjector) Name() string { return "faultinjector" }

// Initialize hooks every operation chain. Registering BEFORE the callback that
// actually executes means the statement never reaches the database, so an
// injected failure inside a transaction leaves exactly the state a rollback
// would.
// The type returned by db.Callback().Create() and friends is unexported, so the
// registrations are closures rather than a table of processors.
func (f *faultInjector) Initialize(db *gorm.DB) error {
	registrations := []func() error{
		func() error {
			return db.Callback().Create().Before("gorm:create").Register("faults:create", f.maybeFail)
		},
		func() error {
			return db.Callback().Query().Before("gorm:query").Register("faults:query", f.maybeFail)
		},
		func() error {
			return db.Callback().Update().Before("gorm:update").Register("faults:update", f.maybeFail)
		},
		func() error {
			return db.Callback().Delete().Before("gorm:delete").Register("faults:delete", f.maybeFail)
		},
		func() error {
			return db.Callback().Row().Before("gorm:row").Register("faults:row", f.maybeFail)
		},
		func() error {
			return db.Callback().Raw().Before("gorm:raw").Register("faults:raw", f.maybeFail)
		},
	}

	for _, register := range registrations {
		if err := register(); err != nil {
			return err
		}
	}
	return nil
}

func (f *faultInjector) maybeFail(db *gorm.DB) {
	if db.Statement == nil {
		return
	}

	// Raw and Exec have their SQL built before execution; Create and Update
	// build theirs inside the callback we run before, so match on the table too.
	sql := db.Statement.SQL.String()
	table := db.Statement.Table

	f.mu.Lock()
	defer f.mu.Unlock()

	for _, r := range f.rules {
		if !strings.Contains(sql, r.match) && !strings.EqualFold(table, r.match) {
			continue
		}

		r.seen++

		if r.nth > 0 {
			// Positional: let every other call through, fail only this one.
			if r.seen != r.nth {
				continue
			}
		} else {
			if r.remaining == 0 {
				continue
			}
			r.remaining--
		}

		r.fired++
		_ = db.AddError(r.err)
		return
	}
}

// FailNext arms the injector to fail the next `times` operations whose SQL (or
// target table) contains `match`.
//
// `match` is a SUBSTRING, which is the sharp edge of this tool: a fault armed on
// "outbox" also matches `SELECT count(*) FROM outbox`, so a still-armed rule
// will fail the test's own verification queries rather than the code under test.
// Call disarm() before asserting, or match on something only the write produces.
func (f *faultInjector) FailNext(match string, times int, err error) *faultRule {
	f.mu.Lock()
	defer f.mu.Unlock()

	rule := &faultRule{match: match, remaining: times, err: err}
	f.rules = append(f.rules, rule)
	return rule
}

// FailNthCall arms a fault on the Nth database operation after arming, letting
// every other call through. `match` narrows which calls are counted; "" counts
// all of them.
//
// This is what makes a SWEEP possible: run the same operation once per position
// and fail a different step each time, so every error branch on the path is
// walked rather than the one or two a test author happened to pick.
func (f *faultInjector) FailNthCall(match string, n int, err error) *faultRule {
	f.mu.Lock()
	defer f.mu.Unlock()

	rule := &faultRule{match: match, nth: n, err: err}
	f.rules = append(f.rules, rule)
	return rule
}

// FailNextRetryable arms a fault that database.IsRetryable will classify as
// worth replaying, so InTx's retry path is exercised with a realistic error
// rather than a generic one.
func (f *faultInjector) FailNextRetryable(match string, times int) *faultRule {
	return f.FailNext(match, times, &pgconn.PgError{
		Code:    database.CodeDeadlockDetected,
		Message: "deadlock detected",
	})
}

// FailNextTerminal arms a fault that must NOT be retried: a unique violation
// means an invariant held and replaying produces the same result.
func (f *faultInjector) FailNextTerminal(match string, times int) *faultRule {
	return f.FailNext(match, times, &pgconn.PgError{
		Code:    database.CodeUniqueViolation,
		Message: "duplicate key value violates unique constraint",
	})
}

func (f *faultInjector) disarm() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = nil
}

func (r *faultRule) timesFired() int { return r.fired }

// newStoreWithFaults is newStore plus the injector, on its own connection.
func newStoreWithFaults(t *testing.T) (*repository.Store, *gorm.DB, *faultInjector) {
	t.Helper()

	store, db := newStore(t)
	faults := newFaultInjector()
	require.NoError(t, db.Use(faults))
	t.Cleanup(faults.disarm)

	return store, db, faults
}

// openTestDB opens a connection with a fixed pool size and does NOT truncate.
func openTestDB(t *testing.T, maxConns int) *gorm.DB {
	t.Helper()

	db, err := database.Open(context.Background(), database.Options{
		DSN:          testDSN,
		MaxOpenConns: maxConns,
		MaxIdleConns: maxConns,
	}, logger.New("error", false))
	require.NoError(t, err)
	t.Cleanup(func() { _ = database.Close(db) })

	return db
}

// newIsolatedStore is the primary store for a test, pinned to ONE backend so the
// test can identify and terminate the exact server process its transaction runs
// on. It truncates, like newStore.
func newIsolatedStore(t *testing.T) (*repository.Store, *gorm.DB) {
	t.Helper()

	db := openTestDB(t, 1) // one backend, so pg_backend_pid() is unambiguous
	truncateAll(t, db)
	return repository.New(db, 3), db
}

// newAuxStore is a SECOND connection to the same database that deliberately does
// not truncate — for a test that needs to act from outside its own transaction,
// or to read state after its own connection has been destroyed.
func newAuxStore(t *testing.T) *repository.Store {
	t.Helper()
	return repository.New(openTestDB(t, 2), 3)
}
