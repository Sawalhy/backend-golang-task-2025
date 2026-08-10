// Package tests holds integration tests that run against a REAL Postgres.
//
// Nothing here mocks the database, and that is the whole point. A mock returns
// what you told it to return, so it is structurally incapable of exhibiting a
// race: it cannot lose an update, cannot deadlock, and cannot enforce a CHECK
// constraint. Every interesting bug in this system is a database race, so a
// suite built on mocks would pass while the system overselling stock.
//
// Two ways to get that Postgres:
//
//	TEST_DATABASE_URL set  -> use it (the compose stack, CI service container)
//	otherwise              -> testcontainers-go starts one and throws it away
//
// The first path is what makes the suite runnable here, where the Go toolchain
// itself is containerised; the second is what makes `go test ./...` work on a
// machine with Go installed and nothing else running.
package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/database"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

var (
	testDSN       string
	testContainer testcontainers.Container
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	dsn, cleanup, err := provisionDatabase(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot provision test database: %v\n", err)
		// Skip rather than fail: a developer with neither Docker nor a database
		// should not see a wall of red for an environment problem.
		fmt.Fprintln(os.Stderr, "set TEST_DATABASE_URL or start Docker to run integration tests")
		os.Exit(0)
	}
	testDSN = dsn

	if err := applyMigrations(dsn); err != nil {
		fmt.Fprintf(os.Stderr, "applying migrations: %v\n", err)
		cleanup()
		os.Exit(1)
	}

	code := m.Run()
	closeBenchPool() // the benchmarks share one pool for the whole process
	cleanup()
	os.Exit(code)
}

func provisionDatabase(ctx context.Context) (string, func(), error) {
	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		return dsn, func() {}, nil
	}

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("orders_test"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		return "", nil, err
	}
	testContainer = container

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = container.Terminate(ctx)
		return "", nil, err
	}

	return dsn, func() { _ = container.Terminate(context.Background()) }, nil
}

// applyMigrations runs the real migration files, not AutoMigrate. Tests must
// exercise the same CHECK constraints and partial unique indexes production
// has — those constraints ARE the invariants under test.
func applyMigrations(dsn string) error {
	dir := os.Getenv("MIGRATIONS_DIR")
	if dir == "" {
		dir = "../migrations"
	}

	m, err := migrate.New("file://"+dir, dsn)
	if err != nil {
		return err
	}
	defer func() { _, _ = m.Close() }()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}

// newStore opens a connection and wipes the tables, so each test starts from a
// known state regardless of what ran before it. The pool is closed when the test
// that asked for it finishes.
func newStore(tb testing.TB) (*repository.Store, *gorm.DB) {
	tb.Helper()

	db := openDB(tb, 25) // headroom for the concurrency tests
	tb.Cleanup(func() { _ = database.Close(db) })

	truncateAll(tb, db)
	return repository.New(db, 3), db
}

// openDB opens a pool and deliberately does NOT register a cleanup: the caller
// owns its lifetime.
//
// Split out of newStore for the benchmarks, which share a single pool across the
// whole process (bench_test.go) because a benchmark body is re-run for every N
// the framework tries. Tying the pool to the first caller's Cleanup would close
// it underneath everything that ran after.
//
// It takes testing.TB rather than *testing.T so the benchmarks can call it.
func openDB(tb testing.TB, maxOpen int) *gorm.DB {
	tb.Helper()

	db, err := database.Open(context.Background(), database.Options{
		DSN:          testDSN,
		MaxOpenConns: maxOpen,
		MaxIdleConns: maxOpen,
	}, logger.New("error", false))
	require.NoError(tb, err)

	return db
}

// truncateAll resets state between tests.
//
// TRUNCATE ... RESTART IDENTITY CASCADE rather than DELETE: it is far faster,
// and it resets the sequences so ids are predictable per test. Order does not
// matter because CASCADE follows the foreign keys.
func truncateAll(tb testing.TB, db *gorm.DB) {
	tb.Helper()
	err := db.Exec(`
		TRUNCATE notifications, outbox, payments, reservations, order_items,
		         orders, inventory, products, users, audit_logs,
		         daily_sales_rollup
		RESTART IDENTITY CASCADE`).Error
	require.NoError(tb, err)
}
