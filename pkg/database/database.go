// Package database opens the GORM connection and owns pool sizing.
package database

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// Postgres error codes we branch on rather than string-match.
const (
	// CodeDeadlockDetected: two transactions locked the same rows in opposite
	// order (failure mode G). Prevented by sorting line items on product_id;
	// this covers the residue and is safe to retry.
	CodeDeadlockDetected = "40P01"
	// CodeSerializationFailure: a concurrent update invalidated this snapshot.
	// Also retryable.
	CodeSerializationFailure = "40001"
	// CodeUniqueViolation: our partial unique indexes reporting an invariant
	// held — a second live payment intent, a duplicate notification. Not an
	// error to retry; a race that we lost, with its own branch.
	CodeUniqueViolation = "23505"
	// CodeCheckViolation: CHECK (available >= 0) rejected an oversell.
	CodeCheckViolation = "23514"
)

type Options struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	LogLevel        gormlogger.LogLevel
	SlowThreshold   time.Duration
}

// Open connects and configures the pool, then verifies the connection actually
// works — sql.Open alone is lazy and would defer the failure to the first query.
func Open(ctx context.Context, opts Options, log *slog.Logger) (*gorm.DB, error) {
	gcfg := &gorm.Config{
		Logger: gormlogger.New(
			slogWriter{log},
			gormlogger.Config{
				SlowThreshold:             cmpOr(opts.SlowThreshold, 200*time.Millisecond),
				LogLevel:                  cmpOr(opts.LogLevel, gormlogger.Warn),
				IgnoreRecordNotFoundError: true,
				Colorful:                  false,
			},
		),
		// The handlers translate domain outcomes to HTTP status codes themselves;
		// GORM's own transaction wrapper around every single write would add a
		// round trip per statement for no benefit.
		SkipDefaultTransaction: true,
		NowFunc:                func() time.Time { return time.Now().UTC() },
	}

	db, err := gorm.Open(postgres.Open(opts.DSN), gcfg)
	if err != nil {
		return nil, fmt.Errorf("opening postgres: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("unwrapping sql.DB: %w", err)
	}

	// Pool sizing is the difference between surviving 1000 concurrent orders and
	// failure mode J. A 4-core Postgres saturates around 8-16 connections; past
	// that, added connections contend on the same cores and throughput drops.
	sqlDB.SetMaxOpenConns(opts.MaxOpenConns)
	sqlDB.SetMaxIdleConns(opts.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(opts.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(opts.ConnMaxIdleTime)

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}

	log.Info("database connected",
		"max_open_conns", opts.MaxOpenConns,
		"max_idle_conns", opts.MaxIdleConns)

	return db, nil
}

func Close(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// PGErrorCode extracts the SQLSTATE from an error, unwrapping as needed.
// Returns "" when err did not come from Postgres.
func PGErrorCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// IsRetryable reports whether err is a transient serialization conflict that the
// same transaction can simply be replayed against.
//
// Note what is NOT here: unique violations and check violations. Those mean an
// invariant held and we lost a race — replaying produces the same result.
func IsRetryable(err error) bool {
	switch PGErrorCode(err) {
	case CodeDeadlockDetected, CodeSerializationFailure:
		return true
	default:
		return false
	}
}

func IsUniqueViolation(err error) bool { return PGErrorCode(err) == CodeUniqueViolation }
func IsCheckViolation(err error) bool  { return PGErrorCode(err) == CodeCheckViolation }

// slogWriter adapts GORM's Printf-style logger onto slog.
type slogWriter struct{ log *slog.Logger }

func (w slogWriter) Printf(format string, args ...any) {
	w.log.Info("gorm", "msg", fmt.Sprintf(format, args...))
}

func cmpOr[T comparable](v, fallback T) T {
	var zero T
	if v == zero {
		return fallback
	}
	return v
}
