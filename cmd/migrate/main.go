// Command migrate applies or rolls back database migrations.
//
// golang-migrate, not GORM's AutoMigrate. AutoMigrate cannot drop a column or
// change a type, so a schema that evolves silently diverges from what the structs
// say — and it offers no way to express the CHECK constraints and partial unique
// indexes that carry this system's invariants. Those live in versioned SQL where
// they can be reviewed.
//
//	go run ./cmd/migrate up
//	go run ./cmd/migrate down 1
//	go run ./cmd/migrate version
//	go run ./cmd/migrate force 1     # clear a dirty state after a failed migration
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/Sawalhy/backend-golang-task-2025/internal/config"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := logger.New(cfg.LogLevel, false)

	dir := os.Getenv("MIGRATIONS_DIR")
	if dir == "" {
		dir = "migrations"
	}

	m, err := migrate.New("file://"+dir, cfg.DB.DSN)
	if err != nil {
		return fmt.Errorf("opening migrations from %s: %w", dir, err)
	}
	defer func() {
		// Both returned errors are closing errors; neither should mask a
		// migration failure, so they are logged rather than returned.
		if srcErr, dbErr := m.Close(); srcErr != nil || dbErr != nil {
			log.Warn("closing migrator", "source_err", srcErr, "db_err", dbErr)
		}
	}()

	cmd := "up"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	switch cmd {
	case "up":
		err = m.Up()
	case "down":
		// Bare `down` would roll back EVERY migration and drop the database
		// contents. Require an explicit step count.
		if len(os.Args) < 3 {
			return errors.New("down requires a step count, e.g. `down 1`")
		}
		n, convErr := strconv.Atoi(os.Args[2])
		if convErr != nil || n < 1 {
			return fmt.Errorf("down step count must be a positive integer, got %q", os.Args[2])
		}
		err = m.Steps(-n)
	case "version":
		v, dirty, verErr := m.Version()
		if verErr != nil {
			return verErr
		}
		fmt.Printf("version=%d dirty=%t\n", v, dirty)
		return nil
	case "force":
		if len(os.Args) < 3 {
			return errors.New("force requires a version")
		}
		v, convErr := strconv.Atoi(os.Args[2])
		if convErr != nil {
			return fmt.Errorf("force version must be an integer, got %q", os.Args[2])
		}
		err = m.Force(v)
	default:
		return fmt.Errorf("unknown command %q (want: up, down N, version, force N)", cmd)
	}

	// ErrNoChange means the schema is already where it should be. That is a
	// successful outcome for an idempotent `migrate up` at container start, not
	// a failure worth exiting non-zero over.
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("running %s: %w", cmd, err)
	}

	log.Info("migrations applied", "command", cmd)
	return nil
}
