// Command relay publishes outbox rows to RabbitMQ.
//
// It runs as its own process rather than as a goroutine inside the API for one
// reason: the API scales with request volume and the relay does not. Two relay
// instances are plenty regardless of how many API replicas exist, and SKIP
// LOCKED means they never double-publish.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Sawalhy/backend-golang-task-2025/internal/config"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/workers"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/database"
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
	log := logger.New(cfg.LogLevel, cfg.IsProduction())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The relay's pool is small on purpose: it runs one claiming transaction at
	// a time, and every connection it holds is one the API cannot use.
	db, err := database.Open(ctx, database.Options{
		DSN:             cfg.DB.DSN,
		MaxOpenConns:    4,
		MaxIdleConns:    2,
		ConnMaxLifetime: cfg.DB.ConnMaxLifetime,
	}, log)
	if err != nil {
		return err
	}
	defer func() { _ = database.Close(db) }()

	broker, err := workers.Connect(cfg.Rabbit.URL, cfg.Rabbit.Exchange, log)
	if err != nil {
		return err
	}
	defer func() { _ = broker.Close() }()

	// Idempotent, so every process declaring on boot removes any startup
	// ordering requirement between api, worker and relay.
	if err := broker.DeclareTopology(); err != nil {
		return err
	}

	pub, err := broker.NewPublisher()
	if err != nil {
		return err
	}
	defer func() { _ = pub.Close() }()

	store := repository.New(db, cfg.Order.DeadlockRetries)
	relay := workers.NewRelay(store, pub, cfg.Worker.RelayInterval, cfg.Worker.RelayBatchSize, log)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error { return relay.Run(gctx) })

	// Lag reporting on its own schedule. Depth alone is ambiguous — 500 rows
	// moving fast is healthy, three stuck for ten minutes is an outage — so the
	// age of the oldest unsent row is reported alongside it.
	g.Go(func() error {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-ticker.C:
				relay.ReportLag(gctx)
			}
		}
	})

	if err := g.Wait(); err != nil {
		return fmt.Errorf("relay: %w", err)
	}

	log.Info("relay stopped cleanly")
	return nil
}
