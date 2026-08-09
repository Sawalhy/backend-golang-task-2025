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

	store := repository.New(db, cfg.Order.DeadlockRetries)

	g, gctx := errgroup.WithContext(ctx)

	// Supervise redials when the broker connection drops. A Connection and its
	// Channels cannot be acquired once and reused forever: when RabbitMQ
	// restarts, every later publish fails with "channel/connection is not open"
	// and the relay stops delivering while still looking perfectly healthy.
	//
	// Everything the session owns is rebuilt inside this closure, because a
	// Channel belongs to a Connection and cannot outlive it. Nothing is lost by
	// the restart: unpublished rows still have sent_at IS NULL, so the next
	// session claims exactly the same batch.
	g.Go(func() error {
		return workers.Supervise(gctx, cfg.Rabbit.URL, cfg.Rabbit.Exchange, "relay", log,
			func(sessionCtx context.Context, broker *workers.Broker) error {
				pub, err := broker.NewPublisher()
				if err != nil {
					return err
				}
				defer func() { _ = pub.Close() }()

				relay := workers.NewRelay(store, pub, cfg.Worker.RelayInterval, cfg.Worker.RelayBatchSize, log)
				return relay.Run(sessionCtx)
			})
	})

	// Lag reporting runs against the DATABASE, on the parent context, so it keeps
	// reporting while the broker is down and the relay is between sessions —
	// which is exactly when a growing backlog matters most.
	g.Go(func() error {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-ticker.C:
				workers.ReportOutboxLag(gctx, store, log)
			}
		}
	})

	if err := g.Wait(); err != nil {
		return fmt.Errorf("relay: %w", err)
	}

	log.Info("relay stopped cleanly")
	return nil
}
