// Command worker runs every background job: the queue consumers and the
// periodic loops.
//
// The two kinds of work here are genuinely different, and mixing them up is a
// common design error:
//
//	CONSUMERS react to events. A message arrives, something happened, do the work.
//	LOOPS     react to the passage of time. Nothing happened, and that is the point.
//
// The reservation reaper is a loop, not a consumer, because its trigger is
// "Sarah still hasn't paid" — no message will ever be published to announce that
// nothing occurred. The absence of an event is not an event, so only a timer can
// notice it.
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
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
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

	db, err := database.Open(ctx, database.Options{
		DSN:             cfg.DB.DSN,
		MaxOpenConns:    cfg.DB.MaxOpenConns,
		MaxIdleConns:    cfg.DB.MaxIdleConns,
		ConnMaxLifetime: cfg.DB.ConnMaxLifetime,
		ConnMaxIdleTime: cfg.DB.ConnMaxIdleTime,
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

	if err := broker.DeclareTopology(); err != nil {
		return err
	}

	store := repository.New(db, cfg.Order.DeadlockRetries)

	provider := services.NewSimulatedProvider(
		cfg.Payments.SimulatedFailureRate,
		cfg.Payments.SimulatedTimeoutRate,
		cfg.Payments.SimulatedLatency,
	)
	payments := services.NewPaymentService(store, provider, log)
	reaper := services.NewReaperService(store, cfg.Worker.RelayBatchSize, log)

	email := services.NewNotificationService(store,
		services.NewLoggingNotifier("email", 0.05, 50*time.Millisecond, log),
		cfg.Worker.NotifyLeaseTTL, log)
	sms := services.NewNotificationService(store,
		services.NewLoggingNotifier("sms", 0.05, 30*time.Millisecond, log),
		cfg.Worker.NotifyLeaseTTL, log)

	g, gctx := errgroup.WithContext(ctx)

	// --- consumers ----------------------------------------------------------
	// Each queue gets its own consumer with its own bounded pool. Email and SMS
	// are separate queues, not two consumers on one: sharing a queue would make
	// them competing consumers, so an order.paid would reach one of them and the
	// customer would get an email or a text, never both.

	g.Go(func() error {
		return workers.RunConsumer(gctx, broker, workers.QueuePayments,
			cfg.Rabbit.Prefetch, cfg.Worker.Concurrency,
			workers.PaymentHandler(payments.ProcessOrder), log)
	})

	g.Go(func() error {
		return workers.RunConsumer(gctx, broker, workers.QueueEmail,
			cfg.Rabbit.Prefetch, cfg.Worker.Concurrency, email.Handle, log)
	})

	g.Go(func() error {
		return workers.RunConsumer(gctx, broker, workers.QueueSMS,
			cfg.Rabbit.Prefetch, cfg.Worker.Concurrency, sms.Handle, log)
	})

	g.Go(func() error {
		return workers.RunConsumer(gctx, broker, workers.QueueRefunds,
			cfg.Rabbit.Prefetch, cfg.Worker.Concurrency, payments.HandleRefund, log)
	})

	// --- periodic loops -----------------------------------------------------

	// The reaper reclaims stock from abandoned checkouts (failure mode F).
	g.Go(func() error {
		return everyTick(gctx, cfg.Worker.ReaperInterval, func(ctx context.Context) {
			// Drain rather than one batch per tick, so a backlog clears at
			// database speed instead of batchSize per interval.
			for {
				n, err := reaper.ReapOnce(ctx)
				if err != nil {
					log.Error("reaper failed", "error", err)
					return
				}
				if n == 0 {
					return
				}
			}
		})
	})

	// Reclaims notification rows abandoned by a worker that died mid-send.
	g.Go(func() error {
		return everyTick(gctx, cfg.Worker.NotifyLeaseTTL, func(ctx context.Context) {
			if err := email.SweepExpiredLeases(ctx); err != nil {
				log.Error("sweeping notification leases", "error", err)
			}
		})
	})

	log.Info("worker started",
		"concurrency", cfg.Worker.Concurrency,
		"prefetch", cfg.Rabbit.Prefetch,
		"reaper_interval", cfg.Worker.ReaperInterval)

	if err := g.Wait(); err != nil {
		return fmt.Errorf("worker: %w", err)
	}

	log.Info("worker stopped cleanly")
	return nil
}

// everyTick runs fn on an interval until ctx is cancelled.
//
// The select on ctx.Done is what gives this goroutine a guaranteed exit path. A
// bare `for { time.Sleep(d); fn() }` cannot be stopped, so it outlives shutdown
// and is never collected — the plainest form of a goroutine leak.
func everyTick(ctx context.Context, interval time.Duration, fn func(context.Context)) error {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			fn(ctx)
		}
	}
}
