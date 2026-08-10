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
	//
	// All four live inside ONE supervised session. Channels belong to a
	// Connection, so when the connection drops every consumer must be rebuilt —
	// and a consumer parked on a delivery channel that will never deliver again
	// is indistinguishable from an idle one. Supervise ends the session and
	// redials; unacked messages are redelivered by the broker.
	g.Go(func() error {
		return workers.Supervise(gctx, cfg.Rabbit.URL, cfg.Rabbit.Exchange, "worker", log,
			func(sessionCtx context.Context, broker *workers.Broker) error {
				cg, cctx := errgroup.WithContext(sessionCtx)

				cg.Go(func() error {
					return workers.RunConsumer(cctx, broker, workers.QueuePayments,
						cfg.Rabbit.Prefetch, cfg.Worker.Concurrency,
						workers.PaymentHandler(payments.ProcessOrder), log)
				})
				cg.Go(func() error {
					return workers.RunConsumer(cctx, broker, workers.QueueEmail,
						cfg.Rabbit.Prefetch, cfg.Worker.Concurrency, email.Handle, log)
				})
				cg.Go(func() error {
					return workers.RunConsumer(cctx, broker, workers.QueueSMS,
						cfg.Rabbit.Prefetch, cfg.Worker.Concurrency, sms.Handle, log)
				})
				cg.Go(func() error {
					return workers.RunConsumer(cctx, broker, workers.QueueRefunds,
						cfg.Rabbit.Prefetch, cfg.Worker.Concurrency, payments.HandleRefund, log)
				})

				return cg.Wait()
			})
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
	// ReleaseExpiredLeases is channel-agnostic, so one service sweeping covers
	// both email and SMS rows — running it per-service would only duplicate work.
	g.Go(func() error {
		return everyTick(gctx, cfg.Worker.NotifyLeaseTTL, func(ctx context.Context) {
			if err := email.SweepExpiredLeases(ctx); err != nil {
				log.Error("sweeping notification leases", "error", err)
			}
		})
	})

	// Re-drives notifications whose send failed. This one IS per-channel: a
	// service only knows how to send down its own transport.
	//
	// Without it a failed send is terminal. MarkFailed returns the row to
	// UNCLAIMED, but the broker delivery that produced it was acked the moment
	// the handler returned, so no redelivery is coming and nothing else scans for
	// the row — the customer is simply never told. The absence of an email is not
	// an event, so only a timer can notice it.
	g.Go(func() error {
		return everyTick(gctx, cfg.Worker.NotifyRetryInterval, func(ctx context.Context) {
			for _, n := range []*services.NotificationService{email, sms} {
				if _, err := n.RetryFailed(ctx, 100); err != nil {
					log.Error("retrying failed notifications", "error", err)
				}
			}
		})
	})

	// Recovers charges abandoned by a worker that died mid-payment (the other
	// half of failure mode E). Nothing else can see these: redelivery skips
	// non-PENDING orders, the reaper only expires PENDING ones, and
	// reconciliation only looks at UNKNOWN. A timer is the only thing that can
	// notice a process stopped existing.
	//
	// The grace period must exceed the provider timeout, or this "recovers"
	// payments that are merely slow while their first call is still in flight.
	g.Go(func() error {
		grace := 4 * cfg.Payments.Timeout
		if grace < time.Minute {
			grace = time.Minute
		}
		return everyTick(gctx, time.Minute, func(ctx context.Context) {
			if _, err := payments.RecoverStuckCharges(ctx, grace, 50); err != nil {
				log.Error("recovering stuck charges", "error", err)
			}
			if _, err := payments.SweepUnknownPayments(ctx, 50); err != nil {
				log.Error("sweeping unknown payments", "error", err)
			}
		})
	})

	// Materialises closed days into daily_sales_rollup.
	//
	// This is the other kind of time-triggered work: nothing happens at midnight
	// to announce that a day has ended, so only a clock can notice. It resumes
	// from the last materialised day, so a worker down for a week catches up on
	// its first tick, and the upsert makes re-running a day harmless.
	//
	// Hourly rather than daily so a restart never has to wait until tomorrow to
	// close yesterday — the work is idempotent, so the extra runs cost nothing.
	reports := services.NewReportService(store, log)
	g.Go(func() error {
		// Run once at startup rather than waiting a full interval, so a fresh
		// deployment has a populated rollup immediately.
		if _, err := reports.RollupClosedDays(gctx, 30); err != nil {
			log.Error("initial sales rollup failed", "error", err)
		}
		return everyTick(gctx, time.Hour, func(ctx context.Context) {
			if _, err := reports.RollupClosedDays(ctx, 30); err != nil {
				log.Error("sales rollup failed", "error", err)
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
