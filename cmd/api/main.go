// Command api serves the HTTP API.
//
// One of three entry points sharing one binary image: cmd/api, cmd/worker and
// cmd/relay. Splitting them means they scale independently — API replicas track
// request load, workers track queue depth — while a single image keeps
// deployment to one artifact.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/Sawalhy/backend-golang-task-2025/internal/api/routes"
	"github.com/Sawalhy/backend-golang-task-2025/internal/config"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
	"github.com/Sawalhy/backend-golang-task-2025/internal/workers"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/database"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

//	@title			Concurrent Order Processing API
//	@version		1.0
//	@description	Order processing with reserve-pay-commit inventory, an outbox
//	@description	relay, and compare-and-swap state transitions throughout.
//	@description
//	@description	POST /orders returns 202, not 201: when it returns, stock is
//	@description	reserved and the order exists, but payment has not happened yet.
//	@description	Poll GET /orders/{id}/status for the outcome.
//
//	@host		localhost:8080
//	@BasePath	/api/v1
//
//	@securityDefinitions.apikey	BearerAuth
//	@in							header
//	@name						Authorization
//	@description				Paste the token from POST /auth/login as: Bearer <token>
func main() {
	// The runtime image is distroless: no shell, no curl, no wget. A container
	// healthcheck therefore has to be the binary itself, probing over HTTP and
	// reporting through its exit code.
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		if err := selfCheck(); err != nil {
			fmt.Fprintf(os.Stderr, "unhealthy: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

// selfCheck probes this container's own readiness endpoint.
func selfCheck() error {
	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8080"
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/readyz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("readyz returned %d", resp.StatusCode)
	}
	return nil
}

// run exists so every failure path returns an error instead of calling
// os.Exit deep in the call stack, where deferred cleanup would be skipped.
func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log := logger.New(cfg.LogLevel, cfg.IsProduction())

	// signal.NotifyContext cancels ctx on SIGINT/SIGTERM. That cancellation is
	// threaded into every request and every database call, so a shutdown unwinds
	// in-flight work instead of severing it.
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

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer func() { _ = rdb.Close() }()

	// Redis being down must not stop the API from booting: it backs rate
	// limiting only, and the limiter fails open by design.
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Warn("redis unavailable at startup, rate limiting will fail open", "error", err)
	}

	store := repository.New(db, cfg.Order.DeadlockRetries)

	// The hub holds this instance's SSE connections. It is per-instance by
	// design: each API replica gets its own exclusive queue bound to order.#, so
	// all of them see every event and each pushes to whichever clients it
	// happens to hold. No sticky sessions, no shared state between replicas.
	hub := services.NewStatusHub()
	instanceID := uuid.NewString()


	engine := routes.Build(routes.Deps{
		Cfg:     cfg,
		Log:     log,
		Auth:    services.NewAuthService(store, cfg.Auth),
		Orders:  services.NewOrderService(store, cfg.Order, log),
		Catalog: services.NewCatalogService(store),
		Reports: services.NewReportService(store, log),
		Hub:     hub,
		Redis:   rdb,
		Health: func() error {
			pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			sqlDB, err := db.DB()
			if err != nil {
				return err
			}
			return sqlDB.PingContext(pingCtx)
		},
	})

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler:      engine,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
		// Without a header timeout a slowloris client can hold connections open
		// indefinitely and exhaust the accept queue.
		ReadHeaderTimeout: 5 * time.Second,
	}

	// The server runs on its own goroutine so main can block on ctx.Done and
	// drive the shutdown. errCh gives the goroutine a way to report a failure
	// rather than dying silently — a goroutine with no exit path is a leak, and
	// one with no error path is worse.
	errCh := make(chan error, 1)
	go func() {
		log.Info("api listening", "port", cfg.HTTP.Port, "env", cfg.Env)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// Feed the hub from the broker, redialling whenever the connection drops.
	//
	// RabbitMQ being unreachable must not stop the API booting — it backs SSE
	// only, and every other route is unaffected — so Supervise retries in the
	// background instead of failing startup. It also exits when ctx is
	// cancelled, giving the goroutine a guaranteed exit path.
	go func() {
		_ = workers.Supervise(ctx, cfg.Rabbit.URL, cfg.Rabbit.Exchange, log,
			func(sessionCtx context.Context, broker *workers.Broker) error {
				return workers.RunOrderEventBackplane(sessionCtx, broker, instanceID, hub.Publish, log)
			})
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		log.Info("shutdown signal received, draining connections")
	}

	// Graceful shutdown: stop accepting, let in-flight requests finish. A fresh
	// context is required here — ctx is already cancelled, and passing it would
	// abort the very requests we are trying to drain.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown failed: %w", err)
	}

	log.Info("api stopped cleanly")
	return nil
}
