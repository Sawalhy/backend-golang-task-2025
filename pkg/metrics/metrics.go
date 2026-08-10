// Package metrics holds the Prometheus instruments and the /metrics handler.
//
// The metric set is chosen from DESIGN_NOTES §5.19, and the selection rule is
// the whole point:
//
//	Every failure mode in §4 leaves all eight containers healthy and every
//	endpoint returning 200.
//
// A stranded outbox row, a worker dying mid-charge, a payment whose outcome is
// unknown — none of them fail a health check. These are the numbers that make
// those failures visible, one per mode, kept deliberately short. An alert nobody
// acts on trains everyone to ignore alerts.
//
// Instruments are package-level vars on the default registry, which also brings
// the Go runtime and process collectors along for free. Threading a registry
// through every constructor would buy testability this codebase does not need —
// the values are asserted by scraping /metrics, not by injecting a fake.
package metrics

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// --- HTTP (the RED metrics: rate, errors, duration) -------------------------

var (
	// Route, not path: /api/v1/orders/:id keeps the label cardinality bounded.
	// Using the raw path would mint a new time series per order id and take the
	// Prometheus server down with it.
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests by method, route and status class.",
	}, []string{"method", "route", "status"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "http_request_duration_seconds",
		Help: "HTTP request latency. Buckets are set for a 202-returning API.",
		// The default buckets top out at 10s and bunch around 5ms, which wastes
		// resolution here: order intake should land in single-digit ms and
		// anything past 2s is already an incident.
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"method", "route"})
)

// --- one instrument per failure mode (§5.19) --------------------------------

var (
	// Mode B — order committed, event never published. Alert above 60s.
	//
	// AGE, not depth. outbox_pending_rows can sit at 3 forever while one poisoned
	// row never leaves; the age of the oldest unsent row is what distinguishes
	// "500 rows moving fast" from "3 rows stuck since Tuesday".
	OutboxOldestUnsentSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_oldest_unsent_seconds",
		Help: "Age of the oldest unpublished outbox row. The mode B alarm.",
	})

	// Kept alongside the age because the PAIR identifies the fault: both climbing
	// is a dead relay, count flat with age climbing is a poison row.
	OutboxPendingRows = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_pending_rows",
		Help: "Unpublished outbox rows. Read together with the age gauge.",
	})

	// Mode E — a process died holding work. Sustained non-zero means processes
	// are dying, whatever the health checks say.
	JobsReclaimed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_reclaimed_total",
		Help: "Work items reclaimed after a lease expired, by kind.",
	}, []string{"kind"})

	// Mode C — the "we may have charged them" pile. Any sustained value is a
	// human's problem, not a retry's.
	PaymentsUnknown = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "payments_unknown",
		Help: "Payments whose outcome the provider never confirmed.",
	})

	// Mode F — stock reclaimed from abandoned checkouts. A spike means payments
	// are failing, not that customers got bored.
	ReservationsExpired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "reservations_expired_total",
		Help: "Reservations reaped after their hold expired.",
	})

	// Mode G — deadlocks are prevented by sorting line items on product_id, so
	// anything above baseline means a write path bypassed the sort.
	DeadlockRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "deadlock_retries_total",
		Help: "Transactions retried after a serialization failure or deadlock.",
	})
)

// RegisterDBPool exposes pool saturation, failure mode J.
//
// GaugeFunc rather than a sampling goroutine: the value is read at scrape time,
// so there is no timer to leak and no staleness window. Alert above 80%
// sustained — the pool is the first thing to run out under a 1000-order burst,
// and it runs out silently.
//
// sync.Once because the default registry panics on a duplicate registration and
// two processes in one test binary would otherwise take the suite down.
func RegisterDBPool(db *sql.DB) {
	dbPoolOnce.Do(func() {
		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "db_pool_in_use",
			Help: "Connections currently checked out of the pool.",
		}, func() float64 { return float64(db.Stats().InUse) })

		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "db_pool_max",
			Help: "Configured pool ceiling. in_use/max is the mode J alarm.",
		}, func() float64 { return float64(db.Stats().MaxOpenConnections) })
	})
}

var dbPoolOnce sync.Once

// Handler serves the exposition format. Mounted on the API's existing engine;
// the worker and relay have no HTTP server, so they use Serve below.
func Handler() http.Handler { return promhttp.Handler() }

// Serve runs a metrics-only HTTP listener until ctx is cancelled.
//
// The worker and relay are not HTTP services, but Prometheus only knows how to
// scrape, so they each need a socket. It carries nothing but /metrics — no
// business routes, no auth surface, and it binds a separate port so it can be
// left off the public ingress.
func Serve(ctx context.Context, addr string, log *slog.Logger) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Shutdown needs its own context: ctx is already cancelled by the time this
	// goroutine wakes, and passing it would abort the shutdown it is driving.
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("metrics listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
