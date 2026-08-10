// Benchmarks for the concurrent operations, README.md:247 (Deliverable 6).
//
// These sit alongside `cmd/loadtest` and answer a different question. The load
// test drives the whole system through HTTP and reports what a client sees:
// throughput, p50/p99, the outcome mix. It cannot say WHERE the time went.
// These take the statements the design actually rests on and measure each one
// directly, against real Postgres, under real contention: every goroutine hits
// the same inventory row, which is what a popular product looks like.
//
// Running them:
//
//	go test ./tests -bench . -run NONE -benchmem
//
// `-run NONE` matches no test, so only benchmarks run. Standard `testing`
// output only — ns/op, B/op, allocs/op.
//
// Postgres comes from TEST_DATABASE_URL or testcontainers, same as the tests —
// see support_test.go. Nothing is mocked, because a mock cannot contend, and a
// benchmark of a mock measures the mock.
//
// Absolute figures are host-bound: on any machine where Postgres is behind a VM,
// commit fsync dominates every write path here.
package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/database"
)

// benchStock is seeded far above any achievable iteration count, so these
// measure the success path throughout. Let stock run out partway and the
// remaining iterations measure a rejected UPDATE instead — a different and much
// cheaper statement — and the average quietly becomes meaningless.
const benchStock = 1_000_000_000

// benchPoolConns matches the pool the concurrency tests use.
const benchPoolConns = 25

// One pool for every benchmark in this package, opened on first use.
//
// A benchmark body is re-executed for every N the framework tries, so opening a
// pool at the top of one opens a pool per round and releases none of them until
// the whole benchmark ends. Sharing one is also closer to the truth: api, worker
// and relay contend for a fixed pool too.
var (
	benchPoolOnce sync.Once
	benchStoreRef *repository.Store
	benchDBRef    *gorm.DB
)

func benchPool(b *testing.B) (*repository.Store, *gorm.DB) {
	b.Helper()

	benchPoolOnce.Do(func() {
		benchDBRef = openDB(b, benchPoolConns)
		benchStoreRef = repository.New(benchDBRef, 3)
	})
	require.NotNil(b, benchDBRef, "benchmark pool failed to open")

	return benchStoreRef, benchDBRef
}

// closeBenchPool is called by TestMain after the run completes. The pool
// outlives every individual benchmark, so no b.Cleanup can own it.
func closeBenchPool() {
	if benchDBRef != nil {
		_ = database.Close(benchDBRef)
	}
}

// benchFixture is what one timed round runs against: a fresh user and the
// product rows the workers will hit.
type benchFixture struct {
	user     uint64
	products []uint64
}

// benchFail records the first error raised inside a RunParallel body.
//
// b.Fatal is not usable there: FailNow must be called from the goroutine running
// the benchmark, and the parallel bodies are not it. b.Error is safe from any
// goroutine, so take the first error, report it once, and let the worker return.
func benchFail(b *testing.B, once *atomic.Bool, err error) {
	if once.CompareAndSwap(false, true) {
		b.Error(err)
	}
}

// benchReset truncates and reseeds, and every benchmark below calls it before
// each timed round rather than once at the top.
//
// This is not tidiness. The testing package runs a benchmark body repeatedly
// with a growing N, and a database keeps everything the previous round did:
// dead tuples piling up on the one hot inventory row, an outbox nobody is
// draining, orders and reservations growing without bound. Seeding once and
// reusing it drifted by 8x from the first round to the last, monotonically —
// so the reported figure was mostly a function of how long the benchmark had
// already been running. Resetting per round costs a TRUNCATE that ResetTimer
// excludes, and makes consecutive rounds comparable.
//
// `products` rows are created after the truncate, so ids differ every round.
// Nothing may cache them across rounds.
func benchReset(b *testing.B, store *repository.Store, db *gorm.DB, prefix string, products int) benchFixture {
	b.Helper()

	truncateAll(b, db)

	f := benchFixture{
		user:     seedUser(b, db, prefix+"@bench.local"),
		products: make([]uint64, products),
	}
	for i := range f.products {
		f.products[i] = mustProduct(b, store,
			fmt.Sprintf("%s-%d", prefix, i), 1000, benchStock).ID
	}
	return f
}

// The conditional UPDATE on its own, with nothing else in the transaction.
//
// This is the statement failure mode A turns on, and the narrowest thing worth
// measuring: one round trip, one row, no read-modify-write gap. Every goroutine
// takes the same row, so Postgres holds a row lock for the duration of each
// UPDATE and the workers queue on it.
func BenchmarkReserveInventory(b *testing.B) {
	store, db := benchPool(b)
	inv := store.Inventory()
	ctx := context.Background()

	f := benchReset(b, store, db, "reserve", 1)
	id := f.products[0]

	var failed atomic.Bool

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := inv.Reserve(ctx, nil, id, 1); err != nil {
				benchFail(b, &failed, err)
				return
			}
		}
	})
}

// The whole intake transaction: load the products, reserve, insert the order,
// its items, its reservations and the outbox event, then commit.
//
// This is the graded core (README.md:189) and the honest per-order cost of an
// accepted order — everything POST /orders does except serving HTTP. Payment is
// absent because it is absent from the transaction: it happens later, in the
// worker, with no lock held. That is the decision this benchmark prices.
//
// The reserving UPDATE takes the row lock partway through the transaction and
// Postgres holds it until COMMIT, so the queue behind that row is waiting on the
// rest of the transaction and its commit, not just on the UPDATE. Everything the
// transaction does after reserving is time the next buyer waits.
func BenchmarkOrderIntake(b *testing.B) {
	store, db := benchPool(b)
	orders := newOrderSvc(store)
	ctx := context.Background()

	f := benchReset(b, store, db, "intake", 1)
	id := f.products[0]

	var failed atomic.Bool

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := orders.Create(ctx, services.CreateOrderInput{
				UserID: f.user,
				Lines:  []services.OrderLine{{ProductID: id, Qty: 1}},
			})
			if err != nil {
				benchFail(b, &failed, err)
				return
			}
		}
	})
}

// A two-line order: the second reservation, and the sort in normalizeLines.
//
// Contended on both products deliberately: that is also the arrangement that
// deadlocks without the ordering rule, so a regression there surfaces here as
// 40P01 retries inflating ns/op rather than as a silent slowdown.
func BenchmarkOrderIntakeMultiLine(b *testing.B) {
	store, db := benchPool(b)
	orders := newOrderSvc(store)
	ctx := context.Background()

	f := benchReset(b, store, db, "intake-multi", 2)
	first, second := f.products[0], f.products[1]

	var failed atomic.Bool

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := orders.Create(ctx, services.CreateOrderInput{
				UserID: f.user,
				Lines: []services.OrderLine{
					// Written backwards on purpose. Intake sorts by product_id,
					// so these take the same lock order as everyone else despite
					// arriving reversed — which is exactly what stops this
					// benchmark from deadlocking against itself.
					{ProductID: second, Qty: 1},
					{ProductID: first, Qty: 1},
				},
			})
			if err != nil {
				benchFail(b, &failed, err)
				return
			}
		}
	})
}

// The CAS that makes at-least-once delivery survivable, measured on the branch
// that actually runs concurrently.
//
// A winning transition happens once per order. A LOSING one happens every time
// the broker redelivers, every time two workers race the same order.created,
// every time a consumer retries — so the losing branch is the hot one, and it is
// the branch the no-double-charge guarantee rests on. Here the order is already
// CHARGING before the clock starts, so every iteration is a duplicate delivery
// reaching a worker that has been beaten to it: the UPDATE matches no row,
// RowsAffected is 0, and the caller learns it does not own the payment.
//
// The statement matches nothing, so it writes no tuple and forces no commit —
// which is the argument for putting dedupe in a CAS rather than in a read
// followed by a decision.
func BenchmarkTransitionLostRace(b *testing.B) {
	store, db := benchPool(b)
	ctx := context.Background()

	f := benchReset(b, store, db, "cas", 1)

	order, err := newOrderSvc(store).Create(ctx, services.CreateOrderInput{
		UserID: f.user,
		Lines:  []services.OrderLine{{ProductID: f.products[0], Qty: 1}},
	})
	require.NoError(b, err)

	// One winner, before the clock starts. Everything after this loses.
	won, err := store.Orders().Transition(ctx, nil, order.ID, models.OrderPending, models.OrderCharging)
	require.NoError(b, err)
	require.True(b, won, "the first transition must win or the benchmark measures the wrong branch")

	var failed atomic.Bool

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			won, err := store.Orders().Transition(ctx, nil, order.ID,
				models.OrderPending, models.OrderCharging)
			if err != nil {
				benchFail(b, &failed, err)
				return
			}
			if won {
				benchFail(b, &failed, errors.New("CAS won twice: the order was not left CHARGING"))
				return
			}
		}
	})
}
