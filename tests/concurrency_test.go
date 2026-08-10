package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Sawalhy/backend-golang-task-2025/internal/config"
	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

func testOrderConfig() config.OrderConfig {
	return config.OrderConfig{
		ReservationTTL:   15 * time.Minute,
		MaxItemsPerOrder: 50,
		DeadlockRetries:  3,
	}
}

func newOrderSvc(store *repository.Store) *services.OrderService {
	return services.NewOrderService(store, testOrderConfig(), logger.New("error", false))
}

func mustProduct(tb testing.TB, store *repository.Store, sku string, priceCents int64, stock int) *models.Product {
	tb.Helper()

	p, err := services.NewCatalogService(store).CreateProduct(context.Background(),
		services.CreateProductInput{SKU: sku, Name: sku, PriceCents: priceCents, Stock: stock})
	require.NoError(tb, err)
	return p
}

// placeConcurrently releases n goroutines simultaneously and collects their errors.
//
// The barrier is the entire point. Goroutines are spawned, each blocks on the
// same channel, and closing it releases all of them within microseconds so they
// genuinely contend for the same inventory row. Starting them in a plain loop
// lets each finish before the next begins, so they never collide and the test
// passes against code that oversells freely.
func placeConcurrently(t *testing.T, orders *services.OrderService, n int, build func(i int) services.CreateOrderInput) []error {
	t.Helper()

	var (
		wg      sync.WaitGroup
		release = make(chan struct{})
		results = make([]error, n)
	)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-release
			_, err := orders.Create(context.Background(), build(idx))
			results[idx] = err
		}(i)
	}

	// Let every goroutine reach the barrier before starting them, so spawn cost
	// does not stagger the release.
	time.Sleep(150 * time.Millisecond)
	close(release)
	wg.Wait()

	return results
}

func classify(t *testing.T, errs []error) (succeeded, outOfStock int) {
	t.Helper()

	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, models.ErrInsufficientStock):
			outOfStock++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	return succeeded, outOfStock
}

// The headline guarantee: failure mode A. Fifty customers, one unit.
//
// This is the test the whole inventory design exists to pass. Against the naive
// SELECT-then-UPDATE it fails immediately — several buyers read available=1,
// all decide yes, and the last unit sells many times over.
func TestNoOversellOnTheLastUnit(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	user := seedUser(t, db, "lastunit@example.com")
	product := mustProduct(t, store, "LAST-ONE", 9999, 1)
	orders := newOrderSvc(store)

	const buyers = 50
	errs := placeConcurrently(t, orders, buyers, func(int) services.CreateOrderInput {
		return services.CreateOrderInput{
			UserID: user,
			Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 1}},
		}
	})

	succeeded, outOfStock := classify(t, errs)
	assert.Equal(t, 1, succeeded, "exactly one buyer may get the last unit")
	assert.Equal(t, buyers-1, outOfStock, "everyone else must get insufficient stock, not an error")

	// The ledger has to balance too: one unit moved from available to reserved.
	inv, err := store.Inventory().Get(ctx, product.ID)
	require.NoError(t, err)
	assert.Equal(t, 0, inv.Available, "available must never go negative or stay stale")
	assert.Equal(t, 1, inv.Reserved)

	// And exactly one order exists — a rejected attempt must leave nothing
	// behind, which is what rolling the whole transaction back buys.
	var orderCount, reservationCount int64
	require.NoError(t, db.Raw(`SELECT count(*) FROM orders`).Scan(&orderCount).Error)
	require.NoError(t, db.Raw(`SELECT count(*) FROM reservations`).Scan(&reservationCount).Error)
	assert.EqualValues(t, 1, orderCount)
	assert.EqualValues(t, 1, reservationCount)
}

// The same guarantee where the answer is not 1, so a bug that happens to allow
// exactly one order through would still be caught.
func TestConcurrentBuyersCannotExceedStock(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	const (
		stock  = 7
		buyers = 60
	)

	user := seedUser(t, db, "limited@example.com")
	product := mustProduct(t, store, "LIMITED", 1500, stock)
	orders := newOrderSvc(store)

	errs := placeConcurrently(t, orders, buyers, func(int) services.CreateOrderInput {
		return services.CreateOrderInput{
			UserID: user,
			Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 1}},
		}
	})

	succeeded, outOfStock := classify(t, errs)
	assert.Equal(t, stock, succeeded, "exactly the available stock may be sold")
	assert.Equal(t, buyers-stock, outOfStock)

	inv, err := store.Inventory().Get(ctx, product.ID)
	require.NoError(t, err)
	assert.Equal(t, 0, inv.Available)
	assert.Equal(t, stock, inv.Reserved)
}

// The CHECK constraint is the second, independent guard. Even if the conditional
// UPDATE were wrong, the database itself must refuse to go negative — this
// asserts the constraint actually exists in the migration rather than only in
// the design document.
func TestDatabaseRefusesNegativeStock(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	product := mustProduct(t, store, "CHECKED", 100, 1)

	err := store.DB().WithContext(ctx).
		Exec(`UPDATE inventory SET available = available - 5 WHERE product_id = ?`, product.ID).Error

	require.Error(t, err, "the CHECK constraint must reject an oversell written directly")
	assert.Contains(t, err.Error(), "available")
}

// Failure mode G. Two multi-item orders that grab the same products in opposite
// order deadlock in Postgres, and one of them is killed with 40P01.
//
// Intake sorts line items by product_id before touching inventory, which imposes
// a total order on resources and makes a wait cycle impossible. Here half the
// goroutines submit the items reversed; if the sort were removed this test would
// start failing with deadlock errors rather than insufficient-stock ones.
func TestOppositeOrderLineItemsDoNotDeadlock(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	user := seedUser(t, db, "deadlock@example.com")
	first := mustProduct(t, store, "DEADLOCK-A", 500, 200)
	second := mustProduct(t, store, "DEADLOCK-B", 700, 200)
	orders := newOrderSvc(store)

	const buyers = 40
	errs := placeConcurrently(t, orders, buyers, func(i int) services.CreateOrderInput {
		lines := []services.OrderLine{
			{ProductID: first.ID, Qty: 1},
			{ProductID: second.ID, Qty: 1},
		}
		if i%2 == 1 {
			lines[0], lines[1] = lines[1], lines[0] // deliberately reversed
		}
		return services.CreateOrderInput{UserID: user, Lines: lines}
	})

	// classify fails the test on anything that is not nil or ErrInsufficientStock,
	// so a surviving deadlock surfaces here.
	succeeded, outOfStock := classify(t, errs)
	assert.Equal(t, buyers, succeeded, "stock is ample; every order should succeed")
	assert.Zero(t, outOfStock)

	for _, p := range []*models.Product{first, second} {
		inv, err := store.Inventory().Get(ctx, p.ID)
		require.NoError(t, err)
		assert.Equal(t, 200-buyers, inv.Available)
		assert.Equal(t, buyers, inv.Reserved)
	}
}

// Duplicate lines for one product must be merged, not inserted twice: the
// UNIQUE (order_id, product_id) constraint would reject the second, and merging
// is also what keeps the sort a total order.
func TestDuplicateLinesAreMergedNotRejected(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	user := seedUser(t, db, "dupes@example.com")
	product := mustProduct(t, store, "MERGED", 250, 10)

	order, err := newOrderSvc(store).Create(ctx, services.CreateOrderInput{
		UserID: user,
		Lines: []services.OrderLine{
			{ProductID: product.ID, Qty: 2},
			{ProductID: product.ID, Qty: 3},
		},
	})
	require.NoError(t, err)

	require.Len(t, order.Items, 1, "the two lines must collapse into one")
	assert.Equal(t, 5, order.Items[0].Qty)
	assert.EqualValues(t, 1250, order.TotalCents)

	inv, err := store.Inventory().Get(ctx, product.ID)
	require.NoError(t, err)
	assert.Equal(t, 5, inv.Available, "all five units reserved in one go")
}

// A client retrying a request whose response was lost must not end up with two
// orders. The pre-check is a convenience; the partial unique index is the
// guarantee.
func TestIdempotencyKeyPreventsDuplicateOrders(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	user := seedUser(t, db, "retry@example.com")
	product := mustProduct(t, store, "IDEMPOTENT", 400, 20)
	orders := newOrderSvc(store)
	key := "client-retry-key-1"

	first, err := orders.Create(ctx, services.CreateOrderInput{
		UserID: user, IdempotencyKey: &key,
		Lines: []services.OrderLine{{ProductID: product.ID, Qty: 2}},
	})
	require.NoError(t, err)

	second, err := orders.Create(ctx, services.CreateOrderInput{
		UserID: user, IdempotencyKey: &key,
		Lines: []services.OrderLine{{ProductID: product.ID, Qty: 2}},
	})
	require.NoError(t, err)

	assert.Equal(t, first.ID, second.ID, "the retry must return the original order")

	var orderCount int64
	require.NoError(t, db.Raw(`SELECT count(*) FROM orders`).Scan(&orderCount).Error)
	assert.EqualValues(t, 1, orderCount)

	// Critically, the retry must not reserve the stock a second time.
	inv, err := store.Inventory().Get(ctx, product.ID)
	require.NoError(t, err)
	assert.Equal(t, 18, inv.Available)
	assert.Equal(t, 2, inv.Reserved)
}

// Every accepted order must have written its event in the same transaction
// (failure mode B). An order with no outbox row is stranded forever.
func TestAcceptedOrderAlwaysWritesItsEvent(t *testing.T) {
	store, db := newStore(t)

	user := seedUser(t, db, "outbox@example.com")
	product := mustProduct(t, store, "EVENTED", 300, 5)
	orders := newOrderSvc(store)

	const buyers = 20
	errs := placeConcurrently(t, orders, buyers, func(int) services.CreateOrderInput {
		return services.CreateOrderInput{
			UserID: user,
			Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 1}},
		}
	})
	succeeded, _ := classify(t, errs)

	var events int64
	require.NoError(t, db.Raw(
		`SELECT count(*) FROM outbox WHERE routing_key = ?`, models.EventOrderCreated).
		Scan(&events).Error)

	assert.EqualValues(t, succeeded, events,
		"one order.created per accepted order, no more and no fewer")

	// Rejected attempts roll back, so they must leave no event behind either.
	var orderCount int64
	require.NoError(t, db.Raw(`SELECT count(*) FROM orders`).Scan(&orderCount).Error)
	assert.EqualValues(t, orderCount, events)
}
