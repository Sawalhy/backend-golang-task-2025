package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

// scriptedProvider is a payment provider with deterministic outcomes and a gate
// that can hold a charge mid-flight.
//
// The gate is what makes failure mode D testable at all: a cancel racing a live
// charge is a timing bug, and reproducing it by hoping the scheduler interleaves
// two goroutines correctly would be flaky. Blocking inside Charge makes the race
// deterministic.
//
// It is idempotent the way a real provider is — a repeated key returns the
// stored result instead of charging again — and counts DISTINCT charges
// separately from calls, which is what lets a test assert "the card was charged
// exactly once" rather than merely "the code ran once".
type scriptedProvider struct {
	mu        sync.Mutex
	outcome   services.PaymentOutcome
	reason    string
	chargeErr error

	results map[string]services.ChargeResult
	calls   map[string]int
	charges int // distinct keys actually charged
	replays int // calls that returned a stored result
	refunds int

	gate      chan struct{} // if non-nil, Charge blocks on it
	started   chan struct{}
	startOnce sync.Once
}

func newScriptedProvider(outcome services.PaymentOutcome) *scriptedProvider {
	return &scriptedProvider{
		outcome: outcome,
		results: make(map[string]services.ChargeResult),
		calls:   make(map[string]int),
		started: make(chan struct{}),
	}
}

func (p *scriptedProvider) Name() string { return "scripted" }

func (p *scriptedProvider) Charge(ctx context.Context, req services.ChargeRequest) (services.ChargeResult, error) {
	p.mu.Lock()
	p.calls[req.IdempotencyKey]++
	if prev, ok := p.results[req.IdempotencyKey]; ok {
		p.replays++
		p.mu.Unlock()
		return prev, nil // same key, same answer: the card is not charged again
	}
	gate := p.gate
	p.mu.Unlock()

	p.startOnce.Do(func() { close(p.started) })

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return services.ChargeResult{Outcome: services.OutcomeUnknown}, ctx.Err()
		}
	}

	// A timeout records nothing: the whole point of UNKNOWN is that the provider
	// may well have charged despite our not hearing back.
	if p.outcome == services.OutcomeUnknown {
		return services.ChargeResult{Outcome: services.OutcomeUnknown}, services.ErrProviderTimeout
	}

	res := services.ChargeResult{Outcome: p.outcome, Reason: p.reason}
	if p.outcome == services.OutcomeSucceeded {
		res.ProviderRef = "ch_" + req.IdempotencyKey[:8]
	}

	p.mu.Lock()
	p.results[req.IdempotencyKey] = res
	p.charges++
	p.mu.Unlock()

	return res, p.chargeErr
}

func (p *scriptedProvider) Refund(ctx context.Context, providerRef string, amountCents int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refunds++
	return nil
}

// Lookup is what the reconciler uses to ask "did this actually go through?".
func (p *scriptedProvider) Lookup(key string) (services.ChargeResult, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	res, ok := p.results[key]
	return res, ok
}

func (p *scriptedProvider) stats() (charges, replays, refunds int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.charges, p.replays, p.refunds
}

// --- fixtures ---------------------------------------------------------------

type sagaFixture struct {
	store    *repository.Store
	db       *gorm.DB
	orders   *services.OrderService
	payments *services.PaymentService
	provider *scriptedProvider
	user     uint64
	product  *models.Product
	orderID  uint64
}

func newSagaFixture(t *testing.T, outcome services.PaymentOutcome, stock int) *sagaFixture {
	t.Helper()

	store, db := newStore(t)
	provider := newScriptedProvider(outcome)
	log := logger.New("error", false)

	f := &sagaFixture{
		store:    store,
		db:       db,
		orders:   newOrderSvc(store),
		payments: services.NewPaymentService(store, provider, log),
		provider: provider,
		user:     seedUser(t, db, "saga@example.com"),
		product:  mustProduct(t, store, "SAGA", 4200, stock),
	}

	order, err := f.orders.Create(context.Background(), services.CreateOrderInput{
		UserID: f.user,
		Lines:  []services.OrderLine{{ProductID: f.product.ID, Qty: 1}},
	})
	require.NoError(t, err)
	f.orderID = order.ID

	return f
}

func (f *sagaFixture) status(t *testing.T) models.OrderStatus {
	t.Helper()
	return orderStatusOf(t, f.store, f.orderID)
}

func (f *sagaFixture) inventory(t *testing.T) *models.Inventory {
	t.Helper()
	inv, err := f.store.Inventory().Get(context.Background(), f.product.ID)
	require.NoError(t, err)
	return inv
}

func (f *sagaFixture) countEvents(t *testing.T, routingKey string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, f.db.Raw(
		`SELECT count(*) FROM outbox WHERE routing_key = ?`, routingKey).Scan(&n).Error)
	return n
}

func (f *sagaFixture) payment(t *testing.T) models.Payment {
	t.Helper()
	rows, err := f.store.Payments().ListForOrder(context.Background(), f.orderID)
	require.NoError(t, err)
	require.Len(t, rows, 1, "there must be exactly one payment intent per order")
	return rows[0]
}

// --- failure mode D: cancel arrives while the charge is in flight -----------

// The subtlest correctness bug in the system. The customer cancels while the
// card is being charged, and the charge wins.
//
// The naive implementation cancels the order and releases the stock
// immediately, and the customer ends up charged for an order the system says
// was cancelled. Here the cancel moves CHARGING -> CANCELLING, which is a
// recorded INTENT to cancel rather than a cancellation, and the payment worker
// resolves it once it knows the outcome.
func TestCancelDuringChargeCompensatesWithRefund(t *testing.T) {
	f := newSagaFixture(t, services.OutcomeSucceeded, 5)
	ctx := context.Background()

	f.provider.gate = make(chan struct{})

	done := make(chan error, 1)
	go func() { done <- f.payments.ProcessOrder(ctx, f.orderID) }()

	// Wait until the charge is genuinely in flight: the order is CHARGING, the
	// intent is committed, and the provider has not answered.
	select {
	case <-f.provider.started:
	case <-time.After(10 * time.Second):
		t.Fatal("provider was never called")
	}
	require.Equal(t, models.OrderCharging, f.status(t))

	// The cancel lands mid-charge. It must NOT report success, and must NOT
	// release the stock — the charge may be about to succeed.
	res, err := f.orders.Cancel(ctx, f.orderID, f.user, false)
	require.NoError(t, err)
	assert.True(t, res.Pending, "a cancel racing a live charge is 202, not 200")
	assert.Equal(t, models.OrderCancelling, f.status(t))

	inv := f.inventory(t)
	assert.Equal(t, 4, inv.Available, "stock must stay held while the charge is live")
	assert.Equal(t, 1, inv.Reserved)

	// Let the charge complete. It succeeds, so the money moved.
	close(f.provider.gate)
	require.NoError(t, <-done)

	// A saga cannot roll back a committed step in another system, so the
	// compensation is a refund — a second forward action, not an undo.
	assert.Equal(t, models.OrderCancelledRefunded, f.status(t))
	assert.EqualValues(t, 1, f.countEvents(t, models.EventRefundRequested),
		"a refund must be requested for money that actually moved")
	assert.EqualValues(t, 1, f.countEvents(t, models.EventOrderCancelled))

	// Only now does the stock come back.
	inv = f.inventory(t)
	assert.Equal(t, 5, inv.Available, "stock returns once the outcome is known")
	assert.Equal(t, 0, inv.Reserved)

	charges, _, _ := f.provider.stats()
	assert.Equal(t, 1, charges, "the card is charged exactly once")
}

// The other branch: the cancel raced the charge and the CARD WAS DECLINED. No
// money moved, so there is nothing to compensate — the order is simply
// cancelled and no refund is requested.
func TestCancelDuringChargeWithDeclineNeedsNoRefund(t *testing.T) {
	f := newSagaFixture(t, services.OutcomeDeclined, 5)
	f.provider.reason = "card_declined"
	ctx := context.Background()

	f.provider.gate = make(chan struct{})

	done := make(chan error, 1)
	go func() { done <- f.payments.ProcessOrder(ctx, f.orderID) }()

	select {
	case <-f.provider.started:
	case <-time.After(10 * time.Second):
		t.Fatal("provider was never called")
	}

	res, err := f.orders.Cancel(ctx, f.orderID, f.user, false)
	require.NoError(t, err)
	require.True(t, res.Pending)

	close(f.provider.gate)
	require.NoError(t, <-done)

	assert.Equal(t, models.OrderCancelled, f.status(t),
		"a declined charge means plain CANCELLED, not CANCELLED_REFUNDED")
	assert.EqualValues(t, 0, f.countEvents(t, models.EventRefundRequested),
		"refunding a charge that never landed would pay the customer money they never spent")

	inv := f.inventory(t)
	assert.Equal(t, 5, inv.Available)
	assert.Equal(t, 0, inv.Reserved)
}

// Cancelling before anything is charged is the easy path, and must stay easy:
// immediate, final, stock straight back.
func TestCancelBeforeChargeIsImmediate(t *testing.T) {
	f := newSagaFixture(t, services.OutcomeSucceeded, 5)
	ctx := context.Background()

	res, err := f.orders.Cancel(ctx, f.orderID, f.user, false)
	require.NoError(t, err)
	assert.False(t, res.Pending, "nothing is in flight, so the cancel is final")
	assert.Equal(t, models.OrderCancelled, f.status(t))

	inv := f.inventory(t)
	assert.Equal(t, 5, inv.Available)
	assert.Equal(t, 0, inv.Reserved)

	// And the payment worker must not then charge a cancelled order.
	require.NoError(t, f.payments.ProcessOrder(ctx, f.orderID))
	charges, _, _ := f.provider.stats()
	assert.Zero(t, charges, "a cancelled order must never be charged")
}

// --- failure mode C: no double charge --------------------------------------

// At-least-once delivery guarantees this happens: the same order.created is
// consumed twice. Every step is a CAS, so the second delivery finds the work
// already done.
func TestDuplicateDeliveryChargesOnlyOnce(t *testing.T) {
	f := newSagaFixture(t, services.OutcomeSucceeded, 5)
	ctx := context.Background()

	require.NoError(t, f.payments.ProcessOrder(ctx, f.orderID))
	require.NoError(t, f.payments.ProcessOrder(ctx, f.orderID))
	require.NoError(t, f.payments.ProcessOrder(ctx, f.orderID))

	assert.Equal(t, models.OrderPaid, f.status(t))

	charges, _, _ := f.provider.stats()
	assert.Equal(t, 1, charges, "three deliveries, one charge")

	// And exactly one intent, because the partial unique index allows only one
	// live intent per order.
	payment := f.payment(t)
	assert.Equal(t, models.PaymentSucceeded, payment.Status)

	// Stock is consumed once, not three times.
	inv := f.inventory(t)
	assert.Equal(t, 4, inv.Available)
	assert.Equal(t, 0, inv.Reserved, "sold stock leaves reserved without returning")
}

// Concurrent duplicate deliveries — two workers racing on the same message.
func TestConcurrentDeliveriesChargeOnlyOnce(t *testing.T) {
	f := newSagaFixture(t, services.OutcomeSucceeded, 5)
	ctx := context.Background()

	const workers = 8
	var wg sync.WaitGroup
	release := make(chan struct{})
	errs := make([]error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-release
			errs[idx] = f.payments.ProcessOrder(ctx, f.orderID)
		}(i)
	}
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}

	assert.Equal(t, models.OrderPaid, f.status(t))
	charges, _, _ := f.provider.stats()
	assert.Equal(t, 1, charges, "eight concurrent workers, one charge")
}

// The partial unique index is what makes "one live intent" an invariant rather
// than a convention. UNKNOWN must block a second intent, because it means the
// customer MAY have been charged.
func TestSecondLiveIntentIsRefused(t *testing.T) {
	f := newSagaFixture(t, services.OutcomeSucceeded, 5)
	ctx := context.Background()

	err := f.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		_, err := f.store.Payments().CreateIntent(ctx, tx, f.orderID, 4200, "scripted")
		return err
	})
	require.NoError(t, err)

	err = f.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		_, err := f.store.Payments().CreateIntent(ctx, tx, f.orderID, 4200, "scripted")
		return err
	})
	require.ErrorIs(t, err, models.ErrPaymentPending,
		"a second live intent would mean a second idempotency key, and a second charge")
}

// --- failure mode E: a worker dies mid-charge -------------------------------

// The gap this exposes: nothing else in the system can recover a half-charged
// order. Redelivery skips it because it is not PENDING, the reaper skips it
// because it only expires PENDING, and reconciliation skips it because the
// intent is INITIATED rather than UNKNOWN.
//
// This test asserts that inaction explicitly, so the recovery sweep below has a
// documented reason to exist.
func TestRedeliveryAloneCannotRecoverAHalfChargedOrder(t *testing.T) {
	f := newSagaFixture(t, services.OutcomeSucceeded, 5)
	ctx := context.Background()

	killWorkerMidCharge(t, f)

	// A redelivered order.created changes nothing.
	require.NoError(t, f.payments.ProcessOrder(ctx, f.orderID))
	assert.Equal(t, models.OrderCharging, f.status(t))

	// Neither does the reaper, even once the reservation has expired — and that
	// restraint is correct, because a live charge may still be in flight.
	age(t, f.store, f.orderID)
	reaped, err := newReaper(f.store).ReapOnce(ctx)
	require.NoError(t, err)
	assert.Zero(t, reaped)
	assert.Equal(t, models.OrderCharging, f.status(t))

	charges, _, _ := f.provider.stats()
	assert.Zero(t, charges, "the dead worker never reached the provider")
}

// The recovery sweep closes that gap: it re-drives the abandoned intent with the
// SAME idempotency key, so the provider either replays the original result or
// performs the charge for the first time — never twice.
func TestRecoverStuckChargeCompletesTheOrder(t *testing.T) {
	f := newSagaFixture(t, services.OutcomeSucceeded, 5)
	ctx := context.Background()

	killWorkerMidCharge(t, f)
	backdatePayment(t, f, 2*time.Minute)

	recovered, err := f.payments.RecoverStuckCharges(ctx, time.Minute, 50)
	require.NoError(t, err)
	assert.Equal(t, 1, recovered)

	assert.Equal(t, models.OrderPaid, f.status(t), "the order must reach a terminal state")

	payment := f.payment(t)
	assert.Equal(t, models.PaymentSucceeded, payment.Status)

	charges, _, _ := f.provider.stats()
	assert.Equal(t, 1, charges, "recovery must not double charge")

	inv := f.inventory(t)
	assert.Equal(t, 4, inv.Available)
	assert.Equal(t, 0, inv.Reserved)

	assert.EqualValues(t, 1, f.countEvents(t, models.EventOrderPaid))
}

// The same recovery when the card is declined: the order fails and the stock
// goes back, rather than being held forever by a dead worker.
func TestRecoverStuckChargeReleasesStockOnDecline(t *testing.T) {
	f := newSagaFixture(t, services.OutcomeDeclined, 5)
	ctx := context.Background()

	killWorkerMidCharge(t, f)
	backdatePayment(t, f, 2*time.Minute)

	_, err := f.payments.RecoverStuckCharges(ctx, time.Minute, 50)
	require.NoError(t, err)

	assert.Equal(t, models.OrderFailed, f.status(t))

	inv := f.inventory(t)
	assert.Equal(t, 5, inv.Available, "a declined charge must return the stock")
	assert.Equal(t, 0, inv.Reserved)
}

// The grace period exists so the sweep does not "recover" a payment that is
// merely slow while its first call is still in flight — which would be the
// double-charge this whole design avoids, reintroduced by the fix for E.
func TestRecoveryLeavesRecentIntentsAlone(t *testing.T) {
	f := newSagaFixture(t, services.OutcomeSucceeded, 5)
	ctx := context.Background()

	killWorkerMidCharge(t, f) // intent is brand new, so well inside the grace period

	recovered, err := f.payments.RecoverStuckCharges(ctx, time.Minute, 50)
	require.NoError(t, err)
	assert.Zero(t, recovered, "a charge that may still be in flight must not be re-driven")
	assert.Equal(t, models.OrderCharging, f.status(t))
}

// A provider that times out leaves UNKNOWN: we cannot say whether the customer
// was charged, so the order must not be failed OR completed on a guess. The
// intent parks in UNKNOWN, which the partial unique index treats as live.
func TestTimeoutParksPaymentInUnknown(t *testing.T) {
	f := newSagaFixture(t, services.OutcomeUnknown, 5)
	ctx := context.Background()

	require.NoError(t, f.payments.ProcessOrder(ctx, f.orderID))

	payment := f.payment(t)
	assert.Equal(t, models.PaymentUnknown, payment.Status,
		"an unknown outcome must not be guessed either way")
	assert.Equal(t, models.OrderCharging, f.status(t))

	// Stock stays held: the customer may have paid for it.
	inv := f.inventory(t)
	assert.Equal(t, 4, inv.Available)
	assert.Equal(t, 1, inv.Reserved)
}

// Reconciliation resolves UNKNOWN by asking the provider what it believes, which
// is the only honest way out of that state.
func TestReconciliationResolvesUnknownAsNeverCharged(t *testing.T) {
	f := newSagaFixture(t, services.OutcomeUnknown, 5)
	ctx := context.Background()

	require.NoError(t, f.payments.ProcessOrder(ctx, f.orderID))
	require.Equal(t, models.PaymentUnknown, f.payment(t).Status)

	// The provider has no record of the key, so the request never landed.
	resolved, err := f.payments.SweepUnknownPayments(ctx, 50)
	require.NoError(t, err)
	assert.Equal(t, 1, resolved)

	assert.Equal(t, models.OrderFailed, f.status(t))
	assert.Equal(t, models.PaymentDeclined, f.payment(t).Status)

	inv := f.inventory(t)
	assert.Equal(t, 5, inv.Available, "stock returns once we know no charge landed")
	assert.Equal(t, 0, inv.Reserved)
}

// --- helpers ----------------------------------------------------------------

// killWorkerMidCharge reproduces a SIGKILLed worker: the order was claimed and
// the intent committed, but the process died before calling the provider.
//
// Done directly rather than by cancelling a context, because the point is that
// NO deferred cleanup ran — the process simply stopped existing, which is what
// makes this unrecoverable without a sweep.
func killWorkerMidCharge(t *testing.T, f *sagaFixture) {
	t.Helper()
	ctx := context.Background()

	require.NoError(t, f.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		ok, err := f.store.Orders().Transition(ctx, tx, f.orderID,
			models.OrderPending, models.OrderCharging)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("could not claim the order")
		}
		_, err = f.store.Payments().CreateIntent(ctx, tx, f.orderID, 4200, "scripted")
		return err
	}))
}

// backdatePayment ages the intent past the recovery grace period without
// sleeping for it.
func backdatePayment(t *testing.T, f *sagaFixture, by time.Duration) {
	t.Helper()
	require.NoError(t, f.db.Exec(
		`UPDATE payments SET updated_at = now() - (? * interval '1 second') WHERE order_id = ?`,
		by.Seconds(), f.orderID).Error)
}
