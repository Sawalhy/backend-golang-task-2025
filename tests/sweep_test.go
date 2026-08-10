package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
)

// Sweeps: run one operation repeatedly, failing a DIFFERENT step each time, and
// assert the same invariant after every one.
//
// The individual tests in failure_paths_test.go pick a step and prove the
// rollback holds there. A sweep proves it holds at EVERY step, which guards
// three things the picked-step tests cannot:
//
//   - a step later moved OUTSIDE the transaction
//   - an error later swallowed (a missing return, a discarded `_ =`)
//   - a non-transactional side effect later added — a cache write, an HTTP call,
//     a direct publish. That last one silently reintroduces failure mode B, and
//     it is exactly the change someone makes without realising.
//
// Each position runs as a subtest, so a failure names the step rather than
// leaving you to bisect:
//
//	--- FAIL: TestIntakeRollsBackAtEveryStep/fail_at_call_4
//
// Rollback itself is Postgres's job, not ours. What is ours is that the work is
// inside a transaction at all, that errors propagate, and that nothing escapes
// the transaction's reach — and that is what these assert.

var errInjected = errors.New("injected failure")

// sweepSteps is how many positions to probe. Higher than any operation's actual
// call count, so the sweep cannot silently stop short if a step is added; the
// loop reports how many positions actually fired.
const sweepSteps = 12

// TestIntakeRollsBackAtEveryStep walks every database call in order intake.
//
// Intake is the operation where a partial write is worst: a surviving order with
// no reservation is stock sold twice, and a surviving order with no outbox row is
// an order nobody will ever charge.
func TestIntakeRollsBackAtEveryStep(t *testing.T) {
	fired := 0

	for n := 1; n <= sweepSteps; n++ {
		t.Run(fmt.Sprintf("fail_at_call_%d", n), func(t *testing.T) {
			store, db, faults := newStoreWithFaults(t)
			ctx := context.Background()

			user := seedUser(t, db, fmt.Sprintf("sweep-intake-%d@example.com", n))
			product := mustProduct(t, store, fmt.Sprintf("SWEEP-%d", n), 1000, 10)

			// Arm only now: setup above must not be counted.
			rule := faults.FailNthCall("", n, errInjected)

			_, err := newOrderSvc(store).Create(ctx, oneLineOrder(user, product.ID))

			faults.disarm() // before asserting, or the checks below hit the fault
			if rule.timesFired() == 0 {
				t.Skipf("intake makes fewer than %d calls", n)
			}
			fired++

			require.Error(t, err, "a failed step must fail the operation")
			assertIntakeLeftNothing(t, store, db, product.ID)
		})
	}

	assert.GreaterOrEqual(t, fired, 5,
		"the sweep should have exercised most of intake's steps; got %d", fired)
}

// assertIntakeLeftNothing is the invariant, identical at every position: a
// failed intake leaves no trace anywhere.
func assertIntakeLeftNothing(t *testing.T, store *repository.Store, db *gorm.DB, productID uint64) {
	t.Helper()

	assert.Zero(t, countRows(t, db, "orders"), "no order may survive")
	assert.Zero(t, countRows(t, db, "order_items"), "no line items may survive")
	assert.Zero(t, countRows(t, db, "reservations"), "no reservation may survive")
	assert.Zero(t, countRows(t, db, "outbox"),
		"no event may survive — an event for an order that does not exist is worse than none")

	inv, err := store.Inventory().Get(context.Background(), productID)
	require.NoError(t, err)
	assert.Equal(t, 10, inv.Available, "stock must be exactly as it started")
	assert.Equal(t, 0, inv.Reserved)
}

// TestCancelRollsBackAtEveryStep walks the cancel path.
//
// A partial cancel is the dangerous direction: stock released while the order
// stays live means the same units can be sold twice.
func TestCancelRollsBackAtEveryStep(t *testing.T) {
	fired := 0

	for n := 1; n <= sweepSteps; n++ {
		t.Run(fmt.Sprintf("fail_at_call_%d", n), func(t *testing.T) {
			store, db, faults := newStoreWithFaults(t)
			ctx := context.Background()

			user := seedUser(t, db, fmt.Sprintf("sweep-cancel-%d@example.com", n))
			product := mustProduct(t, store, fmt.Sprintf("SWEEPC-%d", n), 1000, 10)

			order, err := newOrderSvc(store).Create(ctx, oneLineOrder(user, product.ID))
			require.NoError(t, err)

			rule := faults.FailNthCall("", n, errInjected)
			_, cancelErr := newOrderSvc(store).Cancel(ctx, order.ID, user, false)
			faults.disarm()

			if rule.timesFired() == 0 {
				t.Skipf("cancel makes fewer than %d calls", n)
			}
			fired++

			require.Error(t, cancelErr)

			// The order stays exactly as it was, and so does the stock.
			assert.Equal(t, models.OrderPending, orderStatusOf(t, store, order.ID),
				"a failed cancel must not half-move the order")

			inv, err := store.Inventory().Get(ctx, product.ID)
			require.NoError(t, err)
			assert.Equal(t, 9, inv.Available,
				"stock must not be released by a cancel that did not complete")
			assert.Equal(t, 1, inv.Reserved)

			held, err := store.Orders().ReservationsForOrder(ctx, nil, order.ID, models.ReservationHeld)
			require.NoError(t, err)
			assert.Len(t, held, 1, "the reservation must still be HELD")
		})
	}

	assert.Positive(t, fired)
}

// TestReaperRollsBackAtEveryStep walks the reaper.
//
// The reaper both releases stock and expires the order. Doing one without the
// other is how stock gets invented or lost.
func TestReaperRollsBackAtEveryStep(t *testing.T) {
	fired := 0

	for n := 1; n <= sweepSteps; n++ {
		t.Run(fmt.Sprintf("fail_at_call_%d", n), func(t *testing.T) {
			store, db, faults := newStoreWithFaults(t)
			ctx := context.Background()

			user := seedUser(t, db, fmt.Sprintf("sweep-reap-%d@example.com", n))
			product := mustProduct(t, store, fmt.Sprintf("SWEEPR-%d", n), 1000, 10)

			order, err := newOrderSvc(store).Create(ctx, oneLineOrder(user, product.ID))
			require.NoError(t, err)
			age(t, store, order.ID)

			rule := faults.FailNthCall("", n, errInjected)
			_, reapErr := newReaper(store).ReapOnce(ctx)
			faults.disarm()

			if rule.timesFired() == 0 {
				t.Skipf("the reaper makes fewer than %d calls", n)
			}
			fired++

			require.Error(t, reapErr)

			// Either it fully reaped or it did nothing. Never half.
			status := orderStatusOf(t, store, order.ID)
			require.Equal(t, models.OrderPending, status,
				"a failed reap must leave the order untouched for the next sweep")

			inv, err := store.Inventory().Get(ctx, product.ID)
			require.NoError(t, err)
			assert.Equal(t, 9, inv.Available,
				"stock must not be released without the order being expired")
			assert.Equal(t, 1, inv.Reserved)
		})
	}

	assert.Positive(t, fired)
}
