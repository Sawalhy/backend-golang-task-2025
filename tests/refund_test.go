package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
)

// The saga tests prove a refund is REQUESTED — the order reaches
// CANCELLED_REFUNDED and a payment.refund_requested event lands in the outbox.
// Nothing there proves it is ever EXECUTED.
//
// That gap matters more than most: this is the compensating action of the whole
// cancel-vs-charge design, and it is the one path where a bug costs real money in
// the direction nobody complains about — paying a customer back twice.

// chargedFixture drives an order to PAID so there is a SUCCEEDED payment with a
// provider reference to refund.
func chargedFixture(t *testing.T) *sagaFixture {
	t.Helper()

	f := newSagaFixture(t, services.OutcomeSucceeded, 5)
	require.NoError(t, f.payments.ProcessOrder(context.Background(), f.orderID))
	require.Equal(t, models.OrderPaid, f.status(t))

	payment := f.payment(t)
	require.Equal(t, models.PaymentSucceeded, payment.Status)
	require.NotNil(t, payment.ProviderRef, "a successful charge must record its provider reference")

	return f
}

func refundEvent(orderID uint64, paymentID string) models.Envelope {
	return models.NewOrderEvent(models.EventRefundRequested, orderID, map[string]any{
		"paymentId":   paymentID,
		"amountCents": 4200,
	})
}

func TestRefundIsExecutedAgainstTheProvider(t *testing.T) {
	f := chargedFixture(t)
	ctx := context.Background()
	payment := f.payment(t)

	require.NoError(t, f.payments.HandleRefund(ctx, refundEvent(f.orderID, payment.ID.String())))

	_, _, refunds := f.provider.stats()
	assert.Equal(t, 1, refunds, "the money must actually be sent back")

	assert.Equal(t, models.PaymentRefunded, f.payment(t).Status)
	assert.EqualValues(t, 1, f.countEvents(t, models.EventRefundCompleted),
		"completion must be announced so notifications can follow")
}

// At-least-once delivery guarantees this event arrives more than once. Refunding
// twice pays the customer money they never spent — the mirror image of the
// double charge, and just as wrong.
func TestDuplicateRefundEventsRefundOnce(t *testing.T) {
	f := chargedFixture(t)
	ctx := context.Background()
	payment := f.payment(t)
	ev := refundEvent(f.orderID, payment.ID.String())

	for i := 0; i < 4; i++ {
		require.NoError(t, f.payments.HandleRefund(ctx, ev))
	}

	_, _, refunds := f.provider.stats()
	assert.Equal(t, 1, refunds, "four deliveries, one refund")
	assert.EqualValues(t, 1, f.countEvents(t, models.EventRefundCompleted))
}

// Only a charge that actually landed can be refunded. A declined payment never
// took money, so refunding it would be inventing a payout.
func TestRefundSkipsPaymentsThatNeverSucceeded(t *testing.T) {
	f := newSagaFixture(t, services.OutcomeDeclined, 5)
	ctx := context.Background()

	require.NoError(t, f.payments.ProcessOrder(ctx, f.orderID))
	payment := f.payment(t)
	require.Equal(t, models.PaymentDeclined, payment.Status)

	require.NoError(t, f.payments.HandleRefund(ctx, refundEvent(f.orderID, payment.ID.String())))

	_, _, refunds := f.provider.stats()
	assert.Zero(t, refunds, "a declined charge must never be refunded")
	assert.Equal(t, models.PaymentDeclined, f.payment(t).Status)
}

// A SUCCEEDED payment with no provider reference means we never confirmed what
// the provider did with it. Guessing at the card network is worse than stopping:
// the error nacks the delivery, which dead-letters it for a human.
func TestRefundWithoutAProviderReferenceEscalates(t *testing.T) {
	f := chargedFixture(t)
	ctx := context.Background()
	payment := f.payment(t)

	require.NoError(t, f.db.Exec(
		`UPDATE payments SET provider_ref = NULL WHERE id = ?`, payment.ID).Error)

	err := f.payments.HandleRefund(ctx, refundEvent(f.orderID, payment.ID.String()))
	require.Error(t, err, "an unverifiable refund must escalate, not proceed")
	assert.Contains(t, err.Error(), "provider reference")

	_, _, refunds := f.provider.stats()
	assert.Zero(t, refunds)
	assert.Equal(t, models.PaymentSucceeded, f.payment(t).Status,
		"the payment must stay refundable once a human sorts it out")
}

// A provider that refuses the refund must not be recorded as refunded. The error
// retries and then dead-letters — money owed to a customer needs a person, not a
// silent success.
func TestRefundFailureLeavesThePaymentRefundable(t *testing.T) {
	f := chargedFixture(t)
	ctx := context.Background()
	payment := f.payment(t)

	f.provider.mu.Lock()
	f.provider.refundErr = errors.New("provider refused the refund")
	f.provider.mu.Unlock()

	err := f.payments.HandleRefund(ctx, refundEvent(f.orderID, payment.ID.String()))
	require.Error(t, err)

	assert.Equal(t, models.PaymentSucceeded, f.payment(t).Status,
		"a failed refund must not be recorded as done")
	assert.Zero(t, f.countEvents(t, models.EventRefundCompleted))
}

func TestRefundRejectsMalformedEvents(t *testing.T) {
	f := chargedFixture(t)
	ctx := context.Background()

	tests := []struct {
		name string
		data map[string]any
	}{
		{"missing paymentId", map[string]any{"amountCents": 100}},
		{"empty paymentId", map[string]any{"paymentId": ""}},
		{"malformed uuid", map[string]any{"paymentId": "not-a-uuid"}},
		{"wrong type", map[string]any{"paymentId": 12345}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev := models.NewOrderEvent(models.EventRefundRequested, f.orderID, tc.data)
			assert.Error(t, f.payments.HandleRefund(ctx, ev))
		})
	}

	_, _, refunds := f.provider.stats()
	assert.Zero(t, refunds, "a malformed event must not move money")
}

// An event naming a payment that does not exist is a bug or a stale replay, and
// must surface rather than be swallowed.
func TestRefundForAnUnknownPaymentErrors(t *testing.T) {
	f := chargedFixture(t)

	err := f.payments.HandleRefund(context.Background(),
		refundEvent(f.orderID, uuid.NewString()))
	require.Error(t, err)
	assert.ErrorIs(t, err, models.ErrNotFound)
}

// The full compensation, end to end: a cancel racing a live charge produces a
// refund request, and processing that request actually returns the money.
func TestCancelDuringChargeLeadsToAnExecutedRefund(t *testing.T) {
	f := newSagaFixture(t, services.OutcomeSucceeded, 5)
	ctx := context.Background()

	f.provider.gate = make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- f.payments.ProcessOrder(ctx, f.orderID) }()

	<-f.provider.started
	res, err := f.orders.Cancel(ctx, f.orderID, f.user, false)
	require.NoError(t, err)
	require.True(t, res.Pending)

	close(f.provider.gate)
	require.NoError(t, <-done)
	require.Equal(t, models.OrderCancelledRefunded, f.status(t))

	// The relay would carry this; here we hand it straight to the consumer.
	payment := f.payment(t)
	require.NoError(t, f.payments.HandleRefund(ctx, refundEvent(f.orderID, payment.ID.String())))

	charges, _, refunds := f.provider.stats()
	assert.Equal(t, 1, charges, "charged once")
	assert.Equal(t, 1, refunds, "and refunded once")
	assert.Equal(t, models.PaymentRefunded, f.payment(t).Status)

	// The customer is whole: stock back, money back.
	inv := f.inventory(t)
	assert.Equal(t, 5, inv.Available)
	assert.Equal(t, 0, inv.Reserved)
}
