package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The state machine is the only thing standing between at-least-once delivery
// and corrupt orders, so its shape is asserted rather than assumed.

func TestLegalTransitions(t *testing.T) {
	legal := []struct{ from, to OrderStatus }{
		{OrderPending, OrderCharging},
		{OrderPending, OrderCancelled},
		{OrderPending, OrderExpired},
		{OrderCharging, OrderPaid},
		{OrderCharging, OrderFailed},
		{OrderCharging, OrderCancelling},
		{OrderCancelling, OrderCancelled},
		{OrderCancelling, OrderCancelledRefunded},
		{OrderPaid, OrderFulfilled},
		{OrderPaid, OrderRefunded},
	}

	for _, tc := range legal {
		assert.True(t, CanTransition(tc.from, tc.to), "%s -> %s should be legal", tc.from, tc.to)
	}
}

// The edges that would corrupt an order if they were ever reachable.
func TestIllegalTransitions(t *testing.T) {
	illegal := []struct {
		from, to OrderStatus
		why      string
	}{
		{OrderPaid, OrderPending, "an order cannot un-pay itself"},
		{OrderPending, OrderPaid, "payment must go through CHARGING, or money was never taken"},
		{OrderCancelled, OrderPaid, "charging a cancelled order is failure mode D"},
		{OrderExpired, OrderCharging, "reclaimed stock cannot be charged for"},
		{OrderFulfilled, OrderCancelling, "shipped goods cannot be cancelled"},
		{OrderFailed, OrderPaid, "a declined card does not become a payment"},
		{OrderPending, OrderPending, "a no-op transition would mask a lost race"},
	}

	for _, tc := range illegal {
		assert.False(t, CanTransition(tc.from, tc.to), "%s -> %s must be illegal: %s", tc.from, tc.to, tc.why)
	}
}

func TestTerminalStatuses(t *testing.T) {
	terminal := []OrderStatus{
		OrderCancelled, OrderCancelledRefunded, OrderExpired,
		OrderFailed, OrderFulfilled, OrderRefunded,
	}
	for _, s := range terminal {
		assert.True(t, TerminalOrderStatus(s), "%s should be terminal", s)
	}

	for _, s := range []OrderStatus{OrderPending, OrderCharging, OrderCancelling, OrderPaid} {
		assert.False(t, TerminalOrderStatus(s), "%s still has somewhere to go", s)
	}
}

// Every status must be reachable from PENDING, or it is dead vocabulary that
// will mislead the next person reading the state machine.
func TestEveryStatusIsReachable(t *testing.T) {
	reachable := map[OrderStatus]bool{OrderPending: true}

	for changed := true; changed; {
		changed = false
		for _, from := range AllOrderStatuses() {
			if !reachable[from] {
				continue
			}
			for _, to := range AllOrderStatuses() {
				if CanTransition(from, to) && !reachable[to] {
					reachable[to] = true
					changed = true
				}
			}
		}
	}

	for _, s := range AllOrderStatuses() {
		assert.True(t, reachable[s], "%s is unreachable from PENDING", s)
	}
}

func TestAllOrderStatusesIsComplete(t *testing.T) {
	all := AllOrderStatuses()
	assert.Len(t, all, 10)

	seen := map[OrderStatus]bool{}
	for _, s := range all {
		assert.False(t, seen[s], "%s listed twice", s)
		seen[s] = true
	}
}

// Table names are explicit because GORM's pluraliser is not something to bet a
// schema on, and a wrong one fails at runtime rather than compile time.
func TestTableNames(t *testing.T) {
	expected := map[string]string{
		User{}.TableName():             "users",
		Product{}.TableName():          "products",
		Inventory{}.TableName():        "inventory",
		Order{}.TableName():            "orders",
		OrderItem{}.TableName():        "order_items",
		Reservation{}.TableName():      "reservations",
		Payment{}.TableName():          "payments",
		Outbox{}.TableName():           "outbox",
		Notification{}.TableName():     "notifications",
		AuditLog{}.TableName():         "audit_logs",
		DailySalesRollup{}.TableName(): "daily_sales_rollup",
	}
	for got, want := range expected {
		assert.Equal(t, want, got)
	}
}

// The payment's own id IS the provider idempotency key. Exposed as a method so
// call sites read as intent rather than a field access someone could swap for
// something generated per attempt.
func TestPaymentIdempotencyKeyIsItsID(t *testing.T) {
	p := Payment{}
	require.Empty(t, p.ProviderRef)

	key := p.IdempotencyKey()
	assert.Equal(t, p.ID.String(), key)
	assert.Equal(t, key, p.IdempotencyKey(), "the key must never change between reads")
}

func TestNewOrderEvent(t *testing.T) {
	ev := NewOrderEvent(EventOrderPaid, 42, map[string]any{"totalCents": 100})

	assert.Equal(t, EventOrderPaid, ev.EventType)
	assert.Equal(t, "order", ev.Aggregate.Type)
	assert.EqualValues(t, 42, ev.Aggregate.ID)
	assert.NotEqual(t, ev.EventID, NewOrderEvent(EventOrderPaid, 42, nil).EventID,
		"each event needs its own dedupe id")
	assert.False(t, ev.OccurredAt.IsZero())
}

// Enums round-trip through database/sql as strings.
func TestEnumValueAndScan(t *testing.T) {
	v, err := OrderPaid.Value()
	require.NoError(t, err)
	assert.Equal(t, "PAID", v)

	var status OrderStatus
	require.NoError(t, status.Scan("CHARGING"))
	assert.Equal(t, OrderCharging, status)

	// Drivers hand back []byte as readily as string.
	require.NoError(t, status.Scan([]byte("PAID")))
	assert.Equal(t, OrderPaid, status)

	require.NoError(t, status.Scan(nil))
	assert.Equal(t, OrderStatus(""), status)

	assert.Error(t, status.Scan(42), "an unexpected type must not be silently coerced")

	var pay PaymentStatus
	require.NoError(t, pay.Scan("UNKNOWN"))
	assert.Equal(t, PaymentUnknown, pay)

	var role UserRole
	require.NoError(t, role.Scan("ADMIN"))
	assert.Equal(t, RoleAdmin, role)

	var res ReservationStatus
	require.NoError(t, res.Scan("HELD"))
	assert.Equal(t, ReservationHeld, res)

	var notif NotificationStatus
	require.NoError(t, notif.Scan("SENT"))
	assert.Equal(t, NotificationSent, notif)
}
