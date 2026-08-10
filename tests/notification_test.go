package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

// flakyNotifier fails its first failFirst sends, then succeeds. Counting calls
// is what lets a test assert how many attempts a delivery actually took, rather
// than merely that it ended up SENT.
type flakyNotifier struct {
	channel   string
	failFirst int

	mu    sync.Mutex
	calls int
}

func (n *flakyNotifier) Channel() string { return n.channel }

func (n *flakyNotifier) Send(ctx context.Context, channel, kind string, orderID uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls++
	if n.calls <= n.failFirst {
		return fmt.Errorf("simulated %s outage", channel)
	}
	return nil
}

func (n *flakyNotifier) callCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.calls
}

func newNotifySvc(store *repository.Store, n services.Notifier) *services.NotificationService {
	return services.NewNotificationService(store, n, time.Minute, logger.New("error", false))
}

// notificationRow reads the single notification for an order and channel.
func notificationRow(t *testing.T, store *repository.Store, orderID uint64, channel string) models.Notification {
	t.Helper()
	rows, err := store.Notifications().ListForOrder(context.Background(), orderID)
	require.NoError(t, err)
	for _, r := range rows {
		if r.Channel == channel {
			return r
		}
	}
	t.Fatalf("no %s notification for order %d", channel, orderID)
	return models.Notification{}
}

// anOrder creates a PENDING order to hang notifications off. Notifications only
// need the foreign key to resolve; nothing here cares about the order's state.
func anOrder(t *testing.T, store *repository.Store, sku, email string) uint64 {
	t.Helper()
	user := seedUser(t, store.DB(), email)
	product := mustProduct(t, store, sku, 2500, 10)

	order, err := newOrderSvc(store).Create(context.Background(), services.CreateOrderInput{
		UserID: user,
		Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 1}},
	})
	require.NoError(t, err)
	return order.ID
}

// Failure mode I, the "never" half.
//
// A send that fails returns the row to UNCLAIMED — but the broker delivery that
// produced it was acked the moment the handler returned, so no redelivery is
// coming. Before RetryFailed existed the row sat there forever and the customer
// was simply never told.
func TestFailedNotificationIsRetriedUntilItSends(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	orderID := anOrder(t, store, "NOTIFY-RETRY", "retry@example.com")
	notifier := &flakyNotifier{channel: "email", failFirst: 1}
	svc := newNotifySvc(store, notifier)

	// The event path: one send, and it fails.
	require.NoError(t, svc.Handle(ctx, models.NewOrderEvent(models.EventOrderPaid, orderID, nil)))

	row := notificationRow(t, store, orderID, "email")
	require.Equal(t, models.NotificationUnclaimed, row.Status,
		"a failed send must leave the row claimable, not SENDING")
	require.Equal(t, 1, row.Attempts)
	require.Equal(t, 1, notifier.callCount())

	// The sweep is the only thing that can find it again.
	n, err := svc.RetryFailed(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "the sweep must claim the abandoned row")

	row = notificationRow(t, store, orderID, "email")
	assert.Equal(t, models.NotificationSent, row.Status, "the customer must eventually be told")
	assert.Equal(t, 2, row.Attempts)
	assert.Equal(t, 2, notifier.callCount())
	assert.NotNil(t, row.SentAt)
}

// The budget has to be finite, or a permanently broken transport is retried
// forever and the row never becomes visible as a problem.
func TestNotificationRetriesStopAtTheAttemptBudget(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	orderID := anOrder(t, store, "NOTIFY-BUDGET", "budget@example.com")
	notifier := &flakyNotifier{channel: "email", failFirst: 100} // never recovers
	svc := newNotifySvc(store, notifier)

	require.NoError(t, svc.Handle(ctx, models.NewOrderEvent(models.EventOrderPaid, orderID, nil)))

	// Four more attempts spends the budget of five.
	for i := 0; i < 4; i++ {
		claimed, err := svc.RetryFailed(ctx, 10)
		require.NoError(t, err)
		require.Equal(t, 1, claimed, "attempt %d should still be claimable", i+2)
	}

	row := notificationRow(t, store, orderID, "email")
	assert.Equal(t, models.NotificationFailed, row.Status)
	assert.Equal(t, 5, row.Attempts)
	assert.Equal(t, 5, notifier.callCount())
	require.NotNil(t, row.LastError)

	// And it must stay out of the pool rather than being picked up forever.
	claimed, err := svc.RetryFailed(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 0, claimed, "a spent row must not be claimed again")
	assert.Equal(t, 5, notifier.callCount(), "and must not be sent again")
}

// The sweep is per-channel because a service owns exactly one transport. Without
// the channel filter the email worker claims SMS rows and sends them down the
// wrong pipe — and the SMS worker then finds nothing to do.
func TestRetrySweepClaimsOnlyItsOwnChannel(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	orderID := anOrder(t, store, "NOTIFY-CHANNEL", "channel@example.com")

	emailNotifier := &flakyNotifier{channel: "email", failFirst: 1}
	smsNotifier := &flakyNotifier{channel: "sms", failFirst: 1}
	emailSvc := newNotifySvc(store, emailNotifier)
	smsSvc := newNotifySvc(store, smsNotifier)

	paid := models.NewOrderEvent(models.EventOrderPaid, orderID, nil)
	require.NoError(t, emailSvc.Handle(ctx, paid))
	require.NoError(t, smsSvc.Handle(ctx, paid))

	// Two separate rows, both failed, both waiting.
	require.Equal(t, models.NotificationUnclaimed, notificationRow(t, store, orderID, "email").Status)
	require.Equal(t, models.NotificationUnclaimed, notificationRow(t, store, orderID, "sms").Status)

	claimed, err := emailSvc.RetryFailed(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, claimed, "email sweep claims exactly its own row")

	assert.Equal(t, models.NotificationSent, notificationRow(t, store, orderID, "email").Status)
	assert.Equal(t, models.NotificationUnclaimed, notificationRow(t, store, orderID, "sms").Status,
		"the SMS row must be untouched by the email sweep")
	assert.Equal(t, 1, smsNotifier.callCount(), "the SMS transport must not have been used")

	claimed, err = smsSvc.RetryFailed(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, claimed)
	assert.Equal(t, models.NotificationSent, notificationRow(t, store, orderID, "sms").Status)
}
