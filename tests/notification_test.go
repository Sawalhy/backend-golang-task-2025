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

// countingNotifier records every send and can be made to fail deterministically.
//
// Counting SENDS is the point: failure mode I is "sent twice, or never", and
// both halves are invisible to a test that only checks the row's status.
type countingNotifier struct {
	mu       sync.Mutex
	channel  string
	sends    int
	failNext int  // fail this many sends before succeeding
	gate     chan struct{}
	started  chan struct{}
	once     sync.Once
}

func newCountingNotifier(channel string) *countingNotifier {
	return &countingNotifier{channel: channel, started: make(chan struct{})}
}

func (n *countingNotifier) Channel() string { return n.channel }

func (n *countingNotifier) Send(ctx context.Context, channel, kind string, orderID uint64) error {
	n.once.Do(func() { close(n.started) })

	n.mu.Lock()
	gate := n.gate
	n.mu.Unlock()

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	n.sends++
	if n.failNext > 0 {
		n.failNext--
		return errors.New("simulated transport failure")
	}
	return nil
}

func (n *countingNotifier) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.sends
}

func notificationsFor(t *testing.T, store *repository.Store, orderID uint64) []models.Notification {
	t.Helper()
	rows, err := store.Notifications().ListForOrder(context.Background(), orderID)
	require.NoError(t, err)
	return rows
}

// newNotifiableOrder creates an order and returns its id. The notification
// service only needs the order to exist and be readable.
func newNotifiableOrder(t *testing.T, store *repository.Store, db *gorm.DB, email string) uint64 {
	t.Helper()

	user := seedUser(t, db, email)
	product := mustProduct(t, store, "NOTIFY-"+email, 1000, 10)

	order, err := newOrderSvc(store).Create(context.Background(), services.CreateOrderInput{
		UserID: user,
		Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 1}},
	})
	require.NoError(t, err)
	return order.ID
}

func paidEvent(orderID uint64) models.Envelope {
	return models.NewOrderEvent(models.EventOrderPaid, orderID, nil)
}

// --- "never" ---------------------------------------------------------------

func TestNotificationIsSentAndRecorded(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "notify1@example.com")
	notifier := newCountingNotifier("email")
	svc := services.NewNotificationService(store, notifier, time.Minute, logger.New("error", false))

	require.NoError(t, svc.Handle(ctx, paidEvent(orderID)))

	assert.Equal(t, 1, notifier.count())

	rows := notificationsFor(t, store, orderID)
	require.Len(t, rows, 1)
	assert.Equal(t, models.NotificationSent, rows[0].Status)
	assert.Equal(t, "email", rows[0].Channel)
	assert.Equal(t, "confirmation", rows[0].Kind)
	assert.NotNil(t, rows[0].SentAt)
}

// --- "twice" ---------------------------------------------------------------

// At-least-once delivery guarantees the same order.paid arrives more than once.
// The unique index on (order_id, channel, kind) is what makes "one confirmation
// per order" an invariant rather than a hope.
func TestDuplicateEventsSendOneNotification(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "notify2@example.com")
	notifier := newCountingNotifier("email")
	svc := services.NewNotificationService(store, notifier, time.Minute, logger.New("error", false))

	for i := 0; i < 4; i++ {
		require.NoError(t, svc.Handle(ctx, paidEvent(orderID)))
	}

	assert.Equal(t, 1, notifier.count(), "four deliveries, one email")
	assert.Len(t, notificationsFor(t, store, orderID), 1)
}

// Two workers racing the same event. The lease is what stops both sending.
func TestConcurrentWorkersSendOneNotification(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "notify3@example.com")
	notifier := newCountingNotifier("email")
	log := logger.New("error", false)

	const workers = 8
	var wg sync.WaitGroup
	release := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			svc := services.NewNotificationService(store, notifier, time.Minute, log)
			<-release
			_ = svc.Handle(ctx, paidEvent(orderID))
		}()
	}
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()

	assert.Equal(t, 1, notifier.count(), "eight concurrent workers, one email")
	assert.Len(t, notificationsFor(t, store, orderID), 1)
}

// Email and SMS are SEPARATE rows, because they are separate jobs that must both
// happen. If they shared one row (or one queue) the customer would get an email
// or a text, never both.
func TestEmailAndSmsAreIndependent(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "notify4@example.com")
	log := logger.New("error", false)

	email := newCountingNotifier("email")
	sms := newCountingNotifier("sms")

	require.NoError(t, services.NewNotificationService(store, email, time.Minute, log).
		Handle(ctx, paidEvent(orderID)))
	require.NoError(t, services.NewNotificationService(store, sms, time.Minute, log).
		Handle(ctx, paidEvent(orderID)))

	assert.Equal(t, 1, email.count())
	assert.Equal(t, 1, sms.count(), "sms must not be suppressed by the email row")

	rows := notificationsFor(t, store, orderID)
	require.Len(t, rows, 2)

	channels := map[string]bool{}
	for _, r := range rows {
		channels[r.Channel] = true
		assert.Equal(t, models.NotificationSent, r.Status)
	}
	assert.True(t, channels["email"] && channels["sms"])
}

// --- leases ----------------------------------------------------------------

// A worker that dies mid-send leaves the row SENDING. Without an expiry it would
// stay that way forever and the customer would never be told anything — so the
// lease is what makes the work reclaimable.
func TestExpiredLeaseIsReclaimed(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "notify5@example.com")

	// Create the row and claim it with a lease that is already expired,
	// reproducing a worker that took the job and died.
	var notificationID uint64
	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		n, _, err := store.Notifications().Ensure(ctx, tx, orderID, "email", "confirmation")
		if err != nil {
			return err
		}
		notificationID = n.ID
		ok, err := store.Notifications().Claim(ctx, tx, n.ID, -time.Minute) // already expired
		require.True(t, ok)
		return err
	}))

	rows := notificationsFor(t, store, orderID)
	require.Equal(t, models.NotificationSending, rows[0].Status)

	reclaimed, err := store.Notifications().ReleaseExpiredLeases(ctx, 100)
	require.NoError(t, err)
	assert.EqualValues(t, 1, reclaimed)

	rows = notificationsFor(t, store, orderID)
	assert.Equal(t, models.NotificationUnclaimed, rows[0].Status,
		"an abandoned send must become claimable again")
	assert.Nil(t, rows[0].LeaseUntil)

	// And a fresh worker then delivers it.
	notifier := newCountingNotifier("email")
	svc := services.NewNotificationService(store, notifier, time.Minute, logger.New("error", false))
	require.NoError(t, svc.Handle(ctx, paidEvent(orderID)))

	assert.Equal(t, 1, notifier.count())
	rows = notificationsFor(t, store, orderID)
	assert.Equal(t, models.NotificationSent, rows[0].Status)
	_ = notificationID
}

// A LIVE lease must not be stolen, or two workers send the same email.
func TestLiveLeaseIsNotReclaimed(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "notify6@example.com")

	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		n, _, err := store.Notifications().Ensure(ctx, tx, orderID, "email", "confirmation")
		if err != nil {
			return err
		}
		ok, err := store.Notifications().Claim(ctx, tx, n.ID, time.Hour) // long, live lease
		require.True(t, ok)
		return err
	}))

	reclaimed, err := store.Notifications().ReleaseExpiredLeases(ctx, 100)
	require.NoError(t, err)
	assert.Zero(t, reclaimed, "a live lease belongs to whoever holds it")

	// A second worker arriving now must find nothing to do.
	notifier := newCountingNotifier("email")
	svc := services.NewNotificationService(store, notifier, time.Minute, logger.New("error", false))
	require.NoError(t, svc.Handle(ctx, paidEvent(orderID)))
	assert.Zero(t, notifier.count(), "must not send while another worker holds the lease")
}

// The service's own sweep, which is what the worker runs on a timer.
func TestSweepExpiredLeasesReclaimsAbandonedSends(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "notify7@example.com")

	require.NoError(t, store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		n, _, err := store.Notifications().Ensure(ctx, tx, orderID, "email", "confirmation")
		if err != nil {
			return err
		}
		_, err = store.Notifications().Claim(ctx, tx, n.ID, -time.Minute)
		return err
	}))

	notifier := newCountingNotifier("email")
	svc := services.NewNotificationService(store, notifier, time.Minute, logger.New("error", false))

	require.NoError(t, svc.SweepExpiredLeases(ctx))

	rows := notificationsFor(t, store, orderID)
	assert.Equal(t, models.NotificationUnclaimed, rows[0].Status)
}

// --- retries ---------------------------------------------------------------

// A transport failure must not lose the notification: the row goes back to
// UNCLAIMED so a later attempt picks it up.
func TestFailedSendBecomesClaimableAgain(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "notify8@example.com")

	notifier := newCountingNotifier("email")
	notifier.failNext = 1
	svc := services.NewNotificationService(store, notifier, time.Minute, logger.New("error", false))

	require.NoError(t, svc.Handle(ctx, paidEvent(orderID)))

	rows := notificationsFor(t, store, orderID)
	require.Len(t, rows, 1)
	assert.Equal(t, models.NotificationUnclaimed, rows[0].Status,
		"a failed send must be retried, not dropped")
	assert.Equal(t, 1, rows[0].Attempts)
	require.NotNil(t, rows[0].LastError)

	// The retry succeeds.
	require.NoError(t, svc.Handle(ctx, paidEvent(orderID)))

	rows = notificationsFor(t, store, orderID)
	assert.Equal(t, models.NotificationSent, rows[0].Status)
	assert.Equal(t, 2, notifier.count(), "one failure plus one success")
}

// A permanently broken transport must eventually stop, or a poisoned row is
// retried forever and looks like a busy worker.
func TestSendGivesUpAfterTheAttemptBudget(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "notify9@example.com")

	notifier := newCountingNotifier("email")
	notifier.failNext = 99 // never succeeds
	svc := services.NewNotificationService(store, notifier, time.Minute, logger.New("error", false))

	for i := 0; i < 6; i++ {
		require.NoError(t, svc.Handle(ctx, paidEvent(orderID)))
	}

	rows := notificationsFor(t, store, orderID)
	require.Len(t, rows, 1)
	assert.Equal(t, models.NotificationFailed, rows[0].Status,
		"the attempt budget must be enforced")
	assert.GreaterOrEqual(t, rows[0].Attempts, 5)
}

// Events that are not notifiable must not create rows.
func TestNonNotifiableEventsAreIgnored(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "notify10@example.com")
	notifier := newCountingNotifier("email")
	svc := services.NewNotificationService(store, notifier, time.Minute, logger.New("error", false))

	require.NoError(t, svc.Handle(ctx, models.NewOrderEvent(models.EventOrderCreated, orderID, nil)))

	assert.Zero(t, notifier.count())
	assert.Empty(t, notificationsFor(t, store, orderID))
}

// A cancellation produces a different KIND, so it coexists with a confirmation
// rather than being suppressed by the unique index.
func TestDifferentKindsCoexist(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "notify11@example.com")
	notifier := newCountingNotifier("email")
	svc := services.NewNotificationService(store, notifier, time.Minute, logger.New("error", false))

	require.NoError(t, svc.Handle(ctx, paidEvent(orderID)))
	require.NoError(t, svc.Handle(ctx, models.NewOrderEvent(models.EventOrderCancelled, orderID, nil)))

	rows := notificationsFor(t, store, orderID)
	require.Len(t, rows, 2)
	assert.Equal(t, 2, notifier.count())

	kinds := map[string]bool{}
	for _, r := range rows {
		kinds[r.Kind] = true
	}
	assert.True(t, kinds["confirmation"] && kinds["cancellation"])
}

// --- the retry sweep --------------------------------------------------------

// notificationForChannel reads the one notification for an order and channel.
// The sweep tests need to watch two channels move independently, which
// notificationsFor's slice makes awkward.
func notificationForChannel(t *testing.T, store *repository.Store, orderID uint64, channel string) models.Notification {
	t.Helper()
	for _, r := range notificationsFor(t, store, orderID) {
		if r.Channel == channel {
			return r
		}
	}
	t.Fatalf("no %s notification for order %d", channel, orderID)
	return models.Notification{}
}

// Failure mode I, the "never" half.
//
// TestFailedSendBecomesClaimableAgain above retries by handling a second
// paidEvent, which re-claims the row by id. Production gets no such second
// delivery: the broker message was acked the moment the handler returned. The
// sweep is the only thing that can find the row again, and before it existed
// the row sat at UNCLAIMED forever and the customer was simply never told.
func TestFailedNotificationIsRetriedUntilItSends(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "notify12@example.com")

	notifier := newCountingNotifier("email")
	notifier.failNext = 1
	svc := services.NewNotificationService(store, notifier, time.Minute, logger.New("error", false))

	// The event path: one send, and it fails.
	require.NoError(t, svc.Handle(ctx, paidEvent(orderID)))

	row := notificationForChannel(t, store, orderID, "email")
	require.Equal(t, models.NotificationUnclaimed, row.Status,
		"a failed send must leave the row claimable, not SENDING")
	require.Equal(t, 1, row.Attempts)
	require.Equal(t, 1, notifier.count())

	// The sweep, with no second delivery anywhere in sight.
	n, err := svc.RetryFailed(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "the sweep must claim the abandoned row")

	row = notificationForChannel(t, store, orderID, "email")
	assert.Equal(t, models.NotificationSent, row.Status, "the customer must eventually be told")
	assert.Equal(t, 2, row.Attempts)
	assert.Equal(t, 2, notifier.count())
	assert.NotNil(t, row.SentAt)
}

// The budget has to be finite through the sweep too, or a permanently broken
// transport is retried forever and the row never becomes visible as a problem.
func TestNotificationRetriesStopAtTheAttemptBudget(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "notify13@example.com")

	notifier := newCountingNotifier("email")
	notifier.failNext = 99 // never recovers
	svc := services.NewNotificationService(store, notifier, time.Minute, logger.New("error", false))

	require.NoError(t, svc.Handle(ctx, paidEvent(orderID)))

	// Four more attempts spends the budget of five.
	for i := 0; i < 4; i++ {
		claimed, err := svc.RetryFailed(ctx, 10)
		require.NoError(t, err)
		require.Equal(t, 1, claimed, "attempt %d should still be claimable", i+2)
	}

	row := notificationForChannel(t, store, orderID, "email")
	assert.Equal(t, models.NotificationFailed, row.Status)
	assert.Equal(t, 5, row.Attempts)
	assert.Equal(t, 5, notifier.count())
	require.NotNil(t, row.LastError)

	// And it must stay out of the pool rather than being picked up forever.
	claimed, err := svc.RetryFailed(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 0, claimed, "a spent row must not be claimed again")
	assert.Equal(t, 5, notifier.count(), "and must not be sent again")
}

// The sweep is per-channel because a service owns exactly one transport. Without
// the channel filter the email worker claims SMS rows and sends them down the
// wrong pipe — and the SMS worker then finds nothing to do.
func TestRetrySweepClaimsOnlyItsOwnChannel(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	orderID := newNotifiableOrder(t, store, db, "notify14@example.com")

	emailNotifier := newCountingNotifier("email")
	emailNotifier.failNext = 1
	smsNotifier := newCountingNotifier("sms")
	smsNotifier.failNext = 1

	log := logger.New("error", false)
	emailSvc := services.NewNotificationService(store, emailNotifier, time.Minute, log)
	smsSvc := services.NewNotificationService(store, smsNotifier, time.Minute, log)

	require.NoError(t, emailSvc.Handle(ctx, paidEvent(orderID)))
	require.NoError(t, smsSvc.Handle(ctx, paidEvent(orderID)))

	// Two separate rows, both failed, both waiting.
	require.Equal(t, models.NotificationUnclaimed, notificationForChannel(t, store, orderID, "email").Status)
	require.Equal(t, models.NotificationUnclaimed, notificationForChannel(t, store, orderID, "sms").Status)

	claimed, err := emailSvc.RetryFailed(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, claimed, "email sweep claims exactly its own row")

	assert.Equal(t, models.NotificationSent, notificationForChannel(t, store, orderID, "email").Status)
	assert.Equal(t, models.NotificationUnclaimed, notificationForChannel(t, store, orderID, "sms").Status,
		"the SMS row must be untouched by the email sweep")
	assert.Equal(t, 1, smsNotifier.count(), "the SMS transport must not have been used")

	claimed, err = smsSvc.RetryFailed(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, claimed)
	assert.Equal(t, models.NotificationSent, notificationForChannel(t, store, orderID, "sms").Status)
}
