package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/workers"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

func newRelayFixture(t *testing.T) (*repository.Store, *gorm.DB, *workers.Relay, *workers.Publisher) {
	t.Helper()

	url := requireBroker(t)
	store, db := newStore(t)
	log := logger.New("error", false)

	broker, err := workers.Connect(url, "orders", "test", log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = broker.Close() })
	require.NoError(t, broker.DeclareTopology())

	pub, err := broker.NewPublisher()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	return store, db, workers.NewRelay(store, pub, 50*time.Millisecond, 100, log), pub
}

func enqueue(t *testing.T, store *repository.Store, routingKey string, orderID uint64) models.Envelope {
	t.Helper()

	ev := models.NewOrderEvent(routingKey, orderID, nil)
	require.NoError(t, store.InTx(context.Background(), func(ctx context.Context, tx *gorm.DB) error {
		_, err := store.Outbox().Enqueue(ctx, tx, ev)
		return err
	}))
	return ev
}

func unsentCount(t *testing.T, store *repository.Store) int64 {
	t.Helper()
	n, err := store.Outbox().PendingCount(context.Background())
	require.NoError(t, err)
	return n
}

func TestRelayPublishesAndMarksSent(t *testing.T) {
	store, db, relay, _ := newRelayFixture(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		enqueue(t, store, models.EventOrderCreated, uint64(9000+i))
	}
	require.EqualValues(t, 3, unsentCount(t, store))

	published, err := relay.DrainOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, published)

	assert.Zero(t, unsentCount(t, store), "the backlog must be gone")

	// sent_at is what proves the row was acknowledged, not merely attempted.
	var withoutTimestamp int64
	require.NoError(t, db.Raw(
		`SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&withoutTimestamp).Error)
	assert.Zero(t, withoutTimestamp)
}

// THE property that makes the outbox trustworthy.
//
// A plain publish is a write to a socket buffer: it returns long before the
// broker has the message. Marking sent_at on the back of that means a broker
// crash silently loses an event the database believes was delivered — the outbox
// would be recording a lie.
//
// Here the publisher's channel is closed first, so every publish fails. Nothing
// may be marked sent, and the rows must remain claimable.
func TestRelayLeavesRowsUnsentWhenThePublishFails(t *testing.T) {
	store, _, relay, pub := newRelayFixture(t)
	ctx := context.Background()

	enqueue(t, store, models.EventOrderCreated, 9100)
	enqueue(t, store, models.EventOrderPaid, 9101)

	// Break the publisher: the broker can no longer confirm anything.
	require.NoError(t, pub.Close())

	published, err := relay.DrainOnce(ctx)
	require.NoError(t, err, "a broker failure must not kill the relay")
	assert.Zero(t, published, "nothing was confirmed, so nothing was published")

	assert.EqualValues(t, 2, unsentCount(t, store),
		"unconfirmed events must stay unsent so a later pass retries them")
}

// A failed publish bumps attempts, so a poison event is visible in metrics rather
// than being retried silently forever.
func TestRelayRecordsFailedAttempts(t *testing.T) {
	store, db, relay, pub := newRelayFixture(t)
	ctx := context.Background()

	enqueue(t, store, models.EventOrderCreated, 9200)
	require.NoError(t, pub.Close())

	_, err := relay.DrainOnce(ctx)
	require.NoError(t, err)

	var attempts int
	require.NoError(t, db.Raw(`SELECT attempts FROM outbox LIMIT 1`).Scan(&attempts).Error)
	assert.Positive(t, attempts, "a failed publish must be counted")
}

// Two relay instances must never publish the same event twice. FOR UPDATE SKIP
// LOCKED is what allows them to run concurrently: rows one instance holds are
// invisible to the other for the life of its transaction.
func TestTwoRelaysDoNotDoublePublish(t *testing.T) {
	store, db, relayA, _ := newRelayFixture(t)
	ctx := context.Background()

	url := requireBroker(t)
	log := logger.New("error", false)
	brokerB, err := workers.Connect(url, "orders", "test", log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = brokerB.Close() })
	pubB, err := brokerB.NewPublisher()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pubB.Close() })
	relayB := workers.NewRelay(store, pubB, 50*time.Millisecond, 100, log)

	const events = 25
	for i := 0; i < events; i++ {
		enqueue(t, store, models.EventOrderCreated, uint64(9300+i))
	}

	type result struct {
		n   int
		err error
	}
	done := make(chan result, 2)
	release := make(chan struct{})

	for _, r := range []*workers.Relay{relayA, relayB} {
		go func(relay *workers.Relay) {
			<-release
			n, err := relay.DrainOnce(ctx)
			done <- result{n, err}
		}(r)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)

	total := 0
	for i := 0; i < 2; i++ {
		res := <-done
		require.NoError(t, res.err)
		total += res.n
	}

	// The sum across both relays must equal the number of events: each row is
	// claimed by exactly one of them.
	assert.Equal(t, events, total, "an event claimed twice would be published twice")
	assert.Zero(t, unsentCount(t, store))

	var sent int64
	require.NoError(t, db.Raw(
		`SELECT count(*) FROM outbox WHERE sent_at IS NOT NULL`).Scan(&sent).Error)
	assert.EqualValues(t, events, sent)
}

// Draining an empty outbox is the common case — the relay polls every 500ms and
// almost always finds nothing.
func TestRelayOnEmptyOutboxIsANoop(t *testing.T) {
	store, _, relay, _ := newRelayFixture(t)

	published, err := relay.DrainOnce(context.Background())
	require.NoError(t, err)
	assert.Zero(t, published)
	assert.Zero(t, unsentCount(t, store))
}

// Lag reporting must work off the DATABASE, so it keeps reporting while the
// broker is down — which is exactly when a growing backlog matters.
func TestOutboxLagReflectsTheBacklog(t *testing.T) {
	store, _, _, _ := newRelayFixture(t)
	ctx := context.Background()

	assert.Zero(t, unsentCount(t, store))

	age, err := store.Outbox().OldestUnsentAge(ctx)
	require.NoError(t, err)
	assert.Zero(t, age, "nothing pending means no lag")

	enqueue(t, store, models.EventOrderCreated, 9400)

	assert.EqualValues(t, 1, unsentCount(t, store))
	age, err = store.Outbox().OldestUnsentAge(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, age, time.Duration(0))

	// Does not panic or error with a broker that was never contacted.
	workers.ReportOutboxLag(ctx, store, logger.New("error", false))
}
