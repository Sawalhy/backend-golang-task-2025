package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/workers"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

// The consumer machinery: delivery, ack/nack policy, and the bounded pool.
//
// These need a live broker but nothing killed — a message is published, the
// consumer handles it, and the acknowledgement is observed. That is a different
// and much cheaper thing than the supervisor tests, which need the connection to
// die underneath a running session.

// consumerFixture declares a private queue bound to a unique routing key, so
// tests neither collide with each other nor with the real topology.
type consumerFixture struct {
	broker  *workers.Broker
	pub     *workers.Publisher
	queue   string
	routing string
}

func newConsumerFixture(t *testing.T) *consumerFixture {
	t.Helper()

	url := requireBroker(t)
	log := logger.New("error", false)

	broker, err := workers.Connect(url, "orders", "consumer-test", log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = broker.Close() })
	require.NoError(t, broker.DeclareTopology())

	// A queue of our own, so a stray message from the real topology cannot make a
	// test pass or fail for the wrong reason. Declared with a raw connection
	// rather than through Broker: production code has no business growing a
	// "make me a scratch queue" method for the sake of tests.
	unique := fmt.Sprintf("%d", time.Now().UnixNano())
	queue := "test.consumer." + unique
	routing := "order.testevent." + unique

	declareScratchQueue(t, url, queue, routing)
	t.Cleanup(func() { deleteScratchQueue(url, queue) })

	pub, err := broker.NewPublisher()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	return &consumerFixture{broker: broker, pub: pub, queue: queue, routing: routing}
}

func (f *consumerFixture) publish(t *testing.T, ev models.Envelope) {
	t.Helper()
	body, err := json.Marshal(ev)
	require.NoError(t, err)
	require.NoError(t, f.pub.PublishConfirmed(
		context.Background(), f.routing, ev.EventID.String(), body))
}

// publishRaw sends a body that is not a valid envelope.
func (f *consumerFixture) publishRaw(t *testing.T, body []byte) {
	t.Helper()
	require.NoError(t, f.pub.PublishConfirmed(
		context.Background(), f.routing, "raw", body))
}

// run starts a consumer and returns a cancel func. The consumer exits when the
// context is cancelled, which is its guaranteed exit path.
func (f *consumerFixture) run(t *testing.T, prefetch, concurrency int, h workers.Handler) context.CancelFunc {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		_ = workers.RunConsumer(ctx, f.broker, f.queue, prefetch, concurrency, h, logger.New("error", false))
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("consumer did not exit after cancellation: a goroutine with no exit path is a leak")
		}
	})
	return cancel
}

func TestConsumerDeliversEventsToTheHandler(t *testing.T) {
	f := newConsumerFixture(t)

	received := make(chan models.Envelope, 4)
	f.run(t, 4, 2, func(ctx context.Context, ev models.Envelope) error {
		received <- ev
		return nil
	})

	ev := models.NewOrderEvent(models.EventOrderPaid, 1234, map[string]any{"totalCents": 500})
	f.publish(t, ev)

	select {
	case got := <-received:
		assert.Equal(t, ev.EventID, got.EventID)
		assert.EqualValues(t, 1234, got.Aggregate.ID)
	case <-time.After(15 * time.Second):
		t.Fatal("the handler was never called")
	}
}

// A handler returning nil acks, so the message must not come back. Acking BEFORE
// the work would mean a crash mid-handler loses the job (failure mode E), so the
// ack is deliberately last.
func TestSuccessfulHandlingAcksTheMessage(t *testing.T) {
	f := newConsumerFixture(t)

	var calls int64
	f.run(t, 4, 1, func(ctx context.Context, ev models.Envelope) error {
		atomic.AddInt64(&calls, 1)
		return nil
	})

	f.publish(t, models.NewOrderEvent(models.EventOrderPaid, 1, nil))

	require.Eventually(t, func() bool { return atomic.LoadInt64(&calls) == 1 },
		15*time.Second, 100*time.Millisecond, "the handler should run once")

	// If it had been nacked-and-requeued, the count would keep climbing.
	time.Sleep(2 * time.Second)
	assert.EqualValues(t, 1, atomic.LoadInt64(&calls),
		"an acked message must not be redelivered")
}

// A failing handler gets ONE retry, then the message is dead-lettered. Requeuing
// forever on a message that always fails is an infinite loop that looks like a
// busy worker; the Redelivered flag bounds it without needing a counter.
func TestFailingHandlerRetriesOnceThenDeadLetters(t *testing.T) {
	f := newConsumerFixture(t)

	var calls int64
	f.run(t, 4, 1, func(ctx context.Context, ev models.Envelope) error {
		atomic.AddInt64(&calls, 1)
		return errors.New("handler always fails")
	})

	f.publish(t, models.NewOrderEvent(models.EventOrderPaid, 2, nil))

	// First delivery plus one requeued redelivery.
	require.Eventually(t, func() bool { return atomic.LoadInt64(&calls) >= 2 },
		20*time.Second, 100*time.Millisecond, "a failure should be retried once")

	// And then it stops: the second failure nacks without requeue.
	time.Sleep(3 * time.Second)
	assert.EqualValues(t, 2, atomic.LoadInt64(&calls),
		"a poison message must stop after one retry, not loop forever")
}

// A body that is not a valid envelope will never become one, so it goes straight
// to the dead-letter queue rather than round-tripping forever.
func TestUnparseableMessagesAreDiscardedImmediately(t *testing.T) {
	f := newConsumerFixture(t)

	var calls int64
	f.run(t, 4, 1, func(ctx context.Context, ev models.Envelope) error {
		atomic.AddInt64(&calls, 1)
		return nil
	})

	f.publishRaw(t, []byte("this is not json"))

	// Give it time to be delivered and discarded.
	time.Sleep(3 * time.Second)
	assert.Zero(t, atomic.LoadInt64(&calls),
		"the handler must never see a message that could not be parsed")

	// A valid message afterwards still works — the consumer is not wedged.
	f.publish(t, models.NewOrderEvent(models.EventOrderPaid, 3, nil))
	require.Eventually(t, func() bool { return atomic.LoadInt64(&calls) == 1 },
		15*time.Second, 100*time.Millisecond,
		"a bad message must not poison the consumer for good ones")
}

// SetLimit is what stops a backlog becoming an unbounded fan-out that exhausts
// the database pool. Concurrency 2 must never run three handlers at once.
func TestConcurrencyIsBounded(t *testing.T) {
	f := newConsumerFixture(t)

	const limit = 2
	var (
		mu      sync.Mutex
		inFlight int
		peak     int
	)

	f.run(t, 10, limit, func(ctx context.Context, ev models.Envelope) error {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		mu.Unlock()

		time.Sleep(300 * time.Millisecond) // hold the slot

		mu.Lock()
		inFlight--
		mu.Unlock()
		return nil
	})

	for i := 0; i < 10; i++ {
		f.publish(t, models.NewOrderEvent(models.EventOrderPaid, uint64(100+i), nil))
	}

	time.Sleep(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	assert.LessOrEqual(t, peak, limit,
		"more than %d concurrent handlers means SetLimit is not bounding the pool", limit)
	assert.Positive(t, peak, "the consumer should have done some work")
}

// Cancelling the context must drain in-flight work rather than abandoning it
// mid-charge, and the goroutine must actually exit. The fixture's cleanup
// asserts the exit; this asserts the drain.
func TestCancellationWaitsForInFlightWork(t *testing.T) {
	f := newConsumerFixture(t)

	started := make(chan struct{})
	var finished int64

	cancel := f.run(t, 4, 1, func(ctx context.Context, ev models.Envelope) error {
		select {
		case <-started:
		default:
			close(started)
		}
		time.Sleep(500 * time.Millisecond)
		atomic.AddInt64(&finished, 1)
		return nil
	})

	f.publish(t, models.NewOrderEvent(models.EventOrderPaid, 999, nil))

	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never started")
	}

	cancel()

	// The handler was mid-flight when cancellation arrived; it must complete.
	require.Eventually(t, func() bool { return atomic.LoadInt64(&finished) == 1 },
		10*time.Second, 50*time.Millisecond,
		"in-flight work must finish rather than being abandoned")
}

// PaymentHandler adapts a service call onto the Handler signature, and must
// reject an event carrying no aggregate id rather than charging order zero.
func TestPaymentHandlerRequiresAnOrderID(t *testing.T) {
	var seen uint64
	h := workers.PaymentHandler(func(ctx context.Context, orderID uint64) error {
		seen = orderID
		return nil
	})

	require.NoError(t, h(context.Background(),
		models.NewOrderEvent(models.EventOrderCreated, 77, nil)))
	assert.EqualValues(t, 77, seen)

	err := h(context.Background(), models.Envelope{EventType: models.EventOrderCreated})
	require.Error(t, err, "an event with no order id must not reach the payment service")
	assert.Contains(t, err.Error(), "order id")
}

// The relay's ticker loop, as opposed to DrainOnce which the relay tests drive
// directly. Run must pick up work without being told.
func TestRelayRunDrainsOnItsTicker(t *testing.T) {
	store, _, relay, _ := newRelayFixture(t)

	enqueue(t, store, models.EventOrderCreated, 8800)
	require.EqualValues(t, 1, unsentCount(t, store))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = relay.Run(ctx)
	}()

	require.Eventually(t, func() bool { return unsentCount(t, store) == 0 },
		15*time.Second, 200*time.Millisecond,
		"the relay loop should drain the outbox without prompting")

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("relay did not stop when its context was cancelled")
	}
}

// --- scratch queue plumbing -------------------------------------------------

func declareScratchQueue(t *testing.T, url, queue, routingKey string) {
	t.Helper()

	conn, err := amqp.Dial(url)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()

	// Non-durable and not auto-delete: it must survive a consumer disconnecting
	// mid-test, and cleanup removes it explicitly rather than by side effect.
	_, err = ch.QueueDeclare(queue, false, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(queue, routingKey, "orders", false, nil))
}

func deleteScratchQueue(url, queue string) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		return
	}
	defer func() { _ = ch.Close() }()

	_, _ = ch.QueueDelete(queue, false, false, false)
}
