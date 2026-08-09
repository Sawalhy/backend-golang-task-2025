package tests

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
	"github.com/Sawalhy/backend-golang-task-2025/internal/workers"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

// These tests cover the one hop the SSE tests cannot: RabbitMQ -> backplane ->
// hub. The SSE tests call hub.Publish directly, which proves the handler and the
// fan-out but says nothing about whether the queue is declared correctly, bound
// to the right pattern, or consumed at all.
//
// They need a real broker, so they skip when TEST_RABBITMQ_URL is unset rather
// than failing — a developer without RabbitMQ running should still get a green
// suite for everything else.
func requireBroker(t *testing.T) string {
	t.Helper()

	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("TEST_RABBITMQ_URL not set; skipping broker integration test")
	}
	return url
}

// awaitDelivery republishes ev until sub receives it, then returns what arrived.
//
// Sleeping for a fixed interval and publishing once is a race, not a wait: the
// consumer goroutine has to declare its queue and bind it before the exchange
// will route anything, and a topic exchange DISCARDS a message that matches no
// binding — silently, with no error to the publisher. Under any slowdown
// (coverage instrumentation, a loaded machine, CI) the publish beats the bind
// and the event is gone forever.
//
// Republishing the same envelope is safe: the event id is constant, so a
// consumer that receives several copies sees one logical event, which is exactly
// the at-least-once contract everything downstream already assumes.
func awaitDelivery(t *testing.T, pub *workers.Publisher, sub *services.Subscriber, ev models.Envelope, timeout time.Duration) models.Envelope {
	t.Helper()

	body, err := json.Marshal(ev)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	deadline := time.After(timeout)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()

	require.NoError(t, pub.PublishConfirmed(ctx, ev.EventType, ev.EventID.String(), body))

	for {
		select {
		case got := <-sub.Events():
			return got
		case <-tick.C:
			// The binding may not have existed yet; try again.
			if err := pub.PublishConfirmed(ctx, ev.EventType, ev.EventID.String(), body); err != nil {
				t.Fatalf("publishing %s: %v", ev.EventType, err)
			}
		case <-deadline:
			t.Fatalf("event %s never reached the hub: check the sse.<id> queue binding to order.#",
				ev.EventType)
			return models.Envelope{}
		}
	}
}

func TestBackplaneDeliversOrderEventsToHub(t *testing.T) {
	url := requireBroker(t)
	log := logger.New("error", false)

	broker, err := workers.Connect(url, "orders", log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = broker.Close() })

	require.NoError(t, broker.DeclareTopology())

	hub := services.NewStatusHub()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// The backplane runs until ctx is cancelled; cancelling is its exit path.
	started := make(chan struct{})
	go func() {
		close(started)
		_ = workers.RunOrderEventBackplane(ctx, broker, "test-"+t.Name(), hub.Publish, log)
	}()
	<-started

	sub := hub.Subscribe(4242)
	defer hub.Unsubscribe(sub)

	pub, err := broker.NewPublisher()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	ev := models.NewOrderEvent(models.EventOrderPaid, 4242, map[string]any{"totalCents": 999})
	got := awaitDelivery(t, pub, sub, ev, 20*time.Second)

	assert.Equal(t, models.EventOrderPaid, got.EventType)
	assert.Equal(t, uint64(4242), got.Aggregate.ID)
	assert.Equal(t, ev.EventID, got.EventID, "the event id must survive the round trip intact")
}

// Every API instance must see EVERY event. If the per-instance queues competed
// the way the payments queue does, an order.paid would reach one instance while
// the customer's connection sat on another, and that customer would never be
// told. This is the test that would catch someone "simplifying" the backplane
// into a shared durable queue.
func TestBackplaneQueuesDoNotCompete(t *testing.T) {
	url := requireBroker(t)
	log := logger.New("error", false)

	broker, err := workers.Connect(url, "orders", log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = broker.Close() })
	require.NoError(t, broker.DeclareTopology())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Two instances, exactly as two API replicas would be.
	hubA, hubB := services.NewStatusHub(), services.NewStatusHub()
	go func() { _ = workers.RunOrderEventBackplane(ctx, broker, "instance-a-"+t.Name(), hubA.Publish, log) }()
	go func() { _ = workers.RunOrderEventBackplane(ctx, broker, "instance-b-"+t.Name(), hubB.Publish, log) }()

	subA := hubA.Subscribe(777)
	defer hubA.Unsubscribe(subA)
	subB := hubB.Subscribe(777)
	defer hubB.Unsubscribe(subB)

	pub, err := broker.NewPublisher()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	// Republished until A has it, which also gives B's binding time to exist.
	ev := models.NewOrderEvent(models.EventOrderCancelled, 777, nil)
	gotA := awaitDelivery(t, pub, subA, ev, 20*time.Second)
	assert.Equal(t, uint64(777), gotA.Aggregate.ID)

	// B must receive the SAME event independently. If the queues competed, one
	// instance would get it and the other would wait here forever — which is a
	// customer whose browser is connected to the wrong replica never being told
	// their order was cancelled.
	select {
	case gotB := <-subB.Events():
		assert.Equal(t, uint64(777), gotB.Aggregate.ID)
		assert.Equal(t, gotA.EventID, gotB.EventID, "both instances see the same event")
	case <-time.After(20 * time.Second):
		t.Fatal("instance-b never received the event: the per-instance queues are competing")
	}
}

// order.# must match every order event but not payment.* — the SSE stream has no
// use for refund plumbing, and a binding of # would forward it anyway.
func TestBackplaneBindingScopesToOrderEvents(t *testing.T) {
	url := requireBroker(t)
	log := logger.New("error", false)

	broker, err := workers.Connect(url, "orders", log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = broker.Close() })
	require.NoError(t, broker.DeclareTopology())

	hub := services.NewStatusHub()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() { _ = workers.RunOrderEventBackplane(ctx, broker, "scope-"+t.Name(), hub.Publish, log) }()

	sub := hub.Subscribe(555)
	defer hub.Unsubscribe(sub)

	pub, err := broker.NewPublisher()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	// FIRST prove the backplane is actually live, by getting a matching event
	// through. Without this the negative assertion below passes vacuously: if
	// the queue were not yet bound, NOTHING would arrive and the test would
	// report success while proving nothing at all. A test that cannot fail for
	// the right reason is worse than no test.
	live := models.NewOrderEvent(models.EventOrderPaid, 555, nil)
	got := awaitDelivery(t, pub, sub, live, 20*time.Second)
	require.Equal(t, models.EventOrderPaid, got.EventType)

	// Drain any duplicates from the republishing above, so what follows is
	// unambiguous.
	drain(sub, 500*time.Millisecond)

	// Now the real assertion: payment.* is not order.*, so it must not route to
	// the SSE stream. The customer's status feed has no use for refund plumbing.
	refund := models.NewOrderEvent(models.EventRefundRequested, 555, nil)
	body, err := json.Marshal(refund)
	require.NoError(t, err)
	require.NoError(t, pub.PublishConfirmed(ctx, refund.EventType, refund.EventID.String(), body))

	select {
	case leaked := <-sub.Events():
		t.Fatalf("payment event leaked onto the order stream: %s", leaked.EventType)
	case <-time.After(3 * time.Second):
		// Correct: order.# does not match payment.refund_requested.
	}
}

// drain discards whatever is already buffered, so a later assertion about "no
// event arrived" is not confused by an earlier one.
func drain(sub *services.Subscriber, quiet time.Duration) {
	for {
		select {
		case <-sub.Events():
		case <-time.After(quiet):
			return
		}
	}
}
