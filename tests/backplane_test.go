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

	// Give the consumer time to declare and bind before publishing; a message
	// published to an exchange with no matching binding is silently discarded,
	// which would make this test flaky rather than failing.
	time.Sleep(500 * time.Millisecond)

	sub := hub.Subscribe(4242)
	defer hub.Unsubscribe(sub)

	pub, err := broker.NewPublisher()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	ev := models.NewOrderEvent(models.EventOrderPaid, 4242, map[string]any{"totalCents": 999})
	body, err := json.Marshal(ev)
	require.NoError(t, err)

	require.NoError(t, pub.PublishConfirmed(ctx, ev.EventType, ev.EventID.String(), body))

	select {
	case got := <-sub.Events():
		assert.Equal(t, models.EventOrderPaid, got.EventType)
		assert.Equal(t, uint64(4242), got.Aggregate.ID)
		assert.Equal(t, ev.EventID, got.EventID, "the event id must survive the round trip intact")
	case <-time.After(10 * time.Second):
		t.Fatal("event never reached the hub: check the sse.<id> queue binding to order.#")
	}
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
	time.Sleep(800 * time.Millisecond)

	subA := hubA.Subscribe(777)
	defer hubA.Unsubscribe(subA)
	subB := hubB.Subscribe(777)
	defer hubB.Unsubscribe(subB)

	pub, err := broker.NewPublisher()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	ev := models.NewOrderEvent(models.EventOrderCancelled, 777, nil)
	body, err := json.Marshal(ev)
	require.NoError(t, err)
	require.NoError(t, pub.PublishConfirmed(ctx, ev.EventType, ev.EventID.String(), body))

	for name, sub := range map[string]*services.Subscriber{"instance-a": subA, "instance-b": subB} {
		select {
		case got := <-sub.Events():
			assert.Equal(t, uint64(777), got.Aggregate.ID)
		case <-time.After(10 * time.Second):
			t.Fatalf("%s never received the event: the per-instance queues are competing", name)
		}
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
	time.Sleep(500 * time.Millisecond)

	sub := hub.Subscribe(555)
	defer hub.Unsubscribe(sub)

	pub, err := broker.NewPublisher()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	// Not an order.* routing key, so it must not reach the stream.
	refund := models.NewOrderEvent(models.EventRefundRequested, 555, nil)
	body, err := json.Marshal(refund)
	require.NoError(t, err)
	require.NoError(t, pub.PublishConfirmed(ctx, refund.EventType, refund.EventID.String(), body))

	select {
	case got := <-sub.Events():
		t.Fatalf("payment event leaked onto the order stream: %s", got.EventType)
	case <-time.After(2 * time.Second):
		// Correct: order.# does not match payment.refund_requested.
	}
}
