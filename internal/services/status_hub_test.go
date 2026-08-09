package services

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
)

func orderEvent(orderID uint64, eventType string) models.Envelope {
	return models.NewOrderEvent(eventType, orderID, nil)
}

func TestHubDeliversToSubscriber(t *testing.T) {
	hub := NewStatusHub()

	sub := hub.Subscribe(42)
	defer hub.Unsubscribe(sub)

	hub.Publish(orderEvent(42, models.EventOrderPaid))

	select {
	case ev := <-sub.Events():
		assert.Equal(t, models.EventOrderPaid, ev.EventType)
		assert.Equal(t, uint64(42), ev.Aggregate.ID)
	case <-time.After(time.Second):
		t.Fatal("expected the event to be delivered")
	}
}

// Every API instance receives every order's events off its own queue, so the hub
// is the only thing preventing one customer's stream from carrying another
// customer's orders.
func TestHubIsolatesOrders(t *testing.T) {
	hub := NewStatusHub()

	mine := hub.Subscribe(1)
	defer hub.Unsubscribe(mine)
	theirs := hub.Subscribe(2)
	defer hub.Unsubscribe(theirs)

	hub.Publish(orderEvent(2, models.EventOrderPaid))

	select {
	case ev := <-mine.Events():
		t.Fatalf("received an event for another order: %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}

	select {
	case ev := <-theirs.Events():
		assert.Equal(t, uint64(2), ev.Aggregate.ID)
	case <-time.After(time.Second):
		t.Fatal("the correct subscriber got nothing")
	}
}

// THE important property. Publish runs on the backplane consumer's goroutine, so
// if a slow client could block it, one stalled browser would stop event delivery
// for every other client on the instance.
func TestPublishNeverBlocksOnAFullSubscriber(t *testing.T) {
	hub := NewStatusHub()

	sub := hub.Subscribe(7)
	defer hub.Unsubscribe(sub)

	// Nothing ever reads from sub, so the buffer fills and then overflows.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10_000; i++ {
			hub.Publish(orderEvent(7, models.EventOrderPaid))
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Publish blocked on a subscriber that stopped reading")
	}

	// Dropping is correct: events are doorbells, and the handler re-reads
	// authoritative state from Postgres on every one it does receive.
	assert.NotEmpty(t, sub.Events(), "the buffered events should still be there")
}

func TestUnsubscribeClosesChannelAndReleasesMemory(t *testing.T) {
	hub := NewStatusHub()

	sub := hub.Subscribe(9)
	require.Equal(t, 1, hub.SubscriberCount(9))
	require.Equal(t, 1, hub.Orders())

	hub.Unsubscribe(sub)

	assert.Zero(t, hub.SubscriberCount(9))
	// The per-order map must be dropped too, or the outer map grows by one entry
	// for every order ever streamed and never shrinks.
	assert.Zero(t, hub.Orders(), "empty order entries must not accumulate")

	_, open := <-sub.Events()
	assert.False(t, open, "the channel should be closed so the handler loop exits")
}

// A handler can hit both its deferred Unsubscribe and an explicit one on an
// error path; closing a closed channel panics.
func TestDoubleUnsubscribeIsSafe(t *testing.T) {
	hub := NewStatusHub()
	sub := hub.Subscribe(11)

	hub.Unsubscribe(sub)
	assert.NotPanics(t, func() { hub.Unsubscribe(sub) })
}

func TestPublishToNobodyIsHarmless(t *testing.T) {
	hub := NewStatusHub()
	assert.NotPanics(t, func() { hub.Publish(orderEvent(404, models.EventOrderPaid)) })
	assert.NotPanics(t, func() { hub.Publish(orderEvent(0, models.EventOrderPaid)) })
}

// Run with -race. Connections open and close constantly while the backplane
// publishes, so the map is under genuine concurrent mutation and iteration.
func TestHubUnderConcurrentUse(t *testing.T) {
	hub := NewStatusHub()

	const (
		publishers  = 8
		subscribers = 8
		iterations  = 200
	)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					hub.Publish(orderEvent(uint64(id%4), models.EventOrderPaid))
				}
			}
		}(p)
	}

	for s := 0; s < subscribers; s++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				sub := hub.Subscribe(uint64(id % 4))
				select {
				case <-sub.Events():
				default:
				}
				hub.Unsubscribe(sub)
			}
		}(s)
	}

	// Let the subscriber churn finish, then stop the publishers.
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Every subscriber unsubscribed, so nothing may be left behind.
	assert.Zero(t, hub.Orders(), "subscriptions leaked after concurrent churn")
}
