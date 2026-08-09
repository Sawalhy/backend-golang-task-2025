package services

import (
	"sync"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
)

// StatusHub fans order events out to the SSE connections held by THIS API
// instance.
//
// It is the clearest example in the codebase of the rule for choosing a
// synchronisation primitive:
//
//	the subscriber map is SHARED STATE      -> sync.RWMutex
//	events MOVE from the broker to handlers -> channels
//
// Building a goroutine-plus-channel rig to own the map would be the classic
// misuse: a mutex is a lock, and this needs a lock. Reads (Publish walking the
// map) hugely outnumber writes (connect/disconnect), which is exactly what
// RWMutex is for.
//
// The hub is per-instance and deliberately not shared. Four API replicas each
// hold their own, and each is fed by its own exclusive RabbitMQ queue bound to
// `order.#`, so every instance sees every event and pushes to whichever clients
// it happens to be holding. No coordination between replicas, no sticky
// sessions, no shared state to keep consistent.
type StatusHub struct {
	mu   sync.RWMutex
	subs map[uint64]map[*Subscriber]struct{}
}

// Subscriber is one SSE connection's view of one order.
type Subscriber struct {
	// Buffered so Publish never blocks. Sized for a burst of the handful of
	// events one order can produce in quick succession.
	ch      chan models.Envelope
	orderID uint64
}

// Events is the stream the handler ranges over.
func (s *Subscriber) Events() <-chan models.Envelope { return s.ch }

func NewStatusHub() *StatusHub {
	return &StatusHub{subs: make(map[uint64]map[*Subscriber]struct{})}
}

// Subscribe registers interest in one order and starts buffering immediately.
//
// Callers must Subscribe BEFORE reading the order's current state. Reading first
// leaves a window between the read and the registration in which an event can
// fire and be missed forever — the client would then sit on a stale status with
// no further update coming. Subscribing first can only produce a duplicate,
// which the client can trivially ignore.
func (h *StatusHub) Subscribe(orderID uint64) *Subscriber {
	sub := &Subscriber{ch: make(chan models.Envelope, 16), orderID: orderID}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.subs[orderID] == nil {
		h.subs[orderID] = make(map[*Subscriber]struct{})
	}
	h.subs[orderID][sub] = struct{}{}
	return sub
}

// Unsubscribe removes a connection and closes its channel.
//
// Must be deferred the moment a Subscriber is obtained. A subscriber that is
// never removed keeps its entry in the map and its goroutine parked on a send —
// a leak that grows with every disconnected client.
func (h *StatusHub) Unsubscribe(sub *Subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()

	subs, ok := h.subs[sub.orderID]
	if !ok {
		return
	}
	if _, present := subs[sub]; !present {
		return // already removed; closing twice would panic
	}

	delete(subs, sub)
	close(sub.ch)

	// Drop the empty inner map, or the outer map grows without bound over the
	// lifetime of the process — one entry per order ever streamed.
	if len(subs) == 0 {
		delete(h.subs, sub.orderID)
	}
}

// Publish delivers an event to every subscriber of that order.
//
// The send is NON-BLOCKING, and that is the important part. A client on a slow
// connection whose buffer has filled must not be able to block this call,
// because Publish runs on the backplane consumer's goroutine — one stalled
// browser would stop event delivery for every other client on the instance.
//
// Dropping is safe here because the events are doorbells, not state: each one
// says "order 1001 changed, go look". A client that misses one still gets the
// next, and on reconnect the handler re-reads current state from Postgres. The
// authoritative answer always comes from the database, never from having
// received every message.
func (h *StatusHub) Publish(ev models.Envelope) {
	orderID := ev.Aggregate.ID
	if orderID == 0 {
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for sub := range h.subs[orderID] {
		select {
		case sub.ch <- ev:
		default:
			// Buffer full: drop rather than stall the fan-out.
		}
	}
}

// SubscriberCount reports live subscriptions for one order. Used by tests and
// worth exposing as a gauge.
func (h *StatusHub) SubscriberCount(orderID uint64) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs[orderID])
}

// Orders reports how many distinct orders currently have subscribers.
func (h *StatusHub) Orders() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}
