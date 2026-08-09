package workers

import (
	"context"
	"log/slog"
	"math/rand"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// NotifyClose reports connection loss. amqp091 closes this channel with the
// reason when the TCP connection or the AMQP session goes away.
func (b *Broker) NotifyClose() chan *amqp.Error {
	return b.conn.NotifyClose(make(chan *amqp.Error, 1))
}

// Supervise runs a broker session and redials whenever it ends.
//
// This exists because of a failure observed in a running system, not a
// hypothetical one: a Connection and its Channels are established ONCE at
// startup, and amqp091 does not reconnect. When the broker restarts — or just
// drops the connection under load — every subsequent publish returns
//
//	Exception (504) Reason: "channel/connection is not open"
//
// ...forever. The relay stays alive, looks healthy to any process-level check,
// and never publishes another event. The outbox grows without bound and the
// entire async pipeline silently stops. It took 241 identical failures in a row
// to notice.
//
// The fix is that a broker connection must be treated as a SESSION that ends,
// not a resource acquired once. Everything a session owns — channels,
// publishers, consumers, queue declarations — is rebuilt on reconnect, because
// a Channel belongs to a Connection and cannot outlive it.
//
// Two properties make this safe to retry blindly:
//
//   - DeclareTopology is idempotent, so re-declaring on every reconnect is free.
//   - Unpublished outbox rows still have sent_at IS NULL, so a session that died
//     mid-batch loses nothing. The next session picks the same rows up.
//
// Backoff is capped and jittered. Without jitter, every service reconnects in
// lockstep the moment the broker returns and knocks it straight over again —
// the same thundering-herd problem the deadlock retry avoids.
func Supervise(ctx context.Context, url, exchange, name string, log *slog.Logger, run func(ctx context.Context, b *Broker) error) error {
	const (
		minBackoff = 500 * time.Millisecond
		maxBackoff = 30 * time.Second
	)
	backoff := minBackoff

	for {
		if ctx.Err() != nil {
			return nil
		}

		broker, err := Connect(url, exchange, name, log)
		if err != nil {
			log.Error("cannot reach broker, retrying", "error", err, "retry_in", backoff)
			if !sleepCtx(ctx, backoff) {
				return nil
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		if err := broker.DeclareTopology(); err != nil {
			log.Error("declaring topology failed", "error", err)
			_ = broker.Close()
			if !sleepCtx(ctx, backoff) {
				return nil
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		// A live session resets the budget, so a long-running service that drops
		// once does not start its next reconnect at 30 seconds.
		backoff = minBackoff

		err = runSession(ctx, broker, log, run)
		_ = broker.Close()

		if ctx.Err() != nil {
			return nil // ordinary shutdown
		}

		log.Warn("broker session ended, reconnecting", "error", err)
		if !sleepCtx(ctx, backoff) {
			return nil
		}
		backoff = nextBackoff(backoff, maxBackoff)
	}
}

// runSession runs one session and returns when either the work finishes or the
// connection drops.
//
// The watcher goroutine is what turns a silent dead connection into a session
// that ends. Without it, a consumer blocked on a delivery channel that will
// never receive again simply parks forever, and a publisher retries into a void.
func runSession(ctx context.Context, broker *Broker, log *slog.Logger, run func(ctx context.Context, b *Broker) error) error {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel() // guaranteed exit path for the watcher below

	closed := broker.NotifyClose()

	go func() {
		select {
		case reason := <-closed:
			if reason != nil {
				log.Error("broker connection lost", "reason", reason.Reason, "code", reason.Code)
			}
			cancel() // ends the session so Supervise redials
		case <-sessionCtx.Done():
		}
	}()

	return run(sessionCtx, broker)
}

// sleepCtx waits, returning false if the context was cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		next = max
	}
	// Up to 25% jitter, so services that dropped together do not return together.
	return next + time.Duration(rand.Int63n(int64(next/4)+1))
}
