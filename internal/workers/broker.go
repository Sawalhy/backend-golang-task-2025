// Package workers holds the broker plumbing and the background loops: the outbox
// relay, the queue consumers, and the periodic reaper.
package workers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/tracing"
)

// Queue names. One queue per JOB THAT MUST HAPPEN; N consumers on a queue are
// for throughput.
//
// Email and SMS get separate queues deliberately. Sharing one would make them
// competing consumers of the same message, so a single order.paid would be
// delivered to exactly one of them and the customer would get an email OR a
// text, never both.
const (
	QueuePayments = "payments"
	QueueEmail    = "notifications.email"
	QueueSMS      = "notifications.sms"
	QueueRefunds  = "refunds"

	ExchangeDLX = "dlx"

	// HeaderTraceparent is the W3C name, used verbatim so any other tracing-aware
	// consumer of this exchange interoperates without agreeing on a convention.
	HeaderTraceparent = "traceparent"
)

// binding describes one queue and the routing keys it subscribes to.
var topology = []struct {
	queue    string
	bindings []string
}{
	{QueuePayments, []string{models.EventOrderCreated}},
	{QueueEmail, []string{models.EventOrderPaid, models.EventOrderCancelled, models.EventOrderFailed}},
	{QueueSMS, []string{models.EventOrderPaid, models.EventOrderCancelled}},
	{QueueRefunds, []string{models.EventRefundRequested}},
}

type Broker struct {
	conn     *amqp.Connection
	exchange string
	log      *slog.Logger
}

// Connect dials the broker under a given name.
//
// The name is not decoration. RabbitMQ's management UI lists connections by
// host:port otherwise, so during an incident you are looking at a wall of
// identical rows with no way to tell the relay from a worker from an API
// replica. Naming them makes "which process is holding these 400 unacked
// messages?" answerable.
//
// Heartbeat and Locale have to be set explicitly: amqp.Dial supplies defaults
// for them, but DialConfig does not, and a zero heartbeat disables dead-peer
// detection entirely — which is precisely the failure this package exists to
// survive.
func Connect(url, exchange, name string, log *slog.Logger) (*Broker, error) {
	conn, err := amqp.DialConfig(url, amqp.Config{
		Heartbeat:  10 * time.Second,
		Locale:     "en_US",
		Properties: amqp.Table{"connection_name": name},
	})
	if err != nil {
		return nil, fmt.Errorf("dialling rabbitmq: %w", err)
	}
	return &Broker{conn: conn, exchange: exchange, log: log}, nil
}

func (b *Broker) Close() error { return b.conn.Close() }

// DeclareTopology creates the exchange, queues and bindings.
//
// Declaration is idempotent, so every process can safely declare on boot and no
// startup ordering between api, worker and relay is required.
//
// Everything is durable, and messages are published persistent: a broker restart
// must not lose an order.created that the outbox has already marked sent.
func (b *Broker) DeclareTopology() error {
	ch, err := b.conn.Channel()
	if err != nil {
		return fmt.Errorf("opening channel: %w", err)
	}
	defer ch.Close()

	// A topic exchange, so a routing key like order.paid can fan out to any
	// number of queues. The publisher never learns how many — bindings decide
	// that, and consumers own the bindings. Adding a fraud service later means
	// declaring one queue with one binding and changing no publishing code.
	if err := ch.ExchangeDeclare(b.exchange, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring exchange %s: %w", b.exchange, err)
	}

	// A direct dead-letter exchange with one DLQ per source queue, so a poisoned
	// message stays traceable to where it came from. A single shared DLQ turns
	// triage into guesswork.
	if err := ch.ExchangeDeclare(ExchangeDLX, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring dlx: %w", err)
	}

	for _, t := range topology {
		dlqKey := t.queue + ".failed"

		args := amqp.Table{
			"x-dead-letter-exchange":    ExchangeDLX,
			"x-dead-letter-routing-key": dlqKey,
		}
		if _, err := ch.QueueDeclare(t.queue, true, false, false, false, args); err != nil {
			return fmt.Errorf("declaring queue %s: %w", t.queue, err)
		}
		for _, key := range t.bindings {
			if err := ch.QueueBind(t.queue, key, b.exchange, false, nil); err != nil {
				return fmt.Errorf("binding %s to %s: %w", t.queue, key, err)
			}
		}

		dlq := t.queue + ".dlq"
		if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declaring dlq %s: %w", dlq, err)
		}
		if err := ch.QueueBind(dlq, dlqKey, ExchangeDLX, false, nil); err != nil {
			return fmt.Errorf("binding dlq %s: %w", dlq, err)
		}
	}

	b.log.Info("rabbitmq topology declared", "exchange", b.exchange, "queues", len(topology))
	return nil
}

// Publisher wraps a channel in confirm mode.
//
// Publisher confirms are the whole reason the outbox is trustworthy. A plain
// Publish is a write to a socket buffer: it returns successfully long before the
// broker has the message, so marking outbox.sent_at on the back of it means a
// broker crash silently loses an event the database believes was delivered.
// Waiting for the ack makes duplicates possible and losses impossible, which is
// the correct direction to fail.
type Publisher struct {
	ch       *amqp.Channel
	exchange string
}

func (b *Broker) NewPublisher() (*Publisher, error) {
	ch, err := b.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("opening publisher channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, fmt.Errorf("enabling publisher confirms: %w", err)
	}
	return &Publisher{ch: ch, exchange: b.exchange}, nil
}

func (p *Publisher) Close() error { return p.ch.Close() }

// PublishConfirmed publishes one message and blocks until the broker acks it.
//
// The traceparent taken from ctx goes on as a header, which is the second half
// of the handoff: outbox.trace_id carried the trace across the commit, this
// carries it across the broker to whichever process consumes the message.
func (p *Publisher) PublishConfirmed(ctx context.Context, routingKey string, eventID string, body []byte) error {
	headers := amqp.Table{}
	if tp := tracing.Traceparent(ctx); tp != "" {
		headers[HeaderTraceparent] = tp
	}

	dc, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, p.exchange, routingKey,
		false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // survive a broker restart
			MessageId:    eventID,         // the consumer's dedupe key
			Timestamp:    time.Now().UTC(),
			Headers:      headers,
			Body:         body,
		})
	if err != nil {
		return fmt.Errorf("publishing %s: %w", routingKey, err)
	}

	ack, err := dc.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("waiting for confirm on %s: %w", routingKey, err)
	}
	if !ack {
		// A nack means the broker refused it. Returning an error leaves sent_at
		// NULL, so the relay will try again — exactly what should happen.
		return fmt.Errorf("broker nacked %s (event %s)", routingKey, eventID)
	}
	return nil
}

// Consume opens a channel with prefetch applied and returns its delivery stream.
//
// Prefetch is not a tuning knob to leave at default. Without it a consumer is
// handed the entire queue at once, so its competing consumers get nothing and
// the "pool" of N workers is one worker with a long backlog.
func (b *Broker) Consume(queue string, prefetch int) (*amqp.Channel, <-chan amqp.Delivery, error) {
	ch, err := b.conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("opening consumer channel: %w", err)
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		ch.Close()
		return nil, nil, fmt.Errorf("setting qos on %s: %w", queue, err)
	}

	// autoAck is false. With it enabled the broker considers a message delivered
	// the moment it hits the socket, so a worker that dies mid-charge loses the
	// job entirely (failure mode E). Manual ack after the work is done means a
	// dead worker's message is redelivered instead.
	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		ch.Close()
		return nil, nil, fmt.Errorf("consuming %s: %w", queue, err)
	}
	return ch, deliveries, nil
}
