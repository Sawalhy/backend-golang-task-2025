package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
)

// RunOrderEventBackplane feeds this API instance's SSE hub from RabbitMQ.
//
// The queue here is nothing like the work queues, and the differences are all
// deliberate:
//
//	exclusive + auto-delete  one queue PER INSTANCE, destroyed when the
//	                         connection drops. Its name carries the instance id.
//	not durable              a queue of doorbells for connections that no longer
//	                         exist is worthless after a restart.
//	autoAck                  losing an event costs nothing: the client re-reads
//	                         authoritative state from Postgres on reconnect.
//
// The critical property is that these instances must NOT compete. The payments
// queue has N consumers precisely so each message is handled once; here, every
// instance must receive EVERY event, because any of them might be holding the
// browser connection that cares about it. Competing consumers would deliver an
// order.paid to one instance while the customer's connection sat on another,
// and that customer would simply never be told.
//
// Separate queues bound to the same routing key is what turns "handled once"
// into "delivered to all". Same exchange, same messages, different topology.
func RunOrderEventBackplane(ctx context.Context, b *Broker, instanceID string, onEvent func(models.Envelope), log *slog.Logger) error {
	ch, err := b.conn.Channel()
	if err != nil {
		return fmt.Errorf("opening backplane channel: %w", err)
	}
	defer ch.Close()

	queue := "sse." + instanceID

	q, err := ch.QueueDeclare(queue,
		false, // durable: no
		true,  // autoDelete: goes when the last consumer goes
		true,  // exclusive: this connection only
		false, nil)
	if err != nil {
		return fmt.Errorf("declaring backplane queue %s: %w", queue, err)
	}

	// order.# matches order.created, order.paid, order.cancelled, order.expired
	// and order.fulfilled — every state change a client could care about.
	if err := ch.QueueBind(q.Name, "order.#", b.exchange, false, nil); err != nil {
		return fmt.Errorf("binding backplane queue %s: %w", q.Name, err)
	}

	deliveries, err := ch.Consume(q.Name, "", true /* autoAck */, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("consuming backplane queue %s: %w", q.Name, err)
	}

	log.Info("sse backplane attached", "queue", q.Name, "binding", "order.#")

	for {
		select {
		case <-ctx.Done():
			log.Info("sse backplane stopping", "queue", q.Name)
			return nil

		case d, ok := <-deliveries:
			if !ok {
				log.Warn("sse backplane delivery channel closed", "queue", q.Name)
				return nil
			}

			var ev models.Envelope
			if err := json.Unmarshal(d.Body, &ev); err != nil {
				log.Error("backplane received unparseable event", "error", err)
				continue
			}
			onEvent(ev)
		}
	}
}
