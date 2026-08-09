package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Sawalhy/backend-golang-task-2025/internal/api/middleware"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

// heartbeatInterval keeps idle connections alive. Proxies and load balancers
// close a connection that has been silent for 30-60s, and an SSE stream for an
// order awaiting payment is legitimately silent for minutes. A comment line
// costs nothing and stops the connection being reaped.
const heartbeatInterval = 20 * time.Second

// StreamOrderStatus handles GET /api/v1/orders/{id}/events.
//
// Server-Sent Events rather than WebSocket, deliberately: the client never sends
// anything on this channel, so a bidirectional protocol solves a problem that
// does not exist here. SSE is plain HTTP — it survives proxies, needs no
// upgrade handshake, and browsers reconnect automatically.
//
// The ordering below is the part worth reading:
//
//	1. subscribe        (starts buffering)
//	2. read current state and send it
//	3. stream whatever arrived during 1-2, then live events
//
// Doing 2 before 1 leaves a gap: an order that transitions between the read and
// the subscribe emits an event nobody is listening for, and the client sits on a
// stale status forever with no further update coming. Subscribing first can only
// produce a duplicate — the client sees PAID twice — which is harmless.
//
//	@Summary		Stream order status changes (SSE)
//	@Description	Emits an initial `status` event with current state, then one event per transition. Terminates when the order reaches a terminal state.
//	@Tags			orders
//	@Produce		text/event-stream
//	@Param			id	path	int	true	"order id"
//	@Success		200	{string}	string	"event stream"
//	@Failure		404	{object}	ErrorResponse
//	@Security		BearerAuth
//	@Router			/orders/{id}/events [get]
func (h *OrderHandler) StreamOrderStatus(c *gin.Context) {
	id, ok := idParam(c, "id")
	if !ok {
		return
	}

	ctx := c.Request.Context()
	userID, isAdmin := middleware.UserID(c), middleware.IsAdmin(c)

	// Authorise before opening the stream: once the headers are written the
	// status code is fixed, and a 200 stream carrying an error is nobody's idea
	// of a good API.
	view, err := h.orders.Status(ctx, id, userID, isAdmin)
	if err != nil {
		fail(c, err)
		return
	}

	flusher, isFlusher := c.Writer.(http.Flusher)
	if !isFlusher {
		// Cannot stream through this writer — better to say so than to buffer
		// the whole "stream" until the handler returns.
		fail(c, fmt.Errorf("%w: streaming unsupported", ErrStreamingUnsupported))
		return
	}

	// 1. Subscribe FIRST. Everything published from here on is buffered for us.
	sub := h.hub.Subscribe(id)
	defer h.hub.Unsubscribe(sub) // the connection's guaranteed exit path

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	// Tells nginx not to buffer the response, which would otherwise hold events
	// until the buffer filled and defeat the entire point.
	c.Header("X-Accel-Buffering", "no")
	c.Writer.WriteHeader(http.StatusOK)

	log := logger.FromContext(ctx)

	// 2. Current state, so a client that connects late is immediately correct
	//    rather than waiting for the next transition that may never come.
	if !writeEvent(c, flusher, "status", view) {
		return
	}

	// An order that is already terminal will never emit again. Close instead of
	// holding a connection open forever for an event that cannot arrive.
	if view.Terminal {
		writeComment(c, flusher, "order is in a terminal state; closing")
		return
	}

	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	// 3. Live stream.
	for {
		select {
		case <-ctx.Done():
			// Client disconnected or the server is shutting down. The deferred
			// Unsubscribe runs here — without it this order's entry in the hub
			// would leak for the lifetime of the process.
			log.Debug("sse client disconnected", "order_id", id)
			return

		case ev, open := <-sub.Events():
			if !open {
				return
			}

			// The event is a doorbell, not state: by the time it arrives the
			// order may have moved on again. Re-read the authoritative answer
			// from Postgres rather than trusting the payload.
			fresh, err := h.orders.Status(ctx, id, userID, isAdmin)
			if err != nil {
				log.Error("re-reading order for sse", "order_id", id, "error", err)
				return
			}
			if !writeEvent(c, flusher, eventName(ev.EventType), fresh) {
				return
			}
			if fresh.Terminal {
				writeComment(c, flusher, "order reached a terminal state; closing")
				return
			}

		case <-heartbeat.C:
			if !writeComment(c, flusher, "keep-alive") {
				return
			}
		}
	}
}

// writeEvent emits one SSE frame. Returns false once the connection is gone, so
// the caller stops rather than writing into a dead socket.
func writeEvent(c *gin.Context, flusher http.Flusher, name string, payload any) bool {
	body, err := json.Marshal(payload)
	if err != nil {
		return false
	}

	// SSE frame: optional event name, one data line, blank line to terminate.
	if _, err := fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", name, body); err != nil {
		return false
	}
	flusher.Flush()
	return true
}

func writeComment(c *gin.Context, flusher http.Flusher, text string) bool {
	if _, err := fmt.Fprintf(c.Writer, ": %s\n\n", text); err != nil {
		return false
	}
	flusher.Flush()
	return true
}

// eventName maps a routing key onto an SSE event name: order.paid -> paid.
func eventName(routingKey string) string {
	for i := len(routingKey) - 1; i >= 0; i-- {
		if routingKey[i] == '.' {
			return routingKey[i+1:]
		}
	}
	return routingKey
}
