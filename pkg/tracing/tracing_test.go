package tracing_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Sawalhy/backend-golang-task-2025/pkg/tracing"
)

// The distributed part of distributed tracing is this round trip and nothing
// else: a trace id survives being written to a column, read back by a different
// process, and resumed — with no call stack shared between the two ends.
//
// Everything else (the exporter, Jaeger, the UI) is plumbing that either works
// or is visibly absent. This is the part that silently produces two unrelated
// traces if it is wrong.
func TestTraceparentSurvivesTheOutboxRoundTrip(t *testing.T) {
	// No endpoint: export disabled, tracing itself fully live. That is exactly the
	// configuration every other test and every `go run` uses, which is the point
	// — the correlation path under test here is the one that always runs, not a
	// special mode that only exists when Jaeger does.
	shutdown, err := tracing.Init(context.Background(), "test", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	// Stand in for the API handler: a span exists, and its traceparent is what
	// Enqueue would store on the outbox row.
	producer, span := tracing.Tracer().Start(context.Background(), "POST /orders")
	traceparent := tracing.Traceparent(producer)
	originalID := tracing.TraceID(producer)
	span.End()

	require.NotEmpty(t, traceparent, "a request span must always yield a traceparent to store")
	require.NotEmpty(t, originalID)

	// Stand in for the consumer, minutes later in another process: it has the
	// header and nothing else.
	consumer := tracing.WithTraceparent(context.Background(), traceparent)
	require.Equal(t, originalID, tracing.TraceID(consumer),
		"consumer must rejoin the request's trace, not start its own")
}

// A message published before tracing existed, or by a producer that does not
// trace, has no header. That must cost a trace, never a message.
func TestMissingTraceparentIsNotAnError(t *testing.T) {
	ctx := tracing.WithTraceparent(context.Background(), "")
	require.Empty(t, tracing.TraceID(ctx))

	ctx = tracing.WithTraceparent(context.Background(), "not-a-traceparent")
	require.Empty(t, tracing.TraceID(ctx))
}
