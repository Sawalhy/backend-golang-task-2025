// Package tracing wires OpenTelemetry, and does only what DESIGN_NOTES §5.19
// asks for: one id, generated at the API edge, that travels with the work.
//
// The hard part of distributed tracing here is not the SDK — it is that the
// handler and the consumer are separate processes running minutes apart with no
// call stack between them. An in-process tracer cannot bridge that. So the trace
// context is WRITTEN DOWN: outbox.trace_id holds the W3C traceparent, the relay
// reads it back into an AMQP header, and the consumer resumes the trace from it.
// Same move as everything else in this system — the outbox carries causality
// because the network cannot.
//
// Deliberately no spans around individual DB calls. They triple the span count
// and answer a question (which query was slow) that pg_stat_statements already
// answers better. The spans that exist mark the process boundaries, which is
// exactly where the time actually disappears in an async pipeline.
package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName identifies this codebase's instrumentation in the exported spans.
const ScopeName = "github.com/Sawalhy/backend-golang-task-2025"

// Shutdown flushes buffered spans. Always non-nil, so callers can defer it
// without a nil check even when tracing is disabled.
type Shutdown func(context.Context) error

// Init installs the global tracer provider and the W3C propagator.
//
// An empty endpoint disables EXPORT — not tracing. That distinction is the whole
// design of this function: a provider is installed either way, so trace ids are
// always real, always land on log lines, and always propagate through the outbox
// and the broker. Only the shipping of spans to a collector is conditional.
//
// The alternative — no provider when there is no endpoint — leaves the global
// no-op tracer in place, and then every span has an INVALID context: no trace
// id on any log line, an empty outbox.trace_id, and the correlation path
// silently untested everywhere except the one environment that runs Jaeger.
// Paying an allocation per span to keep one code path is the better trade.
//
// Empty is the default because tests, `go run`, and anyone who has not started
// Jaeger must not be blocked by a missing collector.
func Init(ctx context.Context, service, endpoint string) (Shutdown, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(service),
		)),
	}

	if endpoint != "" {
		// WithInsecure: this ships to a Jaeger sitting on the compose network, not
		// across the internet. Production would terminate TLS at the collector.
		exp, err := otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(endpoint),
			otlptracehttp.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("building otlp exporter for %s: %w", endpoint, err)
		}
		// Batched, not synchronous: a span export must never sit in the latency
		// path of a payment.
		opts = append(opts, sdktrace.WithBatcher(exp))
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

// Tracer returns this codebase's tracer. Before Init, or with tracing disabled,
// this is the no-op tracer and every Start below costs approximately nothing.
func Tracer() trace.Tracer { return otel.Tracer(ScopeName) }

// Traceparent serialises the span in ctx to a W3C traceparent header value, or
// "" when there is no recording span.
//
// This is what gets stored on the outbox row. A string column rather than a
// binary or split representation, because the value it holds is already the
// interchange format — the relay copies it into an AMQP header without parsing.
func Traceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier["traceparent"]
}

// WithTraceparent resumes a trace from a stored or received traceparent, making
// the next span started on the returned context a child of the original request.
//
// A malformed or empty value yields ctx unchanged: an unreadable trace header is
// a lost trace, never a failed message.
func WithTraceparent(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx,
		propagation.MapCarrier{"traceparent": traceparent})
}

// TraceID returns the 32-hex-character trace id on ctx, or "" if there is none.
// This is the value that goes on every log line, so a trace found in Jaeger and
// the logs describing it are reachable from each other by the same string.
func TraceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.TraceID().IsValid() {
		return ""
	}
	return sc.TraceID().String()
}
