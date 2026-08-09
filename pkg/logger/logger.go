// Package logger builds the process-wide structured logger.
//
// log/slog, not logrus or zap: it is stdlib since Go 1.21, structured, and adds
// no dependency. Never fmt.Println — an unstructured line cannot be filtered,
// aggregated, or correlated to a trace.
package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// contextKey is unexported so no other package can collide with our keys.
type contextKey struct{ name string }

var (
	traceIDKey = &contextKey{"trace_id"}
	loggerKey  = &contextKey{"logger"}
)

// New returns a logger writing JSON in production and human-readable text in
// development. Production log lines are consumed by machines; development ones
// by a person reading a terminal.
func New(level string, production bool) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}

	var h slog.Handler
	if production {
		h = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		h = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(h)
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// WithTraceID returns a context carrying the trace id and a logger already
// tagged with it, so every line emitted downstream is correlated without each
// call site remembering to attach it.
func WithTraceID(ctx context.Context, base *slog.Logger, traceID string) context.Context {
	ctx = context.WithValue(ctx, traceIDKey, traceID)
	return context.WithValue(ctx, loggerKey, base.With("trace_id", traceID))
}

// TraceID returns the trace id carried by ctx, or "" if there is none.
func TraceID(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey).(string); ok {
		return v
	}
	return ""
}

// FromContext returns the request-scoped logger, falling back to the default so
// callers never have to nil-check. A logger is a convenience, not a dependency
// worth propagating an error for.
func FromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}
