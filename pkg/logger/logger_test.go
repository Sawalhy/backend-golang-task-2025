package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRespectsLevel(t *testing.T) {
	tests := []struct {
		level      string
		wantsDebug bool
	}{
		{"debug", true},
		{"info", false},
		{"warn", false},
		{"error", false},
		{"WARNING", false},
		{"nonsense", false}, // unparseable falls back to info
		{"", false},
	}

	for _, tc := range tests {
		t.Run(tc.level, func(t *testing.T) {
			log := New(tc.level, true)
			assert.Equal(t, tc.wantsDebug, log.Enabled(context.Background(), slog.LevelDebug))
			assert.True(t, log.Enabled(context.Background(), slog.LevelError),
				"errors must always be emitted")
		})
	}
}

// The trace id is what ties a log line to the request that produced it, so it
// must ride on the context rather than being passed by hand at every call site.
func TestTraceIDRoundTrips(t *testing.T) {
	base := New("info", true)

	assert.Empty(t, TraceID(context.Background()), "no trace id before one is set")

	ctx := WithTraceID(context.Background(), base, "trace-abc-123")
	assert.Equal(t, "trace-abc-123", TraceID(ctx))
}

// FromContext falls back to the default rather than returning nil: a logger is a
// convenience, and nil-checking one at every call site is worse than a default.
func TestFromContextFallsBack(t *testing.T) {
	assert.NotNil(t, FromContext(context.Background()))
	assert.NotPanics(t, func() { FromContext(context.Background()).Info("still works") })
}

// The point of putting the logger on the context: everything downstream is
// tagged without remembering to attach it.
func TestContextLoggerCarriesTheTraceID(t *testing.T) {
	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, nil))

	ctx := WithTraceID(context.Background(), base, "trace-xyz")
	FromContext(ctx).Info("something happened")

	var line map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &line))
	assert.Equal(t, "trace-xyz", line["trace_id"])
	assert.Equal(t, "something happened", line["msg"])
}

// Values are keyed by an unexported type, so nothing outside this package can
// collide with them by using the same string.
func TestContextKeysAreNotStringCollidable(t *testing.T) {
	ctx := context.WithValue(context.Background(), "trace_id", "impostor") //nolint:staticcheck // deliberate
	assert.Empty(t, TraceID(ctx), "a plain string key must not be mistaken for ours")
}
