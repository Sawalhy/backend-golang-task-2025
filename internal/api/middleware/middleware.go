// Package middleware holds the Gin middleware chain.
//
// This is one of the two packages allowed to know Gin exists (the other is
// handlers). internal/services deliberately does not, so every rule in there can
// also run in cmd/worker where there is no HTTP request at all.
package middleware

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/metrics"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/tracing"
)

// Context keys for values the middleware chain puts on the request.
const (
	CtxUserID  = "auth.user_id"
	CtxRole    = "auth.role"
	CtxTraceID = "trace.id"
)

// RequestID opens the request's trace span and threads its id through ctx, so
// every log line emitted anywhere below this point correlates to one request
// without each call site remembering to pass it.
//
// Tracing and the log trace id are deliberately ONE mechanism, not two. Running
// a uuid for logs alongside an unrelated OpenTelemetry trace id gives two ids
// for one request and forces a human mid-incident to join them by timestamp.
// The span is started here, and its trace id becomes the logged trace_id — so a
// waterfall in Jaeger and the log lines describing it share a single string.
//
// Precedence: an inbound W3C traceparent continues that trace; otherwise an
// inbound X-Request-ID is honoured so a caller can still correlate; otherwise a
// fresh uuid. With tracing disabled there is no valid trace id and this collapses
// to exactly the previous behaviour.
func RequestID(base *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// FullPath is the ROUTE PATTERN (/api/v1/orders/:id), not the URL. Span
		// names and metric labels built from the raw path would mint a new series
		// per order id and eventually take the tracing backend down with them.
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}

		// Extract before starting: a traceparent on the inbound request makes this
		// span a child of the caller's, which is what stitches services together.
		ctx := otel.GetTextMapPropagator().Extract(
			c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))

		ctx, span := tracing.Tracer().Start(ctx, c.Request.Method+" "+route,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.request.method", c.Request.Method),
				attribute.String("http.route", route),
			))
		defer span.End()

		id := tracing.TraceID(ctx)
		if id == "" {
			if id = c.GetHeader("X-Request-ID"); id == "" {
				id = uuid.NewString()
			}
		}

		c.Set(CtxTraceID, id)
		c.Header("X-Request-ID", id)

		// Replace the request's context so handlers and everything they call
		// receive the trace-tagged logger and the span via ctx, not via a global.
		c.Request = c.Request.WithContext(logger.WithTraceID(ctx, base, id))

		c.Next()

		status := c.Writer.Status()
		span.SetAttributes(attribute.Int("http.response.status_code", status))
		// Only 5xx marks the span errored. A 409 from insufficient stock is the
		// system working correctly, and colouring it red trains everyone to ignore
		// red spans.
		if status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
	}
}

// Metrics records the RED metrics for every request.
//
// Placed outside auth and rate limiting in the chain so a 401 or a 429 is still
// counted — those are exactly the responses you want a rate on when something is
// wrong, and a limiter that starts rejecting everything would otherwise make the
// traffic simply disappear from the dashboard.
func Metrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		start := time.Now()

		c.Next()

		metrics.HTTPDuration.WithLabelValues(c.Request.Method, route).
			Observe(time.Since(start).Seconds())
		metrics.HTTPRequests.WithLabelValues(c.Request.Method, route,
			strconv.Itoa(c.Writer.Status())).Inc()
	}
}

// Logging emits one structured line per request after it completes.
func Logging(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		status := c.Writer.Status()
		attrs := []any{
			"method", c.Request.Method,
			"path", path,
			"status", status,
			"duration_ms", time.Since(start).Milliseconds(),
			"trace_id", c.GetString(CtxTraceID),
		}
		if uid := c.GetUint64(CtxUserID); uid != 0 {
			attrs = append(attrs, "user_id", uid)
		}

		l := logger.FromContext(c.Request.Context())
		switch {
		case status >= 500:
			l.Error("request failed", append(attrs, "errors", c.Errors.String())...)
		case status >= 400:
			l.Warn("request rejected", attrs...)
		default:
			l.Info("request", attrs...)
		}
	}
}

// Recovery turns a panic into a 500 instead of killing the process.
//
// Gin's own recovery does this too; this version logs through slog with the
// trace id attached, so a panic is findable alongside the request that caused it.
func Recovery(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				logger.FromContext(c.Request.Context()).Error("panic recovered",
					"panic", r,
					"path", c.Request.URL.Path,
					"trace_id", c.GetString(CtxTraceID))

				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"error":    "internal server error",
					"trace_id": c.GetString(CtxTraceID),
				})
			}
		}()
		c.Next()
	}
}

// Auth validates the bearer token and puts the identity on the context.
func Auth(auth *services.AuthService) gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if header == "" {
			abortUnauthorized(c, "authorization header required")
			return
		}

		// Reject anything that is not exactly "Bearer <token>". Being lenient
		// about the scheme is how a Basic credential ends up parsed as a JWT.
		parts := strings.SplitN(header, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			abortUnauthorized(c, "authorization header must be 'Bearer <token>'")
			return
		}

		claims, err := auth.Verify(strings.TrimSpace(parts[1]))
		if err != nil {
			abortUnauthorized(c, "invalid or expired token")
			return
		}

		c.Set(CtxUserID, claims.UserID)
		c.Set(CtxRole, string(claims.Role))

		// Also on the request context, so the layers that must not import Gin can
		// still attribute the change they are about to make. This is what puts a
		// user id on the audit row for an admin fulfilment or a customer
		// cancellation, while a worker-driven transition stays attributed to
		// nobody.
		c.Request = c.Request.WithContext(models.WithActor(c.Request.Context(), claims.UserID))

		c.Next()
	}
}

// RequireAdmin gates the admin routes. It runs after Auth, which has already put
// the role on the context.
func RequireAdmin() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.GetString(CtxRole) != string(models.RoleAdmin) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error":    "admin role required",
				"trace_id": c.GetString(CtxTraceID),
			})
			return
		}
		c.Next()
	}
}

func abortUnauthorized(c *gin.Context, msg string) {
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
		"error":    msg,
		"trace_id": c.GetString(CtxTraceID),
	})
}

// UserID returns the authenticated user id, or 0 on an unauthenticated route.
func UserID(c *gin.Context) uint64 { return c.GetUint64(CtxUserID) }

// IsAdmin reports whether the caller holds the admin role.
func IsAdmin(c *gin.Context) bool { return c.GetString(CtxRole) == string(models.RoleAdmin) }

// ErrUnauthenticated is returned by helpers that need an identity and found none.
var ErrUnauthenticated = errors.New("no authenticated user on request")
