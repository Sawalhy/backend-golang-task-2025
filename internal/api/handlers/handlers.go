// Package handlers translates HTTP to service calls and back. It holds no
// business rules — anything that decides what the system does belongs in
// internal/services, where a worker can reach it too.
package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/Sawalhy/backend-golang-task-2025/internal/api/middleware"
	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

// ErrorResponse is the single error shape every endpoint returns.
type ErrorResponse struct {
	Error   string `json:"error"`
	TraceID string `json:"trace_id,omitempty"`
}

// statusFor maps domain errors onto HTTP status codes in ONE place.
//
// This is why services return typed errors instead of formatted strings: the
// alternative is each handler string-matching on messages, which breaks silently
// the first time someone rewords one.
//
// The interesting entry is ErrInsufficientStock -> 409 Conflict, not 400 or 500.
// The request was well-formed and the server is healthy; the world simply
// changed between the customer loading the page and pressing buy. 409 says
// exactly that, and a client can sensibly re-read stock and retry.
func statusFor(err error) int {
	switch {
	case errors.Is(err, models.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, models.ErrInvalidInput):
		return http.StatusBadRequest
	case errors.Is(err, models.ErrUnauthorized):
		return http.StatusUnauthorized
	case errors.Is(err, models.ErrForbidden):
		return http.StatusForbidden
	case errors.Is(err, models.ErrAlreadyExists):
		return http.StatusConflict
	case errors.Is(err, models.ErrInsufficientStock):
		return http.StatusConflict
	case errors.Is(err, models.ErrOrderNotOpen):
		return http.StatusConflict
	case errors.Is(err, models.ErrPaymentPending):
		return http.StatusConflict
	case errors.Is(err, models.ErrLostRace):
		// Somebody else changed the row first. The client's view is stale, so
		// this is a conflict to re-read and retry, not a server fault.
		return http.StatusConflict
	case errors.Is(err, models.ErrIllegalTransition):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

// fail writes the error response. 5xx bodies are deliberately generic: an
// internal error message can leak schema details, file paths or query fragments.
// The real error goes to the log, keyed by the trace id the client is given.
func fail(c *gin.Context, err error) {
	status := statusFor(err)
	traceID := c.GetString(middleware.CtxTraceID)

	msg := err.Error()
	if status >= 500 {
		logger.FromContext(c.Request.Context()).Error("request failed",
			"error", err, "path", c.Request.URL.Path)
		msg = "internal server error"
	}

	c.AbortWithStatusJSON(status, ErrorResponse{Error: msg, TraceID: traceID})
}

// ErrStreamingUnsupported means the ResponseWriter cannot flush, so an SSE
// stream would be buffered until the handler returned — which is not a stream.
var ErrStreamingUnsupported = errors.New("streaming unsupported by this connection")

// wrapBinding turns a Gin binding/validator failure into ErrInvalidInput, so it
// flows through the same statusFor mapping as every other error and comes back
// as 400 with the validator's own explanation of which field was wrong.
func wrapBinding(err error) error {
	return fmt.Errorf("%w: %s", models.ErrInvalidInput, err.Error())
}

// idParam parses a positive integer path parameter.
func idParam(c *gin.Context, name string) (uint64, bool) {
	raw := c.Param(name)
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		fail(c, models.ErrInvalidInput)
		return 0, false
	}
	return id, true
}

// Pagination is bounded on purpose: an unbounded limit lets one request ask for
// the entire orders table and take the database with it.
type Pagination struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

const (
	defaultLimit = 20
	maxLimit     = 100
)

func pagination(c *gin.Context) Pagination {
	limit := defaultLimit
	if v, err := strconv.Atoi(c.Query("limit")); err == nil && v > 0 {
		limit = min(v, maxLimit)
	}
	offset := 0
	if v, err := strconv.Atoi(c.Query("offset")); err == nil && v > 0 {
		offset = v
	}
	return Pagination{Limit: limit, Offset: offset}
}

// PageResponse is the envelope for every paginated list.
type PageResponse[T any] struct {
	Data   []T   `json:"data"`
	Total  int64 `json:"total"`
	Limit  int   `json:"limit"`
	Offset int   `json:"offset"`
}
