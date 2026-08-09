package tests

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/api/routes"
	"github.com/Sawalhy/backend-golang-task-2025/internal/config"
	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

// sseFixture is a full HTTP stack wired to the test database.
type sseFixture struct {
	server *httptest.Server
	hub    *services.StatusHub
	store  *repository.Store
	db     *gorm.DB
	token  string
	userID uint64
}

func newSSEFixture(t *testing.T) *sseFixture {
	t.Helper()
	gin.SetMode(gin.TestMode)

	store, db := newStore(t)
	log := logger.New("error", false)

	cfg := &config.Config{
		Env: "test",
		Auth: config.AuthConfig{
			JWTSecret:  "test-secret-that-is-long-enough-for-hs256",
			TokenTTL:   time.Hour,
			BcryptCost: 4, // fast: these tests are not about hashing cost
		},
		Order: config.OrderConfig{
			ReservationTTL:   15 * time.Minute,
			MaxItemsPerOrder: 50,
			DeadlockRetries:  3,
		},
	}

	auth := services.NewAuthService(store, cfg.Auth)
	hub := services.NewStatusHub()

	// Redis points nowhere on purpose. The limiter fails OPEN, so this also
	// asserts that a Redis outage does not take the API down with it.
	engine := routes.Build(routes.Deps{
		Cfg:     cfg,
		Log:     log,
		Auth:    auth,
		Orders:  services.NewOrderService(store, cfg.Order, log),
		Catalog: services.NewCatalogService(store),
		Reports: services.NewReportService(store, log),
		Hub:     hub,
		Redis:   redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}),
	})

	server := httptest.NewServer(engine)
	t.Cleanup(server.Close)

	user, err := auth.Register(context.Background(), services.RegisterInput{
		Email: "sse@example.com", Password: "password123", Name: "SSE Tester",
	})
	require.NoError(t, err)

	token, _, err := auth.Login(context.Background(),
		services.Credentials{Email: "sse@example.com", Password: "password123"})
	require.NoError(t, err)

	return &sseFixture{server: server, hub: hub, store: store, db: db, token: token, userID: user.ID}
}

// createOrder places a real order through the intake service.
func (f *sseFixture) createOrder(t *testing.T) uint64 {
	t.Helper()
	ctx := context.Background()

	catalog := services.NewCatalogService(f.store)
	product, err := catalog.CreateProduct(ctx, services.CreateProductInput{
		SKU: "SSE-1", Name: "Streamed Product", PriceCents: 500, Stock: 10,
	})
	require.NoError(t, err)

	orders := services.NewOrderService(f.store, config.OrderConfig{
		ReservationTTL: 15 * time.Minute, MaxItemsPerOrder: 50, DeadlockRetries: 3,
	}, logger.New("error", false))

	order, err := orders.Create(ctx, services.CreateOrderInput{
		UserID: f.userID,
		Lines:  []services.OrderLine{{ProductID: product.ID, Qty: 1}},
	})
	require.NoError(t, err)
	return order.ID
}

// sseEvent is one parsed frame.
type sseEvent struct {
	name string
	data map[string]any
}

// readSSE reads frames until n are collected or the stream ends.
func readSSE(t *testing.T, body *bufio.Scanner, n int) []sseEvent {
	t.Helper()

	var (
		out     []sseEvent
		current sseEvent
	)
	for body.Scan() {
		line := body.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			current.name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			payload := strings.TrimPrefix(line, "data: ")
			require.NoError(t, json.Unmarshal([]byte(payload), &current.data))
		case line == "" && current.name != "":
			out = append(out, current)
			current = sseEvent{}
			if len(out) >= n {
				return out
			}
		}
	}
	return out
}

func TestSSESendsCurrentStateImmediately(t *testing.T) {
	f := newSSEFixture(t)
	orderID := f.createOrder(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp := f.openStream(t, ctx, orderID)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/event-stream")

	// A client that connects late must be correct immediately rather than
	// waiting for a transition that may never come.
	events := readSSE(t, bufio.NewScanner(resp.Body), 1)
	require.Len(t, events, 1)
	assert.Equal(t, "status", events[0].name)
	assert.Equal(t, string(models.OrderPending), events[0].data["status"])
	assert.Equal(t, float64(orderID), events[0].data["order_id"])
}

func TestSSEPushesTransitionsAndClosesOnTerminal(t *testing.T) {
	f := newSSEFixture(t)
	orderID := f.createOrder(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp := f.openStream(t, ctx, orderID)
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	initial := readSSE(t, scanner, 1)
	require.Len(t, initial, 1)
	require.Equal(t, string(models.OrderPending), initial[0].data["status"])

	// Move the order, then ring the doorbell exactly as the backplane would.
	err := f.store.InTx(context.Background(), func(ctx context.Context, tx *gorm.DB) error {
		ok, err := f.store.Orders().Transition(ctx, tx, orderID, models.OrderPending, models.OrderCancelled)
		require.True(t, ok)
		return err
	})
	require.NoError(t, err)

	f.hub.Publish(models.NewOrderEvent(models.EventOrderCancelled, orderID, nil))

	next := readSSE(t, scanner, 1)
	require.Len(t, next, 1, "the transition should have been pushed")
	assert.Equal(t, "cancelled", next[0].name, "event name comes from the routing key")

	// The handler re-reads from Postgres rather than trusting the event payload,
	// so the pushed status is authoritative.
	assert.Equal(t, string(models.OrderCancelled), next[0].data["status"])
	assert.Equal(t, true, next[0].data["terminal"])

	// CANCELLED is terminal, so holding the connection open would be waiting for
	// an event that can never arrive. The server should close it.
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for scanner.Scan() {
		}
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("stream stayed open after the order reached a terminal state")
	}
}

// A customer must not be able to stream somebody else's order, and the refusal
// has to happen before the stream opens — once headers are written the status
// code is fixed.
func TestSSERejectsOtherPeoplesOrders(t *testing.T) {
	f := newSSEFixture(t)
	orderID := f.createOrder(t)

	ctx := context.Background()
	auth := services.NewAuthService(f.store, config.AuthConfig{
		JWTSecret: "test-secret-that-is-long-enough-for-hs256", TokenTTL: time.Hour, BcryptCost: 4,
	})
	_, err := auth.Register(ctx, services.RegisterInput{
		Email: "intruder@example.com", Password: "password123", Name: "Intruder",
	})
	require.NoError(t, err)
	intruderToken, _, err := auth.Login(ctx,
		services.Credentials{Email: "intruder@example.com", Password: "password123"})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodGet, f.streamURL(orderID), nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+intruderToken)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// 404, not 403: "you may not see this order" still confirms it exists.
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestSSERequiresAuthentication(t *testing.T) {
	f := newSSEFixture(t)
	orderID := f.createOrder(t)

	resp, err := http.Get(f.streamURL(orderID))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func (f *sseFixture) streamURL(orderID uint64) string {
	return f.server.URL + "/api/v1/orders/" + itoa(orderID) + "/events"
}

func (f *sseFixture) openStream(t *testing.T, ctx context.Context, orderID uint64) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.streamURL(orderID), nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+f.token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
