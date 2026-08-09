package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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

// apiFixture is the whole HTTP stack over the real database, with one admin and
// two customers already signed in.
//
// Requests go through the engine with a recorder rather than a live socket:
// these endpoints are request/response, so there is nothing to stream and no
// reason to pay for a TCP listener per call.
type apiFixture struct {
	store    *repository.Store
	db       *gorm.DB
	engine   *gin.Engine
	admin    string
	customer string
	other    string

	adminID    uint64
	customerID uint64
	otherID    uint64
}

// testRedis returns a client for the real Redis when one is configured.
//
// Falling back to an unreachable address still works — the limiter fails open —
// but MaxRetries must be off, or go-redis dials five times per request and every
// call pays about 1.7 seconds. The fail-open path has its own dedicated test in
// internal/api/middleware; here Redis is incidental.
func testRedis() *redis.Client {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:1"
	}
	return redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 200 * time.Millisecond,
		MaxRetries:  -1,
	})
}

func newAPIFixture(t *testing.T) *apiFixture {
	t.Helper()
	gin.SetMode(gin.TestMode)

	store, db := newStore(t)
	log := logger.New("error", false)

	cfg := &config.Config{
		Env: "test",
		Auth: config.AuthConfig{
			JWTSecret:  "test-secret-that-is-long-enough-for-hs256",
			TokenTTL:   time.Hour,
			BcryptCost: 4,
			// Effectively unlimited: these tests are about the endpoints, and a
			// real limit would make them fail on request count rather than on
			// behaviour. The limiter has its own tests.
			RateLimitRPS:   1_000_000,
			RateLimitBurst: 1_000_000,
		},
		Order: testOrderConfig(),
	}

	auth := services.NewAuthService(store, cfg.Auth)

	engine := routes.Build(routes.Deps{
		Cfg:     cfg,
		Log:     log,
		Auth:    auth,
		Orders:  services.NewOrderService(store, cfg.Order, log),
		Catalog: services.NewCatalogService(store),
		Reports: services.NewReportService(store, log),
		Hub:     services.NewStatusHub(),
		Redis:   testRedis(),
	})

	f := &apiFixture{store: store, db: db, engine: engine}
	ctx := context.Background()

	admin, err := auth.Register(ctx, services.RegisterInput{
		Email: "admin@test.com", Password: "adminpassword", Name: "Admin", Role: models.RoleAdmin,
	})
	require.NoError(t, err)
	f.adminID = admin.ID

	customer, err := auth.Register(ctx, services.RegisterInput{
		Email: "customer@test.com", Password: "customerpass", Name: "Customer",
	})
	require.NoError(t, err)
	f.customerID = customer.ID

	other, err := auth.Register(ctx, services.RegisterInput{
		Email: "other@test.com", Password: "otherpassword", Name: "Other",
	})
	require.NoError(t, err)
	f.otherID = other.ID

	f.admin = f.login(t, "admin@test.com", "adminpassword")
	f.customer = f.login(t, "customer@test.com", "customerpass")
	f.other = f.login(t, "other@test.com", "otherpassword")

	return f
}

func (f *apiFixture) login(t *testing.T, email, password string) string {
	t.Helper()

	w := f.do(t, http.MethodPost, "/api/v1/auth/login", "",
		map[string]string{"email": email, "password": password})
	require.Equal(t, http.StatusOK, w.Code, "login failed: %s", w.Body.String())

	var out struct {
		Token string `json:"token"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.NotEmpty(t, out.Token)
	return out.Token
}

func (f *apiFixture) do(t *testing.T, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	w := httptest.NewRecorder()
	f.engine.ServeHTTP(w, req)
	return w
}

func decode[T any](t *testing.T, w *httptest.ResponseRecorder) T {
	t.Helper()
	var out T
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out), "body: %s", w.Body.String())
	return out
}

// mustAPIProduct creates a product through the admin endpoint.
func (f *apiFixture) mustAPIProduct(t *testing.T, sku string, priceCents int64, stock int) models.Product {
	t.Helper()

	w := f.do(t, http.MethodPost, "/api/v1/products", f.admin, map[string]any{
		"sku": sku, "name": sku, "price_cents": priceCents, "stock": stock,
	})
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	return decode[models.Product](t, w)
}

// --- health -----------------------------------------------------------------

func TestHealthEndpoints(t *testing.T) {
	f := newAPIFixture(t)

	// Liveness deliberately does not touch the database: a probe that fails on a
	// slow query gets the pod killed during exactly the incident it should
	// survive.
	w := f.do(t, http.MethodGet, "/healthz", "", nil)
	assert.Equal(t, http.StatusOK, w.Code)

	// Readiness does check dependencies — an instance that cannot reach Postgres
	// should leave the load balancer rather than serve 500s.
	w = f.do(t, http.MethodGet, "/readyz", "", nil)
	assert.Equal(t, http.StatusOK, w.Code)
}

// --- users and auth ---------------------------------------------------------

func TestCreateUser(t *testing.T) {
	f := newAPIFixture(t)

	w := f.do(t, http.MethodPost, "/api/v1/users", "", map[string]any{
		"email": "new@test.com", "password": "password123", "name": "New Person",
	})
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())

	user := decode[models.User](t, w)
	assert.Equal(t, "new@test.com", user.Email)
	assert.Equal(t, models.RoleCustomer, user.Role)
	assert.NotContains(t, w.Body.String(), "password",
		"the hash must never be serialised")
}

func TestCreateUserValidation(t *testing.T) {
	f := newAPIFixture(t)

	tests := []struct {
		name string
		body map[string]any
	}{
		{"missing email", map[string]any{"password": "password123", "name": "X"}},
		{"malformed email", map[string]any{"email": "not-an-email", "password": "password123", "name": "X"}},
		{"short password", map[string]any{"email": "a@b.com", "password": "short", "name": "X"}},
		{"missing name", map[string]any{"email": "a@b.com", "password": "password123"}},
		{"empty body", map[string]any{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := f.do(t, http.MethodPost, "/api/v1/users", "", tc.body)
			assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		})
	}
}

func TestDuplicateEmailIsRejected(t *testing.T) {
	f := newAPIFixture(t)

	w := f.do(t, http.MethodPost, "/api/v1/users", "", map[string]any{
		"email": "customer@test.com", "password": "password123", "name": "Impostor",
	})
	assert.Equal(t, http.StatusConflict, w.Code)
}

// Without this check the role field is a privilege escalation: anyone who can
// POST /users could post "role":"ADMIN" and reach every admin route.
func TestSelfRegistrationCannotMintAnAdmin(t *testing.T) {
	f := newAPIFixture(t)

	w := f.do(t, http.MethodPost, "/api/v1/users", "", map[string]any{
		"email": "sneaky@test.com", "password": "password123", "name": "Sneaky", "role": "ADMIN",
	})
	require.Equal(t, http.StatusCreated, w.Code)

	user := decode[models.User](t, w)
	assert.Equal(t, models.RoleCustomer, user.Role, "an anonymous caller must not be able to self-promote")

	// And the token it gets must not open admin routes.
	token := f.login(t, "sneaky@test.com", "password123")
	w = f.do(t, http.MethodGet, "/api/v1/admin/orders", token, nil)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestLogin(t *testing.T) {
	f := newAPIFixture(t)

	tests := []struct {
		name     string
		email    string
		password string
		want     int
	}{
		{"correct credentials", "customer@test.com", "customerpass", http.StatusOK},
		{"wrong password", "customer@test.com", "wrongpassword", http.StatusUnauthorized},
		{"unknown email", "nobody@test.com", "customerpass", http.StatusUnauthorized},
		{"malformed email", "not-an-email", "customerpass", http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := f.do(t, http.MethodPost, "/api/v1/auth/login", "",
				map[string]string{"email": tc.email, "password": tc.password})
			assert.Equal(t, tc.want, w.Code, w.Body.String())
		})
	}
}

// Both failure paths return the same status, so login is not an
// account-enumeration oracle.
func TestLoginDoesNotRevealWhetherAnAccountExists(t *testing.T) {
	f := newAPIFixture(t)

	unknown := f.do(t, http.MethodPost, "/api/v1/auth/login", "",
		map[string]string{"email": "ghost@test.com", "password": "whatever123"})
	wrongPass := f.do(t, http.MethodPost, "/api/v1/auth/login", "",
		map[string]string{"email": "customer@test.com", "password": "whatever123"})

	assert.Equal(t, unknown.Code, wrongPass.Code)
	assert.Equal(t, http.StatusUnauthorized, unknown.Code)
}

func TestGetUserAuthorisation(t *testing.T) {
	f := newAPIFixture(t)

	tests := []struct {
		name   string
		target uint64
		token  string
		want   int
	}{
		{"own profile", f.customerID, f.customer, http.StatusOK},
		{"someone else's", f.otherID, f.customer, http.StatusForbidden},
		{"admin reads anyone", f.customerID, f.admin, http.StatusOK},
		{"unauthenticated", f.customerID, "", http.StatusUnauthorized},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := f.do(t, http.MethodGet, fmt.Sprintf("/api/v1/users/%d", tc.target), tc.token, nil)
			assert.Equal(t, tc.want, w.Code, w.Body.String())
		})
	}
}

func TestUpdateUser(t *testing.T) {
	f := newAPIFixture(t)

	w := f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/users/%d", f.customerID), f.customer,
		map[string]string{"name": "Renamed", "email": "renamed@test.com"})
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, "Renamed", decode[models.User](t, w).Name)

	// Cannot edit somebody else.
	w = f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/users/%d", f.otherID), f.customer,
		map[string]string{"name": "Hacked", "email": "hacked@test.com"})
	assert.Equal(t, http.StatusForbidden, w.Code)

	// Cannot take an email that is already in use.
	w = f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/users/%d", f.customerID), f.customer,
		map[string]string{"name": "Renamed", "email": "other@test.com"})
	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestInvalidPathIdsAreRejected(t *testing.T) {
	f := newAPIFixture(t)

	for _, path := range []string{"/api/v1/users/abc", "/api/v1/users/0", "/api/v1/orders/xyz"} {
		w := f.do(t, http.MethodGet, path, f.customer, nil)
		assert.Equal(t, http.StatusBadRequest, w.Code, path)
	}
}

func TestMalformedTokensAreRejected(t *testing.T) {
	f := newAPIFixture(t)

	for _, header := range []string{"garbage", "Bearer", "Basic abc", "Bearer not.a.jwt"} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/orders", nil)
		req.Header.Set("Authorization", header)
		w := httptest.NewRecorder()
		f.engine.ServeHTTP(w, req)
		assert.Equal(t, http.StatusUnauthorized, w.Code, header)
	}
}

// --- products ---------------------------------------------------------------

func TestProductCrud(t *testing.T) {
	f := newAPIFixture(t)

	created := f.mustAPIProduct(t, "API-1", 2500, 10)
	assert.Equal(t, "API-1", created.SKU)
	assert.True(t, created.Active)

	w := f.do(t, http.MethodGet, fmt.Sprintf("/api/v1/products/%d", created.ID), "", nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.EqualValues(t, 2500, decode[models.Product](t, w).PriceCents)

	w = f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/products/%d", created.ID), f.admin,
		map[string]any{"name": "Renamed", "price_cents": 3000, "active": false})
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	updated := decode[models.Product](t, w)
	assert.Equal(t, "Renamed", updated.Name)
	assert.EqualValues(t, 3000, updated.PriceCents)
	assert.False(t, updated.Active)

	w = f.do(t, http.MethodGet, "/api/v1/products/999999", "", nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestProductWritesRequireAdmin(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "API-2", 100, 5)

	w := f.do(t, http.MethodPost, "/api/v1/products", f.customer,
		map[string]any{"sku": "NOPE", "name": "Nope", "price_cents": 100, "stock": 1})
	assert.Equal(t, http.StatusForbidden, w.Code)

	w = f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/products/%d", product.ID), f.customer,
		map[string]any{"name": "Nope", "price_cents": 100, "active": true})
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestProductValidation(t *testing.T) {
	f := newAPIFixture(t)
	f.mustAPIProduct(t, "DUPLICATE", 100, 1)

	tests := []struct {
		name string
		body map[string]any
		want int
	}{
		{"duplicate sku", map[string]any{"sku": "DUPLICATE", "name": "X", "price_cents": 100, "stock": 1}, http.StatusConflict},
		{"missing sku", map[string]any{"name": "X", "price_cents": 100, "stock": 1}, http.StatusBadRequest},
		{"negative price", map[string]any{"sku": "NEG", "name": "X", "price_cents": -5, "stock": 1}, http.StatusBadRequest},
		{"negative stock", map[string]any{"sku": "NEGSTOCK", "name": "X", "price_cents": 100, "stock": -1}, http.StatusBadRequest},
		{"bad currency", map[string]any{"sku": "CUR", "name": "X", "price_cents": 100, "stock": 1, "currency": "POUNDS"}, http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := f.do(t, http.MethodPost, "/api/v1/products", f.admin, tc.body)
			assert.Equal(t, tc.want, w.Code, w.Body.String())
		})
	}
}

func TestListProductsPaginates(t *testing.T) {
	f := newAPIFixture(t)
	for i := 0; i < 5; i++ {
		f.mustAPIProduct(t, fmt.Sprintf("PAGE-%d", i), 100, 1)
	}

	w := f.do(t, http.MethodGet, "/api/v1/products?limit=2", "", nil)
	require.Equal(t, http.StatusOK, w.Code)

	page := decode[struct {
		Data   []models.Product `json:"data"`
		Total  int64            `json:"total"`
		Limit  int              `json:"limit"`
		Offset int              `json:"offset"`
	}](t, w)
	assert.Len(t, page.Data, 2)
	assert.EqualValues(t, 5, page.Total)
	assert.Equal(t, 2, page.Limit)

	// An unbounded limit would let one request pull the whole table.
	w = f.do(t, http.MethodGet, "/api/v1/products?limit=100000", "", nil)
	require.Equal(t, http.StatusOK, w.Code)
	capped := decode[struct {
		Limit int `json:"limit"`
	}](t, w)
	assert.LessOrEqual(t, capped.Limit, 100, "the page size must be capped")
}

// `available` is a point-in-time reading, not a promise — which is why the API
// never invites "check stock, then order" as two steps.
func TestGetInventory(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "INV-1", 500, 7)

	w := f.do(t, http.MethodGet, fmt.Sprintf("/api/v1/products/%d/inventory", product.ID), "", nil)
	require.Equal(t, http.StatusOK, w.Code)

	inv := decode[models.Inventory](t, w)
	assert.Equal(t, 7, inv.Available)
	assert.Equal(t, 0, inv.Reserved)

	w = f.do(t, http.MethodGet, "/api/v1/products/999999/inventory", "", nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// --- orders -----------------------------------------------------------------

func (f *apiFixture) placeOrder(t *testing.T, token string, productID uint64, qty int) models.Order {
	t.Helper()

	w := f.do(t, http.MethodPost, "/api/v1/orders", token, map[string]any{
		"items": []map[string]any{{"product_id": productID, "qty": qty}},
	})
	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	return decode[models.Order](t, w)
}

// 202, not 201: when this returns the stock is reserved and the order exists,
// but the payment has not happened.
func TestCreateOrderReturns202(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "ORD-1", 1500, 10)

	order := f.placeOrder(t, f.customer, product.ID, 2)
	assert.Equal(t, models.OrderPending, order.Status)
	assert.EqualValues(t, 3000, order.TotalCents)
	require.Len(t, order.Items, 1)
}

func TestCreateOrderValidation(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "ORD-2", 100, 5)

	tests := []struct {
		name  string
		body  map[string]any
		token string
		want  int
	}{
		{"empty items", map[string]any{"items": []any{}}, f.customer, http.StatusBadRequest},
		{"missing items", map[string]any{}, f.customer, http.StatusBadRequest},
		{"zero quantity", map[string]any{"items": []map[string]any{{"product_id": product.ID, "qty": 0}}}, f.customer, http.StatusBadRequest},
		{"negative quantity", map[string]any{"items": []map[string]any{{"product_id": product.ID, "qty": -3}}}, f.customer, http.StatusBadRequest},
		{"unknown product", map[string]any{"items": []map[string]any{{"product_id": 999999, "qty": 1}}}, f.customer, http.StatusNotFound},
		{"more than stock", map[string]any{"items": []map[string]any{{"product_id": product.ID, "qty": 500}}}, f.customer, http.StatusConflict},
		{"unauthenticated", map[string]any{"items": []map[string]any{{"product_id": product.ID, "qty": 1}}}, "", http.StatusUnauthorized},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := f.do(t, http.MethodPost, "/api/v1/orders", tc.token, tc.body)
			assert.Equal(t, tc.want, w.Code, w.Body.String())
		})
	}
}

func TestListOrdersIsScopedToTheCaller(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "ORD-3", 100, 50)

	f.placeOrder(t, f.customer, product.ID, 1)
	f.placeOrder(t, f.customer, product.ID, 1)
	f.placeOrder(t, f.other, product.ID, 1)

	w := f.do(t, http.MethodGet, "/api/v1/orders", f.customer, nil)
	require.Equal(t, http.StatusOK, w.Code)

	page := decode[struct {
		Data  []models.Order `json:"data"`
		Total int64          `json:"total"`
	}](t, w)
	assert.EqualValues(t, 2, page.Total, "a customer must see only their own orders")
	for _, o := range page.Data {
		assert.Equal(t, f.customerID, o.UserID)
	}
}

// 404 rather than 403: "you may not see this order" still confirms it exists,
// which leaks whether an id is real to anyone willing to enumerate.
func TestGetOrderHidesOtherPeoplesOrders(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "ORD-4", 100, 5)
	order := f.placeOrder(t, f.customer, product.ID, 1)

	w := f.do(t, http.MethodGet, fmt.Sprintf("/api/v1/orders/%d", order.ID), f.customer, nil)
	assert.Equal(t, http.StatusOK, w.Code)

	w = f.do(t, http.MethodGet, fmt.Sprintf("/api/v1/orders/%d", order.ID), f.other, nil)
	assert.Equal(t, http.StatusNotFound, w.Code)

	// Admins see everything.
	w = f.do(t, http.MethodGet, fmt.Sprintf("/api/v1/orders/%d", order.ID), f.admin, nil)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestOrderStatusEndpoint(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "ORD-5", 100, 5)
	order := f.placeOrder(t, f.customer, product.ID, 1)

	w := f.do(t, http.MethodGet, fmt.Sprintf("/api/v1/orders/%d/status", order.ID), f.customer, nil)
	require.Equal(t, http.StatusOK, w.Code)

	view := decode[services.OrderStatusView](t, w)
	assert.Equal(t, models.OrderPending, view.Status)
	assert.False(t, view.Terminal)
	// RabbitMQ is transport, not storage, so attempt history comes from Postgres.
	assert.Empty(t, view.Payments)
}

func TestCancelOrder(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "ORD-6", 100, 5)
	order := f.placeOrder(t, f.customer, product.ID, 2)

	w := f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/orders/%d/cancel", order.ID), f.customer, nil)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	body := decode[struct {
		Pending bool `json:"pending"`
	}](t, w)
	assert.False(t, body.Pending, "nothing was in flight, so the cancel is final")

	inv, err := f.store.Inventory().Get(context.Background(), product.ID)
	require.NoError(t, err)
	assert.Equal(t, 5, inv.Available, "cancelling returns the stock")

	// Cancelling again is a conflict, not a second cancellation.
	w = f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/orders/%d/cancel", order.ID), f.customer, nil)
	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestCancelSomeoneElsesOrder(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "ORD-7", 100, 5)
	order := f.placeOrder(t, f.customer, product.ID, 1)

	w := f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/orders/%d/cancel", order.ID), f.other, nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// An Idempotency-Key makes retrying a request whose response was lost safe.
func TestIdempotencyKeyHeaderIsHonoured(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "ORD-8", 100, 10)

	body := map[string]any{"items": []map[string]any{{"product_id": product.ID, "qty": 1}}}

	send := func() *httptest.ResponseRecorder {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/orders", bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+f.customer)
		req.Header.Set("Idempotency-Key", "retry-me-once")
		w := httptest.NewRecorder()
		f.engine.ServeHTTP(w, req)
		return w
	}

	first, second := send(), send()
	require.Equal(t, http.StatusAccepted, first.Code)
	require.Equal(t, http.StatusAccepted, second.Code)

	assert.Equal(t, decode[models.Order](t, first).ID, decode[models.Order](t, second).ID,
		"the retry must return the original order")

	inv, err := f.store.Inventory().Get(context.Background(), product.ID)
	require.NoError(t, err)
	assert.Equal(t, 9, inv.Available, "the retry must not reserve stock twice")
}

// --- admin ------------------------------------------------------------------

func TestAdminRoutesRequireTheAdminRole(t *testing.T) {
	f := newAPIFixture(t)

	paths := []struct{ method, path string }{
		{http.MethodGet, "/api/v1/admin/orders"},
		{http.MethodGet, "/api/v1/admin/reports/daily"},
		{http.MethodGet, "/api/v1/admin/inventory/low-stock"},
	}

	for _, p := range paths {
		t.Run(p.path, func(t *testing.T) {
			w := f.do(t, p.method, p.path, f.customer, nil)
			assert.Equal(t, http.StatusForbidden, w.Code)

			w = f.do(t, p.method, p.path, "", nil)
			assert.Equal(t, http.StatusUnauthorized, w.Code)

			w = f.do(t, p.method, p.path, f.admin, nil)
			assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		})
	}
}

func TestAdminListsEveryonesOrders(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "ADM-1", 100, 50)

	f.placeOrder(t, f.customer, product.ID, 1)
	f.placeOrder(t, f.other, product.ID, 1)

	w := f.do(t, http.MethodGet, "/api/v1/admin/orders", f.admin, nil)
	require.Equal(t, http.StatusOK, w.Code)

	page := decode[struct {
		Total int64 `json:"total"`
	}](t, w)
	assert.EqualValues(t, 2, page.Total)

	// Filtering by status uses the (status, created_at DESC) composite index.
	w = f.do(t, http.MethodGet, "/api/v1/admin/orders?status=PENDING", f.admin, nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.EqualValues(t, 2, decode[struct {
		Total int64 `json:"total"`
	}](t, w).Total)
}

// Admins do not get a free hand with orders.status: the request goes through the
// same state machine as everything else, so an illegal edge is refused.
func TestAdminOrderStatusGoesThroughTheStateMachine(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "ADM-2", 100, 5)
	order := f.placeOrder(t, f.customer, product.ID, 1)

	// PENDING -> FULFILLED is not an edge.
	w := f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/admin/orders/%d/status", order.ID), f.admin,
		map[string]string{"status": "FULFILLED"})
	assert.Equal(t, http.StatusConflict, w.Code, w.Body.String())

	// Marking an order PAID by hand would mean money that was never taken, so it
	// is not offered at all.
	w = f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/admin/orders/%d/status", order.ID), f.admin,
		map[string]string{"status": "PAID"})
	assert.Equal(t, http.StatusBadRequest, w.Code)

	// Cancelling is legal from PENDING.
	w = f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/admin/orders/%d/status", order.ID), f.admin,
		map[string]string{"status": "CANCELLED"})
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, models.OrderCancelled, decode[models.Order](t, w).Status)
}

func TestAdminFulfilsAPaidOrder(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "ADM-3", 100, 5)
	order := f.placeOrder(t, f.customer, product.ID, 1)

	// Drive it to PAID through the state machine.
	ctx := context.Background()
	require.NoError(t, f.store.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		if _, err := f.store.Orders().Transition(ctx, tx, order.ID, models.OrderPending, models.OrderCharging); err != nil {
			return err
		}
		_, err := f.store.Orders().Transition(ctx, tx, order.ID, models.OrderCharging, models.OrderPaid)
		return err
	}))

	w := f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/admin/orders/%d/status", order.ID), f.admin,
		map[string]string{"status": "FULFILLED"})
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, models.OrderFulfilled, decode[models.Order](t, w).Status)
}

func TestAdminDailyReport(t *testing.T) {
	f := newAPIFixture(t)

	w := f.do(t, http.MethodGet, "/api/v1/admin/reports/daily", f.admin, nil)
	require.Equal(t, http.StatusOK, w.Code)

	report := decode[struct {
		Timezone string `json:"timezone"`
	}](t, w)
	assert.Equal(t, "UTC", report.Timezone, "the day boundary must be stated, not inherited")

	// Malformed dates are rejected rather than silently ignored.
	w = f.do(t, http.MethodGet, "/api/v1/admin/reports/daily?from=not-a-date", f.admin, nil)
	assert.Equal(t, http.StatusBadRequest, w.Code)

	w = f.do(t, http.MethodGet, "/api/v1/admin/reports/daily?from=2026-01-01&to=2026-02-01", f.admin, nil)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAdminLowStock(t *testing.T) {
	f := newAPIFixture(t)
	f.mustAPIProduct(t, "LOW-1", 100, 2)
	f.mustAPIProduct(t, "PLENTY-1", 100, 500)

	w := f.do(t, http.MethodGet, "/api/v1/admin/inventory/low-stock?threshold=5", f.admin, nil)
	require.Equal(t, http.StatusOK, w.Code)

	out := decode[struct {
		Data      []repository.InventoryWithProduct `json:"data"`
		Threshold int                               `json:"threshold"`
	}](t, w)
	assert.Equal(t, 5, out.Threshold)
	require.Len(t, out.Data, 1)
	assert.Equal(t, "LOW-1", out.Data[0].SKU)
}

// Restock is a read-modify-write by nature, so it carries an optimistic lock.
// Two admins editing at once would otherwise silently clobber each other.
func TestAdminRestockUsesOptimisticLocking(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "RESTOCK-1", 100, 3)

	w := f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/admin/inventory/%d", product.ID), f.admin,
		map[string]any{"available": 50, "version": 0})
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	inv := decode[models.Inventory](t, w)
	assert.Equal(t, 50, inv.Available)
	assert.Equal(t, 1, inv.Version, "a successful write bumps the version")

	// A stale version loses.
	w = f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/admin/inventory/%d", product.ID), f.admin,
		map[string]any{"available": 99, "version": 0})
	assert.Equal(t, http.StatusConflict, w.Code, "a stale version must not clobber a concurrent edit")

	w = f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/admin/inventory/%d", product.ID), f.customer,
		map[string]any{"available": 99, "version": 1})
	assert.Equal(t, http.StatusForbidden, w.Code)
}

// A price change must not rewrite history: order_items stores a snapshot.
func TestPriceChangesDoNotRewriteHistoricalOrders(t *testing.T) {
	f := newAPIFixture(t)
	product := f.mustAPIProduct(t, "SNAP-1", 1000, 10)

	order := f.placeOrder(t, f.customer, product.ID, 2)
	require.EqualValues(t, 2000, order.TotalCents)

	w := f.do(t, http.MethodPut, fmt.Sprintf("/api/v1/products/%d", product.ID), f.admin,
		map[string]any{"name": "SNAP-1", "price_cents": 9999, "active": true})
	require.Equal(t, http.StatusOK, w.Code)

	w = f.do(t, http.MethodGet, fmt.Sprintf("/api/v1/orders/%d", order.ID), f.customer, nil)
	require.Equal(t, http.StatusOK, w.Code)

	after := decode[models.Order](t, w)
	assert.EqualValues(t, 2000, after.TotalCents, "the order keeps the price it was placed at")
	require.Len(t, after.Items, 1)
	assert.EqualValues(t, 1000, after.Items[0].UnitPriceCents)
}
