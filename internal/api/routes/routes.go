// Package routes wires handlers, middleware and services into a Gin engine.
package routes

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/Sawalhy/backend-golang-task-2025/internal/api/handlers"
	"github.com/Sawalhy/backend-golang-task-2025/internal/api/middleware"
	"github.com/Sawalhy/backend-golang-task-2025/internal/config"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
)

type Deps struct {
	Cfg     *config.Config
	Log     *slog.Logger
	Auth    *services.AuthService
	Orders  *services.OrderService
	Catalog *services.CatalogService
	Redis   *redis.Client
	Health  func() error
}

// Build returns the configured engine.
//
// Middleware order is not arbitrary. Recovery must be outermost so it catches
// panics from everything inside it; RequestID must precede logging so the trace
// id exists by the time anything logs; auth must precede the rate limiter so the
// limiter can key on the user rather than lumping everyone behind one NAT into a
// single bucket.
func Build(d Deps) *gin.Engine {
	if d.Cfg.IsProduction() {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(
		middleware.Recovery(d.Log),
		middleware.RequestID(d.Log),
		middleware.Logging(d.Log),
	)

	// Trust no proxy headers by default. Gin trusts all inbound X-Forwarded-For
	// out of the box, which lets any client spoof its IP and walk straight past
	// an IP-keyed rate limit.
	_ = r.SetTrustedProxies(nil)

	limiter := middleware.NewRateLimiter(d.Redis, d.Cfg.Auth.RateLimitRPS, d.Cfg.Auth.RateLimitBurst)
	auth := middleware.Auth(d.Auth)

	r.GET("/healthz", func(c *gin.Context) {
		// Liveness only: the process is up. Deliberately does not touch the
		// database — a liveness probe that fails on a slow query gets the pod
		// killed during exactly the incident you need it alive for.
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	r.GET("/readyz", func(c *gin.Context) {
		// Readiness: can this instance actually serve? This one does check
		// dependencies, because an instance that cannot reach Postgres should be
		// taken out of the load balancer rather than returning 500s.
		if d.Health != nil {
			if err := d.Health(); err != nil {
				c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable", "error": err.Error()})
				return
			}
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	v1 := r.Group("/api/v1")

	authH := handlers.NewAuthHandler(d.Auth)
	productH := handlers.NewProductHandler(d.Catalog)
	orderH := handlers.NewOrderHandler(d.Orders)
	adminH := handlers.NewAdminHandler(d.Orders, d.Catalog)

	// --- public --------------------------------------------------------------
	// Rate limited by IP. Login especially: without a limit it is an offline
	// password guessing oracle with unlimited attempts.
	pub := v1.Group("")
	pub.Use(limiter.Middleware())
	{
		pub.POST("/users", authH.CreateUser)
		pub.POST("/auth/login", authH.Login)
		pub.GET("/products", productH.ListProducts)
		pub.GET("/products/:id", productH.GetProduct)
		pub.GET("/products/:id/inventory", productH.GetInventory)
	}

	// --- authenticated -------------------------------------------------------
	priv := v1.Group("")
	priv.Use(auth, limiter.Middleware())
	{
		priv.GET("/users/:id", authH.GetUser)
		priv.PUT("/users/:id", authH.UpdateUser)

		priv.POST("/orders", orderH.CreateOrder)
		priv.GET("/orders", orderH.ListOrders)
		priv.GET("/orders/:id", orderH.GetOrder)
		priv.GET("/orders/:id/status", orderH.GetOrderStatus)
		// PUT on a verb is not RESTful; the spec specifies it at README.md:86,
		// so it is implemented as specified and noted in the README.
		priv.PUT("/orders/:id/cancel", orderH.CancelOrder)
	}

	// --- admin ---------------------------------------------------------------
	admin := v1.Group("/admin")
	admin.Use(auth, middleware.RequireAdmin(), limiter.Middleware())
	{
		admin.GET("/orders", adminH.ListOrders)
		admin.PUT("/orders/:id/status", adminH.UpdateOrderStatus)
		admin.GET("/reports/daily", adminH.DailyReport)
		admin.GET("/inventory/low-stock", adminH.LowStock)
		admin.PUT("/inventory/:id", adminH.Restock)
	}

	// Product writes are admin-only but live under /products per README.md:77.
	adminProducts := v1.Group("/products")
	adminProducts.Use(auth, middleware.RequireAdmin(), limiter.Middleware())
	{
		adminProducts.POST("", productH.CreateProduct)
		adminProducts.PUT("/:id", productH.UpdateProduct)
	}

	return r
}
