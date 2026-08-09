package handlers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Sawalhy/backend-golang-task-2025/internal/api/middleware"
	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
)

type AdminHandler struct {
	orders  *services.OrderService
	catalog *services.CatalogService
}

func NewAdminHandler(orders *services.OrderService, catalog *services.CatalogService) *AdminHandler {
	return &AdminHandler{orders: orders, catalog: catalog}
}

// ListOrders handles GET /api/v1/admin/orders — every user's orders.
//
//	@Summary	List all orders
//	@Tags		admin
//	@Produce	json
//	@Param		status	query		string	false	"filter by status"
//	@Param		limit	query		int		false	"page size"
//	@Param		offset	query		int		false	"offset"
//	@Success	200		{object}	PageResponse[models.Order]
//	@Security	BearerAuth
//	@Router		/admin/orders [get]
func (h *AdminHandler) ListOrders(c *gin.Context) {
	p := pagination(c)

	f := repository.ListFilter{Limit: p.Limit, Offset: p.Offset}
	if s := c.Query("status"); s != "" {
		st := models.OrderStatus(s)
		f.Status = &st
	}

	items, total, err := h.orders.List(c.Request.Context(), f)
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, PageResponse[models.Order]{
		Data: items, Total: total, Limit: p.Limit, Offset: p.Offset,
	})
}

type updateOrderStatusRequest struct {
	Status string `json:"status" binding:"required"`
}

// UpdateOrderStatus handles PUT /api/v1/admin/orders/{id}/status.
//
// Admins do not get a free hand with this column. The request goes through the
// same state machine as everything else, so "set this order to PAID" is refused
// unless PENDING -> ... -> PAID is a legal edge from where it currently sits.
// An admin endpoint that writes orders.status directly would be the one hole
// through which every invariant in this system leaks.
//
// Only transitions with no side effects elsewhere are exposed here: fulfilment
// and cancellation. Marking an order PAID by hand would mean money that was
// never taken, so it is not offered.
//
//	@Summary	Update an order's status
//	@Tags		admin
//	@Accept		json
//	@Produce	json
//	@Param		id		path		int							true	"order id"
//	@Param		body	body		updateOrderStatusRequest	true	"target status"
//	@Success	200		{object}	models.Order
//	@Failure	400,403,404,409	{object}	ErrorResponse
//	@Security	BearerAuth
//	@Router		/admin/orders/{id}/status [put]
func (h *AdminHandler) UpdateOrderStatus(c *gin.Context) {
	id, ok := idParam(c, "id")
	if !ok {
		return
	}

	var req updateOrderStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, wrapBinding(err))
		return
	}

	ctx := c.Request.Context()
	var err error

	switch models.OrderStatus(req.Status) {
	case models.OrderFulfilled:
		err = h.orders.Fulfil(ctx, id)
	case models.OrderCancelled:
		_, err = h.orders.Cancel(ctx, id, middleware.UserID(c), true)
	default:
		fail(c, wrapBinding(errUnsupportedStatus(req.Status)))
		return
	}
	if err != nil {
		fail(c, err)
		return
	}

	o, err := h.orders.Get(ctx, id, middleware.UserID(c), true)
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, o)
}

// DailyReport handles GET /api/v1/admin/reports/daily.
//
// Aggregated in SQL, not in Go. Pulling a day of orders into the process to sum
// a column is how report generation starves order processing — the aggregation
// belongs where the data already is.
//
//	@Summary	Daily sales report
//	@Tags		admin
//	@Produce	json
//	@Param		from	query		string	false	"inclusive start date, YYYY-MM-DD (UTC)"
//	@Param		to		query		string	false	"exclusive end date, YYYY-MM-DD (UTC)"
//	@Success	200		{array}		services.DailyReport
//	@Security	BearerAuth
//	@Router		/admin/reports/daily [get]
func (h *AdminHandler) DailyReport(c *gin.Context) {
	// Days are UTC. The spec does not say which timezone "daily" means, so the
	// boundary is documented rather than inherited from the server's locale.
	to := time.Now().UTC().AddDate(0, 0, 1).Truncate(24 * time.Hour)
	from := to.AddDate(0, 0, -30)

	if v := c.Query("from"); v != "" {
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			fail(c, wrapBinding(errBadDate("from")))
			return
		}
		from = t.UTC()
	}
	if v := c.Query("to"); v != "" {
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			fail(c, wrapBinding(errBadDate("to")))
			return
		}
		to = t.UTC()
	}

	rows, err := h.catalog.DailySales(c.Request.Context(), from, to)
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": rows, "from": from, "to": to, "timezone": "UTC"})
}

// LowStock handles GET /api/v1/admin/inventory/low-stock.
//
// This is a sequential scan and that is the intended trade: indexing
// inventory.available would tax every order to accelerate an occasional admin
// query. See DESIGN_NOTES.md §5.18.
//
//	@Summary	Low stock alerts
//	@Tags		admin
//	@Produce	json
//	@Param		threshold	query	int	false	"alert at or below this level (default 10)"
//	@Success	200			{array}	repository.InventoryWithProduct
//	@Security	BearerAuth
//	@Router		/admin/inventory/low-stock [get]
func (h *AdminHandler) LowStock(c *gin.Context) {
	threshold := 10
	if v, err := strconv.Atoi(c.Query("threshold")); err == nil && v >= 0 {
		threshold = v
	}

	rows, err := h.catalog.LowStock(c.Request.Context(), threshold, 100)
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": rows, "threshold": threshold})
}

type restockRequest struct {
	Available int `json:"available" binding:"min=0"`
	// Version is the optimistic lock. Two admins restocking at once would
	// otherwise silently overwrite each other's absolute value.
	Version int `json:"version"`
}

// Restock handles PUT /api/v1/admin/inventory/{id}.
//
//	@Summary	Set absolute stock level
//	@Tags		admin
//	@Accept		json
//	@Produce	json
//	@Param		id		path	int				true	"product id"
//	@Param		body	body	restockRequest	true	"stock"
//	@Success	200		{object}	models.Inventory
//	@Failure	400,403,404,409	{object}	ErrorResponse	"409 = version conflict"
//	@Security	BearerAuth
//	@Router		/admin/inventory/{id} [put]
func (h *AdminHandler) Restock(c *gin.Context) {
	id, ok := idParam(c, "id")
	if !ok {
		return
	}

	var req restockRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, wrapBinding(err))
		return
	}

	if err := h.catalog.Restock(c.Request.Context(), id, req.Available, req.Version); err != nil {
		fail(c, err)
		return
	}

	inv, err := h.catalog.GetInventory(c.Request.Context(), id)
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, inv)
}

type stringError string

func (e stringError) Error() string { return string(e) }

func errUnsupportedStatus(s string) error {
	return stringError("status " + s + " cannot be set directly; allowed: FULFILLED, CANCELLED")
}

func errBadDate(field string) error {
	return stringError(field + " must be a date in YYYY-MM-DD form")
}
