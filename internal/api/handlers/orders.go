package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/Sawalhy/backend-golang-task-2025/internal/api/middleware"
	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
)

type OrderHandler struct {
	orders *services.OrderService
	hub    *services.StatusHub
}

func NewOrderHandler(orders *services.OrderService, hub *services.StatusHub) *OrderHandler {
	return &OrderHandler{orders: orders, hub: hub}
}

type orderLineRequest struct {
	ProductID uint64 `json:"product_id" binding:"required,min=1"`
	Qty       int    `json:"qty"        binding:"required,min=1"`
}

type createOrderRequest struct {
	Items []orderLineRequest `json:"items" binding:"required,min=1,max=50,dive"`
}

// CreateOrder handles POST /api/v1/orders.
//
// Returns 202 Accepted, not 201 Created, and the distinction is the architecture
// showing through. What has definitely happened when this returns: the stock is
// reserved and the order exists. What has NOT happened: the payment. That runs in
// a worker moments later, because holding a database transaction open across a
// multi-second provider call would serialise every other buyer of those products
// behind it.
//
// 202 says "accepted for processing, outcome pending" — which is the truth.
// Answering 201 would imply a completed resource and invite clients to treat the
// order as paid.
//
// Clients poll GET /orders/{id}/status, or subscribe to the SSE stream, for the
// outcome. An Idempotency-Key header makes retrying this call safe.
//
//	@Summary	Place an order
//	@Tags		orders
//	@Accept		json
//	@Produce	json
//	@Param		Idempotency-Key	header	string				false	"client retry key"
//	@Param		body			body	createOrderRequest	true	"order"
//	@Success	202	{object}	models.Order
//	@Failure	400,401,409	{object}	ErrorResponse	"409 = insufficient stock"
//	@Security	BearerAuth
//	@Router		/orders [post]
func (h *OrderHandler) CreateOrder(c *gin.Context) {
	var req createOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, wrapBinding(err))
		return
	}

	lines := make([]services.OrderLine, 0, len(req.Items))
	for _, it := range req.Items {
		lines = append(lines, services.OrderLine{ProductID: it.ProductID, Qty: it.Qty})
	}

	var key *string
	if raw := c.GetHeader("Idempotency-Key"); raw != "" {
		key = &raw
	}

	order, err := h.orders.Create(c.Request.Context(), services.CreateOrderInput{
		UserID:         middleware.UserID(c),
		Lines:          lines,
		IdempotencyKey: key,
	})
	if err != nil {
		fail(c, err)
		return
	}

	c.JSON(http.StatusAccepted, order)
}

// ListOrders handles GET /api/v1/orders — the caller's own orders only.
//
//	@Summary	List your orders
//	@Tags		orders
//	@Produce	json
//	@Param		limit	query		int		false	"page size"
//	@Param		offset	query		int		false	"offset"
//	@Param		status	query		string	false	"filter by status"
//	@Success	200		{object}	PageResponse[models.Order]
//	@Security	BearerAuth
//	@Router		/orders [get]
func (h *OrderHandler) ListOrders(c *gin.Context) {
	p := pagination(c)
	userID := middleware.UserID(c)

	f := repository.ListFilter{UserID: &userID, Limit: p.Limit, Offset: p.Offset}
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

// GetOrder handles GET /api/v1/orders/{id}.
//
//	@Summary	Get an order
//	@Tags		orders
//	@Produce	json
//	@Param		id	path		int	true	"order id"
//	@Success	200	{object}	models.Order
//	@Failure	404	{object}	ErrorResponse
//	@Security	BearerAuth
//	@Router		/orders/{id} [get]
func (h *OrderHandler) GetOrder(c *gin.Context) {
	id, ok := idParam(c, "id")
	if !ok {
		return
	}
	o, err := h.orders.Get(c.Request.Context(), id, middleware.UserID(c), middleware.IsAdmin(c))
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, o)
}

// GetOrderStatus handles GET /api/v1/orders/{id}/status.
//
// A separate endpoint from GET /orders because the order is processed
// asynchronously: this is the one clients poll, and it carries the payment
// attempt history that answers "what actually happened to my charge?".
//
//	@Summary	Get order status and payment attempts
//	@Tags		orders
//	@Produce	json
//	@Param		id	path		int	true	"order id"
//	@Success	200	{object}	services.OrderStatusView
//	@Failure	404	{object}	ErrorResponse
//	@Security	BearerAuth
//	@Router		/orders/{id}/status [get]
func (h *OrderHandler) GetOrderStatus(c *gin.Context) {
	id, ok := idParam(c, "id")
	if !ok {
		return
	}
	view, err := h.orders.Status(c.Request.Context(), id, middleware.UserID(c), middleware.IsAdmin(c))
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, view)
}

// CancelOrder handles PUT /api/v1/orders/{id}/cancel.
//
// Two success codes, and the difference is real:
//
//	200 — cancelled outright. Nothing was charged, stock is already back.
//	202 — cancellation ACCEPTED but not final. A charge is in flight and we do
//	      not yet know whether it landed. The order is CANCELLING; the payment
//	      worker will settle it as CANCELLED or CANCELLED_REFUNDED.
//
// Returning 200 in the second case would claim a cancellation the system cannot
// yet guarantee, which is precisely the lie failure mode D produces.
//
//	@Summary	Cancel an order
//	@Tags		orders
//	@Produce	json
//	@Param		id	path		int	true	"order id"
//	@Success	200	{object}	services.CancelResult	"cancelled"
//	@Success	202	{object}	services.CancelResult	"cancellation pending payment outcome"
//	@Failure	404,409	{object}	ErrorResponse
//	@Security	BearerAuth
//	@Router		/orders/{id}/cancel [put]
func (h *OrderHandler) CancelOrder(c *gin.Context) {
	id, ok := idParam(c, "id")
	if !ok {
		return
	}

	res, err := h.orders.Cancel(c.Request.Context(), id, middleware.UserID(c), middleware.IsAdmin(c))
	if err != nil {
		fail(c, err)
		return
	}

	status := http.StatusOK
	if res.Pending {
		status = http.StatusAccepted
	}
	c.JSON(status, gin.H{
		"order":   res.Order,
		"pending": res.Pending,
		"message": cancelMessage(res.Pending),
	})
}

func cancelMessage(pending bool) string {
	if pending {
		return "cancellation accepted; a payment is in flight and will be refunded if it succeeds"
	}
	return "order cancelled and stock released"
}
