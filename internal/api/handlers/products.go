package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
)

type ProductHandler struct {
	catalog *services.CatalogService
}

func NewProductHandler(catalog *services.CatalogService) *ProductHandler {
	return &ProductHandler{catalog: catalog}
}

type createProductRequest struct {
	SKU         string `json:"sku"          binding:"required,min=1,max=64"`
	Name        string `json:"name"         binding:"required,min=1,max=200"`
	Description string `json:"description"  binding:"max=2000"`
	// Money crosses the wire as integer cents, the same as it is stored. A JSON
	// float here would reintroduce the rounding the schema exists to avoid.
	PriceCents int64  `json:"price_cents" binding:"required,min=0"`
	Currency   string `json:"currency"    binding:"omitempty,len=3"`
	Stock      int    `json:"stock"       binding:"min=0"`
}

// ListProducts handles GET /api/v1/products.
//
//	@Summary	List products
//	@Tags		products
//	@Produce	json
//	@Param		limit	query		int	false	"page size (max 100)"
//	@Param		offset	query		int	false	"offset"
//	@Success	200		{object}	PageResponse[models.Product]
//	@Router		/products [get]
func (h *ProductHandler) ListProducts(c *gin.Context) {
	p := pagination(c)

	// Customers see only active products; the admin list shows everything.
	items, total, err := h.catalog.ListProducts(c.Request.Context(), c.Query("all") != "true", p.Limit, p.Offset)
	if err != nil {
		fail(c, err)
		return
	}

	c.JSON(http.StatusOK, PageResponse[models.Product]{
		Data: items, Total: total, Limit: p.Limit, Offset: p.Offset,
	})
}

// GetProduct handles GET /api/v1/products/{id}.
//
//	@Summary	Get product details
//	@Tags		products
//	@Produce	json
//	@Param		id	path		int	true	"product id"
//	@Success	200	{object}	models.Product
//	@Failure	404	{object}	ErrorResponse
//	@Router		/products/{id} [get]
func (h *ProductHandler) GetProduct(c *gin.Context) {
	id, ok := idParam(c, "id")
	if !ok {
		return
	}
	p, err := h.catalog.GetProduct(c.Request.Context(), id)
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, p)
}

// CreateProduct handles POST /api/v1/products (admin).
//
//	@Summary	Create a product
//	@Tags		products
//	@Accept		json
//	@Produce	json
//	@Param		body	body		createProductRequest	true	"product"
//	@Success	201		{object}	models.Product
//	@Failure	400,403,409	{object}	ErrorResponse
//	@Security	BearerAuth
//	@Router		/products [post]
func (h *ProductHandler) CreateProduct(c *gin.Context) {
	var req createProductRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, wrapBinding(err))
		return
	}

	p, err := h.catalog.CreateProduct(c.Request.Context(), services.CreateProductInput{
		SKU:         req.SKU,
		Name:        req.Name,
		Description: req.Description,
		PriceCents:  req.PriceCents,
		Currency:    req.Currency,
		Stock:       req.Stock,
	})
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusCreated, p)
}

type updateProductRequest struct {
	Name        string `json:"name"        binding:"required,min=1,max=200"`
	Description string `json:"description" binding:"max=2000"`
	PriceCents  int64  `json:"price_cents" binding:"min=0"`
	Active      *bool  `json:"active"      binding:"required"`
}

// UpdateProduct handles PUT /api/v1/products/{id} (admin).
//
//	@Summary	Update a product
//	@Tags		products
//	@Accept		json
//	@Produce	json
//	@Param		id		path		int						true	"product id"
//	@Param		body	body		updateProductRequest	true	"product"
//	@Success	200		{object}	models.Product
//	@Failure	400,403,404	{object}	ErrorResponse
//	@Security	BearerAuth
//	@Router		/products/{id} [put]
func (h *ProductHandler) UpdateProduct(c *gin.Context) {
	id, ok := idParam(c, "id")
	if !ok {
		return
	}

	var req updateProductRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, wrapBinding(err))
		return
	}

	p, err := h.catalog.UpdateProduct(c.Request.Context(), services.UpdateProductInput{
		ID:          id,
		Name:        req.Name,
		Description: req.Description,
		PriceCents:  req.PriceCents,
		Active:      *req.Active,
	})
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, p)
}

// GetInventory handles GET /api/v1/products/{id}/inventory.
//
// Note what this returns: `available` is a point-in-time reading, not a promise.
// By the time the client renders it another customer may have reserved the last
// unit. Only the conditional UPDATE at order time decides who actually gets it,
// which is why the API never "checks stock then orders" as two steps.
//
//	@Summary	Check inventory for a product
//	@Tags		products
//	@Produce	json
//	@Param		id	path		int	true	"product id"
//	@Success	200	{object}	models.Inventory
//	@Failure	404	{object}	ErrorResponse
//	@Router		/products/{id}/inventory [get]
func (h *ProductHandler) GetInventory(c *gin.Context) {
	id, ok := idParam(c, "id")
	if !ok {
		return
	}
	inv, err := h.catalog.GetInventory(c.Request.Context(), id)
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, inv)
}
