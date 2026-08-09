package services

import (
	"context"
	"fmt"
	"strings"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
)

// CatalogService covers products, inventory reads and the admin reports. It
// exists so handlers never touch the repository directly — the layering rule is
// what keeps HTTP concerns out of the data layer and lets a worker reuse any of
// this without a request in hand.
type CatalogService struct {
	store *repository.Store
}

func NewCatalogService(store *repository.Store) *CatalogService {
	return &CatalogService{store: store}
}

type CreateProductInput struct {
	SKU         string
	Name        string
	Description string
	PriceCents  int64
	Currency    string
	Stock       int
}

func (s *CatalogService) CreateProduct(ctx context.Context, in CreateProductInput) (*models.Product, error) {
	if strings.TrimSpace(in.SKU) == "" {
		return nil, fmt.Errorf("%w: sku is required", models.ErrInvalidInput)
	}
	if in.PriceCents < 0 {
		return nil, fmt.Errorf("%w: price cannot be negative", models.ErrInvalidInput)
	}
	if in.Stock < 0 {
		return nil, fmt.Errorf("%w: stock cannot be negative", models.ErrInvalidInput)
	}

	currency := strings.ToUpper(strings.TrimSpace(in.Currency))
	if currency == "" {
		currency = "USD"
	}
	if len(currency) != 3 {
		return nil, fmt.Errorf("%w: currency must be a 3-letter code", models.ErrInvalidInput)
	}

	p := &models.Product{
		SKU:         strings.TrimSpace(in.SKU),
		Name:        strings.TrimSpace(in.Name),
		Description: in.Description,
		PriceCents:  in.PriceCents,
		Currency:    currency,
		Active:      true,
	}
	if err := s.store.Products().CreateWithInventory(ctx, p, in.Stock); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *CatalogService) GetProduct(ctx context.Context, id uint64) (*models.Product, error) {
	return s.store.Products().GetByID(ctx, id)
}

func (s *CatalogService) ListProducts(ctx context.Context, activeOnly bool, limit, offset int) ([]models.Product, int64, error) {
	return s.store.Products().List(ctx, repository.ProductFilter{
		ActiveOnly: activeOnly, Limit: limit, Offset: offset,
	})
}

type UpdateProductInput struct {
	ID          uint64
	Name        string
	Description string
	PriceCents  int64
	Active      bool
}

// UpdateProduct changes catalogue data only. It cannot touch stock: inventory
// moves through Reserve/Release/Commit or the explicit restock path, never
// through a general-purpose update that could clobber a concurrent reservation.
func (s *CatalogService) UpdateProduct(ctx context.Context, in UpdateProductInput) (*models.Product, error) {
	if in.PriceCents < 0 {
		return nil, fmt.Errorf("%w: price cannot be negative", models.ErrInvalidInput)
	}

	p := &models.Product{
		ID:          in.ID,
		Name:        strings.TrimSpace(in.Name),
		Description: in.Description,
		PriceCents:  in.PriceCents,
		Active:      in.Active,
	}
	if err := s.store.Products().Update(ctx, p); err != nil {
		return nil, err
	}
	// Existing orders keep the price they were placed at: order_items stores a
	// snapshot, so this change is not retroactive.
	return s.store.Products().GetByID(ctx, in.ID)
}

func (s *CatalogService) GetInventory(ctx context.Context, productID uint64) (*models.Inventory, error) {
	return s.store.Inventory().Get(ctx, productID)
}

func (s *CatalogService) LowStock(ctx context.Context, threshold, limit int) ([]repository.InventoryWithProduct, error) {
	if threshold < 0 {
		threshold = 0
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	return s.store.Inventory().LowStock(ctx, threshold, limit)
}

// Restock is the admin path, and the only writer that uses inventory.version.
// The hot path does not need optimistic locking because its UPDATE is already
// atomic; setting an absolute value is a read-modify-write, so two admins
// editing at once would otherwise silently clobber one another.
func (s *CatalogService) Restock(ctx context.Context, productID uint64, available, expectedVersion int) error {
	if available < 0 {
		return fmt.Errorf("%w: available cannot be negative", models.ErrInvalidInput)
	}
	return s.store.Inventory().SetAvailable(ctx, nil, productID, available, expectedVersion)
}

// The daily sales report lives in ReportService: it is served from two sources
// (the materialised rollup for closed days, live aggregation for today) and has
// its own scheduled job, which is more than a catalogue concern.
