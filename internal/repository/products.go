package repository

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/database"
)

type ProductRepo struct{ *Store }

func (s *Store) Products() *ProductRepo { return &ProductRepo{s} }

// CreateWithInventory inserts a product and its inventory row together.
//
// One transaction, because a product without an inventory row is unbuyable: the
// conditional UPDATE in Reserve matches zero rows and every purchase returns
// "insufficient stock" for a product that looks in stock in the catalogue.
func (r *ProductRepo) CreateWithInventory(ctx context.Context, p *models.Product, stock int) error {
	return r.InTx(ctx, func(ctx context.Context, tx *gorm.DB) error {
		if err := tx.WithContext(ctx).Create(p).Error; err != nil {
			if database.IsUniqueViolation(err) {
				return fmt.Errorf("product sku %q: %w", p.SKU, models.ErrAlreadyExists)
			}
			return fmt.Errorf("creating product: %w", err)
		}
		inv := models.Inventory{ProductID: p.ID, Available: stock}
		if err := tx.WithContext(ctx).Create(&inv).Error; err != nil {
			return fmt.Errorf("creating inventory for product %d: %w", p.ID, err)
		}
		return nil
	})
}

func (r *ProductRepo) GetByID(ctx context.Context, id uint64) (*models.Product, error) {
	var p models.Product
	err := r.db.WithContext(ctx).Where("id = ?", id).Take(&p).Error
	if err != nil {
		if notFound(err) {
			return nil, fmt.Errorf("product %d: %w", id, models.ErrNotFound)
		}
		return nil, fmt.Errorf("reading product %d: %w", id, err)
	}
	return &p, nil
}

// LoadForOrder fetches every product referenced by an order in one query, inside
// the caller's transaction.
//
// One query, not one per line: N round trips inside a transaction hold locks for
// N network latencies, and this runs on the hot path. Only active products are
// returned, so an inactive product simply comes back missing and the caller
// reports which id it could not sell.
func (r *ProductRepo) LoadForOrder(ctx context.Context, tx *gorm.DB, ids []uint64) (map[uint64]models.Product, error) {
	if len(ids) == 0 {
		return map[uint64]models.Product{}, nil
	}

	var rows []models.Product
	err := r.txOrDB(tx).WithContext(ctx).
		Where("id IN ? AND active", ids).Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("loading products: %w", err)
	}

	out := make(map[uint64]models.Product, len(rows))
	for _, p := range rows {
		out[p.ID] = p
	}
	return out, nil
}

type ProductFilter struct {
	ActiveOnly bool
	Limit      int
	Offset     int
}

func (r *ProductRepo) List(ctx context.Context, f ProductFilter) ([]models.Product, int64, error) {
	q := r.db.WithContext(ctx).Model(&models.Product{})
	if f.ActiveOnly {
		q = q.Where("active")
	}

	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("counting products: %w", err)
	}

	var out []models.Product
	if err := q.Order("created_at DESC").Limit(f.Limit).Offset(f.Offset).Find(&out).Error; err != nil {
		return nil, 0, fmt.Errorf("listing products: %w", err)
	}
	return out, total, nil
}

func (r *ProductRepo) Update(ctx context.Context, p *models.Product) error {
	res := r.db.WithContext(ctx).Model(&models.Product{}).
		Where("id = ?", p.ID).
		Updates(map[string]any{
			"name":        p.Name,
			"description": p.Description,
			"price_cents": p.PriceCents,
			"active":      p.Active,
			"updated_at":  gorm.Expr("now()"),
		})
	if res.Error != nil {
		return fmt.Errorf("updating product %d: %w", p.ID, res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("product %d: %w", p.ID, models.ErrNotFound)
	}
	return nil
}
