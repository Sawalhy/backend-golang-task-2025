package repository

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
)

// InventoryRepo is the hot path. Every statement here is one round trip with no
// read-modify-write gap, because that gap is exactly failure mode A.
type InventoryRepo struct{ *Store }

func (s *Store) Inventory() *InventoryRepo { return &InventoryRepo{s} }

// Reserve moves qty from available to reserved for one product.
//
// This is the single most important statement in the system. The naive version
// is SELECT available, check it in Go, then UPDATE — and between the SELECT and
// the UPDATE another transaction can do the same thing, so both see stock 1,
// both decide yes, and the last item is sold twice.
//
// Here the check lives in the WHERE clause, so the read and the write are the
// same statement and cannot interleave. Postgres takes a row lock for the
// duration of a single UPDATE, so the second transaction blocks, then re-evaluates
// `available >= qty` against the committed result of the first.
//
// Two independent guards, deliberately:
//
//	WHERE available >= qty     — the fast path, returns a clean 409
//	CHECK (available >= 0)     — the database's own guarantee, in migration 001,
//	                             which holds even if this query is ever wrong
//
// RowsAffected == 0 means the row exists but had insufficient stock, OR the
// product has no inventory row at all. Both are ErrInsufficientStock to the
// caller: the customer cannot have it either way.
func (r *InventoryRepo) Reserve(ctx context.Context, tx *gorm.DB, productID uint64, qty int) error {
	if qty <= 0 {
		return fmt.Errorf("%w: qty must be positive, got %d", models.ErrInvalidInput, qty)
	}

	res := r.txOrDB(tx).WithContext(ctx).Exec(`
		UPDATE inventory
		   SET available  = available - ?,
		       reserved   = reserved  + ?,
		       updated_at = now()
		 WHERE product_id = ?
		   AND available >= ?`,
		qty, qty, productID, qty)

	if res.Error != nil {
		return fmt.Errorf("reserving %d of product %d: %w", qty, productID, res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("reserving %d of product %d: %w", qty, productID, models.ErrInsufficientStock)
	}
	return nil
}

// Release returns reserved stock to available. Used when an order is cancelled,
// expires, or its payment is declined.
//
// The `reserved >= qty` guard makes a double release impossible: replaying the
// same release twice would otherwise inflate available and invent stock that
// does not exist. Losing that race is not an error — at-least-once delivery
// means the second call is expected — so the caller gets ErrLostRace and decides.
func (r *InventoryRepo) Release(ctx context.Context, tx *gorm.DB, productID uint64, qty int) error {
	if qty <= 0 {
		return fmt.Errorf("%w: qty must be positive, got %d", models.ErrInvalidInput, qty)
	}

	res := r.txOrDB(tx).WithContext(ctx).Exec(`
		UPDATE inventory
		   SET available  = available + ?,
		       reserved   = reserved  - ?,
		       updated_at = now()
		 WHERE product_id = ?
		   AND reserved  >= ?`,
		qty, qty, productID, qty)

	if res.Error != nil {
		return fmt.Errorf("releasing %d of product %d: %w", qty, productID, res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("releasing %d of product %d: %w", qty, productID, models.ErrLostRace)
	}
	return nil
}

// Commit consumes reserved stock permanently: the goods have shipped, so the
// units leave `reserved` without returning to `available`.
func (r *InventoryRepo) Commit(ctx context.Context, tx *gorm.DB, productID uint64, qty int) error {
	if qty <= 0 {
		return fmt.Errorf("%w: qty must be positive, got %d", models.ErrInvalidInput, qty)
	}

	res := r.txOrDB(tx).WithContext(ctx).Exec(`
		UPDATE inventory
		   SET reserved   = reserved - ?,
		       updated_at = now()
		 WHERE product_id = ?
		   AND reserved  >= ?`,
		qty, productID, qty)

	if res.Error != nil {
		return fmt.Errorf("committing %d of product %d: %w", qty, productID, res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("committing %d of product %d: %w", qty, productID, models.ErrLostRace)
	}
	return nil
}

// Get reads one inventory row. Read-only, so no lock and no transaction.
func (r *InventoryRepo) Get(ctx context.Context, productID uint64) (*models.Inventory, error) {
	var inv models.Inventory
	err := r.db.WithContext(ctx).Where("product_id = ?", productID).Take(&inv).Error
	if err != nil {
		if notFound(err) {
			return nil, fmt.Errorf("inventory for product %d: %w", productID, models.ErrNotFound)
		}
		return nil, fmt.Errorf("reading inventory for product %d: %w", productID, err)
	}
	return &inv, nil
}

// LowStock backs GET /admin/inventory/low-stock.
//
// This is a sequential scan on purpose. An index on inventory.available would
// serve this query, but `available` is the most-updated column in the system, so
// indexing it taxes every single order — and an indexed column also loses
// heap-only-tuple updates, adding an index write to every reservation. Paying
// that on the write path to speed up an occasional admin query over ~10k rows
// (a few ms) is the wrong trade. See DESIGN_NOTES.md §5.18.
func (r *InventoryRepo) LowStock(ctx context.Context, threshold, limit int) ([]InventoryWithProduct, error) {
	var out []InventoryWithProduct

	err := r.db.WithContext(ctx).Raw(`
		SELECT i.product_id, i.available, i.reserved, i.updated_at,
		       p.sku, p.name
		  FROM inventory i
		  JOIN products  p ON p.id = i.product_id
		 WHERE i.available <= ?
		   AND p.active
		 ORDER BY i.available ASC
		 LIMIT ?`, threshold, limit).Scan(&out).Error
	if err != nil {
		return nil, fmt.Errorf("listing low stock: %w", err)
	}
	return out, nil
}

type InventoryWithProduct struct {
	ProductID uint64 `json:"product_id"`
	SKU       string `json:"sku"`
	Name      string `json:"name"`
	Available int    `json:"available"`
	Reserved  int    `json:"reserved"`
	UpdatedAt string `json:"updated_at"`
}

// SetAvailable is the admin restock path, and the one place the `version` column
// earns its keep. The hot path never needs optimistic locking because the
// conditional UPDATE is already atomic — but an admin setting an absolute value
// is a read-modify-write by nature, and two admins editing at once would silently
// clobber each other.
func (r *InventoryRepo) SetAvailable(ctx context.Context, tx *gorm.DB, productID uint64, available, expectedVersion int) error {
	res := r.txOrDB(tx).WithContext(ctx).Exec(`
		UPDATE inventory
		   SET available  = ?,
		       version    = version + 1,
		       updated_at = now()
		 WHERE product_id = ?
		   AND version     = ?`,
		available, productID, expectedVersion)

	if res.Error != nil {
		return fmt.Errorf("setting stock for product %d: %w", productID, res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("setting stock for product %d: %w", productID, models.ErrLostRace)
	}
	return nil
}
