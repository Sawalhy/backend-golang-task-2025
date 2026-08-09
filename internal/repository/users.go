package repository

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/database"
)

type UserRepo struct{ *Store }

func (s *Store) Users() *UserRepo { return &UserRepo{s} }

// Create relies on the unique index for uniqueness rather than a prior SELECT.
// "Check then insert" is a race: two signups with the same email both find it
// free, both insert, and the index is what actually stops the second one. Given
// the index has to be there anyway, the SELECT only adds a round trip and a false
// sense of safety.
func (r *UserRepo) Create(ctx context.Context, u *models.User) error {
	err := r.db.WithContext(ctx).Create(u).Error
	if err != nil {
		if database.IsUniqueViolation(err) {
			return fmt.Errorf("email %q: %w", u.Email, models.ErrAlreadyExists)
		}
		return fmt.Errorf("creating user: %w", err)
	}
	return nil
}

func (r *UserRepo) GetByID(ctx context.Context, id uint64) (*models.User, error) {
	var u models.User
	err := r.db.WithContext(ctx).Where("id = ?", id).Take(&u).Error
	if err != nil {
		if notFound(err) {
			return nil, fmt.Errorf("user %d: %w", id, models.ErrNotFound)
		}
		return nil, fmt.Errorf("reading user %d: %w", id, err)
	}
	return &u, nil
}

// GetByEmail backs login. The email column is citext, so comparison is
// case-insensitive without lower() defeating the unique index.
func (r *UserRepo) GetByEmail(ctx context.Context, email string) (*models.User, error) {
	var u models.User
	err := r.db.WithContext(ctx).Where("email = ?", email).Take(&u).Error
	if err != nil {
		if notFound(err) {
			return nil, models.ErrNotFound
		}
		return nil, fmt.Errorf("reading user by email: %w", err)
	}
	return &u, nil
}

func (r *UserRepo) Update(ctx context.Context, id uint64, name, email string) (*models.User, error) {
	res := r.db.WithContext(ctx).Model(&models.User{}).
		Where("id = ?", id).
		Updates(map[string]any{"name": name, "email": email, "updated_at": gorm.Expr("now()")})
	if res.Error != nil {
		if database.IsUniqueViolation(res.Error) {
			return nil, fmt.Errorf("email %q: %w", email, models.ErrAlreadyExists)
		}
		return nil, fmt.Errorf("updating user %d: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return nil, fmt.Errorf("user %d: %w", id, models.ErrNotFound)
	}
	return r.GetByID(ctx, id)
}

// --- audit -----------------------------------------------------------------

type AuditRepo struct{ *Store }

func (s *Store) Audit() *AuditRepo { return &AuditRepo{s} }

// Record writes an audit row. It takes tx so an audit entry commits atomically
// with the change it describes — an audit log that can disagree with the data is
// worse than none.
func (r *AuditRepo) Record(ctx context.Context, tx *gorm.DB, e *models.AuditLog) error {
	if err := r.txOrDB(tx).WithContext(ctx).Create(e).Error; err != nil {
		return fmt.Errorf("recording audit entry: %w", err)
	}
	return nil
}
