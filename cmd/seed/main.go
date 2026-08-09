// Command seed loads demo data: two users and a catalogue with deliberately
// awkward stock levels.
//
// It is idempotent — an existing row is left alone rather than duplicated — so
// it can run on every `docker compose up` without accumulating junk.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/Sawalhy/backend-golang-task-2025/internal/config"
	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/repository"
	"github.com/Sawalhy/backend-golang-task-2025/internal/services"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/database"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := logger.New(cfg.LogLevel, false)
	ctx := context.Background()

	db, err := database.Open(ctx, database.Options{
		DSN:          cfg.DB.DSN,
		MaxOpenConns: 4,
		MaxIdleConns: 2,
	}, log)
	if err != nil {
		return err
	}
	defer func() { _ = database.Close(db) }()

	store := repository.New(db, cfg.Order.DeadlockRetries)
	auth := services.NewAuthService(store, cfg.Auth)
	catalog := services.NewCatalogService(store)

	users := []services.RegisterInput{
		{Email: "admin@example.com", Password: "admin12345", Name: "Admin", Role: models.RoleAdmin},
		{Email: "sarah@example.com", Password: "customer123", Name: "Sarah", Role: models.RoleCustomer},
		{Email: "omar@example.com", Password: "customer123", Name: "Omar", Role: models.RoleCustomer},
	}
	for _, u := range users {
		if _, err := auth.Register(ctx, u); err != nil {
			if errors.Is(err, models.ErrAlreadyExists) {
				log.Info("user already present, skipping", "email", u.Email)
				continue
			}
			return err
		}
		log.Info("seeded user", "email", u.Email, "role", u.Role)
	}

	// Stock levels chosen to make the interesting paths reachable by hand:
	// the single-unit product is the one to hammer concurrently to prove
	// nothing oversells.
	products := []services.CreateProductInput{
		{SKU: "LAPTOP-01", Name: "ThinkPad X1", Description: "14in ultrabook", PriceCents: 189900, Stock: 25},
		{SKU: "MOUSE-01", Name: "Wireless Mouse", Description: "Ergonomic", PriceCents: 4599, Stock: 500},
		{SKU: "KEYB-01", Name: "Mechanical Keyboard", Description: "Tactile switches", PriceCents: 12900, Stock: 40},
		{SKU: "MONITOR-01", Name: "27in 4K Monitor", Description: "IPS panel", PriceCents: 54900, Stock: 8},
		{SKU: "RARE-01", Name: "Last One In Stock", Description: "Exactly one unit — the oversell test", PriceCents: 99900, Stock: 1},
		{SKU: "LOWSTOCK-01", Name: "Nearly Gone", Description: "Trips the low-stock alert", PriceCents: 2999, Stock: 3},
	}
	for _, p := range products {
		created, err := catalog.CreateProduct(ctx, p)
		if err != nil {
			if errors.Is(err, models.ErrAlreadyExists) {
				log.Info("product already present, skipping", "sku", p.SKU)
				continue
			}
			return err
		}
		log.Info("seeded product", "sku", created.SKU, "id", created.ID, "stock", p.Stock)
	}

	log.Info("seed complete",
		"admin", "admin@example.com / admin12345",
		"customer", "sarah@example.com / customer123")
	return nil
}
