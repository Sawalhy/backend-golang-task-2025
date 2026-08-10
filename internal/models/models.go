// Package models holds the domain entities and the rules that are pure functions
// of them. It imports no Gin, no GORM callbacks, and does no I/O.
//
// Schema is owned by migrations/, not by these structs: GORM's AutoMigrate never
// runs outside tests (CLAUDE.md), because it cannot drop a column and will
// silently diverge from the migration history. Tags here are for mapping only.
package models

import (
	"time"

	"github.com/google/uuid"
)

// Money is integer cents throughout. Never float: 0.1 + 0.2 != 0.3 in IEEE 754
// and it surfaces in a financial report eventually.

type User struct {
	ID           uint64    `gorm:"primaryKey"          json:"id"`
	Email        string    `gorm:"column:email"        json:"email"`
	PasswordHash string    `gorm:"column:password_hash" json:"-"` // never serialised
	Name         string    `gorm:"column:name"         json:"name"`
	Role         UserRole  `gorm:"column:role"         json:"role"`
	CreatedAt    time.Time `                            json:"created_at"`
	UpdatedAt    time.Time `                            json:"updated_at"`
}

func (User) TableName() string { return "users" }

type Product struct {
	ID          uint64    `gorm:"primaryKey"            json:"id"`
	SKU         string    `gorm:"column:sku"            json:"sku"`
	Name        string    `gorm:"column:name"           json:"name"`
	Description string    `gorm:"column:description"    json:"description"`
	PriceCents  int64     `gorm:"column:price_cents"    json:"price_cents"`
	Currency    string    `gorm:"column:currency"       json:"currency"`
	Active      bool      `gorm:"column:active"         json:"active"`
	CreatedAt   time.Time `                              json:"created_at"`
	UpdatedAt   time.Time `                              json:"updated_at"`
}

func (Product) TableName() string { return "products" }

// Inventory has no status column on purpose: it is a counter with an invariant,
// not a lifecycle. available >= 0 is a CHECK constraint, so the oversell
// guarantee holds even when application code is wrong.
type Inventory struct {
	ProductID uint64    `gorm:"primaryKey;column:product_id" json:"product_id"`
	Available int       `gorm:"column:available"             json:"available"`
	Reserved  int       `gorm:"column:reserved"              json:"reserved"`
	Version   int       `gorm:"column:version"               json:"version"`
	UpdatedAt time.Time `                                     json:"updated_at"`
}

func (Inventory) TableName() string { return "inventory" }

type Order struct {
	ID             uint64      `gorm:"primaryKey"               json:"id"`
	UserID         uint64      `gorm:"column:user_id"           json:"user_id"`
	Status         OrderStatus `gorm:"column:status"            json:"status"`
	TotalCents     int64       `gorm:"column:total_cents"       json:"total_cents"`
	Currency       string      `gorm:"column:currency"          json:"currency"`
	IdempotencyKey *string     `gorm:"column:idempotency_key"   json:"-"`
	CreatedAt      time.Time   `                                 json:"created_at"`
	UpdatedAt      time.Time   `                                 json:"updated_at"`
	PaidAt         *time.Time  `gorm:"column:paid_at"           json:"paid_at,omitempty"`
	CancelledAt    *time.Time  `gorm:"column:cancelled_at"      json:"cancelled_at,omitempty"`

	Items []OrderItem `gorm:"foreignKey:OrderID" json:"items,omitempty"`
}

func (Order) TableName() string { return "orders" }

type OrderItem struct {
	ID        uint64 `gorm:"primaryKey"       json:"id"`
	OrderID   uint64 `gorm:"column:order_id"  json:"order_id"`
	ProductID uint64 `gorm:"column:product_id" json:"product_id"`
	Qty       int    `gorm:"column:qty"       json:"qty"`
	// UnitPriceCents is a snapshot taken at order time. If this were a join to
	// products.price, an admin raising a price would retroactively rewrite every
	// historical order and every past report.
	UnitPriceCents int64 `gorm:"column:unit_price_cents" json:"unit_price_cents"`
}

func (OrderItem) TableName() string { return "order_items" }

// Reservation exists because of failure mode F: a customer abandons checkout and
// stock is held hostage. The reaper expires these.
type Reservation struct {
	ID        uint64            `gorm:"primaryKey"        json:"id"`
	OrderID   uint64            `gorm:"column:order_id"   json:"order_id"`
	ProductID uint64            `gorm:"column:product_id" json:"product_id"`
	Qty       int               `gorm:"column:qty"        json:"qty"`
	Status    ReservationStatus `gorm:"column:status"     json:"status"`
	ExpiresAt time.Time         `gorm:"column:expires_at" json:"expires_at"`
	CreatedAt time.Time         `                          json:"created_at"`
	UpdatedAt time.Time         `                          json:"updated_at"`
}

func (Reservation) TableName() string { return "reservations" }

// Payment.ID IS the provider idempotency key. One row per payment intent,
// committed before the provider call and reused across every retry of that
// intent — a fresh key per attempt means a charge per attempt.
type Payment struct {
	ID          uuid.UUID     `gorm:"primaryKey;column:id"  json:"id"`
	OrderID     uint64        `gorm:"column:order_id"       json:"order_id"`
	Status      PaymentStatus `gorm:"column:status"         json:"status"`
	AmountCents int64         `gorm:"column:amount_cents"   json:"amount_cents"`
	Provider    string        `gorm:"column:provider"       json:"provider"`
	ProviderRef *string       `gorm:"column:provider_ref"   json:"provider_ref,omitempty"`
	Attempts    int           `gorm:"column:attempts"       json:"attempts"`
	CreatedAt   time.Time     `                              json:"created_at"`
	UpdatedAt   time.Time     `                              json:"updated_at"`
}

func (Payment) TableName() string { return "payments" }

// IdempotencyKey is the payment's own id. Named as a method so call sites read
// as intent rather than as a field access that could be swapped for something
// per-attempt.
func (p Payment) IdempotencyKey() string { return p.ID.String() }

// Outbox is the dual-write fix (failure mode B). The event row is written in the
// same transaction as the state change it describes, so "committed but never
// queued" cannot happen. The relay publishes it afterwards.
type Outbox struct {
	ID         uint64     `gorm:"primaryKey"         json:"id"`
	EventID    uuid.UUID  `gorm:"column:event_id"    json:"event_id"`
	RoutingKey string     `gorm:"column:routing_key" json:"routing_key"`
	Payload    []byte     `gorm:"column:payload;type:jsonb" json:"payload"`
	CreatedAt  time.Time  `                           json:"created_at"`
	SentAt     *time.Time `gorm:"column:sent_at"     json:"sent_at,omitempty"`
	Attempts   int        `gorm:"column:attempts"    json:"attempts"`
	// TraceID is the W3C traceparent of the request that produced this event.
	// The relay copies it onto the AMQP message so the consumer's span joins the
	// originating request's trace — the only way to correlate two processes that
	// share no call stack. Empty when tracing is disabled.
	TraceID string `gorm:"column:trace_id" json:"trace_id,omitempty"`
}

func (Outbox) TableName() string { return "outbox" }

type Notification struct {
	ID         uint64             `gorm:"primaryKey"          json:"id"`
	OrderID    uint64             `gorm:"column:order_id"     json:"order_id"`
	Channel    string             `gorm:"column:channel"      json:"channel"` // email | sms
	Kind       string             `gorm:"column:kind"         json:"kind"`
	Status     NotificationStatus `gorm:"column:status"       json:"status"`
	LeaseUntil *time.Time         `gorm:"column:lease_until"  json:"lease_until,omitempty"`
	Attempts   int                `gorm:"column:attempts"     json:"attempts"`
	LastError  *string            `gorm:"column:last_error"   json:"last_error,omitempty"`
	CreatedAt  time.Time          `                            json:"created_at"`
	SentAt     *time.Time         `gorm:"column:sent_at"      json:"sent_at,omitempty"`
}

func (Notification) TableName() string { return "notifications" }

type AuditLog struct {
	ID          uint64    `gorm:"primaryKey"            json:"id"`
	ActorUserID *uint64   `gorm:"column:actor_user_id"  json:"actor_user_id,omitempty"`
	EntityType  string    `gorm:"column:entity_type"    json:"entity_type"`
	EntityID    string    `gorm:"column:entity_id"      json:"entity_id"`
	Action      string    `gorm:"column:action"         json:"action"`
	Before      []byte    `gorm:"column:before;type:jsonb" json:"before,omitempty"`
	After       []byte    `gorm:"column:after;type:jsonb"  json:"after,omitempty"`
	CreatedAt   time.Time `                              json:"created_at"`
}

func (AuditLog) TableName() string { return "audit_logs" }

type DailySalesRollup struct {
	Day         time.Time `gorm:"primaryKey;column:day" json:"day"`
	OrdersCount int       `gorm:"column:orders_count"   json:"orders_count"`
	GrossCents  int64     `gorm:"column:gross_cents"    json:"gross_cents"`
	Currency    string    `gorm:"column:currency"       json:"currency"`
	ComputedAt  time.Time `gorm:"column:computed_at"    json:"computed_at"`
}

func (DailySalesRollup) TableName() string { return "daily_sales_rollup" }
