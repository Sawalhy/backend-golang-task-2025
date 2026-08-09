package models

import (
	"database/sql/driver"
	"fmt"
	"slices"
)

// Status values mirror the Postgres enums created in migrations/000001_init.up.sql.
// Both halves have to agree; the test in status_test.go asserts that they do.

type OrderStatus string

const (
	OrderPending           OrderStatus = "PENDING"
	OrderCharging          OrderStatus = "CHARGING"
	OrderPaid              OrderStatus = "PAID"
	OrderFailed            OrderStatus = "FAILED"
	OrderCancelling        OrderStatus = "CANCELLING"
	OrderCancelled         OrderStatus = "CANCELLED"
	OrderCancelledRefunded OrderStatus = "CANCELLED_REFUNDED"
	OrderExpired           OrderStatus = "EXPIRED"
	OrderFulfilled         OrderStatus = "FULFILLED"
	OrderRefunded          OrderStatus = "REFUNDED"
)

type ReservationStatus string

const (
	ReservationHeld      ReservationStatus = "HELD"
	ReservationCommitted ReservationStatus = "COMMITTED"
	ReservationReleased  ReservationStatus = "RELEASED"
	ReservationExpired   ReservationStatus = "EXPIRED"
)

type PaymentStatus string

const (
	PaymentInitiated PaymentStatus = "INITIATED"
	PaymentSucceeded PaymentStatus = "SUCCEEDED"
	PaymentDeclined  PaymentStatus = "DECLINED"
	// PaymentUnknown means the provider call did not return a verdict. The
	// customer may or may not have been charged, so this state blocks opening a
	// second payment intent — see the partial unique index in migration 001.
	PaymentUnknown  PaymentStatus = "UNKNOWN"
	PaymentRefunded PaymentStatus = "REFUNDED"
)

type NotificationStatus string

const (
	NotificationUnclaimed NotificationStatus = "UNCLAIMED"
	NotificationSending   NotificationStatus = "SENDING"
	NotificationSent      NotificationStatus = "SENT"
	NotificationFailed    NotificationStatus = "FAILED"
)

type UserRole string

const (
	RoleCustomer UserRole = "CUSTOMER"
	RoleAdmin    UserRole = "ADMIN"
)

// orderTransitions is the whole order state machine. Go cannot express
// TypeScript's compile-time Next<S>, so this runtime map plus TestNoIllegalEdges
// stands in for it: every write to orders.status goes through Transition, and
// Transition consults this map.
//
// Absent keys are terminal by construction — PAID/FULFILLED/REFUNDED and the
// cancelled family have no outgoing edges beyond what is listed.
var orderTransitions = map[OrderStatus][]OrderStatus{
	OrderPending:    {OrderCharging, OrderCancelled, OrderExpired},
	OrderCharging:   {OrderPaid, OrderFailed, OrderCancelling},
	OrderCancelling: {OrderCancelled, OrderCancelledRefunded},
	OrderPaid:       {OrderFulfilled, OrderRefunded},
}

// CanTransition reports whether from → to is an edge in the state machine.
// It is pure: no database, no context. The CAS that actually performs the
// transition lives in the repository layer.
func CanTransition(from, to OrderStatus) bool {
	return slices.Contains(orderTransitions[from], to)
}

// TerminalOrderStatus reports whether an order can never change again.
func TerminalOrderStatus(s OrderStatus) bool {
	return len(orderTransitions[s]) == 0
}

// AllOrderStatuses is the authoritative list, used by the migration-parity test.
func AllOrderStatuses() []OrderStatus {
	return []OrderStatus{
		OrderPending, OrderCharging, OrderPaid, OrderFailed, OrderCancelling,
		OrderCancelled, OrderCancelledRefunded, OrderExpired, OrderFulfilled,
		OrderRefunded,
	}
}

// --- database/sql plumbing -------------------------------------------------
//
// Each status is a Postgres enum, not text. Raw SQL in this codebase casts
// explicitly (?::order_status) so the server never has to guess a parameter
// type; Value/Scan handle the Go side.

func (s OrderStatus) Value() (driver.Value, error) { return string(s), nil }
func (s *OrderStatus) Scan(v any) error            { return scanEnum(v, (*string)(s)) }

func (s ReservationStatus) Value() (driver.Value, error) { return string(s), nil }
func (s *ReservationStatus) Scan(v any) error            { return scanEnum(v, (*string)(s)) }

func (s PaymentStatus) Value() (driver.Value, error) { return string(s), nil }
func (s *PaymentStatus) Scan(v any) error            { return scanEnum(v, (*string)(s)) }

func (s NotificationStatus) Value() (driver.Value, error) { return string(s), nil }
func (s *NotificationStatus) Scan(v any) error            { return scanEnum(v, (*string)(s)) }

func (r UserRole) Value() (driver.Value, error) { return string(r), nil }
func (r *UserRole) Scan(v any) error            { return scanEnum(v, (*string)(r)) }

// scanEnum accepts the two shapes a driver may hand back for a text-ish column.
func scanEnum(v any, dst *string) error {
	switch t := v.(type) {
	case nil:
		*dst = ""
	case string:
		*dst = t
	case []byte:
		*dst = string(t)
	default:
		return fmt.Errorf("cannot scan %T into enum", v)
	}
	return nil
}
