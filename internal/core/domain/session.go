// Package domain holds checkout's core types: the checkout session aggregate,
// its state machine vocabulary, and the repository port. The session is an
// ephemeral, short-TTL quote — RFC-0015: cart is the item-list authority,
// product is the price authority at checkout time, order stays the only
// orders-writer.
package domain

import (
	"context"
	"time"
)

// SessionStatus enumerates the checkout FSM states. States advance strictly
// forward through the funnel; `expired` and `cancelled` are terminal. The
// legal transitions live in the logic layer's transition table.
type SessionStatus string

const (
	StatusOpen        SessionStatus = "open"
	StatusAddressSet  SessionStatus = "address_set"
	StatusShippingSet SessionStatus = "shipping_set"
	StatusReady       SessionStatus = "ready"
	StatusConfirming  SessionStatus = "confirming"
	StatusCompleted   SessionStatus = "completed"
	StatusCancelled   SessionStatus = "cancelled"
	StatusExpired     SessionStatus = "expired"
)

// Terminal reports whether the status can never transition again.
func (s SessionStatus) Terminal() bool {
	return s == StatusCompleted || s == StatusCancelled || s == StatusExpired
}

// ActiveStatuses are the non-terminal states — the partial unique index on
// user_id spans exactly this set (one active session per user).
func ActiveStatuses() []SessionStatus {
	return []SessionStatus{StatusOpen, StatusAddressSet, StatusShippingSet, StatusReady, StatusConfirming}
}

// ExpiredReason records who noticed the expiry: the durable Temporal timer
// (P2) or the lazy check on a read/mutation.
type ExpiredReason string

const (
	ExpiredByTimer ExpiredReason = "timer"
	ExpiredByLazy  ExpiredReason = "lazy"
)

// Address is the shipping address captured at the address step. Stored as
// jsonb; validation happens at the web boundary.
type Address struct {
	FullName string `json:"full_name"`
	Line1    string `json:"line1"`
	Line2    string `json:"line2,omitempty"`
	City     string `json:"city"`
	Region   string `json:"region"`
	PostCode string `json:"post_code"`
	Country  string `json:"country"`
}

// SessionItem is one snapshotted cart line. UnitPriceMinor comes from
// product (the authority); CartPriceMinor is cart's denormalized price kept
// for the price-changed diff. All money is int64 minor units.
type SessionItem struct {
	ProductID      string `json:"product_id"`
	ProductName    string `json:"product_name"`
	Quantity       int    `json:"quantity"`
	UnitPriceMinor int64  `json:"unit_price_minor"`
	CartPriceMinor int64  `json:"cart_price_minor"`
	PriceChanged   bool   `json:"price_changed"`
}

// Session is the checkout session aggregate — an auditable quote.
type Session struct {
	ID                 string         `json:"id"`
	UserID             string         `json:"user_id"`
	Status             SessionStatus  `json:"status"`
	Items              []SessionItem  `json:"items"`
	Address            *Address       `json:"address,omitempty"`
	ShippingMethod     string         `json:"shipping_method,omitempty"`
	ShippingFeeMinor   int64          `json:"shipping_fee_minor"`
	TaxMinor           int64          `json:"tax_minor"`
	PromoCode          string         `json:"promo_code,omitempty"`
	DiscountMinor      int64          `json:"discount_minor"`
	SubtotalMinor      int64          `json:"subtotal_minor"`
	TotalMinor         int64          `json:"total_minor"`
	Currency           string         `json:"currency"`
	PaymentMethodToken string         `json:"-"` // tok_… reference; never serialized outward
	OrderID            string         `json:"order_id,omitempty"`
	ExpiresAt          time.Time      `json:"expires_at"`
	ExpiredReason      *ExpiredReason `json:"expired_reason,omitempty"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

// SessionRepository is the persistence port for sessions.
type SessionRepository interface {
	Create(ctx context.Context, s *Session) error
	// FindByID returns the session regardless of owner; ownership is enforced
	// in the logic layer (a foreign owner reads as ErrSessionNotFound).
	FindByID(ctx context.Context, id string) (*Session, error)
	// FindActiveByUserID returns the user's single active (non-terminal)
	// session, or ErrSessionNotFound.
	FindActiveByUserID(ctx context.Context, userID string) (*Session, error)
	// UpdateStatus moves status and stamps updated_at. Conditional: it only
	// applies when the current status matches `from`, returning
	// ErrStaleTransition otherwise (optimistic concurrency).
	UpdateStatus(ctx context.Context, id string, from, to SessionStatus) error
	// SetAddress persists the address and the address_set status in one write
	// (same conditional semantics as UpdateStatus).
	SetAddress(ctx context.Context, id string, from SessionStatus, addr *Address) error
	// MarkExpired conditionally expires a non-terminal session, recording who
	// noticed (timer vs lazy). Expiring an already-terminal session is a
	// no-op, not an error — late timers must be harmless.
	MarkExpired(ctx context.Context, id string, reason ExpiredReason) error
}

// Dollars converts integer minor units (cents) to a dollars amount for
// display/serialization boundaries — the same helper order-service uses so
// browser-facing money renders identically across the funnel.
func Dollars(minor int64) float64 {
	return float64(minor) / 100
}
