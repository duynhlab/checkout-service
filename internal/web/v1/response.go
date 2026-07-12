package v1

import (
	"time"

	"github.com/duynhlab/checkout-service/internal/core/domain"
)

// sessionItemResponse is one snapshot line in the HTTP shape. Browser-facing
// money is rendered as dollars (matching cart and order — the two APIs the
// SPA consumes next to this one); minor units stay internal and east-west.
type sessionItemResponse struct {
	ProductID    string  `json:"product_id"`
	ProductName  string  `json:"product_name"`
	Quantity     int     `json:"quantity"`
	UnitPrice    float64 `json:"unit_price"`
	CartPrice    float64 `json:"cart_price"`
	PriceChanged bool    `json:"price_changed"`
}

// sessionResponse mirrors domain.Session with money rendered as dollars —
// the web layer owns the wire representation (domain never serializes
// directly), same as order's response DTO. expired_reason is deliberately
// absent: an expired session is never rendered (every path answers 410
// SESSION_EXPIRED first), so the field has no wire consumer.
type sessionResponse struct {
	ID             string                `json:"id"`
	UserID         string                `json:"user_id"`
	Status         domain.SessionStatus  `json:"status"`
	Items          []sessionItemResponse `json:"items"`
	Address        *domain.Address       `json:"address,omitempty"`
	ShippingMethod string                `json:"shipping_method,omitempty"`
	ShippingFee    float64               `json:"shipping_fee"`
	Tax            float64               `json:"tax"`
	PromoCode      string                `json:"promo_code,omitempty"`
	Discount       float64               `json:"discount"`
	Subtotal       float64               `json:"subtotal"`
	Total          float64               `json:"total"`
	Currency       string                `json:"currency"`
	OrderID        string                `json:"order_id,omitempty"`
	ExpiresAt      time.Time             `json:"expires_at"`
	CreatedAt      time.Time             `json:"created_at"`
	UpdatedAt      time.Time             `json:"updated_at"`
}

// toSessionResponse maps a domain session (minor units) to its HTTP shape
// (dollars).
func toSessionResponse(s *domain.Session) sessionResponse {
	items := make([]sessionItemResponse, len(s.Items))
	for i, it := range s.Items {
		items[i] = sessionItemResponse{
			ProductID:    it.ProductID,
			ProductName:  it.ProductName,
			Quantity:     it.Quantity,
			UnitPrice:    domain.Dollars(it.UnitPriceMinor),
			CartPrice:    domain.Dollars(it.CartPriceMinor),
			PriceChanged: it.PriceChanged,
		}
	}
	return sessionResponse{
		ID:             s.ID,
		UserID:         s.UserID,
		Status:         s.Status,
		Items:          items,
		Address:        s.Address,
		ShippingMethod: s.ShippingMethod,
		ShippingFee:    domain.Dollars(s.ShippingFeeMinor),
		Tax:            domain.Dollars(s.TaxMinor),
		PromoCode:      s.PromoCode,
		Discount:       domain.Dollars(s.DiscountMinor),
		Subtotal:       domain.Dollars(s.SubtotalMinor),
		Total:          domain.Dollars(s.TotalMinor),
		Currency:       s.Currency,
		OrderID:        s.OrderID,
		ExpiresAt:      s.ExpiresAt,
		CreatedAt:      s.CreatedAt,
		UpdatedAt:      s.UpdatedAt,
	}
}
