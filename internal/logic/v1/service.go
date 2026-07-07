// Package logicv1 contains the checkout business logic, kept free of
// transport concerns so it is trivially unit-testable.
package logicv1

import (
	"errors"
	"fmt"
)

// Pricing rules. Amounts are integer cents to avoid float rounding drift.
const (
	// FreeShippingThresholdCents is the subtotal at which shipping becomes free.
	FreeShippingThresholdCents = 50_00
	// ShippingFeeCents is the flat shipping fee below the free threshold.
	ShippingFeeCents = 5_99
	// MaxQuantityPerItem bounds a single line item quantity.
	MaxQuantityPerItem = 100
)

var (
	// ErrEmptyCart is returned when a quote is requested for no items.
	ErrEmptyCart = errors.New("cart must contain at least one item")
	// ErrInvalidItem is returned when a line item fails validation.
	ErrInvalidItem = errors.New("invalid line item")
)

// Item is one line in the cart.
type Item struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int    `json:"unit_price_cents"`
}

// Quote is the priced result of a checkout request.
type Quote struct {
	Items         int  `json:"items"`
	SubtotalCents int  `json:"subtotal_cents"`
	ShippingCents int  `json:"shipping_cents"`
	TotalCents    int  `json:"total_cents"`
	FreeShipping  bool `json:"free_shipping"`
}

// CheckoutService prices carts.
type CheckoutService struct{}

// NewCheckoutService wires the checkout business logic.
func NewCheckoutService() *CheckoutService {
	return &CheckoutService{}
}

// Quote validates the cart and computes subtotal, shipping and total.
func (s *CheckoutService) Quote(items []Item) (*Quote, error) {
	if len(items) == 0 {
		return nil, ErrEmptyCart
	}

	subtotal := 0
	count := 0
	for i, it := range items {
		if err := validateItem(i, it); err != nil {
			return nil, err
		}
		subtotal += it.Quantity * it.UnitPriceCents
		count += it.Quantity
	}

	shipping := ShippingFeeCents
	free := subtotal >= FreeShippingThresholdCents
	if free {
		shipping = 0
	}

	return &Quote{
		Items:         count,
		SubtotalCents: subtotal,
		ShippingCents: shipping,
		TotalCents:    subtotal + shipping,
		FreeShipping:  free,
	}, nil
}

// validateItem enforces per-line invariants.
func validateItem(idx int, it Item) error {
	switch {
	case it.SKU == "":
		return fmt.Errorf("%w: item %d has empty sku", ErrInvalidItem, idx)
	case it.Quantity <= 0:
		return fmt.Errorf("%w: item %d (%s) quantity must be positive", ErrInvalidItem, idx, it.SKU)
	case it.Quantity > MaxQuantityPerItem:
		return fmt.Errorf("%w: item %d (%s) quantity exceeds %d", ErrInvalidItem, idx, it.SKU, MaxQuantityPerItem)
	case it.UnitPriceCents < 0:
		return fmt.Errorf("%w: item %d (%s) unit price must not be negative", ErrInvalidItem, idx, it.SKU)
	default:
		return nil
	}
}
