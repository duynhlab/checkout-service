package logicv1

import (
	"errors"
	"testing"
)

func TestQuote(t *testing.T) {
	svc := NewCheckoutService()

	tests := []struct {
		name         string
		items        []Item
		wantErr      error
		wantSubtotal int
		wantShipping int
		wantTotal    int
		wantFree     bool
	}{
		{
			name:    "empty cart",
			items:   nil,
			wantErr: ErrEmptyCart,
		},
		{
			name:         "below free shipping threshold",
			items:        []Item{{SKU: "sku-1", Quantity: 2, UnitPriceCents: 10_00}},
			wantSubtotal: 20_00,
			wantShipping: ShippingFeeCents,
			wantTotal:    20_00 + ShippingFeeCents,
		},
		{
			name:         "free shipping at threshold",
			items:        []Item{{SKU: "sku-1", Quantity: 5, UnitPriceCents: 10_00}},
			wantSubtotal: 50_00,
			wantShipping: 0,
			wantTotal:    50_00,
			wantFree:     true,
		},
		{
			name: "multiple lines",
			items: []Item{
				{SKU: "sku-1", Quantity: 1, UnitPriceCents: 15_00},
				{SKU: "sku-2", Quantity: 3, UnitPriceCents: 20_00},
			},
			wantSubtotal: 75_00,
			wantShipping: 0,
			wantTotal:    75_00,
			wantFree:     true,
		},
		{
			name:    "empty sku",
			items:   []Item{{SKU: "", Quantity: 1, UnitPriceCents: 100}},
			wantErr: ErrInvalidItem,
		},
		{
			name:    "zero quantity",
			items:   []Item{{SKU: "sku-1", Quantity: 0, UnitPriceCents: 100}},
			wantErr: ErrInvalidItem,
		},
		{
			name:    "quantity over limit",
			items:   []Item{{SKU: "sku-1", Quantity: MaxQuantityPerItem + 1, UnitPriceCents: 100}},
			wantErr: ErrInvalidItem,
		},
		{
			name:    "negative price",
			items:   []Item{{SKU: "sku-1", Quantity: 1, UnitPriceCents: -1}},
			wantErr: ErrInvalidItem,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := svc.Quote(tt.items)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Quote() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Quote() unexpected error: %v", err)
			}
			if got.SubtotalCents != tt.wantSubtotal {
				t.Errorf("SubtotalCents = %d, want %d", got.SubtotalCents, tt.wantSubtotal)
			}
			if got.ShippingCents != tt.wantShipping {
				t.Errorf("ShippingCents = %d, want %d", got.ShippingCents, tt.wantShipping)
			}
			if got.TotalCents != tt.wantTotal {
				t.Errorf("TotalCents = %d, want %d", got.TotalCents, tt.wantTotal)
			}
			if got.FreeShipping != tt.wantFree {
				t.Errorf("FreeShipping = %v, want %v", got.FreeShipping, tt.wantFree)
			}
		})
	}
}
