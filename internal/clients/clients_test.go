package clients

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"

	inventoryv1 "github.com/duynhlab/pkg/proto/inventory/v1"
	orderv1 "github.com/duynhlab/pkg/proto/order/v1"
	productv1 "github.com/duynhlab/pkg/proto/product/v1"

	"github.com/duynhlab/checkout-service/internal/core/domain"
	logicv1 "github.com/duynhlab/checkout-service/internal/logic/v1"
)

// fakeInvSvc embeds the generated client so only the RPCs under test need a
// body; the rest are unimplemented (never called here).
type fakeInvSvc struct {
	inventoryv1.InventoryServiceClient
	resp      *inventoryv1.BatchGetAvailabilityResponse
	err       error
	checkResp *inventoryv1.CheckAvailabilityResponse
	checkErr  error
}

func (f *fakeInvSvc) BatchGetAvailability(_ context.Context, _ *inventoryv1.BatchGetAvailabilityRequest, _ ...grpc.CallOption) (*inventoryv1.BatchGetAvailabilityResponse, error) {
	return f.resp, f.err
}

func (f *fakeInvSvc) CheckAvailability(_ context.Context, _ *inventoryv1.CheckAvailabilityRequest, _ ...grpc.CallOption) (*inventoryv1.CheckAvailabilityResponse, error) {
	return f.checkResp, f.checkErr
}

// fakeProdSvc embeds the generated product client; only BatchGetCurrentPrices
// has a body.
type fakeProdSvc struct {
	productv1.ProductServiceClient
	resp *productv1.BatchGetCurrentPricesResponse
	err  error
}

func (f *fakeProdSvc) BatchGetCurrentPrices(_ context.Context, _ *productv1.BatchGetCurrentPricesRequest, _ ...grpc.CallOption) (*productv1.BatchGetCurrentPricesResponse, error) {
	return f.resp, f.err
}

func TestInventoryClient_CheckAvailability_MapsResult(t *testing.T) {
	resp := &inventoryv1.CheckAvailabilityResponse{
		CanFulfill: false,
		Shortages: []*inventoryv1.Shortage{
			{SkuId: "2", Requested: 5, AvailableToPromise: 1},
		},
		// A tracked shortage and an untracked SKU in one reply: the transport must
		// keep them apart, because the logic layer answers them differently (a
		// requote vs a fail-closed 503). Dropping this mapping silently reverts the
		// whole cross-repo feature, so it is asserted here and not only upstream.
		UnknownSkuIds: []string{"3"},
	}
	c := &InventoryClient{c: &fakeInvSvc{checkResp: resp}}

	got, err := c.CheckAvailability(context.Background(), []logicv1.AvailabilityLine{
		{SKUID: "1", Quantity: 2}, {SKUID: "2", Quantity: 5},
	})
	if err != nil {
		t.Fatalf("CheckAvailability() error = %v", err)
	}
	if got.CanFulfill {
		t.Error("CanFulfill = true, want false")
	}
	if len(got.Shortages) != 1 || got.Shortages[0].SKUID != "2" ||
		got.Shortages[0].Requested != 5 || got.Shortages[0].AvailableToPromise != 1 {
		t.Errorf("shortages = %+v, want one {2,5,1}", got.Shortages)
	}
	if len(got.UnknownSKUIDs) != 1 || got.UnknownSKUIDs[0] != "3" {
		t.Errorf("unknown = %+v, want [3]", got.UnknownSKUIDs)
	}
}

func TestInventoryClient_CheckAvailability_PropagatesError(t *testing.T) {
	c := &InventoryClient{c: &fakeInvSvc{checkErr: errors.New("timeout")}}

	if _, err := c.CheckAvailability(context.Background(), []logicv1.AvailabilityLine{{SKUID: "1", Quantity: 1}}); err == nil {
		t.Fatal("CheckAvailability() error = nil, want propagated error")
	}
}

func TestProductClient_BatchGetCurrentPrices_Maps(t *testing.T) {
	resp := &productv1.BatchGetCurrentPricesResponse{
		Prices: []*productv1.CurrentPrice{
			{SkuId: "1", Name: "Mouse", PriceMinor: 2999, Currency: "USD", Sellable: true},
		},
	}
	c := &ProductClient{c: &fakeProdSvc{resp: resp}}

	out, err := c.BatchGetCurrentPrices(context.Background(), []string{"1"})
	if err != nil {
		t.Fatalf("BatchGetCurrentPrices() error = %v", err)
	}
	if len(out) != 1 || out[0].ProductID != "1" || out[0].UnitPriceMinor != 2999 ||
		out[0].Currency != "USD" || !out[0].Sellable {
		t.Errorf("mapped price = %+v, want {1,Mouse,2999,USD,true}", out)
	}
}

func TestProductClient_BatchGetCurrentPrices_PropagatesError(t *testing.T) {
	c := &ProductClient{c: &fakeProdSvc{err: errors.New("boom")}}

	if _, err := c.BatchGetCurrentPrices(context.Background(), []string{"1"}); err == nil {
		t.Fatal("BatchGetCurrentPrices() error = nil, want propagated error")
	}
}

// fakeOrderSvc embeds the generated order client; only CreateOrder has a body,
// and it keeps the request so the mapping can be asserted.
type fakeOrderSvc struct {
	orderv1.OrderServiceClient
	got  *orderv1.CreateOrderRequest
	resp *orderv1.CreateOrderResponse
	err  error
}

func (f *fakeOrderSvc) CreateOrder(_ context.Context, in *orderv1.CreateOrderRequest, _ ...grpc.CallOption) (*orderv1.CreateOrderResponse, error) {
	f.got = in
	return f.resp, f.err
}

// The item mapping is the boundary where checkout's int64 quantity becomes
// order's int32, and where the totals components cross so the charged total
// equals the session total (P4). Both are asserted here because a silent
// truncation or a dropped component only shows up as a money difference in
// production.
func TestOrderClient_CreateOrder_MapsItemsAndTotals(t *testing.T) {
	f := &fakeOrderSvc{resp: &orderv1.CreateOrderResponse{OrderId: "42", Status: "pending"}}
	c := &OrderClient{c: f}

	id, status, err := c.CreateOrder(context.Background(), "alice",
		[]domain.SessionItem{
			{ProductID: "1", ProductName: "Wireless Mouse", Quantity: 2, UnitPriceMinor: 2999},
			{ProductID: "2", ProductName: "USB Hub", Quantity: 1, UnitPriceMinor: 7999},
		},
		"tok_visa_ok", "idem-1", 300, 263, 299)
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	if id != "42" || status != "pending" {
		t.Errorf("got (%q, %q), want (\"42\", \"pending\")", id, status)
	}

	if n := len(f.got.GetItems()); n != 2 {
		t.Fatalf("items = %d, want 2", n)
	}
	first := f.got.GetItems()[0]
	if first.GetProductId() != "1" || first.GetProductName() != "Wireless Mouse" ||
		first.GetQuantity() != 2 || first.GetUnitPriceMinor() != 2999 {
		t.Errorf("item[0] = %+v, want the session line verbatim", first)
	}
	// The three components must all cross: order charges their sum, so a
	// dropped one is a wrong charge, not a wrong display.
	if f.got.GetShippingFeeMinor() != 300 || f.got.GetTaxMinor() != 263 || f.got.GetDiscountMinor() != 299 {
		t.Errorf("totals = fee %d tax %d discount %d, want 300/263/299",
			f.got.GetShippingFeeMinor(), f.got.GetTaxMinor(), f.got.GetDiscountMinor())
	}
	if f.got.GetUserId() != "alice" || f.got.GetIdempotencyKey() != "idem-1" {
		t.Errorf("identity/idempotency did not cross: %+v", f.got)
	}
}

// A transport error must surface, not be swallowed into an empty order id —
// checkout retries on it, and a silent "" would look like a created order.
func TestOrderClient_CreateOrder_PropagatesError(t *testing.T) {
	c := &OrderClient{c: &fakeOrderSvc{err: errors.New("unavailable")}}
	if _, _, err := c.CreateOrder(context.Background(), "alice", nil, "tok", "k", 0, 0, 0); err == nil {
		t.Fatal("want the transport error to surface")
	}
}
