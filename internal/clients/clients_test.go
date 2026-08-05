package clients

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"

	inventoryv1 "github.com/duynhlab/pkg/proto/inventory/v1"
	productv1 "github.com/duynhlab/pkg/proto/product/v1"

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
