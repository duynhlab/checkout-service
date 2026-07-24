package clients

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"

	inventoryv1 "github.com/duynhlab/pkg/proto/inventory/v1"
)

// fakeInvSvc embeds the generated client so only BatchGetAvailability needs a
// body; the other RPCs are unimplemented (they are never called here).
type fakeInvSvc struct {
	inventoryv1.InventoryServiceClient
	resp *inventoryv1.BatchGetAvailabilityResponse
	err  error
}

func (f *fakeInvSvc) BatchGetAvailability(_ context.Context, _ *inventoryv1.BatchGetAvailabilityRequest, _ ...grpc.CallOption) (*inventoryv1.BatchGetAvailabilityResponse, error) {
	return f.resp, f.err
}

func TestInventoryClient_BatchGetAvailability_MapsStatusAndQty(t *testing.T) {
	resp := &inventoryv1.BatchGetAvailabilityResponse{
		Availabilities: []*inventoryv1.SkuAvailability{
			{SkuId: "1", AvailableToPromise: 5, Status: inventoryv1.AvailabilityStatus_AVAILABILITY_STATUS_IN_STOCK},
			{SkuId: "2", AvailableToPromise: 1, Status: inventoryv1.AvailabilityStatus_AVAILABILITY_STATUS_LOW_STOCK},
			{SkuId: "3", AvailableToPromise: 0, Status: inventoryv1.AvailabilityStatus_AVAILABILITY_STATUS_OUT_OF_STOCK},
			{SkuId: "4", AvailableToPromise: 0, Status: inventoryv1.AvailabilityStatus_AVAILABILITY_STATUS_UNKNOWN},
			{SkuId: "5", AvailableToPromise: 0, Status: inventoryv1.AvailabilityStatus_AVAILABILITY_STATUS_UNSPECIFIED},
		},
	}
	if NewInventoryClient(nil) == nil { // constructor smoke (no dial)
		t.Fatal("NewInventoryClient returned nil")
	}
	c := &InventoryClient{c: &fakeInvSvc{resp: resp}}

	out, err := c.BatchGetAvailability(context.Background(), []string{"1", "2", "3", "4", "5"})
	if err != nil {
		t.Fatalf("BatchGetAvailability() error = %v", err)
	}
	if len(out) != 5 {
		t.Fatalf("got %d availabilities, want 5", len(out))
	}
	// Definite statuses (in/low/out of stock) are Known; UNKNOWN/UNSPECIFIED are not.
	wantKnown := map[string]bool{"1": true, "2": true, "3": true, "4": false, "5": false}
	for _, a := range out {
		if a.Known != wantKnown[a.SKUID] {
			t.Errorf("sku %s Known = %v, want %v", a.SKUID, a.Known, wantKnown[a.SKUID])
		}
	}
	if out[0].AvailableQty != 5 { // int64 passthrough, no clamp
		t.Errorf("sku 1 AvailableQty = %d, want 5", out[0].AvailableQty)
	}
}

func TestInventoryClient_BatchGetAvailability_PropagatesError(t *testing.T) {
	c := &InventoryClient{c: &fakeInvSvc{err: errors.New("unavailable")}}

	if _, err := c.BatchGetAvailability(context.Background(), []string{"1"}); err == nil {
		t.Fatal("BatchGetAvailability() error = nil, want propagated error")
	}
}
