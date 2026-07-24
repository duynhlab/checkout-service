package v1

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// fakeInventory records BatchGetAvailability calls; safe for the async shadow
// goroutine (the mutex + awaitShadow give the test a race-free read).
type fakeInventory struct {
	mu      sync.Mutex
	calls   int
	avails  []SkuAvailability
	err     error
	doPanic bool
}

func (f *fakeInventory) BatchGetAvailability(_ context.Context, _ []string) ([]SkuAvailability, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.doPanic {
		panic("inventory client boom")
	}
	return f.avails, f.err
}

func (f *fakeInventory) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func shadowCart() *fakeCart {
	return &fakeCart{lines: []CartLine{
		{ProductID: "1", ProductName: "Mouse", Quantity: 2, CartPriceMinor: 2999},
	}}
}

func shadowProducts() *fakeProducts {
	return &fakeProducts{infos: []ProductInfo{
		{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, AvailableQty: 5},
	}}
}

func TestCompareStructural(t *testing.T) {
	tests := []struct {
		name string
		ids  []string
		inv  []SkuAvailability
		want string
	}{
		{"all known", []string{"1", "2"}, []SkuAvailability{{"1", 5, true}, {"2", 0, true}}, "ok"},
		{"missing sku", []string{"1", "2"}, []SkuAvailability{{"1", 5, true}}, "missing"},
		{"unknown status", []string{"1"}, []SkuAvailability{{"1", 0, false}}, "unknown"},
		{"negative atp", []string{"1"}, []SkuAvailability{{"1", -3, true}}, "unknown"},
		{"missing outranks unknown", []string{"1", "2"}, []SkuAvailability{{"1", 0, false}}, "missing"},
		{"empty ids", []string{}, []SkuAvailability{{"1", 5, true}}, "ok"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compareStructural(tt.ids, tt.inv); got != tt.want {
				t.Errorf("compareStructural = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCreateSession_ProductMode_SkipsInventory(t *testing.T) {
	inv := &fakeInventory{avails: []SkuAvailability{{"1", 5, true}}}
	// Default source (product) must never call inventory.
	svc := newSvc(&fakeRepo{}, shadowCart(), shadowProducts()).
		WithAvailabilitySource(AvailabilitySourceProduct, 100, inv)

	if _, created, err := svc.CreateSession(context.Background(), "7"); err != nil || !created {
		t.Fatalf("CreateSession() = (created=%v, err=%v), want created", created, err)
	}
	svc.awaitShadow()
	if inv.callCount() != 0 {
		t.Errorf("product mode called inventory %d times, want 0", inv.callCount())
	}
}

func TestCreateSession_ShadowMode_CallsInventory(t *testing.T) {
	inv := &fakeInventory{avails: []SkuAvailability{{"1", 5, true}}}
	svc := newSvc(&fakeRepo{}, shadowCart(), shadowProducts()).
		WithAvailabilitySource(AvailabilitySourceShadow, 100, inv)

	s, created, err := svc.CreateSession(context.Background(), "7")
	if err != nil || !created {
		t.Fatalf("CreateSession() = (created=%v, err=%v), want created", created, err)
	}
	svc.awaitShadow()
	if inv.callCount() != 1 {
		t.Errorf("shadow mode called inventory %d times, want 1", inv.callCount())
	}
	// Product stays authoritative: shadow never alters the snapshot.
	if s.Items[0].UnitPriceMinor != 2999 {
		t.Errorf("shadow altered the snapshot: unit price = %d, want 2999", s.Items[0].UnitPriceMinor)
	}
}

func TestCreateSession_ShadowSampleZero_SkipsInventory(t *testing.T) {
	inv := &fakeInventory{avails: []SkuAvailability{{"1", 5, true}}}
	// 0% sampling: shadow enabled but every op is sampled out.
	svc := newSvc(&fakeRepo{}, shadowCart(), shadowProducts()).
		WithAvailabilitySource(AvailabilitySourceShadow, 0, inv)

	if _, created, err := svc.CreateSession(context.Background(), "7"); err != nil || !created {
		t.Fatalf("CreateSession() = (created=%v, err=%v), want created", created, err)
	}
	svc.awaitShadow()
	if inv.callCount() != 0 {
		t.Errorf("0%% sampling called inventory %d times, want 0", inv.callCount())
	}
}

func TestCreateSession_ShadowError_DoesNotFailRequest(t *testing.T) {
	inv := &fakeInventory{err: errors.New("inventory down")}
	svc := newSvc(&fakeRepo{}, shadowCart(), shadowProducts()).
		WithAvailabilitySource(AvailabilitySourceShadow, 100, inv)

	s, created, err := svc.CreateSession(context.Background(), "7")
	if err != nil || !created {
		t.Fatalf("shadow error leaked: CreateSession() = (created=%v, err=%v), want created", created, err)
	}
	svc.awaitShadow()
	if inv.callCount() != 1 || s == nil || len(s.Items) != 1 {
		t.Errorf("shadow error not swallowed cleanly: calls=%d session=%+v", inv.callCount(), s)
	}
}

// The contract's headline invariant: a panic in the shadow goroutine must be
// recovered, never crash the process or fail the request.
func TestCreateSession_ShadowPanic_DoesNotCrash(t *testing.T) {
	inv := &fakeInventory{doPanic: true}
	svc := newSvc(&fakeRepo{}, shadowCart(), shadowProducts()).
		WithAvailabilitySource(AvailabilitySourceShadow, 100, inv)

	s, created, err := svc.CreateSession(context.Background(), "7")
	if err != nil || !created {
		t.Fatalf("shadow panic leaked: CreateSession() = (created=%v, err=%v), want created", created, err)
	}
	svc.awaitShadow() // must return — the recover() lets Done() run
	if inv.callCount() != 1 || s == nil {
		t.Errorf("shadow panic path wrong: calls=%d session=%+v", inv.callCount(), s)
	}
}
