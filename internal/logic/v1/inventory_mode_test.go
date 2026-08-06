package v1

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/duynhlab/checkout-service/internal/core/domain"
)

type fakePrices struct {
	infos []PriceInfo
	err   error
	calls int
}

func (f *fakePrices) BatchGetCurrentPrices(_ context.Context, _ []string) ([]PriceInfo, error) {
	f.calls++
	return f.infos, f.err
}

type fakeChecker struct {
	res   AvailabilityResult
	err   error
	calls int
}

func (f *fakeChecker) CheckAvailability(_ context.Context, _ []AvailabilityLine) (AvailabilityResult, error) {
	f.calls++
	return f.res, f.err
}

func TestMergeCatalog_CanFulfill_ClearsAndFiltersLines(t *testing.T) {
	lines := []AvailabilityLine{{SKUID: "1", Quantity: 2}, {SKUID: "2", Quantity: 1}, {SKUID: "3", Quantity: 1}, {SKUID: "4", Quantity: 1}}
	prices := []PriceInfo{
		{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, Sellable: true, Currency: "USD"},
		{ProductID: "2", Name: "Gone", UnitPriceMinor: 999, Sellable: false},                // unsellable → omit
		{ProductID: "3", Name: "EUR", UnitPriceMinor: 500, Sellable: true, Currency: "EUR"}, // wrong currency → omit
		{ProductID: "9", Name: "Extra", UnitPriceMinor: 100, Sellable: true},                // not requested → omit
	}
	out := mergeCatalog(lines, prices, AvailabilityResult{CanFulfill: true})

	if len(out) != 1 || out[0].ProductID != "1" {
		t.Fatalf("want only sellable/USD/requested SKU 1, got %+v", out)
	}
	if out[0].AvailableQty != 2 { // cleared ⇒ AvailableQty == requested
		t.Errorf("cleared line AvailableQty = %d, want 2", out[0].AvailableQty)
	}
}

// The fail-open fix: CanFulfill=false is authoritative — every line is blocked
// even when Shortages is empty/incomplete (Inventory makes no completeness
// guarantee there).
func TestMergeCatalog_CanFulfillFalse_BlocksEveryLine_EvenWithoutShortages(t *testing.T) {
	lines := []AvailabilityLine{{SKUID: "1", Quantity: 2}, {SKUID: "2", Quantity: 3}}
	prices := []PriceInfo{
		{ProductID: "1", Sellable: true, UnitPriceMinor: 100},
		{ProductID: "2", Sellable: true, UnitPriceMinor: 200},
	}
	out := mergeCatalog(lines, prices, AvailabilityResult{CanFulfill: false}) // no shortages listed

	if len(out) != 2 {
		t.Fatalf("want 2 entries, got %+v", out)
	}
	for _, p := range out {
		if p.AvailableQty != 0 {
			t.Errorf("sku %s AvailableQty = %d, want 0 (basket unfulfillable ⇒ blocked)", p.ProductID, p.AvailableQty)
		}
	}
}

// Duplicate SKU lines cannot oversell: CanFulfill=false blocks the SKU
// regardless of how many lines reference it.
func TestMergeCatalog_DuplicateSKU_CanFulfillFalse_Blocked(t *testing.T) {
	lines := []AvailabilityLine{{SKUID: "1", Quantity: 3}, {SKUID: "1", Quantity: 3}}
	prices := []PriceInfo{{ProductID: "1", Sellable: true}}
	out := mergeCatalog(lines, prices, AvailabilityResult{CanFulfill: false, Shortages: []Shortage{{SKUID: "1", Requested: 6, AvailableToPromise: 5}}})
	if len(out) != 1 || out[0].AvailableQty != 0 {
		t.Errorf("dup-SKU unfulfillable must block: %+v", out)
	}
}

// A contradictory reply (CanFulfill=true but a shortage listed) still blocks
// that line.
func TestMergeCatalog_CanFulfillTrue_StrayShortage_BlocksThatLine(t *testing.T) {
	out := mergeCatalog(
		[]AvailabilityLine{{SKUID: "1", Quantity: 2}, {SKUID: "2", Quantity: 1}},
		[]PriceInfo{{ProductID: "1", Sellable: true}, {ProductID: "2", Sellable: true}},
		AvailabilityResult{CanFulfill: true, Shortages: []Shortage{{SKUID: "2"}}},
	)
	byID := map[string]int{}
	for _, p := range out {
		byID[p.ProductID] = p.AvailableQty
	}
	if byID["1"] != 2 || byID["2"] != 0 {
		t.Errorf("want sku1 cleared(2), sku2 blocked(0), got %+v", byID)
	}
}

// splitSvc builds a service on the two real authorities. There is no other path to
// contrast it with since RFC-0021 phase 4 — the product-availability fallback, the
// source flag and the canary dial are gone.
func splitSvc(prices *fakePrices, checker *fakeChecker) *CheckoutService {
	return NewCheckoutService(&fakeRepo{}, &fakeCart{}, prices, checker, time.Minute)
}

func TestResolveCatalog_SplitReads(t *testing.T) {
	prices := &fakePrices{infos: []PriceInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, Sellable: true}}}
	checker := &fakeChecker{res: AvailabilityResult{CanFulfill: true}}
	svc := splitSvc(prices, checker)

	out, _, err := svc.resolveCatalog(context.Background(), []AvailabilityLine{{SKUID: "1", Quantity: 2}})
	if err != nil {
		t.Fatalf("resolveCatalog error = %v", err)
	}
	if prices.calls != 1 || checker.calls != 1 {
		t.Errorf("the catalog read must call prices+checker once each: prices=%d checker=%d", prices.calls, checker.calls)
	}
	if len(out) != 1 || out[0].UnitPriceMinor != 2999 || out[0].AvailableQty != 2 {
		t.Errorf("merged = %+v, want price 2999 qty 2", out)
	}
}

// Fail-closed: an Inventory CheckAvailability error must surface as an error
// (→ caller maps to ErrUpstream/503), NEVER as an empty/out-of-stock result.
func TestResolveCatalog_CheckAvailabilityError_FailsClosed(t *testing.T) {
	prices := &fakePrices{infos: []PriceInfo{{ProductID: "1", Sellable: true}}}
	checker := &fakeChecker{err: errors.New("inventory timeout")}
	svc := splitSvc(prices, checker)

	out, _, err := svc.resolveCatalog(context.Background(), []AvailabilityLine{{SKUID: "1", Quantity: 1}})
	if err == nil {
		t.Fatal("CheckAvailability error must propagate (fail-closed), got nil")
	}
	if out != nil {
		t.Errorf("no partial result on failure, got %+v", out)
	}
}

// An unknown SKU is a DATA problem, not a business answer. Inventory cannot say
// whether the SKU is buyable, so checkout must not either: fail closed with
// ErrUpstream (→ 503, retryable) rather than reporting it blocked, which would
// tell the shopper an item is gone on the evidence of a missing balance row.
func TestResolveCatalog_UnknownSKU_FailsClosed(t *testing.T) {
	prices := &fakePrices{infos: []PriceInfo{{ProductID: "1", Sellable: true}}}
	checker := &fakeChecker{res: AvailabilityResult{UnknownSKUIDs: []string{"1"}}}
	svc := splitSvc(prices, checker)

	out, answered, err := svc.resolveCatalog(context.Background(), []AvailabilityLine{{SKUID: "1", Quantity: 1}})
	if !errors.Is(err, ErrUpstream) {
		t.Fatalf("err = %v, want ErrUpstream (fail closed on a data gap)", err)
	}
	if out != nil || answered {
		t.Errorf("no partial answer on a data gap: out=%+v answered=%v", out, answered)
	}
	if !strings.Contains(err.Error(), "1") {
		t.Errorf("err = %q, want the offending sku named for the operator", err)
	}
}

// The customer-facing difference the field exists for: a tracked shortage is a
// requote, an untracked SKU is a 503. Same can_fulfill=false on the wire.
func TestResolveCatalog_TrackedShortage_IsNotAnError(t *testing.T) {
	prices := &fakePrices{infos: []PriceInfo{{ProductID: "1", Sellable: true}}}
	checker := &fakeChecker{res: AvailabilityResult{
		Shortages: []Shortage{{SKUID: "1", Requested: 5, AvailableToPromise: 2}},
	}}
	svc := splitSvc(prices, checker)

	out, _, err := svc.resolveCatalog(context.Background(), []AvailabilityLine{{SKUID: "1", Quantity: 5}})
	if err != nil {
		t.Fatalf("a shortage is a business answer, not an error: %v", err)
	}
	if len(out) != 1 || out[0].AvailableQty != 0 {
		t.Errorf("merged = %+v, want the line blocked so the shopper requotes", out)
	}
}

// An unknown SKU forces can_fulfill=false, so a mixed reply must not be filed as
// a shortage: the data gap is the blocking reason an operator has to act on.
func TestResolveCatalog_UnknownSKU_OutranksShortage(t *testing.T) {
	prices := &fakePrices{infos: []PriceInfo{
		{ProductID: "1", Sellable: true}, {ProductID: "2", Sellable: true},
	}}
	checker := &fakeChecker{res: AvailabilityResult{
		Shortages:     []Shortage{{SKUID: "1", Requested: 5, AvailableToPromise: 2}},
		UnknownSKUIDs: []string{"2"},
	}}
	svc := splitSvc(prices, checker)

	_, _, err := svc.resolveCatalog(context.Background(),
		[]AvailabilityLine{{SKUID: "1", Quantity: 5}, {SKUID: "2", Quantity: 1}})
	if !errors.Is(err, ErrUpstream) {
		t.Fatalf("err = %v, want ErrUpstream: an unknown SKU outranks a shortage", err)
	}
}

func TestResolveCatalog_PriceError(t *testing.T) {
	prices := &fakePrices{err: errors.New("price down")}
	checker := &fakeChecker{}
	svc := splitSvc(prices, checker)

	if _, _, err := svc.resolveCatalog(context.Background(), []AvailabilityLine{{SKUID: "1", Quantity: 1}}); err == nil {
		t.Fatal("BatchGetCurrentPrices error must propagate, got nil")
	}
	// Concurrent fetch: checker may or may not have been invoked, but its result
	// is discarded once prices error — the call still returns the price error.
}

// A panic in a concurrent fetch goroutine must be recovered into an error, not
// crash the process (the goroutines are joined, but an unrecovered goroutine
// panic still kills Go).
type panicChecker struct{}

func (panicChecker) CheckAvailability(context.Context, []AvailabilityLine) (AvailabilityResult, error) {
	panic("checker boom")
}

func TestResolveCatalog_PanicRecoveredAsError(t *testing.T) {
	svc := NewCheckoutService(&fakeRepo{}, &fakeCart{},
		&fakePrices{infos: []PriceInfo{{ProductID: "1", Sellable: true}}}, panicChecker{}, time.Minute)

	if _, _, err := svc.resolveCatalog(context.Background(), []AvailabilityLine{{SKUID: "1", Quantity: 1}}); err == nil {
		t.Fatal("a panicking checker must be recovered into an error, got nil")
	}
}

// End-to-end fail-closed at CREATE: an Inventory timeout must surface as
// ErrUpstream (retryable), never a snapshot that silently drops availability.
func TestCreateSession_CheckAvailabilityError_FailsClosed(t *testing.T) {
	cart := &fakeCart{lines: []CartLine{{ProductID: "1", ProductName: "Mouse", Quantity: 1, CartPriceMinor: 2999}}}
	prices := &fakePrices{infos: []PriceInfo{{ProductID: "1", Sellable: true, UnitPriceMinor: 2999}}}
	checker := &fakeChecker{err: errors.New("inventory timeout")}
	svc := NewCheckoutService(&fakeRepo{}, cart, prices, checker, time.Minute)

	if _, _, err := svc.CreateSession(context.Background(), "7"); !errors.Is(err, ErrUpstream) {
		t.Fatalf("inventory timeout at create must fail closed to ErrUpstream, got %v", err)
	}
}

// End-to-end fail-closed at CONFIRM (revalidate): an Inventory timeout maps to
// ErrUpstream (503, retryable) — specifically NOT ErrStockUnavailable. This is
// the headline contract: a timeout is never read as out-of-stock.
func TestRevalidate_CheckAvailabilityError_FailsClosed(t *testing.T) {
	idem := &fakeIdem{}
	prices := &fakePrices{infos: []PriceInfo{{ProductID: "1", Sellable: true}}}
	checker := &fakeChecker{err: errors.New("inventory timeout")}
	svc := NewCheckoutService(&fakeRepo{}, &fakeCart{}, prices, checker, time.Minute).
		WithConfirm(idem, &fakeOrders{})
	session := &domain.Session{ID: "s1", Items: []domain.SessionItem{{ProductID: "1", Quantity: 1, UnitPriceMinor: 2999}}}

	_, err := svc.revalidate(context.Background(), session, 42)
	if !errors.Is(err, ErrUpstream) {
		t.Fatalf("inventory timeout at confirm must be ErrUpstream, got %v", err)
	}
	if errors.Is(err, ErrStockUnavailable) {
		t.Fatal("timeout must NEVER be read as out-of-stock")
	}
	if idem.released != 1 {
		t.Errorf("revalidate must Release the key on upstream error, released=%d", idem.released)
	}
}
