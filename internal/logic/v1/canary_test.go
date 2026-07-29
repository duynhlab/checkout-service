package v1

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/duynhlab/pkg/idempotency"
)

// The canary splits the availability READ AUTHORITY, so unlike shadow sampling
// (which only observes) it must be STICKY: the same user has to see the same
// authority for their whole funnel. A per-request coin flip would let a session
// be created against inventory and confirmed against product — a session
// accepted as in-stock and then refused at confirm, an error the canary
// mechanism itself invented rather than one it found.
func TestCanary_IsStickyPerUser(t *testing.T) {
	for _, pct := range []int{1, 25, 50, 75, 99} {
		t.Run(fmt.Sprintf("pct=%d", pct), func(t *testing.T) {
			for i := 0; i < 200; i++ {
				user := fmt.Sprintf("user-%d", i)
				first := inCanary("", user, pct)
				for r := 0; r < 5; r++ {
					if got := inCanary("", user, pct); got != first {
						t.Fatalf("inCanary(%q, %d) flipped between calls: %v then %v",
							user, pct, first, got)
					}
				}
			}
		})
	}
}

// 0 and 100 are the ends of the dial an operator actually uses: 100 must be
// "every user" (so a source flip with no canary configured behaves exactly as
// before this change) and 0 must be "no user" (so the flag can be flipped to
// inventory with zero blast radius, then opened up).
func TestCanary_BoundsAreAbsolute(t *testing.T) {
	for i := 0; i < 500; i++ {
		user := fmt.Sprintf("user-%d", i)
		if !inCanary("", user, 100) {
			t.Fatalf("inCanary(%q, 100) = false; 100%% must include every user", user)
		}
		if inCanary("", user, 0) {
			t.Fatalf("inCanary(%q, 0) = true; 0%% must include nobody", user)
		}
	}
	// Out-of-range values cannot reach here through flagx, but the function must
	// not invert or panic if they ever do.
	if !inCanary("", "u", 101) || inCanary("", "u", -1) {
		t.Error("out-of-range percentages must clamp, not invert")
	}
}

// The split has to be roughly the requested share, or the dial lies about how
// much traffic an operator has exposed. Tolerance is wide (±10 points over 1000
// users) because this asserts "the hash spreads", not a statistical property.
func TestCanary_ShareApproximatesThePercentage(t *testing.T) {
	const users = 1000
	for _, pct := range []int{10, 50, 90} {
		in := 0
		for i := 0; i < users; i++ {
			if inCanary("", fmt.Sprintf("user-%d", i), pct) {
				in++
			}
		}
		share := in * 100 / users
		if share < pct-10 || share > pct+10 {
			t.Errorf("pct=%d produced %d%% of users (%d/%d); the dial does not reflect real exposure",
				pct, share, in, users)
		}
	}
}

// Stickiness must survive a process restart and be identical across replicas —
// two checkout pods must not send the same user to different authorities. That
// rules out any per-process seed (maphash) and any map iteration order, so the
// hash is pinned here by VALUE, not just by behaviour.
func TestCanary_HashIsStableAcrossProcesses(t *testing.T) {
	// First 4 bytes of HMAC-SHA256(key="", userID). If the construction changes,
	// every user's assignment changes with it — a silent re-shuffle mid-rollout,
	// which is why this test pins the numbers.
	for user, want := range map[string]uint32{
		"1":       0x41e0a944,
		"7":       0x306e9426,
		"alice":   0xce3837f7,
		"user-42": 0xa23888e1,
	} {
		if got := userBucketHash("", user); got != want {
			t.Errorf("userBucketHash(%q) = %#08x, want %#08x — changing the hash re-shuffles every user mid-rollout",
				user, got, want)
		}
	}
}

// The canary only applies in `inventory` mode. In product mode there is nothing
// to canary, and in shadow mode Product is still authoritative while inventory
// is merely observed — applying the dial there would silently disable the shadow
// soak that gates the read flip.
func TestResolveCatalog_CanaryOnlyAppliesInInventoryMode(t *testing.T) {
	for _, source := range []string{AvailabilitySourceProduct, AvailabilitySourceShadow} {
		t.Run(source, func(t *testing.T) {
			prods := &fakeProducts{infos: []ProductInfo{{ProductID: "1", UnitPriceMinor: 2999, AvailableQty: 5}}}
			prices := &fakePrices{}
			checker := &fakeChecker{}
			svc := newSvc(&fakeRepo{}, &fakeCart{}, prods).
				WithAvailabilitySource(source, 100, nil).
				WithInventoryMode(prices, checker).
				WithAvailabilityCanary(0, "") // would exclude everyone, if it applied

			if _, err := svc.resolveCatalog(context.Background(), "user-1",
				[]AvailabilityLine{{SKUID: "1", Quantity: 1}}); err != nil {
				t.Fatalf("resolveCatalog error = %v", err)
			}
			if prods.calls == 0 {
				t.Error("product path was not used; the canary must not apply outside inventory mode")
			}
			if checker.calls != 0 {
				t.Error("inventory was consulted in a non-inventory source")
			}

		})
	}
}

// 0% in inventory mode must route every read to Product — this is the state an
// operator flips the source in with, before opening the dial.
func TestResolveCatalog_CanaryZeroKeepsEveryReadOnProduct(t *testing.T) {
	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "1", UnitPriceMinor: 2999, AvailableQty: 5}}}
	prices := &fakePrices{}
	checker := &fakeChecker{}
	svc := inventorySvc(prods, prices, checker).WithAvailabilityCanary(0, "")

	for i := 0; i < 20; i++ {
		if _, err := svc.resolveCatalog(context.Background(), fmt.Sprintf("user-%d", i),
			[]AvailabilityLine{{SKUID: "1", Quantity: 1}}); err != nil {
			t.Fatalf("resolveCatalog error = %v", err)
		}
	}
	if checker.calls != 0 {
		t.Errorf("inventory was called %d times at 0%% canary", checker.calls)
	}
	if prods.calls != 20 {
		t.Errorf("product calls = %d, want 20 — every read must fall back", prods.calls)
	}
}

// 100% (the default) must behave exactly as inventory mode did before the canary
// existed, so introducing the dial cannot change the meaning of the flag that is
// already documented.
func TestResolveCatalog_CanaryDefaultsToFullInventory(t *testing.T) {
	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "1", UnitPriceMinor: 2999, AvailableQty: 5}}}
	prices := &fakePrices{infos: []PriceInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, Sellable: true}}}
	checker := &fakeChecker{res: AvailabilityResult{CanFulfill: true}}
	// No WithAvailabilityCanary call at all — the constructor's default applies.
	svc := inventorySvc(prods, prices, checker)

	if _, err := svc.resolveCatalog(context.Background(), "user-1",
		[]AvailabilityLine{{SKUID: "1", Quantity: 1}}); err != nil {
		t.Fatalf("resolveCatalog error = %v", err)
	}
	if checker.calls != 1 {
		t.Errorf("inventory calls = %d, want 1 — the default must be full inventory, not 0%%", checker.calls)
	}
}

// A partially-open dial must actually split: some users on inventory, some on
// product, and each one consistently.
func TestResolveCatalog_CanarySplitsUsers(t *testing.T) {
	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "1", UnitPriceMinor: 2999, AvailableQty: 5}}}
	prices := &fakePrices{infos: []PriceInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, Sellable: true}}}
	checker := &fakeChecker{res: AvailabilityResult{CanFulfill: true}}
	svc := inventorySvc(prods, prices, checker).WithAvailabilityCanary(50, "")

	for i := 0; i < 100; i++ {
		if _, err := svc.resolveCatalog(context.Background(), fmt.Sprintf("user-%d", i),
			[]AvailabilityLine{{SKUID: "1", Quantity: 1}}); err != nil {
			t.Fatalf("resolveCatalog error = %v", err)
		}
	}
	if checker.calls == 0 || prods.calls == 0 {
		t.Errorf("50%% canary sent everything one way: inventory=%d product=%d", checker.calls, prods.calls)
	}
	if checker.calls+prods.calls != 100 {
		t.Errorf("reads = %d, want exactly one authority per call", checker.calls+prods.calls)
	}
}

// THE property the canary exists for, asserted across a real funnel.
//
// A user's checkout spans two availability reads — CreateSession and the confirm
// revalidate — and they must consult the SAME authority. Bucketing confirm on
// anything other than the user id (the session id is the tempting mistake, since
// it is right there) splits the funnel: a session created against Inventory gets
// confirmed against Product, so it can be accepted as in-stock and then refused,
// which looks like an inventory bug and is not one.
//
// The percentage is chosen so the two candidate keys land on OPPOSITE sides:
// user "7" buckets at 6 and session "sess-1" at 37, so at 20% only the user id is
// inside the canary. A confirm that bucketed on the session id would fall back to
// Product here and fail the assertion.
func TestCanary_FunnelUsesOneAuthorityForCreateAndConfirm(t *testing.T) {
	// user "7" is bucket 54 and session "sess-1" is bucket 12, so a 30% dial
	// EXCLUDES the user and would INCLUDE the session id — a confirm that bucketed
	// on the wrong key routes to inventory and fails the assertions below.
	const canaryPct = 30

	prods := &fakeProducts{infos: []ProductInfo{
		{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, AvailableQty: 5},
	}}
	prices := &fakePrices{infos: []PriceInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, Sellable: true}}}
	checker := &fakeChecker{res: AvailabilityResult{CanFulfill: true}}

	cart := &fakeCart{lines: []CartLine{{ProductID: "1", ProductName: "Mouse", Quantity: 2, CartPriceMinor: 2999}}}
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	orders := &fakeOrders{orderID: "42", status: "pending"}

	svc := newSvc(repo, cart, prods).
		WithConfirm(idem, orders).
		WithAvailabilitySource(AvailabilitySourceInventory, 0, nil).
		WithInventoryMode(prices, checker).
		WithAvailabilityCanary(canaryPct, "")

	if _, _, err := svc.CreateSession(context.Background(), "7"); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if checker.calls != 0 || prods.calls != 1 {
		t.Fatalf("create: inventory=%d product=%d; want 0/1 — user 7 is OUTSIDE a %d%% canary",
			checker.calls, prods.calls, canaryPct)
	}

	if _, err := svc.Confirm(context.Background(), "7", "sess-1", "key-1"); err != nil {
		t.Fatalf("Confirm: %v", err)
	}
	if checker.calls != 0 {
		t.Errorf("confirm consulted inventory %d time(s) while create used product; the funnel split across two authorities — confirm must bucket on the USER id, not the session id",
			checker.calls)
	}
	if prods.calls != 2 {
		t.Errorf("product answered %d read(s), want 2 (create + confirm on one authority)", prods.calls)
	}
}

// The path counter is the ONLY view of the canary's real exposure: a dial set to
// 50 that buckets badly, or a source flip that silently kept every read on
// Product, is visible here and nowhere else. So the labels have to be asserted
// with ASYMMETRIC counts — swapping the two constants would make the dashboard
// confidently wrong (it would read "still on Product" while traffic is on
// Inventory, and the operator would ramp further), and equal counts per arm cannot
// tell the correct code from the swapped code.
func TestResolveCatalog_RecordsTheAuthorityRoutedTo(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	// The instrument is captured at package init from the global provider, so a
	// provider swapped in here does not reach it. Re-create the counter against
	// the test provider and point the package variable at it for the duration.
	m := otel.Meter("checkout")
	c, err := m.Int64Counter("checkout.availability.path")
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	origCounter := availabilityPathCounter
	availabilityPathCounter = c
	t.Cleanup(func() { availabilityPathCounter = origCounter })

	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "1", UnitPriceMinor: 2999, AvailableQty: 5}}}
	prices := &fakePrices{infos: []PriceInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, Sellable: true}}}
	checker := &fakeChecker{res: AvailabilityResult{CanFulfill: true}}
	svc := inventorySvc(prods, prices, checker).WithAvailabilityCanary(30, "")

	line := []AvailabilityLine{{SKUID: "1", Quantity: 1}}
	// Counts are deliberately ASYMMETRIC (2 vs 1). With one read per arm the
	// assertion is satisfied identically by correct code and by code that swapped
	// the two label constants, so it would prove nothing about the labels at all.
	for _, user := range []string{"sess-1", "sess-1"} { // bucket 12 — inside 30%
		if _, err := svc.resolveCatalog(context.Background(), user, line); err != nil {
			t.Fatalf("resolveCatalog: %v", err)
		}
	}
	if _, err := svc.resolveCatalog(context.Background(), "7", line); err != nil { // bucket 54 — outside
		t.Fatalf("resolveCatalog: %v", err)
	}

	got := pathCounts(t, reader)
	if got[availabilityPathInventory] != 2 || got[availabilityPathProduct] != 1 {
		t.Errorf("path counts = %v, want inventory=2 product=1 — the label must name the authority the read was routed to", got)
	}
}

// pathCounts sums checkout.availability.path by its `path` attribute.
func pathCounts(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, mm := range sm.Metrics {
			if mm.Name != "checkout.availability.path" {
				continue
			}
			sum, ok := mm.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				if v, found := dp.Attributes.Value(attribute.Key("path")); found {
					out[v.AsString()] += dp.Value
				}
			}
		}
	}
	return out
}

// Pins the ACCEPTED limitation, so nobody later "fixes" it silently or assumes a
// guarantee the design does not make.
//
// Membership is recomputed per read from the current configuration, so moving the
// dial while a session is open CAN split that session's two reads. It is a
// documented trade-off, not a bug: checkout only ever checks stock and never
// reserves (ADR-020), so the worst outcome is the existing requote path, and
// persisting the authority per session would mean a schema column for a dial that
// phase 4 deletes. The operational rule lives in the cutover runbook (space ramp
// steps by at least the session TTL).
func TestCanary_MovingTheDialCanSplitAnOpenSession(t *testing.T) {
	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, AvailableQty: 5}}}
	prices := &fakePrices{infos: []PriceInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, Sellable: true}}}
	checker := &fakeChecker{res: AvailabilityResult{CanFulfill: true}}
	svc := inventorySvc(prods, prices, checker).WithAvailabilityCanary(0, "")

	line := []AvailabilityLine{{SKUID: "1", Quantity: 1}}
	// Create-time read under a closed dial: Product answers.
	if _, err := svc.resolveCatalog(context.Background(), "7", line); err != nil {
		t.Fatalf("resolveCatalog: %v", err)
	}
	if prods.calls != 1 || checker.calls != 0 {
		t.Fatalf("with the dial at 0, product=%d inventory=%d; want 1/0", prods.calls, checker.calls)
	}

	// The dial moves to 60% between the two reads. User "7" (bucket 54) is now
	// inside it. This simulates two CONFIGURATIONS, not a supported live reload —
	// WithAvailabilityCanary is wiring-time only (see its doc).
	svc.WithAvailabilityCanary(60, "")
	if _, err := svc.resolveCatalog(context.Background(), "7", line); err != nil {
		t.Fatalf("resolveCatalog after the ramp: %v", err)
	}
	if checker.calls != 1 {
		t.Errorf("inventory calls after the ramp = %d, want 1 — this documents that a ramp DOES move an in-flight user; if this ever changes, the guarantee in canary.go must change with it",
			checker.calls)
	}
}

// The salt must actually change the bucket, and must keep it deterministic —
// otherwise it is decoration. Without it the bucket is a pure function of a
// public algorithm and the caller's own subject claim, so a caller can register
// until they land on the arm they want and the percentage stops bounding exposure.
func TestCanary_SaltChangesTheBucketAndStaysDeterministic(t *testing.T) {
	// HMAC-SHA256 keyed with "pepper". Pinned for the same reason as the unkeyed
	// values: changing the construction re-shuffles every user mid-rollout.
	for user, want := range map[string]uint32{
		"1":       0x13c71d08,
		"7":       0x86693d50,
		"alice":   0xf2f95d05,
		"user-42": 0xd018d3ec,
	} {
		if got := userBucketHash("pepper", user); got != want {
			t.Errorf("userBucketHash(pepper, %q) = %#08x, want %#08x", user, got, want)
		}
		// Deterministic across calls — the whole point of not using maphash.
		if userBucketHash("pepper", user) != userBucketHash("pepper", user) {
			t.Errorf("salted hash of %q is not deterministic", user)
		}
	}

	// A different key must move users. User "7" sits at bucket 54 unkeyed and 20
	// keyed with "pepper", so a 30% dial excludes them in one world and includes
	// them in the other — the property that makes an offline bucket computation
	// useless to an attacker who does not hold the key.
	if inCanary("", "7", 30) {
		t.Error("user 7 should be OUTSIDE a 30% canary unkeyed (bucket 54)")
	}
	if !inCanary("pepper", "7", 30) {
		t.Error("user 7 should be INSIDE a 30% canary keyed with pepper (bucket 20); the key is not reaching the hash")
	}
}

// Stickiness must survive salting — the salt comes from configuration, never from
// process state, so two replicas with the same config still agree.
func TestCanary_SaltedAssignmentIsStillSticky(t *testing.T) {
	for _, salt := range []string{"", "pepper", "another-salt"} {
		for i := 0; i < 100; i++ {
			user := fmt.Sprintf("user-%d", i)
			first := inCanary(salt, user, 50)
			for r := 0; r < 3; r++ {
				if inCanary(salt, user, 50) != first {
					t.Fatalf("salt=%q user=%q flipped between calls", salt, user)
				}
			}
		}
	}
}

// The configured key has to reach the bucket decision, not just exist. Asserted
// through the routing OUTCOME rather than the helper, so it covers the whole path
// from the builder to the hash.
func TestResolveCatalog_UsesTheConfiguredKey(t *testing.T) {
	prods := &fakeProducts{infos: []ProductInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, AvailableQty: 5}}}
	prices := &fakePrices{infos: []PriceInfo{{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, Sellable: true}}}
	checker := &fakeChecker{res: AvailabilityResult{CanFulfill: true}}
	svc := inventorySvc(prods, prices, checker).WithAvailabilityCanary(30, "pepper")

	if _, err := svc.resolveCatalog(context.Background(), "7",
		[]AvailabilityLine{{SKUID: "1", Quantity: 1}}); err != nil {
		t.Fatalf("resolveCatalog: %v", err)
	}
	// Unkeyed, user "7" is bucket 54 and would be OUTSIDE a 30% dial; keyed with
	// "pepper" they are bucket 20 and inside it. So inventory answering here proves
	// the key reached the routing decision, and product answering would prove it
	// was dropped somewhere between config and hash.
	if checker.calls != 1 {
		t.Errorf("inventory calls = %d, want 1 — the key is not reaching the routing decision", checker.calls)
	}
	if prods.calls != 0 {
		t.Errorf("product answered %d read(s) for a user the KEYED bucket puts inside the canary", prods.calls)
	}
}

// The membership boundary, pinned exactly. `< pct` vs `<= pct` differs for one
// bucket in a hundred, which is invisible to a distribution test but doubles the
// blast radius at pct=1 — the first ramp step, the one the dial exists for.
//
// Numeric user id 208 hashes to bucket exactly 20 unkeyed, so it is the witness:
// OUT at pct=20 (20 < 20 is false) and IN at pct=21.
func TestCanary_MembershipBoundaryIsExclusive(t *testing.T) {
	const onTheBoundary = "208" // unkeyed bucket == 20

	if inCanary("", onTheBoundary, 20) {
		t.Error("a user whose bucket EQUALS pct must be outside the canary; `<=` would expose pct+1 points, i.e. 2x the intended blast radius at pct=1")
	}
	if !inCanary("", onTheBoundary, 21) {
		t.Error("a user whose bucket is below pct must be inside the canary")
	}
	// And the dial only ever opens: nobody leaves the canary as pct rises.
	for pct := 21; pct <= 100; pct++ {
		if !inCanary("", onTheBoundary, pct) {
			t.Fatalf("user left the canary at pct=%d after being inside at 21; ramping must be monotonic", pct)
		}
	}
}

// An unidentified caller must never be routed by the canary. Without this, every
// subject-less request shares one bucket, so that whole cohort flips arms wholesale
// at a single ramp step — the opposite of a gradual rollout.
func TestCanary_EmptyUserIsNeverInTheCanary(t *testing.T) {
	for _, pct := range []int{1, 50, 99} {
		if inCanary("", "", pct) {
			t.Errorf("empty user id is inside a %d%% canary; an unidentified caller must not be routed by it", pct)
		}
	}
	// 100 still means everyone: the dial being fully open is not a routing
	// decision about a particular user, it is the end state of the migration.
	if !inCanary("", "", 100) {
		t.Error("at 100% the canary must include every read, including an unidentified one")
	}
}

// Production user ids are numeric (Confirm parses an int32), so the spread has to
// hold for that shape and not only for the `user-%d` strings the other tests use.
func TestCanary_SpreadHoldsForNumericIds(t *testing.T) {
	const users = 2000
	for _, pct := range []int{10, 50, 90} {
		in := 0
		for i := 1; i <= users; i++ {
			if inCanary("", fmt.Sprintf("%d", i), pct) {
				in++
			}
		}
		if share := in * 100 / users; share < pct-5 || share > pct+5 {
			t.Errorf("numeric ids at pct=%d gave %d%%; the dial must reflect real exposure for production id shapes", pct, share)
		}
	}
}

// The fingerprint exists so two pods can be compared without printing the key. It
// must not leak the key, must be stable, and must be empty exactly when no key is
// set — that emptiness is what makes "unkeyed" visible in a startup log.
func TestSaltFingerprint(t *testing.T) {
	if got := SaltFingerprint(""); got != "" {
		t.Errorf("SaltFingerprint(\"\") = %q, want empty so an unkeyed deployment is obvious", got)
	}
	a, b := SaltFingerprint("pepper"), SaltFingerprint("pepper")
	if a != b {
		t.Error("fingerprint is not stable")
	}
	if a == SaltFingerprint("other") {
		t.Error("two different keys share a fingerprint; it cannot be used to compare pods")
	}
	if strings.Contains(a, "pepper") {
		t.Errorf("fingerprint %q contains the key", a)
	}
}

// The shadow soak must keep running whatever the canary says. It is the evidence
// that gates the read flip, so gating it on the dial would silently remove the
// data the flip depends on — and asserted through CreateSession, because
// resolveCatalog does not run the compare, its callers do.
//
// A REAL fetcher matters here: with a nil one the shadow path cannot fire in
// either variant, so the test would pass whether or not the canary wrongly gated
// it.
func TestCanary_DoesNotGateTheShadowSoak(t *testing.T) {
	prods := &fakeProducts{infos: []ProductInfo{
		{ProductID: "1", Name: "Mouse", UnitPriceMinor: 2999, AvailableQty: 5},
	}}
	shadow := &fakeInventory{avails: []SkuAvailability{{SKUID: "1", AvailableQty: 5}}}
	cart := &fakeCart{lines: []CartLine{{ProductID: "1", ProductName: "Mouse", Quantity: 2, CartPriceMinor: 2999}}}

	svc := newSvc(&fakeRepo{}, cart, prods).
		WithAvailabilitySource(AvailabilitySourceShadow, 100, shadow).
		WithAvailabilityCanary(0, "") // fully closed — must not matter here

	if _, _, err := svc.CreateSession(context.Background(), "user-1"); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	svc.awaitShadow()

	if shadow.callCount() == 0 {
		t.Error("the shadow compare did not run at a 0% canary; the canary must not gate the soak that gates the read flip")
	}
}
