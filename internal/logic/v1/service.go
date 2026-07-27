// Package v1 holds checkout's business logic: the session FSM, the cart
// snapshot with product-authoritative price re-validation (RFC-0015), and the
// lazy-expiry backstop. Transport-free: web handlers and (in P2) the Temporal
// worker both drive this layer.
package v1

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/duynhlab/checkout-service/internal/core/domain"
	"github.com/duynhlab/checkout-service/middleware"
)

// CartLine is the item-list view checkout snapshots (from cart.v1/GetCart).
type CartLine struct {
	ProductID      string
	ProductName    string
	Quantity       int
	CartPriceMinor int64
}

// ProductInfo is the price/stock authority view (from product.v1/GetProducts).
type ProductInfo struct {
	ProductID      string
	Name           string
	UnitPriceMinor int64
	AvailableQty   int
}

// AbandonmentNotifier is the logic-layer port for the abandonment-workflow
// signals (RFC-0015 P2, ADR-019). All methods are best-effort fire-and-forget;
// a nil notifier disables them (Temporal absent → lazy-only expiry).
type AbandonmentNotifier interface {
	SessionStarted(ctx context.Context, sessionID string)
	SessionActivity(ctx context.Context, sessionID string)
	SessionFinalized(ctx context.Context, sessionID string)
}

// CartFetcher is the logic-layer port for the cart snapshot.
type CartFetcher interface {
	GetCart(ctx context.Context, userID string) ([]CartLine, error)
}

// ProductFetcher is the logic-layer port for price/stock re-validation.
type ProductFetcher interface {
	GetProducts(ctx context.Context, ids []string) ([]ProductInfo, error)
}

// SkuAvailability is inventory-service's availability view (from
// inventory.v1/BatchGetAvailability). AvailableQty is the derived
// available-to-promise (int64, no lossy narrowing); Known is true only when
// inventory returned a definite status for the SKU (RFC-0021 P2-4 shadow reads).
type SkuAvailability struct {
	SKUID        string
	AvailableQty int64
	Known        bool
}

// InventoryAvailabilityFetcher is the logic-layer port for inventory-service's
// availability, used ONLY for phase-2 shadow comparison — never for a checkout
// decision in this phase (Product stays authoritative). Best-effort: a nil
// fetcher or a non-shadow source disables it.
type InventoryAvailabilityFetcher interface {
	BatchGetAvailability(ctx context.Context, skuIDs []string) ([]SkuAvailability, error)
}

// PriceInfo is Product's price-only view (from product.v1/BatchGetCurrentPrices)
// — the price authority half of the split read (RFC-0021 P2-5). It carries no
// availability: stock comes from Inventory in inventory mode.
type PriceInfo struct {
	ProductID      string
	Name           string
	UnitPriceMinor int64
	Currency       string
	Sellable       bool
}

// PriceFetcher is the logic-layer port for Product's price authority in
// inventory mode (RFC-0021 P2-5).
type PriceFetcher interface {
	BatchGetCurrentPrices(ctx context.Context, skuIDs []string) ([]PriceInfo, error)
}

// AvailabilityLine is one basket line (sku + requested qty) for a check.
type AvailabilityLine struct {
	SKUID    string
	Quantity int
}

// Shortage names a SKU that cannot be fully fulfilled and by how much.
type Shortage struct {
	SKUID              string
	Requested          int64
	AvailableToPromise int64
}

// AvailabilityResult is Inventory's basket answer (from
// inventory.v1/CheckAvailability): whether the whole basket is fulfillable and
// the per-SKU shortages when it is not.
type AvailabilityResult struct {
	CanFulfill bool
	Shortages  []Shortage
}

// AvailabilityChecker is the logic-layer port for Inventory's basket
// availability gate in inventory mode (RFC-0021 P2-5). Distinct from the
// shadow-only InventoryAvailabilityFetcher: this one drives a checkout decision.
type AvailabilityChecker interface {
	CheckAvailability(ctx context.Context, items []AvailabilityLine) (AvailabilityResult, error)
}

// Availability source modes for CHECKOUT_AVAILABILITY_SOURCE (RFC-0021 P2-4).
// product: inventory is never called (default, current behavior). shadow:
// inventory is called in parallel and compared, but Product decides. inventory:
// reserved for P2-5 (inventory decides) — treated as product here.
const (
	AvailabilitySourceProduct   = "product"
	AvailabilitySourceShadow    = "shadow"
	AvailabilitySourceInventory = "inventory"
)

// ShippingQuoter is the logic-layer port for shipping.v1/GetQuote (RFC-0015
// P3): the fee authority for PUT …/shipping. ErrInvalidQuote marks an unknown
// method/region (→ 400); any other error is transport trouble (→ 503).
type ShippingQuoter interface {
	GetQuote(ctx context.Context, method, region string) (feeMinor int64, etaDays int32, err error)
}

// DefaultSessionTTL is the reset-on-activity session deadline (RFC-0015: the
// clock models user presence, nothing is reserved).
const DefaultSessionTTL = 30 * time.Minute

// defaultCurrency mirrors the platform's single-currency posture (RFC-0010).
const defaultCurrency = "USD"

// CheckoutService orchestrates checkout sessions.
type CheckoutService struct {
	repo     domain.SessionRepository
	cart     CartFetcher
	products ProductFetcher
	ttl      time.Duration
	// now is injectable for lazy-expiry tests.
	now func() time.Time
	// P2 confirm dependencies, wired via WithConfirm (nil pre-P2).
	idem   IdemStore
	orders OrderCreator
	// P3 shipping-quote dependency, wired via WithQuoter (nil = 0-fee stub).
	quoter ShippingQuoter
	// P2 abandonment notifier, wired via WithAbandonment (nil = disabled).
	notifier AbandonmentNotifier
	// RFC-0021 P2-4 availability shadow reads, wired via WithAvailabilitySource
	// (nil fetcher or non-"shadow" source = disabled — Product-only path).
	availabilitySource string
	availability       InventoryAvailabilityFetcher
	// shadowSamplePct (0..100) throttles how many ops are shadowed; 100 = every op.
	shadowSamplePct int
	// shadowWG tracks in-flight async shadow-compares so tests can await them;
	// the shadow path itself never blocks the request.
	shadowWG sync.WaitGroup
	// RFC-0021 P2-5 inventory-mode split reads, wired via WithInventoryMode
	// (nil = disabled; resolveCatalog falls back to Product's GetProducts).
	prices  PriceFetcher
	checker AvailabilityChecker
}

// WithQuoter wires the shipping GetQuote port (nil keeps the P2 0-fee stub —
// local dev without shipping still works, totals just have no fee/tax).
func (s *CheckoutService) WithQuoter(q ShippingQuoter) *CheckoutService {
	s.quoter = q
	return s
}

// WithAbandonment wires the abandonment-workflow notifier (nil-safe).
func (s *CheckoutService) WithAbandonment(n AbandonmentNotifier) *CheckoutService {
	s.notifier = n
	return s
}

// WithAvailabilitySource wires phase-2 inventory shadow reads (RFC-0021 P2-4).
// source is the validated CHECKOUT_AVAILABILITY_SOURCE flag; samplePct (0..100)
// throttles the shadow volume. A nil fetcher or a non-"shadow" source disables
// the inventory call entirely (Product-only path).
func (s *CheckoutService) WithAvailabilitySource(source string, samplePct int, inv InventoryAvailabilityFetcher) *CheckoutService {
	s.availabilitySource = source
	s.shadowSamplePct = samplePct
	s.availability = inv
	return s
}

// WithInventoryMode wires the phase-2 inventory-mode split-read dependencies
// (RFC-0021 P2-5): Product's price authority + Inventory's availability gate.
// They are only exercised when the source is `inventory`; nil keeps the
// Product-only path even if the flag is misconfigured.
func (s *CheckoutService) WithInventoryMode(prices PriceFetcher, checker AvailabilityChecker) *CheckoutService {
	s.prices = prices
	s.checker = checker
	return s
}

// resolveCatalog fetches per-line price + availability honoring the availability
// source. product/shadow → one Product `GetProducts` (price+stock, today's
// behavior; shadow-compare runs separately, async). inventory → Product
// `BatchGetCurrentPrices` (price authority) + Inventory `CheckAvailability`
// (availability gate), merged into `[]ProductInfo` so downstream stock checks
// (AvailableQty >= requested) work unchanged. A CheckAvailability transport
// error propagates as an error — the caller maps it to `ErrUpstream` (503),
// NEVER to out-of-stock (fail-closed: a timeout is not a shortage).
func (s *CheckoutService) resolveCatalog(ctx context.Context, lines []AvailabilityLine) ([]ProductInfo, error) {
	ids := make([]string, len(lines))
	for i, l := range lines {
		ids[i] = l.SKUID
	}
	if s.availabilitySource != AvailabilitySourceInventory || s.prices == nil || s.checker == nil {
		return s.products.GetProducts(ctx, ids)
	}
	// Inventory mode: Product prices + Inventory availability, fetched
	// CONCURRENTLY (independent reads) so tail latency is the slower call, not
	// the sum. Each goroutine recovers so a panic mapping a malformed reply
	// becomes an error, never a process crash. Any transport error propagates →
	// caller maps to ErrUpstream (fail-closed: a timeout is never a shortage).
	type priceRes struct {
		infos []PriceInfo
		err   error
	}
	type availRes struct {
		res AvailabilityResult
		err error
	}
	pc := make(chan priceRes, 1)
	ac := make(chan availRes, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				pc <- priceRes{err: fmt.Errorf("price fetch panicked: %v", r)}
			}
		}()
		infos, err := s.prices.BatchGetCurrentPrices(ctx, ids)
		pc <- priceRes{infos: infos, err: err}
	}()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ac <- availRes{err: fmt.Errorf("availability check panicked: %v", r)}
			}
		}()
		res, err := s.checker.CheckAvailability(ctx, lines)
		ac <- availRes{res: res, err: err}
	}()
	pr, ar := <-pc, <-ac
	if pr.err != nil {
		return nil, pr.err
	}
	if ar.err != nil {
		return nil, ar.err
	}
	return mergeCatalog(lines, pr.infos, ar.res), nil
}

// mergeCatalog reshapes split inventory-mode reads into the ProductInfo view the
// snapshot/re-validation logic already understands, keyed to the requested
// lines. `CanFulfill` is the AUTHORITATIVE basket verdict: when Inventory says
// the basket cannot be fulfilled, every line is blocked (AvailableQty=0) rather
// than trusting the per-SKU Shortages list — the contract makes no completeness
// guarantee for Shortages when CanFulfill is false, and one order ships from one
// warehouse (RFC-0021), so a basket that can't be fulfilled fails as a whole.
// This also sidesteps duplicate-SKU lines and any int64 ATP arithmetic. When the
// basket CAN be fulfilled, a stray shortage still blocks that line (defense
// against a contradictory reply). Only requested, sellable, correctly-priced
// SKUs become entries; anything else reads as delisted (omitted, like
// GetProducts omitting a SKU). A cleared line gets AvailableQty = requested so
// the existing `requested > AvailableQty` shortage test passes it.
func mergeCatalog(lines []AvailabilityLine, prices []PriceInfo, avail AvailabilityResult) []ProductInfo {
	reqByID := make(map[string]int, len(lines))
	for _, l := range lines {
		reqByID[l.SKUID] = l.Quantity
	}
	short := make(map[string]struct{}, len(avail.Shortages))
	for _, sh := range avail.Shortages {
		short[sh.SKUID] = struct{}{}
	}
	out := make([]ProductInfo, 0, len(prices))
	for _, p := range prices {
		req, requested := reqByID[p.ProductID]
		if !requested || !p.Sellable || (p.Currency != "" && p.Currency != defaultCurrency) {
			// Not asked for, unsellable, or a currency we can't charge as USD:
			// omit ⇒ reads as delisted, exactly like GetProducts leaving it out.
			continue
		}
		available := req // cleared ⇒ AvailableQty == requested (passes the gate)
		if !avail.CanFulfill {
			available = 0 // basket unfulfillable ⇒ block every line
		} else if _, isShort := short[p.ProductID]; isShort {
			available = 0 // contradictory shortage on a fulfillable basket ⇒ block it
		}
		out = append(out, ProductInfo{
			ProductID:      p.ProductID,
			Name:           p.Name,
			UnitPriceMinor: p.UnitPriceMinor,
			AvailableQty:   available,
		})
	}
	return out
}

const (
	// shadowCompareTimeout bounds the whole detached inventory read (created at
	// admission, so scheduling delay counts against it too).
	shadowCompareTimeout = 2 * time.Second
	// shadowMaxInflight caps concurrent shadow compares so a slow inventory can
	// never let goroutines/streams grow with request rate — excess is shed.
	shadowMaxInflight = 32
)

// shadowSem is the process-wide admission bound for shadow compares. Buffered to
// shadowMaxInflight; a full channel means "shed this one" (recorded "skipped").
var shadowSem = make(chan struct{}, shadowMaxInflight)

// maybeShadowCompare fires an availability shadow-compare against
// inventory-service when the source is "shadow" (RFC-0021 P2-4). Fire-and-forget
// on a detached, timeout-bounded, concurrency-capped, panic-safe goroutine: it
// NEVER adds latency to, fails, crashes, or otherwise affects the caller's
// request — it only emits inventory_shadow_compare_total{result}. Product stays
// authoritative. ids is the SKU set just fetched.
func (s *CheckoutService) maybeShadowCompare(ctx context.Context, ids []string) {
	if s.availabilitySource != AvailabilitySourceShadow || s.availability == nil || len(ids) == 0 {
		return
	}
	// Sample down the shadow volume (operator dial; 100 = every op).
	//nolint:gosec // sampling decision for telemetry, not security-sensitive
	if s.shadowSamplePct < 100 && rand.IntN(100) >= s.shadowSamplePct {
		return
	}
	// Hard concurrency bound: shed load rather than pile up goroutines/streams
	// when inventory is slow.
	select {
	case shadowSem <- struct{}{}:
	default:
		recordShadowCompare(ctx, "skipped")
		return
	}
	skus := append([]string(nil), ids...)
	// Detach from the request context (cancelled on handler return) but bound
	// the whole thing — created HERE so time waiting to be scheduled also counts.
	bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), shadowCompareTimeout)

	s.shadowWG.Add(1)
	go func() {
		defer s.shadowWG.Done()
		defer cancel()
		defer func() { <-shadowSem }()
		// Best-effort contract: an unrecovered panic in ANY goroutine crashes
		// the whole process, so a panic here (e.g. a nil deref mapping a
		// malformed inventory reply) must be swallowed as a shadow "error".
		defer func() {
			if r := recover(); r != nil {
				recordShadowCompare(context.Background(), "error")
			}
		}()

		avails, err := s.availability.BatchGetAvailability(bg, skus)
		if err != nil {
			recordShadowCompare(bg, "error")
			return
		}
		recordShadowCompare(bg, compareStructural(skus, avails))
	}()
}

// compareStructural is the phase-2 shadow signal (RFC-0021 P2-4). It is
// STRUCTURAL, not an exact-quantity compare: in phase 2 inventory is not yet
// written (order writes land in phase 3), so its balances are the frozen
// backfill snapshot and would legitimately drift from Product's live stock —
// exact equality would be misleading. Instead it asks "does inventory KNOW every
// requested SKU and answer sanely?", surfacing backfill/SKU-namespace gaps:
//   - "missing"  — a requested SKU is absent from the inventory response
//   - "unknown"  — a SKU is present but has no definite status, or negative ATP
//   - "ok"       — every SKU present, definite status, non-negative ATP
//
// "missing" outranks "unknown" (a gap is the harder signal). Pure, no side effects.
func compareStructural(ids []string, inventory []SkuAvailability) string {
	inv := make(map[string]SkuAvailability, len(inventory))
	for _, a := range inventory {
		inv[a.SKUID] = a
	}
	unknown := false
	for _, id := range ids {
		a, ok := inv[id]
		if !ok {
			return "missing"
		}
		if !a.Known || a.AvailableQty < 0 {
			unknown = true
		}
	}
	if unknown {
		return "unknown"
	}
	return "ok"
}

// awaitShadow blocks until all in-flight shadow-compares finish. Test-only
// determinism helper; production never waits on best-effort telemetry.
func (s *CheckoutService) awaitShadow() { s.shadowWG.Wait() }

// NewCheckoutService wires the logic layer. ttl <= 0 falls back to
// DefaultSessionTTL.
func NewCheckoutService(repo domain.SessionRepository, cart CartFetcher, products ProductFetcher, ttl time.Duration) *CheckoutService {
	if ttl <= 0 {
		ttl = DefaultSessionTTL
	}
	return &CheckoutService{repo: repo, cart: cart, products: products, ttl: ttl, now: time.Now}
}

// CreateSession snapshots the user's cart into a new session — or returns the
// existing active session (created=false): POST /sessions is idempotent, one
// active session per user. Prices come from product (the checkout-time
// authority); cart's denormalized price is kept per line for the
// price-changed diff. An empty cart is ErrEmptyCart.
func (s *CheckoutService) CreateSession(ctx context.Context, userID string) (*domain.Session, bool, error) {
	ctx, span := middleware.StartSpan(ctx, "checkout.session.create", trace.WithAttributes(
		attribute.String("layer", "logic"),
		attribute.String("user.id", userID),
	))
	defer span.End()

	// Idempotent create: an active session short-circuits (after the lazy
	// expiry check — an expired "active" session is retired first).
	if existing, err := s.repo.FindActiveByUserID(ctx, userID); err == nil {
		if !s.lazyExpire(ctx, existing) {
			span.SetAttributes(attribute.Bool("session.reused", true))
			// Re-opening checkout is user presence: reset BOTH expiry clocks
			// (DB deadline + workflow timer), like any other activity.
			s.touch(ctx, existing)
			return existing, false, nil
		}
	} else if !errors.Is(err, domain.ErrSessionNotFound) {
		span.RecordError(err)
		return nil, false, err
	}

	lines, err := s.cart.GetCart(ctx, userID)
	if err != nil {
		span.RecordError(err)
		return nil, false, ErrUpstream
	}
	if len(lines) == 0 {
		return nil, false, ErrEmptyCart
	}

	availLines := make([]AvailabilityLine, 0, len(lines))
	ids := make([]string, 0, len(lines))
	for _, l := range lines {
		availLines = append(availLines, AvailabilityLine{SKUID: l.ProductID, Quantity: l.Quantity})
		ids = append(ids, l.ProductID)
	}
	// Product mode: one GetProducts. Inventory mode (P2-5): Product prices +
	// Inventory availability, merged. A CheckAvailability failure → ErrUpstream.
	infos, err := s.resolveCatalog(ctx, availLines)
	if err != nil {
		span.RecordError(err)
		return nil, false, ErrUpstream
	}
	byID := make(map[string]ProductInfo, len(infos))
	for _, p := range infos {
		byID[p.ProductID] = p
	}
	// RFC-0021 P2-4: shadow-compare availability vs inventory-service (no-op
	// unless source=shadow). Best-effort, async — never affects this create.
	s.maybeShadowCompare(ctx, ids)

	items := make([]domain.SessionItem, 0, len(lines))
	var subtotal int64
	priceChanged := 0
	for _, l := range lines {
		info, ok := byID[l.ProductID]
		if !ok {
			// Product gone from the catalog since it was carted: snapshot the
			// line with the cart price and flag it — confirm-time
			// re-validation (P2) is the gate that blocks it.
			info = ProductInfo{ProductID: l.ProductID, Name: l.ProductName, UnitPriceMinor: l.CartPriceMinor}
		}
		item := domain.SessionItem{
			ProductID:      l.ProductID,
			ProductName:    l.ProductName,
			Quantity:       l.Quantity,
			UnitPriceMinor: info.UnitPriceMinor,
			CartPriceMinor: l.CartPriceMinor,
			PriceChanged:   !ok || info.UnitPriceMinor != l.CartPriceMinor,
		}
		if item.PriceChanged {
			priceChanged++
		}
		subtotal += item.UnitPriceMinor * int64(item.Quantity)
		items = append(items, item)
	}

	now := s.now()
	session := &domain.Session{
		UserID:        userID,
		Status:        domain.StatusOpen,
		Items:         items,
		SubtotalMinor: subtotal,
		// Shipping fee, tax, and discount join in P2/P3; a fresh session's
		// total is its subtotal.
		TotalMinor: subtotal,
		Currency:   defaultCurrency,
		ExpiresAt:  now.Add(s.ttl),
	}
	if err := s.repo.Create(ctx, session); err != nil {
		if errors.Is(err, domain.ErrActiveSessionExists) {
			// Lost a concurrent-create race: surface the winner.
			if winner, ferr := s.repo.FindActiveByUserID(ctx, userID); ferr == nil {
				return winner, false, nil
			}
		}
		span.RecordError(err)
		return nil, false, err
	}
	span.SetAttributes(
		attribute.Int("items.count", len(items)),
		attribute.Int("items.price_changed", priceChanged),
	)
	if s.notifier != nil {
		s.notifier.SessionStarted(ctx, session.ID)
	}
	return session, true, nil
}

// GetSession returns the caller's session. Foreign or unknown ids are both
// ErrSessionNotFound (anti-IDOR); an elapsed TTL is recorded lazily and
// surfaces as ErrSessionExpired.
func (s *CheckoutService) GetSession(ctx context.Context, userID, id string) (*domain.Session, error) {
	ctx, span := middleware.StartSpan(ctx, "checkout.session.get", trace.WithAttributes(
		attribute.String("layer", "logic"),
	))
	defer span.End()

	session, err := s.ownedSession(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if s.lazyExpire(ctx, session) {
		return nil, ErrSessionExpired
	}
	return session, nil
}

// SetAddress stores the shipping address and moves the session to
// address_set (a legal re-entry from any pre-confirm state).
func (s *CheckoutService) SetAddress(ctx context.Context, userID, id string, addr *domain.Address) (*domain.Session, error) {
	ctx, span := middleware.StartSpan(ctx, "checkout.session.set_address", trace.WithAttributes(
		attribute.String("layer", "logic"),
	))
	defer span.End()

	session, err := s.ownedSession(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if s.lazyExpire(ctx, session) {
		return nil, ErrSessionExpired
	}
	if !CanTransition(session.Status, domain.StatusAddressSet) {
		return nil, ErrInvalidTransition
	}
	// The invalidation zeroes fee/tax, so an applied promo must be re-clamped
	// against the shrunk total (a fixed discount larger than the bare
	// subtotal would otherwise push the composed total negative).
	discount, err := s.sessionDiscount(ctx, session, session.SubtotalMinor, 0, 0)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if err := s.repo.SetAddress(ctx, session.ID, session.Status, addr, discount); err != nil {
		span.RecordError(err)
		return nil, err
	}
	session.Status = domain.StatusAddressSet
	session.Address = addr
	// Mirror the SQL quote invalidation (destination changed → the old fee,
	// tax, and method are meaningless) so the response tells the truth.
	session.ShippingMethod = ""
	session.ShippingFeeMinor = 0
	session.TaxMinor = 0
	session.DiscountMinor = discount
	session.TotalMinor = session.SubtotalMinor - discount
	s.touch(ctx, session)
	return session, nil
}

// SetShipping prices the chosen method via shipping's GetQuote (the fee
// authority, ADR: rates live in shipping-service), computes the flat tax on
// (subtotal + fee) from the seeded rule table, and moves the session to
// shipping_set with the total recomputed in SQL (RFC-0015 P3).
func (s *CheckoutService) SetShipping(ctx context.Context, userID, id, method string) (*domain.Session, error) {
	ctx, span := middleware.StartSpan(ctx, "checkout.session.set_shipping", trace.WithAttributes(
		attribute.String("layer", "logic"),
	))
	defer span.End()

	session, err := s.ownedSession(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if s.lazyExpire(ctx, session) {
		return nil, ErrSessionExpired
	}
	if !CanTransition(session.Status, domain.StatusShippingSet) {
		return nil, ErrInvalidTransition
	}
	if session.Address == nil {
		// FSM should make this unreachable (shipping requires address_set),
		// but the quote needs a region — fail closed, not with a nil deref.
		return nil, ErrInvalidTransition
	}

	feeMinor, taxMinor, err := s.priceShipping(ctx, method, session.Address.Country, session.SubtotalMinor)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	discount, err := s.sessionDiscount(ctx, session, session.SubtotalMinor, feeMinor, taxMinor)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if err := s.repo.SetShipping(ctx, session.ID, session.Status, session.UpdatedAt, method, feeMinor, taxMinor, discount); err != nil {
		span.RecordError(err)
		return nil, err
	}
	session.Status = domain.StatusShippingSet
	session.ShippingMethod = method
	session.ShippingFeeMinor = feeMinor
	session.TaxMinor = taxMinor
	session.DiscountMinor = discount
	session.TotalMinor = session.SubtotalMinor + feeMinor + taxMinor - discount
	s.touch(ctx, session)
	return session, nil
}

// priceShipping resolves the fee (shipping GetQuote) and the flat tax
// (rate_bps on subtotal + fee). A nil quoter keeps the 0-stub for both —
// degraded local dev, not an error.
func (s *CheckoutService) priceShipping(ctx context.Context, method, region string, subtotalMinor int64) (int64, int64, error) {
	if s.quoter == nil {
		return 0, 0, nil
	}
	feeMinor, _, err := s.quoter.GetQuote(ctx, method, region)
	if err != nil {
		if errors.Is(err, ErrInvalidQuote) {
			return 0, 0, ErrInvalidQuote
		}
		return 0, 0, ErrUpstream
	}
	if feeMinor < 0 {
		// Defense-in-depth: a negative fee would lower the payable amount.
		return 0, 0, ErrUpstream
	}
	bps, err := s.repo.GetTaxRateBps(ctx, region)
	if err != nil {
		return 0, 0, err
	}
	taxMinor, err := flatTax(subtotalMinor+feeMinor, bps)
	if err != nil {
		return 0, 0, err
	}
	return feeMinor, taxMinor, nil
}

// SetPayment attaches an opaque tok_ payment reference and moves the session
// to ready. PAN-shaped input is rejected BEFORE any persistence — the same
// PCI-shaped rule order and payment enforce.
func (s *CheckoutService) SetPayment(ctx context.Context, userID, id, token string) (*domain.Session, error) {
	ctx, span := middleware.StartSpan(ctx, "checkout.session.set_payment", trace.WithAttributes(
		attribute.String("layer", "logic"),
	))
	defer span.End()

	if !domain.ValidPaymentToken(token) {
		return nil, ErrInvalidPaymentToken
	}
	session, err := s.ownedSession(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if s.lazyExpire(ctx, session) {
		return nil, ErrSessionExpired
	}
	if !CanTransition(session.Status, domain.StatusReady) {
		return nil, ErrInvalidTransition
	}
	if err := s.repo.SetPaymentToken(ctx, session.ID, session.Status, token); err != nil {
		span.RecordError(err)
		return nil, err
	}
	session.Status = domain.StatusReady
	session.PaymentMethodToken = token
	s.touch(ctx, session)
	return session, nil
}

// flatTax computes base × bps/10000 with floor division (truncation is
// deliberate and identical at every call site — ≤1 minor unit, shopper's
// favor) and an overflow guard: the multiplication must not wrap before the
// division (review finding — order-side bounds allow subtotals large enough).
func flatTax(baseMinor int64, bps int32) (int64, error) {
	if baseMinor < 0 || baseMinor > math.MaxInt64/10_001 {
		return 0, ErrUpstream
	}
	return baseMinor * int64(bps) / 10_000, nil
}

// touch is the reset-on-activity half of the expiry contract: after any
// successful mutation the DB deadline moves to now+TTL (best-effort — the
// mutation already succeeded; a missed bump only shortens the session, and
// the abandonment workflow's own timer is reset by the activity signal).
func (s *CheckoutService) touch(ctx context.Context, session *domain.Session) {
	session.ExpiresAt = s.now().Add(s.ttl)
	_ = s.repo.Touch(ctx, session.ID, session.ExpiresAt)
	if s.notifier != nil {
		s.notifier.SessionActivity(ctx, session.ID)
	}
}

// Cancel moves the session to the terminal cancelled state. Cancelling an
// already-cancelled session is idempotent (no error); terminal states other
// than cancelled reject with ErrInvalidTransition.
func (s *CheckoutService) Cancel(ctx context.Context, userID, id string) error {
	ctx, span := middleware.StartSpan(ctx, "checkout.session.cancel", trace.WithAttributes(
		attribute.String("layer", "logic"),
	))
	defer span.End()

	session, err := s.ownedSession(ctx, userID, id)
	if err != nil {
		return err
	}
	if session.Status == domain.StatusCancelled {
		return nil // idempotent
	}
	if s.lazyExpire(ctx, session) {
		return ErrSessionExpired
	}
	if !CanTransition(session.Status, domain.StatusCancelled) {
		return ErrInvalidTransition
	}
	if err := s.repo.UpdateStatus(ctx, session.ID, session.Status, domain.StatusCancelled); err != nil {
		if errors.Is(err, domain.ErrStaleTransition) {
			// The abandonment timer can expire the row between our read and
			// this CAS; a cancel of an already-terminal session is a success
			// for the user, not a conflict.
			if fresh, ferr := s.repo.FindByID(ctx, session.ID); ferr == nil && fresh.Status.Terminal() {
				return nil
			}
		}
		span.RecordError(err)
		return err
	}
	if s.notifier != nil {
		s.notifier.SessionFinalized(ctx, session.ID)
	}
	return nil
}

// ownedSession loads a session and enforces ownership: a foreign session is
// reported exactly like a missing one.
func (s *CheckoutService) ownedSession(ctx context.Context, userID, id string) (*domain.Session, error) {
	session, err := s.repo.FindByID(ctx, id)
	if err != nil {
		if errors.Is(err, domain.ErrSessionNotFound) {
			return nil, ErrSessionNotFound
		}
		return nil, err
	}
	if session.UserID != userID {
		return nil, ErrSessionNotFound
	}
	return session, nil
}

// lazyExpire is the correctness backstop (RFC-0015): a session past its
// deadline is treated as expired on EVERY read and mutation, regardless of
// what the Temporal timer (P2) has done. It records the expiry best-effort —
// the predicate's answer never depends on the write succeeding.
func (s *CheckoutService) lazyExpire(ctx context.Context, session *domain.Session) bool {
	if session.Status.Terminal() {
		return session.Status == domain.StatusExpired
	}
	// `confirming` never lazily expires: the confirm flow (P2) owns that
	// state's fate — completed or back to shipping_set. Mirrors the FSM table
	// and MarkExpired's SQL predicate.
	if session.Status == domain.StatusConfirming {
		return false
	}
	if s.now().Before(session.ExpiresAt) {
		return false
	}
	// Best-effort record; MarkExpired is conditional and idempotent.
	_ = s.repo.MarkExpired(ctx, session.ID, domain.ExpiredByLazy)
	RecordSessionExpired(ctx, string(domain.ExpiredByLazy))
	return true
}
