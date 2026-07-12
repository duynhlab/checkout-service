// Package v1 holds checkout's business logic: the session FSM, the cart
// snapshot with product-authoritative price re-validation (RFC-0015), and the
// lazy-expiry backstop. Transport-free: web handlers and (in P2) the Temporal
// worker both drive this layer.
package v1

import (
	"context"
	"errors"
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

// CartFetcher is the logic-layer port for the cart snapshot.
type CartFetcher interface {
	GetCart(ctx context.Context, userID string) ([]CartLine, error)
}

// ProductFetcher is the logic-layer port for price/stock re-validation.
type ProductFetcher interface {
	GetProducts(ctx context.Context, ids []string) ([]ProductInfo, error)
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
}

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

	ids := make([]string, 0, len(lines))
	for _, l := range lines {
		ids = append(ids, l.ProductID)
	}
	infos, err := s.products.GetProducts(ctx, ids)
	if err != nil {
		span.RecordError(err)
		return nil, false, ErrUpstream
	}
	byID := make(map[string]ProductInfo, len(infos))
	for _, p := range infos {
		byID[p.ProductID] = p
	}

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
	if err := s.repo.SetAddress(ctx, session.ID, session.Status, addr); err != nil {
		span.RecordError(err)
		return nil, err
	}
	session.Status = domain.StatusAddressSet
	session.Address = addr
	s.touch(ctx, session)
	return session, nil
}

// SetShipping records the chosen shipping method and moves the session to
// shipping_set. P2 stubs the fee and tax at 0 — shipping's GetQuote and the
// tax rules join in P3 — so the total stays subtotal − discount.
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
	const feeMinor = 0 // P2 stub until the P3 GetQuote integration
	if err := s.repo.SetShipping(ctx, session.ID, session.Status, method, feeMinor); err != nil {
		span.RecordError(err)
		return nil, err
	}
	session.Status = domain.StatusShippingSet
	session.ShippingMethod = method
	session.ShippingFeeMinor = feeMinor
	session.TotalMinor = session.SubtotalMinor + feeMinor + session.TaxMinor - session.DiscountMinor
	s.touch(ctx, session)
	return session, nil
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

// touch is the reset-on-activity half of the expiry contract: after any
// successful mutation the DB deadline moves to now+TTL (best-effort — the
// mutation already succeeded; a missed bump only shortens the session, and
// the abandonment workflow's own timer is reset by the activity signal).
func (s *CheckoutService) touch(ctx context.Context, session *domain.Session) {
	session.ExpiresAt = s.now().Add(s.ttl)
	_ = s.repo.Touch(ctx, session.ID, session.ExpiresAt)
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
		span.RecordError(err)
		return err
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
	return true
}
