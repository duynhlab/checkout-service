package v1

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/duynhlab/checkout-service/internal/core/domain"
	"github.com/duynhlab/pkg/obsx"
)

// Promo errors surfaced to the web layer (404/409 PROMO_*).
var (
	ErrPromoInvalid   = errors.New("promo code not found")
	ErrPromoExpired   = errors.New("promo code expired")
	ErrPromoExhausted = errors.New("promo code exhausted")
)

// discountFor computes the discount for a promo against the CURRENT totals
// components, clamped so the composed total can never go negative. Percent
// reuses flatTax's checked multiply (value ≤ 100 ⇒ bps ≤ 10000 by schema
// CHECK), which carries the overflow guard (review finding).
func discountFor(p *domain.Promo, subtotal, fee, tax int64) (int64, error) {
	var d int64
	switch p.Kind {
	case "percent":
		if p.Value < 1 || p.Value > 100 {
			// Schema CHECK enforces this; fail closed if it ever drifts.
			return 0, ErrPromoInvalid
		}
		var err error
		if d, err = flatTax(subtotal, int32(p.Value)*100); err != nil { //nolint:gosec // bounded 1..100 above
			return 0, err
		}
	default: // "fixed" by schema CHECK
		d = p.Value
	}
	if ceiling := subtotal + fee + tax; d > ceiling {
		d = ceiling
	}
	return d, nil
}

// sessionDiscount recomputes the applied promo's discount for new totals
// components — every totals-changing write goes through this so a percent
// code stays a percentage and a fixed code stays clamped (review finding:
// a frozen apply-time discount drifts and can push totals negative).
func (s *CheckoutService) sessionDiscount(ctx context.Context, session *domain.Session, subtotal, fee, tax int64) (int64, error) {
	if session.PromoCode == "" {
		return 0, nil
	}
	p, err := s.repo.GetPromo(ctx, session.PromoCode)
	if err != nil {
		if errors.Is(err, domain.ErrPromoNotFound) {
			return 0, nil // code retired mid-session: drop the discount
		}
		return 0, err
	}
	return discountFor(p, subtotal, fee, tax)
}

// ApplyPromo attaches a code to the session (open..ready). Validation here is
// the user-facing preview — existence, expiry, remaining capacity — while the
// authoritative gate stays the atomic redemption at confirm. Applying NEVER
// counts a use.
func (s *CheckoutService) ApplyPromo(ctx context.Context, userID, id, code string) (*domain.Session, error) {
	ctx, span := obsx.StartSpan(ctx, tracerScope, "checkout.session.apply_promo", trace.WithAttributes(
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
	// Any pre-confirm working state may hold a promo; the FSM status itself
	// does not move (promo is an attribute, not a step).
	switch session.Status { //nolint:exhaustive // confirming/terminal states have no promo entry
	case domain.StatusOpen, domain.StatusAddressSet, domain.StatusShippingSet, domain.StatusReady:
	default:
		return nil, ErrInvalidTransition
	}

	p, err := s.repo.GetPromo(ctx, code)
	if err != nil {
		if errors.Is(err, domain.ErrPromoNotFound) {
			return nil, ErrPromoInvalid
		}
		span.RecordError(err)
		return nil, err
	}
	if p.ExpiresAt != nil && p.ExpiresAt.Before(s.now()) {
		return nil, ErrPromoExpired
	}
	if p.MaxRedemptions != nil && p.RedeemedCount >= *p.MaxRedemptions {
		return nil, ErrPromoExhausted
	}
	if p.PerUserLimit != nil {
		mine, cerr := s.repo.CountUserRedemptions(ctx, code, userID)
		if cerr != nil {
			span.RecordError(cerr)
			return nil, cerr
		}
		if mine >= *p.PerUserLimit {
			return nil, ErrPromoExhausted
		}
	}

	discount, err := discountFor(p, session.SubtotalMinor, session.ShippingFeeMinor, session.TaxMinor)
	if err != nil {
		return nil, err
	}
	if err := s.repo.SetPromo(ctx, session.ID, session.Status, code, discount); err != nil {
		span.RecordError(err)
		return nil, err
	}
	session.PromoCode = code
	session.DiscountMinor = discount
	session.TotalMinor = session.SubtotalMinor + session.ShippingFeeMinor + session.TaxMinor - discount
	s.touch(ctx, session)
	return session, nil
}

// RemovePromo detaches the applied code (no use was ever counted). Gives a
// user whose code no longer fits the totals an exit that is not "wait for
// the TTL" (review finding).
func (s *CheckoutService) RemovePromo(ctx context.Context, userID, id string) (*domain.Session, error) {
	ctx, span := obsx.StartSpan(ctx, tracerScope, "checkout.session.remove_promo", trace.WithAttributes(
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
	switch session.Status { //nolint:exhaustive // confirming/terminal states have no promo entry
	case domain.StatusOpen, domain.StatusAddressSet, domain.StatusShippingSet, domain.StatusReady:
	default:
		return nil, ErrInvalidTransition
	}
	if err := s.repo.SetPromo(ctx, session.ID, session.Status, "", 0); err != nil {
		span.RecordError(err)
		return nil, err
	}
	session.PromoCode = ""
	session.DiscountMinor = 0
	session.TotalMinor = session.SubtotalMinor + session.ShippingFeeMinor + session.TaxMinor
	s.touch(ctx, session)
	return session, nil
}
