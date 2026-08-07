package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/duynhlab/checkout-service/internal/core/domain"
)

// GetPromo loads a code for the apply-time advisory checks. Missing codes
// answer domain.ErrPromoNotFound.
func (r *SessionRepository) GetPromo(ctx context.Context, code string) (_ *domain.Promo, err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	var p domain.Promo
	err = r.db.QueryRow(ctx, `
		SELECT code, kind, value, expires_at, max_redemptions, redeemed_count, per_user_limit
		FROM promo_codes WHERE code = $1`, code).
		Scan(&p.Code, &p.Kind, &p.Value, &p.ExpiresAt, &p.MaxRedemptions, &p.RedeemedCount, &p.PerUserLimit)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrPromoNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get promo: %w", err)
	}
	return &p, nil
}

// CountUserRedemptions returns how many times a user has redeemed a code.
func (r *SessionRepository) CountUserRedemptions(ctx context.Context, code, userID string) (_ int, err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	var n int
	if err := r.db.QueryRow(ctx, `
		SELECT count(*) FROM promo_redemptions WHERE code = $1 AND user_id = $2`,
		code, userID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count redemptions: %w", err)
	}
	return n, nil
}

// SetPromo attaches (or replaces) the code and its computed discount on the
// session, recomposing the total in SQL — the same conditional-write style as
// every other money mutation. from guards the status; the caller has already
// validated the code.
func (r *SessionRepository) SetPromo(ctx context.Context, id string, from domain.SessionStatus, code string, discountMinor int64) (err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	tag, err := r.db.Exec(ctx, `
		UPDATE checkout_sessions
		SET promo_code = NULLIF($3, ''), discount_minor = $4,
		    total_minor = subtotal_minor + shipping_fee_minor + tax_minor - $4,
		    updated_at = now()
		WHERE id = $1 AND status = $2`, id, from, code, discountMinor)
	if err != nil {
		return fmt.Errorf("set promo: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrStaleTransition
	}
	return nil
}

// StripPromo removes an applied code from a CONFIRMING session under its
// claim binding (the PROMO_EXHAUSTED/EXPIRED-at-confirm path — the sibling of
// the requote write) and recomposes the total.
func (r *SessionRepository) StripPromo(ctx context.Context, id string, keyID int64) (err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	// shipping_set, not ready: a lost 409 response followed by a same-key
	// retry must land on INVALID_TRANSITION (the user re-runs the funnel and
	// SEES the stripped totals) — never on a silent full-price confirm.
	tag, err := r.db.Exec(ctx, `
		UPDATE checkout_sessions
		SET promo_code = NULL, discount_minor = 0, confirm_key_id = NULL,
		    status = 'shipping_set',
		    total_minor = subtotal_minor + shipping_fee_minor + tax_minor,
		    updated_at = now()
		WHERE id = $1 AND status = 'confirming' AND confirm_key_id = $2`, id, keyID)
	if err != nil {
		return fmt.Errorf("strip promo: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrStaleTransition
	}
	return nil
}

// RedeemPromo counts one redemption, atomically and exactly once per session
// (ADR-022). The FOR UPDATE on the code row serializes every redemption of a
// code, which makes the per-user count phantom-free; UNIQUE(code, session_id)
// + ON CONFLICT makes crash-re-driven confirms idempotent.
func (r *SessionRepository) RedeemPromo(ctx context.Context, code, userID, sessionID string) (err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("redeem begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock the code row: from here every check below is serialized per code.
	var expiresAt *time.Time
	var maxRedemptions, perUserLimit *int
	var redeemed int
	err = tx.QueryRow(ctx, `
		SELECT expires_at, max_redemptions, redeemed_count, per_user_limit
		FROM promo_codes WHERE code = $1 FOR UPDATE`, code).
		Scan(&expiresAt, &maxRedemptions, &redeemed, &perUserLimit)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrPromoNotFound
	}
	if err != nil {
		return fmt.Errorf("redeem lock: %w", err)
	}

	// Idempotency anchor FIRST (review finding): a crash-re-driven confirm
	// whose redemption already committed must short-circuit to success BEFORE
	// any expiry/cap evaluation — time flipping the expiry between attempts
	// must never reject a redemption that already happened.
	tag, err := tx.Exec(ctx, `
		INSERT INTO promo_redemptions (code, user_id, session_id)
		VALUES ($1, $2, $3) ON CONFLICT (code, session_id) DO NOTHING`,
		code, userID, sessionID)
	if err != nil {
		return fmt.Errorf("redeem insert: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx) // already counted for this session
	}

	if expiresAt != nil && expiresAt.Before(time.Now()) {
		return domain.ErrPromoExpired // rollback via defer
	}

	if maxRedemptions != nil && redeemed >= *maxRedemptions {
		return domain.ErrPromoExhausted // rollback via defer
	}
	if perUserLimit != nil {
		var mine int
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM promo_redemptions WHERE code = $1 AND user_id = $2`,
			code, userID).Scan(&mine); err != nil {
			return fmt.Errorf("redeem user count: %w", err)
		}
		// mine includes the row just inserted in THIS tx.
		if mine > *perUserLimit {
			return domain.ErrPromoExhausted
		}
	}

	if _, err := tx.Exec(ctx, `
		UPDATE promo_codes SET redeemed_count = redeemed_count + 1 WHERE code = $1`, code); err != nil {
		return fmt.Errorf("redeem increment: %w", err)
	}
	return tx.Commit(ctx)
}

// BackfillRedemptionOrder records the order an existing redemption produced —
// best-effort provenance so ops can tell a used redemption from a burned one.
func (r *SessionRepository) BackfillRedemptionOrder(ctx context.Context, code, sessionID, orderID string) (err error) {
	defer func() { err = classify(err) }()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	_, err = r.db.Exec(ctx, `
		UPDATE promo_redemptions SET order_id = $3
		WHERE code = $1 AND session_id = $2 AND order_id IS NULL`, code, sessionID, orderID)
	if err != nil {
		return fmt.Errorf("backfill redemption order: %w", err)
	}
	return nil
}
