//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/duynhlab/checkout-service/internal/core/domain"
)

func TestRedeemPromo_IdempotentPerSession(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	if err := repo.RedeemPromo(ctx, "SAVE5", "7", "11111111-1111-1111-1111-111111111111"); err != nil {
		t.Fatalf("first redeem: %v", err)
	}
	// Crash re-drive: same session redeems again — success, no double count.
	if err := repo.RedeemPromo(ctx, "SAVE5", "7", "11111111-1111-1111-1111-111111111111"); err != nil {
		t.Fatalf("re-drive redeem: %v", err)
	}
	p, _ := repo.GetPromo(ctx, "SAVE5")
	if p.RedeemedCount != 1 {
		t.Fatalf("redeemed_count = %d, want exactly 1", p.RedeemedCount)
	}
}

func TestRedeemPromo_GlobalCapRace(t *testing.T) {
	// The RFC exit criterion: 50 concurrent redeemers, cap 5, exactly 5 win.
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	var wg sync.WaitGroup
	wins := make(chan struct{}, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sid := uuidN(n)
			if err := repo.RedeemPromo(ctx, "SCARCE", userN(n), sid); err == nil {
				wins <- struct{}{}
			} else if !errors.Is(err, domain.ErrPromoExhausted) {
				t.Errorf("goroutine %d: unexpected err %v", n, err)
			}
		}(i)
	}
	wg.Wait()
	close(wins)
	won := len(wins)
	p, _ := repo.GetPromo(ctx, "SCARCE")
	if won != 5 || p.RedeemedCount != 5 {
		t.Fatalf("wins=%d redeemed_count=%d, want exactly 5/5", won, p.RedeemedCount)
	}
}

func TestRedeemPromo_PerUserLimitRace(t *testing.T) {
	// The weak path (review finding): SAME user, distinct sessions, racing a
	// per-user limit of 1 — exactly one may win.
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	var wg sync.WaitGroup
	wins := make(chan struct{}, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if err := repo.RedeemPromo(ctx, "ONETIME", "7", uuidN(n)); err == nil {
				wins <- struct{}{}
			} else if !errors.Is(err, domain.ErrPromoExhausted) {
				t.Errorf("goroutine %d: unexpected err %v", n, err)
			}
		}(i)
	}
	wg.Wait()
	close(wins)
	if won := len(wins); won != 1 {
		t.Fatalf("same-user wins = %d, want exactly 1", won)
	}
}

func TestRedeemPromo_ExpiredAndUnknown(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	if err := repo.RedeemPromo(ctx, "EXPIRED1", "7", uuidN(1)); !errors.Is(err, domain.ErrPromoExpired) {
		t.Fatalf("expired err = %v", err)
	}
	if err := repo.RedeemPromo(ctx, "NOPE", "7", uuidN(2)); !errors.Is(err, domain.ErrPromoNotFound) {
		t.Fatalf("unknown err = %v", err)
	}
}

func uuidN(n int) string {
	return fmt.Sprintf("11111111-1111-1111-1111-%012d", n)
}

func userN(n int) string { return fmt.Sprintf("user-%d", n) }

// Covers the promo methods the RedeemPromo suites do not touch — their
// conditional-write semantics against the real SQL, and (since the
// unavailability classifier added a deferred wrapper to every method) their
// happy paths, so the wrapper is exercised where it runs.

func TestCountUserRedemptions_CountsOnlyThatUser(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	if err := repo.RedeemPromo(ctx, "SAVE5", "7", "11111111-1111-1111-1111-111111111111"); err != nil {
		t.Fatalf("seed redeem: %v", err)
	}
	n, err := repo.CountUserRedemptions(ctx, "SAVE5", "7")
	if err != nil || n != 1 {
		t.Fatalf("count(user 7) = %d, %v — want 1", n, err)
	}
	other, err := repo.CountUserRedemptions(ctx, "SAVE5", "8")
	if err != nil || other != 0 {
		t.Fatalf("count(user 8) = %d, %v — want 0 (must not leak across users)", other, err)
	}
}

func TestSetPromo_RecomposesTotalUnderStatusGuard(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	s := newSession("promo-user")
	if err := repo.Create(ctx, s); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := repo.SetPromo(ctx, s.ID, domain.StatusOpen, "SAVE5", 500); err != nil {
		t.Fatalf("set promo: %v", err)
	}
	got, err := repo.FindByID(ctx, s.ID)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got.PromoCode != "SAVE5" || got.DiscountMinor != 500 ||
		got.TotalMinor != got.SubtotalMinor+got.ShippingFeeMinor+got.TaxMinor-500 {
		t.Fatalf("promo=%q discount=%d total=%d — SQL must recompose the total",
			got.PromoCode, got.DiscountMinor, got.TotalMinor)
	}
	// The guard: a stale `from` status must refuse the write.
	if err := repo.SetPromo(ctx, s.ID, domain.StatusReady, "SAVE5", 500); !errors.Is(err, domain.ErrStaleTransition) {
		t.Fatalf("stale status guard: got %v, want ErrStaleTransition", err)
	}
}

func TestStripPromo_OnlyUnderTheClaimBinding(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()

	s := newSession("strip-user")
	if err := repo.Create(ctx, s); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := repo.SetPromo(ctx, s.ID, domain.StatusOpen, "SAVE5", 500); err != nil {
		t.Fatalf("set promo: %v", err)
	}
	// Bind the confirm claim the strip must run under.
	if err := repo.UpdateStatus(ctx, s.ID, domain.StatusOpen, domain.StatusReady); err != nil {
		t.Fatalf("to ready: %v", err)
	}
	if err := repo.BeginConfirm(ctx, s.ID, 42); err != nil {
		t.Fatalf("begin confirm: %v", err)
	}

	// A foreign key id must NOT strip (409-class, not silent).
	if err := repo.StripPromo(ctx, s.ID, 99); !errors.Is(err, domain.ErrStaleTransition) {
		t.Fatalf("foreign key strip: got %v, want ErrStaleTransition", err)
	}
	// The bound key strips, clears the claim, re-parks at shipping_set.
	if err := repo.StripPromo(ctx, s.ID, 42); err != nil {
		t.Fatalf("strip: %v", err)
	}
	got, err := repo.FindByID(ctx, s.ID)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got.PromoCode != "" || got.DiscountMinor != 0 ||
		got.Status != domain.StatusShippingSet || got.ConfirmKeyID != nil {
		t.Fatalf("after strip: promo=%q discount=%d status=%s key=%v — want cleared shipping_set",
			got.PromoCode, got.DiscountMinor, got.Status, got.ConfirmKeyID)
	}
}

func TestBackfillRedemptionOrder_SetsOrderOnceNeverOverwrites(t *testing.T) {
	repo := NewSessionRepository(newTestDB(t))
	ctx := context.Background()
	const sid = "22222222-2222-2222-2222-222222222222"

	if err := repo.RedeemPromo(ctx, "SAVE5", "7", sid); err != nil {
		t.Fatalf("seed redeem: %v", err)
	}
	if err := repo.BackfillRedemptionOrder(ctx, "SAVE5", sid, "order-1"); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	// Second backfill must be a no-op: order_id IS NULL guards the write.
	if err := repo.BackfillRedemptionOrder(ctx, "SAVE5", sid, "order-2"); err != nil {
		t.Fatalf("re-backfill errored: %v", err)
	}
	var oid string
	if err := repo.db.QueryRow(ctx,
		`SELECT order_id FROM promo_redemptions WHERE code = 'SAVE5' AND session_id = $1`, sid,
	).Scan(&oid); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if oid != "order-1" {
		t.Fatalf("order_id = %q, want the FIRST backfill to stick", oid)
	}
}
