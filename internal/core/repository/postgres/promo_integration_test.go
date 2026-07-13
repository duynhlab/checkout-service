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
