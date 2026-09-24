package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/duynhlab/checkout-service/internal/core/domain"
	"github.com/duynhlab/pkg/idempotency"
	"github.com/duynhlab/pkg/logger/slogx"
)

func eventsIn(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		if line != "" && json.Unmarshal([]byte(line), &m) == nil && m["event"] != nil {
			out = append(out, m)
		}
	}
	return out
}

func eventCtx() (context.Context, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slogx.WithContext(context.Background(), slogx.New(slogx.Config{Stdout: buf})), buf
}

func TestConfirm_EmitsSessionConfirmed(t *testing.T) {
	ctx, buf := eventCtx()
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
	orders := &fakeOrders{orderID: "501", status: "pending"}
	if _, err := confirmSvc(repo, inStock(), idem, orders).Confirm(ctx, "7", "sess-1", "key-1"); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	ev := eventsIn(t, buf)
	if len(ev) != 1 || ev[0]["event"] != "checkout.session.confirmed" || ev[0]["checkout.session.id"] != "sess-1" || ev[0]["order.id"] != "501" {
		t.Errorf("events = %v", ev)
	}
}

func TestConfirm_EmitsSessionRequoted(t *testing.T) {
	cases := map[string]struct {
		prods  *fakeProducts
		reason string
	}{
		"price drift": {&fakeProducts{infos: []ProductInfo{{ProductID: "1", UnitPriceMinor: 3499, AvailableQty: 5}}}, "price_changed"},
		"shortage":    {&fakeProducts{infos: []ProductInfo{{ProductID: "1", UnitPriceMinor: 2999, AvailableQty: 1}}}, "stock_unavailable"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, buf := eventCtx()
			repo := &fakeRepo{byID: readySession()}
			idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}
			_, _ = confirmSvc(repo, tc.prods, idem, &fakeOrders{}).Confirm(ctx, "7", "sess-1", "key-1")
			ev := eventsIn(t, buf)
			if len(ev) != 1 || ev[0]["event"] != "checkout.session.requoted" || ev[0]["reason"] != tc.reason {
				t.Errorf("events = %v", ev)
			}
		})
	}
}

func TestEmitSessionExpired(t *testing.T) {
	ctx, buf := eventCtx()
	EmitSessionExpired(ctx, "sess-1", "timer")
	ev := eventsIn(t, buf)
	if len(ev) != 1 || ev[0]["event"] != "checkout.session.expired" || ev[0]["reason"] != "timer" {
		t.Errorf("events = %v", ev)
	}
}

// A Finish that fails after completion sends the same-key retry through the
// completed-recovery arm. The session is confirmed once, so the event is
// written once — by whichever call cached the answer.
func TestConfirm_SessionConfirmedOnceAcrossAFailedFinish(t *testing.T) {
	ctx, buf := eventCtx()
	repo := &fakeRepo{byID: readySession()}
	idem := &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true, finishFailNext: true}
	orders := &fakeOrders{orderID: "501", status: "pending"}
	svc := confirmSvc(repo, inStock(), idem, orders)
	if _, err := svc.Confirm(ctx, "7", "sess-1", "key-1"); err == nil {
		t.Fatal("want the Finish failure")
	}
	if n := len(eventsIn(t, buf)); n != 0 {
		t.Fatalf("events after the failed Finish = %d, want 0", n)
	}
	// The row as the database holds it after the first call.
	key := int64(11)
	repo.byID.Status = domain.StatusCompleted
	repo.byID.ConfirmKeyID = &key
	repo.byID.OrderID = "501"
	if _, err := svc.Confirm(ctx, "7", "sess-1", "key-1"); err != nil {
		t.Fatalf("recovery retry: %v", err)
	}
	ev := eventsIn(t, buf)
	if len(ev) != 1 || ev[0]["event"] != "checkout.session.confirmed" || ev[0]["order.id"] != "501" {
		t.Errorf("events = %v, want exactly one confirmed", ev)
	}
}

func TestConfirm_EmitsRequotedAvailabilityUnknown(t *testing.T) {
	ctx, buf := eventCtx()
	prods := inStock()
	prods.unknown = []string{"1"}
	_, _ = confirmSvc(&fakeRepo{byID: readySession()}, prods, &fakeIdem{record: &idempotency.Record{ID: 11}, proceed: true}, &fakeOrders{}).
		Confirm(ctx, "7", "sess-1", "key-1")
	ev := eventsIn(t, buf)
	if len(ev) != 1 || ev[0]["reason"] != "availability_unknown" {
		t.Errorf("events = %v", ev)
	}
}

// The lazy expiry event comes only from the call that flipped the row.
func TestLazyExpiry_EmitsOnlyWhenTheRowFlipped(t *testing.T) {
	for name, already := range map[string]bool{"flipped here": false, "flipped elsewhere first": true} {
		t.Run(name, func(t *testing.T) {
			ctx, buf := eventCtx()
			stale := liveSession(domain.StatusAddressSet)
			stale.ExpiresAt = time.Now().Add(-time.Minute)
			repo := &fakeRepo{byID: stale, alreadyExpired: already}
			if _, err := newSvc(repo, &fakeCart{}, &fakeProducts{}).SetShipping(ctx, "7", "sess-1", "standard"); !errors.Is(err, ErrSessionExpired) {
				t.Fatalf("err = %v, want ErrSessionExpired", err)
			}
			want := 1
			if already {
				want = 0
			}
			if n := len(eventsIn(t, buf)); n != want {
				t.Errorf("expired events = %d, want %d", n, want)
			}
		})
	}
}
