package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

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
