package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/duynhlab/checkout-service/config"
	"github.com/duynhlab/pkg/logger/slogx"
)

// dialEastWest sizes its connection slice from the target list rather than from
// a literal, and main destructures the first five entries positionally. That
// pairing is the invariant: it used to be a [5] array beside a five-entry
// literal with nothing tying them together, so the first sixth east-west
// dependency would have compiled and then panicked on conns[5] at startup. The
// list has already grown once, for inventory in RFC-0021 phase 4.
//
// grpcx.Dial wraps grpc.NewClient, which resolves lazily — no target has to be
// reachable for this to run.
func TestDialEastWest_SizesConnsFromTargetList(t *testing.T) {
	cfg := &config.Config{Checkout: config.CheckoutConfig{
		CartGRPCAddr:      "dns:///cart:9090",
		ProductGRPCAddr:   "dns:///product:9090",
		OrderGRPCAddr:     "dns:///order:9090",
		ShippingGRPCAddr:  "dns:///shipping:9090",
		InventoryGRPCAddr: "dns:///inventory:9090",
	}}

	conns, cleanup, ok := dialEastWest(cfg, slogx.New(slogx.Config{Stdout: io.Discard}))
	if !ok {
		t.Fatal("dialEastWest reported failure on lazy dials")
	}
	defer cleanup()

	// main reads conns[0..4]; anything shorter is an out-of-range panic at
	// startup, which is exactly what the [5] array made possible.
	if len(conns) < 5 {
		t.Fatalf("len(conns) = %d, want at least the 5 main destructures", len(conns))
	}
	// Every slot filled: a nil here means the loop and the slice length
	// disagree, the same class of bug from the other direction.
	for i, c := range conns {
		if c == nil {
			t.Errorf("conns[%d] is nil — the dial loop did not fill the whole slice", i)
		}
	}
}

// healthPayload is the probe body for the API server. The worker process serves
// the same `status` field from a raw mux, so the shape is pinned here to keep
// the two from drifting.
func TestHealthPayload_ShapeIsStatusOnly(t *testing.T) {
	got := healthPayload("shutting_down")
	if len(got) != 1 {
		t.Fatalf("payload = %v, want exactly one field", got)
	}
	if got["status"] != "shutting_down" {
		t.Errorf(`payload["status"] = %v, want "shutting_down"`, got["status"])
	}
}

type stubPool struct{ err error }

func (p stubPool) Ping(context.Context) error { return p.err }

// The probe contract Kubernetes and compose depend on: /health is liveness and
// must not consult the database, /ready reports draining before it reports a
// database problem, and every answer carries the same one-field body. These are
// the states an operator reads during a rollout, so each one is pinned.
//
// It also pins the middleware chain the ADR-038 migration rewrote. Both probes
// are mounted UNDER httpmw.Tracing and httpmw.Logging, so a chain that panics —
// a nil logger reaching Logging, a Tracing that mis-handles an empty service
// name — fails every case here rather than only in a live stack.
func TestProbes_StatesAndBodies(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cases := []struct {
		name     string
		path     string
		draining bool
		pingErr  error
		wantCode int
		wantBody string
	}{
		{"health is liveness only", "/health", false, errors.New("db down"), http.StatusOK, `{"status":"ok"}`},
		{"health stays up while draining", "/health", true, nil, http.StatusOK, `{"status":"ok"}`},
		{"ready reports draining first", "/ready", true, errors.New("db down"), http.StatusServiceUnavailable, `{"status":"shutting_down"}`},
		{"ready reports a dead pool", "/ready", false, errors.New("db down"), http.StatusServiceUnavailable, `{"status":"db_unavailable"}`},
		{"ready ok when healthy", "/ready", false, nil, http.StatusOK, `{"status":"ok"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var draining atomic.Bool
			draining.Store(tc.draining)
			srv := setupServer(&config.Config{}, "checkout", slogx.New(slogx.Config{Stdout: io.Discard}), nil, nil,
				stubPool{err: tc.pingErr}, &draining)

			rec := httptest.NewRecorder()
			srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))

			if rec.Code != tc.wantCode {
				t.Errorf("code = %d, want %d", rec.Code, tc.wantCode)
			}
			if got := strings.TrimSpace(rec.Body.String()); got != tc.wantBody {
				t.Errorf("body = %s, want %s", got, tc.wantBody)
			}
		})
	}
}

// The probes must stay OUT of the trace and the access log. httpmw's
// DefaultSkipRoutes carries /health and /ready, and matching is exact on the
// Gin route pattern — so this asserts the pattern the router actually
// registered, which is what the skip list is matched against.
func TestProbeRoutesAreRegisteredUnderTheSkippedPatterns(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var draining atomic.Bool
	srv := setupServer(&config.Config{}, "checkout", slogx.New(slogx.Config{Stdout: io.Discard}), nil, nil, stubPool{}, &draining)

	engine, ok := srv.Handler.(*gin.Engine)
	if !ok {
		t.Fatalf("handler is %T, want *gin.Engine", srv.Handler)
	}
	registered := map[string]bool{}
	for _, r := range engine.Routes() {
		registered[r.Path] = true
	}
	for _, p := range []string{"/health", "/ready"} {
		if !registered[p] {
			t.Errorf("%s is not registered as an exact route — httpmw's skip list would not match it", p)
		}
	}
}

// shutdownTelemetry must hand the exporter a live context bounded by its own
// timeout, independent of whatever the HTTP drain consumed (homelab#1359).
func TestShutdownTelemetry_FreshBoundedContext(t *testing.T) {
	t.Run("live context with own deadline", func(t *testing.T) {
		const timeout = 5 * time.Second
		var (
			called      bool
			entryErr    error
			hasDeadline bool
			remaining   time.Duration
		)
		// Inspect inside the fake: the helper cancels its context on return.
		err := shutdownTelemetry(func(ctx context.Context) error {
			called = true
			entryErr = ctx.Err()
			var deadline time.Time
			deadline, hasDeadline = ctx.Deadline()
			remaining = time.Until(deadline)
			return nil
		}, timeout)
		if err != nil {
			t.Fatalf("shutdownTelemetry returned %v, want nil", err)
		}
		if !called {
			t.Fatal("shutdown func was not called")
		}
		if entryErr != nil {
			t.Fatalf("context already done on entry: %v", entryErr)
		}
		if !hasDeadline {
			t.Fatal("context has no deadline")
		}
		if remaining <= 0 || remaining > timeout {
			t.Fatalf("deadline %v from now, want within (0, %v]", remaining, timeout)
		}
	})

	t.Run("exporter error is returned", func(t *testing.T) {
		want := errors.New("exporter down")
		err := shutdownTelemetry(func(context.Context) error { return want }, time.Second)
		if !errors.Is(err, want) {
			t.Fatalf("got %v, want %v", err, want)
		}
	})
}
