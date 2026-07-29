package v1

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/duynhlab/pkg/obsx"
)

// Business metrics for the P2 confirm/abandonment slice, answering the
// on-call questions defined with the feature (observability-with-feature):
//  1. How many confirms succeed vs bounce on PRICE_CHANGED?      → confirmed / price-changed counters
//  2. Are sessions expiring via the timer or the lazy backstop?  → expired{reason}: lazy-majority ⇒ the worker is down
//  3. Is the confirm hop (product re-validate + order gRPC) slow? → confirm duration histogram
//
// Instruments ride the global OTel MeterProvider that obsx.SetupObservability
// installs (RFC-0014 OTLP pipeline → collector → VictoriaMetrics). Names are
// OTel-style; the collector's prometheusremotewrite exporter renders them as
// checkout_sessions_confirmed_total, checkout_price_changed_total,
// checkout_sessions_expired_total{reason}, checkout_confirm_duration_seconds*.
// Before obsx setup the global provider is a no-op, so init here is safe.
var (
	meter = otel.Meter("checkout")

	confirmedCounter, _ = meter.Int64Counter("checkout.sessions.confirmed",
		metric.WithDescription("Checkout sessions successfully confirmed into an order"))
	priceChangedCounter, _ = meter.Int64Counter("checkout.price.changed",
		metric.WithDescription("Confirms bounced with PRICE_CHANGED or STOCK_UNAVAILABLE (session requoted)"))
	expiredCounter, _ = meter.Int64Counter("checkout.sessions.expired",
		metric.WithDescription("Sessions marked expired, by who noticed (timer = abandonment workflow, lazy = read-path backstop)"))
	confirmDuration, _ = meter.Float64Histogram("checkout.confirm.duration",
		metric.WithDescription("End-to-end confirm handler duration"), metric.WithUnit("s"),
		// obsx installs SLO-tuned Views only for its named HTTP instruments;
		// this business histogram needs its own second-scale boundaries, else
		// the SDK's ms-scale default (0,5,…,10000) collapses every sub-5s
		// confirm into bucket 0.
		metric.WithExplicitBucketBoundaries(obsx.DurationBuckets...))
	promoRedeemedCounter, _ = meter.Int64Counter("checkout.promo.redeemed",
		metric.WithDescription("Promo redemptions counted at confirm (P4)"))
	promoRejectedCounter, _ = meter.Int64Counter("checkout.promo.rejected",
		metric.WithDescription("Promo rejections at the authoritative confirm gate, by reason"))
	// RFC-0021 P2-4: one increment per availability shadow-compare (create or
	// confirm). result ∈ {ok,missing,unknown,error,skipped} — a STRUCTURAL check
	// (does inventory know every SKU & answer sanely), not an exact-qty compare,
	// since inventory is unwritten until phase 3. Bounded label only — no
	// sku/user/order ids. Renders as inventory_shadow_compare_total{result}.
	shadowCompareCounter, _ = meter.Int64Counter("inventory.shadow.compare",
		metric.WithDescription("Checkout availability shadow-compares vs inventory-service (RFC-0021 P2-4), by result"))
	// RFC-0021 P3: which authority an availability read was ROUTED to. One
	// increment per resolveCatalog call, so the canary's REAL exposure is
	// measurable instead of inferred from the flag — a dial set to 50 that is
	// bucketing badly, or a source flip that silently kept every read on Product,
	// both show up here and nowhere else. Bounded to two values.
	//
	// READ-weighted, while the dial is USER-weighted: a funnel emits two reads
	// (create + the confirm revalidate) and the confirm read is skipped on
	// idempotent re-entry, so the ratio here approximates the dial only when both
	// cohorts have similar reads-per-user. Good enough to spot "the flip did
	// nothing" or "exposure is far off the dial"; not a measurement of the share of
	// USERS.
	//
	// Counted at the ROUTING decision, before the upstream answers, and that is
	// deliberate: the question this metric exists for is "how much traffic is
	// exposed to inventory", which is true whether or not inventory then succeeded.
	// A read routed to inventory that times out still counts here — its failure is
	// the RED alerts' and ErrUpstream's business, not this counter's. Hence
	// "routed", not "answered".
	availabilityPathCounter, _ = meter.Int64Counter("checkout.availability.path",
		metric.WithDescription("Availability reads by the authority they were routed to (RFC-0021 P3 canary), by path"))
)

// Availability read authorities — bounded metric labels.
const (
	availabilityPathProduct   = "product"
	availabilityPathInventory = "inventory"
)

// recordAvailabilityPath counts one availability read against the authority it was
// ROUTED to. Not "served": the increment happens at the routing decision, before
// the upstream answers, so this counter measures EXPOSURE and cannot be divided by
// a success count to get an answer rate.
func recordAvailabilityPath(ctx context.Context, path string) {
	availabilityPathCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("path", path)))
}

// recordShadowCompare counts one shadow-compare outcome with its bounded result.
func recordShadowCompare(ctx context.Context, result string) {
	shadowCompareCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}

// recordPromoRejected counts a confirm-gate rejection with its bounded reason.
func recordPromoRejected(ctx context.Context, err error) {
	reason := "exhausted"
	if errors.Is(err, ErrPromoExpired) {
		reason = "expired"
	}
	promoRejectedCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// RecordSessionExpired counts an expiry with its bounded reason label
// ("timer" | "lazy"). Exported for the worker's MarkSessionExpired activity.
func RecordSessionExpired(ctx context.Context, reason string) {
	expiredCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}
