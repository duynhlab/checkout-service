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
)

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
