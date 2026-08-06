package v1

import (
	"context"
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The package's instruments are created at init from the GLOBAL meter provider,
// which is a no-op until something installs one. OTel's global provider
// delegates, so installing a real provider in TestMain retro-wires the
// already-created instruments — that is what makes label assertions possible
// without restructuring production code to inject a meter.
//
// Worth the harness: checkout_availability_check_total{result} is what the
// CheckoutAvailabilityErrors and CheckoutAvailabilityUnknownSKU alerts select
// on, so a wrong label value is an alert that never fires. Nothing else in this
// package could catch that.
var testMetricReader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(testMetricReader)))
	os.Exit(m.Run())
}

// availabilityResultCount totals checkout.availability.check for one `result`
// label. The reader accumulates across the whole package run, so callers must
// assert a DELTA rather than an absolute value.
func availabilityResultCount(t *testing.T, result string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := testMetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if mt.Name != "checkout.availability.check" {
				continue
			}
			sum, ok := mt.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				if v, ok := dp.Attributes.Value("result"); ok && v.AsString() == result {
					total += dp.Value
				}
			}
		}
	}
	return total
}
