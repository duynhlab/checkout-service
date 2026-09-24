package v1

import (
	"context"
	"log/slog"

	"github.com/duynhlab/pkg/logger/slogx"
)

// Catalog events for the session decisions this service owns (RFC-0031 §
// Event catalog), each written next to the business counter for the same
// decision so the two agree on when it happened.

// emitSessionConfirmed writes checkout.session.confirmed: the session was
// handed to order and completed.
func emitSessionConfirmed(ctx context.Context, sessionID, orderID string) {
	slogx.FromContext(ctx).Event(ctx, slog.LevelInfo, "checkout.session.confirmed", "session confirmed",
		slog.String("checkout.session.id", sessionID), slog.String("order.id", orderID))
}

// emitSessionRequoted writes checkout.session.requoted; reason is
// price_changed, stock_unavailable or availability_unknown.
func emitSessionRequoted(ctx context.Context, sessionID, reason string) {
	slogx.FromContext(ctx).Event(ctx, slog.LevelInfo, "checkout.session.requoted", "session requoted",
		slog.String("checkout.session.id", sessionID), slog.String("reason", reason))
}

// EmitSessionExpired writes checkout.session.expired; reason is timer or
// lazy. Exported for the worker's expiry activity, like RecordSessionExpired.
func EmitSessionExpired(ctx context.Context, sessionID, reason string) {
	slogx.FromContext(ctx).Event(ctx, slog.LevelInfo, "checkout.session.expired", "session expired",
		slog.String("checkout.session.id", sessionID), slog.String("reason", reason))
}
