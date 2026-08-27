package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"
)

// notifyTimeout bounds each best-effort signal send, detached from the
// caller's request context so a client disconnect cannot cancel it.
const notifyTimeout = 2 * time.Second

// Notifier sends the session-lifecycle signals to the abandonment workflow.
// Every method is best-effort: failures are logged and swallowed — a Temporal
// outage degrades expiry to lazy-only (ADR-019), never the user request.
type Notifier struct {
	temporal  Signaler
	taskQueue string
	ttl       time.Duration
	logger    *zap.Logger
}

// NewNotifier wires the sender side. temporal must be non-nil — pass a Lazy
// when the startup dial lost the bring-up race; its signals return
// ErrTemporalUnavailable (logged, swallowed) until the background redial
// connects, and the lazy expires_at backstop covers those sessions.
func NewNotifier(temporal Signaler, taskQueue string, ttl time.Duration, logger *zap.Logger) *Notifier {
	return &Notifier{temporal: temporal, taskQueue: taskQueue, ttl: ttl, logger: logger}
}

// SessionStarted ensures the watch exists and resets it (Signal-With-Start:
// starts the workflow when absent, signals it when running — idempotent).
func (n *Notifier) SessionStarted(ctx context.Context, sessionID string) {
	n.signalWithStart(ctx, sessionID)
}

// SessionActivity resets the watch after a successful mutation. Also
// Signal-With-Start: covers a worker/Temporal outage at create time and a
// completed prior run (post-finalize re-activity restarts the watch; a
// resurrected watch on a terminal row exits harmlessly via ExpireIfDue).
func (n *Notifier) SessionActivity(ctx context.Context, sessionID string) {
	n.signalWithStart(ctx, sessionID)
}

// SessionFinalized stops the watch after confirm or cancel. Plain signal —
// NotFound just means there is nothing to stop.
func (n *Notifier) SessionFinalized(ctx context.Context, sessionID string) {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), notifyTimeout)
	go func() {
		defer cancel()
		err := n.temporal.SignalWorkflow(sctx, WorkflowID(sessionID), "", SignalFinalize, nil)
		var notFound *serviceerror.NotFound
		switch {
		case err == nil, errors.As(err, &notFound):
		case errors.Is(err, ErrTemporalUnavailable):
			// The redial loop already warns about the outage itself — one
			// Debug per signal instead of a Warn per mutation.
			n.logger.Debug("abandonment finalize skipped: Temporal not connected yet",
				zap.String("session_id", sessionID))
		default:
			n.logger.Warn("abandonment finalize signal failed (lazy expiry still covers this session)",
				zap.String("session_id", sessionID), zap.Error(err))
		}
	}()
}

// signalWithStart runs fire-and-forget: a degraded Temporal must not add its
// timeout to user-facing mutation latency (review finding) — the signal is
// best-effort by contract either way.
func (n *Notifier) signalWithStart(ctx context.Context, sessionID string) {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), notifyTimeout)
	go func() {
		defer cancel()
		_, err := n.temporal.SignalWithStartWorkflow(sctx, WorkflowID(sessionID), SignalActivity, nil,
			client.StartWorkflowOptions{
				ID:        WorkflowID(sessionID),
				TaskQueue: n.taskQueue,
				// Fixed one-liner in the Temporal UI/CLI execution list, so an
				// operator reads the watch's job without opening the payload.
				// Only the FIRST SignalWithStart creates the execution, so the
				// TTL shown is the one the run actually armed with.
				StaticSummary: fmt.Sprintf("abandoned-checkout watch: session %s, TTL %s", sessionID, n.ttl),
				// Business-key lookup: `SessionId = '<sid>'` in UI/CLI list
				// filters. Registered on the namespace by temporal-bootstrap
				// (local-stack) / the temporal-search-attributes Job (cluster)
				// — a start referencing an unregistered attribute is rejected,
				// which here would surface as the Warn below on every mutation.
				TypedSearchAttributes: temporal.NewSearchAttributes(
					temporal.NewSearchAttributeKeyKeyword("SessionId").ValueSet(sessionID)),
			}, AbandonedCheckoutWorkflow, Input{SessionID: sessionID, TTL: n.ttl})
		switch {
		case err == nil:
		case errors.Is(err, ErrTemporalUnavailable):
			// See SessionFinalized: outage noise belongs to the redial loop,
			// not to every mutation.
			n.logger.Debug("abandonment signal skipped: Temporal not connected yet",
				zap.String("session_id", sessionID))
		default:
			n.logger.Warn("abandonment signal-with-start failed (lazy expiry still covers this session)",
				zap.String("session_id", sessionID), zap.Error(err))
		}
	}()
}
