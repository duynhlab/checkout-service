package workflow

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
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
		if err != nil && !errors.As(err, &notFound) {
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
			}, AbandonedCheckoutWorkflow, Input{SessionID: sessionID, TTL: n.ttl})
		if err != nil {
			n.logger.Warn("abandonment signal-with-start failed (lazy expiry still covers this session)",
				zap.String("session_id", sessionID), zap.Error(err))
		}
	}()
}
