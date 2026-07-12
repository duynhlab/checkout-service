// Package workflow implements the AbandonedCheckoutWorkflow (RFC-0015 P2,
// homelab ADR-019): a durable per-session wake-up timer for checkout
// abandonment. The DB deadline (checkout_sessions.expires_at, bumped by Touch
// on every mutation) is the ONLY clock that decides expiry; this workflow just
// makes it timely. A fired timer runs the ExpireIfDue activity, whose SQL
// expires the row only when `expires_at <= now()` — if the deadline moved (a
// lost or racing activity signal, a TTL config change, an idempotent session
// reuse), the activity answers "not due + remaining" and the timer re-arms to
// the DB's own remaining time. Losing any signal can therefore delay expiry,
// never mis-expire (doubt-cycle c findings).
//
// SDK usage per official docs: signal channels + selector
// (docs.temporal.io/develop/go/message-passing — including the
// drain-before-return caveat), timers and cancellation
// (docs.temporal.io/develop/go/timers,
// pkg.go.dev/go.temporal.io/sdk/workflow#NewTimer — a cancelled timer future
// completes with CanceledError and still fires its selector callback).
package workflow

import (
	"context"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/duynhlab/checkout-service/internal/core/domain"
	logicv1 "github.com/duynhlab/checkout-service/internal/logic/v1"
)

// Signal and query names — the contract with the service-side notifier.
const (
	SignalActivity = "activity" // any successful session mutation (resets the timer)
	SignalFinalize = "finalize" // confirm or cancel (stops the workflow)
	QueryState     = "session_state"
)

// maxResets bounds a single run's history: past it the workflow
// Continue-As-News. Mutations are Kong-rate-limited and each needs a DB
// write, so 500 is far beyond any legitimate session's activity.
const maxResets = 500

// WorkflowID returns the deterministic per-session workflow id.
func WorkflowID(sessionID string) string { return "checkout-abandon-" + sessionID }

// Input parameterizes one abandonment watch. TTL is fixed per run for
// determinism; a config change reaches running sessions through the DB
// deadline (the activity re-arms from expires_at), not through this value.
type Input struct {
	SessionID string
	TTL       time.Duration
}

// State is the QueryState answer — advisory, for operators.
type State struct {
	ArmedUntil time.Time // when the CURRENT timer fires (workflow clock)
	Resets     int       // activity signals consumed this run
}

// SessionExpirer is the activity-side port (repo.ExpireDue).
type SessionExpirer interface {
	ExpireDue(ctx context.Context, id string, lockTakeover time.Duration) (domain.ExpireOutcome, time.Duration, error)
}

// Activities holds the worker-side dependencies. LockTakeover mirrors the
// serve path's IDEMPOTENCY_LOCK_TAKEOVER — the parked-confirm recovery in
// ExpireDue uses it as the provably-dead threshold.
type Activities struct {
	Sessions     SessionExpirer
	LockTakeover time.Duration
}

// ExpireResult crosses the activity boundary (must be serializable).
type ExpireResult struct {
	Outcome   domain.ExpireOutcome
	Remaining time.Duration // only for OutcomeNotDue: time until the DB deadline
}

// ExpireIfDue expires the session iff its DB deadline has elapsed. Idempotent
// and conditional (skips confirming/terminal rows); safe under unlimited
// retries.
func (a *Activities) ExpireIfDue(ctx context.Context, sessionID string) (ExpireResult, error) {
	outcome, remaining, err := a.Sessions.ExpireDue(ctx, sessionID, a.LockTakeover)
	if err != nil {
		return ExpireResult{}, err
	}
	if outcome == domain.OutcomeExpired {
		logicv1.RecordSessionExpired(ctx, string(domain.ExpiredByTimer))
	}
	return ExpireResult{Outcome: outcome, Remaining: remaining}, nil
}

// AbandonedCheckoutWorkflow watches one session until it is finalized,
// expired, or leaves the timer's jurisdiction (confirming/terminal — a later
// mutation resurrects the watch via the notifier's SignalWithStart).
func AbandonedCheckoutWorkflow(ctx workflow.Context, in Input) error {
	activityCh := workflow.GetSignalChannel(ctx, SignalActivity)
	finalizeCh := workflow.GetSignalChannel(ctx, SignalFinalize)

	resets := 0
	wait := in.TTL
	armedUntil := workflow.Now(ctx).Add(wait)
	if err := workflow.SetQueryHandler(ctx, QueryState, func() (State, error) {
		return State{ArmedUntil: armedUntil, Resets: resets}, nil
	}); err != nil {
		return err
	}

	actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    5 * time.Minute,
			// Unlimited attempts: a DB outage delays expiry; the lazy
			// backstop keeps correctness meanwhile (ADR-019).
		},
	})

	for {
		timerCtx, cancelTimer := workflow.WithCancel(ctx)
		timer := workflow.NewTimer(timerCtx, wait)
		armedUntil = workflow.Now(ctx).Add(wait)

		fired := ""
		sel := workflow.NewSelector(ctx)
		// Signals FIRST: on a tie (buffered signals + fired timer after a
		// worker outage) user activity must win over the timer — though with
		// the DB-authoritative activity even the timer branch is harmless.
		sel.AddReceive(finalizeCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
			fired = "finalize"
		})
		sel.AddReceive(activityCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
			fired = "activity"
		})
		sel.AddFuture(timer, func(f workflow.Future) {
			if f.Get(ctx, nil) == nil {
				fired = "timer"
			}
			// CanceledError ⇒ leave fired as set by a signal callback.
		})
		sel.Select(ctx)
		cancelTimer()

		// Workflow cancellation: the timer future resolves with
		// CanceledError and no signal fired — return instead of spinning on
		// a dead context (doubt-cycle c).
		if ctx.Err() != nil {
			return temporal.NewCanceledError("workflow cancelled")
		}

		switch fired {
		case "finalize":
			drain(activityCh, finalizeCh)
			return nil
		case "activity":
			resets++
			if resets >= maxResets {
				if finalized := drain(activityCh, finalizeCh); finalized {
					return nil // a buffered finalize must never be dropped by CAN
				}
				return workflow.NewContinueAsNewError(ctx, AbandonedCheckoutWorkflow, in)
			}
			wait = in.TTL
			continue
		case "timer":
			var res ExpireResult
			if err := workflow.ExecuteActivity(actCtx, "ExpireIfDue", in.SessionID).Get(ctx, &res); err != nil {
				return err // only on workflow-level death; retries are unlimited
			}
			if res.Outcome == domain.OutcomeNotDue {
				// The DB deadline moved (signal lost/raced): re-arm to it.
				wait = res.Remaining
				continue
			}
			// Expired now, or out of jurisdiction (terminal/confirming/gone):
			// this watch is done. A confirming session that later un-parks is
			// re-watched by the next mutation's SignalWithStart.
			drain(activityCh, finalizeCh)
			return nil
		default:
			// Selector woke without a real event (defensive): loop with the
			// same deadline.
			wait = armedUntil.Sub(workflow.Now(ctx))
			if wait <= 0 {
				wait = time.Second
			}
			continue
		}
	}
}

// drain empties both signal channels before a return path (the documented
// Temporal caveat) and reports whether a finalize was among them.
func drain(activityCh, finalizeCh workflow.ReceiveChannel) bool {
	finalized := false
	for activityCh.ReceiveAsync(nil) {
	}
	for finalizeCh.ReceiveAsync(nil) {
		finalized = true
	}
	return finalized
}
