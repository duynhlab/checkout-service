package workflow

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"

	"github.com/duynhlab/checkout-service/internal/core/domain"
	"github.com/duynhlab/pkg/logger/slogx"
)

const testTTL = 30 * time.Minute

func newEnv(t *testing.T) (*testsuite.TestWorkflowEnvironment, *Activities) {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	acts := &Activities{}
	env.RegisterWorkflow(AbandonedCheckoutWorkflow)
	env.RegisterActivity(acts.ExpireIfDue)
	return env, acts
}

func TestAbandon_TimerExpiresWhenDue(t *testing.T) {
	env, acts := newEnv(t)
	env.OnActivity(acts.ExpireIfDue, mock.Anything, "sess-1").
		Return(ExpireResult{Outcome: domain.OutcomeExpired}, nil).Once()

	env.ExecuteWorkflow(AbandonedCheckoutWorkflow, Input{SessionID: "sess-1", TTL: testTTL})
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("workflow = completed %v err %v", env.IsWorkflowCompleted(), env.GetWorkflowError())
	}
	env.AssertExpectations(t)
}

func TestAbandon_ActivitySignalResetsTimer(t *testing.T) {
	env, acts := newEnv(t)
	var firedAt time.Time
	env.OnActivity(acts.ExpireIfDue, mock.Anything, "sess-1").
		Run(func(mock.Arguments) { firedAt = env.Now() }).
		Return(ExpireResult{Outcome: domain.OutcomeExpired}, nil).Once()

	start := env.Now()
	// One mutation just before the deadline: the timer must restart from a
	// full TTL, so expiry happens at ~(29m + 30m), not at 30m.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalActivity, nil)
	}, testTTL-time.Minute)

	env.ExecuteWorkflow(AbandonedCheckoutWorkflow, Input{SessionID: "sess-1", TTL: testTTL})
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("workflow err: %v", env.GetWorkflowError())
	}
	if got := firedAt.Sub(start); got < 2*testTTL-2*time.Minute {
		t.Errorf("expire fired after %v, want ~%v (reset-on-activity)", got, 2*testTTL-time.Minute)
	}
	env.AssertExpectations(t)
}

func TestAbandon_FinalizeStopsWorkflowWithoutExpiring(t *testing.T) {
	env, acts := newEnv(t)
	// No .OnActivity return registered on purpose: any call would fail the test.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalFinalize, nil)
	}, time.Minute)

	env.ExecuteWorkflow(AbandonedCheckoutWorkflow, Input{SessionID: "sess-1", TTL: testTTL})
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("workflow err: %v", env.GetWorkflowError())
	}
	env.AssertNotCalled(t, "ExpireIfDue", mock.Anything, mock.Anything)
	_ = acts
}

func TestAbandon_NotDueRearmsToDBDeadline(t *testing.T) {
	// The DB deadline moved without a signal (lost SignalWithStart, TTL
	// config change, idempotent reuse): the fired timer is a wake-up, the
	// activity answers "not due, 10m remaining", and the workflow re-arms.
	env, acts := newEnv(t)
	env.OnActivity(acts.ExpireIfDue, mock.Anything, "sess-1").
		Return(ExpireResult{Outcome: domain.OutcomeNotDue, Remaining: 10 * time.Minute}, nil).Once()
	env.OnActivity(acts.ExpireIfDue, mock.Anything, "sess-1").
		Return(ExpireResult{Outcome: domain.OutcomeExpired}, nil).Once()

	env.ExecuteWorkflow(AbandonedCheckoutWorkflow, Input{SessionID: "sess-1", TTL: testTTL})
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("workflow err: %v", env.GetWorkflowError())
	}
	env.AssertExpectations(t)
}

func TestAbandon_GoneSessionExitsQuietly(t *testing.T) {
	// Terminal / confirming / deleted rows are out of the timer's
	// jurisdiction: exit without error (a later mutation re-creates the
	// watch via SignalWithStart).
	env, acts := newEnv(t)
	env.OnActivity(acts.ExpireIfDue, mock.Anything, "sess-1").
		Return(ExpireResult{Outcome: domain.OutcomeGone}, nil).Once()

	env.ExecuteWorkflow(AbandonedCheckoutWorkflow, Input{SessionID: "sess-1", TTL: testTTL})
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("workflow err: %v", env.GetWorkflowError())
	}
	env.AssertExpectations(t)
}

func TestAbandon_QueryReportsArmedDeadline(t *testing.T) {
	env, acts := newEnv(t)
	env.OnActivity(acts.ExpireIfDue, mock.Anything, "sess-1").
		Return(ExpireResult{Outcome: domain.OutcomeExpired}, nil)

	env.RegisterDelayedCallback(func() {
		v, err := env.QueryWorkflow(QueryState)
		if err != nil {
			t.Errorf("query: %v", err)
			return
		}
		var st State
		if err := v.Get(&st); err != nil {
			t.Errorf("decode state: %v", err)
			return
		}
		if st.ArmedUntil.IsZero() {
			t.Error("ArmedUntil not tracked")
		}
	}, time.Minute)

	env.ExecuteWorkflow(AbandonedCheckoutWorkflow, Input{SessionID: "sess-1", TTL: testTTL})
	if !env.IsWorkflowCompleted() {
		t.Fatal("not completed")
	}
	env.AssertExpectations(t)
}

type fakeExpirer struct{ outcome domain.ExpireOutcome }

func (f fakeExpirer) ExpireDue(context.Context, string, time.Duration) (domain.ExpireOutcome, time.Duration, error) {
	return f.outcome, 0, nil
}

// The timer's expiry writes checkout.session.expired with reason timer, once:
// ExpireDue reports OutcomeExpired only for the call that flipped the row.
func TestExpireIfDue_EmitsTimerExpiry(t *testing.T) {
	for outcome, want := range map[domain.ExpireOutcome]int{
		domain.OutcomeExpired: 1, domain.OutcomeNotDue: 0, domain.OutcomeGone: 0,
	} {
		buf := &bytes.Buffer{}
		ctx := slogx.WithContext(context.Background(), slogx.New(slogx.Config{Stdout: buf}))
		acts := &Activities{Sessions: fakeExpirer{outcome: outcome}}
		if _, err := acts.ExpireIfDue(ctx, "sess-1"); err != nil {
			t.Fatalf("%s: %v", outcome, err)
		}
		out := buf.String()
		n := strings.Count(out, `"event":"checkout.session.expired"`)
		if n != want || (want == 1 && !strings.Contains(out, `"reason":"timer"`)) {
			t.Errorf("%s: events = %d, want %d: %s", outcome, n, want, out)
		}
	}
}
