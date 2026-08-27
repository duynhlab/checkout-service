package workflow

import (
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// driveToResetCap feeds maxResets activity signals one virtual minute apart —
// each lands inside the TTL, so the timer never fires and the run reaches the
// rotation branch with no ExpireIfDue call.
func driveToResetCap(env *testsuite.TestWorkflowEnvironment, andThen func()) {
	sent := 0
	var send func()
	send = func() {
		sent++
		env.SignalWorkflow(SignalActivity, nil)
		if sent < maxResets {
			env.RegisterDelayedCallback(send, time.Minute)
			return
		}
		if andThen != nil {
			andThen()
		}
	}
	env.RegisterDelayedCallback(send, time.Minute)
}

// TestAbandon_ContinueAsNewAtResetCap pins the history bound: the maxResets-th
// activity signal rotates the run via Continue-As-New instead of growing
// history forever.
func TestAbandon_ContinueAsNewAtResetCap(t *testing.T) {
	env, _ := newEnv(t)
	driveToResetCap(env, nil)

	env.ExecuteWorkflow(AbandonedCheckoutWorkflow, Input{SessionID: "sess-1", TTL: testTTL})
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	var can *workflow.ContinueAsNewError
	if err := env.GetWorkflowError(); !errors.As(err, &can) {
		t.Fatalf("expected ContinueAsNewError at the reset cap, got %v", err)
	}
}

// TestRotateAtCap_BufferedFinalizeCompletes pins the drain rule: a finalize
// already buffered when the rotation runs must complete the run — a rotation
// that dropped it would leave a finalized session watched forever. Driven
// through a wrapper workflow because the main loop's selector always consumes
// a ready finalize before the cap-hitting activity signal (finalize is added
// first), so the buffered-at-rotation interleaving cannot be produced by
// signals alone.
func TestRotateAtCap_BufferedFinalizeCompletes(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	wrapper := func(ctx workflow.Context) error {
		activityCh := workflow.GetSignalChannel(ctx, SignalActivity)
		finalizeCh := workflow.GetSignalChannel(ctx, SignalFinalize)
		// Let the delayed finalize land and buffer before rotating.
		if err := workflow.Sleep(ctx, time.Minute); err != nil {
			return err
		}
		return rotateAtCap(ctx, activityCh, finalizeCh, Input{SessionID: "sess-1", TTL: testTTL})
	}
	env.RegisterWorkflow(wrapper)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalFinalize, nil)
	}, 30*time.Second)

	env.ExecuteWorkflow(wrapper)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("expected clean completion (buffered finalize wins), got %v", err)
	}
}

func TestClampRearm(t *testing.T) {
	for _, tc := range []struct {
		in, want time.Duration
	}{
		{5 * time.Minute, 5 * time.Minute},
		{0, time.Second},
		{-time.Minute, time.Second},
	} {
		if got := clampRearm(tc.in); got != tc.want {
			t.Errorf("clampRearm(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
