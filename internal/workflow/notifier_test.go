package workflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"
)

// fakeSignaler captures the calls the Notifier fires and unblocks the test
// when each fire-and-forget goroutine lands.
type fakeSignaler struct {
	mu        sync.Mutex
	started   []client.StartWorkflowOptions
	signalled []string
	err       error
	done      chan struct{}
}

func (f *fakeSignaler) SignalWithStartWorkflow(_ context.Context, _, _ string, _ any,
	options client.StartWorkflowOptions, _ any, _ ...any) (client.WorkflowRun, error) {
	f.mu.Lock()
	f.started = append(f.started, options)
	f.mu.Unlock()
	f.done <- struct{}{}
	return nil, f.err
}

func (f *fakeSignaler) SignalWorkflow(_ context.Context, workflowID, _, signalName string, _ any) error {
	f.mu.Lock()
	f.signalled = append(f.signalled, workflowID+"/"+signalName)
	f.mu.Unlock()
	f.done <- struct{}{}
	return f.err
}

func (f *fakeSignaler) wait(t *testing.T) {
	t.Helper()
	select {
	case <-f.done:
	case <-time.After(5 * time.Second):
		t.Fatal("notifier goroutine never called the signaler")
	}
}

// TestSignalWithStart_StartOptionsContract pins the operator-facing start
// options: the SessionId Keyword search attribute (must match what
// temporal-bootstrap / the cluster Job registered) and the StaticSummary
// one-liner. A drift here is invisible to the workflow tests — the options
// live on the client call, not in workflow code.
func TestSignalWithStart_StartOptionsContract(t *testing.T) {
	f := &fakeSignaler{done: make(chan struct{}, 1)}
	n := NewNotifier(f, "checkout", 30*time.Minute, zap.NewNop())

	n.SessionStarted(context.Background(), "sid-123")
	f.wait(t)

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.started) != 1 {
		t.Fatalf("expected 1 SignalWithStart, got %d", len(f.started))
	}
	opts := f.started[0]
	if opts.ID != WorkflowID("sid-123") || opts.TaskQueue != "checkout" {
		t.Errorf("ID/TaskQueue = %q/%q", opts.ID, opts.TaskQueue)
	}
	for _, want := range []string{"sid-123", "30m0s"} {
		if !strings.Contains(opts.StaticSummary, want) {
			t.Errorf("StaticSummary %q does not contain %q", opts.StaticSummary, want)
		}
	}
	got, ok := opts.TypedSearchAttributes.GetKeyword(temporal.NewSearchAttributeKeyKeyword("SessionId"))
	if !ok || got != "sid-123" {
		t.Errorf("SessionId search attribute = %q (present=%v), want %q", got, ok, "sid-123")
	}
	// The value must survive the payload round-trip the server performs — a
	// non-JSON-encodable value would be rejected at start time.
	if _, err := converter.GetDefaultDataConverter().ToPayload(got); err != nil {
		t.Errorf("SessionId value not encodable: %v", err)
	}
}

// TestNotifier_SwallowsFailures pins the best-effort contract on every path:
// a degraded Temporal must never fail the caller.
func TestNotifier_SwallowsFailures(t *testing.T) {
	for name, err := range map[string]error{
		"unavailable": ErrTemporalUnavailable,
		"other":       errors.New("boom"),
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeSignaler{done: make(chan struct{}, 1), err: err}
			n := NewNotifier(f, "checkout", time.Minute, zap.NewNop())

			n.SessionActivity(context.Background(), "sid-a")
			f.wait(t)
			n.SessionFinalized(context.Background(), "sid-a")
			f.wait(t)

			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.started) != 1 || len(f.signalled) != 1 {
				t.Fatalf("started=%d signalled=%d, want 1/1", len(f.started), len(f.signalled))
			}
			if want := WorkflowID("sid-a") + "/" + SignalFinalize; f.signalled[0] != want {
				t.Errorf("finalize signal = %q, want %q", f.signalled[0], want)
			}
		})
	}
}
