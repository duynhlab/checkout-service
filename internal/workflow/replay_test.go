package workflow

import (
	"os"
	"path/filepath"
	"testing"

	"go.temporal.io/sdk/worker"
)

// TestReplayRecordedHistories replays every recorded AbandonedCheckoutWorkflow
// history against the current workflow code — the determinism gate that
// order-service has carried since ADR-030 and that ADR-064 makes a
// requirement for every versioned worker. A failure here means the change is
// history-incompatible: an in-flight abandon chain (30-minute timer,
// ContinueAsNew up to 500 resets) started under the recorded code would break
// at the next worker deploy. Fix the change — never delete a history to make
// this pass.
//
// gen1 is this worker's first corpus, recorded from local-stack running the
// SDK 1.48.0 / temporalx v0.38.0 build (the ADR-063 bump this test shipped
// with). Three shapes cover the workflow's decision points:
//   - expired: the timer fires, ExpireIfDue runs, the run completes
//   - finalized: SignalFinalize wins the selector race, no activity runs
//   - reset_then_expired: SignalActivity cancels and re-arms the timer
//     IN-RUN (ContinueAsNew only happens at the 500-reset bound, which no
//     recorded history exercises — a corpus gap worth knowing, not fixing
//     with a 500-signal synthetic)
//
// Corpus quality was verified by mutation: renaming the activity invocation
// fails the expired/reset histories with a non-determinism error; a
// timer-duration change does NOT fail replay (the replayer compares command
// kinds, not durations) — so duration changes still need the compose gate.
//
// The skip below covers only the window between opening the corpus directory
// and recording it; CHECKOUT_RELEASE_GATE turns the skip into a failure so a
// tag cannot be cut inside that window (same mechanism as order's
// ORDER_RELEASE_GATE).
func TestReplayRecordedHistories(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "gen1", "history_*.json"))
	if err != nil {
		t.Fatalf("glob testdata/gen1: %v", err)
	}
	if len(files) == 0 {
		if os.Getenv("CHECKOUT_RELEASE_GATE") != "" {
			t.Fatal("gen-1 corpus missing and CHECKOUT_RELEASE_GATE is set — record testdata/gen1 before tagging (see testdata/README.md)")
		}
		t.Skip("gen-1 corpus not recorded yet — MUST exist before the worker build is tagged (see testdata/README.md)")
	}

	replayFiles(t, files)
}

func replayFiles(t *testing.T, files []string) {
	t.Helper()
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflow(AbandonedCheckoutWorkflow)
			if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, f); err != nil {
				t.Errorf("history %s does not replay against current workflow code: %v", f, err)
			}
		})
	}
}
