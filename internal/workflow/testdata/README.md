# AbandonedCheckoutWorkflow replay corpus

Recorded event histories replayed by `../replay_test.go` on every test run —
the determinism gate for this worker (ADR-064; pattern from
order-service `internal/saga/testdata`).

## Recording a history (local-stack)

1. Bring up local-stack on the code you are about to tag and drive the shape
   you need (start a checkout session; then let the timer expire, confirm the
   session to finalize, or touch it — e.g. PUT address — for an in-run timer
   reset). For fast expiry, recreate the checkout API with
   `SESSION_TTL_SECONDS=45s` (a Go DURATION — a bare "45" silently falls back
   to the 1800s default).
2. Export the history as JSON:

   ```bash
   docker exec local-stack-temporal-admin-tools-1 \
     temporal workflow show \
       --workflow-id "abandoned-checkout:<session-id>" \
       --namespace mop --output json > history_<shape>.json
   ```

   (adjust the container name to the stack's; `tctl` works too on older CLIs)
3. Drop the file into the current generation directory as
   `history_<shape>.json` and run
   `go test ./internal/workflow/ -run TestReplayRecordedHistories`.

## Generations

A new generation directory is opened only when pinned versioning makes old
histories unclaimable by the new build (see order's testdata/README for the
full reasoning). `gen1` = SDK 1.48.0 / temporalx v0.38.0 (ADR-063 bump),
recorded 2026-08-27. `CHECKOUT_RELEASE_GATE=1 go test ...` turns the
missing-corpus skip into a failure — the tag gate.
