# AGENTS.md

Source of truth for AI agents working in `checkout-service`. Read before any
task.

## Authority and scope

This repository implements the service. It does **not** define the contract.

- **Canonical contract:** [`homelab/docs/api/checkout.md`](https://github.com/duynhlab/homelab/blob/main/docs/api/checkout.md)
- **Shared API rules:** [`homelab/docs/api/api.md`](https://github.com/duynhlab/homelab/blob/main/docs/api/api.md)

Implement against those files. When this repository and the contract disagree,
**stop and classify the mismatch** using
[Resolving a mismatch](https://github.com/duynhlab/homelab/blob/main/docs/api/README.md#resolving-a-mismatch)
before changing either side. One class — an implementation that violates the
intended contract — **blocks the release tag**. This service decides what a
shopper is charged, so that rule is not paperwork.

No route, payload, error-code or funnel-state inventory belongs in this file.
Manifests, gateway routing, NetworkPolicy, database topology and platform
observability belong to [duynhlab/homelab](https://github.com/duynhlab/homelab).

## Contribution workflow for AI agents

- **Never push to `main`.** Branch → PR → squash-merge. Prefixes: `feat/`
  `fix/` `chore/` `docs/` `refactor/` `ci/`.
- Conventional commits; subject ≤50 chars, imperative, no trailing period;
  body wrapped at 72. **No attribution trailers** (no Co-authored-by etc.).
- Verify identity before committing: `git config user.email` must be the
  duynhlab personal identity; `gh` CLI on the `duynhne` account.
- Run the local-stack e2e audit in homelab when the change affects the request
  path.

## Build, test, lint

These are the commands CI runs, so a green local run means a green pipeline.

```bash
go build ./...
go vet ./...
go test -race ./...
go test -tags=integration ./internal/core/repository/...   # needs Docker (testcontainers)
golangci-lint run
```

Local development against an unreleased `pkg`: `pkg` is one module per package,
so its root has no `go.mod` and a single `replace github.com/duynhlab/pkg` can no
longer resolve. Use one commented `replace` line per module — the trailer in
`go.mod` shows the shape, and
[`docs/api/pkg.md`](https://github.com/duynhlab/homelab/blob/main/docs/api/pkg.md)
explains why.

## Architecture boundaries

**3-layer, dependencies flow one way only: transport → logic → core** — plus a
workflow package that is neither.

- **Transport** — `internal/web/v1/` (HTTP). There is **no gRPC server**; this
  service is a gRPC **client** of five others, and those adapters live in
  `internal/clients/`.
- **Logic** — `internal/logic/v1/` holds the funnel FSM, the confirm path, promo
  rules and metrics.
- **Core** — `internal/core/` owns the session domain and the Postgres
  repositories.
- **Workflow** — `internal/workflow/` holds the abandoned-checkout timer.
  Workflow code has different rules from everything else; see the determinism
  invariant below.

One binary, three modes: serve (no argument), `worker`, `migrate`. There is no
`seed` — checkout has no demo data.

## Invariants

Rules an implementer can violate at the keyboard. Most of them decide whether a
shopper is charged the right amount, or can escape a stuck session at all.

- **The FSM is single-source and forward-only, and unknown states have no
  edges.** Edits re-enter the same or an earlier state, never a forward jump; a
  price change at confirm drops back for a requote; terminal states have no
  outgoing edges. Fail closed on anything unrecognised.
- **Lazy expiry is the correctness backstop; the timer is only the janitor.** A
  session past its deadline is treated as expired on *every* read and mutation,
  regardless of what the workflow has done. The predicate's answer never depends
  on a write succeeding, which is why an infrastructure blip cannot resurrect a
  dead session.
- **The confirm deadline must stay below a quarter of the idempotency-key
  takeover window,** and startup exits non-zero if it is not. A takeover is a
  *proof* the previous owner is dead; shrinking that margin makes two live
  executions under one key possible. A configuration gate has to exit non-zero
  so orchestrators see it.
- **The attempt sentinel means a requote can never coexist with an existing
  order.** Once the marker is set, revalidation is skipped forever after —
  requoting past a possible order would lie about it.
- **A price change or a stock shortage requotes and does *not* burn the
  idempotency key.** Failure only delays the retry; the session drops back a
  state so the shopper has somewhere to go.
- **Fail closed on inventory: a timeout is never a shortage.** Transport errors
  propagate as upstream failures, never as out-of-stock. The dependency is
  required at construction rather than optional, so a missing one fails loudly at
  wiring time instead of degrading into a read that cannot be right.
- **Definite versus degraded is decided on whether the upstream answered, not on
  how many rows came back.** A one-line basket whose only SKU went unsellable
  once produced an empty result that was reported as retryable — an instruction
  the shopper could never satisfy, on a session with no way out. A definite
  answer must requote, not retry.
- **An unknown SKU escapes the confirming state instead of parking in it.** The
  transport branch is right for a hung upstream and catastrophic for a condition
  that lasts until an operator seeds a missing balance row: the shopper would be
  unable to confirm, unable to cancel and unable to start again. The requote uses
  snapshot values so no line is falsely flagged.
- **Promo lock contention must surface as a visible 500, not a fake 503.** The
  statement-scoped lock timeout sits below the query deadline on purpose;
  without it, waiters die at the query deadline and the classifier reads that as
  the datastore being unavailable, so a hot promo code manufactures fake failover
  alerts. Transaction-scoped is also the only form safe through a transaction
  pooler.
- **Promo redemption is atomic, exactly once per session, and never counted at
  apply time.** Apply is a preview; the authoritative gate is at confirm.
  Rejection is shaped like a price change so a same-key retry lands on an invalid
  transition, never a silent full-price order.
- **The abandoned-checkout workflow treats the database deadline as the only
  clock, and a workflow-code change is a determinism hazard.** The TTL is fixed
  per run so a configuration change reaches running sessions only through the
  deadline; signals are drained before every return; losing a signal can delay
  expiry, never mis-expire. Editing the command sequence breaks replay of
  in-flight histories.
- **Money is integer minor units end to end.** Dollars exist only on the
  browser-facing wire. Rounding is floor division in the shopper's favour, with
  an overflow guard, and the discount is clamped.
- **A foreign session is indistinguishable from a missing one** — both answer
  not-found. Expired answers gone.
- **Card-shaped payment input is rejected before persistence**, counting total
  digits rather than the longest run, so a token-shaped string cannot smuggle a
  PAN into the session table or the order handoff.
- **Only a short list of database error codes becomes a 503; anything that could
  be our bug stays a 500.** The classifier runs from a deferred call on every
  exported repository method so no query site can forget it, and a cancelled
  context is explicitly *not* ours — a closed browser tab must not burn the error
  budget.
- **One active session per user is enforced by a partial unique index**, not by
  application checks.
- **Pooler-safe database settings live in `pkg/dbx`.** One DSN serves the app and
  migrations so both connect identically.
- **Graceful-shutdown ordering is load-bearing:** readiness 503 → drain delay →
  HTTP shutdown → pool close → OTel last.

## Repository map

- `cmd/main.go` — wiring, the three modes, graceful shutdown
- `config/config.go` — env config and validation, including the startup gates above
- `internal/clients/` — the cart, product, inventory, shipping and order adapters
- `internal/web/v1/` — HTTP handlers and the browser-facing response shapes
- `internal/logic/v1/` — the FSM, the confirm path, promo rules, metrics, errors
- `internal/core/domain/` — session and payment-token models, sentinel errors
- `internal/core/repository/postgres/` — session, promo and idempotency repositories, and the error classifier
- `internal/workflow/` — the abandoned-checkout workflow and its helpers
- `db/migrations/` — forward-only golang-migrate SQL, embedded
- `middleware/` — tracing and logging only

## Gotchas

- Kyverno admission rejects a workload image tagged `:latest` or unpinned. The
  published image is `ghcr.io/duynhlab/checkout-service/checkout-service:<tag>` —
  the repository path repeats, and the tag carries no `v` prefix. The worker is
  the same image under the `worker` argument.
- Metrics leave over OTLP. There is no `/metrics` endpoint and nothing scrapes
  this service.
- The availability metric has **four** outcomes, not three: the unknown-SKU case
  is its own value precisely because it is neither a clean result nor a transport
  error.
- Product is asked for prices through the batch price RPC. It is not a catalog
  listing call, and it is not cached on product's side — that is deliberate.

## API change synchronization

An API change is not done when the code compiles.

- The contract in homelab and this repository move **together** — same change,
  and either the same PR pair or an immediate follow-up.
- Behaviour that is designed but not deployed is marked **`Planned`** in the
  contract; it is never described as current.
- A material mismatch between the contract and this implementation **blocks the
  release tag** until it is reconciled or explicitly accepted.
