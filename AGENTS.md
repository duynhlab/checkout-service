# AGENTS.md

Source of truth for AI agents working in `checkout-service`. Read before any
task. Platform docs live in `duynhlab/homelab` (start at `docs/README.md`
there); this service is specified by **RFC-0015**.

## Contribution workflow for AI agents

- **Never push to `main`.** Branch → PR → squash-merge. Prefixes: `feat/`
  `fix/` `chore/` `docs/` `refactor/` `ci/`.
- Conventional commits; subject ≤50 chars, imperative, no trailing period;
  body wrapped at 72. **No attribution trailers** (no Co-authored-by etc.).
- Verify identity before committing: `git config user.email` must be the
  duynhlab personal identity; `gh` CLI on the `duynhne` account.
- Before a PR: `go build ./... && go test -race ./... && golangci-lint run`
  plus the integration run (below) and the local-stack e2e audit in
  `homelab/local-stack/README.md` when the change affects the request path.

## Project overview

checkout-service is the **session/UX orchestrator** between the SPA and
order-service (RFC-0015): it owns the ephemeral, 30-minute-TTL
**checkout session** driven by an explicit FSM
(`open → address_set → shipping_set → ready → confirming → completed`;
`cancelled`/`expired` terminal), snapshots the cart at session creation, and
re-validates **price and stock against product-service** — cart is the
item-list authority, product is the price authority at checkout time.
Confirm hands off to order-service (P2); order remains the only orders-writer
and saga-starter.

- **Client-only service**: no gRPC server. It dials cart (`cart.v1/GetCart`)
  and product (`product.v1/GetProducts`) over `pkg/grpcx`.
- Routes are Variant A collection-noun paths (naming convention v3.0.1,
  ADR-017): `/checkout/v1/private/checkout/sessions[…]` — checkout, like
  auth, is a process-named service, so its owning segment is the literal
  **`checkout`** with resources (`sessions`) nested beneath it. All routes
  private (Kong edge-JWT +
  authoritative in-service `pkg/authmw`), owner-scoped by JWT `user_id`.
- **Lazy-expiry backstop**: every read/mutation treats `now > expires_at` as
  expired regardless of the (P2) Temporal timer — correctness never depends
  on a worker being alive.

## Repository layout

```
checkout-service/
├── cmd/main.go                    # wiring: config, obsx, DB, gRPC clients, JWT verifier, routes; `migrate` subcommand
├── config/config.go               # env-based config + Validate(); CheckoutConfig (TTL, gRPC targets)
├── db/migrations/sql/             # golang-migrate *.up.sql (sessions + items; partial unique active-per-user index)
├── internal/
│   ├── web/v1/handler.go          # session handlers — validation, httpx error translation
│   ├── logic/v1/                  # FSM transition table, snapshot + re-validation, lazy expiry (NO SQL)
│   ├── clients/                   # gRPC stub adapters → logic ports (cart, product)
│   └── core/
│       ├── database.go            # pgxpool (simple protocol — pooler-safe)
│       ├── domain/                # Session aggregate, statuses, errors, SessionRepository port
│       └── repository/postgres/   # raw SQL; conditional (optimistic) transitions
├── middleware/                    # tracing (obsx-driven) + zap logging
└── Dockerfile
```

## Build, test, lint

```bash
GOTOOLCHAIN=auto go build ./...
GOTOOLCHAIN=auto go test -race ./...                                   # unit (no Docker)
GOTOOLCHAIN=auto go test -tags=integration ./internal/core/repository/...  # real Postgres via testcontainers
GOTOOLCHAIN=auto golangci-lint run --timeout=10m
```

- Unit tests: stdlib only, hand-written fakes, table-driven subtests.
- The repository layer is integration-tested (build tag `integration`), so
  `core/repository` is excluded from the Sonar unit-coverage gate; everything
  else must keep **new-code coverage ≥ 80%**.

## Conventions & gotchas

- **3-layer, one-way**: web → logic → core. gRPC *client* adapters
  (`internal/clients`) satisfy logic-layer ports — logic never imports
  generated stubs.
- The FSM lives in ONE place (`internal/logic/v1/fsm.go`); handlers never
  compare statuses themselves. Rejected moves are `INVALID_TRANSITION` (409).
- Money is **int64 minor units** end-to-end; floats never cross a boundary
  (conversion happens in cart/product servers, not here).
- Anti-IDOR: a foreign session answers **404**, indistinguishable from a
  missing one; expiry answers **410 SESSION_EXPIRED** (it existed).
- All observability through `pkg/obsx` (RFC-0014, OTLP push): middleware
  chain **tracing → logging → metrics**; no Prometheus scrape config here.
- Migrations are forward-only `*.up.sql` (pkg/migratex); the init container
  reuses this image with `args: ["migrate"]`. No `seed` subcommand — no demo
  data in P1.
- Kyverno admission (cluster): pinned image
  `ghcr.io/duynhlab/checkout-service/checkout-service:<tag>`, probes +
  resources required; PSS restricted.
- Mermaid only for diagrams — never ASCII art.
