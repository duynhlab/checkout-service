# checkout-service

The checkout counter: it snapshots a cart into a short-lived session, walks the
shopper through address, shipping and payment, re-checks the price and the stock
at the last moment, and hands the result to order-service.

## Responsibilities

- **Owns:** the checkout session — funnel state, the price snapshot, the
  address, shipping and payment-token selections, tax rules, promo codes and
  their redemptions, and the confirm idempotency ledger.
- **Does not own:** orders — `order-service` is the only writer of those — cart
  contents, catalog prices, stock, shipping rates, or payment authorisation. It
  reads all of those from their owners and stores a snapshot, never a copy it
  maintains.

## Tech

| Area | Technology |
|------|------------|
| Runtime | Go 1.26 |
| Transports | HTTP (private) · gRPC **client only** — no gRPC server |
| Workflows | Temporal — orchestrator of the abandoned-checkout timer |
| Data | PostgreSQL |
| Platform libraries | `authmw`, `dbx`, `grpcx`, `httpx`, `idempotency`, `logger/zapx`, `migratex`, `obsx`, `proto`, `temporalx` |

It dials five services: cart, product, inventory, shipping and order.

## API

- **Canonical contract:** [`homelab/docs/api/checkout.md`](https://github.com/duynhlab/homelab/blob/main/docs/api/checkout.md)
- **Shared conventions:** [`homelab/docs/api/api.md`](https://github.com/duynhlab/homelab/blob/main/docs/api/api.md)
- **Surfaces:** JWT-protected HTTP for the shopper's own session. HTTP `:8080`
  also carries `/health` and `/ready`.

The funnel's states, its routes, its payloads and every error code live in the
contract, so there is one place to change when they change.

## Run locally

Prefer the homelab **local-stack** — a session touches five services and a
Temporal namespace, so it is not meaningful in isolation.

One binary, three modes:

```bash
go run cmd/main.go migrate   # apply schema migrations
go run cmd/main.go           # serve HTTP :8080
go run cmd/main.go worker    # run the Temporal worker instead
```

There is no `seed` subcommand — checkout has no demo data.

## Verify

The commands CI runs, so a green local run means a green pipeline:

```bash
go build ./...
go test -race ./...
go test -tags=integration ./internal/core/repository/...   # needs Docker (testcontainers)
golangci-lint run
```

## Docs

- [Canonical contract](https://github.com/duynhlab/homelab/blob/main/docs/api/checkout.md)
- [local-stack guide](https://github.com/duynhlab/homelab/blob/main/local-stack/README.md)

## License

MIT
