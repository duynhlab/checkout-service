package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/duynhlab/checkout-service/internal/core/domain"
)

// connectionExceptionClass is the SQLSTATE class Postgres uses for every
// connection exception. The whole class qualifies as unavailability, because
// each member says the connection failed, not that the statement was wrong.
const connectionExceptionClass = "08"

// unavailableCodes are the SQLSTATEs outside class 08 that mean this server
// cannot serve the request right now. Kept deliberately short: anything that
// could be OUR bug must keep answering 500, or the bug becomes invisible behind
// a "retry" the shopper follows forever.
var unavailableCodes = map[string]struct{}{
	"57P01": {}, // admin_shutdown — the primary was stopped (CNPG switchover)
	"57P02": {}, // crash_shutdown
	"57P03": {}, // cannot_connect_now — server still starting or recovering
	"25006": {}, // read_only_sql_transaction — reached a standby, not the primary
}

// classify tags an infrastructure failure with domain.ErrUnavailable, leaving
// every other error untouched. It runs as a deferred step on each exported
// repository method so no query site can forget it — a missed site would
// silently answer 500 during a failover, which is the bug this exists to fix.
//
// The original error stays in the chain: the HTTP body is deliberately opaque,
// so the wrapped cause is the only breadcrumb an operator gets.
func classify(err error) error {
	if err == nil || errors.Is(err, domain.ErrUnavailable) {
		return err
	}
	if !isUnavailable(err) {
		return err
	}
	return fmt.Errorf("%w: %w", domain.ErrUnavailable, err)
}

// isUnavailable answers "could this Postgres not serve us?" as opposed to "was
// this request wrong?".
func isUnavailable(err error) bool {
	// A server-side error means Postgres answered, so the SQLSTATE is the whole
	// verdict — do not fall through to the transport checks below, or a unique
	// violation raced with a deadline would read as an outage.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if strings.HasPrefix(pgErr.Code, connectionExceptionClass) {
			return true
		}
		_, ok := unavailableCodes[pgErr.Code]
		return ok
	}

	// Could not establish a connection at all — the plainest form of "this
	// Postgres is not there", and what the failover window actually looks like
	// from a client: the pooler's address stops accepting connections.
	//
	// This arm is not decorative. pgconn.SafeToRetry reports FALSE for a
	// ConnectError, and a refused dial fails long before the query deadline
	// below, so without it the single most obvious outage fell straight through
	// to a bare 500 — which is what the integration test caught after every
	// unit test had passed.
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}

	// The client went away. Billing that to our own availability would let a
	// closed browser tab burn the error budget.
	if errors.Is(err, context.Canceled) {
		return false
	}

	// No answer from Postgres at all. DeadlineExceeded is the most likely shape
	// of a failover here, not an edge case: this package caps every query at
	// queryTimeout, and a demoted primary hangs rather than refusing. It also
	// covers a saturated pool, which is unavailability by any useful definition.
	//
	// The tradeoff is deliberate: a pathologically slow query now reads as
	// unavailability too. Accepted, because the shopper-facing answer ("not
	// now") is the same either way, the wrapped cause and the span still name
	// the real failure, and the SLO is unaffected — its error ratio selects
	// 5xx, so 500 and 503 burn budget identically.
	//
	// SafeToRetry covers the failures pgx knows never reached the server, such
	// as a refused dial while opening a pool connection.
	return errors.Is(err, context.DeadlineExceeded) || pgconn.SafeToRetry(err)
}
