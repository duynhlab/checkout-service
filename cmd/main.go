// checkout-service — RFC-0015: the session/UX orchestrator between the SPA
// and order-service. Client-only on gRPC (no server): it dials cart
// (item-list authority), product (price authority), inventory (availability
// authority, RFC-0021 phase 4), shipping (fee authority) and order (the P2
// confirm handoff). Subcommands: `migrate` applies the embedded schema
// migrations; `worker` runs the Temporal worker for the
// AbandonedCheckoutWorkflow (ADR-019). No `seed` — checkout has no demo data.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"

	"github.com/duynhlab/pkg/authmw"
	"github.com/duynhlab/pkg/grpcx"
	"github.com/duynhlab/pkg/httpmw"
	"github.com/duynhlab/pkg/idempotency"
	"github.com/duynhlab/pkg/logger/slogx"
	"github.com/duynhlab/pkg/migratex"
	"github.com/duynhlab/pkg/obsx"
	"github.com/duynhlab/pkg/temporalx"

	"github.com/duynhlab/checkout-service/config"
	migrations "github.com/duynhlab/checkout-service/db/migrations"
	"github.com/duynhlab/checkout-service/internal/clients"
	database "github.com/duynhlab/checkout-service/internal/core"
	"github.com/duynhlab/checkout-service/internal/core/repository/postgres"
	logicv1 "github.com/duynhlab/checkout-service/internal/logic/v1"
	webv1 "github.com/duynhlab/checkout-service/internal/web/v1"
	checkoutwf "github.com/duynhlab/checkout-service/internal/workflow"
)

func main() {
	ctx := context.Background()
	cfg := config.Load()

	logger := slogx.New(slogx.Config{Level: os.Getenv("LOG_LEVEL")})
	slogx.SetDefault(logger)

	// Subcommand `migrate` runs the embedded SQL set and exits. `worker` is
	// handled below, after observability and the DB pool exist.
	if len(os.Args) > 1 && runSubcommand(os.Args[1], cfg, logger) {
		return
	}

	if err := cfg.Validate(); err != nil {
		panic("Configuration validation failed: " + err.Error())
	}

	logger.Info(ctx, "Service starting",
		slog.String("service.version", cfg.Service.Version),
		slog.String("deployment.environment.name", cfg.Service.Env),
		slog.String("port", cfg.Service.Port),
	)

	pool, err := database.Connect(context.Background(), cfg)
	if err != nil {
		logger.Error(ctx, "Failed to connect to database", slogx.Err(err))
		return
	}
	defer pool.Close()
	logger.Info(ctx, "Database connection pool established")

	// RFC-0014: single OTel wiring point — traces, OTLP metrics, logs.
	tp, logger := initObservability(logger)

	if cfg.Profiling.Enabled {
		stopProfiling, err := obsx.SetupProfiling()
		if err != nil {
			logger.Warn(ctx, "Failed to initialize profiling", slogx.Err(err))
		} else {
			logger.Info(ctx, "Profiling initialized", slog.String("endpoint", cfg.Profiling.Endpoint))
			defer func() {
				if err := stopProfiling(context.Background()); err != nil {
					logger.Error(ctx, "Profiling shutdown error", slogx.Err(err))
				}
			}()
		}
	} else {
		logger.Info(ctx, "Profiling disabled (PROFILING_ENABLED=false)")
	}

	// `<binary> worker` runs the Temporal worker for the abandonment
	// workflow and serves no HTTP beyond health probes.
	if maybeRunWorker(cfg, logger, pool) {
		if tp != nil {
			_ = tp.Shutdown(context.Background()) // flush the worker's final metrics/spans
		}
		return
	}

	// East-west gRPC clients (lazy dial — grpcx.Dial uses grpc.NewClient, so
	// an unreachable target fails per-call, not at startup).
	conns, cleanup, ok := dialEastWest(cfg, logger)
	if !ok {
		return
	}
	defer cleanup()
	// Positional, matching dialEastWest's target order. The slice is sized from
	// that target list, so adding a sixth target grows conns too: this read stays
	// in bounds, and the new connection is simply unused until wired up here.
	cartConn, productConn, orderConn, shippingConn, inventoryConn := conns[0], conns[1], conns[2], conns[3], conns[4]

	// Deadline-fencing invariant (RFC-0015 P2 confirm): a lock takeover must
	// PROVE the previous owner is dead, which holds only when the takeover
	// window dwarfs the longest possible confirm execution.
	if cfg.Checkout.IdempotencyLockTakeover <= 4*logicv1.MaxConfirmWrite {
		// Fatal: a config gate must exit non-zero so orchestrators see it.
		logger.Fatal(ctx, "IDEMPOTENCY_LOCK_TAKEOVER must exceed 4× the longest confirm write",
			slog.Duration("takeover", cfg.Checkout.IdempotencyLockTakeover),
			slog.Duration("max_confirm_write", logicv1.MaxConfirmWrite))
	}

	repo := postgres.NewSessionRepository(pool)
	// The catalog read is split by authority: product answers price,
	// inventory answers availability (RFC-0021). Both are constructor
	// arguments because neither is optional.
	svc := logicv1.NewCheckoutService(repo,
		clients.NewCartClient(cartConn),
		clients.NewProductClient(productConn),
		clients.NewInventoryClient(inventoryConn),
		cfg.Checkout.SessionTTL,
	).WithConfirm(
		// WrapIdem routes the store's errors through the same unavailability
		// classifier as the session repository — the idempotency table lives
		// on the same pool, and its Claim is the first write of every confirm.
		postgres.WrapIdem(idempotency.New(pool, cfg.Checkout.IdempotencyLockTakeover)),
		clients.NewOrderClient(orderConn),
	).WithQuoter(clients.NewShippingClient(shippingConn))

	// Abandonment notifier (ADR-019): best-effort signals to the durable
	// timer. Temporal being unreachable is NOT fatal — signals no-op (and
	// expiry stays lazy-only) until the background redial connects (BUGS-6:
	// the old flow gave up after the startup budget and never re-dialed, so
	// AbandonedCheckoutWorkflow never started for any session).
	lazyTemporal := configureTemporal(cfg, logger)
	defer lazyTemporal.Close()
	svc = svc.WithAbandonment(checkoutwf.NewNotifier(lazyTemporal, cfg.Temporal.TaskQueue, cfg.Checkout.SessionTTL, logger))
	handler := webv1.NewHandler(svc)

	// Retention: finished idempotency rows cache address-bearing session
	// JSON; reap them after the 24h replay window (unfinished rows are never
	// reaped — a parked confirm's claim binding must not rot).
	go runIdempotencyReaper(repo, logger)

	// Local OIDC JWT verification (ADR-041): Keycloak is the issuer; the JWKS
	// URL is derived from the issuer (realm certs endpoint) unless
	// OIDC_JWKS_URL overrides it. NewVerifier does not block on an unreachable
	// JWKS — it refreshes in the background, so it is safe to build at startup.
	verifier, err := authmw.NewVerifier(authmw.Config{
		Issuer:   cfg.OIDCIssuer,
		Audience: cfg.OIDCAudience,
		JWKSURL:  cfg.OIDCJWKSURL,
	})
	if err != nil {
		logger.Error(ctx, "JWT verifier init failed", slogx.Err(err))
		return
	}

	var isShuttingDown atomic.Bool
	srv := setupServer(cfg, obsx.ConfigFromEnv().ServiceName, logger, handler, verifier, pool, &isShuttingDown)
	runGracefulShutdown(cfg, logger, srv, tp, pool, &isShuttingDown)
}

// idempotencyRetention is the replay window (Stripe-style 24h) after which a
// FINISHED key row (and its cached response body) is deleted.
const idempotencyRetention = 24 * time.Hour

// runIdempotencyReaper deletes expired finished idempotency rows hourly.
func runIdempotencyReaper(repo *postgres.SessionRepository, logger *slogx.Logger) {
	ctx := context.Background()
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		n, err := repo.ReapFinishedIdempotencyKeys(context.Background(), idempotencyRetention)
		if err != nil {
			logger.Warn(ctx, "idempotency reap failed", slogx.Err(err))
			continue
		}
		if n > 0 {
			logger.Info(ctx, "idempotency keys reaped", slog.Int64("rows", n))
		}
	}
}

// dialEastWest opens the cart/product/order/shipping/inventory client connections
// (in that order) and returns a single cleanup for all of them.
//
// Inventory is in the list, not dialled separately as it was during the RFC-0021
// migration: it is the availability authority now, so a checkout that cannot reach
// it has no second answer to fall back to.
// The connection slice is sized from `targets`, never from a literal. This used
// to be a [5] array while `targets` held five entries with nothing enforcing the
// match — and the list has already grown once (inventory, RFC-0021 phase 4). A
// sixth entry would have compiled and then panicked on conns[5] at startup, and
// no test would have caught it, because `targets` is a literal.
func dialEastWest(cfg *config.Config, logger *slogx.Logger) ([]*grpc.ClientConn, func(), bool) {
	ctx := context.Background()
	targets := []struct {
		name string
		addr string
	}{
		{"cart", cfg.Checkout.CartGRPCAddr},
		{"product", cfg.Checkout.ProductGRPCAddr},
		{"order", cfg.Checkout.OrderGRPCAddr},
		{"shipping", cfg.Checkout.ShippingGRPCAddr},
		// The availability authority since RFC-0021 phase 4. Required, like the
		// others: there is no product-stock path to fall back to, so a checkout
		// that cannot reach inventory must fail its reads loudly rather than
		// answer from somewhere else.
		{"inventory", cfg.Checkout.InventoryGRPCAddr},
	}
	conns := make([]*grpc.ClientConn, len(targets))
	for i, tgt := range targets {
		conn, err := grpcx.Dial(tgt.addr)
		if err != nil {
			logger.Error(ctx, "Failed to dial "+tgt.name+" gRPC", slog.String("addr", tgt.addr), slogx.Err(err))
			for j := range i {
				_ = conns[j].Close()
			}
			return conns, nil, false
		}
		conns[i] = conn
	}
	cleanup := func() {
		for i, tgt := range targets {
			closeConn(conns[i], logger, tgt.name)
		}
	}
	return conns, cleanup, true
}

// initObservability wires the RFC-0014 OTel pipeline (traces, OTLP metrics,
// logs) and tees application logs into it. Returns the shutdown handle (nil
// when setup failed — the service still runs) and the possibly-teed logger.
func initObservability(logger *slogx.Logger) (interface{ Shutdown(context.Context) error }, *slogx.Logger) {
	ctx := context.Background()
	otelCfg := obsx.ConfigFromEnv()
	// ADR-063: the Temporal OTel v2 plugin requires the GLOBAL tracer provider
	// to be the replay-safe one; obsx keeps the option set and installation,
	// this factory only swaps the constructor. temporalx.Dial refuses to start
	// without it.
	obs, err := obsx.SetupObservability(context.Background(), otelCfg,
		obsx.WithTracerProviderFactory(func(c obsx.TracerProviderConfig) obsx.ShutdownTracerProvider {
			return temporalx.NewReplaySafeTracerProvider(c.SDKOptions()...)
		}))
	if err != nil {
		logger.Warn(ctx, "Failed to initialize OpenTelemetry", slogx.Err(err))
		return nil, logger
	}
	// The facade reaches OTLP through the global logger provider obsx
	// installed; rebuilding it only wires Flush, so a Fatal record is
	// exported before the process exits.
	logger = slogx.New(slogx.Config{Level: os.Getenv("LOG_LEVEL"), Flush: obs.ForceFlush})
	slogx.SetDefault(logger)
	logger.Info(ctx, "OpenTelemetry initialized",
		slog.Bool("traces", obs.Enabled().Traces),
		slog.Bool("otlp_metrics", obs.Enabled().Metrics),
		slog.Bool("otlp_logs", obs.Enabled().Logs),
		slog.String("endpoint", otelCfg.Endpoint),
		slog.Float64("sample_rate", otelCfg.SampleRate),
	)
	return obs, logger
}

// Temporal startup-dial budget: the bring-up race (compose/Kind) usually
// resolves within a few seconds; the serve path degrades on exhaustion, the
// worker path is fatal (it can do nothing without Temporal).
const (
	temporalDialAttempts = 5
	temporalDialBackoff  = 2 * time.Second
)

// dialTemporalRetry dials Temporal with a bounded linear-backoff budget — a
// single eager dial loses the bring-up race when Temporal reports healthy
// moments after this process starts (order-service lesson).
func dialTemporalRetry(cfg *config.Config, logger *slogx.Logger) (client.Client, error) {
	ctx := context.Background()
	var lastErr error
	for i := 1; i <= temporalDialAttempts; i++ {
		tc, err := temporalx.Dial(temporalx.Config{HostPort: cfg.Temporal.HostPort, Namespace: cfg.Temporal.Namespace},
			temporalx.WithLogger(logger.Slog()))
		if err == nil {
			return tc, nil
		}
		lastErr = err
		if i < temporalDialAttempts {
			logger.Warn(ctx, "Temporal dial failed; retrying",
				slog.Int("attempt", i), slog.Int("attempts", temporalDialAttempts),
				slog.String("hostport", cfg.Temporal.HostPort), slogx.Err(err))
			time.Sleep(time.Duration(i) * temporalDialBackoff)
		}
	}
	return nil, lastErr
}

// temporalRedialInterval paces the serve path's background redial after the
// startup budget is exhausted (order-service pattern).
const temporalRedialInterval = 15 * time.Second

// configureTemporal dials Temporal for the serve path. Failure is non-fatal
// and no longer permanent: on exhaustion it hands back a Lazy whose
// background loop keeps dialing until Temporal appears, so a checkout pod
// that raced Temporal at bring-up heals itself instead of silently never
// starting AbandonedCheckoutWorkflow until someone restarts it.
func configureTemporal(cfg *config.Config, logger *slogx.Logger) *checkoutwf.Lazy {
	ctx := context.Background()
	dial := func() (client.Client, error) {
		return temporalx.Dial(temporalx.Config{HostPort: cfg.Temporal.HostPort, Namespace: cfg.Temporal.Namespace},
			temporalx.WithLogger(logger.Slog()))
	}
	tc, err := dialTemporalRetry(cfg, logger)
	if err != nil {
		logger.Warn(ctx, "Temporal unavailable at startup; background redial engaged — session expiry stays lazy-only until connected",
			slog.String("hostport", cfg.Temporal.HostPort),
			slog.Duration("redial_interval", temporalRedialInterval), slogx.Err(err))
		return checkoutwf.NewLazy(dial, temporalRedialInterval, logger)
	}
	logger.Info(ctx, "Temporal client initialized",
		slog.String("hostport", cfg.Temporal.HostPort),
		slog.String("namespace", cfg.Temporal.Namespace))
	return checkoutwf.NewLazySeeded(tc, logger)
}

// maybeRunWorker runs the Temporal worker for the abandonment workflow when
// invoked as `<binary> worker`, and reports whether it handled the command.
// Temporal being unreachable after the retry budget is fatal here — the
// worker can do nothing without it (order-worker pattern).
func maybeRunWorker(cfg *config.Config, logger *slogx.Logger, pool *pgxpool.Pool) bool {
	ctx := context.Background()
	if len(os.Args) <= 1 || os.Args[1] != "worker" {
		return false
	}

	tc, err := dialTemporalRetry(cfg, logger)
	if err != nil {
		logger.Fatal(ctx, "Failed to connect to Temporal", slog.String("hostport", cfg.Temporal.HostPort), slogx.Err(err))
	}
	defer tc.Close()

	acts := &checkoutwf.Activities{
		Sessions:     postgres.NewSessionRepository(pool),
		LockTakeover: cfg.Checkout.IdempotencyLockTakeover,
	}
	// ADR-064: versioning from the controller-injected env. Inert while the
	// manifest is still a plain Deployment (no env set → no-op); when the
	// WorkerDeployment lands, the worker registers Pinned without a code change.
	w := temporalx.NewWorker(tc, cfg.Temporal.TaskQueue, temporalx.MustVersioningFromEnv())
	w.RegisterWorkflowWithOptions(checkoutwf.AbandonedCheckoutWorkflow, workflow.RegisterOptions{
		VersioningBehavior: workflow.VersioningBehaviorPinned,
	})
	w.RegisterActivity(acts.ExpireIfDue)

	// Probes need an endpoint even on the worker (order-worker pattern);
	// /ready flips once the poller is about to run.
	ready := &atomic.Bool{}
	healthSrv := startWorkerHealthServer(cfg.Service.Port, logger, ready)
	defer func() { _ = healthSrv.Close() }()

	logger.Info(ctx, "Starting Temporal worker",
		slog.String("hostport", cfg.Temporal.HostPort),
		slog.String("namespace", cfg.Temporal.Namespace),
		slog.String("task_queue", cfg.Temporal.TaskQueue))
	ready.Store(true)
	logger.ProcessStarted(ctx, slogx.ComponentWorker)
	if err := w.Run(worker.InterruptCh()); err != nil {
		logger.ProcessStopped(ctx, slogx.ComponentWorker, slogx.OutcomeError)
		logger.Fatal(ctx, "Temporal worker stopped with error", slogx.Err(err))
	}
	// Written before the caller shuts the OTel SDK down, so it is exported.
	logger.ProcessStopped(ctx, slogx.ComponentWorker, slogx.OutcomeGraceful)
	return true
}

// healthPayload is the probe response body, written once. The API server's
// probes are the only place this shape appears more than twice; the worker
// process below serves the same field from a raw mux, so the two must not drift.
func healthPayload(state string) gin.H { return gin.H{"status": state} }

// startWorkerHealthServer serves /health and /ready for the worker process.
func startWorkerHealthServer(port string, logger *slogx.Logger, ready *atomic.Bool) *http.Server {
	ctx := context.Background()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"starting"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error(ctx, "worker health server failed", slogx.Err(err))
		}
	}()
	return srv
}

// runSubcommand handles `migrate`; returns true when it handled the command.
func runSubcommand(cmd string, cfg *config.Config, logger *slogx.Logger) bool {
	ctx := context.Background()
	if cmd != "migrate" {
		return false
	}
	if err := migratex.Run(migrations.FS, "sql", cfg.Database.BuildDSN()); err != nil {
		logger.Fatal(ctx, "Schema migration failed", slogx.Err(err))
	}
	logger.Info(ctx, "Schema migrations applied")
	return true
}

// setupServer builds the gin engine: tracing → logging → metrics-free infra
// endpoints, then the session routes behind the JWT middleware.
func setupServer(
	cfg *config.Config,
	otelServiceName string,
	logger *slogx.Logger,
	handler *webv1.Handler,
	verifier *authmw.Verifier,
	pool interface {
		Ping(context.Context) error
	},
	isShuttingDown *atomic.Bool,
) *http.Server {
	// gin.New, not gin.Default: Default installs gin's own logger and
	// recovery, which print the raw path and client address past the facade.
	r := gin.New()
	r.Use(httpmw.Tracing(otelServiceName))
	r.Use(httpmw.Logging(logger.Slog()))
	r.Use(httpmw.Recovery(logger.Slog()))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, healthPayload("ok"))
	})
	r.GET("/ready", func(c *gin.Context) {
		if isShuttingDown.Load() {
			c.JSON(http.StatusServiceUnavailable, healthPayload("shutting_down"))
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			c.JSON(http.StatusServiceUnavailable, healthPayload("db_unavailable"))
			return
		}
		c.JSON(http.StatusOK, healthPayload("ok"))
	})

	// Checkout v1 routes — Variant A collection-noun paths (`sessions`,
	// naming convention v3.0.0 / ADR-017), all private.
	webv1.RegisterRoutes(r, handler, authmw.MiddlewareJWT(verifier))

	return &http.Server{
		Addr:              ":" + cfg.Service.Port,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}
}

// runGracefulShutdown serves until SIGTERM/SIGINT, then drains: readiness
// flips first, the HTTP server shuts down, the pool closes, OTel flushes.
func runGracefulShutdown(
	cfg *config.Config,
	logger *slogx.Logger,
	srv *http.Server,
	tp interface{ Shutdown(context.Context) error },
	pool interface{ Close() },
	isShuttingDown *atomic.Bool,
) {
	ctx := context.Background()
	go func() {
		logger.Info(ctx, "Starting checkout service", slog.String("port", cfg.Service.Port))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error(ctx, "Failed to start server", slogx.Err(err))
		}
	}()

	logger.ProcessStarted(ctx, slogx.ComponentAPI)

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-sigCtx.Done()

	isShuttingDown.Store(true)
	drain := cfg.GetReadinessDrainDelayDuration()
	logger.Info(ctx, "Draining before shutdown", slog.Duration("delay", drain))
	time.Sleep(drain)

	shutdownTimeout := cfg.GetShutdownTimeoutDuration()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	outcome := slogx.OutcomeGraceful
	if err := srv.Shutdown(shutdownCtx); err != nil {
		outcome = slogx.OutcomeError
		logger.Error(ctx, "HTTP server shutdown error", slogx.Err(err))
	} else {
		logger.Info(ctx, "HTTP server shutdown complete")
	}

	pool.Close()
	logger.Info(ctx, "Database pool closed")

	// process.stopped goes out BEFORE the OTel SDK shuts down: a record
	// emitted after it is dropped rather than exported.
	logger.ProcessStopped(ctx, slogx.ComponentAPI, outcome)

	if tp != nil {
		if err := tp.Shutdown(shutdownCtx); err != nil {
			logger.Error(ctx, "OpenTelemetry shutdown error", slogx.Err(err))
		} else {
			logger.Info(ctx, "OpenTelemetry shutdown complete")
		}
	}

	logger.Info(ctx, "Graceful shutdown complete")
}

// closeConn closes a gRPC client connection at shutdown.
func closeConn(conn *grpc.ClientConn, logger *slogx.Logger, name string) {
	ctx := context.Background()
	if err := conn.Close(); err != nil {
		logger.Error(ctx, "gRPC connection close error", slog.String("target", name), slogx.Err(err))
	}
}
