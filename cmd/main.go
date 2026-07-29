// checkout-service — RFC-0015: the session/UX orchestrator between the SPA
// and order-service. Client-only on gRPC (no server): it dials cart
// (item-list authority), product (price authority), and order (the P2
// confirm handoff). Subcommands: `migrate` applies the embedded schema
// migrations; `worker` runs the Temporal worker for the
// AbandonedCheckoutWorkflow (ADR-019). No `seed` — checkout has no demo data.
package main

import (
	"context"
	"errors"
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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"

	"github.com/duynhlab/pkg/authmw"
	"github.com/duynhlab/pkg/grpcx"
	"github.com/duynhlab/pkg/idempotency"
	"github.com/duynhlab/pkg/logger/zapx"
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
	"github.com/duynhlab/checkout-service/middleware"
)

func main() {
	cfg := config.Load()

	logger, err := zapx.New(os.Getenv("LOG_LEVEL"))
	if err != nil {
		panic("Failed to initialize logger: " + err.Error())
	}
	defer func() { _ = logger.Sync() }()

	// Subcommand `migrate` runs the embedded SQL set and exits. `worker` is
	// handled below, after observability and the DB pool exist.
	if len(os.Args) > 1 && runSubcommand(os.Args[1], cfg, logger) {
		return
	}

	if err := cfg.Validate(); err != nil {
		panic("Configuration validation failed: " + err.Error())
	}

	logger.Info("Service starting",
		zap.String("service", cfg.Service.Name),
		zap.String("version", cfg.Service.Version),
		zap.String("env", cfg.Service.Env),
		zap.String("port", cfg.Service.Port),
	)

	pool, err := database.Connect(context.Background(), cfg)
	if err != nil {
		logger.Error("Failed to connect to database", zap.Error(err))
		return
	}
	defer pool.Close()
	logger.Info("Database connection pool established")

	// RFC-0014: single OTel wiring point — traces, OTLP metrics, logs.
	tp, logger := initObservability(logger)

	if cfg.Profiling.Enabled {
		stopProfiling, err := obsx.SetupProfiling()
		if err != nil {
			logger.Warn("Failed to initialize profiling", zap.Error(err))
		} else {
			logger.Info("Profiling initialized", zap.String("endpoint", cfg.Profiling.Endpoint))
			defer func() {
				if err := stopProfiling(context.Background()); err != nil {
					logger.Error("Profiling shutdown error", zap.Error(err))
				}
			}()
		}
	} else {
		logger.Info("Profiling disabled (PROFILING_ENABLED=false)")
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
	cartConn, productConn, orderConn, shippingConn := conns[0], conns[1], conns[2], conns[3]

	// Deadline-fencing invariant (RFC-0015 P2 confirm): a lock takeover must
	// PROVE the previous owner is dead, which holds only when the takeover
	// window dwarfs the longest possible confirm execution.
	if cfg.Checkout.IdempotencyLockTakeover <= 4*logicv1.ConfirmDeadline {
		// Fatal: a config gate must exit non-zero so orchestrators see it.
		logger.Fatal("IDEMPOTENCY_LOCK_TAKEOVER must exceed 4× the confirm deadline",
			zap.Duration("takeover", cfg.Checkout.IdempotencyLockTakeover),
			zap.Duration("confirm_deadline", logicv1.ConfirmDeadline))
	}

	repo := postgres.NewSessionRepository(pool)
	// One ProductClient instance serves both GetProducts (product mode) and
	// BatchGetCurrentPrices (inventory-mode price authority, P2-5).
	productClient := clients.NewProductClient(productConn)
	svc := logicv1.NewCheckoutService(repo,
		clients.NewCartClient(cartConn),
		productClient,
		cfg.Checkout.SessionTTL,
	).WithConfirm(
		idempotency.New(pool, cfg.Checkout.IdempotencyLockTakeover),
		clients.NewOrderClient(orderConn),
	).WithQuoter(clients.NewShippingClient(shippingConn))

	// RFC-0021 P2-4: attach inventory shadow reads (no-op in product mode).
	defer wireInventoryShadow(svc, cfg, logger, productClient)()

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

	// Local JWT verification via JWKS — fail-closed, the only credential path.
	verifier, err := authmw.NewVerifier(cfg.JWKSURL, cfg.JWTIssuer, cfg.JWTAudience)
	if err != nil {
		logger.Error("JWT verifier init failed", zap.Error(err))
		return
	}

	var isShuttingDown atomic.Bool
	srv := setupServer(cfg, logger, handler, verifier, pool, &isShuttingDown)
	runGracefulShutdown(cfg, logger, srv, tp, pool, &isShuttingDown)
}

// idempotencyRetention is the replay window (Stripe-style 24h) after which a
// FINISHED key row (and its cached response body) is deleted.
const idempotencyRetention = 24 * time.Hour

// runIdempotencyReaper deletes expired finished idempotency rows hourly.
func runIdempotencyReaper(repo *postgres.SessionRepository, logger *zap.Logger) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		n, err := repo.ReapFinishedIdempotencyKeys(context.Background(), idempotencyRetention)
		if err != nil {
			logger.Warn("idempotency reap failed", zap.Error(err))
			continue
		}
		if n > 0 {
			logger.Info("idempotency keys reaped", zap.Int64("rows", n))
		}
	}
}

// dialEastWest opens the cart/product/order/shipping client connections (in
// that order) and returns a single cleanup for all of them. Inventory is dialed
// separately and only in shadow/inventory mode (see main), so the default
// product path stays inventory-independent.
func dialEastWest(cfg *config.Config, logger *zap.Logger) ([4]*grpc.ClientConn, func(), bool) {
	var conns [4]*grpc.ClientConn
	targets := []struct {
		name string
		addr string
	}{
		{"cart", cfg.Checkout.CartGRPCAddr},
		{"product", cfg.Checkout.ProductGRPCAddr},
		{"order", cfg.Checkout.OrderGRPCAddr},
		{"shipping", cfg.Checkout.ShippingGRPCAddr},
	}
	for i, tgt := range targets {
		conn, err := grpcx.Dial(tgt.addr)
		if err != nil {
			logger.Error("Failed to dial "+tgt.name+" gRPC", zap.String("addr", tgt.addr), zap.Error(err))
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

// wireInventoryShadow dials + attaches the inventory client, but only when the
// availability source is not product — so a bad INVENTORY_GRPC_ADDR can never
// affect the default product path (RFC-0021 P2-4/P2-5). The inventory read path
// is optional: a dial failure disables it and logs, never blocks checkout
// startup. Wires both the shadow-compare fetcher (P2-4) and the inventory-mode
// split-read deps (P2-5: Product prices via the shared productClient + Inventory
// availability). Returns a cleanup that is a no-op unless a connection opened.
func wireInventoryShadow(svc *logicv1.CheckoutService, cfg *config.Config, logger *zap.Logger, productClient *clients.ProductClient) func() {
	if cfg.Checkout.AvailabilitySource == logicv1.AvailabilitySourceProduct {
		return func() {}
	}
	conn, err := grpcx.Dial(cfg.Checkout.InventoryGRPCAddr)
	if err != nil {
		// inventory mode is a deliberate availability-authority choice: a dial
		// failure must NOT silently degrade to product mode. Fail loud. shadow
		// is optional telemetry, so there a dial failure just disables it.
		if cfg.Checkout.AvailabilitySource == logicv1.AvailabilitySourceInventory {
			logger.Fatal("inventory mode selected but inventory gRPC dial failed",
				zap.String("addr", cfg.Checkout.InventoryGRPCAddr), zap.Error(err))
		}
		logger.Error("inventory shadow disabled: dial failed",
			zap.String("addr", cfg.Checkout.InventoryGRPCAddr), zap.Error(err))
		return func() {}
	}
	inv := clients.NewInventoryClient(conn)

	// The canary's assignment is a pure function of (key, user id), so every pod
	// serving the same users MUST hold the same key — a differing one re-shuffles
	// every user's arm, which is the silent split the sticky design exists to
	// prevent. The key itself never reaches a log; its fingerprint does, so two
	// pods can be compared. Warned, not fatal: an unkeyed deployment still works,
	// it just cannot bound exposure against a caller who grinds their own subject
	// claim until it lands on the arm they want.
	fp := logicv1.SaltFingerprint(cfg.Checkout.AvailabilityCanarySalt)
	if cfg.Checkout.AvailabilityCanaryPct > 0 && cfg.Checkout.AvailabilityCanaryPct < 100 && fp == "" {
		logger.Warn("availability canary is partly open with NO key; the percentage bounds honest traffic only",
			zap.Int("canary_pct", cfg.Checkout.AvailabilityCanaryPct),
			zap.String("hint", "set CHECKOUT_AVAILABILITY_CANARY_SALT (a Secret, not a ConfigMap key) before ramping"))
	}
	logger.Info("availability read path configured",
		zap.String("source", cfg.Checkout.AvailabilitySource),
		zap.Int("canary_pct", cfg.Checkout.AvailabilityCanaryPct),
		// Fingerprint, never the key. Empty = unkeyed.
		zap.String("canary_key_fingerprint", fp))
	svc.WithAvailabilitySource(
		cfg.Checkout.AvailabilitySource,
		cfg.Checkout.AvailabilityShadowSamplePct,
		inv,
	).WithInventoryMode(productClient, inv).
		WithAvailabilityCanary(cfg.Checkout.AvailabilityCanaryPct, cfg.Checkout.AvailabilityCanarySalt)
	return func() { closeConn(conn, logger, "inventory") }
}

// initObservability wires the RFC-0014 OTel pipeline (traces, OTLP metrics,
// logs) and tees application logs into it. Returns the shutdown handle (nil
// when setup failed — the service still runs) and the possibly-teed logger.
func initObservability(logger *zap.Logger) (interface{ Shutdown(context.Context) error }, *zap.Logger) {
	otelCfg := obsx.ConfigFromEnv()
	middleware.SetServiceName(otelCfg.ServiceName)
	obs, err := obsx.SetupObservability(context.Background(), otelCfg)
	if err != nil {
		logger.Warn("Failed to initialize OpenTelemetry", zap.Error(err))
		return nil, logger
	}
	minLevel, lvlErr := zapcore.ParseLevel(os.Getenv("LOG_LEVEL"))
	if lvlErr != nil {
		minLevel = zapcore.InfoLevel
	}
	logger = logger.WithOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return zapcore.NewTee(c, obs.ZapCore(otelCfg.ServiceName, minLevel))
	}))
	logger.Info("OpenTelemetry initialized",
		zap.Bool("traces", obs.TracerProvider != nil),
		zap.Bool("otlp_metrics", obs.MeterProvider != nil),
		zap.Bool("otlp_logs", obs.LoggerProvider != nil),
		zap.String("endpoint", otelCfg.Endpoint),
		zap.Float64("sample_rate", otelCfg.SampleRate),
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
func dialTemporalRetry(cfg *config.Config, logger *zap.Logger) (client.Client, error) {
	var lastErr error
	for i := 1; i <= temporalDialAttempts; i++ {
		tc, err := temporalx.Dial(temporalx.Config{HostPort: cfg.Temporal.HostPort, Namespace: cfg.Temporal.Namespace})
		if err == nil {
			return tc, nil
		}
		lastErr = err
		if i < temporalDialAttempts {
			logger.Warn("Temporal dial failed; retrying",
				zap.Int("attempt", i), zap.Int("attempts", temporalDialAttempts),
				zap.String("hostport", cfg.Temporal.HostPort), zap.Error(err))
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
func configureTemporal(cfg *config.Config, logger *zap.Logger) *checkoutwf.Lazy {
	dial := func() (client.Client, error) {
		return temporalx.Dial(temporalx.Config{HostPort: cfg.Temporal.HostPort, Namespace: cfg.Temporal.Namespace})
	}
	tc, err := dialTemporalRetry(cfg, logger)
	if err != nil {
		logger.Warn("Temporal unavailable at startup; background redial engaged — session expiry stays lazy-only until connected",
			zap.String("hostport", cfg.Temporal.HostPort),
			zap.Duration("redial_interval", temporalRedialInterval), zap.Error(err))
		return checkoutwf.NewLazy(dial, temporalRedialInterval, logger)
	}
	logger.Info("Temporal client initialized",
		zap.String("hostport", cfg.Temporal.HostPort),
		zap.String("namespace", cfg.Temporal.Namespace))
	return checkoutwf.NewLazySeeded(tc, logger)
}

// maybeRunWorker runs the Temporal worker for the abandonment workflow when
// invoked as `<binary> worker`, and reports whether it handled the command.
// Temporal being unreachable after the retry budget is fatal here — the
// worker can do nothing without it (order-worker pattern).
func maybeRunWorker(cfg *config.Config, logger *zap.Logger, pool *pgxpool.Pool) bool {
	if len(os.Args) <= 1 || os.Args[1] != "worker" {
		return false
	}

	tc, err := dialTemporalRetry(cfg, logger)
	if err != nil {
		logger.Fatal("Failed to connect to Temporal", zap.String("hostport", cfg.Temporal.HostPort), zap.Error(err))
	}
	defer tc.Close()

	acts := &checkoutwf.Activities{
		Sessions:     postgres.NewSessionRepository(pool),
		LockTakeover: cfg.Checkout.IdempotencyLockTakeover,
	}
	w := temporalx.NewWorker(tc, cfg.Temporal.TaskQueue)
	w.RegisterWorkflow(checkoutwf.AbandonedCheckoutWorkflow)
	w.RegisterActivity(acts.ExpireIfDue)

	// Probes need an endpoint even on the worker (order-worker pattern);
	// /ready flips once the poller is about to run.
	ready := &atomic.Bool{}
	healthSrv := startWorkerHealthServer(cfg.Service.Port, logger, ready)
	defer func() { _ = healthSrv.Close() }()

	logger.Info("Starting Temporal worker",
		zap.String("hostport", cfg.Temporal.HostPort),
		zap.String("namespace", cfg.Temporal.Namespace),
		zap.String("task_queue", cfg.Temporal.TaskQueue))
	ready.Store(true)
	if err := w.Run(worker.InterruptCh()); err != nil {
		logger.Fatal("Temporal worker stopped with error", zap.Error(err))
	}
	return true
}

// startWorkerHealthServer serves /health and /ready for the worker process.
func startWorkerHealthServer(port string, logger *zap.Logger, ready *atomic.Bool) *http.Server {
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
			logger.Error("worker health server failed", zap.Error(err))
		}
	}()
	return srv
}

// runSubcommand handles `migrate`; returns true when it handled the command.
func runSubcommand(cmd string, cfg *config.Config, logger *zap.Logger) bool {
	if cmd != "migrate" {
		return false
	}
	if err := migratex.Run(migrations.FS, "sql", cfg.Database.BuildDSN()); err != nil {
		logger.Fatal("Schema migration failed", zap.Error(err))
	}
	logger.Info("Schema migrations applied")
	return true
}

// setupServer builds the gin engine: tracing → logging → metrics-free infra
// endpoints, then the session routes behind the JWT middleware.
func setupServer(
	cfg *config.Config,
	logger *zap.Logger,
	handler *webv1.Handler,
	verifier *authmw.Verifier,
	pool interface {
		Ping(context.Context) error
	},
	isShuttingDown *atomic.Bool,
) *http.Server {
	r := gin.Default()
	r.Use(middleware.TracingMiddleware())
	r.Use(middleware.LoggingMiddleware(logger))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	r.GET("/ready", func(c *gin.Context) {
		if isShuttingDown.Load() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "shutting_down"})
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "db_unavailable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
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
	logger *zap.Logger,
	srv *http.Server,
	tp interface{ Shutdown(context.Context) error },
	pool interface{ Close() },
	isShuttingDown *atomic.Bool,
) {
	go func() {
		logger.Info("Starting checkout service", zap.String("port", cfg.Service.Port))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Failed to start server", zap.Error(err))
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-ctx.Done()

	isShuttingDown.Store(true)
	drain := cfg.GetReadinessDrainDelayDuration()
	logger.Info("Draining before shutdown", zap.Duration("delay", drain))
	time.Sleep(drain)

	shutdownTimeout := cfg.GetShutdownTimeoutDuration()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", zap.Error(err))
	} else {
		logger.Info("HTTP server shutdown complete")
	}

	pool.Close()
	logger.Info("Database pool closed")

	if tp != nil {
		if err := tp.Shutdown(shutdownCtx); err != nil {
			logger.Error("OpenTelemetry shutdown error", zap.Error(err))
		} else {
			logger.Info("OpenTelemetry shutdown complete")
		}
	}

	logger.Info("Graceful shutdown complete")
}

// closeConn closes a gRPC client connection at shutdown.
func closeConn(conn *grpc.ClientConn, logger *zap.Logger, name string) {
	if err := conn.Close(); err != nil {
		logger.Error("gRPC connection close error", zap.String("target", name), zap.Error(err))
	}
}
