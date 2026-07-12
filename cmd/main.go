// checkout-service — RFC-0015: the session/UX orchestrator between the SPA
// and order-service. Client-only (no gRPC server): it dials cart (item-list
// authority) and product (price authority) to snapshot and re-validate
// checkout sessions. Subcommands: `migrate` applies the embedded schema
// migrations; no `seed` — checkout has no demo data in P1.
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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"

	"github.com/duynhlab/pkg/authmw"
	"github.com/duynhlab/pkg/grpcx"
	"github.com/duynhlab/pkg/idempotency"
	"github.com/duynhlab/pkg/logger/zapx"
	"github.com/duynhlab/pkg/migratex"
	"github.com/duynhlab/pkg/obsx"

	"github.com/duynhlab/checkout-service/config"
	migrations "github.com/duynhlab/checkout-service/db/migrations"
	"github.com/duynhlab/checkout-service/internal/clients"
	database "github.com/duynhlab/checkout-service/internal/core"
	"github.com/duynhlab/checkout-service/internal/core/repository/postgres"
	logicv1 "github.com/duynhlab/checkout-service/internal/logic/v1"
	webv1 "github.com/duynhlab/checkout-service/internal/web/v1"
	"github.com/duynhlab/checkout-service/middleware"
)

func main() {
	cfg := config.Load()

	logger, err := zapx.New(os.Getenv("LOG_LEVEL"))
	if err != nil {
		panic("Failed to initialize logger: " + err.Error())
	}
	defer func() { _ = logger.Sync() }()

	// Subcommand `migrate` runs the embedded SQL set and exits.
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

	// East-west gRPC clients (lazy dial — grpcx.Dial uses grpc.NewClient, so
	// an unreachable target fails per-call, not at startup).
	cartConn, err := grpcx.Dial(cfg.Checkout.CartGRPCAddr)
	if err != nil {
		logger.Error("Failed to dial cart gRPC", zap.String("addr", cfg.Checkout.CartGRPCAddr), zap.Error(err))
		return
	}
	defer closeConn(cartConn, logger, "cart")
	productConn, err := grpcx.Dial(cfg.Checkout.ProductGRPCAddr)
	if err != nil {
		logger.Error("Failed to dial product gRPC", zap.String("addr", cfg.Checkout.ProductGRPCAddr), zap.Error(err))
		return
	}
	defer closeConn(productConn, logger, "product")
	orderConn, err := grpcx.Dial(cfg.Checkout.OrderGRPCAddr)
	if err != nil {
		logger.Error("Failed to dial order gRPC", zap.String("addr", cfg.Checkout.OrderGRPCAddr), zap.Error(err))
		return
	}
	defer closeConn(orderConn, logger, "order")

	// Deadline-fencing invariant (RFC-0015 P2 confirm): a lock takeover must
	// PROVE the previous owner is dead, which holds only when the takeover
	// window dwarfs the longest possible confirm execution.
	if cfg.Checkout.IdempotencyLockTakeover <= 4*logicv1.ConfirmDeadline {
		logger.Error("IDEMPOTENCY_LOCK_TAKEOVER must exceed 4× the confirm deadline",
			zap.Duration("takeover", cfg.Checkout.IdempotencyLockTakeover),
			zap.Duration("confirm_deadline", logicv1.ConfirmDeadline))
		return
	}

	repo := postgres.NewSessionRepository(pool)
	svc := logicv1.NewCheckoutService(repo,
		clients.NewCartClient(cartConn),
		clients.NewProductClient(productConn),
		cfg.Checkout.SessionTTL,
	).WithConfirm(
		idempotency.New(pool, cfg.Checkout.IdempotencyLockTakeover),
		clients.NewOrderClient(orderConn),
	)
	handler := webv1.NewHandler(svc)

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
