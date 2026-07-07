package main

import (
	"context"
	"errors"
	"net/http"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"

	"github.com/duynhlab/checkout-service/config"
	logicv1 "github.com/duynhlab/checkout-service/internal/logic/v1"
	webv1 "github.com/duynhlab/checkout-service/internal/web/v1"
	"github.com/duynhlab/checkout-service/middleware"
	"github.com/duynhlab/pkg/logger/zerolog"
	"github.com/duynhlab/pkg/obsx"
)

func main() {
	// Load configuration
	cfg := config.Load()

	// Initialize Zerolog with LOG_LEVEL from config
	zerolog.Setup(cfg.Logging.Level)

	if err := cfg.Validate(); err != nil {
		panic("Configuration validation failed: " + err.Error())
	}

	log.Info().
		Str("service", cfg.Service.Name).
		Str("version", cfg.Service.Version).
		Str("env", cfg.Service.Env).
		Str("port", cfg.Service.Port).
		Msg("Service starting")

	// Initialize OpenTelemetry tracing
	var tp interface{ Shutdown(context.Context) error }
	var err error
	if cfg.Tracing.Enabled {
		tp, err = middleware.InitTracing(cfg)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to initialize tracing")
		} else {
			log.Info().
				Str("endpoint", cfg.Tracing.Endpoint).
				Float64("sample_rate", cfg.Tracing.SampleRate).
				Msg("Tracing initialized")
		}
	} else {
		log.Info().Msg("Tracing disabled (TRACING_ENABLED=false)")
	}

	// Initialize metrics: install the global OTel MeterProvider bridged onto the
	// Prometheus /metrics endpoint.
	if cfg.Metrics.Enabled {
		shutdownMetrics, metricsErr := obsx.SetupMetrics()
		if metricsErr != nil {
			log.Warn().Err(metricsErr).Msg("Failed to initialize metrics")
		} else {
			log.Info().Msg("Metrics initialized")
			defer func() { _ = shutdownMetrics(context.Background()) }()
		}
	}

	// Initialize Pyroscope profiling
	if cfg.Profiling.Enabled {
		stopProfiling, profErr := obsx.SetupProfiling()
		if profErr != nil {
			log.Warn().Err(profErr).Msg("Failed to initialize profiling")
		} else {
			log.Info().
				Str("endpoint", cfg.Profiling.Endpoint).
				Msg("Profiling initialized")
			defer func() { _ = stopProfiling(context.Background()) }()
		}
	} else {
		log.Info().Msg("Profiling disabled (PROFILING_ENABLED=false)")
	}

	// Wire dependencies: Logic service -> Web handler
	checkoutSvc := logicv1.NewCheckoutService()
	handler := webv1.NewHandler(checkoutSvc, cfg)

	// Setup router and server, then run with graceful shutdown
	var isShuttingDown atomic.Bool
	srv := setupServer(cfg, handler, &isShuttingDown)
	runGracefulShutdown(cfg, srv, tp, &isShuttingDown)
}

// setupServer creates and configures the HTTP server with all routes and middleware.
func setupServer(cfg *config.Config, handler *webv1.Handler, isShuttingDown *atomic.Bool) *http.Server {
	if cfg.IsProduction() {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(gin.Recovery())

	// Tracing middleware
	r.Use(middleware.TracingMiddleware())

	// Logging middleware
	r.Use(middleware.LoggingMiddleware())

	// Prometheus middleware
	r.Use(middleware.PrometheusMiddleware())

	// Health check
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Readiness check
	// Returns 503 once shutdown has started, to drain traffic before HTTP shutdown.
	r.GET("/ready", func(c *gin.Context) {
		if isShuttingDown.Load() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "shutting_down"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Metrics endpoint
	r.GET(cfg.Metrics.Path, gin.WrapH(promhttp.Handler()))

	// Checkout v1 routes
	handler.RegisterRoutes(r)

	// Create HTTP server with ReadHeaderTimeout to prevent Slowloris attacks
	return &http.Server{
		Addr:              ":" + cfg.Service.Port,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}
}

// runGracefulShutdown starts the server and handles graceful shutdown.
// Shutdown sequence (VictoriaMetrics pattern): /ready → 503 → drain delay → HTTP → Tracer.
func runGracefulShutdown(
	cfg *config.Config,
	srv *http.Server,
	tp interface{ Shutdown(context.Context) error },
	isShuttingDown *atomic.Bool,
) {
	// Start server in a goroutine
	go func() {
		log.Info().Str("port", cfg.Service.Port).Msg("Starting checkout service")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("Failed to start server")
		}
	}()

	// Graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Wait for shutdown signal
	<-ctx.Done()
	log.Info().Msg("Shutdown signal received")

	// Mark service as shutting down so /ready returns 503 immediately.
	isShuttingDown.Store(true)

	// Fail readiness first and wait for propagation (best practice for K8s rollout).
	drainDelay := cfg.GetReadinessDrainDelayDuration()
	if drainDelay > 0 {
		log.Info().Dur("delay", drainDelay).Msg("Readiness drain delay started")
		time.Sleep(drainDelay)
		log.Info().Dur("delay", drainDelay).Msg("Readiness drain delay completed")
	}

	// Shutdown context with configurable timeout
	shutdownTimeout := cfg.GetShutdownTimeoutDuration()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	log.Info().Dur("timeout", shutdownTimeout).Msg("Shutting down server...")

	// 1. Shutdown HTTP server
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("HTTP server shutdown error")
	} else {
		log.Info().Msg("HTTP server shutdown complete")
	}

	// 2. Shutdown tracer
	if tp != nil {
		if err := tp.Shutdown(shutdownCtx); err != nil {
			log.Error().Err(err).Msg("Tracer shutdown error")
		} else {
			log.Info().Msg("Tracer shutdown complete")
		}
	}

	log.Info().Msg("Graceful shutdown complete")
}
