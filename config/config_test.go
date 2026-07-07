package config

import (
	"strings"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	cfg := Load()

	if cfg.Service.Port != "8080" {
		t.Errorf("default PORT = %s, want 8080", cfg.Service.Port)
	}
	if cfg.Service.Env != "development" {
		t.Errorf("default ENV = %s, want development", cfg.Service.Env)
	}
	if cfg.Logging.Level != "info" || cfg.Logging.Format != "json" {
		t.Errorf("default logging = %s/%s, want info/json", cfg.Logging.Level, cfg.Logging.Format)
	}
	if !cfg.Metrics.Enabled || cfg.Metrics.Path != "/metrics" {
		t.Errorf("default metrics = %v/%s, want true//metrics", cfg.Metrics.Enabled, cfg.Metrics.Path)
	}
	if cfg.ShutdownTimeout != 10 {
		t.Errorf("default SHUTDOWN_TIMEOUT = %d, want 10", cfg.ShutdownTimeout)
	}
	if cfg.ReadinessDrainDelay != 5 {
		t.Errorf("default READINESS_DRAIN_DELAY = %d, want 5", cfg.ReadinessDrainDelay)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("SERVICE_NAME", "checkout")
	t.Setenv("ENV", "production")
	t.Setenv("LOG_LEVEL", "warn")
	t.Setenv("OTEL_SAMPLE_RATE", "0.5")
	t.Setenv("SHUTDOWN_TIMEOUT", "20s")
	t.Setenv("READINESS_DRAIN_DELAY", "10s")

	cfg := Load()

	if cfg.Service.Name != "checkout" {
		t.Errorf("SERVICE_NAME = %s, want checkout", cfg.Service.Name)
	}
	if !cfg.IsProduction() || cfg.IsDevelopment() {
		t.Errorf("env detection wrong for %s", cfg.Service.Env)
	}
	if cfg.Logging.Level != "warn" {
		t.Errorf("LOG_LEVEL = %s, want warn", cfg.Logging.Level)
	}
	if cfg.Tracing.SampleRate != 0.5 {
		t.Errorf("OTEL_SAMPLE_RATE = %f, want 0.5", cfg.Tracing.SampleRate)
	}
	if cfg.ShutdownTimeout != 20 {
		t.Errorf("SHUTDOWN_TIMEOUT = %d, want 20", cfg.ShutdownTimeout)
	}
	if cfg.ReadinessDrainDelay != 10 {
		t.Errorf("READINESS_DRAIN_DELAY = %d, want 10", cfg.ReadinessDrainDelay)
	}
}

func TestValidateRequiresServiceName(t *testing.T) {
	cfg := Load()

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want error without SERVICE_NAME")
	}
	if !strings.Contains(err.Error(), "SERVICE_NAME") {
		t.Errorf("error should mention SERVICE_NAME, got: %v", err)
	}
}

func TestValidateOK(t *testing.T) {
	t.Setenv("SERVICE_NAME", "checkout")

	cfg := Load()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestValidateRejectsBadEnv(t *testing.T) {
	t.Setenv("SERVICE_NAME", "checkout")
	t.Setenv("ENV", "qa")

	cfg := Load()
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "ENV must be one of") {
		t.Fatalf("Validate() = %v, want ENV error", err)
	}
}

func TestValidateDatabaseOnlyWhenHostSet(t *testing.T) {
	t.Setenv("SERVICE_NAME", "checkout")
	t.Setenv("DB_HOST", "db.local")

	cfg := Load()
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "DB_NAME") {
		t.Fatalf("Validate() = %v, want DB_NAME error once DB_HOST is set", err)
	}
}

func TestBuildDSN(t *testing.T) {
	db := DatabaseConfig{
		Host: "db.local", Port: "5432", Name: "checkout",
		User: "checkout", Password: "secret", SSLMode: "require",
	}
	want := "postgresql://checkout:secret@db.local:5432/checkout?sslmode=require"
	if got := db.BuildDSN(); got != want {
		t.Errorf("BuildDSN() = %s, want %s", got, want)
	}
}
