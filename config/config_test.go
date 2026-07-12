package config

import (
	"strings"
	"testing"
)

func TestBuildDSN(t *testing.T) {
	db := &DatabaseConfig{
		Host: "localhost", Port: "5432", Name: "shipping",
		User: "shipping", Password: "secret", SSLMode: "disable",
	}
	want := "postgresql://shipping:secret@localhost:5432/shipping?sslmode=disable"
	if got := db.BuildDSN(); got != want {
		t.Errorf("BuildDSN() = %q, want %q", got, want)
	}
}

func TestLoad_Defaults(t *testing.T) {
	// Empty value makes getEnv* fall back to the default.
	for _, k := range []string{"SERVICE_NAME", "PORT", "ENV", "TRACING_ENABLED", "OTEL_SAMPLE_RATE", "DB_HOST", "DB_POOL_MAX_CONNECTIONS"} {
		t.Setenv(k, "")
	}
	cfg := Load()
	if cfg.Service.Port != "8080" {
		t.Errorf("default Port = %q, want 8080", cfg.Service.Port)
	}
	if !cfg.Tracing.Enabled {
		t.Error("default Tracing.Enabled = false, want true")
	}
	if cfg.Tracing.SampleRate != 0.1 {
		t.Errorf("default SampleRate = %v, want 0.1", cfg.Tracing.SampleRate)
	}
	if cfg.Database.MaxConnections != 25 {
		t.Errorf("default MaxConnections = %d, want 25", cfg.Database.MaxConnections)
	}
}

func TestLoad_Overrides(t *testing.T) {
	t.Setenv("SERVICE_NAME", "shipping")
	t.Setenv("PORT", "9999")
	t.Setenv("ENV", "production")
	t.Setenv("TRACING_ENABLED", "false")
	t.Setenv("OTEL_SAMPLE_RATE", "0.5")
	t.Setenv("DB_POOL_MAX_CONNECTIONS", "not-a-number") // invalid → falls back to default

	cfg := Load()
	if cfg.Service.Name != "shipping" || cfg.Service.Port != "9999" || cfg.Service.Env != "production" {
		t.Errorf("overrides not applied: %+v", cfg.Service)
	}
	if cfg.Tracing.Enabled {
		t.Error("TRACING_ENABLED=false not applied")
	}
	if cfg.Tracing.SampleRate != 0.5 {
		t.Errorf("SampleRate = %v, want 0.5", cfg.Tracing.SampleRate)
	}
	if cfg.Database.MaxConnections != 25 {
		t.Errorf("invalid int env should fall back to 25, got %d", cfg.Database.MaxConnections)
	}
}

// validConfig returns a Config that passes Validate().
func validConfig() *Config {
	c := &Config{}
	c.Service = ServiceConfig{Name: "shipping", Port: "8080", Env: "production"}
	c.Tracing = TracingConfig{Enabled: true, Endpoint: "otel:4318", SampleRate: 0.1, ServiceName: "shipping"}
	c.Profiling = ProfilingConfig{Enabled: true, Endpoint: "pyro:4040", ServiceName: "shipping"}
	c.Logging = LoggingConfig{Level: "info", Format: "json"}
	c.Database = DatabaseConfig{} // Host empty → database validation skipped
	return c
}

func TestValidate(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("validConfig().Validate() = %v, want nil", err)
	}

	tests := []struct {
		name string
		mut  func(*Config)
	}{
		{"missing service name", func(c *Config) { c.Service.Name = "" }},
		{"non-numeric port", func(c *Config) { c.Service.Port = "abc" }},
		{"invalid env", func(c *Config) { c.Service.Env = "qa" }},
		{"tracing endpoint missing", func(c *Config) { c.Tracing.Endpoint = "" }},
		{"sample rate out of range", func(c *Config) { c.Tracing.SampleRate = 2 }},
		{"profiling endpoint missing", func(c *Config) { c.Profiling.Endpoint = "" }},
		{"invalid log level", func(c *Config) { c.Logging.Level = "trace" }},
		{"invalid log format", func(c *Config) { c.Logging.Format = "xml" }},
		{"db host set but name missing", func(c *Config) { c.Database.Host = "h"; c.Database.User = "u"; c.Database.Password = "p" }},
		{"db bad port", func(c *Config) {
			c.Database.Host = "h"
			c.Database.Name = "n"
			c.Database.User = "u"
			c.Database.Password = "p"
			c.Database.Port = "x"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig()
			tt.mut(c)
			if err := c.Validate(); err == nil {
				t.Errorf("Validate() = nil, want error for %q", tt.name)
			}
		})
	}
}

func TestIsDevelopmentProduction(t *testing.T) {
	tests := []struct {
		env       string
		isDev     bool
		isProd    bool
	}{
		{"development", true, false},
		{"dev", true, false},
		{"production", false, true},
		{"prod", false, true},
		{"staging", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.env, func(t *testing.T) {
			c := &Config{}
			c.Service.Env = tt.env
			if c.IsDevelopment() != tt.isDev {
				t.Errorf("IsDevelopment(%q) = %v, want %v", tt.env, c.IsDevelopment(), tt.isDev)
			}
			if c.IsProduction() != tt.isProd {
				t.Errorf("IsProduction(%q) = %v, want %v", tt.env, c.IsProduction(), tt.isProd)
			}
		})
	}
}

func TestGetEnvHelpers(t *testing.T) {
	t.Run("getEnv falls back when empty", func(t *testing.T) {
		t.Setenv("X_TEST_KEY", "")
		if got := getEnv("X_TEST_KEY", "def"); got != "def" {
			t.Errorf("getEnv empty = %q, want def", got)
		}
		t.Setenv("X_TEST_KEY", "set")
		if got := getEnv("X_TEST_KEY", "def"); got != "set" {
			t.Errorf("getEnv set = %q, want set", got)
		}
	})

	t.Run("getEnvBool accepts truthy variants", func(t *testing.T) {
		for _, v := range []string{"true", "1", "yes", "TRUE"} {
			t.Setenv("X_BOOL", v)
			if !getEnvBool("X_BOOL", false) {
				t.Errorf("getEnvBool(%q) = false, want true", v)
			}
		}
		t.Setenv("X_BOOL", "no")
		if getEnvBool("X_BOOL", true) {
			t.Error("getEnvBool(no) = true, want false")
		}
	})

	t.Run("getEnvInt falls back on invalid", func(t *testing.T) {
		t.Setenv("X_INT", "notnum")
		if got := getEnvInt("X_INT", 7); got != 7 {
			t.Errorf("getEnvInt(invalid) = %d, want 7", got)
		}
		t.Setenv("X_INT", "42")
		if got := getEnvInt("X_INT", 7); got != 42 {
			t.Errorf("getEnvInt(42) = %d, want 42", got)
		}
	})
}

func TestValidateErrorMentionsField(t *testing.T) {
	c := validConfig()
	c.Service.Name = ""
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "SERVICE_NAME") {
		t.Errorf("expected error mentioning SERVICE_NAME, got %v", err)
	}
}
