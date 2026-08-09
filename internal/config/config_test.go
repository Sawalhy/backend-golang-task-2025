package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// No database, no network — config is pure functions over the environment, and
// t.Setenv restores the previous value automatically.

func TestLoadUsesDefaultsWhenEnvironmentIsEmpty(t *testing.T) {
	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "development", cfg.Env)
	assert.Equal(t, 8080, cfg.HTTP.Port)
	assert.Equal(t, 12, cfg.DB.MaxOpenConns)
	assert.Equal(t, 15*time.Minute, cfg.Order.ReservationTTL)
	assert.Equal(t, 10, cfg.Worker.Concurrency)
	assert.NotEmpty(t, cfg.Auth.JWTSecret, "development must fall back to a usable secret")
}

func TestLoadReadsEveryScalarKind(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("JWT_SECRET", "a-secret-that-is-definitely-long-enough-32")
	t.Setenv("HTTP_PORT", "9090")                 // int
	t.Setenv("RESERVATION_TTL", "45s")            // duration
	t.Setenv("PAYMENT_FAILURE_RATE", "0.25")      // float
	t.Setenv("RABBITMQ_EXCHANGE", "custom")       // string

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, 9090, cfg.HTTP.Port)
	assert.Equal(t, 45*time.Second, cfg.Order.ReservationTTL)
	assert.InDelta(t, 0.25, cfg.Payments.SimulatedFailureRate, 1e-9)
	assert.Equal(t, "custom", cfg.Rabbit.Exchange)
	assert.True(t, cfg.IsProduction())
}

// A malformed value falls back to the default rather than refusing to boot. The
// alternative is a service that will not start because somebody typed
// HTTP_PORT=808O, and every default here is safe.
func TestMalformedValuesFallBackToDefaults(t *testing.T) {
	t.Setenv("HTTP_PORT", "not-a-number")
	t.Setenv("RESERVATION_TTL", "banana")
	t.Setenv("PAYMENT_FAILURE_RATE", "high")
	t.Setenv("DB_MAX_OPEN_CONNS", "")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, 8080, cfg.HTTP.Port)
	assert.Equal(t, 15*time.Minute, cfg.Order.ReservationTTL)
	assert.InDelta(t, 0.10, cfg.Payments.SimulatedFailureRate, 1e-9)
	assert.Equal(t, 12, cfg.DB.MaxOpenConns, "an empty value is not an override")
}

// A missing JWT secret in production is a security hole, not a warning, so the
// process must refuse to start rather than serve unsigned-ish tokens.
func TestProductionRefusesToBootWithoutAJWTSecret(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("JWT_SECRET", "")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "JWT_SECRET")
}

func TestProductionRejectsAShortJWTSecret(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("JWT_SECRET", "too-short")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "32")
}

// Development gets a working default so the stack boots from a clean clone.
func TestDevelopmentFallsBackToAnInsecureSecret(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	t.Setenv("JWT_SECRET", "")

	cfg, err := Load()
	require.NoError(t, err)
	assert.NotEmpty(t, cfg.Auth.JWTSecret)
	assert.False(t, cfg.IsProduction())
}

// Pool sizing is the single most load-bearing number under concurrency, so the
// nonsensical combinations are rejected at boot rather than at the first query.
func TestPoolSizingIsValidated(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{
			name:    "zero max open connections",
			env:     map[string]string{"DB_MAX_OPEN_CONNS": "0"},
			wantErr: "DB_MAX_OPEN_CONNS",
		},
		{
			name:    "more idle than open",
			env:     map[string]string{"DB_MAX_OPEN_CONNS": "4", "DB_MAX_IDLE_CONNS": "20"},
			wantErr: "DB_MAX_IDLE_CONNS",
		},
		{
			name:    "zero worker concurrency",
			env:     map[string]string{"WORKER_CONCURRENCY": "0"},
			wantErr: "WORKER_CONCURRENCY",
		},
		{
			name:    "non-positive reservation ttl",
			env:     map[string]string{"RESERVATION_TTL": "0s"},
			wantErr: "RESERVATION_TTL",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// Every problem is reported at once, so a misconfigured deployment is fixed in
// one pass rather than one restart per mistake.
func TestValidationReportsEveryProblemTogether(t *testing.T) {
	t.Setenv("DB_MAX_OPEN_CONNS", "0")
	t.Setenv("WORKER_CONCURRENCY", "0")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DB_MAX_OPEN_CONNS")
	assert.Contains(t, err.Error(), "WORKER_CONCURRENCY")
}

func TestIsProductionOnlyMatchesProduction(t *testing.T) {
	for env, want := range map[string]bool{
		"production":  true,
		"development": false,
		"staging":     false,
		"prod":        false, // deliberately exact: "prod" must not enable production behaviour
	} {
		t.Run(env, func(t *testing.T) {
			c := &Config{Env: env}
			assert.Equal(t, want, c.IsProduction())
		})
	}
}
