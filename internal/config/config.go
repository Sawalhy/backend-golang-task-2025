// Package config loads process configuration from the environment.
//
// Deliberately dependency-free: envconfig and Viper both earn their keep on
// config files, profiles and live reload, none of which this service has. The
// whole surface is ~20 scalars read once at boot, so stdlib plus three helpers
// is smaller than the library that would replace it.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Env      string // development | production
	LogLevel string

	HTTP     HTTPConfig
	DB       DBConfig
	Rabbit   RabbitConfig
	Redis    RedisConfig
	Auth     AuthConfig
	Order    OrderConfig
	Worker   WorkerConfig
	Payments PaymentsConfig
}

type HTTPConfig struct {
	Port            int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

type DBConfig struct {
	DSN string
	// MaxOpenConns is the whole ballgame under load (failure mode J). A 4-core
	// Postgres saturates around 8-16 connections TOTAL; more connections past
	// that point reduce throughput rather than increasing it, because they
	// contend on the same cores. Note this is per process — N API replicas
	// multiply it, which is what exhausts max_connections on Kubernetes.
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

type RabbitConfig struct {
	URL      string
	Exchange string
	// Prefetch bounds how many unacked deliveries a consumer holds. Without it a
	// consumer grabs the whole queue and competing consumers get nothing.
	Prefetch int
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type AuthConfig struct {
	JWTSecret     string
	TokenTTL      time.Duration
	BcryptCost    int
	RateLimitRPS  int
	RateLimitBurst int
}

type OrderConfig struct {
	// ReservationTTL is how long stock is held for an unpaid order before the
	// reaper reclaims it (failure mode F). Too short and slow payments fail;
	// too long and abandoned carts starve real buyers.
	ReservationTTL time.Duration
	MaxItemsPerOrder int
	// DeadlockRetries bounds the jittered retry on Postgres 40P01. Deadlocks are
	// prevented by product_id ordering; this covers the residue.
	DeadlockRetries int
}

type WorkerConfig struct {
	Concurrency    int
	ReaperInterval time.Duration
	RelayInterval  time.Duration
	RelayBatchSize int
	NotifyLeaseTTL time.Duration
	// NotifyRetryInterval paces the re-drive of notifications whose send failed.
	// It is the backoff between attempts, not merely a polling rate: five
	// attempts fired in a tight loop would all hit the same outage.
	NotifyRetryInterval time.Duration
	ShutdownTimeout     time.Duration
}

type PaymentsConfig struct {
	Provider string
	// SimulatedFailureRate drives the fake provider: the design needs declines,
	// timeouts and UNKNOWN outcomes to be reachable in tests and demos.
	SimulatedFailureRate float64
	SimulatedTimeoutRate float64
	SimulatedLatency     time.Duration
	Timeout              time.Duration
}

func Load() (*Config, error) {
	c := &Config{
		Env:      env("APP_ENV", "development"),
		LogLevel: env("LOG_LEVEL", "info"),
		HTTP: HTTPConfig{
			Port:            envInt("HTTP_PORT", 8080),
			ReadTimeout:     envDur("HTTP_READ_TIMEOUT", 10*time.Second),
			WriteTimeout:    envDur("HTTP_WRITE_TIMEOUT", 30*time.Second),
			ShutdownTimeout: envDur("HTTP_SHUTDOWN_TIMEOUT", 20*time.Second),
		},
		DB: DBConfig{
			DSN:             env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/orders?sslmode=disable"),
			MaxOpenConns:    envInt("DB_MAX_OPEN_CONNS", 12),
			MaxIdleConns:    envInt("DB_MAX_IDLE_CONNS", 12),
			ConnMaxLifetime: envDur("DB_CONN_MAX_LIFETIME", time.Hour),
			ConnMaxIdleTime: envDur("DB_CONN_MAX_IDLE_TIME", 10*time.Minute),
		},
		Rabbit: RabbitConfig{
			URL:      env("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
			Exchange: env("RABBITMQ_EXCHANGE", "orders"),
			Prefetch: envInt("RABBITMQ_PREFETCH", 16),
		},
		Redis: RedisConfig{
			Addr:     env("REDIS_ADDR", "localhost:6379"),
			Password: env("REDIS_PASSWORD", ""),
			DB:       envInt("REDIS_DB", 0),
		},
		Auth: AuthConfig{
			JWTSecret:      env("JWT_SECRET", ""),
			TokenTTL:       envDur("JWT_TTL", 24*time.Hour),
			BcryptCost:     envInt("BCRYPT_COST", 12),
			RateLimitRPS:   envInt("RATE_LIMIT_RPS", 50),
			RateLimitBurst: envInt("RATE_LIMIT_BURST", 100),
		},
		Order: OrderConfig{
			ReservationTTL:   envDur("RESERVATION_TTL", 15*time.Minute),
			MaxItemsPerOrder: envInt("MAX_ITEMS_PER_ORDER", 50),
			DeadlockRetries:  envInt("DEADLOCK_RETRIES", 3),
		},
		Worker: WorkerConfig{
			Concurrency:         envInt("WORKER_CONCURRENCY", 10),
			ReaperInterval:      envDur("REAPER_INTERVAL", 30*time.Second),
			RelayInterval:       envDur("RELAY_INTERVAL", 500*time.Millisecond),
			RelayBatchSize:      envInt("RELAY_BATCH_SIZE", 100),
			NotifyLeaseTTL:      envDur("NOTIFY_LEASE_TTL", 60*time.Second),
			NotifyRetryInterval: envDur("NOTIFY_RETRY_INTERVAL", 30*time.Second),
			ShutdownTimeout:     envDur("WORKER_SHUTDOWN_TIMEOUT", 30*time.Second),
		},
		Payments: PaymentsConfig{
			Provider:             env("PAYMENT_PROVIDER", "simulated"),
			SimulatedFailureRate: envFloat("PAYMENT_FAILURE_RATE", 0.10),
			SimulatedTimeoutRate: envFloat("PAYMENT_TIMEOUT_RATE", 0.02),
			SimulatedLatency:     envDur("PAYMENT_LATENCY", 200*time.Millisecond),
			Timeout:              envDur("PAYMENT_TIMEOUT", 5*time.Second),
		},
	}

	return c, c.validate()
}

// validate fails at boot rather than at the first request. A missing JWT secret
// in production is a security hole, not a warning.
func (c *Config) validate() error {
	var problems []string

	if c.Auth.JWTSecret == "" {
		if c.IsProduction() {
			problems = append(problems, "JWT_SECRET must be set in production")
		} else {
			c.Auth.JWTSecret = "dev-only-insecure-secret-do-not-ship"
		}
	} else if len(c.Auth.JWTSecret) < 32 && c.IsProduction() {
		problems = append(problems, "JWT_SECRET must be at least 32 bytes")
	}

	if c.DB.MaxOpenConns < 1 {
		problems = append(problems, "DB_MAX_OPEN_CONNS must be >= 1")
	}
	if c.DB.MaxIdleConns > c.DB.MaxOpenConns {
		problems = append(problems, "DB_MAX_IDLE_CONNS cannot exceed DB_MAX_OPEN_CONNS")
	}
	if c.Order.ReservationTTL <= 0 {
		problems = append(problems, "RESERVATION_TTL must be positive")
	}
	if c.Worker.Concurrency < 1 {
		problems = append(problems, "WORKER_CONCURRENCY must be >= 1")
	}

	if len(problems) > 0 {
		return fmt.Errorf("invalid configuration: %s", strings.Join(problems, "; "))
	}
	return nil
}

func (c *Config) IsProduction() bool { return c.Env == "production" }

// --- helpers ---------------------------------------------------------------
//
// Malformed values fall back to the default rather than failing. The alternative
// is a service that will not boot because someone typed HTTP_PORT=808O; the
// defaults here are all safe, and validate() covers the ones that are not.

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
