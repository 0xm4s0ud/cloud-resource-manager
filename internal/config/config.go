package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr                  string
	MetricsAddr               string
	PostgresDSN               string
	RedisAddr                 string
	ReserveTTL                time.Duration
	MaxExtensions             int
	MaxReserveWallTime        time.Duration
	CapacityCacheTTL          time.Duration
	HTTPReadTimeout           time.Duration
	HTTPWriteTimeout          time.Duration
	SweeperInterval           time.Duration
	RetryAfterSeconds         int
	MigrationsPath            string
	MaxActiveHoldsPerCustomer int
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:                  getenv("HTTP_ADDR", ":8080"),
		MetricsAddr:               getenv("METRICS_ADDR", ""),
		PostgresDSN:               getenv("POSTGRES_DSN", "postgres://capacity:capacity@localhost:5432/capacity?sslmode=disable"),
		RedisAddr:                 getenv("REDIS_ADDR", "localhost:6379"),
		ReserveTTL:                durationEnv("RESERVE_TTL", 15*time.Minute),
		MaxExtensions:             intEnv("MAX_EXTENSIONS", 3),
		MaxReserveWallTime:        durationEnv("MAX_RESERVE_WALL_TIME", 60*time.Minute),
		CapacityCacheTTL:          durationEnv("CAPACITY_CACHE_TTL", 2*time.Second),
		HTTPReadTimeout:           durationEnv("HTTP_READ_TIMEOUT", 10*time.Second),
		HTTPWriteTimeout:          durationEnv("HTTP_WRITE_TIMEOUT", 10*time.Second),
		SweeperInterval:           durationEnv("SWEEPER_INTERVAL", 30*time.Second),
		RetryAfterSeconds:         intEnv("RETRY_AFTER_SECONDS", 5),
		MigrationsPath:            getenv("MIGRATIONS_PATH", "migrations"),
		MaxActiveHoldsPerCustomer: intEnv("MAX_ACTIVE_HOLDS_PER_CUSTOMER", 10),
	}
	if cfg.MaxExtensions < 0 {
		return cfg, fmt.Errorf("MAX_EXTENSIONS must be >= 0")
	}
	if cfg.MaxActiveHoldsPerCustomer < 0 {
		return cfg, fmt.Errorf("MAX_ACTIVE_HOLDS_PER_CUSTOMER must be >= 0")
	}
	return cfg, nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func intEnv(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func durationEnv(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
