package fxmodules

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/0xm4s0ud/cloud-management-plane/internal/config"
	"github.com/0xm4s0ud/cloud-management-plane/internal/httpapi"
	"github.com/0xm4s0ud/cloud-management-plane/internal/observability"
	"github.com/0xm4s0ud/cloud-management-plane/internal/ports"
	"github.com/0xm4s0ud/cloud-management-plane/internal/repository/postgres"
	"github.com/0xm4s0ud/cloud-management-plane/internal/repository/redisx"
	"github.com/0xm4s0ud/cloud-management-plane/internal/service"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/fx"
)

var ConfigModule = fx.Module("config",
	fx.Provide(func() (config.Config, error) { return config.Load() }),
	fx.Provide(func() *slog.Logger {
		return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}),
)

var ObservabilityModule = fx.Module("observability",
	fx.Provide(observability.NewMetrics),
)

var RepositoryModule = fx.Module("repository",
	fx.Provide(
		newPostgres,
		func(db *postgres.DB) ports.PoolRepository { return postgres.NewPoolRepo(db) },
		func(db *postgres.DB) ports.QuotaRepository { return postgres.NewQuotaRepo(db) },
		func(db *postgres.DB) ports.ProductRepository { return postgres.NewProductRepo(db) },
		func(db *postgres.DB) ports.RequestRepository { return postgres.NewRequestRepo(db) },
		func(db *postgres.DB) ports.TxRunner { return db },
		func(db *postgres.DB) ports.AdvisoryLocker { return postgres.NewLocker(db) },
		func(db *postgres.DB) ports.HealthChecker { return db },
		newRedisStores,
		func() ports.Clock { return ports.SystemClock{} },
	),
	fx.Invoke(runMigrations),
)

type pgResult struct {
	fx.Out
	DB *postgres.DB
}

func newPostgres(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (pgResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := postgres.NewDB(ctx, cfg.PostgresDSN)
	if err != nil {
		return pgResult{}, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { db.Close(); return nil }})
	log.Info("postgres_connected")
	return pgResult{DB: db}, nil
}

func runMigrations(cfg config.Config, log *slog.Logger) error {
	path := cfg.MigrationsPath
	if path == "" {
		path = "migrations"
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if err := postgres.MigrateUp(cfg.PostgresDSN, abs); err != nil {
		return err
	}
	log.Info("migrations_applied", "path", abs)
	return nil
}

type redisResult struct {
	fx.Out

	Cache       ports.CacheStore
	Idempotency ports.IdempotencyStore
}

func newRedisStores(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) redisResult {
	client := redisx.New(cfg.RedisAddr)
	lc.Append(fx.Hook{OnStop: func(context.Context) error { return client.Close() }})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		log.Warn("redis_unavailable_degrading_to_noop", "addr", cfg.RedisAddr, "err", err)
		return redisResult{
			Cache:       ports.NoopCache{},
			Idempotency: ports.NoopIdempotency{},
		}
	}
	log.Info("redis_connected", "addr", cfg.RedisAddr)
	return redisResult{
		Cache:       redisx.NewDegradingCache(client),
		Idempotency: redisx.NewDegradingIdempotency(client),
	}
}

var ServiceModule = fx.Module("service",
	fx.Provide(service.NewCapacityService),
)

var HTTPModule = fx.Module("httpapi",
	fx.Provide(httpapi.NewHandler),
	fx.Invoke(registerHTTPServer),
)

type httpParams struct {
	fx.In
	Lifecycle fx.Lifecycle
	Cfg       config.Config
	Handler   *httpapi.Handler
	Metrics   *observability.Metrics
	Log       *slog.Logger
}

func registerHTTPServer(p httpParams) {
	p.Handler.SetDependencyGauge(func(name string, up bool) {
		v := 0.0
		if up {
			v = 1
		}
		p.Metrics.DependencyUp.WithLabelValues(name).Set(v)
	})

	var h http.Handler = p.Handler.Routes()
	h = observability.Middleware(p.Log, p.Metrics)(h)

	srv := &http.Server{
		Addr:         p.Cfg.HTTPAddr,
		Handler:      h,
		ReadTimeout:  p.Cfg.HTTPReadTimeout,
		WriteTimeout: p.Cfg.HTTPWriteTimeout,
	}

	p.Lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			p.Log.Info("http_listen", "addr", p.Cfg.HTTPAddr)
			go func() {
				if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					p.Log.Error("http_server", "err", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		},
	})
}

var SweeperModule = fx.Module("sweeper",
	fx.Invoke(registerSweeper),
)

type sweeperParams struct {
	fx.In
	Lifecycle fx.Lifecycle
	Cfg       config.Config
	Svc       *service.CapacityService
	Metrics   *observability.Metrics
	Log       *slog.Logger
}

func registerSweeper(p sweeperParams) {
	stop := make(chan struct{})
	startSweeperMetrics(p)
	p.Lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go runSweeperLoop(p, stop)
			p.Log.Info("sweeper_started", "interval", p.Cfg.SweeperInterval.String())
			return nil
		},
		OnStop: func(context.Context) error {
			close(stop)
			return nil
		},
	})
}

func startSweeperMetrics(p sweeperParams) {
	addr := p.Cfg.MetricsAddr
	if addr == "" {
		addr = ":8081"
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	p.Lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			p.Log.Info("sweeper_metrics_listen", "addr", addr)
			go func() {
				if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					p.Log.Error("sweeper_metrics", "err", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error { return srv.Shutdown(ctx) },
	})
}

func runSweeperLoop(p sweeperParams, stop <-chan struct{}) {
	t := time.NewTicker(p.Cfg.SweeperInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			n, err := p.Svc.ExpireDue(context.Background())
			if err != nil {
				p.Log.Warn("sweeper_expire", "err", err)
				continue
			}
			if n > 0 {
				p.Log.Info("sweeper_tick", "expired", n)
			}
		}
	}
}
