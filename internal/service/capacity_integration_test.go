package service_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0xm4s0ud/cloud-management-plane/internal/config"
	"github.com/0xm4s0ud/cloud-management-plane/internal/domain"
	"github.com/0xm4s0ud/cloud-management-plane/internal/observability"
	"github.com/0xm4s0ud/cloud-management-plane/internal/ports"
	"github.com/0xm4s0ud/cloud-management-plane/internal/repository/postgres"
	"github.com/0xm4s0ud/cloud-management-plane/internal/repository/redisx"
	"github.com/0xm4s0ud/cloud-management-plane/internal/service"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

func setupService(t *testing.T) (*service.CapacityService, *postgres.DB) {
	t.Helper()
	ctx := context.Background()

	pgC, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("capacity"),
		tcpostgres.WithUsername("capacity"),
		tcpostgres.WithPassword("capacity"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgC.Terminate(context.Background()) })

	dsn, err := pgC.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	for i := 0; i < 30; i++ {
		db, err := postgres.NewDB(ctx, dsn)
		if err == nil {
			db.Close()
			break
		}
		if i == 29 {
			require.NoError(t, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	migrations, err := filepath.Abs(filepath.Join("..", "..", "migrations"))
	require.NoError(t, err)
	require.NoError(t, postgres.MigrateUp(dsn, migrations))

	db, err := postgres.NewDB(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(db.Close)

	var cache ports.CacheStore = ports.NoopCache{}
	var idem ports.IdempotencyStore = ports.NoopIdempotency{}
	if redisC, err := tcredis.Run(ctx, "redis:7-alpine"); err == nil {
		t.Cleanup(func() { _ = redisC.Terminate(ctx) })
		addr, err := redisC.Endpoint(ctx, "")
		require.NoError(t, err)
		client := redisx.New(addr)
		t.Cleanup(func() { _ = client.Close() })
		cache = redisx.NewDegradingCache(client)
		idem = redisx.NewDegradingIdempotency(client)
	}

	cfg := config.Config{
		ReserveTTL:         2 * time.Minute,
		MaxExtensions:      3,
		MaxReserveWallTime: 60 * time.Minute,
		CapacityCacheTTL:   time.Second,
	}
	reg := prometheus.NewRegistry()
	metrics := observability.NewMetricsForTest(reg)
	svc := service.NewCapacityService(service.Params{
		Pools:    postgres.NewPoolRepo(db),
		Quotas:   postgres.NewQuotaRepo(db),
		Products: postgres.NewProductRepo(db),
		Requests: postgres.NewRequestRepo(db),
		Tx:       db,
		Lock:     postgres.NewLocker(db),
		Cache:    cache,
		Idem:     idem,
		Clock:    ports.SystemClock{},
		Cfg:      cfg,
		Metrics:  metrics,
		Log:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	})
	return svc, db
}

func TestReserveConfirmCancelIdempotency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	svc, _ := setupService(t)
	ctx := context.Background()

	key := uuid.NewString()
	req1, err := svc.CreateRequest(ctx, service.CreateInput{
		CustomerID: "cust-demo", IdempotencyKey: key, ProductCode: "vm-small", Quantity: 1,
	})
	require.NoError(t, err)
	require.Equal(t, domain.StatusReserved, req1.Status)

	req2, err := svc.CreateRequest(ctx, service.CreateInput{
		CustomerID: "cust-demo", IdempotencyKey: key, ProductCode: "vm-small", Quantity: 1,
	})
	require.NoError(t, err)
	require.Equal(t, req1.ID, req2.ID)

	confirmed, err := svc.Confirm(ctx, req1.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusCompleted, confirmed.Status)

	confirmed2, err := svc.Confirm(ctx, req1.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusCompleted, confirmed2.Status)
}

func TestExtendCapAndExpire(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	svc, db := setupService(t)
	ctx := context.Background()

	req, err := svc.CreateRequest(ctx, service.CreateInput{
		CustomerID: "cust-demo", IdempotencyKey: uuid.NewString(), ProductCode: "vm-small", Quantity: 1,
	})
	require.NoError(t, err)

	past := time.Now().UTC().Add(-time.Minute)
	_, err = db.Pool.Exec(ctx, `UPDATE capacity_requests SET expires_at = $1 WHERE id = $2`, past, req.ID)
	require.NoError(t, err)

	_, err = svc.Extend(ctx, req.ID)
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonExpired, ae.Reason)

	n, err := svc.ExpireDue(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)

	got, _, _, err := svc.GetRequest(ctx, req.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusExpired, got.Status)
}

func TestConcurrencyNeverOversells(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	svc, db := setupService(t)
	ctx := context.Background()

	// Shrink cpu pool to 10 for the race.
	_, err := db.Pool.Exec(ctx, `
		UPDATE resource_pools SET total = 10, dedicated_amount = 0, reserved_amount = 0 WHERE kind = 'cpu-cores'`)
	require.NoError(t, err)
	_, err = db.Pool.Exec(ctx, `
		UPDATE customer_quotas SET "limit" = 100, used_amount = 0 WHERE customer_id = 'cust-demo' AND resource_kind = 'cpu-cores'`)
	require.NoError(t, err)

	const workers = 40
	var reserved atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			req, err := svc.CreateRequest(ctx, service.CreateInput{
				CustomerID:     "cust-demo",
				IdempotencyKey: fmt.Sprintf("race-%d-%s", i, uuid.NewString()),
				Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 1}},
			})
			if err != nil {
				return
			}
			if req.Status == domain.StatusReserved {
				reserved.Add(1)
			}
		}(i)
	}
	wg.Wait()

	require.LessOrEqual(t, reserved.Load(), int64(10), "must not oversell capacity")

	var reservedAmt int64
	err = db.Pool.QueryRow(ctx, `SELECT reserved_amount FROM resource_pools WHERE kind = 'cpu-cores'`).Scan(&reservedAmt)
	require.NoError(t, err)
	require.Equal(t, reserved.Load(), reservedAmt)
}
