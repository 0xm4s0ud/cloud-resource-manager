package service_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/0xm4s0ud/cloud-management-plane/internal/config"
	"github.com/0xm4s0ud/cloud-management-plane/internal/domain"
	"github.com/0xm4s0ud/cloud-management-plane/internal/observability"
	"github.com/0xm4s0ud/cloud-management-plane/internal/ports"
	"github.com/0xm4s0ud/cloud-management-plane/internal/repository/memory"
	"github.com/0xm4s0ud/cloud-management-plane/internal/service"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

var epoch = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

func testPool(kind string, total int64) domain.ResourcePool {
	return domain.ResourcePool{
		ID:                  uuid.New(),
		Kind:                kind,
		Unit:                "units",
		Total:               total,
		OversubscribeFactor: 1.0,
		Policy:              domain.PoolPolicy{Splittable: true, MinGrain: 1},
	}
}

func testProduct(code string, cpuCores int64) domain.Product {
	return domain.Product{
		ID:               uuid.New(),
		Code:             code,
		ResourceTemplate: map[string]int64{"cpu-cores": cpuCores},
	}
}

func newTestService(
	t *testing.T,
	pools *memory.Pools,
	quotas *memory.Quotas,
	products *memory.Products,
	requests *memory.Requests,
	clk *memory.Clock,
) *service.CapacityService {
	t.Helper()
	reg := prometheus.NewRegistry()
	return service.NewCapacityService(service.Params{
		Pools:    pools,
		Quotas:   quotas,
		Products: products,
		Requests: requests,
		Tx:       memory.Tx{},
		Lock:     memory.Locker{},
		Cache:    ports.NoopCache{},
		Idem:     ports.NoopIdempotency{},
		Clock:    clk,
		Cfg: config.Config{
			ReserveTTL:                15 * time.Minute,
			MaxExtensions:             3,
			MaxReserveWallTime:        60 * time.Minute,
			CapacityCacheTTL:          time.Second,
			MaxActiveHoldsPerCustomer: 10,
		},
		Metrics: observability.NewMetricsForTest(reg),
		Log:     slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	})
}

func TestCreateRequest_ReserveSuccess(t *testing.T) {
	pool := testPool("cpu-cores", 10)
	pools := memory.NewPools(pool)
	clk := memory.NewClock(epoch)
	svc := newTestService(t, pools, memory.NewQuotas(), memory.NewProducts(), memory.NewRequests(), clk)

	req, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 3}},
	})
	require.NoError(t, err)
	require.Equal(t, domain.StatusReserved, req.Status)
	require.NotNil(t, req.ExpiresAt)
	require.Equal(t, int64(3), pools.ReservedAmount(pool.ID))
}

func TestCreateRequest_InsufficientCapacity(t *testing.T) {
	pool := testPool("cpu-cores", 5)
	svc := newTestService(t,
		memory.NewPools(pool),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)

	req, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 10}},
	})
	require.NoError(t, err)
	require.Equal(t, domain.StatusRejected, req.Status)
	require.NotNil(t, req.RejectReason)
	require.Equal(t, domain.ReasonInsufficientCapacity, *req.RejectReason)
}

func TestCreateRequest_QuotaExceeded(t *testing.T) {
	pool := testPool("cpu-cores", 100)
	quota := domain.CustomerQuota{
		ID:           uuid.New(),
		CustomerID:   "cust-q",
		ResourceKind: "cpu-cores",
		Limit:        2,
	}
	svc := newTestService(t,
		memory.NewPools(pool),
		memory.NewQuotas(quota),
		memory.NewProducts(),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)

	req, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-q",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 5}},
	})
	require.NoError(t, err)
	require.Equal(t, domain.StatusRejected, req.Status)
	require.Equal(t, domain.ReasonQuotaExceeded, *req.RejectReason)
}

func TestCreateRequest_InvalidResourceKind(t *testing.T) {
	svc := newTestService(t,
		memory.NewPools(),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)

	_, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "unknown-kind", Amount: 1}},
	})
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonInvalidResourceKind, ae.Reason)
}

func TestCreateRequest_ProductNotFound(t *testing.T) {
	svc := newTestService(t,
		memory.NewPools(),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)

	_, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		ProductCode:    "does-not-exist",
		Quantity:       1,
	})
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonProductNotFound, ae.Reason)
}

func TestCreateRequest_MissingIdempotencyKey(t *testing.T) {
	svc := newTestService(t,
		memory.NewPools(),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)

	_, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID: "cust-1",
		Lines:      []service.LineInput{{ResourceKind: "cpu-cores", Amount: 1}},
	})
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonMissingIdempotency, ae.Reason)
}

func TestCreateRequest_ProductBased(t *testing.T) {
	pool := testPool("cpu-cores", 100)
	pools := memory.NewPools(pool)
	product := testProduct("vm-small", 2)
	svc := newTestService(t,
		pools,
		memory.NewQuotas(),
		memory.NewProducts(product),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)

	req, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		ProductCode:    "vm-small",
		Quantity:       3,
	})
	require.NoError(t, err)
	require.Equal(t, domain.StatusReserved, req.Status)
	require.Equal(t, int64(6), pools.ReservedAmount(pool.ID))
}

func TestCreateRequest_IdempotentDuplicate(t *testing.T) {
	pool := testPool("cpu-cores", 10)
	pools := memory.NewPools(pool)
	svc := newTestService(t,
		pools,
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)

	in := service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 1}},
	}
	req1, err := svc.CreateRequest(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, domain.StatusReserved, req1.Status)

	req2, err := svc.CreateRequest(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, req1.ID, req2.ID)
	require.Equal(t, int64(1), pools.ReservedAmount(pool.ID))
}

func TestCreateRequest_IdempotencyKeyReusedDifferentBody(t *testing.T) {
	pool := testPool("cpu-cores", 10)
	svc := newTestService(t,
		memory.NewPools(pool),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)

	key := uuid.NewString()
	_, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: key,
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 1}},
	})
	require.NoError(t, err)

	_, err = svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: key,
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 2}},
	})
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonIdempotencyKeyReused, ae.Reason)
}

func TestCreateRequest_RejectsNegativeAmount(t *testing.T) {
	svc := newTestService(t,
		memory.NewPools(testPool("cpu-cores", 10)),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)
	_, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: -4}},
	})
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonInvalidInput, ae.Reason)
}

func TestCreateRequest_RejectsAmountAboveMax(t *testing.T) {
	svc := newTestService(t,
		memory.NewPools(testPool("cpu-cores", 10)),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)
	_, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: (1 << 53)}},
	})
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonInvalidInput, ae.Reason)
}

func TestCreateRequest_RejectsQuantityAboveMax(t *testing.T) {
	svc := newTestService(t,
		memory.NewPools(testPool("cpu-cores", 10_000)),
		memory.NewQuotas(),
		memory.NewProducts(testProduct("vm-small", 1)),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)
	_, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		ProductCode:    "vm-small",
		Quantity:       1001,
	})
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonInvalidInput, ae.Reason)
}

func TestCreateRequest_RejectsDuplicateResourceKinds(t *testing.T) {
	svc := newTestService(t,
		memory.NewPools(testPool("cpu-cores", 10)),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)
	_, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines: []service.LineInput{
			{ResourceKind: "cpu-cores", Amount: 1},
			{ResourceKind: "cpu-cores", Amount: 2},
		},
	})
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonInvalidInput, ae.Reason)
}

func TestCreateRequest_RejectsTooManyLines(t *testing.T) {
	svc := newTestService(t,
		memory.NewPools(testPool("cpu-cores", 10)),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)
	lines := make([]service.LineInput, 51)
	for i := range lines {
		lines[i] = service.LineInput{ResourceKind: fmt.Sprintf("kind-%d", i), Amount: 1}
	}
	_, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          lines,
	})
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonInvalidInput, ae.Reason)
}

func TestCreateRequest_TooManyActiveHolds(t *testing.T) {
	pool := testPool("cpu-cores", 100)
	pools := memory.NewPools(pool)
	quotas := memory.NewQuotas()
	products := memory.NewProducts()
	requests := memory.NewRequests()
	clk := memory.NewClock(epoch)

	reg := prometheus.NewRegistry()
	svc := service.NewCapacityService(service.Params{
		Pools:    pools,
		Quotas:   quotas,
		Products: products,
		Requests: requests,
		Tx:       memory.Tx{},
		Lock:     memory.Locker{},
		Cache:    ports.NoopCache{},
		Idem:     ports.NoopIdempotency{},
		Clock:    clk,
		Cfg: config.Config{
			ReserveTTL:                15 * time.Minute,
			MaxExtensions:             3,
			MaxReserveWallTime:        60 * time.Minute,
			CapacityCacheTTL:          time.Second,
			MaxActiveHoldsPerCustomer: 2,
		},
		Metrics: observability.NewMetricsForTest(reg),
		Log:     slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	})

	for i := 0; i < 2; i++ {
		_, err := svc.CreateRequest(context.Background(), service.CreateInput{
			CustomerID:     "cust-hold",
			IdempotencyKey: uuid.NewString(),
			Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 1}},
		})
		require.NoError(t, err)
	}

	_, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-hold",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 1}},
	})
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonTooManyActiveHolds, ae.Reason)
}

func TestConfirm_Success(t *testing.T) {
	pool := testPool("cpu-cores", 10)
	pools := memory.NewPools(pool)
	clk := memory.NewClock(epoch)
	requests := memory.NewRequests()
	svc := newTestService(t, pools, memory.NewQuotas(), memory.NewProducts(), requests, clk)

	req, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 4}},
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), pools.ReservedAmount(pool.ID))

	confirmed, err := svc.Confirm(context.Background(), req.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusCompleted, confirmed.Status)
	require.Equal(t, int64(0), pools.ReservedAmount(pool.ID))
}

func TestConfirm_IdempotentReconfirm(t *testing.T) {
	pool := testPool("cpu-cores", 10)
	svc := newTestService(t,
		memory.NewPools(pool),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)

	req, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 1}},
	})
	require.NoError(t, err)

	c1, err := svc.Confirm(context.Background(), req.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusCompleted, c1.Status)

	c2, err := svc.Confirm(context.Background(), req.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusCompleted, c2.Status)
}

func TestConfirm_AfterExpiry(t *testing.T) {
	pool := testPool("cpu-cores", 10)
	clk := memory.NewClock(epoch)
	svc := newTestService(t,
		memory.NewPools(pool),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		clk,
	)

	req, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 1}},
	})
	require.NoError(t, err)

	clk.Advance(20 * time.Minute)

	_, err = svc.Confirm(context.Background(), req.ID)
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonExpired, ae.Reason)
}

func TestConfirm_WrongState(t *testing.T) {
	pool := testPool("cpu-cores", 10)
	svc := newTestService(t,
		memory.NewPools(pool),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)

	req, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 1}},
	})
	require.NoError(t, err)

	_, err = svc.Cancel(context.Background(), req.ID)
	require.NoError(t, err)

	_, err = svc.Confirm(context.Background(), req.ID)
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonConflict, ae.Reason)
}

func TestCancel_ReleasesCounters(t *testing.T) {
	pool := testPool("cpu-cores", 10)
	pools := memory.NewPools(pool)
	svc := newTestService(t, pools, memory.NewQuotas(), memory.NewProducts(), memory.NewRequests(), memory.NewClock(epoch))

	req, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 5}},
	})
	require.NoError(t, err)
	require.Equal(t, int64(5), pools.ReservedAmount(pool.ID))

	_, err = svc.Cancel(context.Background(), req.ID)
	require.NoError(t, err)
	require.Equal(t, int64(0), pools.ReservedAmount(pool.ID))
}

func TestCancel_WrongState(t *testing.T) {
	pool := testPool("cpu-cores", 10)
	svc := newTestService(t,
		memory.NewPools(pool),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		memory.NewClock(epoch),
	)

	req, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 1}},
	})
	require.NoError(t, err)

	_, err = svc.Confirm(context.Background(), req.ID)
	require.NoError(t, err)

	_, err = svc.Cancel(context.Background(), req.ID)
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonConflict, ae.Reason)
}

func TestExtend_Success(t *testing.T) {
	pool := testPool("cpu-cores", 10)
	clk := memory.NewClock(epoch)
	svc := newTestService(t,
		memory.NewPools(pool),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		clk,
	)

	req, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 1}},
	})
	require.NoError(t, err)
	originalExpiry := *req.ExpiresAt

	extended, err := svc.Extend(context.Background(), req.ID)
	require.NoError(t, err)
	require.Equal(t, 1, extended.ExtensionCount)
	require.True(t, extended.ExpiresAt.After(originalExpiry))
}

func TestExtend_CapReached(t *testing.T) {
	pool := testPool("cpu-cores", 10)
	clk := memory.NewClock(epoch)
	svc := newTestService(t,
		memory.NewPools(pool),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		clk,
	)

	req, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 1}},
	})
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err = svc.Extend(context.Background(), req.ID)
		require.NoError(t, err)
	}
	_, err = svc.Extend(context.Background(), req.ID)
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonExtensionLimit, ae.Reason)
}

func TestExtend_AfterExpiry(t *testing.T) {
	pool := testPool("cpu-cores", 10)
	clk := memory.NewClock(epoch)
	svc := newTestService(t,
		memory.NewPools(pool),
		memory.NewQuotas(),
		memory.NewProducts(),
		memory.NewRequests(),
		clk,
	)

	req, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 1}},
	})
	require.NoError(t, err)

	clk.Advance(20 * time.Minute) // past 15 min TTL

	_, err = svc.Extend(context.Background(), req.ID)
	require.Error(t, err)
	ae, ok := domain.AsAppError(err)
	require.True(t, ok)
	require.Equal(t, domain.ReasonExpired, ae.Reason)
}

func TestExpireDue_ExpiresOverdueRequests(t *testing.T) {
	pool := testPool("cpu-cores", 10)
	pools := memory.NewPools(pool)
	clk := memory.NewClock(epoch)
	svc := newTestService(t, pools, memory.NewQuotas(), memory.NewProducts(), memory.NewRequests(), clk)

	req, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 3}},
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), pools.ReservedAmount(pool.ID))

	clk.Advance(20 * time.Minute)

	n, err := svc.ExpireDue(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, n)

	got, _, _, err := svc.GetRequest(context.Background(), req.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusExpired, got.Status)
	require.Equal(t, int64(0), pools.ReservedAmount(pool.ID))
}

func TestExpireDue_SkipsWhenLockNotAcquired(t *testing.T) {
	pool := testPool("cpu-cores", 10)
	clk := memory.NewClock(epoch)

	reg := prometheus.NewRegistry()
	svc := service.NewCapacityService(service.Params{
		Pools:    memory.NewPools(pool),
		Quotas:   memory.NewQuotas(),
		Products: memory.NewProducts(),
		Requests: memory.NewRequests(),
		Tx:       memory.Tx{},
		Lock:     memory.LockerThatFails{},
		Cache:    ports.NoopCache{},
		Idem:     ports.NoopIdempotency{},
		Clock:    clk,
		Cfg: config.Config{
			ReserveTTL:         15 * time.Minute,
			MaxExtensions:      3,
			MaxReserveWallTime: 60 * time.Minute,
		},
		Metrics: observability.NewMetricsForTest(reg),
		Log:     slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})

	_, err := svc.CreateRequest(context.Background(), service.CreateInput{
		CustomerID:     "cust-1",
		IdempotencyKey: uuid.NewString(),
		Lines:          []service.LineInput{{ResourceKind: "cpu-cores", Amount: 1}},
	})
	require.NoError(t, err)
	clk.Advance(20 * time.Minute)

	n, err := svc.ExpireDue(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, n) // lock skipped → nothing expired
}
