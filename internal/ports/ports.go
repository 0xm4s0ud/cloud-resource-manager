package ports

import (
	"context"
	"time"

	"github.com/0xm4s0ud/cloud-management-plane/internal/domain"
	"github.com/google/uuid"
)

type Clock interface {
	Now() time.Time
}

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

type PoolRepository interface {
	List(ctx context.Context) ([]domain.ResourcePool, error)
	GetByKind(ctx context.Context, kind string) (*domain.ResourcePool, error)
	TryReserve(ctx context.Context, poolID uuid.UUID, amount int64) (ok bool, err error)
	ReleaseReserved(ctx context.Context, poolID uuid.UUID, amount int64) error
	MoveReservedToDedicated(ctx context.Context, poolID uuid.UUID, amount int64) error
}

type QuotaRepository interface {
	Get(ctx context.Context, customerID, kind string) (*domain.CustomerQuota, error)
	ListByCustomer(ctx context.Context, customerID string) ([]domain.CustomerQuota, error)
	TryConsume(ctx context.Context, customerID, kind string, amount int64) (ok bool, err error)
	Release(ctx context.Context, customerID, kind string, amount int64) error
	// Creates quota if missing then tries consume (demo convenience).
	EnsureAndTryConsume(ctx context.Context, customerID, kind string, amount, defaultLimit int64) (ok bool, err error)
}

type ProductRepository interface {
	List(ctx context.Context) ([]domain.Product, error)
	GetByCode(ctx context.Context, code string) (*domain.Product, error)
}

type RequestRepository interface {
	FindByIdempotency(ctx context.Context, customerID, key string) (*domain.CapacityRequest, error)
	Get(ctx context.Context, id uuid.UUID) (*domain.CapacityRequest, error)
	ListByCustomer(ctx context.Context, customerID string, status *domain.RequestStatus) ([]domain.CapacityRequest, error)
	CreateReserved(ctx context.Context, req *domain.CapacityRequest, lines []domain.RequestLine, holds []domain.Hold, ev domain.RequestEvent) error
	CreateRejected(ctx context.Context, req *domain.CapacityRequest, lines []domain.RequestLine, ev domain.RequestEvent) error
	Confirm(ctx context.Context, id uuid.UUID, dedications []domain.Dedication, ev domain.RequestEvent) (updated bool, err error)
	MarkCancelled(ctx context.Context, id uuid.UUID, ev domain.RequestEvent) (updated bool, err error)
	MarkExpired(ctx context.Context, id uuid.UUID, ev domain.RequestEvent) (updated bool, err error)
	Extend(ctx context.Context, id uuid.UUID, expiresAt time.Time, extensionCount int, ev domain.RequestEvent) (updated bool, err error)
	ListExpiredIDs(ctx context.Context, before time.Time, limit int) ([]uuid.UUID, error)
	ListEvents(ctx context.Context, requestID uuid.UUID) ([]domain.RequestEvent, error)
	ListLines(ctx context.Context, requestID uuid.UUID) ([]domain.RequestLine, error)
	ListActiveHolds(ctx context.Context, requestID uuid.UUID) ([]domain.Hold, error)
	UpdateHoldStatus(ctx context.Context, holdID uuid.UUID, from, to domain.HoldStatus) (bool, error)
	CountActiveHoldsByCustomer(ctx context.Context, customerID string) (int, error)
}

type TxRunner interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type AdvisoryLocker interface {
	TryLock(ctx context.Context, key int64) (bool, error)
	Unlock(ctx context.Context, key int64) error
}

type HealthChecker interface {
	Ping(ctx context.Context) error
}

type CacheStore interface {
	GetCapacity(ctx context.Context, key string) ([]byte, bool, error)
	SetCapacity(ctx context.Context, key string, value []byte, ttl time.Duration) error
	InvalidateCapacity(ctx context.Context, key string) error
	Ping(ctx context.Context) error
}

type IdempotencyStore interface {
	Get(ctx context.Context, customerID, key string) (requestID string, ok bool, err error)
	Set(ctx context.Context, customerID, key, requestID string, ttl time.Duration) error
}

type NoopCache struct{}

func (NoopCache) GetCapacity(context.Context, string) ([]byte, bool, error) { return nil, false, nil }
func (NoopCache) SetCapacity(context.Context, string, []byte, time.Duration) error {
	return nil
}
func (NoopCache) InvalidateCapacity(context.Context, string) error { return nil }
func (NoopCache) Ping(context.Context) error                       { return nil }

// Always misses; Postgres unique constraint is the source of truth.
type NoopIdempotency struct{}

func (NoopIdempotency) Get(context.Context, string, string) (string, bool, error) {
	return "", false, nil
}
func (NoopIdempotency) Set(context.Context, string, string, string, time.Duration) error {
	return nil
}

type NoopLocker struct{}

func (NoopLocker) TryLock(context.Context, int64) (bool, error) { return true, nil }
func (NoopLocker) Unlock(context.Context, int64) error          { return nil }
