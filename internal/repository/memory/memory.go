// In-process test fakes with the same capacity/quota math as Postgres.
// Not safe for production.
package memory

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/0xm4s0ud/cloud-management-plane/internal/domain"
	"github.com/google/uuid"
)

type Clock struct {
	mu  sync.Mutex
	now time.Time
}

func NewClock(t time.Time) *Clock { return &Clock{now: t} }

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *Clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type Pools struct {
	mu    sync.Mutex
	pools map[string]*domain.ResourcePool // keyed by Kind
}

func NewPools(ps ...domain.ResourcePool) *Pools {
	m := &Pools{pools: make(map[string]*domain.ResourcePool, len(ps))}
	for i := range ps {
		cp := ps[i]
		m.pools[cp.Kind] = &cp
	}
	return m
}

func (p *Pools) List(_ context.Context) ([]domain.ResourcePool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]domain.ResourcePool, 0, len(p.pools))
	for _, v := range p.pools {
		out = append(out, *v)
	}
	return out, nil
}

func (p *Pools) GetByKind(_ context.Context, kind string) (*domain.ResourcePool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if v, ok := p.pools[kind]; ok {
		cp := *v
		return &cp, nil
	}
	return nil, nil
}

func (p *Pools) TryReserve(_ context.Context, poolID uuid.UUID, amount int64) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pool := p.byID(poolID)
	if pool == nil {
		return false, fmt.Errorf("pool %s not found", poolID)
	}
	effective := int64(math.Floor(float64(pool.Total) * pool.OversubscribeFactor))
	avail := effective - pool.DedicatedAmount - pool.ReservedAmount
	if avail < amount {
		return false, nil
	}
	pool.ReservedAmount += amount
	return true, nil
}

func (p *Pools) ReleaseReserved(_ context.Context, poolID uuid.UUID, amount int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	pool := p.byID(poolID)
	if pool == nil {
		return fmt.Errorf("pool %s not found", poolID)
	}
	if pool.ReservedAmount >= amount {
		pool.ReservedAmount -= amount
	}
	return nil
}

func (p *Pools) MoveReservedToDedicated(_ context.Context, poolID uuid.UUID, amount int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	pool := p.byID(poolID)
	if pool == nil {
		return fmt.Errorf("pool %s not found", poolID)
	}
	if pool.ReservedAmount >= amount {
		pool.ReservedAmount -= amount
		pool.DedicatedAmount += amount
	}
	return nil
}

func (p *Pools) ReservedAmount(poolID uuid.UUID) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pool := p.byID(poolID); pool != nil {
		return pool.ReservedAmount
	}
	return 0
}

func (p *Pools) byID(id uuid.UUID) *domain.ResourcePool {
	for _, v := range p.pools {
		if v.ID == id {
			return v
		}
	}
	return nil
}

type quotaKey struct{ customerID, kind string }

type Quotas struct {
	mu     sync.Mutex
	quotas map[quotaKey]*domain.CustomerQuota
}

func NewQuotas(qs ...domain.CustomerQuota) *Quotas {
	m := &Quotas{quotas: make(map[quotaKey]*domain.CustomerQuota, len(qs))}
	for i := range qs {
		cp := qs[i]
		m.quotas[quotaKey{cp.CustomerID, cp.ResourceKind}] = &cp
	}
	return m
}

func (q *Quotas) Get(_ context.Context, customerID, kind string) (*domain.CustomerQuota, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if v, ok := q.quotas[quotaKey{customerID, kind}]; ok {
		cp := *v
		return &cp, nil
	}
	return nil, nil
}

func (q *Quotas) ListByCustomer(_ context.Context, customerID string) ([]domain.CustomerQuota, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []domain.CustomerQuota
	for k, v := range q.quotas {
		if k.customerID == customerID {
			out = append(out, *v)
		}
	}
	return out, nil
}

func (q *Quotas) TryConsume(_ context.Context, customerID, kind string, amount int64) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	v := q.quotas[quotaKey{customerID, kind}]
	if v == nil {
		return false, nil
	}
	if v.Limit-v.UsedAmount < amount {
		return false, nil
	}
	v.UsedAmount += amount
	return true, nil
}

func (q *Quotas) EnsureAndTryConsume(_ context.Context, customerID, kind string, amount, defaultLimit int64) (bool, error) {
	q.mu.Lock()
	key := quotaKey{customerID, kind}
	if _, ok := q.quotas[key]; !ok {
		q.quotas[key] = &domain.CustomerQuota{
			ID:           uuid.New(),
			CustomerID:   customerID,
			ResourceKind: kind,
			Limit:        defaultLimit,
		}
	}
	v := q.quotas[key]
	if v.Limit-v.UsedAmount < amount {
		q.mu.Unlock()
		return false, nil
	}
	v.UsedAmount += amount
	q.mu.Unlock()
	return true, nil
}

func (q *Quotas) Release(_ context.Context, customerID, kind string, amount int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if v, ok := q.quotas[quotaKey{customerID, kind}]; ok {
		if v.UsedAmount >= amount {
			v.UsedAmount -= amount
		} else {
			v.UsedAmount = 0
		}
	}
	return nil
}

type Products struct {
	mu       sync.Mutex
	products map[string]*domain.Product // keyed by Code
}

func NewProducts(ps ...domain.Product) *Products {
	m := &Products{products: make(map[string]*domain.Product, len(ps))}
	for i := range ps {
		cp := ps[i]
		m.products[cp.Code] = &cp
	}
	return m
}

func (p *Products) List(_ context.Context) ([]domain.Product, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]domain.Product, 0, len(p.products))
	for _, v := range p.products {
		out = append(out, *v)
	}
	return out, nil
}

func (p *Products) GetByCode(_ context.Context, code string) (*domain.Product, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if v, ok := p.products[code]; ok {
		cp := *v
		return &cp, nil
	}
	return nil, nil
}

type Requests struct {
	mu          sync.Mutex
	requests    map[uuid.UUID]*domain.CapacityRequest
	lines       map[uuid.UUID][]domain.RequestLine  // key: requestID
	holds       map[uuid.UUID][]domain.Hold         // key: requestID
	dedications map[uuid.UUID][]domain.Dedication   // key: requestID
	events      map[uuid.UUID][]domain.RequestEvent // key: requestID
	idemIndex   map[string]uuid.UUID                // "customerID:key" → requestID
}

func NewRequests() *Requests {
	return &Requests{
		requests:    make(map[uuid.UUID]*domain.CapacityRequest),
		lines:       make(map[uuid.UUID][]domain.RequestLine),
		holds:       make(map[uuid.UUID][]domain.Hold),
		dedications: make(map[uuid.UUID][]domain.Dedication),
		events:      make(map[uuid.UUID][]domain.RequestEvent),
		idemIndex:   make(map[string]uuid.UUID),
	}
}

func (r *Requests) idemKey(customerID, key string) string {
	return customerID + ":" + key
}

func (r *Requests) FindByIdempotency(_ context.Context, customerID, key string) (*domain.CapacityRequest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id, ok := r.idemIndex[r.idemKey(customerID, key)]; ok {
		if req, ok2 := r.requests[id]; ok2 {
			cp := *req
			return &cp, nil
		}
	}
	return nil, nil
}

func (r *Requests) Get(_ context.Context, id uuid.UUID) (*domain.CapacityRequest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if req, ok := r.requests[id]; ok {
		cp := *req
		return &cp, nil
	}
	return nil, nil
}

func (r *Requests) ListByCustomer(_ context.Context, customerID string, status *domain.RequestStatus) ([]domain.CapacityRequest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []domain.CapacityRequest
	for _, req := range r.requests {
		if req.CustomerID != customerID {
			continue
		}
		if status != nil && req.Status != *status {
			continue
		}
		out = append(out, *req)
	}
	return out, nil
}

func (r *Requests) CreateReserved(_ context.Context, req *domain.CapacityRequest, lines []domain.RequestLine, holds []domain.Hold, ev domain.RequestEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	ik := r.idemKey(req.CustomerID, req.IdempotencyKey)
	if _, exists := r.idemIndex[ik]; exists {
		return fmt.Errorf("create reserved: %w", domain.ErrDuplicateRequest)
	}
	cp := *req
	r.requests[req.ID] = &cp
	r.idemIndex[ik] = req.ID
	r.lines[req.ID] = append(r.lines[req.ID], lines...)
	r.holds[req.ID] = append(r.holds[req.ID], holds...)
	r.events[req.ID] = append(r.events[req.ID], ev)
	return nil
}

func (r *Requests) CreateRejected(_ context.Context, req *domain.CapacityRequest, lines []domain.RequestLine, ev domain.RequestEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	ik := r.idemKey(req.CustomerID, req.IdempotencyKey)
	if _, exists := r.idemIndex[ik]; exists {
		return fmt.Errorf("create rejected: %w", domain.ErrDuplicateRequest)
	}
	cp := *req
	r.requests[req.ID] = &cp
	r.idemIndex[ik] = req.ID
	r.lines[req.ID] = append(r.lines[req.ID], lines...)
	r.events[req.ID] = append(r.events[req.ID], ev)
	return nil
}

func (r *Requests) Confirm(_ context.Context, id uuid.UUID, dedications []domain.Dedication, ev domain.RequestEvent) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	req := r.requests[id]
	if req == nil || req.Status != domain.StatusReserved {
		return false, nil
	}
	req.Status = domain.StatusCompleted
	r.dedications[id] = append(r.dedications[id], dedications...)
	r.events[id] = append(r.events[id], ev)
	return true, nil
}

func (r *Requests) MarkCancelled(_ context.Context, id uuid.UUID, ev domain.RequestEvent) (bool, error) {
	return r.markTerminal(id, domain.StatusCancelled, ev)
}

func (r *Requests) MarkExpired(_ context.Context, id uuid.UUID, ev domain.RequestEvent) (bool, error) {
	return r.markTerminal(id, domain.StatusExpired, ev)
}

func (r *Requests) markTerminal(id uuid.UUID, status domain.RequestStatus, ev domain.RequestEvent) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	req := r.requests[id]
	if req == nil || req.Status != domain.StatusReserved {
		return false, nil
	}
	req.Status = status
	r.events[id] = append(r.events[id], ev)
	return true, nil
}

func (r *Requests) Extend(_ context.Context, id uuid.UUID, expiresAt time.Time, extensionCount int, ev domain.RequestEvent) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	req := r.requests[id]
	if req == nil || req.Status != domain.StatusReserved {
		return false, nil
	}
	req.ExpiresAt = &expiresAt
	req.ExtensionCount = extensionCount
	r.events[id] = append(r.events[id], ev)
	return true, nil
}

func (r *Requests) ListExpiredIDs(_ context.Context, before time.Time, limit int) ([]uuid.UUID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []uuid.UUID
	for _, req := range r.requests {
		if req.Status == domain.StatusReserved && req.ExpiresAt != nil && req.ExpiresAt.Before(before) {
			out = append(out, req.ID)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (r *Requests) ListEvents(_ context.Context, requestID uuid.UUID) ([]domain.RequestEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]domain.RequestEvent(nil), r.events[requestID]...), nil
}

func (r *Requests) ListLines(_ context.Context, requestID uuid.UUID) ([]domain.RequestLine, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]domain.RequestLine(nil), r.lines[requestID]...), nil
}

func (r *Requests) ListActiveHolds(_ context.Context, requestID uuid.UUID) ([]domain.Hold, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []domain.Hold
	for _, h := range r.holds[requestID] {
		if h.Status == domain.HoldActive {
			out = append(out, h)
		}
	}
	return out, nil
}

func (r *Requests) UpdateHoldStatus(_ context.Context, holdID uuid.UUID, from, to domain.HoldStatus) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for reqID := range r.holds {
		for i := range r.holds[reqID] {
			if r.holds[reqID][i].ID == holdID {
				if r.holds[reqID][i].Status != from {
					return false, nil
				}
				r.holds[reqID][i].Status = to
				return true, nil
			}
		}
	}
	return false, nil
}

func (r *Requests) CountActiveHoldsByCustomer(_ context.Context, customerID string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for id, req := range r.requests {
		if req.CustomerID != customerID || req.Status != domain.StatusReserved {
			continue
		}
		for _, h := range r.holds[id] {
			if h.Status == domain.HoldActive {
				n++
				break
			}
		}
	}
	return n, nil
}

type Tx struct{}

func (Tx) WithinTx(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }

// Always grants; use LockerThatFails for the lock-skipped path.
type Locker struct{}

func (Locker) TryLock(context.Context, int64) (bool, error) { return true, nil }

func (Locker) Unlock(context.Context, int64) error { return nil }

type LockerThatFails struct{}

func (LockerThatFails) TryLock(context.Context, int64) (bool, error) { return false, nil }

func (LockerThatFails) Unlock(context.Context, int64) error { return nil }
