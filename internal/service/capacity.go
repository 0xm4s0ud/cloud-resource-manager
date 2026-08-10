package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/0xm4s0ud/cloud-management-plane/internal/config"
	"github.com/0xm4s0ud/cloud-management-plane/internal/domain"
	"github.com/0xm4s0ud/cloud-management-plane/internal/observability"
	"github.com/0xm4s0ud/cloud-management-plane/internal/ports"
	"github.com/google/uuid"
	"go.uber.org/fx"
)

var errRejected = errors.New("capacity rejected")

const (
	defaultQuotaLimit  = int64(10_000)
	sweeperLockKey     = int64(84201501)
	expireBatchSize    = 100
	capacityCacheKey   = "all"
	maxLinesPerRequest = 50
	maxQuantity        = 1000
	maxAmount          = int64(1<<53 - 1) // JSON/JS safe integer bound
)

// Sorted by pool ID to prevent deadlocks on concurrent multi-resource reserves.
type reservationPlan struct {
	kind   string
	poolID uuid.UUID
	amount int64
}

type rejection struct {
	reason  domain.Reason
	message string
	details any
}

type Params struct {
	fx.In

	Pools    ports.PoolRepository
	Quotas   ports.QuotaRepository
	Products ports.ProductRepository
	Requests ports.RequestRepository
	Tx       ports.TxRunner
	Lock     ports.AdvisoryLocker
	Cache    ports.CacheStore
	Idem     ports.IdempotencyStore
	Clock    ports.Clock
	Cfg      config.Config
	Metrics  *observability.Metrics
	Log      *slog.Logger
}

type CapacityService struct {
	pools    ports.PoolRepository
	quotas   ports.QuotaRepository
	products ports.ProductRepository
	requests ports.RequestRepository
	tx       ports.TxRunner
	lock     ports.AdvisoryLocker
	cache    ports.CacheStore
	idem     ports.IdempotencyStore
	clock    ports.Clock
	cfg      config.Config
	metrics  *observability.Metrics
	log      *slog.Logger
}

func NewCapacityService(p Params) *CapacityService {
	return &CapacityService{
		pools:    p.Pools,
		quotas:   p.Quotas,
		products: p.Products,
		requests: p.Requests,
		tx:       p.Tx,
		lock:     p.Lock,
		cache:    p.Cache,
		idem:     p.Idem,
		clock:    p.Clock,
		cfg:      p.Cfg,
		metrics:  p.Metrics,
		log:      p.Log,
	}
}

type CreateInput struct {
	CustomerID     string
	IdempotencyKey string
	ProductCode    string
	Quantity       int
	Lines          []LineInput
	CorrelationID  string
}

type LineInput struct {
	ResourceKind string `json:"resource_kind"`
	Amount       int64  `json:"amount"`
}

type CapacityView struct {
	Pool              domain.ResourcePool `json:"pool"`
	Available         int64               `json:"available"`
	CustomerRemaining *int64              `json:"customer_remaining,omitempty"`
}

func (s *CapacityService) ListCapacity(ctx context.Context, customerID string) ([]CapacityView, error) {
	cacheKey := capacityCacheKey
	if customerID != "" {
		cacheKey = "cust:" + customerID
	}
	if b, ok, _ := s.cache.GetCapacity(ctx, cacheKey); ok {
		var views []CapacityView
		if json.Unmarshal(b, &views) == nil {
			return views, nil
		}
	}

	pools, err := s.pools.List(ctx)
	if err != nil {
		return nil, err
	}

	var quotasByKind map[string]domain.CustomerQuota
	if customerID != "" {
		qs, err := s.quotas.ListByCustomer(ctx, customerID)
		if err != nil {
			return nil, err
		}
		quotasByKind = make(map[string]domain.CustomerQuota, len(qs))
		for _, q := range qs {
			quotasByKind[q.ResourceKind] = q
		}
	}

	views := make([]CapacityView, 0, len(pools))
	for _, p := range pools {
		v := CapacityView{Pool: p, Available: p.Available()}
		if q, ok := quotasByKind[p.Kind]; ok {
			rem := q.Remaining()
			v.CustomerRemaining = &rem
			if rem < v.Available {
				v.Available = rem
			}
		}
		views = append(views, v)
		s.metrics.PoolAvailableRatio.WithLabelValues(p.Kind).Set(
			domain.AvailableRatio(p.Total, p.DedicatedAmount, p.ReservedAmount, p.OversubscribeFactor),
		)
	}

	if b, err := json.Marshal(views); err == nil {
		_ = s.cache.SetCapacity(ctx, cacheKey, b, s.cfg.CapacityCacheTTL)
	}
	return views, nil
}

func (s *CapacityService) ListProducts(ctx context.Context) ([]domain.Product, error) {
	return s.products.List(ctx)
}

func (s *CapacityService) CreateRequest(ctx context.Context, in CreateInput) (*domain.CapacityRequest, error) {
	if in.IdempotencyKey == "" {
		return nil, domain.NewAppError(domain.ReasonMissingIdempotency, "Idempotency-Key is required", nil)
	}
	if in.CustomerID == "" {
		return nil, domain.NewAppError(domain.ReasonConflict, "customer_id is required", nil)
	}
	if err := validateCreateInput(in); err != nil {
		return nil, err
	}
	if in.CorrelationID == "" {
		in.CorrelationID = uuid.NewString()
	}

	bodyHash := computeBodyHash(in)
	if existing, err := s.checkIdempotentReplay(ctx, in, bodyHash); existing != nil || err != nil {
		return existing, err
	}

	if err := s.enforceActiveHoldsLimit(ctx, in.CustomerID); err != nil {
		return nil, err
	}

	plans, product, err := s.buildReservationPlan(ctx, in)
	if err != nil {
		return nil, err
	}

	now := s.clock.Now()
	expires := now.Add(s.cfg.ReserveTTL)
	reqID := uuid.New()

	reserved, rej, err := s.reserveWithinTx(ctx, plans, product, in, reqID, now, expires, bodyHash)
	if err != nil {
		if errors.Is(err, domain.ErrDuplicateRequest) {
			return s.replayAfterDuplicate(ctx, in, bodyHash)
		}
		return nil, err
	}

	if rej != nil {
		req, err := s.persistRejected(ctx, in, reqID, now, plans, product, rej, bodyHash)
		if err != nil {
			return nil, err
		}
		s.recordOutcome(ctx, in, req, "rejected")
		return req, nil
	}

	s.recordOutcome(ctx, in, reserved, "reserved")
	return reserved, nil
}

func (s *CapacityService) enforceActiveHoldsLimit(ctx context.Context, customerID string) error {
	limit := s.cfg.MaxActiveHoldsPerCustomer
	if limit <= 0 {
		return nil
	}
	n, err := s.requests.CountActiveHoldsByCustomer(ctx, customerID)
	if err != nil {
		return err
	}
	if n >= limit {
		return domain.NewAppError(domain.ReasonTooManyActiveHolds,
			"too many active holds for customer", map[string]any{
				"customer_id": customerID,
				"active":      n,
				"limit":       limit,
			})
	}
	return nil
}

func (s *CapacityService) replayAfterDuplicate(ctx context.Context, in CreateInput, bodyHash string) (*domain.CapacityRequest, error) {
	existing, err := s.requests.FindByIdempotency(ctx, in.CustomerID, in.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, domain.NewAppError(domain.ReasonConflict, "duplicate key race but request not found", nil)
	}
	if existing.BodyHash != "" && existing.BodyHash != bodyHash {
		return nil, domain.NewAppError(domain.ReasonIdempotencyKeyReused,
			"idempotency key reused with different body", map[string]any{
				"request_id": existing.ID.String(),
			})
	}
	return existing, nil
}

func (s *CapacityService) checkIdempotentReplay(ctx context.Context, in CreateInput, bodyHash string) (*domain.CapacityRequest, error) {
	if id, ok, _ := s.idem.Get(ctx, in.CustomerID, in.IdempotencyKey); ok {
		if uid, err := uuid.Parse(id); err == nil {
			existing, err := s.requests.Get(ctx, uid)
			if err != nil {
				return nil, err
			}
			if existing != nil {
				return s.matchIdempotentBody(existing, bodyHash)
			}
		}
	}
	existing, err := s.requests.FindByIdempotency(ctx, in.CustomerID, in.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		matched, err := s.matchIdempotentBody(existing, bodyHash)
		if err != nil {
			return nil, err
		}
		_ = s.idem.Set(ctx, in.CustomerID, in.IdempotencyKey, existing.ID.String(), s.cfg.ReserveTTL*2)
		return matched, nil
	}
	return nil, nil
}

func (s *CapacityService) matchIdempotentBody(existing *domain.CapacityRequest, bodyHash string) (*domain.CapacityRequest, error) {
	if existing.BodyHash != "" && existing.BodyHash != bodyHash {
		return nil, domain.NewAppError(domain.ReasonIdempotencyKeyReused,
			"idempotency key reused with different body", map[string]any{
				"request_id": existing.ID.String(),
			})
	}
	return existing, nil
}

func (s *CapacityService) buildReservationPlan(ctx context.Context, in CreateInput) ([]reservationPlan, *domain.Product, error) {
	resolved, product, err := s.resolveLines(ctx, in)
	if err != nil {
		return nil, nil, err
	}

	plans := make([]reservationPlan, 0, len(resolved))
	for kind, amount := range resolved {
		// TODO: performs multiple queries in db, can we read once?
		pool, err := s.pools.GetByKind(ctx, kind)
		if err != nil {
			return nil, nil, err
		}
		if pool == nil {
			return nil, nil, domain.NewAppError(domain.ReasonInvalidResourceKind,
				"unknown resource kind", map[string]any{"kind": kind})
		}
		if pool.Policy.MinGrain > 0 && amount%pool.Policy.MinGrain != 0 {
			return nil, nil, domain.NewAppError(domain.ReasonConflict,
				"amount not aligned to min grain", map[string]any{
					"kind": kind, "amount": amount, "min_grain": pool.Policy.MinGrain,
				})
		}
		plans = append(plans, reservationPlan{kind: kind, poolID: pool.ID, amount: amount})
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].poolID.String() < plans[j].poolID.String() })
	return plans, product, nil
}

func (s *CapacityService) reserveWithinTx(
	ctx context.Context,
	plans []reservationPlan,
	product *domain.Product,
	in CreateInput,
	reqID uuid.UUID,
	now, expires time.Time,
	bodyHash string,
) (*domain.CapacityRequest, *rejection, error) {
	var (
		result *domain.CapacityRequest
		rej    *rejection
	)

	err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		for _, p := range plans {
			ok, err := s.pools.TryReserve(ctx, p.poolID, p.amount)
			if err != nil {
				return err
			}
			if !ok {
				rej = &rejection{
					reason:  domain.ReasonInsufficientCapacity,
					message: "insufficient capacity",
					details: map[string]any{"pool": p.kind, "requested": p.amount},
				}
				return errRejected
			}

			ok, err = s.quotas.EnsureAndTryConsume(ctx, in.CustomerID, p.kind, p.amount, defaultQuotaLimit)
			if err != nil {
				return err
			}
			if !ok {
				rej = &rejection{
					reason:  domain.ReasonQuotaExceeded,
					message: "quota exceeded",
					details: map[string]any{"resource_kind": p.kind, "requested": p.amount},
				}
				return errRejected
			}
		}

		var productID *uuid.UUID
		if product != nil {
			productID = &product.ID
		}
		req := &domain.CapacityRequest{
			ID:             reqID,
			CustomerID:     in.CustomerID,
			IdempotencyKey: in.IdempotencyKey,
			BodyHash:       bodyHash,
			ProductID:      productID,
			Quantity:       resolvedQuantity(in, product),
			Status:         domain.StatusReserved,
			ExpiresAt:      &expires,
			ExtensionCount: 0,
			CorrelationID:  in.CorrelationID,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		lines := buildLines(reqID, plans)
		holds := buildHolds(reqID, plans, now)
		ev := domain.RequestEvent{
			ID:        uuid.New(),
			RequestID: reqID,
			EventType: domain.EventReserved,
			At:        now,
		}
		if err := s.requests.CreateReserved(ctx, req, lines, holds, ev); err != nil {
			return err
		}
		result = req
		return nil
	})

	if errors.Is(err, errRejected) {
		return nil, rej, nil
	}
	if err != nil {
		return nil, nil, err
	}
	return result, nil, nil
}

// Persists REJECTED outside the rolled-back reserve transaction.
func (s *CapacityService) persistRejected(
	ctx context.Context,
	in CreateInput,
	reqID uuid.UUID,
	now time.Time,
	plans []reservationPlan,
	product *domain.Product,
	rej *rejection,
	bodyHash string,
) (*domain.CapacityRequest, error) {
	reason := rej.reason
	req := &domain.CapacityRequest{
		ID:             reqID,
		CustomerID:     in.CustomerID,
		IdempotencyKey: in.IdempotencyKey,
		BodyHash:       bodyHash,
		Status:         domain.StatusRejected,
		RejectReason:   &reason,
		Quantity:       resolvedQuantity(in, product),
		CorrelationID:  in.CorrelationID,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if product != nil {
		req.ProductID = &product.ID
	}

	eventReason := rej.message
	if rej.details != nil {
		if b, err := json.Marshal(rej.details); err == nil {
			eventReason = fmt.Sprintf("%s: %s", rej.message, b)
		}
	}

	lines := buildLines(reqID, plans)
	ev := domain.RequestEvent{
		ID:        uuid.New(),
		RequestID: reqID,
		EventType: domain.EventRejected,
		Reason:    eventReason,
		At:        now,
	}

	if err := s.requests.CreateRejected(ctx, req, lines, ev); err != nil {
		if errors.Is(err, domain.ErrDuplicateRequest) {
			return s.replayAfterDuplicate(ctx, in, bodyHash)
		}
		return nil, err
	}
	return req, nil
}

func (s *CapacityService) recordOutcome(ctx context.Context, in CreateInput, req *domain.CapacityRequest, result string) {
	s.metrics.ReservationAttempts.WithLabelValues(result).Inc()
	if result == "rejected" {
		if req.RejectReason != nil {
			s.metrics.ReservationRejects.WithLabelValues(string(*req.RejectReason)).Inc()
		}
	}
	_ = s.idem.Set(ctx, in.CustomerID, in.IdempotencyKey, req.ID.String(), s.cfg.ReserveTTL*2)
	_ = s.cache.InvalidateCapacity(ctx, capacityCacheKey)
	_ = s.cache.InvalidateCapacity(ctx, "cust:"+in.CustomerID)
}

func (s *CapacityService) resolveLines(ctx context.Context, in CreateInput) (map[string]int64, *domain.Product, error) {
	out := map[string]int64{}
	if in.ProductCode != "" {
		p, err := s.products.GetByCode(ctx, in.ProductCode)
		if err != nil {
			return nil, nil, err
		}
		if p == nil {
			return nil, nil, domain.NewAppError(domain.ReasonProductNotFound,
				"product not found", map[string]any{"product": in.ProductCode})
		}
		qty := in.Quantity
		if qty <= 0 {
			qty = 1
		}
		for k, v := range p.ResourceTemplate {
			amt := v * int64(qty)
			if amt <= 0 || amt > maxAmount {
				return nil, nil, domain.NewAppError(domain.ReasonInvalidInput,
					"resolved line amount out of bounds", map[string]any{
						"kind": k, "amount": amt, "max": maxAmount,
					})
			}
			out[k] += amt
		}
		return out, p, nil
	}
	if len(in.Lines) == 0 {
		return nil, nil, domain.NewAppError(domain.ReasonConflict, "product_id or lines required", nil)
	}
	for _, l := range in.Lines {
		out[l.ResourceKind] += l.Amount
	}
	return out, nil, nil
}

func validateCreateInput(in CreateInput) error {
	if in.ProductCode != "" {
		if in.Quantity < 0 || in.Quantity > maxQuantity {
			return domain.NewAppError(domain.ReasonInvalidInput,
				"quantity out of bounds", map[string]any{
					"quantity": in.Quantity, "max": maxQuantity,
				})
		}
		if len(in.Lines) > 0 {
			return domain.NewAppError(domain.ReasonInvalidInput,
				"provide product_id or lines, not both", nil)
		}
		return nil
	}
	if len(in.Lines) == 0 {
		return nil
	}
	if len(in.Lines) > maxLinesPerRequest {
		return domain.NewAppError(domain.ReasonInvalidInput,
			"too many resource lines", map[string]any{
				"lines": len(in.Lines), "max": maxLinesPerRequest,
			})
	}
	seen := make(map[string]struct{}, len(in.Lines))
	for _, l := range in.Lines {
		if l.ResourceKind == "" {
			return domain.NewAppError(domain.ReasonInvalidInput, "resource_kind is required", nil)
		}
		if _, dup := seen[l.ResourceKind]; dup {
			return domain.NewAppError(domain.ReasonInvalidInput,
				"duplicate resource_kind in lines", map[string]any{"kind": l.ResourceKind})
		}
		seen[l.ResourceKind] = struct{}{}
		if l.Amount <= 0 || l.Amount > maxAmount {
			return domain.NewAppError(domain.ReasonInvalidInput,
				"line amount out of bounds", map[string]any{
					"kind": l.ResourceKind, "amount": l.Amount, "max": maxAmount,
				})
		}
	}
	return nil
}

type bodyFingerprint struct {
	ProductCode string   `json:"product_code,omitempty"`
	Quantity    int      `json:"quantity,omitempty"`
	Lines       []fpLine `json:"lines,omitempty"`
}

type fpLine struct {
	ResourceKind string `json:"resource_kind"`
	Amount       int64  `json:"amount"`
}

func computeBodyHash(in CreateInput) string {
	fp := bodyFingerprint{ProductCode: in.ProductCode}
	if in.ProductCode != "" {
		qty := in.Quantity
		if qty <= 0 {
			qty = 1
		}
		fp.Quantity = qty
	} else {
		lines := make([]fpLine, len(in.Lines))
		for i, l := range in.Lines {
			lines[i] = fpLine{ResourceKind: l.ResourceKind, Amount: l.Amount}
		}
		sort.Slice(lines, func(i, j int) bool {
			if lines[i].ResourceKind == lines[j].ResourceKind {
				return lines[i].Amount < lines[j].Amount
			}
			return lines[i].ResourceKind < lines[j].ResourceKind
		})
		fp.Lines = lines
	}
	b, err := json.Marshal(fp)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (s *CapacityService) GetRequest(ctx context.Context, id uuid.UUID) (*domain.CapacityRequest, []domain.RequestLine, []domain.RequestEvent, error) {
	req, err := s.requests.Get(ctx, id)
	if err != nil {
		return nil, nil, nil, err
	}
	if req == nil {
		return nil, nil, nil, domain.NewAppError(domain.ReasonNotFound, "request not found", nil)
	}
	lines, err := s.requests.ListLines(ctx, id)
	if err != nil {
		return nil, nil, nil, err
	}
	events, err := s.requests.ListEvents(ctx, id)
	if err != nil {
		return nil, nil, nil, err
	}
	return req, lines, events, nil
}

func (s *CapacityService) ListRequests(ctx context.Context, customerID string, status *domain.RequestStatus) ([]domain.CapacityRequest, error) {
	return s.requests.ListByCustomer(ctx, customerID, status)
}

func (s *CapacityService) Confirm(ctx context.Context, id uuid.UUID) (*domain.CapacityRequest, error) {
	req, err := s.requests.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, domain.NewAppError(domain.ReasonNotFound, "request not found", nil)
	}
	if req.Status == domain.StatusCompleted {
		return req, nil
	}
	if req.Status != domain.StatusReserved {
		return nil, domain.NewAppError(domain.ReasonConflict,
			"request is not in RESERVED state", map[string]any{"status": req.Status})
	}
	now := s.clock.Now()
	if req.ExpiresAt != nil && !req.ExpiresAt.After(now) {
		return nil, domain.NewAppError(domain.ReasonExpired, "reservation expired", nil)
	}

	err = s.tx.WithinTx(ctx, func(ctx context.Context) error {
		holds, err := s.requests.ListActiveHolds(ctx, id)
		if err != nil {
			return err
		}
		dedications := make([]domain.Dedication, 0, len(holds))
		for _, h := range holds {
			if err := s.pools.MoveReservedToDedicated(ctx, h.PoolID, h.Amount); err != nil {
				return err
			}
			ok, err := s.requests.UpdateHoldStatus(ctx, h.ID, domain.HoldActive, domain.HoldDedicated)
			if err != nil {
				return err
			}
			if !ok {
				return domain.NewAppError(domain.ReasonConflict, "hold already transitioned", nil)
			}
			dedications = append(dedications, domain.Dedication{
				ID:        uuid.New(),
				RequestID: id,
				PoolID:    h.PoolID,
				HoldID:    h.ID,
				Amount:    h.Amount,
				CreatedAt: now,
			})
		}
		ev := domain.RequestEvent{ID: uuid.New(), RequestID: id, EventType: domain.EventConfirmed, At: now}
		updated, err := s.requests.Confirm(ctx, id, dedications, ev)
		if err != nil {
			return err
		}
		if !updated {
			return domain.NewAppError(domain.ReasonConflict, "confirm race lost", nil)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	product := "raw"
	if req.ProductID != nil {
		product = req.ProductID.String()
	}
	s.metrics.ReservationConfirms.WithLabelValues(product).Inc()
	_ = s.cache.InvalidateCapacity(ctx, capacityCacheKey)
	return s.requests.Get(ctx, id)
}

func (s *CapacityService) Cancel(ctx context.Context, id uuid.UUID) (*domain.CapacityRequest, error) {
	req, err := s.requests.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, domain.NewAppError(domain.ReasonNotFound, "request not found", nil)
	}
	if req.Status == domain.StatusCancelled {
		return req, nil
	}
	if req.Status != domain.StatusReserved {
		return nil, domain.NewAppError(domain.ReasonConflict,
			"request cannot be cancelled", map[string]any{"status": req.Status})
	}
	if err := s.releaseReserved(ctx, req, domain.StatusCancelled); err != nil {
		return nil, err
	}
	s.metrics.ReservationCancels.Inc()
	_ = s.cache.InvalidateCapacity(ctx, capacityCacheKey)
	return s.requests.Get(ctx, id)
}

func (s *CapacityService) Extend(ctx context.Context, id uuid.UUID) (*domain.CapacityRequest, error) {
	req, err := s.requests.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, domain.NewAppError(domain.ReasonNotFound, "request not found", nil)
	}
	now := s.clock.Now()
	if req.Status != domain.StatusReserved {
		if req.Status == domain.StatusExpired {
			return nil, domain.NewAppError(domain.ReasonExpired, "reservation expired", nil)
		}
		return nil, domain.NewAppError(domain.ReasonConflict,
			"request cannot be extended", map[string]any{"status": req.Status})
	}
	if req.ExpiresAt != nil && !req.ExpiresAt.After(now) {
		return nil, domain.NewAppError(domain.ReasonExpired, "reservation expired", nil)
	}
	if req.ExtensionCount >= s.cfg.MaxExtensions {
		return nil, domain.NewAppError(domain.ReasonExtensionLimit,
			"max extensions reached", map[string]any{"max": s.cfg.MaxExtensions})
	}
	base := now
	if req.ExpiresAt != nil && req.ExpiresAt.After(now) {
		base = *req.ExpiresAt
	}
	newExp := base.Add(s.cfg.ReserveTTL)
	if req.CreatedAt.Add(s.cfg.MaxReserveWallTime).Before(newExp) {
		return nil, domain.NewAppError(domain.ReasonExtensionLimit, "max reserve wall time reached", nil)
	}
	ev := domain.RequestEvent{ID: uuid.New(), RequestID: id, EventType: domain.EventExtended, At: now}
	updated, err := s.requests.Extend(ctx, id, newExp, req.ExtensionCount+1, ev)
	if err != nil {
		return nil, err
	}
	if !updated {
		return nil, domain.NewAppError(domain.ReasonConflict, "extend race lost", nil)
	}
	s.metrics.ReservationExtends.Inc()
	return s.requests.Get(ctx, id)
}

// Uses a Postgres advisory lock so only one sweeper instance acts per tick.
func (s *CapacityService) ExpireDue(ctx context.Context) (int, error) {
	ok, err := s.lock.TryLock(ctx, sweeperLockKey)
	if err != nil {
		return 0, err
	}
	if !ok {
		s.metrics.SweeperRuns.WithLabelValues("false").Inc()
		return 0, nil
	}
	defer func() { _ = s.lock.Unlock(ctx, sweeperLockKey) }()
	s.metrics.SweeperRuns.WithLabelValues("true").Inc()

	now := s.clock.Now()
	ids, err := s.requests.ListExpiredIDs(ctx, now, expireBatchSize)
	if err != nil {
		return 0, err
	}

	n := 0
	for _, id := range ids {
		req, err := s.requests.Get(ctx, id)
		if err != nil {
			s.log.Warn("sweeper: get request failed", "id", id, "err", err)
			continue
		}
		if req == nil {
			continue
		}
		if err := s.releaseReserved(ctx, req, domain.StatusExpired); err != nil {
			s.log.Warn("sweeper: expire request failed", "id", id, "err", err)
			continue
		}
		n++
		s.metrics.ReservationExpires.WithLabelValues("sweeper").Inc()
		s.metrics.SweeperExpiredHolds.Inc()
	}
	if n > 0 {
		s.log.Info("sweeper_recovered", "expired_requests", n)
		_ = s.cache.InvalidateCapacity(ctx, capacityCacheKey)
	}
	return n, nil
}

func (s *CapacityService) releaseReserved(ctx context.Context, req *domain.CapacityRequest, terminal domain.RequestStatus) error {
	now := s.clock.Now()
	return s.tx.WithinTx(ctx, func(ctx context.Context) error {
		holds, err := s.requests.ListActiveHolds(ctx, req.ID)
		if err != nil {
			return err
		}
		lines, err := s.requests.ListLines(ctx, req.ID)
		if err != nil {
			return err
		}

		kindByPool := make(map[uuid.UUID]string, len(lines))
		for _, l := range lines {
			kindByPool[l.PoolID] = l.Kind
		}

		holdTo := domain.HoldReleased
		if terminal == domain.StatusExpired {
			holdTo = domain.HoldExpired
		}
		for _, h := range holds {
			if err := s.pools.ReleaseReserved(ctx, h.PoolID, h.Amount); err != nil {
				return err
			}
			if kind := kindByPool[h.PoolID]; kind != "" {
				if err := s.quotas.Release(ctx, req.CustomerID, kind, h.Amount); err != nil {
					return err
				}
			}
			if _, err := s.requests.UpdateHoldStatus(ctx, h.ID, domain.HoldActive, holdTo); err != nil {
				return err
			}
		}

		evType := domain.EventCancelled
		if terminal == domain.StatusExpired {
			evType = domain.EventExpired
		}
		ev := domain.RequestEvent{
			ID:        uuid.New(),
			RequestID: req.ID,
			EventType: evType,
			Reason:    string(terminal),
			At:        now,
		}

		var updated bool
		if terminal == domain.StatusExpired {
			updated, err = s.requests.MarkExpired(ctx, req.ID, ev)
		} else {
			updated, err = s.requests.MarkCancelled(ctx, req.ID, ev)
		}
		if err != nil {
			return err
		}

		_ = updated
		return nil
	})
}

func buildLines(reqID uuid.UUID, plans []reservationPlan) []domain.RequestLine {
	lines := make([]domain.RequestLine, 0, len(plans))
	for _, p := range plans {
		lines = append(lines, domain.RequestLine{
			ID:        uuid.New(),
			RequestID: reqID,
			PoolID:    p.poolID,
			Kind:      p.kind,
			Amount:    p.amount,
		})
	}
	return lines
}

func buildHolds(reqID uuid.UUID, plans []reservationPlan, now time.Time) []domain.Hold {
	holds := make([]domain.Hold, 0, len(plans))
	for _, p := range plans {
		holds = append(holds, domain.Hold{
			ID:        uuid.New(),
			RequestID: reqID,
			PoolID:    p.poolID,
			Amount:    p.amount,
			Status:    domain.HoldActive,
			CreatedAt: now,
			UpdatedAt: now,
		})
	}
	return holds
}

func resolvedQuantity(in CreateInput, product *domain.Product) int {
	// TODO: why passed product in?
	if in.Quantity > 0 {
		return in.Quantity
	}
	return 1
}
