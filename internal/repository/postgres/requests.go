package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/0xm4s0ud/cloud-management-plane/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type RequestRepo struct{ db *DB }

func NewRequestRepo(db *DB) *RequestRepo { return &RequestRepo{db: db} }

func (r *RequestRepo) FindByIdempotency(ctx context.Context, customerID, key string) (*domain.CapacityRequest, error) {
	row := r.db.Q(ctx).QueryRow(ctx, `
		SELECT id, customer_id, idempotency_key, body_hash, product_id, quantity, status, reject_reason,
		       expires_at, extension_count, correlation_id, created_at, updated_at
		FROM capacity_requests WHERE customer_id = $1 AND idempotency_key = $2`, customerID, key)
	req, err := scanRequest(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find by idempotency: %w", err)
	}
	return &req, nil
}

func (r *RequestRepo) Get(ctx context.Context, id uuid.UUID) (*domain.CapacityRequest, error) {
	row := r.db.Q(ctx).QueryRow(ctx, `
		SELECT id, customer_id, idempotency_key, body_hash, product_id, quantity, status, reject_reason,
		       expires_at, extension_count, correlation_id, created_at, updated_at
		FROM capacity_requests WHERE id = $1`, id)
	req, err := scanRequest(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get request: %w", err)
	}
	return &req, nil
}

func (r *RequestRepo) ListByCustomer(ctx context.Context, customerID string, status *domain.RequestStatus) ([]domain.CapacityRequest, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if status != nil {
		rows, err = r.db.Q(ctx).Query(ctx, `
			SELECT id, customer_id, idempotency_key, body_hash, product_id, quantity, status, reject_reason,
			       expires_at, extension_count, correlation_id, created_at, updated_at
			FROM capacity_requests WHERE customer_id = $1 AND status = $2
			ORDER BY created_at DESC`, customerID, string(*status))
	} else {
		rows, err = r.db.Q(ctx).Query(ctx, `
			SELECT id, customer_id, idempotency_key, body_hash, product_id, quantity, status, reject_reason,
			       expires_at, extension_count, correlation_id, created_at, updated_at
			FROM capacity_requests WHERE customer_id = $1
			ORDER BY created_at DESC`, customerID)
	}
	if err != nil {
		return nil, fmt.Errorf("list requests: %w", err)
	}
	defer rows.Close()
	var out []domain.CapacityRequest
	for rows.Next() {
		req, err := scanRequest(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, req)
	}
	return out, rows.Err()
}

func (r *RequestRepo) CreateReserved(ctx context.Context, req *domain.CapacityRequest, lines []domain.RequestLine, holds []domain.Hold, ev domain.RequestEvent) error {
	q := r.db.Q(ctx)
	_, err := q.Exec(ctx, `
		INSERT INTO capacity_requests (
			id, customer_id, idempotency_key, body_hash, product_id, quantity, status, expires_at,
			extension_count, correlation_id, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		req.ID, req.CustomerID, req.IdempotencyKey, req.BodyHash, req.ProductID, req.Quantity, string(req.Status),
		req.ExpiresAt, req.ExtensionCount, req.CorrelationID, req.CreatedAt, req.UpdatedAt)
	if err != nil {
		if IsUniqueViolation(err) {
			return fmt.Errorf("insert reserved request: %w", domain.ErrDuplicateRequest)
		}
		return fmt.Errorf("insert reserved request: %w", err)
	}
	if err := insertLines(ctx, q, lines); err != nil {
		return err
	}
	for _, h := range holds {
		_, err := q.Exec(ctx, `
			INSERT INTO holds (id, request_id, pool_id, amount, status, created_at, updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			h.ID, h.RequestID, h.PoolID, h.Amount, string(h.Status), h.CreatedAt, h.UpdatedAt)
		if err != nil {
			return fmt.Errorf("insert hold: %w", err)
		}
	}
	return insertEvent(ctx, q, ev)
}

func (r *RequestRepo) CreateRejected(ctx context.Context, req *domain.CapacityRequest, lines []domain.RequestLine, ev domain.RequestEvent) error {
	q := r.db.Q(ctx)
	var reason *string
	if req.RejectReason != nil {
		s := string(*req.RejectReason)
		reason = &s
	}
	_, err := q.Exec(ctx, `
		INSERT INTO capacity_requests (
			id, customer_id, idempotency_key, body_hash, product_id, quantity, status, reject_reason,
			extension_count, correlation_id, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		req.ID, req.CustomerID, req.IdempotencyKey, req.BodyHash, req.ProductID, req.Quantity, string(req.Status),
		reason, req.ExtensionCount, req.CorrelationID, req.CreatedAt, req.UpdatedAt)
	if err != nil {
		if IsUniqueViolation(err) {
			return fmt.Errorf("insert rejected request: %w", domain.ErrDuplicateRequest)
		}
		return fmt.Errorf("insert rejected request: %w", err)
	}
	if err := insertLines(ctx, q, lines); err != nil {
		return err
	}
	return insertEvent(ctx, q, ev)
}

func (r *RequestRepo) Confirm(ctx context.Context, id uuid.UUID, dedications []domain.Dedication, ev domain.RequestEvent) (bool, error) {
	q := r.db.Q(ctx)
	tag, err := q.Exec(ctx, `
		UPDATE capacity_requests
		SET status = 'COMPLETED', updated_at = now()
		WHERE id = $1 AND status = 'RESERVED'`, id)
	if err != nil {
		return false, fmt.Errorf("confirm request: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	for _, d := range dedications {
		_, err := q.Exec(ctx, `
			INSERT INTO dedications (id, request_id, pool_id, hold_id, amount, created_at)
			VALUES ($1,$2,$3,$4,$5,$6)`,
			d.ID, d.RequestID, d.PoolID, d.HoldID, d.Amount, d.CreatedAt)
		if err != nil {
			return false, fmt.Errorf("insert dedication: %w", err)
		}
	}
	if err := insertEvent(ctx, q, ev); err != nil {
		return false, err
	}
	return true, nil
}

func (r *RequestRepo) MarkCancelled(ctx context.Context, id uuid.UUID, ev domain.RequestEvent) (bool, error) {
	return r.markTerminal(ctx, id, domain.StatusCancelled, ev)
}

func (r *RequestRepo) MarkExpired(ctx context.Context, id uuid.UUID, ev domain.RequestEvent) (bool, error) {
	return r.markTerminal(ctx, id, domain.StatusExpired, ev)
}

func (r *RequestRepo) markTerminal(ctx context.Context, id uuid.UUID, status domain.RequestStatus, ev domain.RequestEvent) (bool, error) {
	q := r.db.Q(ctx)
	tag, err := q.Exec(ctx, `
		UPDATE capacity_requests
		SET status = $2, updated_at = now()
		WHERE id = $1 AND status = 'RESERVED'`, id, string(status))
	if err != nil {
		return false, fmt.Errorf("mark %s: %w", status, err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	if err := insertEvent(ctx, q, ev); err != nil {
		return false, err
	}
	return true, nil
}

func (r *RequestRepo) Extend(ctx context.Context, id uuid.UUID, expiresAt time.Time, extensionCount int, ev domain.RequestEvent) (bool, error) {
	q := r.db.Q(ctx)
	tag, err := q.Exec(ctx, `
		UPDATE capacity_requests
		SET expires_at = $2, extension_count = $3, updated_at = now()
		WHERE id = $1 AND status = 'RESERVED'`, id, expiresAt, extensionCount)
	if err != nil {
		return false, fmt.Errorf("extend request: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	if err := insertEvent(ctx, q, ev); err != nil {
		return false, err
	}
	return true, nil
}

func (r *RequestRepo) ListExpiredIDs(ctx context.Context, before time.Time, limit int) ([]uuid.UUID, error) {
	rows, err := r.db.Q(ctx).Query(ctx, `
		SELECT id FROM capacity_requests
		WHERE status = 'RESERVED' AND expires_at IS NOT NULL AND expires_at < $1
		ORDER BY expires_at
		LIMIT $2`, before, limit)
	if err != nil {
		return nil, fmt.Errorf("list expired: %w", err)
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *RequestRepo) ListEvents(ctx context.Context, requestID uuid.UUID) ([]domain.RequestEvent, error) {
	rows, err := r.db.Q(ctx).Query(ctx, `
		SELECT id, request_id, event_type, reason, at FROM request_events
		WHERE request_id = $1 ORDER BY at`, requestID)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()
	var out []domain.RequestEvent
	for rows.Next() {
		var e domain.RequestEvent
		var et string
		if err := rows.Scan(&e.ID, &e.RequestID, &et, &e.Reason, &e.At); err != nil {
			return nil, err
		}
		e.EventType = domain.EventType(et)
		out = append(out, e)
	}
	return out, rows.Err()
}

func (r *RequestRepo) ListLines(ctx context.Context, requestID uuid.UUID) ([]domain.RequestLine, error) {
	rows, err := r.db.Q(ctx).Query(ctx, `
		SELECT id, request_id, pool_id, kind, amount FROM request_lines WHERE request_id = $1`, requestID)
	if err != nil {
		return nil, fmt.Errorf("list lines: %w", err)
	}
	defer rows.Close()
	var out []domain.RequestLine
	for rows.Next() {
		var l domain.RequestLine
		if err := rows.Scan(&l.ID, &l.RequestID, &l.PoolID, &l.Kind, &l.Amount); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// Ordered by pool_id to match TryReserve lock ordering.
func (r *RequestRepo) ListActiveHolds(ctx context.Context, requestID uuid.UUID) ([]domain.Hold, error) {
	rows, err := r.db.Q(ctx).Query(ctx, `
		SELECT id, request_id, pool_id, amount, status, created_at, updated_at
		FROM holds WHERE request_id = $1 AND status = 'ACTIVE' ORDER BY pool_id`, requestID)
	if err != nil {
		return nil, fmt.Errorf("list holds: %w", err)
	}
	defer rows.Close()
	var out []domain.Hold
	for rows.Next() {
		var h domain.Hold
		var st string
		if err := rows.Scan(&h.ID, &h.RequestID, &h.PoolID, &h.Amount, &st, &h.CreatedAt, &h.UpdatedAt); err != nil {
			return nil, err
		}
		h.Status = domain.HoldStatus(st)
		out = append(out, h)
	}
	return out, rows.Err()
}

// False when another concurrent transition already won.
func (r *RequestRepo) UpdateHoldStatus(ctx context.Context, holdID uuid.UUID, from, to domain.HoldStatus) (bool, error) {
	tag, err := r.db.Q(ctx).Exec(ctx, `
		UPDATE holds SET status = $3, updated_at = now()
		WHERE id = $1 AND status = $2`, holdID, string(from), string(to))
	if err != nil {
		return false, fmt.Errorf("update hold status: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *RequestRepo) CountActiveHoldsByCustomer(ctx context.Context, customerID string) (int, error) {
	var n int
	err := r.db.Q(ctx).QueryRow(ctx, `
		SELECT COUNT(DISTINCT r.id)
		FROM capacity_requests r
		JOIN holds h ON h.request_id = r.id
		WHERE r.customer_id = $1
		  AND r.status = 'RESERVED'
		  AND h.status = 'ACTIVE'`, customerID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count active holds by customer: %w", err)
	}
	return n, nil
}

func insertLines(ctx context.Context, q Querier, lines []domain.RequestLine) error {
	for _, l := range lines {
		_, err := q.Exec(ctx, `
			INSERT INTO request_lines (id, request_id, pool_id, kind, amount)
			VALUES ($1,$2,$3,$4,$5)`, l.ID, l.RequestID, l.PoolID, l.Kind, l.Amount)
		if err != nil {
			return fmt.Errorf("insert line: %w", err)
		}
	}
	return nil
}

func insertEvent(ctx context.Context, q Querier, ev domain.RequestEvent) error {
	_, err := q.Exec(ctx, `
		INSERT INTO request_events (id, request_id, event_type, reason, at)
		VALUES ($1,$2,$3,$4,$5)`, ev.ID, ev.RequestID, string(ev.EventType), ev.Reason, ev.At)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

func scanRequest(row scannable) (domain.CapacityRequest, error) {
	var req domain.CapacityRequest
	var status string
	var reject *string
	err := row.Scan(&req.ID, &req.CustomerID, &req.IdempotencyKey, &req.BodyHash, &req.ProductID, &req.Quantity,
		&status, &reject, &req.ExpiresAt, &req.ExtensionCount, &req.CorrelationID, &req.CreatedAt, &req.UpdatedAt)
	if err != nil {
		return req, err
	}
	req.Status = domain.RequestStatus(status)
	if reject != nil {
		r := domain.Reason(*reject)
		req.RejectReason = &r
	}
	return req, nil
}

// Postgres unique_violation = 23505
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
