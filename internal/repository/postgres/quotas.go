package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/0xm4s0ud/cloud-management-plane/internal/domain"
	"github.com/jackc/pgx/v5"
)

type QuotaRepo struct{ db *DB }

func NewQuotaRepo(db *DB) *QuotaRepo { return &QuotaRepo{db: db} }

func (r *QuotaRepo) Get(ctx context.Context, customerID, kind string) (*domain.CustomerQuota, error) {
	row := r.db.Q(ctx).QueryRow(ctx, `
		SELECT id, customer_id, resource_kind, "limit", used_amount, created_at, updated_at
		FROM customer_quotas WHERE customer_id = $1 AND resource_kind = $2`, customerID, kind)
	q, err := scanQuota(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get quota: %w", err)
	}
	return &q, nil
}

func (r *QuotaRepo) ListByCustomer(ctx context.Context, customerID string) ([]domain.CustomerQuota, error) {
	rows, err := r.db.Q(ctx).Query(ctx, `
		SELECT id, customer_id, resource_kind, "limit", used_amount, created_at, updated_at
		FROM customer_quotas WHERE customer_id = $1 ORDER BY resource_kind`, customerID)
	if err != nil {
		return nil, fmt.Errorf("list quotas: %w", err)
	}
	defer rows.Close()
	var out []domain.CustomerQuota
	for rows.Next() {
		q, err := scanQuota(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

func (r *QuotaRepo) TryConsume(ctx context.Context, customerID, kind string, amount int64) (bool, error) {
	tag, err := r.db.Q(ctx).Exec(ctx, `
		UPDATE customer_quotas
		SET used_amount = used_amount + $3, updated_at = now()
		WHERE customer_id = $1 AND resource_kind = $2
		  AND "limit" - used_amount >= $3`, customerID, kind, amount)
	if err != nil {
		return false, fmt.Errorf("try consume quota: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *QuotaRepo) EnsureAndTryConsume(ctx context.Context, customerID, kind string, amount, defaultLimit int64) (bool, error) {
	_, err := r.db.Q(ctx).Exec(ctx, `
		INSERT INTO customer_quotas (customer_id, resource_kind, "limit", used_amount)
		VALUES ($1, $2, $3, 0)
		ON CONFLICT (customer_id, resource_kind) DO NOTHING`, customerID, kind, defaultLimit)
	if err != nil {
		return false, fmt.Errorf("ensure quota: %w", err)
	}
	return r.TryConsume(ctx, customerID, kind, amount)
}

func (r *QuotaRepo) Release(ctx context.Context, customerID, kind string, amount int64) error {
	_, err := r.db.Q(ctx).Exec(ctx, `
		UPDATE customer_quotas
		SET used_amount = GREATEST(used_amount - $3, 0), updated_at = now()
		WHERE customer_id = $1 AND resource_kind = $2`, customerID, kind, amount)
	if err != nil {
		return fmt.Errorf("release quota: %w", err)
	}
	return nil
}

func scanQuota(row scannable) (domain.CustomerQuota, error) {
	var q domain.CustomerQuota
	err := row.Scan(&q.ID, &q.CustomerID, &q.ResourceKind, &q.Limit, &q.UsedAmount, &q.CreatedAt, &q.UpdatedAt)
	return q, err
}
