package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/0xm4s0ud/cloud-management-plane/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type PoolRepo struct{ db *DB }

func NewPoolRepo(db *DB) *PoolRepo { return &PoolRepo{db: db} }

func (r *PoolRepo) List(ctx context.Context) ([]domain.ResourcePool, error) {
	rows, err := r.db.Q(ctx).Query(ctx, `
		SELECT id, kind, unit, total, oversubscribe_factor, dedicated_amount, reserved_amount, policy, created_at, updated_at
		FROM resource_pools ORDER BY kind`)
	if err != nil {
		return nil, fmt.Errorf("list pools: %w", err)
	}
	defer rows.Close()
	var out []domain.ResourcePool
	for rows.Next() {
		p, err := scanPool(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (r *PoolRepo) GetByKind(ctx context.Context, kind string) (*domain.ResourcePool, error) {
	row := r.db.Q(ctx).QueryRow(ctx, `
		SELECT id, kind, unit, total, oversubscribe_factor, dedicated_amount, reserved_amount, policy, created_at, updated_at
		FROM resource_pools WHERE kind = $1`, kind)
	p, err := scanPool(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get pool %s: %w", kind, err)
	}
	return &p, nil
}

// Conditional UPDATE, false when zero rows (insufficient capacity).
func (r *PoolRepo) TryReserve(ctx context.Context, poolID uuid.UUID, amount int64) (bool, error) {
	tag, err := r.db.Q(ctx).Exec(ctx, `
		UPDATE resource_pools
		SET reserved_amount = reserved_amount + $2, updated_at = now()
		WHERE id = $1
		  AND FLOOR(total * oversubscribe_factor) - dedicated_amount - reserved_amount >= $2`, poolID, amount)
	if err != nil {
		return false, fmt.Errorf("try reserve pool %s: %w", poolID, err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *PoolRepo) ReleaseReserved(ctx context.Context, poolID uuid.UUID, amount int64) error {
	_, err := r.db.Q(ctx).Exec(ctx, `
		UPDATE resource_pools
		SET reserved_amount = reserved_amount - $2, updated_at = now()
		WHERE id = $1 AND reserved_amount >= $2`, poolID, amount)
	if err != nil {
		return fmt.Errorf("release reserved pool %s: %w", poolID, err)
	}
	return nil
}

// Single UPDATE moves reserved -> dedicated.
func (r *PoolRepo) MoveReservedToDedicated(ctx context.Context, poolID uuid.UUID, amount int64) error {
	_, err := r.db.Q(ctx).Exec(ctx, `
		UPDATE resource_pools
		SET reserved_amount = reserved_amount - $2,
		    dedicated_amount = dedicated_amount + $2,
		    updated_at = now()
		WHERE id = $1 AND reserved_amount >= $2`, poolID, amount)
	if err != nil {
		return fmt.Errorf("move reserved to dedicated pool %s: %w", poolID, err)
	}
	return nil
}

type scannable interface {
	Scan(dest ...any) error
}

func scanPool(row scannable) (domain.ResourcePool, error) {
	var p domain.ResourcePool
	var policy []byte
	err := row.Scan(&p.ID, &p.Kind, &p.Unit, &p.Total, &p.OversubscribeFactor,
		&p.DedicatedAmount, &p.ReservedAmount, &policy, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return p, err
	}
	_ = json.Unmarshal(policy, &p.Policy)
	return p, nil
}
