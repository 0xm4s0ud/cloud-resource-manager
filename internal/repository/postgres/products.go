package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/0xm4s0ud/cloud-management-plane/internal/domain"
	"github.com/jackc/pgx/v5"
)

type ProductRepo struct{ db *DB }

func NewProductRepo(db *DB) *ProductRepo { return &ProductRepo{db: db} }

func (r *ProductRepo) List(ctx context.Context) ([]domain.Product, error) {
	rows, err := r.db.Q(ctx).Query(ctx, `
		SELECT id, code, name, resource_template, created_at FROM products ORDER BY code`)
	if err != nil {
		return nil, fmt.Errorf("list products: %w", err)
	}
	defer rows.Close()
	var out []domain.Product
	for rows.Next() {
		p, err := scanProduct(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (r *ProductRepo) GetByCode(ctx context.Context, code string) (*domain.Product, error) {
	row := r.db.Q(ctx).QueryRow(ctx, `
		SELECT id, code, name, resource_template, created_at FROM products WHERE code = $1`, code)
	p, err := scanProduct(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get product %s: %w", code, err)
	}
	return &p, nil
}

func scanProduct(row scannable) (domain.Product, error) {
	var p domain.Product
	var tmpl []byte
	err := row.Scan(&p.ID, &p.Code, &p.Name, &tmpl, &p.CreatedAt)
	if err != nil {
		return p, err
	}
	p.ResourceTemplate = map[string]int64{}
	_ = json.Unmarshal(tmpl, &p.ResourceTemplate)
	return p, nil
}
