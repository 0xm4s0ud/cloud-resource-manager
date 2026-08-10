package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ctxKey struct{}

type DB struct {
	Pool *pgxpool.Pool
}

type Querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func NewDB(ctx context.Context, dsn string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &DB{Pool: pool}, nil
}

func (d *DB) Close() { d.Pool.Close() }

func (d *DB) Ping(ctx context.Context) error { return d.Pool.Ping(ctx) }

// Repositories call Q(ctx) to join the in-flight transaction.
func (d *DB) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(context.WithValue(ctx, ctxKey{}, tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func (d *DB) Q(ctx context.Context) Querier {
	if tx, ok := ctx.Value(ctxKey{}).(pgx.Tx); ok && tx != nil {
		return tx
	}
	return d.Pool
}

func (d *DB) TryLock(ctx context.Context, key int64) (bool, error) {
	var ok bool
	err := d.Pool.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&ok)
	return ok, err
}

func (d *DB) Unlock(ctx context.Context, key int64) error {
	_, err := d.Pool.Exec(ctx, `SELECT pg_advisory_unlock($1)`, key)
	return err
}
