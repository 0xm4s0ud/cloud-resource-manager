package postgres

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Session-level advisory locks are tied to the backend session, so lock/unlock
// must use the same pinned pool connection for the lock's lifetime.
type Locker struct {
	db   *DB
	mu   sync.Mutex
	conn *pgxpool.Conn
	key  int64
}

func NewLocker(db *DB) *Locker { return &Locker{db: db} }

// Non-blocking; false means another sweeper holds the lock — skip this tick.
func (l *Locker) TryLock(ctx context.Context, key int64) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn != nil {
		return false, nil
	}
	conn, err := l.db.Pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire conn for advisory lock: %w", err)
	}
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&ok); err != nil {
		conn.Release()
		return false, fmt.Errorf("try advisory lock: %w", err)
	}
	if !ok {
		conn.Release()
		return false, nil
	}
	l.conn = conn
	l.key = key
	return true, nil
}

func (l *Locker) Unlock(ctx context.Context, key int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return nil
	}
	_, err := l.conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, key)
	l.conn.Release()
	l.conn = nil
	l.key = 0
	return err
}
