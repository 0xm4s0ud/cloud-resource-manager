package redisx

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb *redis.Client
}

func New(addr string) *Client {
	return &Client{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func (c *Client) Close() error { return c.rdb.Close() }

func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

type Cache struct{ c *Client }

func NewCache(c *Client) *Cache { return &Cache{c: c} }

func (a *Cache) GetCapacity(ctx context.Context, key string) ([]byte, bool, error) {
	val, err := a.c.rdb.Get(ctx, "capacity:"+key).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err // caller may treat as miss
	}
	return val, true, nil
}

func (a *Cache) SetCapacity(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return a.c.rdb.Set(ctx, "capacity:"+key, value, ttl).Err()
}

func (a *Cache) InvalidateCapacity(ctx context.Context, key string) error {
	return a.c.rdb.Del(ctx, "capacity:"+key).Err()
}

func (a *Cache) Ping(ctx context.Context) error { return a.c.Ping(ctx) }

type Idempotency struct{ c *Client }

func NewIdempotency(c *Client) *Idempotency { return &Idempotency{c: c} }

func (a *Idempotency) Get(ctx context.Context, customerID, key string) (string, bool, error) {
	val, err := a.c.rdb.Get(ctx, idemKey(customerID, key)).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return val, true, nil
}

func (a *Idempotency) Set(ctx context.Context, customerID, key, requestID string, ttl time.Duration) error {
	return a.c.rdb.Set(ctx, idemKey(customerID, key), requestID, ttl).Err()
}

func idemKey(customerID, key string) string {
	return fmt.Sprintf("idempotency:%s:%s", customerID, key)
}

type DegradingCache struct {
	inner *Cache
}

func NewDegradingCache(c *Client) *DegradingCache { return &DegradingCache{inner: NewCache(c)} }

func (d *DegradingCache) GetCapacity(ctx context.Context, key string) ([]byte, bool, error) {
	b, ok, err := d.inner.GetCapacity(ctx, key)
	if err != nil {
		return nil, false, nil
	}
	return b, ok, nil
}

func (d *DegradingCache) SetCapacity(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	_ = d.inner.SetCapacity(ctx, key, value, ttl)
	return nil
}

func (d *DegradingCache) InvalidateCapacity(ctx context.Context, key string) error {
	_ = d.inner.InvalidateCapacity(ctx, key)
	return nil
}

func (d *DegradingCache) Ping(ctx context.Context) error { return d.inner.Ping(ctx) }

type DegradingIdempotency struct {
	inner *Idempotency
}

func NewDegradingIdempotency(c *Client) *DegradingIdempotency {
	return &DegradingIdempotency{inner: NewIdempotency(c)}
}

func (d *DegradingIdempotency) Get(ctx context.Context, customerID, key string) (string, bool, error) {
	id, ok, err := d.inner.Get(ctx, customerID, key)
	if err != nil {
		return "", false, nil
	}
	return id, ok, nil
}

func (d *DegradingIdempotency) Set(ctx context.Context, customerID, key, requestID string, ttl time.Duration) error {
	_ = d.inner.Set(ctx, customerID, key, requestID, ttl)
	return nil
}
