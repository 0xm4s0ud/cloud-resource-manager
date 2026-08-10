package domain

import (
	"time"

	"github.com/google/uuid"
)

// PENDING is never persisted as a resting state.
type RequestStatus string

const (
	StatusReserved  RequestStatus = "RESERVED"
	StatusRejected  RequestStatus = "REJECTED"
	StatusCompleted RequestStatus = "COMPLETED"
	StatusCancelled RequestStatus = "CANCELLED"
	StatusExpired   RequestStatus = "EXPIRED"
)

type HoldStatus string

const (
	HoldActive    HoldStatus = "ACTIVE"
	HoldReleased  HoldStatus = "RELEASED"
	HoldDedicated HoldStatus = "DEDICATED"
	HoldExpired   HoldStatus = "EXPIRED"
)

type EventType string

const (
	EventCreated   EventType = "created"
	EventReserved  EventType = "reserved"
	EventRejected  EventType = "rejected"
	EventConfirmed EventType = "confirmed"
	EventCancelled EventType = "cancelled"
	EventExpired   EventType = "expired"
	EventExtended  EventType = "extended"
)

type Reason string

const (
	ReasonInsufficientCapacity Reason = "INSUFFICIENT_CAPACITY"
	ReasonQuotaExceeded        Reason = "QUOTA_EXCEEDED"
	ReasonInvalidResourceKind  Reason = "INVALID_RESOURCE_KIND"
	ReasonProductNotFound      Reason = "PRODUCT_NOT_FOUND"
	ReasonMissingIdempotency   Reason = "MISSING_IDEMPOTENCY_KEY"
	ReasonExpired              Reason = "EXPIRED"
	ReasonExtensionLimit       Reason = "EXTENSION_LIMIT"
	ReasonNotFound             Reason = "NOT_FOUND"
	ReasonConflict             Reason = "CONFLICT"
	ReasonUnavailable          Reason = "UNAVAILABLE"
	ReasonNotImplemented       Reason = "NOT_IMPLEMENTED"
	ReasonIdempotencyKeyReused Reason = "IDEMPOTENCY_KEY_REUSED"
	ReasonTooManyActiveHolds   Reason = "TOO_MANY_ACTIVE_HOLDS"
	ReasonInvalidInput         Reason = "INVALID_INPUT"
)

type PoolPolicy struct {
	Splittable bool  `json:"splittable"`
	MinGrain   int64 `json:"min_grain"`
	MaxGrain   int64 `json:"max_grain,omitempty"`
}

type ResourcePool struct {
	ID                  uuid.UUID  `json:"id"`
	Kind                string     `json:"kind"`
	Unit                string     `json:"unit"`
	Total               int64      `json:"total"`
	OversubscribeFactor float64    `json:"oversubscribe_factor"`
	DedicatedAmount     int64      `json:"dedicated_amount"`
	ReservedAmount      int64      `json:"reserved_amount"`
	Policy              PoolPolicy `json:"policy"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
}

// Available returns display capacity, reserve path must re-check atomically.
func (p ResourcePool) Available() int64 {
	effective := int64(float64(p.Total) * p.OversubscribeFactor)
	avail := effective - p.DedicatedAmount - p.ReservedAmount
	if avail < 0 {
		return 0
	}
	return avail
}

type Product struct {
	ID               uuid.UUID        `json:"id"`
	Code             string           `json:"code"`
	Name             string           `json:"name"`
	ResourceTemplate map[string]int64 `json:"resource_template"`
	CreatedAt        time.Time        `json:"created_at"`
}

type CustomerQuota struct {
	ID           uuid.UUID `json:"id"`
	CustomerID   string    `json:"customer_id"`
	ResourceKind string    `json:"resource_kind"`
	Limit        int64     `json:"limit"`
	UsedAmount   int64     `json:"used_amount"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (q CustomerQuota) Remaining() int64 {
	r := q.Limit - q.UsedAmount
	if r < 0 {
		return 0
	}
	return r
}

type CapacityRequest struct {
	ID             uuid.UUID     `json:"id"`
	CustomerID     string        `json:"customer_id"`
	IdempotencyKey string        `json:"idempotency_key"`
	BodyHash       string        `json:"body_hash,omitempty"`
	ProductID      *uuid.UUID    `json:"product_id,omitempty"`
	Quantity       int           `json:"quantity"`
	Status         RequestStatus `json:"status"`
	RejectReason   *Reason       `json:"reject_reason,omitempty"`
	ExpiresAt      *time.Time    `json:"expires_at,omitempty"`
	ExtensionCount int           `json:"extension_count"`
	CorrelationID  string        `json:"correlation_id"`
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
}

type RequestLine struct {
	ID        uuid.UUID `json:"id"`
	RequestID uuid.UUID `json:"request_id"`
	PoolID    uuid.UUID `json:"pool_id"`
	Kind      string    `json:"kind"`
	Amount    int64     `json:"amount"`
}

type Hold struct {
	ID        uuid.UUID  `json:"id"`
	RequestID uuid.UUID  `json:"request_id"`
	PoolID    uuid.UUID  `json:"pool_id"`
	Amount    int64      `json:"amount"`
	Status    HoldStatus `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

type Dedication struct {
	ID        uuid.UUID `json:"id"`
	RequestID uuid.UUID `json:"request_id"`
	PoolID    uuid.UUID `json:"pool_id"`
	HoldID    uuid.UUID `json:"hold_id"`
	Amount    int64     `json:"amount"`
	CreatedAt time.Time `json:"created_at"`
}

type RequestEvent struct {
	ID        uuid.UUID `json:"id"`
	RequestID uuid.UUID `json:"request_id"`
	EventType EventType `json:"event_type"`
	Reason    string    `json:"reason"`
	At        time.Time `json:"at"`
}
