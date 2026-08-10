package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/0xm4s0ud/cloud-management-plane/internal/domain"
	"github.com/0xm4s0ud/cloud-management-plane/internal/ports"
	"github.com/0xm4s0ud/cloud-management-plane/internal/service"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Handler struct {
	svc           *service.CapacityService
	pgHealth      ports.HealthChecker
	cache         ports.CacheStore
	setDependency func(name string, up bool)
}

func NewHandler(svc *service.CapacityService, pg ports.HealthChecker, cache ports.CacheStore) *Handler {
	return &Handler{svc: svc, pgHealth: pg, cache: cache}
}

func (h *Handler) SetDependencyGauge(fn func(name string, up bool)) { h.setDependency = fn }

func (h *Handler) Routes() chi.Router {
	r := chi.NewRouter()
	r.Get("/health", h.health)
	r.Get("/ready", h.ready)
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/v1", func(r chi.Router) {
		r.Get("/products", h.listProducts)
		r.Get("/capacity", h.listCapacity)
		r.Post("/requests", h.createRequest)
		r.Get("/requests", h.listRequests)
		r.Get("/requests/{id}", h.getRequest)
		r.Post("/requests/{id}/confirm", h.confirm)
		r.Post("/requests/{id}/cancel", h.cancel)
		r.Post("/requests/{id}/extend", h.extend)
	})
	return r
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	pgOK := true
	if h.pgHealth != nil {
		if err := h.pgHealth.Ping(r.Context()); err != nil {
			pgOK = false
		}
	}
	redisOK := true
	if h.cache != nil {
		if err := h.cache.Ping(r.Context()); err != nil {
			redisOK = false
		}
	}
	if h.setDependency != nil {
		h.setDependency("postgres", pgOK)
		h.setDependency("redis", redisOK)
	}
	body := map[string]any{"postgres": pgOK, "redis": redisOK}
	if !pgOK {
		body["status"] = "not_ready"
		w.Header().Set("Retry-After", "5")
		writeJSON(w, http.StatusServiceUnavailable, body)
		return
	}
	body["status"] = "ready"
	writeJSON(w, http.StatusOK, body)
}

func (h *Handler) listProducts(w http.ResponseWriter, r *http.Request) {
	products, err := h.svc.ListProducts(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"products": products})
}

func (h *Handler) listCapacity(w http.ResponseWriter, r *http.Request) {
	customerID := r.URL.Query().Get("customer_id")
	if customerID == "" {
		customerID = r.Header.Get("X-Customer-Id")
	}
	views, err := h.svc.ListCapacity(r.Context(), customerID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"capacity": views})
}

type createBody struct {
	CustomerID string              `json:"customer_id"`
	ProductID  string              `json:"product_id"`
	Quantity   int                 `json:"quantity"`
	Lines      []service.LineInput `json:"lines"`
}

func (h *Handler) createRequest(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, domain.ReasonMissingIdempotency, "Idempotency-Key header is required", nil)
		return
	}
	var body createBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, domain.ReasonConflict, "invalid JSON body", nil)
		return
	}
	customerID := body.CustomerID
	if customerID == "" {
		customerID = r.Header.Get("X-Customer-Id")
	}
	req, err := h.svc.CreateRequest(r.Context(), service.CreateInput{
		CustomerID:     customerID,
		IdempotencyKey: key,
		ProductCode:    body.ProductID,
		Quantity:       body.Quantity,
		Lines:          body.Lines,
		CorrelationID:  r.Header.Get("X-Request-Id"),
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	status := http.StatusCreated
	if req.Status == domain.StatusRejected {
		status = http.StatusConflict
	}
	writeJSON(w, status, req)
}

func (h *Handler) listRequests(w http.ResponseWriter, r *http.Request) {
	customerID := r.URL.Query().Get("customer_id")
	if customerID == "" {
		customerID = r.Header.Get("X-Customer-Id")
	}
	if customerID == "" {
		writeError(w, http.StatusBadRequest, domain.ReasonConflict, "customer_id required", nil)
		return
	}
	var status *domain.RequestStatus
	if s := r.URL.Query().Get("status"); s != "" {
		st := domain.RequestStatus(s)
		status = &st
	}
	list, err := h.svc.ListRequests(r.Context(), customerID, status)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"requests": list})
}

func (h *Handler) getRequest(w http.ResponseWriter, r *http.Request) {
	id, ok := h.idParam(w, r)
	if !ok {
		return
	}
	req, lines, events, err := h.svc.GetRequest(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"request": req, "lines": lines, "events": events})
}

func (h *Handler) confirm(w http.ResponseWriter, r *http.Request) {
	id, ok := h.idParam(w, r)
	if !ok {
		return
	}
	req, err := h.svc.Confirm(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, req)
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	id, ok := h.idParam(w, r)
	if !ok {
		return
	}
	req, err := h.svc.Cancel(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, req)
}

func (h *Handler) extend(w http.ResponseWriter, r *http.Request) {
	id, ok := h.idParam(w, r)
	if !ok {
		return
	}
	req, err := h.svc.Extend(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, req)
}

func (h *Handler) idParam(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, domain.ReasonConflict, "invalid request id", nil)
		return uuid.UUID{}, false
	}
	return id, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, reason domain.Reason, message string, details any) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{"reason": reason, "message": message, "details": details},
	})
}

func writeErr(w http.ResponseWriter, err error) {
	if ae, ok := domain.AsAppError(err); ok {
		writeError(w, statusForReason(ae.Reason), ae.Reason, ae.Message, ae.Details)
		return
	}
	writeError(w, http.StatusServiceUnavailable, domain.ReasonUnavailable, "dependency error", map[string]string{"err": err.Error()})
}

func statusForReason(r domain.Reason) int {
	switch r {
	case domain.ReasonMissingIdempotency, domain.ReasonInvalidResourceKind, domain.ReasonConflict, domain.ReasonInvalidInput:
		return http.StatusBadRequest
	case domain.ReasonProductNotFound, domain.ReasonNotFound:
		return http.StatusNotFound
	case domain.ReasonInsufficientCapacity, domain.ReasonQuotaExceeded, domain.ReasonExpired,
		domain.ReasonExtensionLimit, domain.ReasonIdempotencyKeyReused, domain.ReasonTooManyActiveHolds:
		return http.StatusConflict
	case domain.ReasonUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
