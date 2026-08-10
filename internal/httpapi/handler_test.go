package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/0xm4s0ud/cloud-management-plane/internal/config"
	"github.com/0xm4s0ud/cloud-management-plane/internal/domain"
	"github.com/0xm4s0ud/cloud-management-plane/internal/httpapi"
	"github.com/0xm4s0ud/cloud-management-plane/internal/observability"
	"github.com/0xm4s0ud/cloud-management-plane/internal/ports"
	"github.com/0xm4s0ud/cloud-management-plane/internal/repository/memory"
	"github.com/0xm4s0ud/cloud-management-plane/internal/service"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

var epoch = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

var testCfg = config.Config{
	ReserveTTL:         15 * time.Minute,
	MaxExtensions:      3,
	MaxReserveWallTime: 60 * time.Minute,
	CapacityCacheTTL:   time.Second,
}

func newPool(kind string, total int64) domain.ResourcePool {
	return domain.ResourcePool{
		ID:                  uuid.New(),
		Kind:                kind,
		Unit:                "units",
		Total:               total,
		OversubscribeFactor: 1.0,
		Policy:              domain.PoolPolicy{Splittable: true, MinGrain: 1},
	}
}

func setup(pools *memory.Pools) (*httptest.Server, *memory.Clock) {
	clk := memory.NewClock(epoch)
	reg := prometheus.NewRegistry()
	svc := service.NewCapacityService(service.Params{
		Pools:    pools,
		Quotas:   memory.NewQuotas(),
		Products: memory.NewProducts(),
		Requests: memory.NewRequests(),
		Tx:       memory.Tx{},
		Lock:     memory.Locker{},
		Cache:    ports.NoopCache{},
		Idem:     ports.NoopIdempotency{},
		Clock:    clk,
		Cfg:      testCfg,
		Metrics:  observability.NewMetricsForTest(reg),
		Log:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	})
	h := httpapi.NewHandler(svc, nil, ports.NoopCache{})
	srv := httptest.NewServer(h.Routes())
	return srv, clk
}

func TestReady_OK(t *testing.T) {
	pool := newPool("cpu-cores", 10)
	srv, _ := setup(memory.NewPools(pool))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/ready")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestCreateRequest_StatusMapping(t *testing.T) {
	pool := newPool("cpu-cores", 5)

	tests := []struct {
		name       string
		body       map[string]any
		idemKey    string
		wantStatus int
		wantReason domain.Reason
	}{
		{
			name:       "missing idempotency key → 400",
			body:       map[string]any{"customer_id": "cust-1", "lines": []any{map[string]any{"resource_kind": "cpu-cores", "amount": 1}}},
			idemKey:    "",
			wantStatus: http.StatusBadRequest,
			wantReason: domain.ReasonMissingIdempotency,
		},
		{
			name:       "insufficient capacity → 409",
			body:       map[string]any{"customer_id": "cust-1", "lines": []any{map[string]any{"resource_kind": "cpu-cores", "amount": 100}}},
			idemKey:    uuid.NewString(),
			wantStatus: http.StatusConflict,
			wantReason: domain.ReasonInsufficientCapacity,
		},
		{
			name:       "unknown resource kind → 400",
			body:       map[string]any{"customer_id": "cust-1", "lines": []any{map[string]any{"resource_kind": "gpu-v100", "amount": 1}}},
			idemKey:    uuid.NewString(),
			wantStatus: http.StatusBadRequest,
			wantReason: domain.ReasonInvalidResourceKind,
		},
		{
			name:       "product not found → 404",
			body:       map[string]any{"customer_id": "cust-1", "product_id": "no-such-sku", "quantity": 1},
			idemKey:    uuid.NewString(),
			wantStatus: http.StatusNotFound,
			wantReason: domain.ReasonProductNotFound,
		},
		{
			name:       "successful reserve → 201",
			body:       map[string]any{"customer_id": "cust-1", "lines": []any{map[string]any{"resource_kind": "cpu-cores", "amount": 2}}},
			idemKey:    uuid.NewString(),
			wantStatus: http.StatusCreated,
			wantReason: "",
		},
	}

	srv, _ := setup(memory.NewPools(pool))
	defer srv.Close()

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			b, _ := json.Marshal(tc.body)
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/v1/requests", bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			if tc.idemKey != "" {
				req.Header.Set("Idempotency-Key", tc.idemKey)
			}
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, tc.wantStatus, resp.StatusCode)

			if tc.wantReason != "" {
				var body map[string]any
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
				errObj, ok := body["error"].(map[string]any)
				if !ok {
					return
				}
				require.Equal(t, string(tc.wantReason), errObj["reason"])
			}
		})
	}
}

func TestIDParamHandlers_InvalidID(t *testing.T) {
	pool := newPool("cpu-cores", 10)
	srv, _ := setup(memory.NewPools(pool))
	defer srv.Close()

	endpoints := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/requests/not-a-uuid"},
		{http.MethodPost, "/v1/requests/not-a-uuid/confirm"},
		{http.MethodPost, "/v1/requests/not-a-uuid/cancel"},
		{http.MethodPost, "/v1/requests/not-a-uuid/extend"},
	}

	for _, ep := range endpoints {
		ep := ep
		t.Run(fmt.Sprintf("%s %s", ep.method, ep.path), func(t *testing.T) {
			req, _ := http.NewRequestWithContext(context.Background(), ep.method, srv.URL+ep.path, nil)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}
}

func TestIDParamHandlers_NotFound(t *testing.T) {
	pool := newPool("cpu-cores", 10)
	srv, _ := setup(memory.NewPools(pool))
	defer srv.Close()

	missing := uuid.New()
	endpoints := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/requests/" + missing.String()},
		{http.MethodPost, "/v1/requests/" + missing.String() + "/confirm"},
		{http.MethodPost, "/v1/requests/" + missing.String() + "/cancel"},
		{http.MethodPost, "/v1/requests/" + missing.String() + "/extend"},
	}

	for _, ep := range endpoints {
		ep := ep
		t.Run(fmt.Sprintf("%s %s", ep.method, ep.path), func(t *testing.T) {
			req, _ := http.NewRequestWithContext(context.Background(), ep.method, srv.URL+ep.path, nil)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusNotFound, resp.StatusCode)
		})
	}
}

func createReserved(t *testing.T, srv *httptest.Server, customerID string, amount int64) uuid.UUID {
	t.Helper()
	body := map[string]any{
		"customer_id": customerID,
		"lines":       []any{map[string]any{"resource_kind": "cpu-cores", "amount": amount}},
	}
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/v1/requests", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", uuid.NewString())
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	idStr, ok := got["id"].(string)
	require.True(t, ok, "response missing id field")
	id, err := uuid.Parse(idStr)
	require.NoError(t, err)
	return id
}

func TestConfirmEndpoint_Success(t *testing.T) {
	pool := newPool("cpu-cores", 10)
	srv, _ := setup(memory.NewPools(pool))
	defer srv.Close()

	id := createReserved(t, srv, "cust-1", 2)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
		srv.URL+"/v1/requests/"+id.String()+"/confirm", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, string(domain.StatusCompleted), got["status"])
}

func TestCancelEndpoint_Success(t *testing.T) {
	pool := newPool("cpu-cores", 10)
	srv, _ := setup(memory.NewPools(pool))
	defer srv.Close()

	id := createReserved(t, srv, "cust-1", 2)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
		srv.URL+"/v1/requests/"+id.String()+"/cancel", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, string(domain.StatusCancelled), got["status"])
}

func TestExtendEndpoint_Success(t *testing.T) {
	pool := newPool("cpu-cores", 10)
	srv, _ := setup(memory.NewPools(pool))
	defer srv.Close()

	id := createReserved(t, srv, "cust-1", 2)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
		srv.URL+"/v1/requests/"+id.String()+"/extend", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
