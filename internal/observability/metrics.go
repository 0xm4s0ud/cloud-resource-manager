package observability

import (
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

const RequestIDHeader = "X-Request-Id"

type Metrics struct {
	HTTPRequestsTotal   *prometheus.CounterVec
	HTTPRequestDuration *prometheus.HistogramVec
	HTTPInFlight        prometheus.Gauge
	ReservationAttempts *prometheus.CounterVec
	ReservationRejects  *prometheus.CounterVec
	ReservationConfirms *prometheus.CounterVec
	ReservationCancels  prometheus.Counter
	ReservationExpires  *prometheus.CounterVec
	ReservationExtends  prometheus.Counter
	PoolAvailableRatio  *prometheus.GaugeVec
	QuotaUtilization    *prometheus.GaugeVec
	SweeperRuns         *prometheus.CounterVec
	SweeperExpiredHolds prometheus.Counter
	DependencyUp        *prometheus.GaugeVec
}

var (
	defaultMetrics     *Metrics
	defaultMetricsOnce sync.Once
)

func NewMetrics() *Metrics {
	defaultMetricsOnce.Do(func() {
		defaultMetrics = newMetrics(prometheus.DefaultRegisterer)
	})
	return defaultMetrics
}

func NewMetricsForTest(reg prometheus.Registerer) *Metrics {
	return newMetrics(reg)
}

func newMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		HTTPRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total HTTP requests",
		}, []string{"method", "route", "status"}),
		HTTPRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
		HTTPInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "In-flight HTTP requests",
		}),
		ReservationAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reservation_attempts_total",
			Help: "Reservation attempts by result",
		}, []string{"result"}),
		ReservationRejects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reservation_rejections_total",
			Help: "Reservation rejections by reason",
		}, []string{"reason"}),
		ReservationConfirms: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reservation_confirmations_total",
			Help: "Confirmations by product",
		}, []string{"product"}),
		ReservationCancels: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reservation_cancellations_total",
			Help: "Cancellations",
		}),
		ReservationExpires: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reservation_expirations_total",
			Help: "Expirations by trigger",
		}, []string{"trigger"}),
		ReservationExtends: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reservation_extensions_total",
			Help: "TTL extensions",
		}),
		PoolAvailableRatio: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pool_available_ratio",
			Help: "Available capacity ratio per pool",
		}, []string{"pool"}),
		QuotaUtilization: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "quota_utilization_ratio",
			Help: "Aggregate quota utilization by resource kind",
		}, []string{"resource_kind"}),
		SweeperRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sweeper_runs_total",
			Help: "Sweeper ticks by lock acquisition",
		}, []string{"lock_acquired"}),
		SweeperExpiredHolds: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sweeper_expired_holds_total",
			Help: "Holds expired by sweeper",
		}),
		DependencyUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dependency_up",
			Help: "1 if dependency is reachable",
		}, []string{"dependency"}),
	}
	reg.MustRegister(
		m.HTTPRequestsTotal, m.HTTPRequestDuration, m.HTTPInFlight,
		m.ReservationAttempts, m.ReservationRejects, m.ReservationConfirms,
		m.ReservationCancels, m.ReservationExpires, m.ReservationExtends,
		m.PoolAvailableRatio, m.QuotaUtilization, m.SweeperRuns,
		m.SweeperExpiredHolds, m.DependencyUp,
	)
	return m
}

func Middleware(log *slog.Logger, m *Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rid := r.Header.Get(RequestIDHeader)
			if rid == "" {
				rid = uuid.NewString()
			}
			w.Header().Set(RequestIDHeader, rid)
			r = r.WithContext(withRequestID(r.Context(), rid))

			m.HTTPInFlight.Inc()
			defer m.HTTPInFlight.Dec()

			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			start := time.Now()
			next.ServeHTTP(ww, r)
			elapsed := time.Since(start)

			route := r.URL.Path
			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}
			m.HTTPRequestsTotal.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
			m.HTTPRequestDuration.WithLabelValues(r.Method, route).Observe(elapsed.Seconds())

			attrs := []any{
				"request_id", rid,
				"method", r.Method,
				"route", route,
				"status_code", status,
				"latency_ms", elapsed.Milliseconds(),
			}
			if customer := r.Header.Get("X-Customer-Id"); customer != "" {
				attrs = append(attrs, "customer_id", customer)
			}
			if status >= 400 {
				log.Warn("http_request", attrs...)
			} else {
				log.Info("http_request", attrs...)
			}
		})
	}
}
