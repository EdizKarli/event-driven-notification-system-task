package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
	"github.com/EdizKarli/event-driven-notification-system-task/internal/metrics"
)

type NotificationStore interface {
	Ping(ctx context.Context) error
	CreateNotification(ctx context.Context, n *domain.Notification) error
	GetNotification(ctx context.Context, id string) (*domain.Notification, error)
	CreateBatch(ctx context.Context, batch *domain.Batch, items []*domain.Notification) error
	GetBatch(ctx context.Context, id string) (*domain.Batch, error)
	ListNotifications(ctx context.Context, f domain.ListFilter) ([]*domain.Notification, error)
	CancelNotification(ctx context.Context, id string) (*domain.Notification, error)
	QueueDepth(ctx context.Context) (map[domain.Channel]*domain.ChannelQueue, error)
}

type MetricsSource interface {
	Snapshot() metrics.Snapshot
}

type Server struct {
	logger  *slog.Logger
	store   NotificationStore
	metrics MetricsSource
	now     func() time.Time
}

func NewServer(logger *slog.Logger, store NotificationStore, m MetricsSource) *Server {
	return &Server{logger: logger, store: store, metrics: m, now: time.Now}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /notifications", s.handleListNotifications)
	mux.HandleFunc("POST /notifications", s.handleCreateNotification)
	mux.HandleFunc("GET /notifications/{id}", s.handleGetNotification)
	mux.HandleFunc("POST /notifications/{id}/cancel", s.handleCancelNotification)
	mux.HandleFunc("POST /notifications/batch", s.handleCreateBatch)
	mux.HandleFunc("GET /batches/{id}", s.handleGetBatch)

	var h http.Handler = mux
	h = recoverer(s.logger)(h)
	h = requestLogger(s.logger)(h)
	h = correlationID(h)
	h = cors(h)
	return h
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := s.store.Ping(ctx); err != nil {
		s.logger.Error("health check failed", "error", err, "correlation_id", CorrelationIDFrom(r.Context()))
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "database": "down"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "database": "up"})
}
