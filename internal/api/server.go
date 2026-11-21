package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

type NotificationStore interface {
	Ping(ctx context.Context) error
	CreateNotification(ctx context.Context, n *domain.Notification) error
	GetNotification(ctx context.Context, id string) (*domain.Notification, error)
	CreateBatch(ctx context.Context, batch *domain.Batch, items []*domain.Notification) error
	GetBatch(ctx context.Context, id string) (*domain.Batch, error)
}

type Server struct {
	logger *slog.Logger
	store  NotificationStore
	now    func() time.Time
}

func NewServer(logger *slog.Logger, store NotificationStore) *Server {
	return &Server{logger: logger, store: store, now: time.Now}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /notifications", s.handleCreateNotification)
	mux.HandleFunc("GET /notifications/{id}", s.handleGetNotification)
	mux.HandleFunc("POST /notifications/batch", s.handleCreateBatch)
	mux.HandleFunc("GET /batches/{id}", s.handleGetBatch)

	var h http.Handler = mux
	h = recoverer(s.logger)(h)
	h = requestLogger(s.logger)(h)
	h = correlationID(h)
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
