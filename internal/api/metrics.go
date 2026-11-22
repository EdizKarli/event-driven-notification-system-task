package api

import (
	"context"
	"net/http"
	"time"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
	"github.com/EdizKarli/event-driven-notification-system-task/internal/metrics"
)

type metricsResponse struct {
	Timestamp  time.Time                              `json:"timestamp"`
	Queue      map[domain.Channel]*domain.ChannelQueue `json:"queue"`
	Deliveries metrics.Snapshot                       `json:"deliveries"`
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	queue, err := s.store.QueueDepth(ctx)
	if err != nil {
		s.logger.Error("queue depth failed", "error", err, "correlation_id", CorrelationIDFrom(r.Context()))
		writeError(w, http.StatusServiceUnavailable, "could not read queue depth")
		return
	}

	writeJSON(w, http.StatusOK, metricsResponse{
		Timestamp:  s.now().UTC(),
		Queue:      queue,
		Deliveries: s.metrics.Snapshot(),
	})
}
