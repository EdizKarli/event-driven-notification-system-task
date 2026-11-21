package api

import (
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

type createBatchRequest struct {
	Notifications []domain.NewNotification `json:"notifications"`
}

type createBatchResponse struct {
	BatchID         string   `json:"batch_id"`
	Total           int      `json:"total"`
	NotificationIDs []string `json:"notification_ids"`
}

func (s *Server) handleCreateBatch(w http.ResponseWriter, r *http.Request) {
	var req createBatchRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	for i := range req.Notifications {
		req.Notifications[i].Normalize()
	}

	now := s.now()
	if err := domain.ValidateBatch(req.Notifications, now); err != nil {
		var ve *domain.ValidationError
		if errors.As(err, &ve) {
			writeValidationError(w, ve)
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	correlationID := CorrelationIDFrom(r.Context())
	batch := &domain.Batch{
		ID:        uuid.NewString(),
		Total:     len(req.Notifications),
		CreatedAt: now,
	}

	items := make([]*domain.Notification, len(req.Notifications))
	ids := make([]string, len(req.Notifications))
	for i, item := range req.Notifications {
		n := item.ToNotification(uuid.NewString(), now)
		n.BatchID = &batch.ID
		n.CorrelationID = correlationID
		items[i] = n
		ids[i] = n.ID
	}

	if err := s.store.CreateBatch(r.Context(), batch, items); err != nil {
		s.logger.Error("create batch failed", "error", err, "correlation_id", correlationID)
		writeError(w, http.StatusInternalServerError, "could not create batch")
		return
	}

	s.logger.Info("batch created",
		"batch_id", batch.ID,
		"total", batch.Total,
		"correlation_id", correlationID,
	)
	writeJSON(w, http.StatusAccepted, createBatchResponse{
		BatchID:         batch.ID,
		Total:           batch.Total,
		NotificationIDs: ids,
	})
}

func (s *Server) handleGetBatch(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid batch id")
		return
	}

	batch, err := s.store.GetBatch(r.Context(), id.String())
	if errors.Is(err, domain.ErrBatchNotFound) {
		writeError(w, http.StatusNotFound, "batch not found")
		return
	}
	if err != nil {
		s.logger.Error("get batch failed", "error", err, "correlation_id", CorrelationIDFrom(r.Context()))
		writeError(w, http.StatusInternalServerError, "could not get batch")
		return
	}
	writeJSON(w, http.StatusOK, batch)
}
