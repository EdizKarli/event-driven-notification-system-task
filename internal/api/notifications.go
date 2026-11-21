package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

const (
	IdempotencyKeyHeader     = "Idempotency-Key"
	IdempotentReplayedHeader = "Idempotent-Replayed"
	maxIdempotencyKeyLength  = 255
)

func (s *Server) handleCreateNotification(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get(IdempotencyKeyHeader))
	if len(key) > maxIdempotencyKeyLength {
		writeError(w, http.StatusBadRequest, "Idempotency-Key must be at most 255 characters")
		return
	}

	var req domain.NewNotification
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	req.Normalize()
	now := s.now()
	if err := req.Validate(now); err != nil {
		var ve *domain.ValidationError
		if errors.As(err, &ve) {
			writeValidationError(w, ve)
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	n := req.ToNotification(uuid.NewString(), now)
	n.CorrelationID = CorrelationIDFrom(r.Context())
	if key != "" {
		n.IdempotencyKey = &key
	}

	err := s.store.CreateNotification(r.Context(), n)
	switch {
	case errors.Is(err, domain.ErrDuplicate):
		if !req.Matches(n) {
			writeError(w, http.StatusConflict, "idempotency key was already used with a different request")
			return
		}
		s.logger.Info("idempotent replay",
			"notification_id", n.ID,
			"correlation_id", CorrelationIDFrom(r.Context()),
		)
		w.Header().Set(IdempotentReplayedHeader, "true")
		writeJSON(w, http.StatusAccepted, n)
		return
	case err != nil:
		s.logger.Error("create notification failed", "error", err, "correlation_id", n.CorrelationID)
		writeError(w, http.StatusInternalServerError, "could not create notification")
		return
	}

	s.logger.Info("notification created",
		"notification_id", n.ID,
		"channel", n.Channel,
		"priority", n.Priority,
		"correlation_id", n.CorrelationID,
	)
	writeJSON(w, http.StatusAccepted, n)
}

func (s *Server) handleGetNotification(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid notification id")
		return
	}

	n, err := s.store.GetNotification(r.Context(), id.String())
	if errors.Is(err, domain.ErrNotFound) {
		writeError(w, http.StatusNotFound, "notification not found")
		return
	}
	if err != nil {
		s.logger.Error("get notification failed", "error", err, "correlation_id", CorrelationIDFrom(r.Context()))
		writeError(w, http.StatusInternalServerError, "could not get notification")
		return
	}
	writeJSON(w, http.StatusOK, n)
}

func (s *Server) handleCancelNotification(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid notification id")
		return
	}

	correlationID := CorrelationIDFrom(r.Context())
	n, err := s.store.CancelNotification(r.Context(), id.String())
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "notification not found")
		return
	case errors.Is(err, domain.ErrNotCancellable):
		writeError(w, http.StatusConflict,
			"only pending notifications can be cancelled, current status is "+string(n.Status))
		return
	case err != nil:
		s.logger.Error("cancel notification failed", "error", err, "correlation_id", correlationID)
		writeError(w, http.StatusInternalServerError, "could not cancel notification")
		return
	}

	s.logger.Info("notification cancelled", "notification_id", n.ID, "correlation_id", correlationID)
	writeJSON(w, http.StatusOK, n)
}
