package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

const maxBodyBytes = 16 << 20

type errorResponse struct {
	Error   string              `json:"error"`
	Details []domain.FieldError `json:"details,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

func writeValidationError(w http.ResponseWriter, ve *domain.ValidationError) {
	writeJSON(w, http.StatusUnprocessableEntity, errorResponse{Error: "validation failed", Details: ve.Errors})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("body must contain a single JSON object")
	}
	return nil
}
