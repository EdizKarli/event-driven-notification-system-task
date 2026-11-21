package api

import (
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

const (
	defaultListLimit = 20
	maxListLimit     = 100
)

type listResponse struct {
	Data       []*domain.Notification `json:"data"`
	NextCursor string                 `json:"next_cursor,omitempty"`
}

func (s *Server) handleListNotifications(w http.ResponseWriter, r *http.Request) {
	filter, ve := parseListFilter(r.URL.Query())
	if ve != nil {
		writeValidationError(w, ve)
		return
	}

	pageSize := filter.Limit
	filter.Limit = pageSize + 1

	items, err := s.store.ListNotifications(r.Context(), filter)
	if err != nil {
		s.logger.Error("list notifications failed", "error", err, "correlation_id", CorrelationIDFrom(r.Context()))
		writeError(w, http.StatusInternalServerError, "could not list notifications")
		return
	}

	if items == nil {
		items = []*domain.Notification{}
	}

	resp := listResponse{Data: items}
	if len(items) > pageSize {
		resp.Data = items[:pageSize]
		last := resp.Data[pageSize-1]
		resp.NextCursor = encodeCursor(domain.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}
	writeJSON(w, http.StatusOK, resp)
}

func parseListFilter(q url.Values) (domain.ListFilter, *domain.ValidationError) {
	f := domain.ListFilter{Limit: defaultListLimit}
	ve := &domain.ValidationError{}
	fail := func(field, message string) {
		ve.Errors = append(ve.Errors, domain.FieldError{Field: field, Message: message})
	}

	if v := q.Get("status"); v != "" {
		status := domain.Status(strings.ToLower(v))
		if status.Valid() {
			f.Status = &status
		} else {
			fail("status", "must be one of pending, processing, sent, failed, cancelled")
		}
	}

	if v := q.Get("channel"); v != "" {
		channel := domain.Channel(strings.ToLower(v))
		if channel.Valid() {
			f.Channel = &channel
		} else {
			fail("channel", "must be one of sms, email, push")
		}
	}

	if v := q.Get("batch_id"); v != "" {
		if id, err := uuid.Parse(v); err == nil {
			batchID := id.String()
			f.BatchID = &batchID
		} else {
			fail("batch_id", "must be a valid uuid")
		}
	}

	if v := q.Get("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			f.From = &t
		} else {
			fail("from", "must be an RFC3339 timestamp, e.g. 2026-11-21T00:00:00Z")
		}
	}

	if v := q.Get("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			f.To = &t
		} else {
			fail("to", "must be an RFC3339 timestamp, e.g. 2026-11-22T00:00:00Z")
		}
	}

	if f.From != nil && f.To != nil && !f.From.Before(*f.To) {
		fail("to", "must be after from")
	}

	if v := q.Get("limit"); v != "" {
		limit, err := strconv.Atoi(v)
		if err != nil || limit < 1 || limit > maxListLimit {
			fail("limit", "must be a number between 1 and "+strconv.Itoa(maxListLimit))
		} else {
			f.Limit = limit
		}
	}

	if v := q.Get("cursor"); v != "" {
		c, err := decodeCursor(v)
		if err != nil {
			fail("cursor", "is invalid")
		} else {
			f.After = &c
		}
	}

	if len(ve.Errors) > 0 {
		return f, ve
	}
	return f, nil
}

func encodeCursor(c domain.Cursor) string {
	raw := c.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + c.ID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(s string) (domain.Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return domain.Cursor{}, err
	}
	createdAt, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return domain.Cursor{}, errors.New("malformed cursor")
	}
	t, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return domain.Cursor{}, err
	}
	if _, err := uuid.Parse(id); err != nil {
		return domain.Cursor{}, err
	}
	return domain.Cursor{CreatedAt: t, ID: id}, nil
}
