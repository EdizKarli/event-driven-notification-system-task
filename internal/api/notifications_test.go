package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

func newTestServer(store *fakeStore) http.Handler {
	return NewServer(slog.New(slog.NewTextHandler(io.Discard, nil)), store).Handler()
}

func doRequest(h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateNotification(t *testing.T) {
	store := newFakeStore()
	h := newTestServer(store)

	body := `{"recipient":"+905551234567","channel":"sms","content":"hello","priority":"high"}`
	rec := doRequest(h, http.MethodPost, "/notifications", body, map[string]string{CorrelationIDHeader: "corr-1"})

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}

	var got domain.Notification
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.ID == "" {
		t.Fatal("expected an id")
	}
	if got.Status != domain.StatusPending {
		t.Errorf("expected pending, got %q", got.Status)
	}
	if got.Priority != domain.PriorityHigh {
		t.Errorf("expected high priority, got %q", got.Priority)
	}

	stored, err := store.GetNotification(context.Background(), got.ID)
	if err != nil {
		t.Fatalf("notification was not stored: %v", err)
	}
	if stored.CorrelationID != "corr-1" {
		t.Errorf("expected correlation id corr-1, got %q", stored.CorrelationID)
	}
}

func TestCreateNotificationValidationError(t *testing.T) {
	h := newTestServer(newFakeStore())

	body := `{"recipient":"not-a-phone","channel":"sms","content":"hello"}`
	rec := doRequest(h, http.MethodPost, "/notifications", body, nil)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"field":"recipient"`) {
		t.Errorf("expected recipient error in body: %s", rec.Body.String())
	}
}

func TestCreateNotificationBadBody(t *testing.T) {
	h := newTestServer(newFakeStore())

	tests := map[string]string{
		"invalid json":  `{"recipient":`,
		"unknown field": `{"recipient":"+905551234567","channel":"sms","content":"hi","foo":1}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			rec := doRequest(h, http.MethodPost, "/notifications", body, nil)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rec.Code)
			}
		})
	}
}

func TestCreateNotificationStoreError(t *testing.T) {
	store := newFakeStore()
	store.createErr = errors.New("db is down")
	h := newTestServer(store)

	body := `{"recipient":"+905551234567","channel":"sms","content":"hello"}`
	rec := doRequest(h, http.MethodPost, "/notifications", body, nil)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}

func TestGetNotification(t *testing.T) {
	store := newFakeStore()
	h := newTestServer(store)

	id := "3f1c2a8e-7b4d-4c1a-9e2f-1a2b3c4d5e6f"
	_ = store.CreateNotification(context.Background(), &domain.Notification{ID: id, Status: domain.StatusPending})

	t.Run("found", func(t *testing.T) {
		rec := doRequest(h, http.MethodGet, "/notifications/"+id, "", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), id) {
			t.Errorf("expected id in body: %s", rec.Body.String())
		}
	})

	t.Run("not found", func(t *testing.T) {
		rec := doRequest(h, http.MethodGet, "/notifications/00000000-0000-0000-0000-000000000000", "", nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d", rec.Code)
		}
	})

	t.Run("invalid id", func(t *testing.T) {
		rec := doRequest(h, http.MethodGet, "/notifications/abc", "", nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", rec.Code)
		}
	})
}

func TestHealthDatabaseDown(t *testing.T) {
	store := newFakeStore()
	store.pingErr = errors.New("connection refused")
	h := newTestServer(store)

	rec := doRequest(h, http.MethodGet, "/health", "", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}
