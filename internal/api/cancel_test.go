package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

func TestCancelNotification(t *testing.T) {
	const (
		pendingID = "aaaaaaaa-0000-4000-8000-000000000001"
		sentID    = "aaaaaaaa-0000-4000-8000-000000000002"
	)
	store := newFakeStore()
	_ = store.CreateNotification(context.Background(), &domain.Notification{ID: pendingID, Status: domain.StatusPending})
	_ = store.CreateNotification(context.Background(), &domain.Notification{ID: sentID, Status: domain.StatusSent})
	h := newTestServer(store)

	t.Run("pending is cancelled", func(t *testing.T) {
		rec := doRequest(h, http.MethodPost, "/notifications/"+pendingID+"/cancel", "", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		var got domain.Notification
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if got.Status != domain.StatusCancelled {
			t.Errorf("expected cancelled, got %q", got.Status)
		}
		stored, _ := store.GetNotification(context.Background(), pendingID)
		if stored.Status != domain.StatusCancelled {
			t.Errorf("expected stored status cancelled, got %q", stored.Status)
		}
	})

	t.Run("already cancelled returns conflict", func(t *testing.T) {
		rec := doRequest(h, http.MethodPost, "/notifications/"+pendingID+"/cancel", "", nil)
		if rec.Code != http.StatusConflict {
			t.Fatalf("expected 409, got %d", rec.Code)
		}
	})

	t.Run("sent returns conflict", func(t *testing.T) {
		rec := doRequest(h, http.MethodPost, "/notifications/"+sentID+"/cancel", "", nil)
		if rec.Code != http.StatusConflict {
			t.Fatalf("expected 409, got %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "sent") {
			t.Errorf("expected current status in message: %s", rec.Body.String())
		}
	})

	t.Run("not found", func(t *testing.T) {
		rec := doRequest(h, http.MethodPost, "/notifications/00000000-0000-0000-0000-000000000000/cancel", "", nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d", rec.Code)
		}
	})

	t.Run("invalid id", func(t *testing.T) {
		rec := doRequest(h, http.MethodPost, "/notifications/abc/cancel", "", nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", rec.Code)
		}
	})
}
