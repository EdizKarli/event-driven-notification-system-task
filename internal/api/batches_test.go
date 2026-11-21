package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

const smsItem = `{"recipient":"+905551234567","channel":"sms","content":"hello"}`

func batchBody(items ...string) string {
	return `{"notifications":[` + strings.Join(items, ",") + `]}`
}

func TestCreateBatch(t *testing.T) {
	store := newFakeStore()
	h := newTestServer(store)

	email := `{"recipient":"user@example.com","channel":"email","content":"hi","priority":"low"}`
	rec := doRequest(h, http.MethodPost, "/notifications/batch", batchBody(smsItem, smsItem, email), nil)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp createBatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.BatchID == "" || resp.Total != 3 || len(resp.NotificationIDs) != 3 {
		t.Fatalf("unexpected response: %+v", resp)
	}

	for _, id := range resp.NotificationIDs {
		n, err := store.GetNotification(context.Background(), id)
		if err != nil {
			t.Fatalf("notification %s not stored: %v", id, err)
		}
		if n.BatchID == nil || *n.BatchID != resp.BatchID {
			t.Errorf("notification %s has wrong batch id", id)
		}
	}
}

func TestCreateBatchValidation(t *testing.T) {
	h := newTestServer(newFakeStore())

	tests := []struct {
		name      string
		body      string
		wantField string
	}{
		{"empty", batchBody(), `"field":"notifications"`},
		{"bad item", batchBody(smsItem, `{"recipient":"","channel":"sms","content":"hi"}`), `"field":"notifications[1].recipient"`},
		{"too many", batchBody(strings.Split(strings.Repeat(smsItem+"|", domain.MaxBatchSize+1), "|")[:domain.MaxBatchSize+1]...), `"field":"notifications"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(h, http.MethodPost, "/notifications/batch", tt.body, nil)
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("expected 422, got %d", rec.Code)
			}
			if !strings.Contains(rec.Body.String(), tt.wantField) {
				t.Errorf("expected %s in body: %s", tt.wantField, rec.Body.String())
			}
		})
	}
}

func TestGetBatch(t *testing.T) {
	store := newFakeStore()
	h := newTestServer(store)

	batchID := "8a7b6c5d-4e3f-4a2b-9c1d-0e9f8a7b6c5d"
	_ = store.CreateBatch(context.Background(),
		&domain.Batch{ID: batchID, Total: 2, CreatedAt: time.Now()},
		[]*domain.Notification{
			{ID: "11111111-1111-4111-8111-111111111111", BatchID: &batchID, Status: domain.StatusPending},
			{ID: "22222222-2222-4222-8222-222222222222", BatchID: &batchID, Status: domain.StatusSent},
		},
	)

	t.Run("found", func(t *testing.T) {
		rec := doRequest(h, http.MethodGet, "/batches/"+batchID, "", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		var got domain.Batch
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if got.Total != 2 || got.StatusCounts[domain.StatusPending] != 1 || got.StatusCounts[domain.StatusSent] != 1 {
			t.Errorf("unexpected batch: %+v", got)
		}
	})

	t.Run("not found", func(t *testing.T) {
		rec := doRequest(h, http.MethodGet, "/batches/00000000-0000-0000-0000-000000000000", "", nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d", rec.Code)
		}
	})

	t.Run("invalid id", func(t *testing.T) {
		rec := doRequest(h, http.MethodGet, "/batches/abc", "", nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", rec.Code)
		}
	})
}
