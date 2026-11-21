package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

const idemBody = `{"recipient":"+905551234567","channel":"sms","content":"hello"}`

func createWithKey(t *testing.T, h http.Handler, body, key string) (*domain.Notification, *http.Response) {
	t.Helper()
	headers := map[string]string{}
	if key != "" {
		headers[IdempotencyKeyHeader] = key
	}
	rec := doRequest(h, http.MethodPost, "/notifications", body, headers)
	if rec.Code != http.StatusAccepted {
		return nil, rec.Result()
	}
	var n domain.Notification
	if err := json.Unmarshal(rec.Body.Bytes(), &n); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return &n, rec.Result()
}

func TestIdempotentCreateReturnsSameNotification(t *testing.T) {
	store := newFakeStore()
	h := newTestServer(store)

	first, firstResp := createWithKey(t, h, idemBody, "order-42")
	second, secondResp := createWithKey(t, h, idemBody, "order-42")

	if first == nil || second == nil {
		t.Fatal("expected both requests to succeed")
	}
	if first.ID != second.ID {
		t.Fatalf("expected same id, got %s and %s", first.ID, second.ID)
	}
	if firstResp.Header.Get(IdempotentReplayedHeader) != "" {
		t.Error("first response should not be marked as replayed")
	}
	if secondResp.Header.Get(IdempotentReplayedHeader) != "true" {
		t.Error("second response should be marked as replayed")
	}
	if len(store.items) != 1 {
		t.Fatalf("expected 1 stored notification, got %d", len(store.items))
	}
}

func TestIdempotentCreateDifferentBodyConflicts(t *testing.T) {
	h := newTestServer(newFakeStore())

	createWithKey(t, h, idemBody, "order-43")
	other := `{"recipient":"+905551234567","channel":"sms","content":"a different message"}`
	_, resp := createWithKey(t, h, other, "order-43")

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}
}

func TestCreateWithoutKeyIsNotDeduplicated(t *testing.T) {
	store := newFakeStore()
	h := newTestServer(store)

	first, _ := createWithKey(t, h, idemBody, "")
	second, _ := createWithKey(t, h, idemBody, "")

	if first == nil || second == nil || first.ID == second.ID {
		t.Fatal("expected two different notifications")
	}
	if len(store.items) != 2 {
		t.Fatalf("expected 2 stored notifications, got %d", len(store.items))
	}
}

func TestDifferentKeysCreateDifferentNotifications(t *testing.T) {
	h := newTestServer(newFakeStore())

	first, _ := createWithKey(t, h, idemBody, "key-a")
	second, _ := createWithKey(t, h, idemBody, "key-b")

	if first == nil || second == nil || first.ID == second.ID {
		t.Fatal("expected two different notifications")
	}
}

func TestIdempotencyKeyTooLong(t *testing.T) {
	h := newTestServer(newFakeStore())

	_, resp := createWithKey(t, h, idemBody, strings.Repeat("k", 256))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}
