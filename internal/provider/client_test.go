package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

func testNotification() *domain.Notification {
	return &domain.Notification{
		ID:            "11111111-1111-4111-8111-111111111111",
		Recipient:     "+905551234567",
		Channel:       domain.ChannelSMS,
		Content:       "hello",
		CorrelationID: "corr-1",
	}
}

func TestSendSuccess(t *testing.T) {
	var gotBody request
	var gotHeaders http.Header

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"msg-123","status":"accepted","timestamp":"2026-11-22T12:00:00Z"}`))
	}))
	defer srv.Close()

	resp, err := NewClient(srv.URL, time.Second).Send(context.Background(), testNotification())
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if resp.MessageID != "msg-123" || resp.Status != "accepted" {
		t.Errorf("unexpected response: %+v", resp)
	}
	if gotBody.To != "+905551234567" || gotBody.Channel != "sms" || gotBody.Content != "hello" {
		t.Errorf("unexpected request body: %+v", gotBody)
	}
	if gotHeaders.Get("Idempotency-Key") != "11111111-1111-4111-8111-111111111111" {
		t.Errorf("expected notification id as idempotency key, got %q", gotHeaders.Get("Idempotency-Key"))
	}
	if gotHeaders.Get("X-Correlation-ID") != "corr-1" {
		t.Errorf("expected correlation id header, got %q", gotHeaders.Get("X-Correlation-ID"))
	}
}

func TestSendStatusClassification(t *testing.T) {
	tests := []struct {
		status    int
		retryable bool
	}{
		{http.StatusBadRequest, false},
		{http.StatusUnauthorized, false},
		{http.StatusNotFound, false},
		{http.StatusUnprocessableEntity, false},
		{http.StatusRequestTimeout, true},
		{http.StatusTooManyRequests, true},
		{http.StatusInternalServerError, true},
		{http.StatusBadGateway, true},
		{http.StatusServiceUnavailable, true},
	}

	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
			}))
			defer srv.Close()

			_, err := NewClient(srv.URL, time.Second).Send(context.Background(), testNotification())

			var perr *Error
			if !errors.As(err, &perr) {
				t.Fatalf("expected provider error, got %v", err)
			}
			if perr.StatusCode != tt.status {
				t.Errorf("expected status %d, got %d", tt.status, perr.StatusCode)
			}
			if perr.Retryable != tt.retryable {
				t.Errorf("expected retryable=%v, got %v", tt.retryable, perr.Retryable)
			}
		})
	}
}

func TestSendTimeoutIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL, 50*time.Millisecond).Send(context.Background(), testNotification())

	var perr *Error
	if !errors.As(err, &perr) || !perr.Retryable {
		t.Fatalf("expected retryable error on timeout, got %v", err)
	}
}

func TestSendConnectionRefusedIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()

	_, err := NewClient(url, time.Second).Send(context.Background(), testNotification())

	var perr *Error
	if !errors.As(err, &perr) || !perr.Retryable {
		t.Fatalf("expected retryable error when provider is down, got %v", err)
	}
}
