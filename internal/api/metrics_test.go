package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
	"github.com/EdizKarli/event-driven-notification-system-task/internal/metrics"
)

func TestMetricsEndpoint(t *testing.T) {
	store := newFakeStore()
	_ = store.CreateNotification(context.Background(), &domain.Notification{
		ID: "aaaaaaaa-0000-4000-8000-000000000001", Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Status: domain.StatusPending,
	})
	_ = store.CreateNotification(context.Background(), &domain.Notification{
		ID: "aaaaaaaa-0000-4000-8000-000000000002", Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Status: domain.StatusProcessing,
	})

	rec := metrics.New()
	rec.RecordSent(domain.ChannelSMS, 20*time.Millisecond, time.Second)
	rec.RecordFailed(domain.ChannelEmail)

	h := NewServer(slog.New(slog.NewTextHandler(io.Discard, nil)), store, rec).Handler()
	resp := doRequest(h, http.MethodGet, "/metrics", "", nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}

	var got metricsResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	sms := got.Queue[domain.ChannelSMS]
	if sms == nil || sms.Ready[domain.PriorityHigh] != 1 || sms.Processing != 1 {
		t.Fatalf("unexpected sms queue: %+v", sms)
	}
	if got.Deliveries.Totals.Sent != 1 || got.Deliveries.Totals.Failed != 1 {
		t.Fatalf("unexpected delivery totals: %+v", got.Deliveries.Totals)
	}
}

func TestMetricsDatabaseDown(t *testing.T) {
	store := newFakeStore()
	store.queueErr = errors.New("connection refused")
	h := newTestServer(store)

	resp := doRequest(h, http.MethodGet, "/metrics", "", nil)
	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.Code)
	}
}
