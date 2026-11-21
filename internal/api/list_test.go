package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

var listBase = time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)

func seedList(t *testing.T) *fakeStore {
	t.Helper()
	store := newFakeStore()
	seed := []struct {
		status  domain.Status
		channel domain.Channel
	}{
		{domain.StatusPending, domain.ChannelSMS},
		{domain.StatusSent, domain.ChannelEmail},
		{domain.StatusPending, domain.ChannelPush},
		{domain.StatusFailed, domain.ChannelSMS},
		{domain.StatusPending, domain.ChannelEmail},
	}
	for i, s := range seed {
		_ = store.CreateNotification(context.Background(), &domain.Notification{
			ID:        fmt.Sprintf("00000000-0000-4000-8000-00000000000%d", i),
			Status:    s.status,
			Channel:   s.channel,
			CreatedAt: listBase.Add(time.Duration(i) * time.Minute),
		})
	}
	return store
}

func list(t *testing.T, h http.Handler, query string) listResponse {
	t.Helper()
	rec := doRequest(h, http.MethodGet, "/notifications?"+query, "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for %q, got %d: %s", query, rec.Code, rec.Body.String())
	}
	var resp listResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}

func TestListNotificationsFilters(t *testing.T) {
	h := newTestServer(seedList(t))

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{"no filter", "", 5},
		{"by status", "status=pending", 3},
		{"by channel", "channel=sms", 2},
		{"by status and channel", "status=pending&channel=email", 1},
		{"from", "from=" + url.QueryEscape(listBase.Add(2*time.Minute).Format(time.RFC3339)), 3},
		{"from and to", "from=" + url.QueryEscape(listBase.Add(time.Minute).Format(time.RFC3339)) +
			"&to=" + url.QueryEscape(listBase.Add(3*time.Minute).Format(time.RFC3339)), 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := list(t, h, tt.query)
			if len(resp.Data) != tt.want {
				t.Fatalf("expected %d items, got %d", tt.want, len(resp.Data))
			}
		})
	}
}

func TestListNotificationsNewestFirst(t *testing.T) {
	h := newTestServer(seedList(t))

	resp := list(t, h, "")
	for i := 1; i < len(resp.Data); i++ {
		if resp.Data[i].CreatedAt.After(resp.Data[i-1].CreatedAt) {
			t.Fatal("expected newest first ordering")
		}
	}
}

func TestListNotificationsPagination(t *testing.T) {
	h := newTestServer(seedList(t))

	seen := map[string]bool{}
	query := "limit=2"
	pages := 0
	for {
		resp := list(t, h, query)
		pages++
		for _, n := range resp.Data {
			if seen[n.ID] {
				t.Fatalf("notification %s returned twice", n.ID)
			}
			seen[n.ID] = true
		}
		if resp.NextCursor == "" {
			break
		}
		if len(resp.Data) != 2 {
			t.Fatalf("expected full page before cursor, got %d", len(resp.Data))
		}
		query = "limit=2&cursor=" + resp.NextCursor
		if pages > 10 {
			t.Fatal("pagination did not stop")
		}
	}

	if len(seen) != 5 || pages != 3 {
		t.Fatalf("expected 5 items over 3 pages, got %d items over %d pages", len(seen), pages)
	}
}

func TestListNotificationsInvalidParams(t *testing.T) {
	h := newTestServer(seedList(t))

	tests := []string{
		"status=done",
		"channel=fax",
		"batch_id=abc",
		"from=yesterday",
		"to=tomorrow",
		"from=2026-11-22T00:00:00Z&to=2026-11-21T00:00:00Z",
		"limit=0",
		"limit=101",
		"limit=ten",
		"cursor=not-a-cursor",
	}

	for _, query := range tests {
		t.Run(query, func(t *testing.T) {
			rec := doRequest(h, http.MethodGet, "/notifications?"+query, "", nil)
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("expected 422, got %d", rec.Code)
			}
		})
	}
}

func TestCursorRoundTrip(t *testing.T) {
	c := domain.Cursor{
		CreatedAt: time.Date(2026, 11, 21, 10, 30, 0, 123456000, time.UTC),
		ID:        "00000000-0000-4000-8000-000000000001",
	}
	got, err := decodeCursor(encodeCursor(c))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.CreatedAt.Equal(c.CreatedAt) || got.ID != c.ID {
		t.Fatalf("round trip mismatch: %+v vs %+v", got, c)
	}
}
