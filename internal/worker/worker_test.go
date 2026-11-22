package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
	"github.com/EdizKarli/event-driven-notification-system-task/internal/provider"
)

type fakeStore struct {
	mu           sync.Mutex
	pending      []*domain.Notification
	sent         map[string]string
	failed       map[string]string
	retries      map[string]time.Time
	recoverCalls atomic.Int32
}

func newFakeStore(items ...*domain.Notification) *fakeStore {
	return &fakeStore{
		pending: items,
		sent:    map[string]string{},
		failed:  map[string]string{},
		retries: map[string]time.Time{},
	}
}

func (f *fakeStore) ClaimNotifications(ctx context.Context, ch domain.Channel, limit int, lease time.Duration) ([]*domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var claimed []*domain.Notification
	for _, n := range f.pending {
		if len(claimed) == limit {
			break
		}
		if n.Channel == ch && n.Status == domain.StatusPending {
			n.Status = domain.StatusProcessing
			n.Attempts++
			copied := *n
			claimed = append(claimed, &copied)
		}
	}
	return claimed, nil
}

func (f *fakeStore) MarkSent(ctx context.Context, id, providerMessageID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent[id] = providerMessageID
	return nil
}

func (f *fakeStore) MarkFailed(ctx context.Context, id, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed[id] = reason
	return nil
}

func (f *fakeStore) MarkRetry(ctx context.Context, id, reason string, nextAttemptAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retries[id] = nextAttemptAt
	return nil
}

func (f *fakeStore) RecoverStuck(ctx context.Context) (int64, error) {
	f.recoverCalls.Add(1)
	return 0, nil
}

func (f *fakeStore) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent), len(f.failed)
}

type fakeSender struct {
	send func(n *domain.Notification) (*provider.Response, error)
}

func (s *fakeSender) Send(ctx context.Context, n *domain.Notification) (*provider.Response, error) {
	return s.send(n)
}

func okSender() *fakeSender {
	return &fakeSender{send: func(n *domain.Notification) (*provider.Response, error) {
		return &provider.Response{MessageID: "msg-" + n.ID}, nil
	}}
}

func pending(id string, ch domain.Channel) *domain.Notification {
	return &domain.Notification{
		ID:          id,
		Channel:     ch,
		Status:      domain.StatusPending,
		Priority:    domain.PriorityNormal,
		MaxAttempts: domain.DefaultMaxAttempts,
	}
}

func testConfig() Config {
	return Config{
		BatchSize:       10,
		Concurrency:     4,
		PollInterval:    10 * time.Millisecond,
		Lease:           time.Second,
		SendTimeout:     time.Second,
		RatePerSecond:   0,
		Backoff:         Backoff{Base: time.Second, Max: time.Minute},
		RecoverInterval: 10 * time.Millisecond,
	}
}

func newTestWorker(store Store, sender Sender, cfg Config) *Worker {
	return New(store, sender, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
}

func TestProcessBatchMarksSent(t *testing.T) {
	store := newFakeStore(pending("a", domain.ChannelSMS), pending("b", domain.ChannelSMS))
	w := newTestWorker(store, okSender(), testConfig())

	processed, err := w.processBatch(context.Background(), domain.ChannelSMS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processed != 2 {
		t.Fatalf("expected 2 processed, got %d", processed)
	}
	if store.sent["a"] != "msg-a" || store.sent["b"] != "msg-b" {
		t.Fatalf("unexpected sent map: %v", store.sent)
	}
}

func TestProcessBatchMarksFailed(t *testing.T) {
	store := newFakeStore(pending("a", domain.ChannelSMS))
	sender := &fakeSender{send: func(n *domain.Notification) (*provider.Response, error) {
		return nil, &provider.Error{StatusCode: 400, Retryable: false}
	}}
	w := newTestWorker(store, sender, testConfig())

	if _, err := w.processBatch(context.Background(), domain.ChannelSMS); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reason, ok := store.failed["a"]; !ok || !strings.Contains(reason, "400") {
		t.Fatalf("expected failure with status 400, got %q", reason)
	}
	if len(store.sent) != 0 {
		t.Fatal("nothing should be marked as sent")
	}
}

func TestProcessBatchOnlyClaimsItsChannel(t *testing.T) {
	store := newFakeStore(pending("sms-1", domain.ChannelSMS), pending("email-1", domain.ChannelEmail))
	w := newTestWorker(store, okSender(), testConfig())

	if _, err := w.processBatch(context.Background(), domain.ChannelSMS); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := store.sent["sms-1"]; !ok {
		t.Fatal("sms notification should be sent")
	}
	if _, ok := store.sent["email-1"]; ok {
		t.Fatal("email notification should not be touched by the sms worker")
	}
}

func TestProcessBatchRespectsBatchSize(t *testing.T) {
	store := newFakeStore(
		pending("1", domain.ChannelSMS), pending("2", domain.ChannelSMS), pending("3", domain.ChannelSMS),
		pending("4", domain.ChannelSMS), pending("5", domain.ChannelSMS),
	)
	cfg := testConfig()
	cfg.BatchSize = 2
	w := newTestWorker(store, okSender(), cfg)

	processed, _ := w.processBatch(context.Background(), domain.ChannelSMS)
	if processed != 2 {
		t.Fatalf("expected 2 processed, got %d", processed)
	}
}

func TestProcessBatchLimitsConcurrency(t *testing.T) {
	var items []*domain.Notification
	for i := 0; i < 8; i++ {
		items = append(items, pending(string(rune('a'+i)), domain.ChannelSMS))
	}
	store := newFakeStore(items...)

	var inFlight, maxInFlight atomic.Int32
	sender := &fakeSender{send: func(n *domain.Notification) (*provider.Response, error) {
		current := inFlight.Add(1)
		for {
			seen := maxInFlight.Load()
			if current <= seen || maxInFlight.CompareAndSwap(seen, current) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		return &provider.Response{MessageID: "ok"}, nil
	}}

	cfg := testConfig()
	cfg.Concurrency = 2
	w := newTestWorker(store, sender, cfg)

	if _, err := w.processBatch(context.Background(), domain.ChannelSMS); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := maxInFlight.Load(); got > 2 {
		t.Fatalf("expected at most 2 concurrent sends, got %d", got)
	}
	if sent, _ := store.counts(); sent != 8 {
		t.Fatalf("expected 8 sent, got %d", sent)
	}
}

func TestRunProcessesAllChannelsAndStops(t *testing.T) {
	store := newFakeStore(
		pending("sms-1", domain.ChannelSMS),
		pending("email-1", domain.ChannelEmail),
		pending("push-1", domain.ChannelPush),
	)
	w := newTestWorker(store, okSender(), testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if sent, _ := store.counts(); sent == 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after context was cancelled")
	}
	if sent, _ := store.counts(); sent != 3 {
		t.Fatalf("expected 3 sent, got %d", sent)
	}
}

func manyPending(prefix string, ch domain.Channel, count int) []*domain.Notification {
	items := make([]*domain.Notification, count)
	for i := range items {
		items[i] = pending(prefix+string(rune('a'+i)), ch)
	}
	return items
}

func TestRateLimitSlowsDownSending(t *testing.T) {
	store := newFakeStore(manyPending("sms-", domain.ChannelSMS, 20)...)
	cfg := testConfig()
	cfg.BatchSize = 20
	cfg.Concurrency = 10
	cfg.RatePerSecond = 50
	w := newTestWorker(store, okSender(), cfg)

	start := time.Now()
	if _, err := w.processBatch(context.Background(), domain.ChannelSMS); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 300*time.Millisecond {
		t.Fatalf("20 sends at 50/s should take about 380ms, took %v", elapsed)
	}
	if sent, _ := store.counts(); sent != 20 {
		t.Fatalf("expected 20 sent, got %d", sent)
	}
}

func TestRateLimitIsPerChannel(t *testing.T) {
	items := append(manyPending("sms-", domain.ChannelSMS, 10), manyPending("email-", domain.ChannelEmail, 10)...)
	store := newFakeStore(items...)
	cfg := testConfig()
	cfg.RatePerSecond = 50
	w := newTestWorker(store, okSender(), cfg)

	start := time.Now()
	var wg sync.WaitGroup
	for _, ch := range []domain.Channel{domain.ChannelSMS, domain.ChannelEmail} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = w.processBatch(context.Background(), ch)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	if elapsed > 330*time.Millisecond {
		t.Fatalf("channels should be limited independently (about 180ms), took %v", elapsed)
	}
	if sent, _ := store.counts(); sent != 20 {
		t.Fatalf("expected 20 sent, got %d", sent)
	}
}

func TestNoRateLimitWhenDisabled(t *testing.T) {
	store := newFakeStore(manyPending("sms-", domain.ChannelSMS, 20)...)
	cfg := testConfig()
	cfg.BatchSize = 20
	w := newTestWorker(store, okSender(), cfg)

	start := time.Now()
	_, _ = w.processBatch(context.Background(), domain.ChannelSMS)
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("expected no throttling, took %v", elapsed)
	}
}

func failingSender(err error) *fakeSender {
	return &fakeSender{send: func(n *domain.Notification) (*provider.Response, error) {
		return nil, err
	}}
}

func TestRetryableErrorSchedulesRetry(t *testing.T) {
	store := newFakeStore(pending("a", domain.ChannelSMS))
	w := newTestWorker(store, failingSender(&provider.Error{StatusCode: 503, Retryable: true}), testConfig())

	before := time.Now()
	_, _ = w.processBatch(context.Background(), domain.ChannelSMS)

	next, ok := store.retries["a"]
	if !ok {
		t.Fatal("expected a retry to be scheduled")
	}
	if !next.After(before) {
		t.Errorf("expected next attempt in the future, got %v", next)
	}
	if _, failed := store.failed["a"]; failed {
		t.Error("notification should not be failed yet")
	}
}

func TestPermanentErrorFailsImmediately(t *testing.T) {
	store := newFakeStore(pending("a", domain.ChannelSMS))
	w := newTestWorker(store, failingSender(&provider.Error{StatusCode: 400, Retryable: false}), testConfig())

	_, _ = w.processBatch(context.Background(), domain.ChannelSMS)

	if _, ok := store.failed["a"]; !ok {
		t.Fatal("expected notification to fail on first attempt")
	}
	if _, ok := store.retries["a"]; ok {
		t.Fatal("permanent errors must not be retried")
	}
}

func TestLastAttemptFails(t *testing.T) {
	n := pending("a", domain.ChannelSMS)
	n.Attempts = domain.DefaultMaxAttempts - 1
	store := newFakeStore(n)
	w := newTestWorker(store, failingSender(&provider.Error{StatusCode: 500, Retryable: true}), testConfig())

	_, _ = w.processBatch(context.Background(), domain.ChannelSMS)

	if _, ok := store.failed["a"]; !ok {
		t.Fatal("expected notification to fail after the last attempt")
	}
	if _, ok := store.retries["a"]; ok {
		t.Fatal("no retry should be scheduled after the last attempt")
	}
}

func TestUnknownErrorIsRetried(t *testing.T) {
	store := newFakeStore(pending("a", domain.ChannelSMS))
	w := newTestWorker(store, failingSender(errors.New("something unexpected")), testConfig())

	_, _ = w.processBatch(context.Background(), domain.ChannelSMS)

	if _, ok := store.retries["a"]; !ok {
		t.Fatal("expected unknown errors to be retried")
	}
}

func TestRunRecoversStuckNotifications(t *testing.T) {
	store := newFakeStore()
	w := newTestWorker(store, okSender(), testConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	w.Run(ctx)

	if store.recoverCalls.Load() == 0 {
		t.Fatal("expected the recovery loop to run")
	}
}

type countingMetrics struct {
	sent, failed, retried atomic.Int32
}

func (m *countingMetrics) RecordSent(domain.Channel, time.Duration, time.Duration) { m.sent.Add(1) }
func (m *countingMetrics) RecordFailed(domain.Channel)                             { m.failed.Add(1) }
func (m *countingMetrics) RecordRetry(domain.Channel)                              { m.retried.Add(1) }

func TestWorkerRecordsMetrics(t *testing.T) {
	last := pending("last", domain.ChannelSMS)
	last.Attempts = domain.DefaultMaxAttempts - 1
	store := newFakeStore(pending("ok", domain.ChannelSMS), pending("retry", domain.ChannelSMS), last)

	sender := &fakeSender{send: func(n *domain.Notification) (*provider.Response, error) {
		if n.ID == "ok" {
			return &provider.Response{MessageID: "m"}, nil
		}
		return nil, &provider.Error{StatusCode: 503, Retryable: true}
	}}

	m := &countingMetrics{}
	w := New(store, sender, m, slog.New(slog.NewTextHandler(io.Discard, nil)), testConfig())
	_, _ = w.processBatch(context.Background(), domain.ChannelSMS)

	if m.sent.Load() != 1 || m.retried.Load() != 1 || m.failed.Load() != 1 {
		t.Fatalf("expected 1 sent, 1 retried, 1 failed; got %d, %d, %d", m.sent.Load(), m.retried.Load(), m.failed.Load())
	}
}
