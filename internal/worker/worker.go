package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
	"github.com/EdizKarli/event-driven-notification-system-task/internal/provider"
)

const storeTimeout = 5 * time.Second

type Store interface {
	ClaimNotifications(ctx context.Context, channel domain.Channel, limit int, lease time.Duration) ([]*domain.Notification, error)
	MarkSent(ctx context.Context, id, providerMessageID string) error
	MarkFailed(ctx context.Context, id, reason string) error
	MarkRetry(ctx context.Context, id, reason string, nextAttemptAt time.Time) error
	RecoverStuck(ctx context.Context) (int64, error)
}

type Metrics interface {
	RecordSent(ch domain.Channel, providerLatency, deliveryLatency time.Duration)
	RecordFailed(ch domain.Channel)
	RecordRetry(ch domain.Channel)
}

type noopMetrics struct{}

func (noopMetrics) RecordSent(domain.Channel, time.Duration, time.Duration) {}
func (noopMetrics) RecordFailed(domain.Channel) {}
func (noopMetrics) RecordRetry(domain.Channel) {}

type Sender interface {
	Send(ctx context.Context, n *domain.Notification) (*provider.Response, error)
}

type Config struct {
	BatchSize       int
	Concurrency     int
	PollInterval    time.Duration
	Lease           time.Duration
	SendTimeout     time.Duration
	RatePerSecond   float64
	Backoff         Backoff
	RecoverInterval time.Duration
}

func DefaultConfig() Config {
	return Config{
		BatchSize:       50,
		Concurrency:     10,
		PollInterval:    500 * time.Millisecond,
		Lease:           30 * time.Second,
		SendTimeout:     10 * time.Second,
		RatePerSecond:   100,
		Backoff:         Backoff{Base: 5 * time.Second, Max: 5 * time.Minute},
		RecoverInterval: 30 * time.Second,
	}
}

type Worker struct {
	store    Store
	sender   Sender
	metrics  Metrics
	logger   *slog.Logger
	cfg      Config
	limiters map[domain.Channel]*rate.Limiter
}

func New(store Store, sender Sender, m Metrics, logger *slog.Logger, cfg Config) *Worker {
	if m == nil {
		m = noopMetrics{}
	}

	limit := rate.Inf
	if cfg.RatePerSecond > 0 {
		limit = rate.Limit(cfg.RatePerSecond)
	}

	limiters := make(map[domain.Channel]*rate.Limiter, len(domain.Channels))
	for _, ch := range domain.Channels {
		limiters[ch] = rate.NewLimiter(limit, 1)
	}

	return &Worker{store: store, sender: sender, metrics: m, logger: logger, cfg: cfg, limiters: limiters}
}

func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.runRecovery(ctx)
	}()

	for _, ch := range domain.Channels {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.runChannel(ctx, ch)
		}()
	}
	wg.Wait()
}

func (w *Worker) runChannel(ctx context.Context, ch domain.Channel) {
	w.logger.Info("worker started", "channel", ch)
	for {
		if ctx.Err() != nil {
			w.logger.Info("worker stopped", "channel", ch)
			return
		}

		processed, err := w.processBatch(ctx, ch)
		if err != nil && ctx.Err() == nil {
			w.logger.Error("process batch failed", "channel", ch, "error", err)
		}

		if processed == 0 || err != nil {
			select {
			case <-ctx.Done():
			case <-time.After(w.cfg.PollInterval):
			}
		}
	}
}

func (w *Worker) processBatch(ctx context.Context, ch domain.Channel) (int, error) {
	items, err := w.store.ClaimNotifications(ctx, ch, w.cfg.BatchSize, w.cfg.Lease)
	if err != nil {
		return 0, err
	}

	limiter := w.limiters[ch]
	sem := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, n := range items {
		_ = limiter.Wait(context.WithoutCancel(ctx))
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			w.deliver(ctx, n)
		}()
	}
	wg.Wait()
	return len(items), nil
}

func (w *Worker) deliver(ctx context.Context, n *domain.Notification) {
	sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.SendTimeout)
	defer cancel()

	log := w.logger.With(
		"notification_id", n.ID,
		"channel", n.Channel,
		"priority", n.Priority,
		"attempt", n.Attempts,
		"correlation_id", n.CorrelationID,
	)

	start := time.Now()
	resp, err := w.sender.Send(sendCtx, n)
	providerLatency := time.Since(start)
	duration := providerLatency.Milliseconds()

	storeCtx, cancelStore := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
	defer cancelStore()

	if err == nil {
		if err := w.store.MarkSent(storeCtx, n.ID, resp.MessageID); err != nil {
			log.Error("could not mark notification as sent", "error", err)
			return
		}
		w.metrics.RecordSent(n.Channel, providerLatency, time.Since(n.CreatedAt))
		log.Info("notification sent", "provider_message_id", resp.MessageID, "duration_ms", duration)
		return
	}

	w.handleFailure(storeCtx, log, n, err, duration)
}

func (w *Worker) handleFailure(ctx context.Context, log *slog.Logger, n *domain.Notification, sendErr error, duration int64) {
	retryable := true
	var perr *provider.Error
	if errors.As(sendErr, &perr) {
		retryable = perr.Retryable
	}

	if retryable && n.Attempts < n.MaxAttempts {
		delay := w.cfg.Backoff.Delay(n.Attempts)
		if err := w.store.MarkRetry(ctx, n.ID, sendErr.Error(), time.Now().Add(delay)); err != nil {
			log.Error("could not schedule retry", "error", err)
			return
		}
		w.metrics.RecordRetry(n.Channel)
		log.Warn("notification will be retried",
			"error", sendErr,
			"retry_in_ms", delay.Milliseconds(),
			"duration_ms", duration,
		)
		return
	}

	reason := "permanent error"
	if retryable {
		reason = "max attempts reached"
	}
	if err := w.store.MarkFailed(ctx, n.ID, sendErr.Error()); err != nil {
		log.Error("could not mark notification as failed", "error", err)
		return
	}
	w.metrics.RecordFailed(n.Channel)
	log.Error("notification failed", "reason", reason, "error", sendErr, "duration_ms", duration)
}

func (w *Worker) runRecovery(ctx context.Context) {
	if w.cfg.RecoverInterval <= 0 {
		return
	}
	ticker := time.NewTicker(w.cfg.RecoverInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recovered, err := w.store.RecoverStuck(ctx)
			if err != nil {
				if ctx.Err() == nil {
					w.logger.Error("recover stuck notifications failed", "error", err)
				}
				continue
			}
			if recovered > 0 {
				w.logger.Warn("recovered stuck notifications", "count", recovered)
			}
		}
	}
}
