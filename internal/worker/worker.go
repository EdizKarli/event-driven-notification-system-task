package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
	"github.com/EdizKarli/event-driven-notification-system-task/internal/provider"
)

type Store interface {
	ClaimNotifications(ctx context.Context, channel domain.Channel, limit int, lease time.Duration) ([]*domain.Notification, error)
	MarkSent(ctx context.Context, id, providerMessageID string) error
	MarkFailed(ctx context.Context, id, reason string) error
}

type Sender interface {
	Send(ctx context.Context, n *domain.Notification) (*provider.Response, error)
}

type Config struct {
	BatchSize    int
	Concurrency  int
	PollInterval time.Duration
	Lease        time.Duration
	SendTimeout  time.Duration
}

func DefaultConfig() Config {
	return Config{
		BatchSize:    50,
		Concurrency:  10,
		PollInterval: 500 * time.Millisecond,
		Lease:        30 * time.Second,
		SendTimeout:  10 * time.Second,
	}
}

type Worker struct {
	store  Store
	sender Sender
	logger *slog.Logger
	cfg    Config
}

func New(store Store, sender Sender, logger *slog.Logger, cfg Config) *Worker {
	return &Worker{store: store, sender: sender, logger: logger, cfg: cfg}
}

func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
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

	sem := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, n := range items {
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
	duration := time.Since(start).Milliseconds()

	if err == nil {
		if err := w.store.MarkSent(sendCtx, n.ID, resp.MessageID); err != nil {
			log.Error("could not mark notification as sent", "error", err)
			return
		}
		log.Info("notification sent", "provider_message_id", resp.MessageID, "duration_ms", duration)
		return
	}

	if err := w.store.MarkFailed(sendCtx, n.ID, err.Error()); err != nil {
		log.Error("could not mark notification as failed", "error", err)
		return
	}
	log.Warn("notification failed", "error", err, "duration_ms", duration)
}
