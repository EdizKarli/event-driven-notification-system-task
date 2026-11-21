package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

const notificationColumns = `id::text, batch_id::text, idempotency_key, recipient, channel::text, content,
	priority, status::text, attempts, max_attempts, next_attempt_at, provider_message_id, last_error,
	correlation_id, created_at, updated_at, sent_at`

func scanNotification(row pgx.Row) (*domain.Notification, error) {
	var (
		n             domain.Notification
		channel       string
		status        string
		priority      int16
		correlationID *string
	)
	err := row.Scan(
		&n.ID, &n.BatchID, &n.IdempotencyKey, &n.Recipient, &channel, &n.Content,
		&priority, &status, &n.Attempts, &n.MaxAttempts, &n.NextAttemptAt, &n.ProviderMessageID, &n.LastError,
		&correlationID, &n.CreatedAt, &n.UpdatedAt, &n.SentAt,
	)
	if err != nil {
		return nil, err
	}
	n.Channel = domain.Channel(channel)
	n.Status = domain.Status(status)
	n.Priority = domain.PriorityFromRank(int(priority))
	if correlationID != nil {
		n.CorrelationID = *correlationID
	}
	return &n, nil
}

func (s *Store) CreateNotification(ctx context.Context, n *domain.Notification) error {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO notifications (id, recipient, channel, content, priority, max_attempts, next_attempt_at, correlation_id)
		VALUES ($1::text::uuid, $2, $3::text::notification_channel, $4, $5, $6, $7, NULLIF($8::text, ''))
		RETURNING `+notificationColumns,
		n.ID, n.Recipient, string(n.Channel), n.Content, n.Priority.Rank(), n.MaxAttempts, n.NextAttemptAt, n.CorrelationID,
	)
	created, err := scanNotification(row)
	if err != nil {
		return fmt.Errorf("insert notification: %w", err)
	}
	*n = *created
	return nil
}

func (s *Store) GetNotification(ctx context.Context, id string) (*domain.Notification, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+notificationColumns+` FROM notifications WHERE id = $1::text::uuid`, id)
	n, err := scanNotification(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get notification: %w", err)
	}
	return n, nil
}
