package store

import (
	"context"
	"errors"
	"fmt"
	"time"

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

func (s *Store) CreateBatch(ctx context.Context, batch *domain.Batch, items []*domain.Notification) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx,
		`INSERT INTO batches (id, total, created_at) VALUES ($1::text::uuid, $2, $3)`,
		batch.ID, batch.Total, batch.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert batch: %w", err)
	}

	var (
		ids            = make([]string, len(items))
		recipients     = make([]string, len(items))
		channels       = make([]string, len(items))
		contents       = make([]string, len(items))
		priorities     = make([]int16, len(items))
		maxAttempts    = make([]int32, len(items))
		nextAttemptAts = make([]time.Time, len(items))
		correlationIDs = make([]string, len(items))
	)
	for i, n := range items {
		ids[i] = n.ID
		recipients[i] = n.Recipient
		channels[i] = string(n.Channel)
		contents[i] = n.Content
		priorities[i] = int16(n.Priority.Rank())
		maxAttempts[i] = int32(n.MaxAttempts)
		nextAttemptAts[i] = n.NextAttemptAt
		correlationIDs[i] = n.CorrelationID
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO notifications
			(id, batch_id, recipient, channel, content, priority, max_attempts, next_attempt_at, correlation_id)
		SELECT u.id::uuid, $1::text::uuid, u.recipient, u.channel::notification_channel, u.content,
			u.priority, u.max_attempts, u.next_attempt_at, NULLIF(u.correlation_id, '')
		FROM unnest($2::text[], $3::text[], $4::text[], $5::text[], $6::int2[], $7::int4[], $8::timestamptz[], $9::text[])
			AS u(id, recipient, channel, content, priority, max_attempts, next_attempt_at, correlation_id)`,
		batch.ID, ids, recipients, channels, contents, priorities, maxAttempts, nextAttemptAts, correlationIDs,
	)
	if err != nil {
		return fmt.Errorf("insert batch notifications: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}
	return nil
}

func (s *Store) GetBatch(ctx context.Context, id string) (*domain.Batch, error) {
	b := &domain.Batch{StatusCounts: map[domain.Status]int{}}
	err := s.pool.QueryRow(ctx,
		`SELECT id::text, total, created_at FROM batches WHERE id = $1::text::uuid`, id,
	).Scan(&b.ID, &b.Total, &b.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrBatchNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get batch: %w", err)
	}

	rows, err := s.pool.Query(ctx,
		`SELECT status::text, count(*) FROM notifications WHERE batch_id = $1::text::uuid GROUP BY status`, id,
	)
	if err != nil {
		return nil, fmt.Errorf("count batch statuses: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			status string
			count  int
		)
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("scan batch status: %w", err)
		}
		b.StatusCounts[domain.Status(status)] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read batch statuses: %w", err)
	}
	return b, nil
}
