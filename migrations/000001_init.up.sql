CREATE TYPE notification_channel AS ENUM ('sms', 'email', 'push');

CREATE TYPE notification_status AS ENUM ('pending', 'processing', 'sent', 'failed', 'cancelled');

CREATE TABLE batches (
    id         UUID PRIMARY KEY,
    total      INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE notifications (
    id                  UUID PRIMARY KEY,
    batch_id            UUID REFERENCES batches (id),
    idempotency_key     TEXT UNIQUE,
    recipient           TEXT NOT NULL,
    channel             notification_channel NOT NULL,
    content             TEXT NOT NULL,
    priority            SMALLINT NOT NULL DEFAULT 1 CHECK (priority BETWEEN 0 AND 2),
    status              notification_status NOT NULL DEFAULT 'pending',
    attempts            INT NOT NULL DEFAULT 0,
    max_attempts        INT NOT NULL DEFAULT 5,
    next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until        TIMESTAMPTZ,
    provider_message_id TEXT,
    last_error          TEXT,
    correlation_id      TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at             TIMESTAMPTZ
);

CREATE INDEX idx_notifications_due
    ON notifications (channel, priority, next_attempt_at)
    WHERE status = 'pending';

CREATE INDEX idx_notifications_created
    ON notifications (created_at DESC, id DESC);

CREATE INDEX idx_notifications_batch
    ON notifications (batch_id);
