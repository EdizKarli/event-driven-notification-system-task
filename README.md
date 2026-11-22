# Event-Driven Notification System

A notification service written in Go that accepts SMS, email and push notifications through a REST API, queues them in PostgreSQL and delivers them asynchronously through an external provider (simulated with [webhook.site](https://webhook.site)).

It supports priorities, batch creation, scheduling, cancellation, idempotent requests, per channel rate limiting, retries with exponential backoff, recovery of stuck work, structured logging with correlation ids and a real-time metrics endpoint.

## Contents

- [Quick start](#quick-start)
- [Architecture](#architecture)
- [Design decisions](#design-decisions)
- [API](#api)
- [Configuration](#configuration)
- [Testing](#testing)
- [Project structure](#project-structure)
- [Trade-offs and future work](#trade-offs-and-future-work)

## Quick start

**Requirements:** Docker with Docker Compose. Go 1.27+ and `make` are only needed to run the tests locally.

**1. Create a webhook.site URL that acts as the provider.**

Open [webhook.site](https://webhook.site), copy your unique URL, click **Edit** and set:

| Setting | Value |
|---|---|
| Default status code | `202` |
| Content type | `application/json` |
| Response body | `{"messageId": "3f6c1e2a-9b8d-4c7e-a1f0-2d3e4f5a6b7c", "status": "accepted", "timestamp": "2026-11-22T12:00:00Z"}` |

**2. Configure and start.**

```bash
cp .env.example .env
# set PROVIDER_URL in .env to your webhook.site URL
docker compose up --build
```

This starts PostgreSQL, applies the migrations, and starts the application and Swagger UI.

| Service | URL |
|---|---|
| API | http://localhost:8080 |
| Swagger UI | http://localhost:8081 |
| PostgreSQL | localhost:5432 (user `notify`, password `notify`) |

**3. Send a notification.**

```bash
curl -i -X POST http://localhost:8080/notifications \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Hello","priority":"high"}'
```

Within a second the request appears on your webhook.site page and `GET /notifications/{id}` shows `"status": "sent"`.

## Architecture

```
client ──HTTP──▶ ┌───────────────────────── Go application ─────────────────────────┐
                 │  API     validate, store as pending, serve status and /metrics   │
                 │  Worker  per channel: claim → rate limit → send → record result  │──HTTP──▶ provider (webhook.site)
                 └─────────────────────────────────┬────────────────────────────────┘
                                                   │ claim with FOR UPDATE SKIP LOCKED
                                                   ▼
                            PostgreSQL: notifications table = state + queue
```

The API and the worker run in the same binary but share nothing except the database, so they can be split into separate deployments without code changes.

**Lifecycle of a notification:**

```
            ┌─────────── transient error, attempts left ───────────┐
            ▼                                                      │
POST ──▶ pending ──(due, claimed)──▶ processing ──(2xx)──▶ sent    │
            │                            ├─────────────────────────┘
            │                            └──(permanent error or no attempts left)──▶ failed
            └──(cancel)──▶ cancelled
```

A notification waiting for its scheduled time or for a retry is simply `pending` with `next_attempt_at` in the future.

## Design decisions

### PostgreSQL as the queue

The `notifications` table is both the source of truth and the work queue. The worker claims work with one statement:

```sql
UPDATE notifications
SET status = 'processing', attempts = attempts + 1, locked_until = now() + interval '30 seconds'
WHERE id IN (
    SELECT id FROM notifications
    WHERE status = 'pending' AND channel = $1 AND next_attempt_at <= now()
    ORDER BY priority, next_attempt_at
    LIMIT 50
    FOR UPDATE SKIP LOCKED)
RETURNING ...
```

- `FOR UPDATE SKIP LOCKED` lets any number of workers poll at the same time without ever claiming the same row.
- `ORDER BY priority` gives the priority queue (high, normal, low).
- `next_attempt_at` covers both scheduled delivery and retry backoff with one mechanism.
- Status changes, cancellation and status queries are plain SQL on the same row, so there is no dual write between a database and a separate broker that could drift apart.
- A partial index on pending rows keeps the claim query fast as the table grows.

The required throughput (at most 100 messages per second per channel) is well within what PostgreSQL handles comfortably. At a much larger scale a dedicated broker such as Kafka or RabbitMQ would be the next step.

### Rate limiting

Each channel has its own token bucket (`golang.org/x/time/rate`) set to 100 tokens per second with a bucket size of 1, so sends are spaced roughly every 10 ms instead of bursting 100 at once. A separate semaphore limits how many sends run at the same time (10 per channel). Channels are limited independently, so a burst of SMS does not slow down email.

### Delivery and retry

Every provider response is classified:

| Outcome | Examples | Action |
|---|---|---|
| Success | 2xx | `sent`, provider message id stored |
| Transient | timeout, connection error, 408, 429, 5xx | retry with backoff |
| Permanent | other 4xx | `failed` immediately, retrying would be rejected again |

- **Exponential backoff with jitter.** The delay starts at 2.5 to 5 seconds and doubles after each failure, capped at 5 minutes. The random part spreads retries out after a provider outage instead of sending them all at once.
- **At most 5 attempts.** After that the notification is `failed` and `last_error` holds the reason.
- **Retries live in the database.** A retry is stored as `pending` with a future `next_attempt_at`, not as a sleeping goroutine. Retries survive restarts and a notification waiting for a retry can still be cancelled.
- **Lease recovery.** A claimed notification holds a 30 second lease (`locked_until`). If the process dies mid-delivery, a recovery loop returns expired leases to `pending`, or to `failed` if no attempts are left. Migration `000002` adds a partial index for this query.
- **Delivery guarantee: at least once.** If the process crashes after the provider accepted a message but before the database was updated, the message can be sent again. To let the provider drop such duplicates, every request carries the notification id as an `Idempotency-Key` header.

### Idempotency

`POST /notifications` accepts an `Idempotency-Key` header. Deduplication is enforced by a unique column and `INSERT ... ON CONFLICT DO NOTHING`, so even concurrent requests with the same key create exactly one notification. A repeated request returns the original notification with `Idempotent-Replayed: true`. Reusing a key with a different payload returns `409` instead of silently returning the old notification.

### Other decisions

- **Batches are all or nothing.** One invalid item rejects the whole batch and the error names the item (`notifications[3].recipient`). The batch and all of its notifications are inserted in one transaction with a single `INSERT ... SELECT FROM unnest(...)`.
- **Cursor pagination.** Lists use a keyset cursor on `(created_at, id)` instead of `OFFSET`, so pages stay stable while new notifications arrive and deep pages stay fast.
- **Atomic cancel.** `UPDATE ... WHERE status = 'pending'` checks and changes the status in one statement, so a cancel cannot race with the worker.
- **202 Accepted** for creation, since delivery happens later.
- **Validation** collects all field errors at once: E.164 numbers and at most 1600 characters for SMS, a valid address and at most 10000 characters for email, a device token and at most 4096 bytes for push.

### Observability

- **Structured logs.** JSON logs via `log/slog`. Every request gets an `X-Correlation-ID` (taken from the request or generated). It is stored with the notification, so the API log line and the worker's delivery log lines for the same notification share the same id.
- **Metrics.** `GET /metrics` returns queue depth per channel (ready by priority, scheduled, processing) read live from the database, and per channel sent, failed and retried counters, success rate, and provider and end-to-end latency (average, p50, p95, p99 over the last 1000 deliveries).
- **Health.** `GET /health` checks the database connection and returns `503` when it is down.

## API

Full specification: [`api/openapi.yaml`](api/openapi.yaml), browsable at http://localhost:8081 when the stack is running.

| Method | Path | Description |
|---|---|---|
| `POST` | `/notifications` | Create a notification (supports `Idempotency-Key`) |
| `POST` | `/notifications/batch` | Create up to 1000 notifications |
| `GET` | `/notifications/{id}` | Get a notification |
| `GET` | `/notifications` | List with `status`, `channel`, `batch_id`, `from`, `to`, `limit`, `cursor` |
| `POST` | `/notifications/{id}/cancel` | Cancel a pending notification |
| `GET` | `/batches/{id}` | Batch summary with counts per status |
| `GET` | `/metrics` | Real-time metrics |
| `GET` | `/health` | Health check |

### Examples

**Create with an idempotency key**

```bash
curl -X POST http://localhost:8080/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-42-shipped' \
  -d '{"recipient":"user@example.com","channel":"email","content":"Your order has shipped"}'
```

```json
{
  "id": "6a1f0b7e-2c3d-4e5f-8a9b-0c1d2e3f4a5b",
  "idempotency_key": "order-42-shipped",
  "recipient": "user@example.com",
  "channel": "email",
  "content": "Your order has shipped",
  "priority": "normal",
  "status": "pending",
  "attempts": 0,
  "max_attempts": 5,
  "next_attempt_at": "2026-11-22T12:00:00Z",
  "correlation_id": "4f2a9c1e8b7d6a5f4e3d2c1b0a9f8e7d",
  "created_at": "2026-11-22T12:00:00Z",
  "updated_at": "2026-11-22T12:00:00Z"
}
```

**Schedule for later**

```bash
curl -X POST http://localhost:8080/notifications \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Reminder","scheduled_at":"2030-01-01T09:00:00Z"}'
```

**Create a batch**

```bash
curl -X POST http://localhost:8080/notifications/batch \
  -H 'Content-Type: application/json' \
  -d '{"notifications":[
        {"recipient":"+905551234567","channel":"sms","content":"Flash sale starts now"},
        {"recipient":"user@example.com","channel":"email","content":"Flash sale starts now","priority":"high"},
        {"recipient":"device-token-1","channel":"push","content":"Flash sale starts now","priority":"low"}]}'
```

```json
{ "batch_id": "c0ffee00-1234-4abc-9def-0123456789ab", "total": 3, "notification_ids": ["...", "...", "..."] }
```

**Batch status and list by batch**

```bash
curl http://localhost:8080/batches/c0ffee00-1234-4abc-9def-0123456789ab
curl 'http://localhost:8080/notifications?batch_id=c0ffee00-1234-4abc-9def-0123456789ab'
```

**List with filters and pagination**

```bash
curl 'http://localhost:8080/notifications?status=failed&channel=sms&from=2026-11-22T00:00:00Z&limit=50'
curl 'http://localhost:8080/notifications?limit=50&cursor=<next_cursor from the previous page>'
```

**Cancel**

```bash
curl -X POST http://localhost:8080/notifications/6a1f0b7e-2c3d-4e5f-8a9b-0c1d2e3f4a5b/cancel
```

**Validation error (422)**

```json
{
  "error": "validation failed",
  "details": [
    { "field": "recipient", "message": "must be a phone number in E.164 format, e.g. +905551234567" }
  ]
}
```

## Configuration

Settings are read from environment variables. Docker Compose loads them from `.env`; every value except `PROVIDER_URL` has a working default.

| Variable | Default | Description |
|---|---|---|
| `PROVIDER_URL` | none | Provider endpoint, your webhook.site URL |
| `DATABASE_URL` | built from the Postgres settings | PostgreSQL connection string |
| `PORT` | `8080` | Port the application listens on inside the container |
| `APP_PORT` | `8080` | Host port for the API |
| `SWAGGER_PORT` | `8081` | Host port for Swagger UI |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | `notify` / `notify` / `notifications` | Database credentials |
| `POSTGRES_PORT` | `5432` | Host port for PostgreSQL |

Worker settings (100 messages per second per channel, 10 concurrent sends per channel, 5 attempts, backoff from 5 seconds up to 5 minutes, 30 second lease) are defined in `worker.DefaultConfig()`.

## Testing

```bash
make test        # go test ./...
make test-race   # with the race detector (needs cgo)
```

The suite covers validation rules, every HTTP handler (against an in-memory store), cursor pagination, idempotency, the provider client against `httptest` servers (success, every error class, timeouts, refused connections), the worker (claiming per channel, bounded concurrency, rate limiting, retry and failure paths, metrics) and backoff and metrics calculations. It runs offline and needs no database.

Other `make` targets: `build`, `run`, `fmt`, `up`, `down`, `logs`.

## Project structure

```
cmd/server/          entry point: config, wiring, graceful shutdown
internal/domain/     types, validation rules, errors
internal/api/        HTTP handlers, middleware (correlation id, logging, recovery, CORS)
internal/store/      PostgreSQL access: CRUD, batch insert, claim, retry, recovery, queue depth
internal/worker/     delivery loop, rate limiting, retries, backoff, lease recovery
internal/provider/   provider HTTP client and error classification
internal/metrics/    in-memory counters and latency percentiles
migrations/          versioned SQL migrations (golang-migrate)
api/openapi.yaml     OpenAPI 3 specification
```

## Trade-offs and future work

The scope was kept to the requirements and a few bonus features within the expected time. Known limitations and the next steps I would take:

- **Rate limit across instances.** The limiter lives in process memory, which is exact for one instance. Running several instances would need a shared limiter, for example a token bucket in Redis.
- **Priority starvation.** Strict priority ordering can delay low priority messages during a long stream of high priority traffic. Aging (raising priority with waiting time) would fix this.
- **Batch idempotency.** Idempotency keys are supported for single notifications. Batches would need a key column on `batches`.
- **Prometheus.** Metrics are JSON and reset on restart. In production they would be Prometheus counters and histograms with Grafana dashboards and alerts on queue growth and success rate.
- **Real providers.** One generic provider client is used for all channels. Real SMS, email and push providers would each get their own client behind the same interface, and the static `messageId` returned by webhook.site would be a real per-message id.
- **Not implemented bonus features:** message templates, WebSocket status updates and distributed tracing. Templates would be a `templates` table with variables rendered at creation time; WebSocket updates could use Postgres `LISTEN/NOTIFY` from the worker; tracing would use OpenTelemetry with the trace context stored next to the correlation id.
- **Retention.** Delivered notifications stay in the table forever. A production setup would archive or partition by date.
- **Authentication** is out of scope.
