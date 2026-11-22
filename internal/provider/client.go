package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

const maxResponseBytes = 1 << 20

type request struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type Response struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

type Error struct {
	StatusCode int
	Retryable  bool
	Err        error
}

func (e *Error) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("provider returned status %d: %v", e.StatusCode, e.Err)
	}
	return fmt.Sprintf("provider request failed: %v", e.Err)
}

func (e *Error) Unwrap() error {
	return e.Err
}

type Client struct {
	url        string
	httpClient *http.Client
}

func NewClient(url string, timeout time.Duration) *Client {
	return &Client{
		url:        url,
		httpClient: &http.Client{Timeout: timeout},
	}
}

func (c *Client) Send(ctx context.Context, n *domain.Notification) (*Response, error) {
	body, err := json.Marshal(request{
		To:      n.Recipient,
		Channel: string(n.Channel),
		Content: n.Content,
	})
	if err != nil {
		return nil, &Error{Retryable: false, Err: fmt.Errorf("encode request: %w", err)}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, &Error{Retryable: false, Err: fmt.Errorf("build request: %w", err)}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", n.ID)
	if n.CorrelationID != "" {
		req.Header.Set("X-Correlation-ID", n.CorrelationID)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, &Error{Retryable: true, Err: err}
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var out Response
		_ = json.Unmarshal(raw, &out)
		return &out, nil
	}

	return nil, &Error{
		StatusCode: resp.StatusCode,
		Retryable:  isRetryableStatus(resp.StatusCode),
		Err:        fmt.Errorf("%s", truncate(string(raw), 200)),
	}
}

func isRetryableStatus(code int) bool {
	return code == http.StatusRequestTimeout ||
		code == http.StatusTooManyRequests ||
		code >= 500
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
