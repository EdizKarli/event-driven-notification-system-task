package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/metrics"
)

func testServer() *Server {
	return NewServer(slog.New(slog.NewTextHandler(io.Discard, nil)), newFakeStore(), metrics.New())
}

func TestHealth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	testServer().Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
	if rec.Header().Get(CorrelationIDHeader) == "" {
		t.Fatal("expected a generated correlation id header")
	}
}

func TestHealthWrongMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	rec := httptest.NewRecorder()

	testServer().Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestCorrelationIDIsKept(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set(CorrelationIDHeader, "abc-123")
	rec := httptest.NewRecorder()

	testServer().Handler().ServeHTTP(rec, req)

	if got := rec.Header().Get(CorrelationIDHeader); got != "abc-123" {
		t.Fatalf("expected abc-123, got %q", got)
	}
}

func TestCorrelationIDInContext(t *testing.T) {
	var seen string
	h := correlationID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = CorrelationIDFrom(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(CorrelationIDHeader, "xyz-789")
	h.ServeHTTP(httptest.NewRecorder(), req)

	if seen != "xyz-789" {
		t.Fatalf("expected xyz-789 in context, got %q", seen)
	}
}

func TestRecovererReturns500(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := recoverer(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}
