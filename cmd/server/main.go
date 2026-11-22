package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/api"
	"github.com/EdizKarli/event-driven-notification-system-task/internal/provider"
	"github.com/EdizKarli/event-driven-notification-system-task/internal/store"
	"github.com/EdizKarli/event-driven-notification-system-task/internal/worker"
)

const defaultDatabaseURL = "postgres://notify:notify@localhost:5432/notifications?sslmode=disable"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	port := getEnv("PORT", "8080")
	databaseURL := getEnv("DATABASE_URL", defaultDatabaseURL)
	providerURL := os.Getenv("PROVIDER_URL")
	if providerURL == "" {
		logger.Error("PROVIDER_URL is required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	connectCtx, cancelConnect := context.WithTimeout(ctx, 10*time.Second)
	st, err := store.New(connectCtx, databaseURL)
	cancelConnect()
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	cfg := worker.DefaultConfig()
	w := worker.New(st, provider.NewClient(providerURL, cfg.SendTimeout), logger, cfg)
	workerDone := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(workerDone)
	}()

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           api.NewServer(logger, st).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("server starting", "port", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown failed", "error", err)
	}
	<-workerDone
	logger.Info("server stopped")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
