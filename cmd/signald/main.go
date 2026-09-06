package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rohanjq/signals/internal/api"
	"github.com/rohanjq/signals/internal/config"
	"github.com/rohanjq/signals/internal/engine"
	"github.com/rohanjq/signals/internal/input"
	"github.com/rohanjq/signals/internal/monitor"
	"github.com/rohanjq/signals/internal/outbox"
	"github.com/rohanjq/signals/internal/store"
)

type serviceStore interface {
	engine.Store
	api.EventStore
	outbox.Store
	Close()
}

func main() {
	if err := run(); err != nil {
		slog.Error("signald stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	settings, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: settings.LogLevel}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	persistence, err := openStore(ctx, settings, logger)
	if err != nil {
		return err
	}
	defer persistence.Close()

	hub := api.NewHub()
	registry, err := engine.NewRegistry(engine.RegistryOptions{
		Series: settings.Series, Periods: settings.Periods, MailboxSize: settings.MailboxSize,
		SnapshotEvery: settings.SnapshotEvery, Store: persistence, Publisher: hub, OutboxDelivery: true,
	})
	if err != nil {
		return fmt.Errorf("create lane registry: %w", err)
	}
	metrics := monitor.NewMetrics()
	upstream, err := input.NewOHLCClient(input.OHLCOptions{
		URL: settings.OHLCWebSocketURL, Token: settings.OHLCToken, Series: settings.Series,
		SeedBars: settings.SeedBars, Sink: registry, Metrics: metrics, Logger: logger,
	})
	if err != nil {
		return fmt.Errorf("create OHLC client: %w", err)
	}
	server, err := api.NewServer(api.ServerOptions{
		Addr: settings.HTTPAddr, AuthToken: settings.APIAuthToken, InsecureNoAuth: settings.InsecureNoAuth,
		AllowedOrigins: settings.AllowedOrigins, Periods: settings.Periods, Registry: registry,
		EventStore: persistence, Hub: hub, Metrics: metrics, Logger: logger, ShutdownTimeout: settings.ShutdownTimeout,
	})
	if err != nil {
		return fmt.Errorf("create API server: %w", err)
	}
	dispatcher := outbox.New(persistence, hub, settings.OutboxPollInterval, logger, metrics)

	group, groupCtx := errgroup.WithContext(ctx)
	registry.Start(groupCtx)
	group.Go(func() error { return dispatcher.Run(groupCtx) })
	group.Go(func() error { return upstream.Run(groupCtx) })
	group.Go(func() error { return server.Run(groupCtx) })
	if err := group.Wait(); err != nil {
		return fmt.Errorf("run service: %w", err)
	}
	logger.Info("signald shutdown complete")
	return nil
}

func openStore(ctx context.Context, settings config.Config, logger *slog.Logger) (serviceStore, error) {
	if settings.DatabaseURL == "" {
		logger.Warn("using volatile in-memory store")
		return store.NewMemory(), nil
	}
	openCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	persistence, err := store.OpenPostgres(openCtx, settings.DatabaseURL, int32(settings.DatabaseMaxConns))
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL: %w", err)
	}
	return persistence, nil
}
