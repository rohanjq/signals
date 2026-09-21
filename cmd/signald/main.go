package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rohanjq/signals/internal/api"
	"github.com/rohanjq/signals/internal/broker"
	"github.com/rohanjq/signals/internal/config"
	"github.com/rohanjq/signals/internal/engine"
	"github.com/rohanjq/signals/internal/input"
	"github.com/rohanjq/signals/internal/model"
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

type hubPublisher struct{ hub *api.Hub }

func (p hubPublisher) Publish(_ context.Context, event model.SignalEvent) error {
	p.hub.Publish(event)
	return nil
}

type evaluationBrokerPublisher struct {
	stream *broker.JetStream
	logger *slog.Logger
	ctx    context.Context
}

func (p evaluationBrokerPublisher) Publish(event model.SignalEvent) {
	for {
		publishCtx, cancel := context.WithTimeout(p.ctx, 2*time.Second)
		err := p.stream.PublishEvaluation(publishCtx, event)
		cancel()
		if err == nil || p.ctx.Err() != nil {
			return
		}
		p.logger.Warn("publish evaluation fact", "series", event.Dataset+"/"+event.Symbol+"/"+event.Timeframe, "provisional", event.Provisional, "err", err)
		if event.Provisional && errors.Is(err, broker.ErrReplaceableEvaluation) {
			// Snapshots repaint and are replaceable. Confirmed and transition frames
			// take the retry/backpressure path below.
			return
		}
		select {
		case <-p.ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

type multiPublisher []outbox.Publisher

func (publishers multiPublisher) Publish(ctx context.Context, event model.SignalEvent) error {
	for _, publisher := range publishers {
		if err := publisher.Publish(ctx, event); err != nil {
			return err
		}
	}
	return nil
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
	metrics := monitor.NewMetrics()
	publishers := multiPublisher{}
	var jetstream *broker.JetStream
	var livePublisher engine.Publisher
	if settings.NATSURL != "" {
		jetstream, err = broker.Open(ctx, settings.NATSURL, settings.NATSToken, settings.NATSReplicas)
		if err != nil {
			return fmt.Errorf("open fact publisher: %w", err)
		}
		defer jetstream.Close()
		publishers = append(publishers, jetstream)
		livePublisher = evaluationBrokerPublisher{stream: jetstream, logger: logger, ctx: ctx}
		logger.Info("JetStream fact publisher enabled")
	}
	registry, err := engine.NewRegistry(engine.RegistryOptions{
		Series: settings.Series, Periods: settings.Periods, MailboxSize: settings.MailboxSize,
		SnapshotEvery: settings.SnapshotEvery, Store: persistence, Publisher: hub,
		LivePublisher: livePublisher, OutboxDelivery: true,
	})
	if err != nil {
		return fmt.Errorf("create lane registry: %w", err)
	}
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
	publishers = append(publishers, hubPublisher{hub: hub})
	dispatcher := outbox.New(persistence, publishers, settings.OutboxPollInterval, logger, metrics)
	// Recover committed confirmed events before accepting new OHLC input. This
	// prevents a next-candle provisional frame from overtaking a confirmed event
	// after a process crash between database commit and broker publication.
	if err := dispatcher.Flush(ctx); err != nil {
		return fmt.Errorf("recover signal outbox before live input: %w", err)
	}

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
