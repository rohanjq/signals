package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/rohanjq/signals/internal/engine"
	"github.com/rohanjq/signals/internal/model"
	"github.com/rohanjq/signals/internal/monitor"
)

type Store interface {
	PendingEvents(context.Context, int) ([]model.SignalEvent, error)
	MarkPublished(context.Context, []int64) error
}

type Dispatcher struct {
	store     Store
	publisher engine.Publisher
	interval  time.Duration
	logger    *slog.Logger
	metrics   *monitor.Metrics
}

func New(store Store, publisher engine.Publisher, interval time.Duration, logger *slog.Logger, metrics *monitor.Metrics) *Dispatcher {
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Dispatcher{store: store, publisher: publisher, interval: interval, logger: logger, metrics: metrics}
}

func (d *Dispatcher) Run(ctx context.Context) error {
	if err := d.flush(ctx); err != nil {
		d.recordFailure()
		d.logger.Warn("initial outbox recovery failed", "err", err)
	} else {
		d.recordSuccess()
	}
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := d.flush(ctx); err != nil {
				d.recordFailure()
				d.logger.Warn("outbox recovery failed", "err", err)
			} else {
				d.recordSuccess()
			}
		}
	}
}

func (d *Dispatcher) recordSuccess() {
	if d.metrics != nil {
		d.metrics.SetOutboxHealthy(true)
	}
}

func (d *Dispatcher) recordFailure() {
	if d.metrics != nil {
		d.metrics.SetOutboxHealthy(false)
		d.metrics.RecordOutboxError()
	}
}

func (d *Dispatcher) flush(ctx context.Context) error {
	for {
		events, err := d.store.PendingEvents(ctx, 250)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		cursors := make([]int64, 0, len(events))
		for _, event := range events {
			d.publisher.Publish(event)
			cursors = append(cursors, event.Cursor)
		}
		if err := d.store.MarkPublished(ctx, cursors); err != nil {
			return err
		}
		if len(events) < 250 {
			return nil
		}
	}
}
