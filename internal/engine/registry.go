package engine

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/rohanjq/signals/internal/model"
)

type RegistryOptions struct {
	Series         []model.SeriesKey
	Periods        []int
	MailboxSize    int
	SnapshotEvery  uint64
	Store          Store
	Publisher      Publisher
	OutboxDelivery bool
}

type Registry struct {
	mu    sync.RWMutex
	lanes map[string]*Lane
	keys  []model.SeriesKey
}

func NewRegistry(options RegistryOptions) (*Registry, error) {
	if len(options.Series) == 0 {
		return nil, fmt.Errorf("at least one series is required")
	}
	registry := &Registry{lanes: make(map[string]*Lane, len(options.Series))}
	for _, key := range options.Series {
		lookup := lookupKey(key.Symbol, key.Timeframe)
		if _, exists := registry.lanes[lookup]; exists {
			return nil, fmt.Errorf("duplicate symbol/timeframe lane %s|%s", key.Symbol, key.Timeframe)
		}
		lane, err := NewLane(LaneOptions{
			Key: key, Periods: options.Periods, MailboxSize: options.MailboxSize,
			SnapshotEvery: options.SnapshotEvery, Store: options.Store, Publisher: options.Publisher,
			OutboxDelivery: options.OutboxDelivery,
		})
		if err != nil {
			return nil, err
		}
		registry.lanes[lookup] = lane
		registry.keys = append(registry.keys, key)
	}
	sort.Slice(registry.keys, func(i, j int) bool { return registry.keys[i].String() < registry.keys[j].String() })
	return registry, nil
}

func (r *Registry) Start(ctx context.Context) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, lane := range r.lanes {
		go lane.Run(ctx)
	}
}

func (r *Registry) Keys() []model.SeriesKey {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]model.SeriesKey(nil), r.keys...)
}

func (r *Registry) Seed(ctx context.Context, symbol, timeframe string, bars []model.Bar) error {
	lane, err := r.lane(symbol, timeframe)
	if err != nil {
		return err
	}
	return lane.Seed(ctx, bars)
}

func (r *Registry) Closed(ctx context.Context, bar model.Bar) error {
	lane, err := r.lane(bar.Symbol, bar.Timeframe)
	if err != nil {
		return err
	}
	return lane.Closed(ctx, bar)
}

func (r *Registry) Forming(ctx context.Context, bar model.Bar) error {
	lane, err := r.lane(bar.Symbol, bar.Timeframe)
	if err != nil {
		return err
	}
	return lane.Forming(ctx, bar)
}

func (r *Registry) Snapshot(symbol, timeframe string) (model.SeriesSnapshot, bool) {
	lane, err := r.lane(symbol, timeframe)
	if err != nil {
		return model.SeriesSnapshot{}, false
	}
	return lane.Snapshot(), true
}

func (r *Registry) Snapshots() []model.SeriesSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshots := make([]model.SeriesSnapshot, 0, len(r.keys))
	for _, key := range r.keys {
		snapshots = append(snapshots, r.lanes[lookupKey(key.Symbol, key.Timeframe)].Snapshot())
	}
	return snapshots
}

func (r *Registry) Ready() bool {
	for _, snapshot := range r.Snapshots() {
		if !snapshot.Ready || snapshot.LastClosedTime == nil {
			return false
		}
	}
	return true
}

func (r *Registry) lane(symbol, timeframe string) (*Lane, error) {
	r.mu.RLock()
	lane := r.lanes[lookupKey(symbol, timeframe)]
	r.mu.RUnlock()
	if lane == nil {
		return nil, fmt.Errorf("series %s|%s is not configured", symbol, timeframe)
	}
	return lane, nil
}

func lookupKey(symbol, timeframe string) string { return symbol + "|" + timeframe }
