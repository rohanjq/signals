package store

import (
	"context"
	"testing"
	"time"

	"github.com/rohanjq/signals/internal/model"
)

func TestMemoryCommitAndResume(t *testing.T) {
	ctx := context.Background()
	store := NewMemory()
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	bar := model.Bar{Symbol: key.Symbol, Timeframe: key.Timeframe, OpenTime: time.Now().UTC(), Open: 1, High: 1, Low: 1, Close: 1, Closed: true}
	event := model.SignalEvent{EventID: "event-1", Dataset: key.Dataset, Symbol: key.Symbol, Timeframe: key.Timeframe, Indicator: &model.IndicatorPayload{Period: 9}}
	committed, err := store.CommitClosed(ctx, key, bar, []model.SignalEvent{event}, nil)
	if err != nil || len(committed) != 1 || committed[0].Cursor != 1 {
		t.Fatalf("commit = %+v, err = %v", committed, err)
	}
	rows, err := store.QueryEvents(ctx, EventFilter{AfterCursor: 0, Period: 9})
	if err != nil || len(rows) != 1 || rows[0].EventID != event.EventID {
		t.Fatalf("query = %+v, err = %v", rows, err)
	}
	if err := store.MarkPublished(ctx, []int64{1}); err != nil {
		t.Fatal(err)
	}
	pending, _ := store.PendingEvents(ctx, 10)
	if len(pending) != 0 {
		t.Fatalf("pending = %d, want 0", len(pending))
	}
}

func TestMemoryRecentPointsIncludesSeedAndReturnsNewestAscending(t *testing.T) {
	ctx := context.Background()
	persistence := NewMemory()
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	start := time.Unix(1_700_000_000, 0).UTC()
	algorithm := model.AlgorithmRef{Name: "ema", Version: "1", ConfigHash: "current"}
	events := make([]model.SignalEvent, 0, 3)
	for index := range 3 {
		events = append(events, model.SignalEvent{
			EventID: "seed-" + string(rune('a'+index)), Dataset: key.Dataset, Symbol: key.Symbol,
			Timeframe: key.Timeframe, BarOpenTime: start.Add(time.Duration(index) * time.Minute),
			Algorithm: algorithm, Indicator: &model.IndicatorPayload{Period: 9, Ready: true},
		})
	}
	events = append(events, model.SignalEvent{
		EventID: "stale", Dataset: key.Dataset, Symbol: key.Symbol, Timeframe: key.Timeframe,
		BarOpenTime: start.Add(3 * time.Minute),
		Algorithm:   model.AlgorithmRef{Name: "ema", Version: "0", ConfigHash: "stale"},
		Indicator:   &model.IndicatorPayload{Period: 9, Ready: true},
	})
	if err := persistence.RebuildSeries(ctx, key, events, nil, nil); err != nil {
		t.Fatal(err)
	}
	points, err := persistence.QueryRecentPoints(ctx, PointFilter{
		Dataset: key.Dataset, Symbol: key.Symbol, Timeframe: key.Timeframe,
		Period: 9, Algorithm: algorithm, Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 2 || points[0].EventID != "seed-b" || points[1].EventID != "seed-c" {
		t.Fatalf("points = %+v, want newest two in ascending time order", points)
	}
}

func TestMemoryKeepsMarketStateOutOfIndicatorPoints(t *testing.T) {
	ctx := context.Background()
	persistence := NewMemory()
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	bar := model.Bar{Symbol: key.Symbol, Timeframe: key.Timeframe, OpenTime: time.Now().UTC(), Open: 1, High: 1, Low: 1, Close: 1, Closed: true}
	state := model.MarketState{Schema: "market.state.v1", AsOf: bar.OpenTime}
	event := model.SignalEvent{
		EventID: "market-1", EventType: "market.state", Dataset: key.Dataset, Symbol: key.Symbol,
		Timeframe: key.Timeframe, BarOpenTime: bar.OpenTime, Market: &state,
	}
	if _, err := persistence.CommitClosed(ctx, key, bar, []model.SignalEvent{event}, nil); err != nil {
		t.Fatal(err)
	}
	points, err := persistence.QueryRecentPoints(ctx, PointFilter{Limit: 10})
	if err != nil || len(points) != 0 {
		t.Fatalf("indicator points = %+v, err = %v", points, err)
	}
	events, err := persistence.QueryEvents(ctx, EventFilter{Limit: 10})
	if err != nil || len(events) != 1 || events[0].Market == nil {
		t.Fatalf("market events = %+v, err = %v", events, err)
	}
	periodEvents, err := persistence.QueryEvents(ctx, EventFilter{Period: 9, Limit: 10})
	if err != nil || len(periodEvents) != 0 {
		t.Fatalf("period-filtered events = %+v, err = %v", periodEvents, err)
	}
}
