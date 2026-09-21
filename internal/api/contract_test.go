package api

import (
	"testing"
	"time"

	"github.com/rohanjq/signals/internal/model"
)

func TestToAnalysisEventPreservesScalarIdentityAndLifecycle(t *testing.T) {
	barTime := time.Date(2026, 9, 6, 10, 5, 0, 0, time.UTC)
	emittedAt := barTime.Add(17 * time.Second)
	event, err := toAnalysisEvent(model.SignalEvent{
		EventID: "event-1", EventType: "indicator.point", Dataset: "binance-spot",
		Symbol: "BTCUSDT", Timeframe: "1m", BarOpenTime: barTime, BarRevision: 3, AnalysisRevision: 4, Provisional: true,
		Algorithm: model.AlgorithmRef{Name: "ema", Version: "2", ConfigHash: "sha256:test"},
		Indicator: &model.IndicatorPayload{Period: 21, Value: 101.25, Samples: 80, Ready: true},
	}, emittedAt)
	if err != nil {
		t.Fatal(err)
	}
	if event.SpecVersion != "1.0" || event.Type != analysisEventType || event.Data.Bar.Status != "provisional" {
		t.Fatalf("unexpected envelope: %+v", event)
	}
	if !event.Data.Bar.CloseTime.Equal(barTime.Add(time.Minute)) || event.Data.Bar.Revision != 3 {
		t.Fatalf("unexpected bar identity: %+v", event.Data.Bar)
	}
	if event.Data.Analysis.Name != "ema" || event.Data.Analysis.Parameters["period"] != 21 {
		t.Fatalf("unexpected analysis identity: %+v", event.Data.Analysis)
	}
	if event.Data.AnalysisRevision != 4 || event.Data.RequiredSamples != 21 {
		t.Fatalf("unexpected analysis lifecycle: %+v", event.Data)
	}
	if len(event.Data.Outputs) != 1 || event.Data.Outputs[0].Type != "number" || event.Data.Outputs[0].Display.Kind != "line" {
		t.Fatalf("unexpected scalar output: %+v", event.Data.Outputs)
	}
}

func TestToAnalysisEventPublishesMarketAsTypedOutputs(t *testing.T) {
	barTime := time.Date(2026, 9, 6, 10, 5, 0, 0, time.UTC)
	event, err := toAnalysisEvent(model.SignalEvent{
		EventID: "event-2", EventType: "market.state", Dataset: "binance-spot",
		Symbol: "BTCUSDT", Timeframe: "5m", BarOpenTime: barTime,
		Algorithm:  model.AlgorithmRef{Name: "market_structure", Version: "1", ConfigHash: "sha256:test"},
		SourceBar:  &model.Bar{OpenTime: barTime, Open: 100, High: 103, Low: 99, Close: 102, Volume: 5, Trades: 7},
		Market:     &model.MarketState{Trend: "up", FVGs: []model.PriceZone{{ID: "fvg-1", Kind: "fvg"}}},
		Indicators: []model.IndicatorState{{Period: 200, Ready: true, Value: floatPointer(101)}},
	}, barTime.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	outputs := make(map[string]analysisOutput, len(event.Data.Outputs))
	for _, output := range event.Data.Outputs {
		outputs[output.Name] = output
	}
	if outputs["trend"].Type != "text" || outputs["fvgs"].Type != "zones" || outputs["structure"].Type != "markers" {
		t.Fatalf("market outputs are not self-describing: %+v", outputs)
	}
	if outputs["ema_200"].Value != float64(101) || event.CorrelationID == "" || event.Data.Bar.Close != 102 {
		t.Fatalf("market event is not a complete alert fact: %+v", event)
	}
}

func floatPointer(value float64) *float64 { return &value }

func TestSnapshotAnalysisEventsUsesFormingBarIdentity(t *testing.T) {
	closedTime := time.Date(2026, 9, 6, 10, 4, 0, 0, time.UTC)
	formingTime := closedTime.Add(time.Minute)
	value := 101.25
	events, err := snapshotAnalysisEvents(model.SeriesSnapshot{
		Series:         model.SeriesKey{Dataset: "binance-spot", Symbol: "BTCUSDT", Timeframe: "1m"},
		LastClosedTime: &closedTime, FormingOpenTime: &formingTime,
		Provisional: []model.IndicatorState{{
			Algorithm: model.AlgorithmRef{Name: "ema", Version: "2", ConfigHash: "sha256:test"},
			Period:    21, Value: &value, Samples: 80, Ready: true,
		}},
	}, formingTime.Add(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || !events[0].Data.Bar.OpenTime.Equal(formingTime) || events[0].Data.Bar.Status != "provisional" {
		t.Fatalf("unexpected snapshot events: %+v", events)
	}
}

func TestAnalysisEventIDIsCanonicalAcrossDeliveryPhases(t *testing.T) {
	barTime := time.Date(2026, 9, 6, 10, 5, 0, 0, time.UTC)
	algorithm := model.AlgorithmRef{Name: "ema", Version: "2", ConfigHash: "sha256:test"}
	live, err := toAnalysisEvent(model.SignalEvent{
		EventID: "internal-live-id", EventType: "indicator.point", Dataset: "binance-spot",
		Symbol: "BTCUSDT", Timeframe: "1m", BarOpenTime: barTime, Algorithm: algorithm,
		Indicator: &model.IndicatorPayload{Period: 21, Value: 101.25, Samples: 80, Ready: true},
	}, barTime.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	value := 101.25
	snapshot, err := snapshotAnalysisEvents(model.SeriesSnapshot{
		Series:         model.SeriesKey{Dataset: "binance-spot", Symbol: "BTCUSDT", Timeframe: "1m"},
		LastClosedTime: &barTime,
		Indicators: []model.IndicatorState{{
			Algorithm: algorithm, Period: 21, Value: &value, Samples: 80, Ready: true,
		}},
	}, barTime.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 || live.ID != snapshot[0].ID {
		t.Fatalf("live ID %q != snapshot ID %#v", live.ID, snapshot)
	}
}

func TestSnapshotPublishesUnreadyAnalysisWithEmptyOutputs(t *testing.T) {
	barTime := time.Date(2026, 9, 6, 10, 5, 0, 0, time.UTC)
	events, err := snapshotAnalysisEvents(model.SeriesSnapshot{
		Series:         model.SeriesKey{Dataset: "binance-spot", Symbol: "BTCUSDT", Timeframe: "1m"},
		LastClosedTime: &barTime,
		Indicators: []model.IndicatorState{{
			Algorithm: model.AlgorithmRef{Name: "ema", Version: "2", ConfigHash: "sha256:test"},
			Period:    200, Samples: 84, Ready: false,
		}},
	}, barTime.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Data.Ready || events[0].Data.Samples != 84 ||
		events[0].Data.RequiredSamples != 200 || len(events[0].Data.Outputs) != 0 {
		t.Fatalf("unexpected warm-up event: %+v", events)
	}
}

func TestMarketPrimitivesHaveStableIDsAndCanonicalDirections(t *testing.T) {
	at := time.Date(2026, 9, 6, 10, 5, 0, 0, time.UTC)
	market := model.MarketState{
		Structure: []model.StructureEvent{{Kind: "BOS", Side: "up", Price: 101.25, Time: at}},
		KeyLevels: []model.PriceLevel{{Kind: "equal_high", Side: "sell_side", Price: 102, Touches: 3}},
	}
	outputs := marketOutputs(market)
	if len(outputs) == 0 {
		t.Fatal("expected market outputs")
	}
	markers := outputs[2].Value.([]pointPrimitive)
	levels := outputs[5].Value.([]levelPrimitive)
	if markers[0].ID == "" || markers[0].Direction != "bullish" || levels[0].ID == "" || levels[0].Direction != "bearish" {
		t.Fatalf("unexpected normalized primitives: markers=%+v levels=%+v", markers, levels)
	}
}
