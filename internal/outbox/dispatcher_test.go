package outbox

import (
	"context"
	"testing"
	"time"

	"github.com/rohanjq/signals/internal/model"
	"github.com/rohanjq/signals/internal/store"
)

type capturePublisher struct {
	events []model.SignalEvent
}

func (p *capturePublisher) Publish(event model.SignalEvent) {
	p.events = append(p.events, event)
}

func TestDispatcherPublishesAndAcknowledgesPendingEvents(t *testing.T) {
	persistence := store.NewMemory()
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	bar := model.Bar{
		Symbol: key.Symbol, Timeframe: key.Timeframe, OpenTime: time.Unix(1_700_000_000, 0).UTC(),
		Open: 100, High: 102, Low: 99, Close: 101, Volume: 1, Closed: true,
	}
	event := model.SignalEvent{
		EventID: "event-1", Dataset: key.Dataset, Symbol: key.Symbol, Timeframe: key.Timeframe,
		BarOpenTime: bar.OpenTime, Indicator: &model.IndicatorPayload{Period: 9, Value: 100, Ready: true},
	}
	if _, err := persistence.CommitClosed(context.Background(), key, bar, []model.SignalEvent{event}, nil); err != nil {
		t.Fatal(err)
	}
	publisher := &capturePublisher{}
	dispatcher := New(persistence, publisher, time.Second, nil, nil)
	if err := dispatcher.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(publisher.events) != 1 || publisher.events[0].Cursor != 1 {
		t.Fatalf("published events = %+v", publisher.events)
	}
	if err := dispatcher.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(publisher.events) != 1 {
		t.Fatalf("event was republished after acknowledgement: %+v", publisher.events)
	}
}
