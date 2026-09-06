package api

import (
	"testing"

	"github.com/rohanjq/signals/internal/model"
)

func TestHubActivationSkipsReplayedCursorAndKeepsProvisional(t *testing.T) {
	hub := NewHub()
	client := hub.subscribe(Subscription{
		Series: []seriesSelector{{Symbol: "BTCUSDT", Timeframe: "1m"}}, Confirmed: true, Provisional: true,
	}, 4)
	defer hub.unsubscribe(client)

	hub.Publish(hubEvent(7, false))
	hub.Publish(hubEvent(0, true))
	client.activate(7)

	select {
	case event := <-client.send:
		if !event.Provisional || event.Cursor != 0 {
			t.Fatalf("event = %+v", event)
		}
	default:
		t.Fatal("expected provisional event after activation")
	}
	select {
	case duplicate := <-client.send:
		t.Fatalf("unexpected duplicate: %+v", duplicate)
	default:
	}
}

func TestHubFiltersProvisionalAndSeries(t *testing.T) {
	hub := NewHub()
	client := hub.subscribe(Subscription{
		Series: []seriesSelector{{Symbol: "ETHUSDT", Timeframe: "1m"}}, Confirmed: true, Provisional: false,
	}, 2)
	defer hub.unsubscribe(client)
	client.activate(0)
	hub.Publish(hubEvent(1, false))
	hub.Publish(model.SignalEvent{Symbol: "ETHUSDT", Provisional: true})
	select {
	case event := <-client.send:
		t.Fatalf("unexpected event: %+v", event)
	default:
	}
}

func TestHubDropsActiveDuplicateCursor(t *testing.T) {
	hub := NewHub()
	client := hub.subscribe(Subscription{Confirmed: true}, 2)
	defer hub.unsubscribe(client)
	client.activate(5)
	hub.Publish(hubEvent(5, false))
	hub.Publish(hubEvent(6, false))
	select {
	case event := <-client.send:
		if event.Cursor != 6 {
			t.Fatalf("cursor=%d, want 6", event.Cursor)
		}
	default:
		t.Fatal("expected cursor 6")
	}
	select {
	case duplicate := <-client.send:
		t.Fatalf("unexpected duplicate: %+v", duplicate)
	default:
	}
}

func hubEvent(cursor int64, provisional bool) model.SignalEvent {
	return model.SignalEvent{
		Cursor: cursor, EventType: "indicator.point", Symbol: "BTCUSDT", Timeframe: "1m", Provisional: provisional,
		Indicator: &model.IndicatorPayload{Period: 9},
	}
}

func TestHubFiltersTypedEventsWithoutDereferencingMarketPayload(t *testing.T) {
	hub := NewHub()
	client := hub.subscribe(Subscription{Analyses: []analysisSelector{{Name: "market_structure"}}, Confirmed: true}, 2)
	defer hub.unsubscribe(client)
	client.activate(0)
	state := model.MarketState{Schema: "market.state.v1"}
	hub.Publish(model.SignalEvent{EventType: "market.state", Algorithm: model.AlgorithmRef{Name: "market_structure"}, Market: &state})
	hub.Publish(hubEvent(1, false))
	select {
	case event := <-client.send:
		if event.EventType != "market.state" || event.Market == nil {
			t.Fatalf("event = %+v", event)
		}
	default:
		t.Fatal("expected market event")
	}
	select {
	case event := <-client.send:
		t.Fatalf("unexpected event: %+v", event)
	default:
	}
}
