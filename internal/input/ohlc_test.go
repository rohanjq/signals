package input

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rohanjq/signals/internal/model"
	"github.com/rohanjq/signals/internal/monitor"
)

type recordedInput struct {
	kind string
	bar  model.Bar
}

type recordingSink struct {
	received chan recordedInput
}

func (s *recordingSink) Seed(_ context.Context, _, _ string, bars []model.Bar) error {
	s.received <- recordedInput{kind: "seed", bar: bars[0]}
	return nil
}

func (s *recordingSink) Closed(_ context.Context, bar model.Bar) error {
	s.received <- recordedInput{kind: "closed", bar: bar}
	return nil
}

func (s *recordingSink) Forming(_ context.Context, bar model.Bar) error {
	s.received <- recordedInput{kind: "forming", bar: bar}
	return nil
}

func TestOHLCClientSubscribesAndDispatchesInWireOrder(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serverErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer upstream-token" {
			serverErr <- context.Canceled
			return
		}
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		var subscription subscribeMessage
		if err := conn.ReadJSON(&subscription); err != nil {
			serverErr <- err
			return
		}
		if subscription.Symbol != "BTCUSDT" || subscription.Timeframe != "1m" || subscription.SeedBars != 200 {
			serverErr <- context.Canceled
			return
		}
		closed := testBar(true, 100)
		forming := testBar(false, 101)
		for _, message := range []serverMessage{
			{Type: "seed", Key: "BTCUSDT|1m", Bars: []model.Bar{closed}},
			{Type: "closed", Key: "BTCUSDT|1m", Bar: &closed},
			{Type: "forming", Key: "BTCUSDT|1m", Bar: &forming},
		} {
			if err := conn.WriteJSON(message); err != nil {
				serverErr <- err
				return
			}
		}
		serverErr <- nil
	}))
	defer server.Close()

	sink := &recordingSink{received: make(chan recordedInput, 3)}
	metrics := monitor.NewMetrics()
	client, err := NewOHLCClient(OHLCOptions{
		URL: strings.Replace(server.URL, "http://", "ws://", 1), Token: "upstream-token",
		Series:   []model.SeriesKey{{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"}},
		SeedBars: 200, Sink: sink, Metrics: metrics, MinBackoff: time.Hour, MaxBackoff: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()

	for index, want := range []string{"seed", "closed", "forming"} {
		select {
		case input := <-sink.received:
			if input.kind != want {
				t.Fatalf("message %d kind = %q, want %q", index, input.kind, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for %s", want)
		}
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("test server: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func testBar(closed bool, closeValue float64) model.Bar {
	return model.Bar{
		Symbol: "BTCUSDT", Timeframe: "1m", OpenTime: time.Unix(1_700_000_000, 0).UTC(),
		Open: 100, High: 102, Low: 99, Close: closeValue, Volume: 10, Trades: 3, Closed: closed,
	}
}
