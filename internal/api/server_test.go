package api

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rohanjq/signals/internal/engine"
	"github.com/rohanjq/signals/internal/model"
	"github.com/rohanjq/signals/internal/monitor"
	"github.com/rohanjq/signals/internal/store"
)

func TestAuthAcceptsHeaderAndWebSocketSubprotocol(t *testing.T) {
	server := &Server{options: ServerOptions{AuthToken: "test-secret"}}
	handler := server.auth(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))

	tests := []struct {
		name   string
		header string
		value  string
		want   int
	}{
		{name: "missing", want: http.StatusUnauthorized},
		{name: "invalid", header: "Authorization", value: "Bearer wrong", want: http.StatusUnauthorized},
		{name: "authorization", header: "Authorization", value: "Bearer test-secret", want: http.StatusNoContent},
		{name: "websocket subprotocol", header: "Sec-WebSocket-Protocol", value: "bearer." + base64.RawURLEncoding.EncodeToString([]byte("test-secret")), want: http.StatusNoContent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/v1/ws", nil)
			if test.header != "" {
				request.Header.Set(test.header, test.value)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d", response.Code, test.want)
			}
		})
	}
}

func TestCORSRejectsUnknownPreflightOrigin(t *testing.T) {
	server := &Server{origins: map[string]struct{}{"https://charts.example": {}}}
	handler := server.cors(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodOptions, "/v1/events", nil)
	request.Header.Set("Origin", "https://attacker.example")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
}

func TestWebSocketSnapshotThenLiveConfirmedEvent(t *testing.T) {
	persistence := store.NewMemory()
	hub := NewHub()
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	registry, err := engine.NewRegistry(engine.RegistryOptions{
		Series: []model.SeriesKey{key}, Periods: []int{2}, MailboxSize: 8,
		SnapshotEvery: 2, Store: persistence, Publisher: hub,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry.Start(ctx)
	start := time.Unix(1_700_000_000, 0).UTC()
	if err := registry.Seed(ctx, key.Symbol, key.Timeframe, []model.Bar{
		apiBar(key, start, 100), apiBar(key, start.Add(time.Minute), 101),
	}); err != nil {
		t.Fatal(err)
	}
	metrics := monitor.NewMetrics()
	metrics.SetConnected(true)
	metrics.SetOutboxHealthy(true)
	server, err := NewServer(ServerOptions{
		Addr: "127.0.0.1:0", AuthToken: "test-secret", AllowedOrigins: []string{"https://charts.example"},
		Periods: []int{2}, Registry: registry, EventStore: persistence, Hub: hub, Metrics: metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.http.Handler)
	defer httpServer.Close()

	protocol := "bearer." + base64.RawURLEncoding.EncodeToString([]byte("test-secret"))
	dialer := websocket.Dialer{Subprotocols: []string{protocol}}
	headers := http.Header{"Origin": []string{"https://charts.example"}}
	conn, _, err := dialer.Dial(strings.Replace(httpServer.URL, "http://", "ws://", 1)+"/v1/ws", headers)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteJSON(wsRequest{
		Protocol: protocolVersion, Type: "subscribe", RequestID: "test-subscribe",
		Data: wsSubscriptionData{
			Series:   []seriesSelector{{Dataset: key.Dataset, Symbol: key.Symbol, Timeframe: key.Timeframe}},
			Analyses: []analysisSelector{{Name: "*"}}, EventTypes: []string{analysisEventType}, Statuses: []string{"confirmed"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	var subscribed wireEnvelope
	if err := conn.ReadJSON(&subscribed); err != nil {
		t.Fatal(err)
	}
	if subscribed.Type != "subscribed" || subscribed.Protocol != protocolVersion || subscribed.SubscriptionID == "" {
		t.Fatalf("subscription acknowledgement = %#v", subscribed)
	}
	var snapshot wireEnvelope
	if err := conn.ReadJSON(&snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Type != "snapshot" {
		t.Fatalf("first envelope type=%q, want snapshot", snapshot.Type)
	}
	snapshotData, ok := snapshot.Data.([]any)
	if !ok || len(snapshotData) == 0 {
		t.Fatalf("snapshot data = %#v, want generic analysis events", snapshot.Data)
	}

	if err := registry.Closed(ctx, apiBar(key, start.Add(2*time.Minute), 102)); err != nil {
		t.Fatal(err)
	}
	var history wireEnvelope
	if err := conn.ReadJSON(&history); err != nil {
		t.Fatal(err)
	}
	if history.Type != "history" {
		t.Fatalf("second envelope type=%q, want history", history.Type)
	}
	historyEvents, ok := history.Data.([]any)
	if !ok || len(historyEvents) != 1 {
		t.Fatalf("history data = %#v, want one seed-derived point", history.Data)
	}
	var event wireEnvelope
	if err := conn.ReadJSON(&event); err != nil {
		t.Fatal(err)
	}
	if event.Type != "event" {
		t.Fatalf("third envelope type=%q, want event", event.Type)
	}
	eventData, ok := event.Data.(map[string]any)
	domainData, dataOK := eventData["data"].(map[string]any)
	analysis, analysisOK := domainData["analysis"].(map[string]any)
	if !ok || !dataOK || !analysisOK || eventData["type"] != analysisEventType || analysis["name"] != "ema" || event.Cursor == "" {
		t.Fatalf("event data = %#v, want generic EMA analysis", event.Data)
	}
	var marketEvent wireEnvelope
	if err := conn.ReadJSON(&marketEvent); err != nil {
		t.Fatal(err)
	}
	marketData, ok := marketEvent.Data.(map[string]any)
	marketDomain, domainOK := marketData["data"].(map[string]any)
	marketAnalysis, analysisOK := marketDomain["analysis"].(map[string]any)
	if marketEvent.Type != "event" || !ok || !domainOK || !analysisOK || marketAnalysis["name"] != "smc.market_state" {
		t.Fatalf("market event = %#v", marketEvent)
	}
}

func apiBar(key model.SeriesKey, openTime time.Time, closeValue float64) model.Bar {
	return model.Bar{
		Symbol: key.Symbol, Timeframe: key.Timeframe, OpenTime: openTime,
		Open: closeValue, High: closeValue + 1, Low: closeValue - 1,
		Close: closeValue, Volume: 1, Trades: 1, Closed: true,
	}
}
