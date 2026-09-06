package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rohanjq/signals/internal/engine"
	"github.com/rohanjq/signals/internal/indicator"
	"github.com/rohanjq/signals/internal/model"
	"github.com/rohanjq/signals/internal/monitor"
	"github.com/rohanjq/signals/internal/store"
)

type EventStore interface {
	QueryEvents(context.Context, store.EventFilter) ([]model.SignalEvent, error)
	QueryRecentPoints(context.Context, store.PointFilter) ([]model.SignalEvent, error)
	Ping(context.Context) error
}

type ServerOptions struct {
	Addr            string
	AuthToken       string
	InsecureNoAuth  bool
	AllowedOrigins  []string
	Periods         []int
	Registry        *engine.Registry
	EventStore      EventStore
	Hub             *Hub
	Metrics         *monitor.Metrics
	Logger          *slog.Logger
	ShutdownTimeout time.Duration
}

type Server struct {
	options ServerOptions
	http    *http.Server
	origins map[string]struct{}
}

type wsRequest struct {
	Protocol  string             `json:"protocol"`
	Type      string             `json:"type"`
	RequestID string             `json:"request_id"`
	Data      wsSubscriptionData `json:"data"`
}

type wsSubscriptionData struct {
	Series      []seriesSelector   `json:"series"`
	Analyses    []analysisSelector `json:"analyses,omitempty"`
	EventTypes  []string           `json:"event_types,omitempty"`
	Statuses    []string           `json:"statuses,omitempty"`
	ResumeAfter string             `json:"resume_after,omitempty"`
	History     wsHistoryRequest   `json:"history,omitempty"`
}

type wsHistoryRequest struct {
	Limit int `json:"limit,omitempty"`
}

const (
	defaultHistoryLimit = 500
	maxHistoryLimit     = 1000
)

func NewServer(options ServerOptions) (*Server, error) {
	if options.Addr == "" || options.Registry == nil || options.EventStore == nil || options.Hub == nil || options.Metrics == nil {
		return nil, fmt.Errorf("API address, registry, event store, hub, and metrics are required")
	}
	if !options.InsecureNoAuth && options.AuthToken == "" {
		return nil, fmt.Errorf("API auth token is required")
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.ShutdownTimeout <= 0 {
		options.ShutdownTimeout = 15 * time.Second
	}
	server := &Server{options: options, origins: make(map[string]struct{}, len(options.AllowedOrigins))}
	for _, origin := range options.AllowedOrigins {
		server.origins[origin] = struct{}{}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.health)
	mux.HandleFunc("GET /readyz", server.ready)
	mux.HandleFunc("GET /metrics", server.metrics)
	mux.Handle("GET /v1/catalog", server.auth(http.HandlerFunc(server.algorithms)))
	mux.Handle("GET /v1/snapshots", server.auth(http.HandlerFunc(server.snapshots)))
	mux.Handle("GET /v1/events", server.auth(http.HandlerFunc(server.signals)))
	mux.Handle("GET /v1/ws", server.auth(http.HandlerFunc(server.websocket)))
	server.http = &http.Server{
		Addr: options.Addr, Handler: server.security(server.cors(mux)),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 90 * time.Second,
	}
	return server, nil
}

func (s *Server) Run(ctx context.Context) error {
	result := make(chan error, 1)
	go func() {
		s.options.Logger.Info("signal API listening", "addr", s.http.Addr)
		result <- s.http.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.options.ShutdownTimeout)
		defer cancel()
		if err := s.http.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return nil
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (s *Server) health(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ready(writer http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	storeReady := s.options.EventStore.Ping(ctx) == nil
	registryReady := s.options.Registry.Ready()
	upstreamReady := s.options.Metrics.Connected()
	outboxReady := s.options.Metrics.OutboxHealthy()
	status := http.StatusOK
	if !storeReady || !registryReady || !upstreamReady || !outboxReady {
		status = http.StatusServiceUnavailable
	}
	writeJSON(writer, status, map[string]any{
		"status": status == http.StatusOK, "store": storeReady, "indicators": registryReady,
		"upstream": upstreamReady, "outbox": outboxReady,
	})
}

func (s *Server) metrics(writer http.ResponseWriter, _ *http.Request) {
	snapshots := s.options.Registry.Snapshots()
	ready := 0
	for _, snapshot := range snapshots {
		if snapshot.Ready && snapshot.LastClosedTime != nil {
			ready++
		}
	}
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
	s.options.Metrics.WritePrometheus(writer,
		ready == len(snapshots) && s.options.Metrics.Connected() && s.options.Metrics.OutboxHealthy(),
		len(snapshots), ready)
}

func (s *Server) algorithms(writer http.ResponseWriter, _ *http.Request) {
	algorithms := make([]map[string]any, 0, len(s.options.Periods)+1)
	for _, period := range s.options.Periods {
		configHash := indicatorConfigHash("ema", indicator.EMAAlgorithmVersion, period)
		algorithms = append(algorithms, map[string]any{
			"id": "ema:" + configHash, "name": "ema", "parameters": map[string]any{"period": period, "price_source": "close"},
			"implementation": map[string]any{"engine": "ytstack-native", "engine_version": indicator.EMAAlgorithmVersion,
				"algorithm_version": indicator.EMAAlgorithmVersion, "config_hash": configHash},
			"warmup":   map[string]any{"minimum_samples": period, "seed": "sma"},
			"statuses": []string{"confirmed", "provisional"},
			"outputs":  []map[string]any{{"name": "value", "type": "number", "unit": "price", "display": map[string]any{"kind": "line", "pane": "price"}}},
		})
	}
	market := indicator.MarketAlgorithm(indicator.DefaultMarketConfig())
	algorithms = append(algorithms, map[string]any{
		"id": market.Name + ":" + market.ConfigHash, "name": market.Name, "parameters": map[string]any{},
		"implementation": map[string]any{"engine": "ytstack-native", "engine_version": market.Version,
			"algorithm_version": market.Version, "config_hash": market.ConfigHash},
		"statuses": []string{"confirmed"}, "outputs": marketOutputCatalog(),
	})
	writeJSON(writer, http.StatusOK, wireEnvelope{Protocol: protocolVersion, Type: "catalog", Data: map[string]any{
		"event_types": []string{analysisEventType}, "analyses": algorithms,
	}})
}

func (s *Server) snapshots(writer http.ResponseWriter, request *http.Request) {
	symbol, timeframe := request.URL.Query().Get("symbol"), request.URL.Query().Get("timeframe")
	if symbol != "" || timeframe != "" {
		if symbol == "" || timeframe == "" {
			writeError(writer, http.StatusBadRequest, "invalid_query", "symbol and timeframe must be provided together")
			return
		}
		snapshot, ok := s.options.Registry.Snapshot(symbol, timeframe)
		if !ok {
			writeError(writer, http.StatusNotFound, "series_not_found", "series is not configured")
			return
		}
		events, err := snapshotAnalysisEvents(snapshot, time.Now())
		if err != nil {
			writeError(writer, http.StatusInternalServerError, "conversion_failed", "snapshot conversion failed")
			return
		}
		writeJSON(writer, http.StatusOK, wireEnvelope{Protocol: protocolVersion, Type: "snapshot", Data: events})
		return
	}
	events := make([]analysisEvent, 0)
	for _, snapshot := range s.options.Registry.Snapshots() {
		converted, err := snapshotAnalysisEvents(snapshot, time.Now())
		if err != nil {
			writeError(writer, http.StatusInternalServerError, "conversion_failed", "snapshot conversion failed")
			return
		}
		events = append(events, converted...)
	}
	writeJSON(writer, http.StatusOK, wireEnvelope{Protocol: protocolVersion, Type: "snapshot", Data: events})
}

func (s *Server) signals(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	filter := store.EventFilter{
		Dataset: query.Get("dataset"), Symbol: query.Get("symbol"), Timeframe: query.Get("timeframe"),
		Analysis: query.Get("analysis"),
	}
	var err error
	if raw := query.Get("resume_after"); raw != "" {
		filter.AfterCursor, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || filter.AfterCursor < 0 {
			writeError(writer, http.StatusBadRequest, "invalid_query", "resume_after must be a non-negative integer string")
			return
		}
	}
	if raw := query.Get("limit"); raw != "" {
		filter.Limit, err = strconv.Atoi(raw)
		if err != nil || filter.Limit <= 0 || filter.Limit > 5000 {
			writeError(writer, http.StatusBadRequest, "invalid_query", "limit must be from 1 to 5000")
			return
		}
	}
	events, err := s.options.EventStore.QueryEvents(request.Context(), filter)
	if err != nil {
		s.options.Logger.Error("query signals", "err", err)
		writeError(writer, http.StatusInternalServerError, "query_failed", "query failed")
		return
	}
	converted, err := toAnalysisEvents(events, time.Now())
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "conversion_failed", "event conversion failed")
		return
	}
	nextCursor := strconv.FormatInt(filter.AfterCursor, 10)
	if len(events) > 0 {
		nextCursor = strconv.FormatInt(events[len(events)-1].Cursor, 10)
	}
	writeJSON(writer, http.StatusOK, wireEnvelope{Protocol: protocolVersion, Type: "page", Cursor: nextCursor, Data: converted})
}

func (s *Server) websocket(writer http.ResponseWriter, request *http.Request) {
	upgrader := websocket.Upgrader{
		ReadBufferSize: 1024, WriteBufferSize: 8192,
		CheckOrigin: func(r *http.Request) bool { return s.originAllowed(r.Header.Get("Origin")) },
	}
	_, protocol := requestCredentials(request)
	if protocol != "" {
		upgrader.Subprotocols = []string{protocol}
	}
	conn, err := upgrader.Upgrade(writer, request, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetReadLimit(16 << 10)
	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	var requestMessage wsRequest
	if err := readWSRequest(conn, &requestMessage); err != nil || requestMessage.Protocol != protocolVersion || requestMessage.Type != "subscribe" || strings.TrimSpace(requestMessage.RequestID) == "" {
		_ = conn.WriteJSON(protocolError(requestMessage.RequestID, "invalid_request", "a signals.v1 subscribe message with request_id is required", false))
		return
	}
	filter, afterCursor, historyLimit, err := makeSubscription(requestMessage.Data)
	if err != nil {
		_ = conn.WriteJSON(protocolError(requestMessage.RequestID, "invalid_subscription", err.Error(), false))
		return
	}
	subscriptionID, err := newSubscriptionID()
	if err != nil {
		_ = conn.WriteJSON(protocolError(requestMessage.RequestID, "internal_error", "could not allocate subscription", true))
		return
	}
	client := s.options.Hub.subscribe(filter, 2048)
	defer s.options.Hub.unsubscribe(client)
	if err := conn.WriteJSON(wireEnvelope{
		Protocol: protocolVersion, Type: "subscribed", RequestID: requestMessage.RequestID, SubscriptionID: subscriptionID,
		Data: map[string]any{"server_time": time.Now().UTC(), "heartbeat_seconds": 25, "delivery": "at_least_once"},
	}); err != nil {
		return
	}

	snapshots := s.filteredSnapshots(filter)
	for _, snapshot := range snapshots {
		events, err := snapshotAnalysisEvents(snapshot, time.Now())
		if err != nil {
			_ = conn.WriteJSON(protocolError(requestMessage.RequestID, "conversion_failed", err.Error(), true))
			return
		}
		if err := conn.WriteJSON(wireEnvelope{Protocol: protocolVersion, Type: "snapshot", SubscriptionID: subscriptionID, Data: events}); err != nil {
			return
		}
	}
	history, err := s.recentPoints(request.Context(), snapshots, historyLimit)
	if err != nil {
		s.options.Logger.Error("query signal history", "err", err)
		_ = conn.WriteJSON(protocolError(requestMessage.RequestID, "history_failed", "history query failed", true))
		return
	}
	convertedHistory, err := toAnalysisEvents(history, time.Now())
	if err != nil {
		_ = conn.WriteJSON(protocolError(requestMessage.RequestID, "conversion_failed", err.Error(), true))
		return
	}
	if err := conn.WriteJSON(wireEnvelope{Protocol: protocolVersion, Type: "history", SubscriptionID: subscriptionID, Data: convertedHistory}); err != nil {
		return
	}
	lastCursor := afterCursor
	if lastCursor > 0 {
		for {
			events, err := s.options.EventStore.QueryEvents(request.Context(), store.EventFilter{AfterCursor: lastCursor, Limit: 5000})
			if err != nil {
				return
			}
			for _, event := range events {
				if matches(filter, event) {
					converted, err := toAnalysisEvent(event, time.Now())
					if err != nil {
						return
					}
					if err := conn.WriteJSON(wireEnvelope{Protocol: protocolVersion, Type: "event", SubscriptionID: subscriptionID, Cursor: eventCursor(event), Data: converted}); err != nil {
						return
					}
				}
				lastCursor = event.Cursor
			}
			if len(events) < 5000 {
				break
			}
		}
	}
	client.activate(lastCursor)
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	})
	ctx, cancel := context.WithCancel(request.Context())
	defer cancel()
	go func() {
		if err := s.writeClient(ctx, conn, client, subscriptionID); err != nil {
			s.options.Logger.Debug("WebSocket writer ended", "err", err)
		}
		_ = conn.Close()
	}()
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func readWSRequest(conn *websocket.Conn, target any) error {
	_, reader, err := conn.NextReader()
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("subscription must contain exactly one JSON value")
	}
	return nil
}

func (s *Server) recentPoints(ctx context.Context, snapshots []model.SeriesSnapshot, limit int) ([]model.SignalEvent, error) {
	events := make([]model.SignalEvent, 0, len(snapshots)*len(s.options.Periods)*limit)
	for _, snapshot := range snapshots {
		for _, state := range snapshot.Indicators {
			points, err := s.options.EventStore.QueryRecentPoints(ctx, store.PointFilter{
				Dataset: snapshot.Series.Dataset, Symbol: snapshot.Series.Symbol,
				Timeframe: snapshot.Series.Timeframe, Period: state.Period,
				Algorithm: state.Algorithm, Limit: limit,
			})
			if err != nil {
				return nil, err
			}
			events = append(events, points...)
		}
	}
	return events, nil
}

func (s *Server) writeClient(ctx context.Context, conn *websocket.Conn, client *hubClient, subscriptionID string) error {
	ping := time.NewTicker(25 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-client.send:
			if !ok {
				return fmt.Errorf("client buffer closed")
			}
			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			converted, err := toAnalysisEvent(event, time.Now())
			if err != nil {
				return err
			}
			if err := conn.WriteJSON(wireEnvelope{Protocol: protocolVersion, Type: "event", SubscriptionID: subscriptionID, Cursor: eventCursor(event), Data: converted}); err != nil {
				return err
			}
		case <-ping.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				return err
			}
		}
	}
}

func (s *Server) filteredSnapshots(filter Subscription) []model.SeriesSnapshot {
	all := s.options.Registry.Snapshots()
	out := make([]model.SeriesSnapshot, 0, len(all))
	for _, snapshot := range all {
		if len(filter.Series) > 0 && !matchesSeries(filter.Series, snapshot.Series.Dataset, snapshot.Series.Symbol, snapshot.Series.Timeframe) {
			continue
		}
		if len(filter.Analyses) > 0 {
			confirmed := snapshot.Indicators[:0:0]
			for _, state := range snapshot.Indicators {
				if matchesAnalysis(filter.Analyses, state.Algorithm) {
					confirmed = append(confirmed, state)
				}
			}
			snapshot.Indicators = confirmed
			provisional := snapshot.Provisional[:0:0]
			for _, state := range snapshot.Provisional {
				if matchesAnalysis(filter.Analyses, state.Algorithm) {
					provisional = append(provisional, state)
				}
			}
			snapshot.Provisional = provisional
			if snapshot.Market != nil {
				if !matchesAnalysis(filter.Analyses, snapshot.Market.Algorithm) {
					snapshot.Market = nil
				}
			}
		}
		if len(filter.EventTypes) > 0 {
			if _, ok := filter.EventTypes[analysisEventType]; !ok {
				snapshot.Indicators = nil
				snapshot.Provisional = nil
				snapshot.Market = nil
			}
		}
		if !filter.Provisional {
			snapshot.Provisional = nil
			snapshot.FormingOpenTime = nil
		}
		if !filter.Confirmed {
			snapshot.Indicators = nil
			snapshot.Market = nil
		}
		out = append(out, snapshot)
	}
	return out
}

func makeSubscription(request wsSubscriptionData) (Subscription, int64, int, error) {
	if len(request.Series) == 0 {
		return Subscription{}, 0, 0, fmt.Errorf("at least one series selector is required")
	}
	for _, selector := range request.Series {
		if strings.TrimSpace(selector.Symbol) == "" || strings.TrimSpace(selector.Timeframe) == "" {
			return Subscription{}, 0, 0, fmt.Errorf("each series requires symbol and timeframe")
		}
		if _, err := model.TimeframeDuration(selector.Timeframe); err != nil {
			return Subscription{}, 0, 0, err
		}
	}
	filter := Subscription{Series: request.Series, Analyses: request.Analyses, EventTypes: stringSet(request.EventTypes)}
	for _, selector := range filter.Analyses {
		if strings.TrimSpace(selector.Name) == "" {
			return Subscription{}, 0, 0, fmt.Errorf("analysis selector name is required")
		}
	}
	if len(filter.EventTypes) == 0 {
		filter.EventTypes[analysisEventType] = struct{}{}
	}
	if _, wildcard := filter.EventTypes["*"]; wildcard {
		filter.EventTypes = nil
	}
	for eventType := range filter.EventTypes {
		if strings.TrimSpace(eventType) == "" {
			return Subscription{}, 0, 0, fmt.Errorf("event type cannot be empty")
		}
	}
	statuses := stringSet(request.Statuses)
	if len(statuses) == 0 {
		statuses["confirmed"] = struct{}{}
	}
	for status := range statuses {
		if status != "confirmed" && status != "provisional" {
			return Subscription{}, 0, 0, fmt.Errorf("statuses must contain only confirmed or provisional")
		}
	}
	_, filter.Confirmed = statuses["confirmed"]
	_, filter.Provisional = statuses["provisional"]
	afterCursor := int64(0)
	var err error
	if request.ResumeAfter != "" {
		afterCursor, err = strconv.ParseInt(request.ResumeAfter, 10, 64)
		if err != nil || afterCursor < 0 {
			return Subscription{}, 0, 0, fmt.Errorf("resume_after must be a non-negative integer string")
		}
	}
	historyLimit := request.History.Limit
	if historyLimit == 0 {
		historyLimit = defaultHistoryLimit
	}
	if historyLimit < 0 || historyLimit > maxHistoryLimit {
		return Subscription{}, 0, 0, fmt.Errorf("history.limit must be from 0 to %d", maxHistoryLimit)
	}
	return filter, afterCursor, historyLimit, nil
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			set[value] = struct{}{}
		}
	}
	return set
}

func newSubscriptionID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func protocolError(requestID, code, message string, retryable bool) wireEnvelope {
	return wireEnvelope{Protocol: protocolVersion, Type: "error", RequestID: requestID,
		Error: &wireError{Code: code, Message: message, Retryable: retryable}}
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if s.options.InsecureNoAuth {
			next.ServeHTTP(writer, request)
			return
		}
		provided, _ := requestCredentials(request)
		if len(provided) != len(s.options.AuthToken) || subtle.ConstantTimeCompare([]byte(provided), []byte(s.options.AuthToken)) != 1 {
			writer.Header().Set("WWW-Authenticate", "Bearer")
			writeError(writer, http.StatusUnauthorized, "unauthorized", "unauthorized")
			return
		}
		next.ServeHTTP(writer, request)
	})
}

func requestCredentials(request *http.Request) (string, string) {
	authorization := request.Header.Get("Authorization")
	if strings.HasPrefix(authorization, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer ")), ""
	}
	for _, protocol := range websocket.Subprotocols(request) {
		if !strings.HasPrefix(protocol, "bearer.") {
			continue
		}
		decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(protocol, "bearer."))
		if err == nil {
			return string(decoded), protocol
		}
	}
	return "", ""
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		origin := request.Header.Get("Origin")
		if origin != "" && s.originAllowed(origin) {
			writer.Header().Set("Access-Control-Allow-Origin", origin)
			writer.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			writer.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			writer.Header().Add("Vary", "Origin")
		}
		if request.Method == http.MethodOptions {
			if origin == "" || !s.originAllowed(origin) {
				http.Error(writer, "origin not allowed", http.StatusForbidden)
				return
			}
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(writer, request)
	})
}

func (s *Server) security(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("X-Frame-Options", "DENY")
		writer.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(writer, request)
	})
}

func (s *Server) originAllowed(origin string) bool {
	if origin == "" {
		return true
	}
	_, ok := s.origins[origin]
	return ok
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeError(writer http.ResponseWriter, status int, code, message string) {
	writeJSON(writer, status, protocolError("", code, message, status >= http.StatusInternalServerError))
}
