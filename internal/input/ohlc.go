package input

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rohanjq/signals/internal/model"
	"github.com/rohanjq/signals/internal/monitor"
)

type Sink interface {
	Seed(context.Context, string, string, []model.Bar) error
	Closed(context.Context, model.Bar) error
	Forming(context.Context, model.Bar) error
}

type OHLCOptions struct {
	URL           string
	Token         string
	Series        []model.SeriesKey
	SeedBars      int
	Sink          Sink
	Metrics       *monitor.Metrics
	Logger        *slog.Logger
	MinBackoff    time.Duration
	MaxBackoff    time.Duration
	HandshakeWait time.Duration
}

type OHLCClient struct {
	options OHLCOptions
}

type subscribeMessage struct {
	Action    string `json:"action"`
	Symbol    string `json:"symbol"`
	Timeframe string `json:"tf"`
	SeedBars  int    `json:"seed_bars"`
}

type serverMessage struct {
	Type string      `json:"type"`
	Key  string      `json:"key,omitempty"`
	Bars []model.Bar `json:"bars,omitempty"`
	Bar  *model.Bar  `json:"bar,omitempty"`
	Err  string      `json:"error,omitempty"`
}

func NewOHLCClient(options OHLCOptions) (*OHLCClient, error) {
	if options.URL == "" || options.Sink == nil || len(options.Series) == 0 || options.SeedBars <= 0 {
		return nil, fmt.Errorf("OHLC URL, sink, series, and positive seed size are required")
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.Metrics == nil {
		options.Metrics = monitor.NewMetrics()
	}
	if options.MinBackoff <= 0 {
		options.MinBackoff = time.Second
	}
	if options.MaxBackoff < options.MinBackoff {
		options.MaxBackoff = 30 * time.Second
	}
	if options.HandshakeWait <= 0 {
		options.HandshakeWait = 10 * time.Second
	}
	return &OHLCClient{options: options}, nil
}

func (c *OHLCClient) Run(ctx context.Context) error {
	backoff := c.options.MinBackoff
	for {
		if ctx.Err() != nil {
			return nil
		}
		connected, err := c.runConnection(ctx)
		c.options.Metrics.SetConnected(false)
		if ctx.Err() != nil {
			return nil
		}
		c.options.Metrics.RecordInputError()
		c.options.Metrics.RecordReconnect()
		if connected {
			backoff = c.options.MinBackoff
		}
		c.options.Logger.Warn("OHLC connection ended", "err", err, "retry_in", backoff)
		jitter := time.Duration(rand.Int64N(int64(backoff/4 + 1)))
		timer := time.NewTimer(backoff + jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		backoff *= 2
		if backoff > c.options.MaxBackoff {
			backoff = c.options.MaxBackoff
		}
	}
}

func (c *OHLCClient) runConnection(ctx context.Context) (bool, error) {
	headers := make(http.Header)
	if c.options.Token != "" {
		headers.Set("Authorization", "Bearer "+c.options.Token)
	}
	dialer := websocket.Dialer{HandshakeTimeout: c.options.HandshakeWait, Proxy: http.ProxyFromEnvironment}
	conn, response, err := dialer.DialContext(ctx, c.options.URL, headers)
	if err != nil {
		if response != nil {
			return false, fmt.Errorf("dial OHLC WebSocket: HTTP %d: %w", response.StatusCode, err)
		}
		return false, fmt.Errorf("dial OHLC WebSocket: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(64 << 20)

	var writeMu sync.Mutex
	for _, key := range c.options.Series {
		writeMu.Lock()
		err := conn.WriteJSON(subscribeMessage{Action: "subscribe", Symbol: key.Symbol, Timeframe: key.Timeframe, SeedBars: c.options.SeedBars})
		writeMu.Unlock()
		if err != nil {
			return true, fmt.Errorf("subscribe %s: %w", key.String(), err)
		}
	}
	c.options.Metrics.SetConnected(true)
	c.options.Logger.Info("connected to OHLC", "url", c.options.URL, "series", len(c.options.Series))

	pingCtx, cancelPing := context.WithCancel(ctx)
	defer cancelPing()
	go func() {
		ticker := time.NewTicker(25 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				writeMu.Lock()
				_ = conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
				writeMu.Unlock()
			}
		}
	}()
	go func() {
		<-pingCtx.Done()
		_ = conn.Close()
	}()

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return true, fmt.Errorf("read OHLC WebSocket: %w", err)
		}
		var message serverMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			return true, fmt.Errorf("decode OHLC message: %w", err)
		}
		if err := c.dispatch(ctx, message); err != nil {
			return true, err
		}
	}
}

func (c *OHLCClient) dispatch(ctx context.Context, message serverMessage) error {
	switch message.Type {
	case "seed":
		symbol, timeframe, err := seriesFromMessage(message)
		if err != nil {
			return err
		}
		if err := c.options.Sink.Seed(ctx, symbol, timeframe, message.Bars); err != nil {
			return fmt.Errorf("apply OHLC seed %s: %w", message.Key, err)
		}
	case "closed":
		if message.Bar == nil || !message.Bar.Closed {
			return fmt.Errorf("invalid closed OHLC message")
		}
		if err := c.options.Sink.Closed(ctx, *message.Bar); err != nil {
			return fmt.Errorf("apply closed OHLC bar: %w", err)
		}
	case "forming":
		if message.Bar == nil || message.Bar.Closed {
			return fmt.Errorf("invalid forming OHLC message")
		}
		if err := c.options.Sink.Forming(ctx, *message.Bar); err != nil {
			return fmt.Errorf("apply forming OHLC bar: %w", err)
		}
	case "error":
		return fmt.Errorf("OHLC server error: %s", message.Err)
	default:
		return fmt.Errorf("unsupported OHLC message type %q", message.Type)
	}
	c.options.Metrics.RecordInput(message.Type)
	return nil
}

func seriesFromMessage(message serverMessage) (string, string, error) {
	if len(message.Bars) > 0 {
		return message.Bars[0].Symbol, message.Bars[0].Timeframe, nil
	}
	for index := 0; index < len(message.Key); index++ {
		if message.Key[index] == '|' && index > 0 && index < len(message.Key)-1 {
			return message.Key[:index], message.Key[index+1:], nil
		}
	}
	return "", "", fmt.Errorf("seed message has no series identity")
}
