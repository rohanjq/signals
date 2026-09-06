package config

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rohanjq/signals/internal/model"
)

type Config struct {
	HTTPAddr           string
	DatabaseURL        string
	DatabaseMaxConns   int
	AllowMemoryStore   bool
	OHLCWebSocketURL   string
	OHLCToken          string
	Dataset            string
	Series             []model.SeriesKey
	Periods            []int
	SeedBars           int
	MailboxSize        int
	SnapshotEvery      uint64
	APIAuthToken       string
	InsecureNoAuth     bool
	AllowedOrigins     []string
	ShutdownTimeout    time.Duration
	OutboxPollInterval time.Duration
	LogLevel           slog.Level
}

type lookupFunc func(string) (string, bool)

func Load() (Config, error) { return load(os.LookupEnv) }

func load(lookup lookupFunc) (Config, error) {
	var config Config
	config.HTTPAddr = valueOr(lookup, "SIGNALD_HTTP_ADDR", "127.0.0.1:8090")
	config.DatabaseURL = valueOr(lookup, "SIGNALD_DATABASE_URL", "postgres://signald:signald@127.0.0.1:5433/signald?sslmode=disable")
	config.OHLCWebSocketURL = valueOr(lookup, "SIGNALD_OHLC_WS_URL", "ws://127.0.0.1:8080/ws")
	config.OHLCToken = valueOr(lookup, "SIGNALD_OHLC_TOKEN", "")
	config.Dataset = valueOr(lookup, "SIGNALD_DATASET", "live")
	config.APIAuthToken = valueOr(lookup, "SIGNALD_API_TOKEN", "")
	config.AllowedOrigins = splitNonEmpty(valueOr(lookup, "SIGNALD_ALLOWED_ORIGINS", "http://127.0.0.1:8080,http://localhost:8080,http://127.0.0.1:8082,http://localhost:8082,http://127.0.0.1:18110,http://localhost:18110"))

	var err error
	config.DatabaseMaxConns, err = intValue(lookup, "SIGNALD_DATABASE_MAX_CONNS", 10, 1, 100)
	if err != nil {
		return Config{}, err
	}
	config.SeedBars, err = intValue(lookup, "SIGNALD_SEED_BARS", 500, 1, 25000)
	if err != nil {
		return Config{}, err
	}
	config.MailboxSize, err = intValue(lookup, "SIGNALD_MAILBOX_SIZE", 1024, 1, 100000)
	if err != nil {
		return Config{}, err
	}
	snapshotEvery, err := intValue(lookup, "SIGNALD_SNAPSHOT_EVERY", 100, 1, 1000000)
	if err != nil {
		return Config{}, err
	}
	config.SnapshotEvery = uint64(snapshotEvery)
	config.AllowMemoryStore, err = boolValue(lookup, "SIGNALD_ALLOW_MEMORY_STORE", false)
	if err != nil {
		return Config{}, err
	}
	config.InsecureNoAuth, err = boolValue(lookup, "SIGNALD_INSECURE_NO_AUTH", false)
	if err != nil {
		return Config{}, err
	}
	config.ShutdownTimeout, err = durationValue(lookup, "SIGNALD_SHUTDOWN_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	config.OutboxPollInterval, err = durationValue(lookup, "SIGNALD_OUTBOX_POLL_INTERVAL", 250*time.Millisecond)
	if err != nil {
		return Config{}, err
	}

	config.Periods, err = parsePeriods(valueOr(lookup, "SIGNALD_EMA_PERIODS", "9,21,50,200"))
	if err != nil {
		return Config{}, err
	}
	config.Series, err = parseSeries(config.Dataset, valueOr(lookup, "SIGNALD_SERIES", "BTCUSDT:1m,BTCUSDT:5m,BTCUSDT:15m,BTCUSDT:1h"))
	if err != nil {
		return Config{}, err
	}
	if config.SeedBars < config.Periods[len(config.Periods)-1] {
		return Config{}, fmt.Errorf("SIGNALD_SEED_BARS must be at least the largest EMA period (%d)", config.Periods[len(config.Periods)-1])
	}
	if !config.InsecureNoAuth && config.APIAuthToken == "" {
		return Config{}, fmt.Errorf("SIGNALD_API_TOKEN is required unless SIGNALD_INSECURE_NO_AUTH=true")
	}
	if config.DatabaseURL == "" && !config.AllowMemoryStore {
		return Config{}, fmt.Errorf("SIGNALD_DATABASE_URL cannot be empty unless SIGNALD_ALLOW_MEMORY_STORE=true")
	}
	if config.Dataset == "" {
		return Config{}, fmt.Errorf("SIGNALD_DATASET cannot be empty")
	}
	if len(config.AllowedOrigins) == 0 {
		return Config{}, fmt.Errorf("SIGNALD_ALLOWED_ORIGINS cannot be empty")
	}

	switch strings.ToLower(valueOr(lookup, "SIGNALD_LOG_LEVEL", "info")) {
	case "debug":
		config.LogLevel = slog.LevelDebug
	case "info":
		config.LogLevel = slog.LevelInfo
	case "warn":
		config.LogLevel = slog.LevelWarn
	case "error":
		config.LogLevel = slog.LevelError
	default:
		return Config{}, fmt.Errorf("invalid SIGNALD_LOG_LEVEL")
	}
	return config, nil
}

func parsePeriods(value string) ([]int, error) {
	parts := splitNonEmpty(value)
	if len(parts) == 0 {
		return nil, fmt.Errorf("SIGNALD_EMA_PERIODS requires at least one period")
	}
	seen := make(map[int]struct{}, len(parts))
	periods := make([]int, 0, len(parts))
	for _, part := range parts {
		period, err := strconv.Atoi(part)
		if err != nil || period <= 0 || period > 100000 {
			return nil, fmt.Errorf("invalid EMA period %q", part)
		}
		if _, exists := seen[period]; exists {
			return nil, fmt.Errorf("duplicate EMA period %d", period)
		}
		seen[period] = struct{}{}
		periods = append(periods, period)
	}
	sort.Ints(periods)
	return periods, nil
}

func parseSeries(dataset, value string) ([]model.SeriesKey, error) {
	parts := splitNonEmpty(value)
	if len(parts) == 0 {
		return nil, fmt.Errorf("SIGNALD_SERIES requires at least one symbol:timeframe")
	}
	seen := make(map[string]struct{}, len(parts))
	series := make([]model.SeriesKey, 0, len(parts))
	for _, part := range parts {
		separator := strings.LastIndexByte(part, ':')
		if separator <= 0 || separator == len(part)-1 {
			return nil, fmt.Errorf("invalid series %q; expected SYMBOL:TIMEFRAME", part)
		}
		key := model.SeriesKey{Dataset: dataset, Symbol: strings.TrimSpace(part[:separator]), Timeframe: strings.TrimSpace(part[separator+1:])}
		if _, err := model.TimeframeDuration(key.Timeframe); err != nil {
			return nil, err
		}
		if _, exists := seen[key.String()]; exists {
			return nil, fmt.Errorf("duplicate series %s", key.String())
		}
		seen[key.String()] = struct{}{}
		series = append(series, key)
	}
	return series, nil
}

func splitNonEmpty(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func valueOr(lookup lookupFunc, name, fallback string) string {
	if value, ok := lookup(name); ok {
		return strings.TrimSpace(value)
	}
	return fallback
}

func intValue(lookup lookupFunc, name string, fallback, minimum, maximum int) (int, error) {
	raw := valueOr(lookup, name, strconv.Itoa(fallback))
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be an integer from %d to %d", name, minimum, maximum)
	}
	return value, nil
}

func boolValue(lookup lookupFunc, name string, fallback bool) (bool, error) {
	raw := valueOr(lookup, name, strconv.FormatBool(fallback))
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	return value, nil
}

func durationValue(lookup lookupFunc, name string, fallback time.Duration) (time.Duration, error) {
	raw := valueOr(lookup, name, fallback.String())
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return value, nil
}
