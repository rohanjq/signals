package store

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rohanjq/signals/internal/model"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Postgres struct {
	pool *pgxpool.Pool
}

const outboxAdvisoryLock int64 = 746264445846543

func OpenPostgres(ctx context.Context, databaseURL string, maxConnections int32) (*Postgres, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	if maxConnections > 0 {
		config.MaxConns = maxConnections
	}
	config.MinConns = 1
	config.MaxConnIdleTime = 5 * time.Minute
	config.MaxConnLifetime = 30 * time.Minute
	config.HealthCheckPeriod = 30 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL pool: %w", err)
	}
	store := &Postgres{pool: pool}
	if err := store.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := store.Migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return store, nil
}

func (s *Postgres) Close() { s.pool.Close() }

func (s *Postgres) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return nil
}

func (s *Postgres) Migrate(ctx context.Context) error {
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", int64(746264445846542)); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS signal_schema_migrations (
		version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create migration ledger: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		prefix, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			return fmt.Errorf("migration %q has no numeric prefix", entry.Name())
		}
		version, err := strconv.Atoi(prefix)
		if err != nil || version <= 0 {
			return fmt.Errorf("migration %q has invalid version", entry.Name())
		}
		var applied bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM signal_schema_migrations WHERE version=$1)`, version).Scan(&applied); err != nil {
			return fmt.Errorf("check migration %d: %w", version, err)
		}
		if applied {
			continue
		}
		sql, err := migrations.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read migration %d: %w", version, err)
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("apply migration %d: %w", version, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO signal_schema_migrations (version) VALUES ($1)
			ON CONFLICT (version) DO NOTHING`, version); err != nil {
			return fmt.Errorf("record migration %d: %w", version, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}

func (s *Postgres) LoadRecovery(ctx context.Context, key model.SeriesKey) (model.SeriesRecovery, error) {
	var recovery model.SeriesRecovery
	err := s.pool.QueryRow(ctx, `SELECT last_open_time, last_bar_hash FROM signal_series_cursors
		WHERE dataset=$1 AND symbol=$2 AND timeframe=$3`, key.Dataset, key.Symbol, key.Timeframe).
		Scan(&recovery.CursorOpenTime, &recovery.CursorHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return recovery, nil
	}
	if err != nil {
		return recovery, fmt.Errorf("load series cursor: %w", err)
	}
	recovery.HasCursor = true
	rows, err := s.pool.Query(ctx, `SELECT indicator, period, algorithm_version, config_hash, last_open_time, state
		FROM signal_reducer_snapshots WHERE dataset=$1 AND symbol=$2 AND timeframe=$3 ORDER BY period`,
		key.Dataset, key.Symbol, key.Timeframe)
	if err != nil {
		return model.SeriesRecovery{}, fmt.Errorf("load reducer snapshots: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var snapshot model.ReducerSnapshot
		if err := rows.Scan(&snapshot.Algorithm.Name, &snapshot.Period, &snapshot.Algorithm.Version,
			&snapshot.Algorithm.ConfigHash, &snapshot.LastOpenTime, &snapshot.State); err != nil {
			return model.SeriesRecovery{}, fmt.Errorf("scan reducer snapshot: %w", err)
		}
		recovery.Snapshots = append(recovery.Snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		return model.SeriesRecovery{}, fmt.Errorf("read reducer snapshots: %w", err)
	}
	return recovery, nil
}

func (s *Postgres) RebuildSeries(ctx context.Context, key model.SeriesKey, events []model.SignalEvent, snapshots []model.ReducerSnapshot, lastBar *model.Bar) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", advisoryKey(key)); err != nil {
		return fmt.Errorf("lock series rebuild: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM signal_reducer_snapshots
		WHERE dataset=$1 AND symbol=$2 AND timeframe=$3`,
		key.Dataset, key.Symbol, key.Timeframe); err != nil {
		return fmt.Errorf("clear stale reducer snapshots: %w", err)
	}
	// Rebuild output is authoritative for this series. Remove points generated
	// from a superseded OHLC seed so lower revision numbers from a historical
	// correction cannot coexist with (and lose to) stale live revisions.
	if _, err := tx.Exec(ctx, `DELETE FROM signal_indicator_points
		WHERE dataset=$1 AND symbol=$2 AND timeframe=$3`,
		key.Dataset, key.Symbol, key.Timeframe); err != nil {
		return fmt.Errorf("clear stale indicator points: %w", err)
	}
	for _, event := range events {
		if err := persistEventState(ctx, tx, event); err != nil {
			return err
		}
	}
	for _, snapshot := range snapshots {
		if err := upsertSnapshot(ctx, tx, key, snapshot); err != nil {
			return err
		}
	}
	if lastBar != nil {
		if err := upsertCursor(ctx, tx, key, *lastBar); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Postgres) CommitClosed(ctx context.Context, key model.SeriesKey, bar model.Bar, events []model.SignalEvent, snapshots []model.ReducerSnapshot) ([]model.SignalEvent, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", outboxAdvisoryLock); err != nil {
		return nil, fmt.Errorf("lock signal outbox ordering: %w", err)
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", advisoryKey(key)); err != nil {
		return nil, fmt.Errorf("lock series: %w", err)
	}

	var lastOpenTime time.Time
	var lastHash string
	err = tx.QueryRow(ctx, `SELECT last_open_time, last_bar_hash FROM signal_series_cursors
		WHERE dataset=$1 AND symbol=$2 AND timeframe=$3 FOR UPDATE`, key.Dataset, key.Symbol, key.Timeframe).Scan(&lastOpenTime, &lastHash)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("read series cursor: %w", err)
	}
	if err == nil && !bar.OpenTime.After(lastOpenTime) {
		if bar.OpenTime.Equal(lastOpenTime) && barHash(bar) == lastHash {
			return nil, tx.Commit(ctx)
		}
		return nil, fmt.Errorf("bar at %s conflicts with durable cursor %s", bar.OpenTime, lastOpenTime)
	}

	committed := make([]model.SignalEvent, 0, len(events))
	for _, event := range events {
		if err := persistEventState(ctx, tx, event); err != nil {
			return nil, err
		}
		payload, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("encode signal event: %w", err)
		}
		var cursor int64
		err = tx.QueryRow(ctx, `INSERT INTO signal_event_outbox
			(event_id,dataset,symbol,timeframe,period,event_type,payload) VALUES ($1,$2,$3,$4,$5,$6,$7)
			ON CONFLICT (event_id) DO UPDATE SET event_id=EXCLUDED.event_id RETURNING cursor`,
			event.EventID, event.Dataset, event.Symbol, event.Timeframe, eventPeriod(event), event.EventType, payload).Scan(&cursor)
		if err != nil {
			return nil, fmt.Errorf("insert signal outbox: %w", err)
		}
		event.Cursor = cursor
		committed = append(committed, event)
	}
	for _, snapshot := range snapshots {
		if err := upsertSnapshot(ctx, tx, key, snapshot); err != nil {
			return nil, err
		}
	}
	if err := upsertCursor(ctx, tx, key, bar); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return committed, nil
}

func (s *Postgres) QueryEvents(ctx context.Context, filter EventFilter) ([]model.SignalEvent, error) {
	filter.normalize()
	rows, err := s.pool.Query(ctx, `SELECT cursor, payload FROM signal_event_outbox
		WHERE cursor > $1
		AND ($2 = '' OR dataset = $2)
		AND ($3 = '' OR symbol = $3)
		AND ($4 = '' OR timeframe = $4)
		AND ($5 = 0 OR period = $5)
		AND ($6 = '' OR event_type = $6)
		AND ($7 = '' OR payload->'algorithm'->>'name' = $7)
		ORDER BY cursor ASC LIMIT $8`, filter.AfterCursor, filter.Dataset, filter.Symbol, filter.Timeframe, filter.Period, filter.EventType, filter.Analysis, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *Postgres) QueryRecentPoints(ctx context.Context, filter PointFilter) ([]model.SignalEvent, error) {
	filter.normalize()
	rows, err := s.pool.Query(ctx, `SELECT event_id, dataset, symbol, timeframe, bar_open_time,
		bar_revision, indicator, period, value, samples, algorithm_version, config_hash
		FROM signal_indicator_points
		WHERE ($1 = '' OR dataset = $1)
		AND ($2 = '' OR symbol = $2)
		AND ($3 = '' OR timeframe = $3)
		AND ($4 = 0 OR period = $4)
		AND ($5 = '' OR indicator = $5)
		AND ($6 = '' OR algorithm_version = $6)
		AND ($7 = '' OR config_hash = $7)
		ORDER BY bar_open_time DESC, event_id DESC LIMIT $8`,
		filter.Dataset, filter.Symbol, filter.Timeframe, filter.Period, filter.Algorithm.Name,
		filter.Algorithm.Version, filter.Algorithm.ConfigHash, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]model.SignalEvent, 0, filter.Limit)
	for rows.Next() {
		var event model.SignalEvent
		var revision, samples int64
		event.Schema = "signal.event.v1"
		event.EventType = "indicator.point"
		event.Indicator = &model.IndicatorPayload{Ready: true}
		if err := rows.Scan(&event.EventID, &event.Dataset, &event.Symbol, &event.Timeframe,
			&event.BarOpenTime, &revision, &event.Algorithm.Name, &event.Indicator.Period,
			&event.Indicator.Value, &samples, &event.Algorithm.Version, &event.Algorithm.ConfigHash); err != nil {
			return nil, err
		}
		event.BarRevision = uint64(revision)
		event.AnalysisRevision = uint64(revision)
		event.Indicator.Samples = uint64(samples)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for left, right := 0, len(events)-1; left < right; left, right = left+1, right-1 {
		events[left], events[right] = events[right], events[left]
	}
	return events, nil
}

func (s *Postgres) PendingEvents(ctx context.Context, limit int) ([]model.SignalEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT cursor, payload FROM signal_event_outbox
		WHERE published_at IS NULL ORDER BY cursor ASC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *Postgres) MarkPublished(ctx context.Context, cursors []int64) error {
	if len(cursors) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `UPDATE signal_event_outbox SET published_at=COALESCE(published_at, now()) WHERE cursor = ANY($1)`, cursors)
	return err
}

func insertPoint(ctx context.Context, tx pgx.Tx, event model.SignalEvent) error {
	if event.Indicator == nil {
		return fmt.Errorf("indicator point %q has no indicator payload", event.EventID)
	}
	_, err := tx.Exec(ctx, `INSERT INTO signal_indicator_points
		(event_id,dataset,symbol,timeframe,bar_open_time,bar_revision,indicator,period,value,samples,algorithm_version,config_hash)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (dataset,symbol,timeframe,bar_open_time,bar_revision,indicator,period,algorithm_version,config_hash)
		DO UPDATE SET event_id=EXCLUDED.event_id,value=EXCLUDED.value,samples=EXCLUDED.samples,
			algorithm_version=EXCLUDED.algorithm_version,config_hash=EXCLUDED.config_hash`, event.EventID, event.Dataset, event.Symbol, event.Timeframe,
		event.BarOpenTime, event.BarRevision, event.Algorithm.Name, event.Indicator.Period, event.Indicator.Value,
		event.Indicator.Samples, event.Algorithm.Version, event.Algorithm.ConfigHash)
	if err != nil {
		return fmt.Errorf("insert indicator point: %w", err)
	}
	return nil
}

func upsertMarketState(ctx context.Context, tx pgx.Tx, event model.SignalEvent) error {
	if event.Market == nil {
		return fmt.Errorf("market state %q has no market payload", event.EventID)
	}
	state, err := json.Marshal(event.Market)
	if err != nil {
		return fmt.Errorf("encode market state: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO signal_market_states
		(event_id,dataset,symbol,timeframe,bar_open_time,bar_revision,algorithm,algorithm_version,config_hash,state)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (dataset,symbol,timeframe,algorithm,algorithm_version,config_hash)
		DO UPDATE SET event_id=EXCLUDED.event_id,bar_open_time=EXCLUDED.bar_open_time,
		bar_revision=EXCLUDED.bar_revision,state=EXCLUDED.state,updated_at=now()
		WHERE signal_market_states.bar_open_time <= EXCLUDED.bar_open_time`,
		event.EventID, event.Dataset, event.Symbol, event.Timeframe, event.BarOpenTime, event.BarRevision,
		event.Algorithm.Name, event.Algorithm.Version, event.Algorithm.ConfigHash, state)
	if err != nil {
		return fmt.Errorf("upsert market state: %w", err)
	}
	return nil
}

func persistEventState(ctx context.Context, tx pgx.Tx, event model.SignalEvent) error {
	switch event.EventType {
	case "indicator.point", "":
		if event.Indicator == nil {
			return fmt.Errorf("signal event %q has no typed payload", event.EventID)
		}
		return insertPoint(ctx, tx, event)
	case "market.state":
		return upsertMarketState(ctx, tx, event)
	default:
		return fmt.Errorf("unsupported signal event type %q", event.EventType)
	}
}

func eventPeriod(event model.SignalEvent) int {
	if event.Indicator == nil {
		return 0
	}
	return event.Indicator.Period
}

func upsertSnapshot(ctx context.Context, tx pgx.Tx, key model.SeriesKey, snapshot model.ReducerSnapshot) error {
	_, err := tx.Exec(ctx, `INSERT INTO signal_reducer_snapshots
		(dataset,symbol,timeframe,indicator,period,algorithm_version,config_hash,last_open_time,state)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (dataset,symbol,timeframe,indicator,period,algorithm_version,config_hash)
		DO UPDATE SET last_open_time=EXCLUDED.last_open_time,state=EXCLUDED.state,updated_at=now()`,
		key.Dataset, key.Symbol, key.Timeframe, snapshot.Algorithm.Name, snapshot.Period,
		snapshot.Algorithm.Version, snapshot.Algorithm.ConfigHash, snapshot.LastOpenTime, snapshot.State)
	if err != nil {
		return fmt.Errorf("upsert reducer snapshot: %w", err)
	}
	return nil
}

func upsertCursor(ctx context.Context, tx pgx.Tx, key model.SeriesKey, bar model.Bar) error {
	result, err := tx.Exec(ctx, `INSERT INTO signal_series_cursors
		(dataset,symbol,timeframe,last_open_time,last_bar_hash) VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (dataset,symbol,timeframe) DO UPDATE SET
		last_open_time=EXCLUDED.last_open_time,last_bar_hash=EXCLUDED.last_bar_hash,updated_at=now()
		WHERE signal_series_cursors.last_open_time <= EXCLUDED.last_open_time`,
		key.Dataset, key.Symbol, key.Timeframe, bar.OpenTime, barHash(bar))
	if err != nil {
		return fmt.Errorf("upsert series cursor: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("series cursor would move backwards from %s", bar.OpenTime)
	}
	return nil
}

type rowScanner interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func scanEvents(rows rowScanner) ([]model.SignalEvent, error) {
	events := make([]model.SignalEvent, 0)
	for rows.Next() {
		var cursor int64
		var payload []byte
		if err := rows.Scan(&cursor, &payload); err != nil {
			return nil, err
		}
		var event model.SignalEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, fmt.Errorf("decode outbox event %d: %w", cursor, err)
		}
		event.Cursor = cursor
		events = append(events, event)
	}
	return events, rows.Err()
}

func advisoryKey(key model.SeriesKey) int64 {
	sum := sha256.Sum256([]byte(key.String()))
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

func barHash(bar model.Bar) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(bar.Symbol))
	_, _ = hash.Write([]byte(bar.Timeframe))
	var data [8]byte
	for _, value := range []float64{bar.Open, bar.High, bar.Low, bar.Close, bar.Volume} {
		binary.BigEndian.PutUint64(data[:], math.Float64bits(value))
		_, _ = hash.Write(data[:])
	}
	binary.BigEndian.PutUint64(data[:], bar.Trades)
	_, _ = hash.Write(data[:])
	return hex.EncodeToString(hash.Sum(nil))
}

type EventFilter struct {
	AfterCursor int64
	Dataset     string
	Symbol      string
	Timeframe   string
	Period      int
	EventType   string
	Analysis    string
	Limit       int
}

type PointFilter struct {
	Dataset   string
	Symbol    string
	Timeframe string
	Period    int
	Algorithm model.AlgorithmRef
	Limit     int
}

func (f *EventFilter) normalize() {
	if f.Limit <= 0 {
		f.Limit = 500
	}
	if f.Limit > 5000 {
		f.Limit = 5000
	}
}

func (f *PointFilter) normalize() {
	if f.Limit <= 0 {
		f.Limit = 500
	}
	if f.Limit > 5000 {
		f.Limit = 5000
	}
}

func SortEvents(events []model.SignalEvent) {
	sort.Slice(events, func(i, j int) bool { return events[i].Cursor < events[j].Cursor })
}
