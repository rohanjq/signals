package engine

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/rohanjq/signals/internal/indicator"
	"github.com/rohanjq/signals/internal/model"
)

const signalSchema = "signal.event.v1"
const marketHistoryLimit = 500

type Store interface {
	LoadRecovery(context.Context, model.SeriesKey) (model.SeriesRecovery, error)
	RebuildSeries(context.Context, model.SeriesKey, []model.SignalEvent, []model.ReducerSnapshot, *model.Bar) error
	CommitClosed(context.Context, model.SeriesKey, model.Bar, []model.SignalEvent, []model.ReducerSnapshot) ([]model.SignalEvent, error)
	MarkPublished(context.Context, []int64) error
}

type Publisher interface {
	Publish(model.SignalEvent)
}

type LaneOptions struct {
	Key            model.SeriesKey
	Periods        []int
	MailboxSize    int
	SnapshotEvery  uint64
	Store          Store
	Publisher      Publisher
	OutboxDelivery bool
}

type Lane struct {
	key            model.SeriesKey
	periods        []int
	mailbox        chan command
	snapshotEvery  uint64
	store          Store
	publisher      Publisher
	outboxDelivery bool

	mu                 sync.RWMutex
	reducers           map[int]*indicator.EMA
	market             *indicator.Market
	lastBar            *model.Bar
	lastClosedRevision uint64
	closedCount        uint64
	provisional        map[int]float64
	formingTime        *time.Time
	formingRevision    uint64
	lastErr            error
}

type commandType uint8

const (
	commandSeed commandType = iota + 1
	commandClosed
	commandForming
)

type command struct {
	typeID commandType
	bars   []model.Bar
	bar    model.Bar
	done   chan error
}

func NewLane(options LaneOptions) (*Lane, error) {
	if options.Key.Dataset == "" || options.Key.Symbol == "" || options.Key.Timeframe == "" {
		return nil, fmt.Errorf("series key requires dataset, symbol, and timeframe")
	}
	if _, err := model.TimeframeDuration(options.Key.Timeframe); err != nil {
		return nil, err
	}
	if len(options.Periods) == 0 {
		return nil, fmt.Errorf("at least one EMA period is required")
	}
	if options.Store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if options.MailboxSize <= 0 {
		options.MailboxSize = 256
	}
	if options.SnapshotEvery == 0 {
		options.SnapshotEvery = 100
	}
	periods := append([]int(nil), options.Periods...)
	sort.Ints(periods)
	for index, period := range periods {
		if period <= 0 || (index > 0 && period == periods[index-1]) {
			return nil, fmt.Errorf("EMA periods must be unique positive integers")
		}
	}
	reducers, err := newReducers(periods)
	if err != nil {
		return nil, err
	}
	market, err := indicator.NewMarket(indicator.DefaultMarketConfig(), marketHistoryLimit)
	if err != nil {
		return nil, err
	}
	return &Lane{
		key:            options.Key,
		periods:        periods,
		mailbox:        make(chan command, options.MailboxSize),
		snapshotEvery:  options.SnapshotEvery,
		store:          options.Store,
		publisher:      options.Publisher,
		outboxDelivery: options.OutboxDelivery,
		reducers:       reducers,
		market:         market,
		provisional:    make(map[int]float64),
	}, nil
}

func (l *Lane) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-l.mailbox:
			var err error
			switch cmd.typeID {
			case commandSeed:
				err = l.applySeed(ctx, cmd.bars)
			case commandClosed:
				err = l.applyClosed(ctx, cmd.bar)
			case commandForming:
				err = l.applyForming(cmd.bar)
			}
			l.mu.Lock()
			l.lastErr = err
			l.mu.Unlock()
			cmd.done <- err
		}
	}
}

func (l *Lane) Seed(ctx context.Context, bars []model.Bar) error {
	return l.submit(ctx, command{typeID: commandSeed, bars: append([]model.Bar(nil), bars...)})
}

func (l *Lane) Closed(ctx context.Context, bar model.Bar) error {
	return l.submit(ctx, command{typeID: commandClosed, bar: bar})
}

func (l *Lane) Forming(ctx context.Context, bar model.Bar) error {
	return l.submit(ctx, command{typeID: commandForming, bar: bar})
}

func (l *Lane) submit(ctx context.Context, cmd command) error {
	cmd.done = make(chan error, 1)
	select {
	case l.mailbox <- cmd:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-cmd.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Lane) Snapshot() model.SeriesSnapshot {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return makeSeriesSnapshot(l.key, l.periods, l.reducers, l.market, l.lastBar, l.lastClosedRevision, l.formingTime, l.formingRevision, l.provisional)
}

func (l *Lane) LastError() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastErr
}

func (l *Lane) applySeed(ctx context.Context, bars []model.Bar) error {
	sort.SliceStable(bars, func(i, j int) bool { return bars[i].OpenTime.Before(bars[j].OpenTime) })
	l.mu.RLock()
	initialized := l.lastBar != nil
	l.mu.RUnlock()
	if initialized {
		return l.applyCatchUpSeed(ctx, bars)
	}
	recovery, err := l.store.LoadRecovery(ctx, l.key)
	if err != nil {
		return fmt.Errorf("load series recovery: %w", err)
	}
	if recovery.HasCursor {
		if err := l.restoreRecoverySeed(recovery, bars); err != nil {
			return err
		}
		return l.applyCatchUpSeed(ctx, bars)
	}

	reducers, err := newReducers(l.periods)
	if err != nil {
		return err
	}
	market, err := indicator.NewMarket(indicator.DefaultMarketConfig(), marketHistoryLimit)
	if err != nil {
		return err
	}
	var lastBar *model.Bar
	var closedCount uint64
	points := make([]model.SignalEvent, 0, len(bars)*len(l.periods))
	for _, bar := range bars {
		if !bar.Closed {
			continue
		}
		if err := l.validateBar(bar); err != nil {
			return err
		}
		if lastBar != nil && !bar.OpenTime.After(lastBar.OpenTime) {
			if bar.OpenTime.Equal(lastBar.OpenTime) && barFingerprint(bar) == barFingerprint(*lastBar) {
				continue
			}
			return fmt.Errorf("seed contains conflicting or out-of-order bar at %s", bar.OpenTime)
		}
		revision := revisionForBar(bar)
		events, err := reduceEMAClosed(l.key, reducers, bar, revision)
		if err != nil {
			return err
		}
		market.OnClosed(bar)
		points = append(points, events...)
		copy := bar
		lastBar = &copy
		closedCount++
	}
	if lastBar != nil {
		points = append(points, newMarketEvent(l.key, *lastBar, market.State(), revisionForBar(*lastBar)))
	}
	snapshots, err := reducerSnapshots(l.key, reducers, market, lastBar)
	if err != nil {
		return err
	}
	if err := l.store.RebuildSeries(ctx, l.key, points, snapshots, lastBar); err != nil {
		return fmt.Errorf("persist rebuilt series: %w", err)
	}

	l.mu.Lock()
	l.reducers = reducers
	l.market = market
	l.lastBar = lastBar
	if lastBar != nil {
		l.lastClosedRevision = revisionForBar(*lastBar)
	}
	l.closedCount = closedCount
	l.provisional = make(map[int]float64)
	l.formingTime = nil
	l.formingRevision = 0
	l.mu.Unlock()
	return nil
}

func (l *Lane) restoreRecoverySeed(recovery model.SeriesRecovery, bars []model.Bar) error {
	reducers, err := newReducers(l.periods)
	if err != nil {
		return err
	}
	market, err := indicator.NewMarket(indicator.DefaultMarketConfig(), marketHistoryLimit)
	if err != nil {
		return err
	}
	byPeriod := make(map[int]model.ReducerSnapshot, len(l.periods))
	var marketSnapshot *model.ReducerSnapshot
	for _, snapshot := range recovery.Snapshots {
		if snapshot.Period == 0 && snapshot.Algorithm == indicator.MarketAlgorithm(indicator.DefaultMarketConfig()) {
			copy := snapshot
			marketSnapshot = &copy
			continue
		}
		expected, configured := reducers[snapshot.Period]
		if !configured || snapshot.Algorithm != emaAlgorithm(snapshot.Period) {
			continue
		}
		if _, duplicate := byPeriod[snapshot.Period]; duplicate {
			return fmt.Errorf("multiple current snapshots for EMA %d", snapshot.Period)
		}
		if err := expected.Restore(snapshot.State); err != nil {
			return fmt.Errorf("restore EMA %d: %w", snapshot.Period, err)
		}
		byPeriod[snapshot.Period] = snapshot
	}
	if len(byPeriod) != len(l.periods) {
		return fmt.Errorf("durable cursor exists without a complete current reducer snapshot set")
	}
	snapshotTime := byPeriod[l.periods[0]].LastOpenTime
	closedCount := reducers[l.periods[0]].Samples()
	if snapshotTime.After(recovery.CursorOpenTime) {
		return fmt.Errorf("reducer snapshot is newer than the durable series cursor")
	}
	for _, period := range l.periods[1:] {
		if !byPeriod[period].LastOpenTime.Equal(snapshotTime) || reducers[period].Samples() != closedCount {
			return fmt.Errorf("reducer snapshots do not share one series checkpoint")
		}
	}

	var lastBar *model.Bar
	foundSnapshot, foundCursor := false, false
	marketBars := make([]model.Bar, 0, len(bars))
	for _, bar := range bars {
		if bar.Closed && !bar.OpenTime.After(recovery.CursorOpenTime) {
			marketBars = append(marketBars, bar)
		}
		if !bar.Closed || bar.OpenTime.Before(snapshotTime) {
			continue
		}
		if err := l.validateBar(bar); err != nil {
			return err
		}
		if bar.OpenTime.Equal(snapshotTime) {
			copy := bar
			lastBar = &copy
			foundSnapshot = true
		} else if bar.OpenTime.Before(recovery.CursorOpenTime) || bar.OpenTime.Equal(recovery.CursorOpenTime) {
			if !foundSnapshot {
				return fmt.Errorf("OHLC seed does not cover reducer snapshot at %s", snapshotTime)
			}
			if _, err := reduceEMAClosed(l.key, reducers, bar, revisionForBar(bar)); err != nil {
				return err
			}
			copy := bar
			lastBar = &copy
			closedCount++
		}
		if bar.OpenTime.Equal(recovery.CursorOpenTime) {
			if barFingerprint(bar) != recovery.CursorHash {
				return fmt.Errorf("OHLC seed conflicts with durable cursor at %s; correction protocol required", bar.OpenTime)
			}
			foundCursor = true
		}
	}
	if !foundSnapshot || !foundCursor || lastBar == nil {
		return fmt.Errorf("OHLC seed does not cover recovery range %s through %s", snapshotTime, recovery.CursorOpenTime)
	}
	if marketSnapshot == nil {
		market.Seed(marketBars)
	} else {
		if marketSnapshot.LastOpenTime.After(recovery.CursorOpenTime) {
			return fmt.Errorf("market snapshot is newer than the durable series cursor")
		}
		if err := market.Restore(marketSnapshot.State); err != nil {
			return fmt.Errorf("restore market state: %w", err)
		}
		for _, bar := range marketBars {
			if bar.OpenTime.After(marketSnapshot.LastOpenTime) {
				market.OnClosed(bar)
			}
		}
	}

	l.mu.Lock()
	l.reducers = reducers
	l.market = market
	l.lastBar = lastBar
	l.lastClosedRevision = revisionForBar(*lastBar)
	l.closedCount = closedCount
	l.provisional = make(map[int]float64)
	l.formingTime = nil
	l.formingRevision = 0
	l.mu.Unlock()
	return nil
}

func (l *Lane) applyCatchUpSeed(ctx context.Context, bars []model.Bar) error {
	for _, bar := range bars {
		if !bar.Closed {
			continue
		}
		l.mu.RLock()
		lastBar := cloneBar(l.lastBar)
		l.mu.RUnlock()
		if lastBar != nil && !bar.OpenTime.After(lastBar.OpenTime) {
			if bar.OpenTime.Equal(lastBar.OpenTime) && barFingerprint(bar) != barFingerprint(*lastBar) {
				return fmt.Errorf("seed conflicts with current bar at %s; correction protocol required", bar.OpenTime)
			}
			continue
		}
		if err := l.applyClosed(ctx, bar); err != nil {
			return err
		}
	}
	return nil
}

func (l *Lane) applyClosed(ctx context.Context, bar model.Bar) error {
	if !bar.Closed {
		return fmt.Errorf("closed input has closed=false")
	}
	if err := l.validateBar(bar); err != nil {
		return err
	}

	l.mu.RLock()
	lastBar := cloneBar(l.lastBar)
	reducers, err := cloneReducers(l.periods, l.reducers)
	market, marketErr := l.market.Clone()
	closedCount := l.closedCount
	l.mu.RUnlock()
	if err != nil {
		return err
	}
	if marketErr != nil {
		return marketErr
	}
	if lastBar != nil && !bar.OpenTime.After(lastBar.OpenTime) {
		if bar.OpenTime.Equal(lastBar.OpenTime) && barFingerprint(bar) == barFingerprint(*lastBar) {
			return nil
		}
		return fmt.Errorf("closed bar %s is older than current cursor %s; reseed required", bar.OpenTime, lastBar.OpenTime)
	}

	revision := revisionForBar(bar)
	events, err := reduceEMAClosed(l.key, reducers, bar, revision)
	if err != nil {
		return err
	}
	marketState := market.OnClosed(bar)
	events = append(events, newMarketEvent(l.key, bar, marketState, revision))
	closedCount++
	var snapshots []model.ReducerSnapshot
	if closedCount%l.snapshotEvery == 0 {
		snapshots, err = reducerSnapshots(l.key, reducers, market, &bar)
		if err != nil {
			return err
		}
	}
	committed, err := l.store.CommitClosed(ctx, l.key, bar, events, snapshots)
	if err != nil {
		return fmt.Errorf("commit closed bar: %w", err)
	}

	l.mu.Lock()
	l.reducers = reducers
	l.market = market
	l.lastBar = cloneBar(&bar)
	l.lastClosedRevision = revision
	l.closedCount = closedCount
	l.provisional = make(map[int]float64)
	l.formingTime = nil
	l.formingRevision = 0
	l.mu.Unlock()
	if l.publisher != nil && !l.outboxDelivery {
		cursors := make([]int64, 0, len(committed))
		for _, event := range committed {
			l.publisher.Publish(event)
			cursors = append(cursors, event.Cursor)
		}
		if err := l.store.MarkPublished(ctx, cursors); err != nil {
			return fmt.Errorf("mark signal outbox published: %w", err)
		}
	}
	return nil
}

func (l *Lane) applyForming(bar model.Bar) error {
	if bar.Closed {
		return fmt.Errorf("forming input has closed=true")
	}
	if err := l.validateBar(bar); err != nil {
		return err
	}
	l.mu.RLock()
	reducers, err := cloneReducers(l.periods, l.reducers)
	lastBar := cloneBar(l.lastBar)
	l.mu.RUnlock()
	if err != nil {
		return err
	}
	if lastBar != nil && bar.OpenTime.Before(lastBar.OpenTime) {
		return nil
	}
	revision := revisionForBar(bar)

	provisional := make(map[int]float64)
	for _, period := range l.periods {
		value, ready, err := reducers[period].Project(bar.Close)
		if err != nil {
			return err
		}
		if !ready {
			continue
		}
		provisional[period] = value
		if l.publisher != nil {
			l.publisher.Publish(newEMAEvent(l.key, bar, period, reducers[period].Samples()+1, value, true, revision))
		}
	}
	l.mu.Lock()
	l.provisional = provisional
	formingTimestamp := bar.OpenTime.UTC()
	l.formingTime = &formingTimestamp
	l.formingRevision = revision
	l.mu.Unlock()
	return nil
}

func revisionForBar(bar model.Bar) uint64 {
	if bar.Closed && bar.Trades < ^uint64(0) {
		return bar.Trades + 1
	}
	return bar.Trades
}

func (l *Lane) validateBar(bar model.Bar) error {
	if bar.Symbol != l.key.Symbol || bar.Timeframe != l.key.Timeframe {
		return fmt.Errorf("bar %s|%s does not match lane %s", bar.Symbol, bar.Timeframe, l.key.String())
	}
	if bar.OpenTime.IsZero() {
		return fmt.Errorf("bar open time is required")
	}
	for name, value := range map[string]float64{
		"open": bar.Open, "high": bar.High, "low": bar.Low, "close": bar.Close, "volume": bar.Volume,
	} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("bar %s must be finite", name)
		}
	}
	if bar.High < bar.Low || bar.High < bar.Open || bar.High < bar.Close || bar.Low > bar.Open || bar.Low > bar.Close || bar.Volume < 0 {
		return fmt.Errorf("bar OHLCV is inconsistent")
	}
	return nil
}

func newReducers(periods []int) (map[int]*indicator.EMA, error) {
	reducers := make(map[int]*indicator.EMA, len(periods))
	for _, period := range periods {
		reducer, err := indicator.NewEMA(period)
		if err != nil {
			return nil, err
		}
		reducers[period] = reducer
	}
	return reducers, nil
}

func cloneReducers(periods []int, source map[int]*indicator.EMA) (map[int]*indicator.EMA, error) {
	cloned, err := newReducers(periods)
	if err != nil {
		return nil, err
	}
	for _, period := range periods {
		state, err := source[period].Snapshot()
		if err != nil {
			return nil, err
		}
		if err := cloned[period].Restore(state); err != nil {
			return nil, err
		}
	}
	return cloned, nil
}

func reduceEMAClosed(key model.SeriesKey, reducers map[int]*indicator.EMA, bar model.Bar, revision uint64) ([]model.SignalEvent, error) {
	periods := make([]int, 0, len(reducers))
	for period := range reducers {
		periods = append(periods, period)
	}
	sort.Ints(periods)
	events := make([]model.SignalEvent, 0, len(periods))
	for _, period := range periods {
		value, ready, err := reducers[period].OnClosed(bar.Close)
		if err != nil {
			return nil, err
		}
		if ready {
			events = append(events, newEMAEvent(key, bar, period, reducers[period].Samples(), value, false, revision))
		}
	}
	return events, nil
}

func newEMAEvent(key model.SeriesKey, bar model.Bar, period int, samples uint64, value float64, provisional bool, revision uint64) model.SignalEvent {
	algorithm := emaAlgorithm(period)
	identity := key.String() + "|" + bar.OpenTime.UTC().Format(time.RFC3339Nano) + "|ema|" +
		algorithm.Version + "|" + algorithm.ConfigHash + "|" + strconv.Itoa(period) + "|" + strconv.FormatBool(provisional) + "|" + strconv.FormatUint(revision, 10)
	sum := sha256.Sum256([]byte(identity))
	return model.SignalEvent{
		Schema:           signalSchema,
		EventID:          hex.EncodeToString(sum[:]),
		EventType:        "indicator.point",
		Dataset:          key.Dataset,
		Symbol:           key.Symbol,
		Timeframe:        key.Timeframe,
		BarOpenTime:      bar.OpenTime.UTC(),
		BarRevision:      revision,
		AnalysisRevision: revision,
		Provisional:      provisional,
		Algorithm:        algorithm,
		Indicator: &model.IndicatorPayload{
			Period:  period,
			Value:   value,
			Samples: samples,
			Ready:   true,
		},
	}
}

func newMarketEvent(key model.SeriesKey, bar model.Bar, state model.MarketState, revision uint64) model.SignalEvent {
	algorithm := state.Algorithm
	identity := key.String() + "|" + bar.OpenTime.UTC().Format(time.RFC3339Nano) + "|" +
		algorithm.Name + "|" + algorithm.Version + "|" + algorithm.ConfigHash + "|false|" + strconv.FormatUint(revision, 10)
	sum := sha256.Sum256([]byte(identity))
	stateCopy := state
	return model.SignalEvent{
		Schema: signalSchema, EventID: hex.EncodeToString(sum[:]), EventType: "market.state",
		Dataset: key.Dataset, Symbol: key.Symbol, Timeframe: key.Timeframe,
		BarOpenTime: bar.OpenTime.UTC(), BarRevision: revision, AnalysisRevision: revision,
		Algorithm: algorithm, Market: &stateCopy,
	}
}

func emaAlgorithm(period int) model.AlgorithmRef {
	config := "ema|" + indicator.EMAAlgorithmVersion + "|" + strconv.Itoa(period) + "|sma-seed"
	sum := sha256.Sum256([]byte(config))
	return model.AlgorithmRef{Name: "ema", Version: indicator.EMAAlgorithmVersion, ConfigHash: "sha256:" + hex.EncodeToString(sum[:])}
}

func reducerSnapshots(key model.SeriesKey, reducers map[int]*indicator.EMA, market *indicator.Market, lastBar *model.Bar) ([]model.ReducerSnapshot, error) {
	if lastBar == nil {
		return nil, nil
	}
	periods := make([]int, 0, len(reducers))
	for period := range reducers {
		periods = append(periods, period)
	}
	sort.Ints(periods)
	out := make([]model.ReducerSnapshot, 0, len(periods))
	for _, period := range periods {
		state, err := reducers[period].Snapshot()
		if err != nil {
			return nil, err
		}
		out = append(out, model.ReducerSnapshot{Algorithm: emaAlgorithm(period), Period: period, LastOpenTime: lastBar.OpenTime.UTC(), State: state})
	}
	marketState, err := market.Snapshot()
	if err != nil {
		return nil, err
	}
	out = append(out, model.ReducerSnapshot{
		Algorithm: indicator.MarketAlgorithm(indicator.DefaultMarketConfig()), Period: 0,
		LastOpenTime: lastBar.OpenTime.UTC(), State: marketState,
	})
	return out, nil
}

func makeSeriesSnapshot(key model.SeriesKey, periods []int, reducers map[int]*indicator.EMA, market *indicator.Market, lastBar *model.Bar, lastClosedRevision uint64, formingTime *time.Time, formingRevision uint64, provisional map[int]float64) model.SeriesSnapshot {
	snapshot := model.SeriesSnapshot{Schema: "signal.snapshot.v1", Series: key, Ready: true,
		LastClosedRevision: lastClosedRevision, FormingRevision: formingRevision}
	if lastBar != nil {
		timestamp := lastBar.OpenTime.UTC()
		snapshot.LastClosedTime = &timestamp
	}
	if formingTime != nil {
		timestamp := formingTime.UTC()
		snapshot.FormingOpenTime = &timestamp
	}
	for _, period := range periods {
		reducer := reducers[period]
		value, ready := reducer.Value()
		state := model.IndicatorState{Algorithm: emaAlgorithm(period), Period: period, Samples: reducer.Samples(),
			Ready: ready, AnalysisRevision: lastClosedRevision}
		if ready {
			valueCopy := value
			state.Value = &valueCopy
		} else {
			snapshot.Ready = false
		}
		snapshot.Indicators = append(snapshot.Indicators, state)
		if projected, ok := provisional[period]; ok {
			projectedCopy := projected
			snapshot.Provisional = append(snapshot.Provisional, model.IndicatorState{
				Algorithm: emaAlgorithm(period), Period: period, Value: &projectedCopy, Samples: reducer.Samples() + 1, Ready: true,
				AnalysisRevision: formingRevision,
			})
		}
	}
	marketState := market.State()
	snapshot.Market = &marketState
	return snapshot
}

func cloneBar(source *model.Bar) *model.Bar {
	if source == nil {
		return nil
	}
	copy := *source
	return &copy
}

func barFingerprint(bar model.Bar) string {
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
