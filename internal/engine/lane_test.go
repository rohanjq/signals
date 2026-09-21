package engine

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/rohanjq/signals/internal/model"
	signalstore "github.com/rohanjq/signals/internal/store"
)

type memoryStore struct {
	mu         sync.Mutex
	rebuilds   int
	commits    int
	failCommit bool
	nextCursor int64
	seedEvents []model.SignalEvent
	liveEvents []model.SignalEvent
	snapshots  []model.ReducerSnapshot
}

func (s *memoryStore) LoadRecovery(context.Context, model.SeriesKey) (model.SeriesRecovery, error) {
	return model.SeriesRecovery{}, nil
}

func (s *memoryStore) RebuildSeries(_ context.Context, _ model.SeriesKey, events []model.SignalEvent, snapshots []model.ReducerSnapshot, _ *model.Bar) ([]model.SignalEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rebuilds++
	s.seedEvents = append([]model.SignalEvent(nil), events...)
	s.snapshots = append([]model.ReducerSnapshot(nil), snapshots...)
	committed := make([]model.SignalEvent, 0, 1)
	for _, event := range events {
		if event.Reset {
			committed = append(committed, event)
		}
	}
	return committed, nil
}

func (s *memoryStore) CommitClosed(_ context.Context, _ model.SeriesKey, _ model.Bar, events []model.SignalEvent, snapshots []model.ReducerSnapshot) ([]model.SignalEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failCommit {
		return nil, errors.New("storage unavailable")
	}
	s.commits++
	committed := append([]model.SignalEvent(nil), events...)
	for index := range committed {
		s.nextCursor++
		committed[index].Cursor = s.nextCursor
	}
	s.liveEvents = append(s.liveEvents, committed...)
	s.snapshots = append(s.snapshots, snapshots...)
	return committed, nil
}

func (s *memoryStore) MarkPublished(context.Context, []int64) error { return nil }

type capturePublisher struct {
	mu     sync.Mutex
	events []model.SignalEvent
}

func (p *capturePublisher) Publish(event model.SignalEvent) {
	p.mu.Lock()
	p.events = append(p.events, event)
	p.mu.Unlock()
}

func TestLaneLiveReplayParityAndProjection(t *testing.T) {
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	bars := testBars(key, 7)

	replayStore := &memoryStore{}
	replay := mustLane(t, key, []int{3, 5}, replayStore, nil)
	liveStore := &memoryStore{}
	publisher := &capturePublisher{}
	live := mustLane(t, key, []int{3, 5}, liveStore, publisher)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go replay.Run(ctx)
	go live.Run(ctx)

	if err := replay.Seed(ctx, bars); err != nil {
		t.Fatal(err)
	}
	if err := live.Seed(ctx, bars[:5]); err != nil {
		t.Fatal(err)
	}
	forming := bars[5]
	forming.Closed = false
	if err := live.Forming(ctx, forming); err != nil {
		t.Fatal(err)
	}
	before := live.Snapshot()
	if len(before.Provisional) != 2 || before.LastClosedTime == nil || !before.LastClosedTime.Equal(bars[4].OpenTime) {
		t.Fatalf("unexpected provisional snapshot: %+v", before)
	}
	for _, bar := range bars[5:] {
		if err := live.Closed(ctx, bar); err != nil {
			t.Fatal(err)
		}
	}

	assertSnapshotEqual(t, replay.Snapshot(), live.Snapshot())
	if liveStore.commits != 2 {
		t.Fatalf("live commits = %d, want 2", liveStore.commits)
	}
	if len(publisher.events) != 8 {
		t.Fatalf("published events = %d, want 8 (2 provisional + 4 EMA + 2 market)", len(publisher.events))
	}
	marketEvents := 0
	for _, event := range publisher.events {
		if event.EventType == "market.state" && event.Market != nil {
			marketEvents++
		}
	}
	if marketEvents != 2 {
		t.Fatalf("market events = %d, want 2", marketEvents)
	}
}

func TestLanePublishesOrderedCompleteLiveFrames(t *testing.T) {
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	store := &memoryStore{}
	live := &capturePublisher{}
	lane, err := NewLane(LaneOptions{Key: key, Periods: []int{2}, MailboxSize: 8, SnapshotEvery: 2,
		Store: store, LivePublisher: live, OutboxDelivery: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lane.Run(ctx)
	bars := testBars(key, 4)
	if err := lane.Seed(ctx, bars[:3]); err != nil {
		t.Fatal(err)
	}
	forming := bars[3]
	forming.Closed = false
	if err := lane.Forming(ctx, forming); err != nil {
		t.Fatal(err)
	}
	if err := lane.Closed(ctx, bars[3]); err != nil {
		t.Fatal(err)
	}
	if len(live.events) != 2 {
		t.Fatalf("live events=%d, want provisional and confirmed", len(live.events))
	}
	if !live.events[0].Provisional || live.events[0].EventType != "market.state" || live.events[0].SourceBar == nil || live.events[0].Market == nil {
		t.Fatalf("first live event is not a complete provisional frame: %+v", live.events[0])
	}
	if live.events[1].Provisional || live.events[1].EventType != "market.state" || live.events[1].SourceBar == nil {
		t.Fatalf("second live event is not a confirmed frame: %+v", live.events[1])
	}
}

func TestLaneDuplicateIsIdempotent(t *testing.T) {
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	store := &memoryStore{}
	lane := mustLane(t, key, []int{2}, store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lane.Run(ctx)
	bars := testBars(key, 3)
	if err := lane.Seed(ctx, bars[:2]); err != nil {
		t.Fatal(err)
	}
	if err := lane.Closed(ctx, bars[2]); err != nil {
		t.Fatal(err)
	}
	if err := lane.Closed(ctx, bars[2]); err != nil {
		t.Fatal(err)
	}
	if store.commits != 1 {
		t.Fatalf("commits = %d, want 1", store.commits)
	}
}

func TestLaneRevisionsAdvanceAcrossFormingAndConfirmed(t *testing.T) {
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	store := &memoryStore{}
	publisher := &capturePublisher{}
	lane := mustLane(t, key, []int{2}, store, publisher)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lane.Run(ctx)
	bars := testBars(key, 3)
	if err := lane.Seed(ctx, bars[:2]); err != nil {
		t.Fatal(err)
	}
	forming := bars[2]
	forming.Closed = false
	if err := lane.Forming(ctx, forming); err != nil {
		t.Fatal(err)
	}
	forming.Close += 0.25
	forming.High += 0.25
	forming.Trades++
	if err := lane.Forming(ctx, forming); err != nil {
		t.Fatal(err)
	}
	closed := forming
	closed.Closed = true
	if err := lane.Closed(ctx, closed); err != nil {
		t.Fatal(err)
	}
	if len(publisher.events) != 4 {
		t.Fatalf("events=%d, want two provisional plus EMA and market confirmations", len(publisher.events))
	}
	want := []uint64{1, 2, 3, 3}
	for index, event := range publisher.events {
		if event.BarRevision != want[index] || event.AnalysisRevision != want[index] {
			t.Fatalf("event %d revisions=(%d,%d), want %d", index, event.BarRevision, event.AnalysisRevision, want[index])
		}
	}
	snapshot := lane.Snapshot()
	if snapshot.LastClosedRevision != 3 || snapshot.FormingOpenTime != nil {
		t.Fatalf("unexpected confirmed snapshot revision: %+v", snapshot)
	}
}

func TestLaneFormingRevisionSurvivesRestart(t *testing.T) {
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	persistence := signalstore.NewMemory()
	bars := testBars(key, 3)

	firstPublisher := &capturePublisher{}
	first := mustLane(t, key, []int{2}, persistence, firstPublisher)
	firstCtx, stopFirst := context.WithCancel(context.Background())
	go first.Run(firstCtx)
	if err := first.Seed(firstCtx, bars[:2]); err != nil {
		t.Fatal(err)
	}
	forming := bars[2]
	forming.Closed = false
	forming.Trades = 7
	if err := first.Forming(firstCtx, forming); err != nil {
		t.Fatal(err)
	}
	stopFirst()

	restartedPublisher := &capturePublisher{}
	restarted := mustLane(t, key, []int{2}, persistence, restartedPublisher)
	restartCtx, stopRestart := context.WithCancel(context.Background())
	defer stopRestart()
	go restarted.Run(restartCtx)
	if err := restarted.Seed(restartCtx, bars[:2]); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Forming(restartCtx, forming); err != nil {
		t.Fatal(err)
	}
	forming.Trades++
	if err := restarted.Forming(restartCtx, forming); err != nil {
		t.Fatal(err)
	}

	if len(firstPublisher.events) != 1 || firstPublisher.events[0].AnalysisRevision != 7 {
		t.Fatalf("first revision events=%+v", firstPublisher.events)
	}
	if len(restartedPublisher.events) != 2 || restartedPublisher.events[0].AnalysisRevision != 7 || restartedPublisher.events[1].AnalysisRevision != 8 {
		t.Fatalf("restarted revision events=%+v", restartedPublisher.events)
	}
}

func TestLaneReconnectSeedCatchesUpWithoutRebuildOrDuplicate(t *testing.T) {
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	store := &memoryStore{}
	publisher := &capturePublisher{}
	lane := mustLane(t, key, []int{2}, store, publisher)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lane.Run(ctx)
	bars := testBars(key, 6)
	if err := lane.Seed(ctx, bars[:3]); err != nil {
		t.Fatal(err)
	}
	if err := lane.Closed(ctx, bars[3]); err != nil {
		t.Fatal(err)
	}
	if err := lane.Seed(ctx, bars); err != nil {
		t.Fatal(err)
	}
	if store.rebuilds != 1 || store.commits != 3 {
		t.Fatalf("rebuilds=%d commits=%d, want 1 and 3", store.rebuilds, store.commits)
	}
	if len(publisher.events) != 6 {
		t.Fatalf("published events=%d, want 6", len(publisher.events))
	}
	snapshot := lane.Snapshot()
	if snapshot.LastClosedTime == nil || !snapshot.LastClosedTime.Equal(bars[5].OpenTime) {
		t.Fatalf("last closed=%v, want %v", snapshot.LastClosedTime, bars[5].OpenTime)
	}
}

func TestLaneRejectsGapThenCatchesUpFromAuthoritativeSeed(t *testing.T) {
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	store := &memoryStore{}
	publisher := &capturePublisher{}
	lane := mustLane(t, key, []int{2}, store, publisher)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lane.Run(ctx)
	bars := testBars(key, 4)
	if err := lane.Seed(ctx, bars[:2]); err != nil {
		t.Fatal(err)
	}
	if err := lane.Closed(ctx, bars[3]); err == nil {
		t.Fatal("expected a non-contiguous closed bar to be rejected")
	}
	if err := lane.Seed(ctx, bars); err != nil {
		t.Fatal(err)
	}
	if store.rebuilds != 1 || store.commits != 2 {
		t.Fatalf("rebuilds=%d commits=%d, want 1 and 2", store.rebuilds, store.commits)
	}
	if len(publisher.events) != 4 {
		t.Fatalf("published events=%d, want 4 from two repaired bars", len(publisher.events))
	}
	snapshot := lane.Snapshot()
	if snapshot.LastClosedTime == nil || !snapshot.LastClosedTime.Equal(bars[3].OpenTime) {
		t.Fatalf("last closed=%v, want %v", snapshot.LastClosedTime, bars[3].OpenTime)
	}
}

func TestLaneRebuildsWhenAuthoritativeSeedInsertsBarBehindCursor(t *testing.T) {
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	store := &memoryStore{}
	lane := mustLane(t, key, []int{2}, store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lane.Run(ctx)
	bars := testBars(key, 5)
	if err := lane.Seed(ctx, append(append([]model.Bar(nil), bars[:2]...), bars[3:]...)); err != nil {
		t.Fatal(err)
	}
	if err := lane.Seed(ctx, bars); err != nil {
		t.Fatal(err)
	}
	if store.rebuilds != 2 {
		t.Fatalf("rebuilds=%d, want 2 after repaired history", store.rebuilds)
	}
}

func TestLanePublishesAuthoritativeResetAfterCorrection(t *testing.T) {
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	store := &memoryStore{}
	publisher := &capturePublisher{}
	lane, err := NewLane(LaneOptions{Key: key, Periods: []int{2}, MailboxSize: 8, SnapshotEvery: 2, Store: store, LivePublisher: publisher, OutboxDelivery: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lane.Run(ctx)
	bars := testBars(key, 5)
	if err := lane.Seed(ctx, bars); err != nil {
		t.Fatal(err)
	}
	corrected := append([]model.Bar(nil), bars...)
	corrected[2].Close++
	corrected[2].High++
	if err := lane.Seed(ctx, corrected); err != nil {
		t.Fatal(err)
	}
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.events) != 1 || !publisher.events[0].Reset {
		t.Fatalf("reset events=%+v", publisher.events)
	}
	if got := len(publisher.events[0].ResetHistory); got != 4 {
		t.Fatalf("reset history=%d want=4", got)
	}
}

func TestLaneRestoresSnapshotAndReplaysOnlyThroughDurableCursor(t *testing.T) {
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	bars := testBars(key, 8)
	persistence := signalstore.NewMemory()

	first := mustLane(t, key, []int{2}, persistence, nil)
	firstCtx, stopFirst := context.WithCancel(context.Background())
	go first.Run(firstCtx)
	if err := first.Seed(firstCtx, bars[:4]); err != nil {
		t.Fatal(err)
	}
	for _, bar := range bars[4:6] {
		if err := first.Closed(firstCtx, bar); err != nil {
			t.Fatal(err)
		}
	}
	stopFirst()

	restarted := mustLane(t, key, []int{2}, persistence, nil)
	restartCtx, stopRestart := context.WithCancel(context.Background())
	defer stopRestart()
	go restarted.Run(restartCtx)
	if err := restarted.Seed(restartCtx, bars); err != nil {
		t.Fatal(err)
	}

	expectedStore := &memoryStore{}
	expected := mustLane(t, key, []int{2}, expectedStore, nil)
	expectedCtx, stopExpected := context.WithCancel(context.Background())
	defer stopExpected()
	go expected.Run(expectedCtx)
	if err := expected.Seed(expectedCtx, bars); err != nil {
		t.Fatal(err)
	}
	assertSnapshotEqual(t, expected.Snapshot(), restarted.Snapshot())

	events, err := persistence.QueryEvents(context.Background(), signalstore.EventFilter{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 8 {
		t.Fatalf("durable live events=%d, want 8", len(events))
	}
}

func TestLaneRebuildsFromAuthoritativeSeedWhenCursorBarWasCorrected(t *testing.T) {
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	bars := testBars(key, 8)
	persistence := signalstore.NewMemory()

	first := mustLane(t, key, []int{2}, persistence, nil)
	firstCtx, stopFirst := context.WithCancel(context.Background())
	go first.Run(firstCtx)
	if err := first.Seed(firstCtx, bars[:6]); err != nil {
		t.Fatal(err)
	}
	stopFirst()

	corrected := append([]model.Bar(nil), bars...)
	corrected[5].Close += 2
	corrected[5].High += 2
	restarted := mustLane(t, key, []int{2}, persistence, nil)
	restartCtx, stopRestart := context.WithCancel(context.Background())
	defer stopRestart()
	go restarted.Run(restartCtx)
	if err := restarted.Seed(restartCtx, corrected); err != nil {
		t.Fatal(err)
	}

	expectedStore := signalstore.NewMemory()
	expected := mustLane(t, key, []int{2}, expectedStore, nil)
	expectedCtx, stopExpected := context.WithCancel(context.Background())
	defer stopExpected()
	go expected.Run(expectedCtx)
	if err := expected.Seed(expectedCtx, corrected); err != nil {
		t.Fatal(err)
	}
	assertSnapshotEqual(t, expected.Snapshot(), restarted.Snapshot())
}

func TestInitializedLaneRebuildsWhenReconnectSeedCorrectsCurrentBar(t *testing.T) {
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	bars := testBars(key, 8)
	persistence := signalstore.NewMemory()
	lane := mustLane(t, key, []int{2}, persistence, nil)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go lane.Run(ctx)
	if err := lane.Seed(ctx, bars); err != nil {
		t.Fatal(err)
	}

	corrected := append([]model.Bar(nil), bars...)
	corrected[len(corrected)-1].Close += 2
	corrected[len(corrected)-1].High += 2
	if err := lane.Seed(ctx, corrected); err != nil {
		t.Fatal(err)
	}

	expectedStore := signalstore.NewMemory()
	expected := mustLane(t, key, []int{2}, expectedStore, nil)
	expectedCtx, stopExpected := context.WithCancel(context.Background())
	defer stopExpected()
	go expected.Run(expectedCtx)
	if err := expected.Seed(expectedCtx, corrected); err != nil {
		t.Fatal(err)
	}
	assertSnapshotEqual(t, expected.Snapshot(), lane.Snapshot())
}

func TestLaneDoesNotAdvanceOnStoreFailure(t *testing.T) {
	key := model.SeriesKey{Dataset: "test", Symbol: "BTCUSDT", Timeframe: "1m"}
	store := &memoryStore{failCommit: true}
	lane := mustLane(t, key, []int{2}, store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lane.Run(ctx)
	bars := testBars(key, 3)
	if err := lane.Seed(ctx, bars[:2]); err != nil {
		t.Fatal(err)
	}
	want := lane.Snapshot()
	if err := lane.Closed(ctx, bars[2]); err == nil {
		t.Fatal("expected storage failure")
	}
	assertSnapshotEqual(t, want, lane.Snapshot())
}

func mustLane(t *testing.T, key model.SeriesKey, periods []int, store Store, publisher Publisher) *Lane {
	t.Helper()
	lane, err := NewLane(LaneOptions{Key: key, Periods: periods, MailboxSize: 8, SnapshotEvery: 2, Store: store, Publisher: publisher})
	if err != nil {
		t.Fatal(err)
	}
	return lane
}

func testBars(key model.SeriesKey, count int) []model.Bar {
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	bars := make([]model.Bar, count)
	for index := range bars {
		close := float64(10 + index)
		bars[index] = model.Bar{
			Symbol: key.Symbol, Timeframe: key.Timeframe, OpenTime: start.Add(time.Duration(index) * time.Minute),
			Open: close, High: close + 1, Low: close - 1, Close: close, Volume: 1, Trades: 1, Closed: true,
		}
	}
	return bars
}

func assertSnapshotEqual(t *testing.T, want, got model.SeriesSnapshot) {
	t.Helper()
	if want.Ready != got.Ready || len(want.Indicators) != len(got.Indicators) {
		t.Fatalf("snapshots differ: want=%+v got=%+v", want, got)
	}
	if want.LastClosedRevision != got.LastClosedRevision || want.FormingRevision != got.FormingRevision {
		t.Fatalf("snapshot revisions differ: want=(%d,%d) got=(%d,%d)",
			want.LastClosedRevision, want.FormingRevision, got.LastClosedRevision, got.FormingRevision)
	}
	if (want.LastClosedTime == nil) != (got.LastClosedTime == nil) || (want.LastClosedTime != nil && !want.LastClosedTime.Equal(*got.LastClosedTime)) {
		t.Fatalf("last closed differs: want=%v got=%v", want.LastClosedTime, got.LastClosedTime)
	}
	if !reflect.DeepEqual(want.Market, got.Market) {
		t.Fatalf("market state differs: want=%+v got=%+v", want.Market, got.Market)
	}
	for index := range want.Indicators {
		left, right := want.Indicators[index], got.Indicators[index]
		if left.Period != right.Period || left.Samples != right.Samples || left.Ready != right.Ready {
			t.Fatalf("indicator %d metadata differs: want=%+v got=%+v", index, left, right)
		}
		if left.Value != nil && (right.Value == nil || math.Abs(*left.Value-*right.Value) > 1e-12) {
			t.Fatalf("indicator %d value differs: want=%v got=%v", index, left.Value, right.Value)
		}
	}
}
