package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/rohanjq/signals/internal/model"
)

type Memory struct {
	mu        sync.RWMutex
	next      int64
	events    []model.SignalEvent
	points    map[string]model.SignalEvent
	markets   map[string]model.SignalEvent
	cursors   map[string]model.Bar
	snapshots map[string]model.ReducerSnapshot
	published map[int64]bool
}

func NewMemory() *Memory {
	return &Memory{
		points: make(map[string]model.SignalEvent), cursors: make(map[string]model.Bar),
		markets: make(map[string]model.SignalEvent), snapshots: make(map[string]model.ReducerSnapshot), published: make(map[int64]bool),
	}
}

func (s *Memory) Ping(context.Context) error { return nil }

func (s *Memory) Close() {}

func (s *Memory) LoadRecovery(_ context.Context, key model.SeriesKey) (model.SeriesRecovery, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	recovery := model.SeriesRecovery{}
	if cursor, ok := s.cursors[key.String()]; ok {
		recovery.HasCursor = true
		recovery.CursorOpenTime = cursor.OpenTime
		recovery.CursorHash = barHash(cursor)
	}
	prefix := key.String() + "|"
	for storageKey, snapshot := range s.snapshots {
		if strings.HasPrefix(storageKey, prefix) {
			snapshot.State = append([]byte(nil), snapshot.State...)
			recovery.Snapshots = append(recovery.Snapshots, snapshot)
		}
	}
	return recovery, nil
}

func (s *Memory) RebuildSeries(_ context.Context, key model.SeriesKey, events []model.SignalEvent, snapshots []model.ReducerSnapshot, lastBar *model.Bar) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lastBar != nil {
		if current, ok := s.cursors[key.String()]; ok && current.OpenTime.After(lastBar.OpenTime) {
			return fmt.Errorf("rebuilt series cursor would move backwards")
		}
	}
	for storageKey := range s.snapshots {
		if strings.HasPrefix(storageKey, key.String()+"|") {
			delete(s.snapshots, storageKey)
		}
	}
	for eventID, event := range s.points {
		if event.Dataset == key.Dataset && event.Symbol == key.Symbol && event.Timeframe == key.Timeframe {
			delete(s.points, eventID)
		}
	}
	for _, event := range events {
		s.persistEventState(event)
	}
	for _, snapshot := range snapshots {
		s.snapshots[snapshotKey(key, snapshot)] = snapshot
	}
	if lastBar != nil {
		s.cursors[key.String()] = *lastBar
	}
	return nil
}

func (s *Memory) CommitClosed(_ context.Context, key model.SeriesKey, bar model.Bar, events []model.SignalEvent, snapshots []model.ReducerSnapshot) ([]model.SignalEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.cursors[key.String()]; ok && !bar.OpenTime.After(current.OpenTime) {
		if bar.OpenTime.Equal(current.OpenTime) && barHash(bar) == barHash(current) {
			return nil, nil
		}
		return nil, fmt.Errorf("bar conflicts with memory cursor")
	}
	committed := make([]model.SignalEvent, 0, len(events))
	for _, event := range events {
		if existing, ok := s.eventByID(event.EventID); ok && existing.Cursor > 0 {
			committed = append(committed, existing)
			continue
		}
		s.next++
		event.Cursor = s.next
		s.persistEventState(event)
		s.events = append(s.events, event)
		committed = append(committed, event)
	}
	for _, snapshot := range snapshots {
		s.snapshots[snapshotKey(key, snapshot)] = snapshot
	}
	s.cursors[key.String()] = bar
	return committed, nil
}

func (s *Memory) QueryEvents(_ context.Context, filter EventFilter) ([]model.SignalEvent, error) {
	filter.normalize()
	s.mu.RLock()
	defer s.mu.RUnlock()
	events := make([]model.SignalEvent, 0, filter.Limit)
	for _, event := range s.events {
		if event.Cursor <= filter.AfterCursor || (filter.Dataset != "" && event.Dataset != filter.Dataset) ||
			(filter.Symbol != "" && event.Symbol != filter.Symbol) || (filter.Timeframe != "" && event.Timeframe != filter.Timeframe) ||
			(filter.Period != 0 && eventPeriod(event) != filter.Period) ||
			(filter.EventType != "" && event.EventType != filter.EventType) ||
			(filter.Analysis != "" && event.Algorithm.Name != filter.Analysis) {
			continue
		}
		events = append(events, event)
		if len(events) == filter.Limit {
			break
		}
	}
	return events, nil
}

func (s *Memory) QueryRecentPoints(_ context.Context, filter PointFilter) ([]model.SignalEvent, error) {
	filter.normalize()
	s.mu.RLock()
	defer s.mu.RUnlock()
	events := make([]model.SignalEvent, 0)
	for _, event := range s.points {
		if (filter.Dataset != "" && event.Dataset != filter.Dataset) ||
			(filter.Symbol != "" && event.Symbol != filter.Symbol) ||
			(filter.Timeframe != "" && event.Timeframe != filter.Timeframe) ||
			(filter.Period != 0 && eventPeriod(event) != filter.Period) ||
			(filter.Algorithm.Name != "" && event.Algorithm.Name != filter.Algorithm.Name) ||
			(filter.Algorithm.Version != "" && event.Algorithm.Version != filter.Algorithm.Version) ||
			(filter.Algorithm.ConfigHash != "" && event.Algorithm.ConfigHash != filter.Algorithm.ConfigHash) {
			continue
		}
		event.Cursor = 0
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].BarOpenTime.Equal(events[j].BarOpenTime) {
			return events[i].EventID < events[j].EventID
		}
		return events[i].BarOpenTime.Before(events[j].BarOpenTime)
	})
	if len(events) > filter.Limit {
		events = events[len(events)-filter.Limit:]
	}
	return events, nil
}

func (s *Memory) PendingEvents(_ context.Context, limit int) ([]model.SignalEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	events := make([]model.SignalEvent, 0, limit)
	for _, event := range s.events {
		if !s.published[event.Cursor] {
			events = append(events, event)
			if len(events) == limit {
				break
			}
		}
	}
	return events, nil
}

func (s *Memory) MarkPublished(_ context.Context, cursors []int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cursor := range cursors {
		s.published[cursor] = true
	}
	return nil
}

func snapshotKey(key model.SeriesKey, snapshot model.ReducerSnapshot) string {
	return key.String() + "|" + snapshot.Algorithm.Name + "|" + snapshot.Algorithm.Version + "|" + snapshot.Algorithm.ConfigHash + fmt.Sprintf("|%d", snapshot.Period)
}

func (s *Memory) persistEventState(event model.SignalEvent) {
	if event.EventType == "market.state" {
		s.markets[marketKey(event)] = event
		return
	}
	for eventID, existing := range s.points {
		if existing.Dataset == event.Dataset && existing.Symbol == event.Symbol &&
			existing.Timeframe == event.Timeframe && existing.BarOpenTime.Equal(event.BarOpenTime) &&
			existing.BarRevision == event.BarRevision && eventPeriod(existing) == eventPeriod(event) &&
			existing.Algorithm.Name == event.Algorithm.Name {
			delete(s.points, eventID)
		}
	}
	s.points[event.EventID] = event
}

func (s *Memory) eventByID(eventID string) (model.SignalEvent, bool) {
	if event, ok := s.points[eventID]; ok {
		return event, true
	}
	for _, event := range s.events {
		if event.EventID == eventID {
			return event, true
		}
	}
	return model.SignalEvent{}, false
}

func marketKey(event model.SignalEvent) string {
	return event.Dataset + "|" + event.Symbol + "|" + event.Timeframe + "|" + event.Algorithm.Name + "|" +
		event.Algorithm.Version + "|" + event.Algorithm.ConfigHash
}
