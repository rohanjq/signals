package indicator

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/rohanjq/signals/internal/model"
)

type Market struct {
	config       MarketConfig
	historyLimit int
	bars         []model.Bar
	samples      uint64
	state        model.MarketState
}

type marketSnapshot struct {
	Version string      `json:"version"`
	Samples uint64      `json:"samples"`
	Bars    []model.Bar `json:"bars"`
}

func NewMarket(config MarketConfig, historyLimit int) (*Market, error) {
	if historyLimit < max(config.RecentBars, config.FVGLookback, config.LiquidityLookback) {
		return nil, fmt.Errorf("market history limit must cover configured lookbacks")
	}
	return &Market{config: config, historyLimit: historyLimit}, nil
}

func (m *Market) OnClosed(bar model.Bar) model.MarketState {
	previous := m.state
	m.bars = append(m.bars, bar)
	if len(m.bars) > m.historyLimit {
		m.bars = append([]model.Bar(nil), m.bars[len(m.bars)-m.historyLimit:]...)
	}
	m.samples++
	m.state = EvaluateMarket(m.bars, m.config)
	m.state.PatternOccurrences = patternOccurrences(m.state.Patterns, bar.OpenTime)
	m.state.ZoneTransitions = zoneTransitions(previous, m.state, bar)
	return m.state
}

func patternOccurrences(patterns []model.PatternEvent, at time.Time) []model.PatternEvent {
	out := make([]model.PatternEvent, 0)
	for _, pattern := range patterns {
		if pattern.Time.Equal(at) {
			out = append(out, pattern)
		}
	}
	return out
}

func allZones(state model.MarketState) []model.PriceZone {
	out := make([]model.PriceZone, 0, len(state.FVGs)+len(state.OrderBlocks))
	out = append(out, state.FVGs...)
	out = append(out, state.OrderBlocks...)
	return out
}

func zoneTransitions(previous, current model.MarketState, bar model.Bar) []model.ZoneTransition {
	before := make(map[string]model.PriceZone)
	for _, zone := range allZones(previous) {
		before[zone.ID] = zone
	}
	after := make(map[string]model.PriceZone)
	out := make([]model.ZoneTransition, 0)
	appendTransition := func(zone model.PriceZone, transition string) {
		out = append(out, model.ZoneTransition{Zone: zone, Transition: transition, At: bar.OpenTime.UTC()})
	}
	for _, zone := range allZones(current) {
		after[zone.ID] = zone
		old, existed := before[zone.ID]
		if !existed {
			appendTransition(zone, "created")
		} else if old.State != "touched" && zone.State == "touched" {
			appendTransition(zone, "touched")
		}
	}
	for id, zone := range before {
		if _, exists := after[id]; exists {
			continue
		}
		intersects := intersectsZone(bar, zone)
		if !intersects {
			appendTransition(zone, "expired")
			continue
		}
		if zone.State != "touched" {
			appendTransition(zone, "touched")
		}
		transition := "mitigated"
		if zone.Kind == "order_block" && ((zone.Side == "up" && bar.Close < zone.Bottom) || (zone.Side == "down" && bar.Close > zone.Top)) {
			transition = "invalidated"
		}
		appendTransition(zone, transition)
	}
	return out
}

func intersectsZone(bar model.Bar, zone model.PriceZone) bool {
	return bar.High >= zone.Bottom && bar.Low <= zone.Top && bar.OpenTime.After(zone.FromTime)
}

func (m *Market) Seed(bars []model.Bar) model.MarketState {
	m.bars = m.bars[:0]
	m.samples = 0
	for _, bar := range bars {
		if !bar.Closed {
			continue
		}
		m.bars = append(m.bars, bar)
		m.samples++
	}
	if len(m.bars) > m.historyLimit {
		m.bars = append([]model.Bar(nil), m.bars[len(m.bars)-m.historyLimit:]...)
	}
	m.state = EvaluateMarket(m.bars, m.config)
	return m.state
}

func (m *Market) State() model.MarketState { return m.state }

func (m *Market) Samples() uint64 { return m.samples }

// Bars returns the bounded closed-bar history currently represented by the
// reducer. Callers use it to compare a reconnect seed with live state without
// exposing the reducer's mutable backing slice.
func (m *Market) Bars() []model.Bar { return append([]model.Bar(nil), m.bars...) }

func (m *Market) Snapshot() ([]byte, error) {
	return json.Marshal(marketSnapshot{Version: MarketAlgorithmVersion, Samples: m.samples, Bars: m.bars})
}

func (m *Market) Restore(data []byte) error {
	var snapshot marketSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode market snapshot: %w", err)
	}
	if snapshot.Version != MarketAlgorithmVersion {
		return fmt.Errorf("market snapshot version %q is not supported", snapshot.Version)
	}
	if len(snapshot.Bars) > m.historyLimit {
		return fmt.Errorf("market snapshot has %d bars, limit is %d", len(snapshot.Bars), m.historyLimit)
	}
	m.bars = append([]model.Bar(nil), snapshot.Bars...)
	m.samples = snapshot.Samples
	m.state = EvaluateMarket(m.bars, m.config)
	return nil
}

func (m *Market) Clone() (*Market, error) {
	clone, err := NewMarket(m.config, m.historyLimit)
	if err != nil {
		return nil, err
	}
	state, err := m.Snapshot()
	if err != nil {
		return nil, err
	}
	if err := clone.Restore(state); err != nil {
		return nil, err
	}
	return clone, nil
}
