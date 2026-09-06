package indicator

import (
	"encoding/json"
	"fmt"

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
	m.bars = append(m.bars, bar)
	if len(m.bars) > m.historyLimit {
		m.bars = append([]model.Bar(nil), m.bars[len(m.bars)-m.historyLimit:]...)
	}
	m.samples++
	m.state = EvaluateMarket(m.bars, m.config)
	return m.state
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
