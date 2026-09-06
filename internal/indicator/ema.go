package indicator

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

const EMAAlgorithmVersion = "1.0.0"

type EMA struct {
	period  int
	samples uint64
	seedSum float64
	value   float64
}

type EMASnapshot struct {
	Period  int     `json:"period"`
	Samples uint64  `json:"samples"`
	SeedSum float64 `json:"seed_sum"`
	Value   float64 `json:"value"`
}

func NewEMA(period int) (*EMA, error) {
	if period < 1 {
		return nil, errors.New("EMA period must be positive")
	}
	return &EMA{period: period}, nil
}

func (e *EMA) Period() int { return e.period }

func (e *EMA) Samples() uint64 { return e.samples }

func (e *EMA) WarmupRequired() int { return e.period }

func (e *EMA) Ready() bool { return e.samples >= uint64(e.period) }

func (e *EMA) Value() (float64, bool) { return e.value, e.Ready() }

func (e *EMA) OnClosed(close float64) (float64, bool, error) {
	if math.IsNaN(close) || math.IsInf(close, 0) {
		return 0, false, errors.New("EMA close must be finite")
	}

	e.samples++
	if e.samples <= uint64(e.period) {
		e.seedSum += close
		if e.samples == uint64(e.period) {
			e.value = e.seedSum / float64(e.period)
			return e.value, true, nil
		}
		return 0, false, nil
	}

	multiplier := 2.0 / float64(e.period+1)
	e.value = close*multiplier + e.value*(1-multiplier)
	return e.value, true, nil
}

func (e *EMA) Project(close float64) (float64, bool, error) {
	copy := *e
	return copy.OnClosed(close)
}

func (e *EMA) Snapshot() ([]byte, error) {
	return json.Marshal(EMASnapshot{
		Period:  e.period,
		Samples: e.samples,
		SeedSum: e.seedSum,
		Value:   e.value,
	})
}

func (e *EMA) Restore(data []byte) error {
	var snapshot EMASnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode EMA snapshot: %w", err)
	}
	if snapshot.Period != e.period {
		return fmt.Errorf("EMA snapshot period %d does not match reducer period %d", snapshot.Period, e.period)
	}
	if snapshot.Samples < uint64(e.period) && snapshot.Value != 0 {
		return errors.New("EMA snapshot has a value before readiness")
	}
	for name, value := range map[string]float64{"seed_sum": snapshot.SeedSum, "value": snapshot.Value} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("EMA snapshot %s must be finite", name)
		}
	}
	e.samples = snapshot.Samples
	e.seedSum = snapshot.SeedSum
	e.value = snapshot.Value
	return nil
}
