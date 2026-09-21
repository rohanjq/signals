package indicator

import (
	"math"
	"slices"
	"testing"
	"time"

	"github.com/rohanjq/signals/internal/model"
)

func TestEvaluateMarketFindsUnfilledFVGAndOrderBlock(t *testing.T) {
	start := time.Unix(1_700_000_000, 0).UTC()
	bars := []model.Bar{
		marketBar(start, 100, 101, 98, 99),
		marketBar(start.Add(time.Minute), 99, 110, 99, 109),
		marketBar(start.Add(2*time.Minute), 112, 114, 111, 113),
	}
	state := EvaluateMarket(bars, DefaultMarketConfig())
	if len(state.FVGs) != 1 || state.FVGs[0].Side != "up" || state.FVGs[0].Bottom != 101 || state.FVGs[0].Top != 111 {
		t.Fatalf("FVGs = %+v", state.FVGs)
	}
	if len(state.OrderBlocks) != 1 || state.OrderBlocks[0].Bottom != 98 || state.OrderBlocks[0].Top != 101 {
		t.Fatalf("order blocks = %+v", state.OrderBlocks)
	}

	bars = append(bars, marketBar(start.Add(3*time.Minute), 112, 113, 100, 102))
	state = EvaluateMarket(bars, DefaultMarketConfig())
	if len(state.FVGs) != 0 {
		t.Fatalf("filled FVGs = %+v, want none", state.FVGs)
	}
}

func TestEvaluateMarketFindsPatterns(t *testing.T) {
	start := time.Unix(1_700_000_000, 0).UTC()
	bars := []model.Bar{
		marketBar(start, 100, 102, 98, 99),
		marketBar(start.Add(time.Minute), 99, 100, 97, 98),
		marketBar(start.Add(2*time.Minute), 99, 100, 95, 96),
		marketBar(start.Add(3*time.Minute), 95.5, 101, 95, 100.5),
	}
	state := EvaluateMarket(bars, DefaultMarketConfig())
	found := false
	for _, pattern := range state.Patterns {
		if pattern.Kind == "bull_engulfing" {
			found = true
		}
	}
	if !found {
		t.Fatalf("patterns = %+v, want bull_engulfing", state.Patterns)
	}
}

func TestMarketEmitsZoneLifecycleTransitions(t *testing.T) {
	start := time.Unix(1_700_000_000, 0).UTC()
	market, err := NewMarket(DefaultMarketConfig(), 500)
	if err != nil {
		t.Fatal(err)
	}
	market.OnClosed(marketBar(start, 100, 101, 98, 99))
	market.OnClosed(marketBar(start.Add(time.Minute), 99, 110, 99, 109))
	created := market.OnClosed(marketBar(start.Add(2*time.Minute), 112, 114, 111, 113))
	if !hasZoneTransition(created.ZoneTransitions, "fvg", "created") {
		t.Fatalf("created transitions = %+v", created.ZoneTransitions)
	}
	touched := market.OnClosed(marketBar(start.Add(3*time.Minute), 112, 113, 105, 110))
	if !hasZoneTransition(touched.ZoneTransitions, "fvg", "touched") {
		t.Fatalf("touched transitions = %+v", touched.ZoneTransitions)
	}
	mitigated := market.OnClosed(marketBar(start.Add(4*time.Minute), 110, 111, 100, 102))
	if !hasZoneTransition(mitigated.ZoneTransitions, "fvg", "mitigated") {
		t.Fatalf("mitigated transitions = %+v", mitigated.ZoneTransitions)
	}
}

func hasZoneTransition(values []model.ZoneTransition, kind, transition string) bool {
	for _, value := range values {
		if value.Zone.Kind == kind && value.Transition == transition {
			return true
		}
	}
	return false
}

func TestDetectPatternsCoversLegacySet(t *testing.T) {
	start := time.Unix(1_700_000_000, 0).UTC()
	filler := marketBar(start, 100, 101, 99, 100)
	tests := []struct {
		name string
		want string
		bars []model.Bar
	}{
		{name: "bull engulfing", want: "bull_engulfing", bars: []model.Bar{filler, filler, marketBar(start, 100, 101, 97, 98), marketBar(start, 97.5, 101.5, 97, 100.5)}},
		{name: "bear engulfing", want: "bear_engulfing", bars: []model.Bar{filler, filler, marketBar(start, 96, 100, 95, 99), marketBar(start, 99.5, 100, 95, 95.5)}},
		{name: "inside bar", want: "inside_bar", bars: []model.Bar{filler, filler, marketBar(start, 100, 110, 90, 105), marketBar(start, 104, 108, 92, 103)}},
		{name: "morning star", want: "morning_star", bars: []model.Bar{filler, filler, marketBar(start, 110, 111, 99, 100), marketBar(start, 100, 102, 98, 100.5), marketBar(start, 100.5, 111, 100, 110)}},
		{name: "evening star", want: "evening_star", bars: []model.Bar{filler, filler, marketBar(start, 100, 111, 99, 110), marketBar(start, 109.5, 112, 108, 109), marketBar(start, 109, 110, 99, 100)}},
		{name: "three soldiers", want: "three_soldiers", bars: []model.Bar{filler, filler, marketBar(start, 100, 105.2, 99.8, 105), marketBar(start, 105, 111.2, 104.8, 111), marketBar(start, 111, 117.2, 110.8, 117)}},
		{name: "three crows", want: "three_crows", bars: []model.Bar{filler, filler, marketBar(start, 117, 117.2, 111.8, 112), marketBar(start, 112, 112.2, 105.8, 106), marketBar(start, 106, 106.2, 99.8, 100)}},
		{name: "tweezer bottom", want: "tweezer_bottom", bars: []model.Bar{filler, filler, filler, marketBar(start, 101, 104, 99, 102), marketBar(start, 102, 106, 99.02, 101.5)}},
		{name: "tweezer top", want: "tweezer_top", bars: []model.Bar{filler, filler, filler, marketBar(start, 101, 105, 98, 102), marketBar(start, 102, 105.02, 96, 101.5)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for index := range test.bars {
				test.bars[index].OpenTime = start.Add(time.Duration(index) * time.Minute)
			}
			patterns := detectPatterns(test.bars, 120)
			kinds := make([]string, 0, len(patterns))
			for _, pattern := range patterns {
				kinds = append(kinds, pattern.Kind)
			}
			if !slices.Contains(kinds, test.want) {
				t.Fatalf("patterns = %v, want %s", kinds, test.want)
			}
		})
	}
}

func TestDetectLiquidityLevelsAndSweeps(t *testing.T) {
	start := time.Unix(1_700_000_000, 0).UTC()
	bars := make([]model.Bar, 20)
	for index := range bars {
		bars[index] = marketBar(start.Add(time.Duration(index)*time.Minute), 100, 102, 98, 100)
	}
	for _, index := range []int{7, 11} {
		bars[index].High = 110
	}
	for _, index := range []int{9, 13} {
		bars[index].Low = 90
	}
	bars[len(bars)-1] = marketBar(start.Add(19*time.Minute), 100, 111, 89, 100)
	levels, sweeps := detectLiquidity(bars, 300, 0.0006)
	hasHigh, hasLow := false, false
	for _, level := range levels {
		hasHigh = hasHigh || (level.Kind == "equal_high" && level.Price == 110 && level.Touches == 2)
		hasLow = hasLow || (level.Kind == "equal_low" && level.Price == 90 && level.Touches == 2)
	}
	if !hasHigh || !hasLow {
		t.Fatalf("levels = %+v", levels)
	}
	hasDownSweep, hasUpSweep := false, false
	for _, sweep := range sweeps {
		hasDownSweep = hasDownSweep || sweep.Side == "down"
		hasUpSweep = hasUpSweep || sweep.Side == "up"
	}
	if !hasDownSweep || !hasUpSweep {
		t.Fatalf("sweeps = %+v", sweeps)
	}
}

func TestStructureRangeLevelsAndPremiumDiscount(t *testing.T) {
	start := time.Unix(1_700_000_000, 0).UTC()
	bars := make([]model.Bar, 11)
	for index := range bars {
		bars[index] = marketBar(start.Add(time.Duration(index)*time.Minute), 100, 101, 99, 100)
	}
	bars[1] = marketBar(start.Add(time.Minute), 100, 105, 99, 104)
	bars[2] = marketBar(start.Add(2*time.Minute), 98, 100, 95, 97)
	bars[4] = marketBar(start.Add(4*time.Minute), 104, 108, 103.5, 107.5)
	bars[5] = marketBar(start.Add(5*time.Minute), 107, 111, 106.5, 110.5)
	bars[7] = marketBar(start.Add(7*time.Minute), 96, 96.5, 91, 92)
	bars[8] = marketBar(start.Add(8*time.Minute), 92.5, 93, 89, 89.5)
	bars[10] = marketBar(start.Add(10*time.Minute), 109, 112, 108, 109)
	swings := []indexedSwing{
		{Index: 1, Type: "H", Price: 105, Time: bars[1].OpenTime},
		{Index: 2, Type: "L", Price: 95, Time: bars[2].OpenTime},
		{Index: 5, Type: "H", Price: 111, Time: bars[5].OpenTime},
		{Index: 8, Type: "L", Price: 89, Time: bars[8].OpenTime},
		{Index: 10, Type: "H", Price: 112, Time: bars[10].OpenTime},
	}
	events := scanBreaks(bars, swings, DefaultMarketConfig())
	kinds := make([]string, 0, len(events))
	for _, event := range events {
		kinds = append(kinds, event.Kind)
	}
	for _, want := range []string{"BOS", "CHoCH", "FAKE"} {
		if !slices.Contains(kinds, want) {
			t.Fatalf("structure events = %v, want %s", kinds, want)
		}
	}

	rangeSwings := []indexedSwing{
		{Index: 1, Type: "H", Price: 110, Time: bars[1].OpenTime},
		{Index: 2, Type: "L", Price: 100, Time: bars[2].OpenTime},
		{Index: 3, Type: "H", Price: 110.02, Time: bars[3].OpenTime},
		{Index: 4, Type: "L", Price: 100.02, Time: bars[4].OpenTime},
		{Index: 5, Type: "H", Price: 110.01, Time: bars[5].OpenTime},
		{Index: 6, Type: "L", Price: 100.01, Time: bars[6].OpenTime},
	}
	config := DefaultMarketConfig()
	config.MaxLevelDistance = 0.1
	config.RangeMaxHeight = 0.15
	if detected := detectRange(bars, rangeSwings, config); detected == nil || detected.Top <= detected.Bottom {
		t.Fatalf("range = %+v", detected)
	}
	levels := keyLevels(bars, rangeSwings, config)
	if len(levels) != 2 || levels[0].Touches < 2 || levels[1].Touches < 2 {
		t.Fatalf("key levels = %+v", levels)
	}
	premium := premiumDiscount(bars, rangeSwings, "up")
	if premium == nil || math.Abs(premium.Equilibrium-105.015) > 1e-12 || premium.POIBottom >= premium.POITop {
		t.Fatalf("premium/discount = %+v", premium)
	}
}

func TestEvaluateMarketIsDeterministic(t *testing.T) {
	start := time.Unix(1_700_000_000, 0).UTC()
	bars := make([]model.Bar, 0, 80)
	price := 100.0
	for index := range 80 {
		delta := 2.0
		if (index/5)%2 == 1 {
			delta = -2
		}
		closeValue := price + delta
		bars = append(bars, marketBar(start.Add(time.Duration(index)*time.Minute), price, max(price, closeValue)+0.5, min(price, closeValue)-0.5, closeValue))
		price = closeValue
	}
	first := EvaluateMarket(bars, DefaultMarketConfig())
	second := EvaluateMarket(append([]model.Bar(nil), bars...), DefaultMarketConfig())
	if first.Algorithm != second.Algorithm || first.Trend != second.Trend || len(first.Swings) != len(second.Swings) || len(first.Structure) != len(second.Structure) {
		t.Fatalf("market evaluation is not deterministic: first=%+v second=%+v", first, second)
	}
}

func TestSwingUsesConfirmationBarTime(t *testing.T) {
	start := time.Unix(1_700_000_000, 0).UTC()
	bars := []model.Bar{
		marketBar(start, 100, 101, 99, 100),
		marketBar(start.Add(time.Minute), 108, 110, 108, 109),
		marketBar(start.Add(2*time.Minute), 103, 106, 100, 101),
	}
	config := DefaultMarketConfig()
	config.ATRMult = 0
	config.MinSwingPct = 0.05
	swings := zigzag(bars, config)
	if len(swings) == 0 {
		t.Fatal("expected a confirmed swing")
	}
	public := publicSwings(swings)
	foundDelayedConfirmation := false
	for index, swing := range swings {
		if !public[index].ConfirmTime.Equal(bars[swing.ConfirmIndex].OpenTime) {
			t.Fatalf("confirmation time = %s, want %s", public[index].ConfirmTime, bars[swing.ConfirmIndex].OpenTime)
		}
		if !public[index].Time.Equal(public[index].ConfirmTime) {
			foundDelayedConfirmation = true
		}
	}
	if !foundDelayedConfirmation {
		t.Fatal("expected at least one pivot to confirm on a later bar")
	}
}

func marketBar(at time.Time, open, high, low, closeValue float64) model.Bar {
	return model.Bar{Symbol: "BTCUSDT", Timeframe: "1m", OpenTime: at, Open: open, High: high, Low: low, Close: closeValue, Volume: 1, Closed: true}
}
