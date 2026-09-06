package indicator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/rohanjq/signals/internal/model"
)

const MarketAlgorithmVersion = "1.0.0"

type MarketConfig struct {
	MinSwingPct       float64
	ATRPeriod         int
	ATRMult           float64
	MinPullbackFrac   float64
	EqualTolerance    float64
	RangeMinSwings    int
	RangeMaxHeight    float64
	SecondMaruMin     float64
	BigVsReference    float64
	BreakBodyBeyond   float64
	OppositeSmallMax  float64
	PACWindow         int
	LevelClusterTol   float64
	LevelSwingLag     int
	MaxLevels         int
	MaxLevelDistance  float64
	RecentBars        int
	FVGLookback       int
	PatternLookback   int
	LiquidityLookback int
	LiquidityTol      float64
}

func DefaultMarketConfig() MarketConfig {
	return MarketConfig{
		MinSwingPct: 0.0005, ATRPeriod: 14, ATRMult: 2, MinPullbackFrac: 0.6,
		EqualTolerance: 0.0015, RangeMinSwings: 4, RangeMaxHeight: 0.05,
		SecondMaruMin: 0.7, BigVsReference: 0.7, BreakBodyBeyond: 0.3,
		OppositeSmallMax: 0.3, PACWindow: 4, LevelClusterTol: 0.0015,
		LevelSwingLag: 2, MaxLevels: 5, MaxLevelDistance: 0.03,
		RecentBars: 400, FVGLookback: 300, PatternLookback: 120,
		LiquidityLookback: 300, LiquidityTol: 0.0006,
	}
}

func MarketAlgorithm(config MarketConfig) model.AlgorithmRef {
	raw := fmt.Sprintf("market|%s|%+v", MarketAlgorithmVersion, config)
	sum := sha256.Sum256([]byte(raw))
	return model.AlgorithmRef{Name: "smc.market_state", Version: MarketAlgorithmVersion, ConfigHash: "sha256:" + hex.EncodeToString(sum[:])}
}

type indexedSwing struct {
	Index        int
	Type         string
	Price        float64
	Time         time.Time
	ConfirmIndex int
	ConfirmTime  time.Time
	Label        string
}

type indexedStructure struct {
	Kind  string
	Side  string
	Price float64
	Time  time.Time
	Zone  *model.PriceZone
}

type candleFeatures struct {
	Range, Body, BodyRatio, Mid, CloseLocation float64
	Direction                                  int
	Up                                         bool
}

func EvaluateMarket(bars []model.Bar, config MarketConfig) model.MarketState {
	state := model.MarketState{Schema: "market.state.v1", Algorithm: MarketAlgorithm(config), Trend: "range"}
	closed := make([]model.Bar, 0, len(bars))
	for _, bar := range bars {
		if bar.Closed {
			closed = append(closed, bar)
		}
	}
	if len(closed) == 0 {
		return state
	}
	state.AsOf = closed[len(closed)-1].OpenTime.UTC()
	state.FVGs = detectFVG(closed, config.FVGLookback)
	state.OrderBlocks = detectOrderBlocks(closed, state.FVGs)
	state.Patterns = detectPatterns(closed, config.PatternLookback)
	state.Liquidity, state.LiquiditySweeps = detectLiquidity(closed, config.LiquidityLookback, config.LiquidityTol)

	structureBars := closed
	structureOffset := 0
	if len(structureBars) > config.RecentBars {
		structureOffset = len(structureBars) - config.RecentBars
		structureBars = structureBars[structureOffset:]
	}
	if len(structureBars) >= config.ATRPeriod+4 {
		swings := mergeByValidPullback(structureBars, zigzag(structureBars, config), config)
		events := scanBreaks(structureBars, swings, config)
		labelSwings(swings, events)
		state.Trend = marketTrend(swings, events, detectRange(structureBars, swings, config) != nil)
		state.Swings = publicSwings(swings)
		state.Structure = publicStructure(events)
		if marketRange := detectRange(structureBars, swings, config); marketRange != nil {
			state.Ranges = []model.PriceZone{*marketRange}
		}
		fullSwings := swings
		if structureOffset > 0 {
			fullSwings = mergeByValidPullback(closed, zigzag(closed, config), config)
		}
		state.KeyLevels = keyLevels(closed, fullSwings, config)
		state.PremiumDiscount = premiumDiscount(closed, swings, state.Trend)
	}
	return state
}

func features(bar model.Bar) candleFeatures {
	rangeValue := bar.High - bar.Low
	if rangeValue == 0 {
		rangeValue = 1e-9
	}
	body := math.Abs(bar.Close - bar.Open)
	direction := 0
	if bar.Close > bar.Open {
		direction = 1
	} else if bar.Close < bar.Open {
		direction = -1
	}
	return candleFeatures{
		Range: rangeValue, Body: body, BodyRatio: body / rangeValue,
		Mid: (bar.High + bar.Low) / 2, CloseLocation: (bar.Close - bar.Low) / rangeValue,
		Direction: direction, Up: bar.Close >= bar.Open,
	}
}

func simpleMarubozu(bar model.Bar) bool { return features(bar).BodyRatio > 0.8 }
func pinBar(bar model.Bar) bool         { return features(bar).BodyRatio < 0.3 }

func zoneID(kind, side string, at time.Time, top, bottom float64) string {
	raw := fmt.Sprintf("%s|%s|%s|%.12g|%.12g", kind, side, at.UTC().Format(time.RFC3339Nano), top, bottom)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func detectFVG(bars []model.Bar, lookback int) []model.PriceZone {
	start := max(2, len(bars)-lookback)
	out := make([]model.PriceZone, 0)
	for index := start; index < len(bars); index++ {
		first, middle, last := bars[index-2], bars[index-1], bars[index]
		anyMarubozu := simpleMarubozu(first) || simpleMarubozu(middle) || simpleMarubozu(last)
		var side string
		var top, bottom, edge, gap float64
		if last.Low > first.High && anyMarubozu {
			side, top, bottom, edge, gap = "up", last.Low, first.High, first.High, last.Low-first.High
		} else if last.High < first.Low && anyMarubozu {
			side, top, bottom, edge, gap = "down", first.Low, last.High, first.Low, first.Low-last.High
		} else {
			continue
		}
		filled := false
		for later := index + 1; later < len(bars); later++ {
			if (side == "up" && bars[later].Low <= edge) || (side == "down" && bars[later].High >= edge) {
				filled = true
				break
			}
		}
		if filled {
			continue
		}
		grade := "weak"
		if gap >= 0.3*features(middle).Range || (simpleMarubozu(first) && simpleMarubozu(middle) && simpleMarubozu(last)) {
			grade = "strong"
		}
		out = append(out, model.PriceZone{
			ID: zoneID("fvg", side, last.OpenTime, top, bottom), Kind: "fvg", Side: side, Grade: grade,
			FromTime: last.OpenTime.UTC(), ToTime: bars[len(bars)-1].OpenTime.UTC(), Top: top, Bottom: bottom,
		})
	}
	if len(out) > 8 {
		out = out[len(out)-8:]
	}
	return out
}

func detectOrderBlocks(bars []model.Bar, gaps []model.PriceZone) []model.PriceZone {
	out := make([]model.PriceZone, 0)
	for _, gap := range gaps {
		gapIndex := sort.Search(len(bars), func(index int) bool { return !bars[index].OpenTime.Before(gap.FromTime) })
		for index, searched := gapIndex-2, 0; index >= 0 && searched < 4; index, searched = index-1, searched+1 {
			bar := bars[index]
			if (gap.Side == "up" && !features(bar).Up) || (gap.Side == "down" && features(bar).Up) {
				out = append(out, model.PriceZone{
					ID: zoneID("order_block", gap.Side, bar.OpenTime, bar.High, bar.Low), Kind: "order_block",
					Side: gap.Side, Grade: gap.Grade, FromTime: bar.OpenTime.UTC(), ToTime: bars[len(bars)-1].OpenTime.UTC(),
					Top: bar.High, Bottom: bar.Low,
				})
				break
			}
		}
	}
	if len(out) > 6 {
		out = out[len(out)-6:]
	}
	return out
}

func detectPatterns(bars []model.Bar, lookback int) []model.PatternEvent {
	start := max(3, len(bars)-lookback)
	out := make([]model.PatternEvent, 0)
	appendPattern := func(bar model.Bar, kind, side string) {
		out = append(out, model.PatternEvent{Kind: kind, Side: side, Time: bar.OpenTime.UTC()})
	}
	for index := start; index < len(bars); index++ {
		previous, current := bars[index-1], bars[index]
		previousFeatures, currentFeatures := features(previous), features(current)
		if currentFeatures.Up && !previousFeatures.Up && currentFeatures.Body >= 1.25*previousFeatures.Body && current.Close > previous.Open && current.Open < previous.Close {
			appendPattern(current, "bull_engulfing", "up")
		}
		if !currentFeatures.Up && previousFeatures.Up && currentFeatures.Body >= 1.25*previousFeatures.Body && current.Close < previous.Open && current.Open > previous.Close {
			appendPattern(current, "bear_engulfing", "down")
		}
		if current.High < previous.High && current.Low > previous.Low {
			appendPattern(current, "inside_bar", "flat")
		}
		if index < start+1 {
			continue
		}
		twoBack := bars[index-2]
		twoBackFeatures := features(twoBack)
		if simpleMarubozu(twoBack) && !twoBackFeatures.Up && previousFeatures.BodyRatio < 0.4 && simpleMarubozu(current) && currentFeatures.Up && current.Close > twoBackFeatures.Mid {
			appendPattern(current, "morning_star", "up")
		}
		if simpleMarubozu(twoBack) && twoBackFeatures.Up && previousFeatures.BodyRatio < 0.4 && simpleMarubozu(current) && !currentFeatures.Up && current.Close < twoBackFeatures.Mid {
			appendPattern(current, "evening_star", "down")
		}
		if simpleMarubozu(twoBack) && simpleMarubozu(previous) && simpleMarubozu(current) && twoBackFeatures.Up && previousFeatures.Up && currentFeatures.Up && previous.Close > twoBack.Close && current.Close > previous.Close {
			appendPattern(current, "three_soldiers", "up")
		}
		if simpleMarubozu(twoBack) && simpleMarubozu(previous) && simpleMarubozu(current) && !twoBackFeatures.Up && !previousFeatures.Up && !currentFeatures.Up && previous.Close < twoBack.Close && current.Close < previous.Close {
			appendPattern(current, "three_crows", "down")
		}
		if pinBar(previous) && pinBar(current) && math.Abs(previous.Low-current.Low)/current.Low < 0.0006 {
			appendPattern(current, "tweezer_bottom", "up")
		}
		if pinBar(previous) && pinBar(current) && math.Abs(previous.High-current.High)/current.High < 0.0006 {
			appendPattern(current, "tweezer_top", "down")
		}
	}
	if len(out) > 12 {
		out = out[len(out)-12:]
	}
	return out
}

type extremum struct {
	Price float64
	Index int
}

type cluster struct {
	Price   float64
	Touches int
	Last    int
}

func detectLiquidity(bars []model.Bar, lookback int, tolerance float64) ([]model.PriceLevel, []model.PatternEvent) {
	start := max(5, len(bars)-lookback)
	highs, lows := make([]extremum, 0), make([]extremum, 0)
	for index := start + 2; index < len(bars)-2; index++ {
		bar := bars[index]
		if bar.High >= bars[index-1].High && bar.High >= bars[index-2].High && bar.High >= bars[index+1].High && bar.High >= bars[index+2].High {
			highs = append(highs, extremum{Price: bar.High, Index: index})
		}
		if bar.Low <= bars[index-1].Low && bar.Low <= bars[index-2].Low && bar.Low <= bars[index+1].Low && bar.Low <= bars[index+2].Low {
			lows = append(lows, extremum{Price: bar.Low, Index: index})
		}
	}
	clusterExtrema := func(values []extremum) []cluster {
		clusters := make([]cluster, 0)
		for _, value := range values {
			matched := -1
			for index := range clusters {
				if math.Abs(clusters[index].Price-value.Price)/value.Price < tolerance {
					matched = index
					break
				}
			}
			if matched >= 0 {
				current := &clusters[matched]
				current.Touches++
				current.Price = (current.Price*float64(current.Touches-1) + value.Price) / float64(current.Touches)
				current.Last = value.Index
			} else {
				clusters = append(clusters, cluster{Price: value.Price, Touches: 1, Last: value.Index})
			}
		}
		filtered := clusters[:0]
		for _, value := range clusters {
			if value.Touches >= 2 {
				filtered = append(filtered, value)
			}
		}
		if len(filtered) > 3 {
			filtered = filtered[len(filtered)-3:]
		}
		return filtered
	}
	highClusters, lowClusters := clusterExtrema(highs), clusterExtrema(lows)
	levels := make([]model.PriceLevel, 0, len(highClusters)+len(lowClusters))
	last := bars[len(bars)-1]
	sweeps := make([]model.PatternEvent, 0)
	for _, value := range highClusters {
		levels = append(levels, model.PriceLevel{Kind: "equal_high", Side: "sell_side", Price: value.Price, Touches: value.Touches})
		if last.High > value.Price && last.Close < value.Price {
			sweeps = append(sweeps, model.PatternEvent{Kind: "liquidity_sweep", Side: "down", Time: last.OpenTime.UTC()})
		}
	}
	for _, value := range lowClusters {
		levels = append(levels, model.PriceLevel{Kind: "equal_low", Side: "buy_side", Price: value.Price, Touches: value.Touches})
		if last.Low < value.Price && last.Close > value.Price {
			sweeps = append(sweeps, model.PatternEvent{Kind: "liquidity_sweep", Side: "up", Time: last.OpenTime.UTC()})
		}
	}
	return levels, sweeps
}

func averageTrueRange(bars []model.Bar, period int) []float64 {
	out := make([]float64, len(bars))
	sum := 0.0
	for index, bar := range bars {
		trueRange := bar.High - bar.Low
		if index > 0 {
			trueRange = max(trueRange, math.Abs(bar.High-bars[index-1].Close), math.Abs(bar.Low-bars[index-1].Close))
		}
		if index < period {
			sum += trueRange
			out[index] = sum / float64(index+1)
		} else {
			out[index] = (out[index-1]*float64(period-1) + trueRange) / float64(period)
		}
	}
	return out
}

func zigzag(bars []model.Bar, config MarketConfig) []indexedSwing {
	atr := averageTrueRange(bars, config.ATRPeriod)
	deviation := func(index int, price float64) float64 {
		if price <= 0 {
			return config.MinSwingPct
		}
		return max(config.MinSwingPct, config.ATRMult*atr[index]/price)
	}
	swings := make([]indexedSwing, 0)
	direction, highIndex, lowIndex := 0, 0, 0
	for index := 1; index < len(bars); index++ {
		if bars[index].High > bars[highIndex].High {
			highIndex = index
		}
		if bars[index].Low < bars[lowIndex].Low {
			lowIndex = index
		}
		if direction >= 0 && bars[index].Low <= bars[highIndex].High*(1-deviation(index, bars[highIndex].High)) {
			swings = append(swings, indexedSwing{
				Index: highIndex, Type: "H", Price: bars[highIndex].High, Time: bars[highIndex].OpenTime.UTC(),
				ConfirmIndex: index, ConfirmTime: bars[index].OpenTime.UTC(),
			})
			direction, lowIndex = -1, index
		} else if direction <= 0 && bars[index].High >= bars[lowIndex].Low*(1+deviation(index, bars[lowIndex].Low)) {
			swings = append(swings, indexedSwing{
				Index: lowIndex, Type: "L", Price: bars[lowIndex].Low, Time: bars[lowIndex].OpenTime.UTC(),
				ConfirmIndex: index, ConfirmTime: bars[index].OpenTime.UTC(),
			})
			direction, highIndex = 1, index
		}
	}
	return swings
}

func structureMarubozu(bar model.Bar, direction int) bool {
	value := features(bar)
	if value.Direction != direction {
		return false
	}
	return value.BodyRatio >= 0.7 || (direction > 0 && value.BodyRatio >= 0.5 && value.CloseLocation >= 0.9) ||
		(direction < 0 && value.BodyRatio >= 0.5 && value.CloseLocation <= 0.1)
}

func marubozuReference(bars []model.Bar, index int) float64 {
	count, maximum := 0, 0.0
	for current := index - 1; current >= 0 && count < 5; current-- {
		value := features(bars[current])
		if value.BodyRatio >= 0.7 || (value.BodyRatio >= 0.5 && (value.CloseLocation >= 0.9 || value.CloseLocation <= 0.1)) {
			count++
			maximum = max(maximum, value.Range)
		}
	}
	return maximum
}

func validPullback(bars []model.Bar, from, to, direction int, config MarketConfig) bool {
	for index := from; index < to; index++ {
		if !structureMarubozu(bars[index], direction) {
			continue
		}
		if structureMarubozu(bars[index+1], direction) {
			return true
		}
		reference := marubozuReference(bars, index)
		value := features(bars[index])
		if reference > 0 && value.Range >= config.BigVsReference*reference && features(bars[index+1]).Range <= config.OppositeSmallMax*value.Range {
			return true
		}
	}
	return false
}

func mergeByValidPullback(bars []model.Bar, swings []indexedSwing, config MarketConfig) []indexedSwing {
	changed := true
	for changed && len(swings) >= 3 {
		changed = false
		for index := 1; index < len(swings)-1; index++ {
			first, middle, last := swings[index-1], swings[index], swings[index+1]
			direction := 1
			if middle.Type == "L" {
				direction = -1
			}
			depth := math.Abs(first.Price - middle.Price)
			impulse := math.Abs(last.Price - middle.Price)
			if index >= 2 {
				impulse = math.Abs(first.Price - swings[index-2].Price)
			}
			retrace := 1.0
			if impulse > 0 {
				retrace = depth / impulse
			}
			if validPullback(bars, first.Index, middle.Index, direction, config) || retrace >= config.MinPullbackFrac {
				continue
			}
			if first.Type == "H" {
				if last.Price >= first.Price {
					swings = append(swings[:index-1], swings[index+1:]...)
				} else {
					swings = append(swings[:index], swings[index+2:]...)
				}
			} else if last.Price <= first.Price {
				swings = append(swings[:index-1], swings[index+1:]...)
			} else {
				swings = append(swings[:index], swings[index+2:]...)
			}
			changed = true
			break
		}
	}
	return swings
}

type breakoutResult struct {
	Status       string
	ConfirmIndex int
	Excursion    float64
}

func evaluateBreakout(bars []model.Bar, index int, line float64, direction int, config MarketConfig) breakoutResult {
	beyond := func(value float64) bool { return (direction > 0 && value > line) || (direction < 0 && value < line) }
	first := bars[index]
	firstFeatures := features(first)
	if !beyond(first.Close) {
		return breakoutResult{Status: "none"}
	}
	excursion := first.Low
	if direction > 0 {
		excursion = first.High
	}
	if index+1 >= len(bars) {
		return breakoutResult{Status: "pending", Excursion: excursion}
	}
	second := bars[index+1]
	secondFeatures := features(second)
	secondExcursion := min(first.Low, second.Low)
	if direction > 0 {
		secondExcursion = max(first.High, second.High)
	}
	if structureMarubozu(first, direction) && structureMarubozu(second, direction) && beyond(second.Close) &&
		((direction > 0 && second.Close > first.Close) || (direction < 0 && second.Close < first.Close)) && secondFeatures.Range >= config.SecondMaruMin*firstFeatures.Range {
		return breakoutResult{Status: "valid", ConfirmIndex: index + 1, Excursion: secondExcursion}
	}
	reference := marubozuReference(bars, index)
	penetration := 0.0
	if firstFeatures.Body > 0 {
		if direction > 0 {
			penetration = max(0, first.Close-max(first.Open, line)) / firstFeatures.Body
		} else {
			penetration = max(0, min(first.Open, line)-first.Close) / firstFeatures.Body
		}
	}
	big := reference > 0 && firstFeatures.Range >= config.BigVsReference*reference
	small := secondFeatures.Range <= config.OppositeSmallMax*firstFeatures.Range &&
		((direction > 0 && second.Low >= firstFeatures.Mid) || (direction < 0 && second.High <= firstFeatures.Mid))
	closedBack := (direction > 0 && second.Close < line) || (direction < 0 && second.Close > line)
	if structureMarubozu(first, direction) && big && penetration > config.BreakBodyBeyond && small && !closedBack {
		return breakoutResult{Status: "valid", ConfirmIndex: index + 1, Excursion: secondExcursion}
	}
	if (structureMarubozu(first, direction) || penetration > config.BreakBodyBeyond) && !closedBack {
		return breakoutResult{Status: "valid", ConfirmIndex: index + 1, Excursion: secondExcursion}
	}
	if closedBack {
		return breakoutResult{Status: "fake", ConfirmIndex: index + 1, Excursion: secondExcursion}
	}
	if structureMarubozu(first, direction) {
		end := index + 1 + config.PACWindow
		for current := index + 2; current <= min(len(bars)-1, end); current++ {
			if (direction > 0 && bars[current].Close > secondExcursion) || (direction < 0 && bars[current].Close < secondExcursion) {
				return breakoutResult{Status: "valid", ConfirmIndex: current, Excursion: secondExcursion}
			}
		}
		if len(bars)-1 >= end {
			return breakoutResult{Status: "fake", ConfirmIndex: index + 1, Excursion: secondExcursion}
		}
		return breakoutResult{Status: "pending", Excursion: secondExcursion}
	}
	return breakoutResult{Status: "fake", ConfirmIndex: index + 1, Excursion: secondExcursion}
}

func falseBreakZone(bars []model.Bar, reference float64, swingIndex, direction int) model.PriceZone {
	start := swingIndex
	for start > 0 && ((direction > 0 && bars[start-1].High > reference) || (direction < 0 && bars[start-1].Low < reference)) {
		start--
	}
	extreme := reference
	for index := start; index <= swingIndex; index++ {
		if direction > 0 {
			extreme = max(extreme, bars[index].High)
		} else {
			extreme = min(extreme, bars[index].Low)
		}
	}
	side := "down"
	if direction > 0 {
		side = "up"
	}
	return model.PriceZone{
		ID:   zoneID("false_break", side, bars[start].OpenTime, max(reference, extreme), min(reference, extreme)),
		Kind: "false_break", Side: side, FromTime: bars[start].OpenTime.UTC(), ToTime: bars[swingIndex].OpenTime.UTC(),
		Top: max(reference, extreme), Bottom: min(reference, extreme),
	}
}

func scanBreaks(bars []model.Bar, swings []indexedSwing, config MarketConfig) []indexedStructure {
	events := make([]indexedStructure, 0)
	trend := "range"
	var lastHigh, lastLow *indexedSwing
	for index := range swings {
		swing := swings[index]
		if swing.Type == "H" {
			if lastHigh != nil && swing.Price > lastHigh.Price {
				breakIndex := -1
				for current := lastHigh.Index + 1; current <= swing.Index; current++ {
					if bars[current].Close > lastHigh.Price {
						breakIndex = current
						break
					}
				}
				if breakIndex < 0 {
					zone := falseBreakZone(bars, lastHigh.Price, swing.Index, 1)
					events = append(events, indexedStructure{Kind: "FAKE", Side: "up", Price: lastHigh.Price, Time: bars[swing.Index].OpenTime.UTC(), Zone: &zone})
				} else if result := evaluateBreakout(bars, breakIndex, lastHigh.Price, 1, config); result.Status == "valid" {
					kind := "BOS"
					if trend == "down" {
						kind = "CHoCH"
					}
					events = append(events, indexedStructure{Kind: kind, Side: "up", Price: lastHigh.Price, Time: bars[result.ConfirmIndex].OpenTime.UTC()})
					trend = "up"
				} else if result.Status == "fake" {
					zone := falseBreakZone(bars, lastHigh.Price, swing.Index, 1)
					events = append(events, indexedStructure{Kind: "FAKE", Side: "up", Price: lastHigh.Price, Time: bars[breakIndex].OpenTime.UTC(), Zone: &zone})
				}
			}
			copy := swing
			lastHigh = &copy
		} else {
			if lastLow != nil && swing.Price < lastLow.Price {
				breakIndex := -1
				for current := lastLow.Index + 1; current <= swing.Index; current++ {
					if bars[current].Close < lastLow.Price {
						breakIndex = current
						break
					}
				}
				if breakIndex < 0 {
					zone := falseBreakZone(bars, lastLow.Price, swing.Index, -1)
					events = append(events, indexedStructure{Kind: "FAKE", Side: "down", Price: lastLow.Price, Time: bars[swing.Index].OpenTime.UTC(), Zone: &zone})
				} else if result := evaluateBreakout(bars, breakIndex, lastLow.Price, -1, config); result.Status == "valid" {
					kind := "BOS"
					if trend == "up" {
						kind = "CHoCH"
					}
					events = append(events, indexedStructure{Kind: kind, Side: "down", Price: lastLow.Price, Time: bars[result.ConfirmIndex].OpenTime.UTC()})
					trend = "down"
				} else if result.Status == "fake" {
					zone := falseBreakZone(bars, lastLow.Price, swing.Index, -1)
					events = append(events, indexedStructure{Kind: "FAKE", Side: "down", Price: lastLow.Price, Time: bars[breakIndex].OpenTime.UTC(), Zone: &zone})
				}
			}
			copy := swing
			lastLow = &copy
		}
	}
	return events
}

func labelSwings(swings []indexedSwing, events []indexedStructure) {
	confirmed := make([]indexedStructure, 0)
	for _, event := range events {
		if event.Kind != "FAKE" {
			confirmed = append(confirmed, event)
		}
	}
	sort.Slice(confirmed, func(i, j int) bool { return confirmed[i].Time.Before(confirmed[j].Time) })
	lastHigh, lastLow := math.NaN(), math.NaN()
	for index := range swings {
		trend := "unknown"
		for _, event := range confirmed {
			if event.Time.After(swings[index].Time) {
				break
			}
			trend = event.Side
		}
		if swings[index].Type == "H" {
			if trend == "down" {
				swings[index].Label = "LH"
			} else if !math.IsNaN(lastHigh) {
				if swings[index].Price > lastHigh {
					swings[index].Label = "HH"
				} else {
					swings[index].Label = "LH"
				}
			}
			lastHigh = swings[index].Price
		} else {
			if trend == "up" {
				swings[index].Label = "HL"
			} else if !math.IsNaN(lastLow) {
				if swings[index].Price < lastLow {
					swings[index].Label = "LL"
				} else {
					swings[index].Label = "HL"
				}
			}
			lastLow = swings[index].Price
		}
	}
}

func median(values []float64) float64 {
	copyValues := append([]float64(nil), values...)
	sort.Float64s(copyValues)
	return copyValues[len(copyValues)/2]
}

func detectRange(bars []model.Bar, swings []indexedSwing, config MarketConfig) *model.PriceZone {
	start := max(0, len(swings)-max(config.RangeMinSwings, 6))
	recent := swings[start:]
	highs, lows := make([]float64, 0), make([]float64, 0)
	for _, swing := range recent {
		if swing.Type == "H" {
			highs = append(highs, swing.Price)
		} else {
			lows = append(lows, swing.Price)
		}
	}
	if len(highs) < 2 || len(lows) < 2 || len(recent) < config.RangeMinSwings {
		return nil
	}
	top, bottom := median(highs), median(lows)
	middle := (top + bottom) / 2
	spread := func(values []float64) float64 {
		minimum, maximum := values[0], values[0]
		for _, value := range values[1:] {
			minimum, maximum = min(minimum, value), max(maximum, value)
		}
		return (maximum - minimum) / middle
	}
	if spread(highs) > config.EqualTolerance*3 || spread(lows) > config.EqualTolerance*3 || (top-bottom)/middle > config.RangeMaxHeight || top <= bottom {
		return nil
	}
	return &model.PriceZone{
		ID: zoneID("range", "flat", recent[0].Time, top, bottom), Kind: "range", Side: "flat",
		FromTime: recent[0].Time, ToTime: bars[len(bars)-1].OpenTime.UTC(), Top: top, Bottom: bottom,
	}
}

func keyLevels(bars []model.Bar, swings []indexedSwing, config MarketConfig) []model.PriceLevel {
	establishedEnd := max(0, len(swings)-config.LevelSwingLag)
	clusters := make([]cluster, 0)
	for _, swing := range swings[:establishedEnd] {
		matched := -1
		for index := range clusters {
			if math.Abs(clusters[index].Price-swing.Price)/swing.Price < config.LevelClusterTol {
				matched = index
				break
			}
		}
		if matched >= 0 {
			clusters[matched].Touches++
			clusters[matched].Last = max(clusters[matched].Last, swing.Index)
		} else {
			clusters = append(clusters, cluster{Price: swing.Price, Touches: 1, Last: swing.Index})
		}
	}
	lastPrice := bars[len(bars)-1].Close
	filtered := clusters[:0]
	for _, value := range clusters {
		if math.Abs(value.Price-lastPrice)/lastPrice <= config.MaxLevelDistance {
			filtered = append(filtered, value)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Touches > filtered[j].Touches || (filtered[i].Touches == filtered[j].Touches && filtered[i].Last > filtered[j].Last)
	})
	if len(filtered) > config.MaxLevels {
		filtered = filtered[:config.MaxLevels]
	}
	levels := make([]model.PriceLevel, 0, len(filtered))
	for _, value := range filtered {
		side := "support"
		if value.Price >= lastPrice {
			side = "resistance"
		}
		levels = append(levels, model.PriceLevel{Kind: "key_level", Side: side, Price: value.Price, Touches: value.Touches})
	}
	return levels
}

func marketTrend(swings []indexedSwing, events []indexedStructure, hasRange bool) string {
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Kind != "FAKE" {
			return events[index].Side
		}
	}
	if hasRange {
		return "range"
	}
	highs, lows := make([]indexedSwing, 0, 2), make([]indexedSwing, 0, 2)
	for index := len(swings) - 1; index >= 0 && (len(highs) < 2 || len(lows) < 2); index-- {
		if swings[index].Type == "H" && len(highs) < 2 {
			highs = append(highs, swings[index])
		} else if swings[index].Type == "L" && len(lows) < 2 {
			lows = append(lows, swings[index])
		}
	}
	if len(highs) == 2 && len(lows) == 2 {
		if highs[0].Price > highs[1].Price && lows[0].Price > lows[1].Price {
			return "up"
		}
		if highs[0].Price < highs[1].Price && lows[0].Price < lows[1].Price {
			return "down"
		}
	}
	return "range"
}

func premiumDiscount(bars []model.Bar, swings []indexedSwing, trend string) *model.PremiumDiscount {
	if len(swings) < 2 {
		return nil
	}
	up := trend != "down"
	var highSwing, lowSwing *indexedSwing
	if up {
		for index := len(swings) - 1; index >= 0; index-- {
			if swings[index].Type != "H" {
				continue
			}
			highCopy := swings[index]
			highSwing = &highCopy
			for previous := index - 1; previous >= 0; previous-- {
				if swings[previous].Type == "L" {
					lowCopy := swings[previous]
					lowSwing = &lowCopy
					break
				}
			}
			break
		}
	} else {
		for index := len(swings) - 1; index >= 0; index-- {
			if swings[index].Type != "L" {
				continue
			}
			lowCopy := swings[index]
			lowSwing = &lowCopy
			for previous := index - 1; previous >= 0; previous-- {
				if swings[previous].Type == "H" {
					highCopy := swings[previous]
					highSwing = &highCopy
					break
				}
			}
			break
		}
	}
	if highSwing == nil || lowSwing == nil || highSwing.Price <= lowSwing.Price {
		return nil
	}
	rangeValue := highSwing.Price - lowSwing.Price
	poiBottom, poiTop := lowSwing.Price+0.2*rangeValue, lowSwing.Price+0.382*rangeValue
	if !up {
		poiBottom, poiTop = lowSwing.Price+0.618*rangeValue, lowSwing.Price+0.8*rangeValue
	}
	from := highSwing.Time
	if lowSwing.Time.Before(from) {
		from = lowSwing.Time
	}
	return &model.PremiumDiscount{
		FromTime: from, ToTime: bars[len(bars)-1].OpenTime.UTC(), Trend: trend,
		High: highSwing.Price, Low: lowSwing.Price, Equilibrium: (highSwing.Price + lowSwing.Price) / 2,
		POITop: max(poiTop, poiBottom), POIBottom: min(poiTop, poiBottom),
	}
}

func publicSwings(values []indexedSwing) []model.Swing {
	out := make([]model.Swing, 0, len(values))
	for _, value := range values {
		out = append(out, model.Swing{Type: value.Type, Label: value.Label, Price: value.Price, Time: value.Time, ConfirmTime: value.ConfirmTime})
	}
	return out
}

func publicStructure(values []indexedStructure) []model.StructureEvent {
	out := make([]model.StructureEvent, 0, len(values))
	for _, value := range values {
		out = append(out, model.StructureEvent{Kind: value.Kind, Side: value.Side, Price: value.Price, Time: value.Time, Zone: value.Zone})
	}
	return out
}
