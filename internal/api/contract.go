package api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/rohanjq/signals/internal/model"
)

const (
	protocolVersion   = "signals.v1"
	analysisEventType = "io.ytstack.signals.analysis.updated.v1"
	analysisSchema    = "urn:ytstack:signals:schema:analysis:v1"
)

type wireEnvelope struct {
	Protocol       string     `json:"protocol"`
	Type           string     `json:"type"`
	RequestID      string     `json:"request_id,omitempty"`
	SubscriptionID string     `json:"subscription_id,omitempty"`
	Cursor         string     `json:"cursor,omitempty"`
	Data           any        `json:"data,omitempty"`
	Error          *wireError `json:"error,omitempty"`
}

type wireError struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Details   map[string]any `json:"details,omitempty"`
}

type analysisEvent struct {
	SpecVersion     string       `json:"specversion"`
	ID              string       `json:"id"`
	Source          string       `json:"source"`
	Type            string       `json:"type"`
	Subject         string       `json:"subject"`
	Time            time.Time    `json:"time"`
	DataContentType string       `json:"datacontenttype"`
	DataSchema      string       `json:"dataschema"`
	Data            analysisData `json:"data"`
}

type analysisData struct {
	Series           model.SeriesKey   `json:"series"`
	Bar              analysisBar       `json:"bar"`
	Analysis         analysisRef       `json:"analysis"`
	AnalysisRevision uint64            `json:"analysis_revision"`
	Outputs          []analysisOutput  `json:"outputs"`
	Ready            bool              `json:"ready"`
	Samples          uint64            `json:"samples,omitempty"`
	RequiredSamples  uint64            `json:"required_samples,omitempty"`
	Implementation   implementationRef `json:"implementation"`
}

type analysisBar struct {
	OpenTime  time.Time `json:"open_time"`
	CloseTime time.Time `json:"close_time"`
	Revision  uint64    `json:"revision"`
	Status    string    `json:"status"`
}

type analysisRef struct {
	Name       string         `json:"name"`
	Parameters map[string]any `json:"parameters"`
}

type implementationRef struct {
	Engine           string `json:"engine"`
	EngineVersion    string `json:"engine_version"`
	AlgorithmVersion string `json:"algorithm_version"`
	ConfigHash       string `json:"config_hash"`
}

type analysisOutput struct {
	Name    string      `json:"name"`
	Type    string      `json:"type"`
	Unit    string      `json:"unit,omitempty"`
	Value   any         `json:"value"`
	Display displayHint `json:"display"`
}

type displayHint struct {
	Kind  string `json:"kind"`
	Pane  string `json:"pane"`
	Group string `json:"group,omitempty"`
	Order int    `json:"order,omitempty"`
}

type pointPrimitive struct {
	ID          string         `json:"id,omitempty"`
	Time        time.Time      `json:"time"`
	Value       *float64       `json:"value,omitempty"`
	Label       string         `json:"label,omitempty"`
	Direction   string         `json:"direction,omitempty"`
	ConfirmedAt *time.Time     `json:"confirmed_at,omitempty"`
	Attributes  map[string]any `json:"attributes,omitempty"`
}

type zonePrimitive struct {
	ID         string         `json:"id"`
	StartTime  time.Time      `json:"start_time"`
	EndTime    time.Time      `json:"end_time"`
	Upper      float64        `json:"upper"`
	Lower      float64        `json:"lower"`
	Label      string         `json:"label,omitempty"`
	Direction  string         `json:"direction,omitempty"`
	State      string         `json:"state,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

type levelPrimitive struct {
	ID         string         `json:"id,omitempty"`
	Value      float64        `json:"value"`
	Label      string         `json:"label,omitempty"`
	Direction  string         `json:"direction,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

func toAnalysisEvent(event model.SignalEvent, emittedAt time.Time) (analysisEvent, error) {
	duration, err := model.TimeframeDuration(event.Timeframe)
	if err != nil {
		return analysisEvent{}, fmt.Errorf("analysis event timeframe: %w", err)
	}
	status := "confirmed"
	if event.Provisional {
		status = "provisional"
	}
	data := analysisData{
		Series: model.SeriesKey{Dataset: event.Dataset, Symbol: event.Symbol, Timeframe: event.Timeframe},
		Bar: analysisBar{
			OpenTime: event.BarOpenTime.UTC(), CloseTime: event.BarOpenTime.Add(duration).UTC(),
			Revision: event.BarRevision, Status: status,
		},
		AnalysisRevision: event.AnalysisRevision,
		Outputs:          []analysisOutput{},
		Ready:            true,
		Implementation: implementationRef{
			Engine: "ytstack-native", EngineVersion: event.Algorithm.Version,
			AlgorithmVersion: event.Algorithm.Version, ConfigHash: event.Algorithm.ConfigHash,
		},
	}
	switch event.EventType {
	case "indicator.point":
		if event.Indicator == nil {
			return analysisEvent{}, fmt.Errorf("indicator event %q has no indicator payload", event.EventID)
		}
		data.Analysis = analysisRef{Name: event.Algorithm.Name, Parameters: map[string]any{"period": event.Indicator.Period}}
		data.Ready = event.Indicator.Ready
		data.Samples = event.Indicator.Samples
		data.RequiredSamples = uint64(event.Indicator.Period)
		if data.Ready {
			data.Outputs = []analysisOutput{{
				Name: "value", Type: "number", Unit: "price", Value: event.Indicator.Value,
				Display: displayHint{Kind: "line", Pane: "price", Group: event.Algorithm.Name, Order: event.Indicator.Period},
			}}
		}
	case "market.state":
		if event.Market == nil {
			return analysisEvent{}, fmt.Errorf("market event %q has no market payload", event.EventID)
		}
		data.Analysis = analysisRef{Name: event.Algorithm.Name, Parameters: map[string]any{}}
		data.Outputs = marketOutputs(*event.Market)
	default:
		return analysisEvent{}, fmt.Errorf("unsupported internal event type %q", event.EventType)
	}
	eventID, err := analysisEventID(data)
	if err != nil {
		return analysisEvent{}, err
	}
	eventTime := emittedAt.UTC()
	if !event.Provisional {
		eventTime = data.Bar.CloseTime
	}
	return analysisEvent{
		SpecVersion: "1.0", ID: eventID, Source: eventSource(event.Dataset),
		Type: analysisEventType, Subject: event.Dataset + "/" + event.Symbol + "/" + event.Timeframe,
		Time: eventTime, DataContentType: "application/json", DataSchema: analysisSchema, Data: data,
	}, nil
}

func toAnalysisEvents(events []model.SignalEvent, emittedAt time.Time) ([]analysisEvent, error) {
	out := make([]analysisEvent, 0, len(events))
	for _, event := range events {
		converted, err := toAnalysisEvent(event, emittedAt)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}

func snapshotAnalysisEvents(snapshot model.SeriesSnapshot, generatedAt time.Time) ([]analysisEvent, error) {
	events := make([]model.SignalEvent, 0, len(snapshot.Indicators)+len(snapshot.Provisional)+1)
	if snapshot.LastClosedTime != nil {
		for _, state := range snapshot.Indicators {
			state.AnalysisRevision = snapshot.LastClosedRevision
			events = append(events, snapshotIndicatorEvent(snapshot.Series, *snapshot.LastClosedTime, state, false))
		}
	}
	if snapshot.FormingOpenTime != nil {
		for _, state := range snapshot.Provisional {
			events = append(events, snapshotIndicatorEvent(snapshot.Series, *snapshot.FormingOpenTime, state, true))
		}
	}
	if snapshot.Market != nil && !snapshot.Market.AsOf.IsZero() {
		market := *snapshot.Market
		events = append(events, model.SignalEvent{
			EventID:   syntheticEventID(snapshot.Series, market.AsOf, market.Algorithm, "market"),
			EventType: "market.state", Dataset: snapshot.Series.Dataset, Symbol: snapshot.Series.Symbol,
			Timeframe: snapshot.Series.Timeframe, BarOpenTime: market.AsOf,
			BarRevision: snapshot.LastClosedRevision, AnalysisRevision: snapshot.LastClosedRevision,
			Algorithm: market.Algorithm, Market: &market,
		})
	}
	return toAnalysisEvents(events, generatedAt)
}

func snapshotIndicatorEvent(series model.SeriesKey, barTime time.Time, state model.IndicatorState, provisional bool) model.SignalEvent {
	value := 0.0
	if state.Value != nil {
		value = *state.Value
	}
	return model.SignalEvent{
		EventID:   syntheticEventID(series, barTime, state.Algorithm, fmt.Sprintf("%d|%t", state.Period, provisional)),
		EventType: "indicator.point", Dataset: series.Dataset, Symbol: series.Symbol, Timeframe: series.Timeframe,
		BarOpenTime: barTime, BarRevision: state.AnalysisRevision, AnalysisRevision: state.AnalysisRevision,
		Provisional: provisional, Algorithm: state.Algorithm,
		Indicator: &model.IndicatorPayload{
			Period: state.Period, Value: value, Samples: state.Samples, Ready: state.Ready,
		},
	}
}

func marketOutputs(market model.MarketState) []analysisOutput {
	return []analysisOutput{
		{Name: "trend", Type: "text", Value: market.Trend, Display: displayHint{Kind: "state", Pane: "status"}},
		{Name: "swings", Type: "points", Value: swingPrimitives(market.Swings), Display: displayHint{Kind: "polyline", Pane: "price", Group: "structure"}},
		{Name: "structure", Type: "markers", Value: structurePrimitives(market.Structure), Display: displayHint{Kind: "markers", Pane: "price", Group: "structure"}},
		{Name: "structure_zones", Type: "zones", Value: structureZonePrimitives(market.Structure), Display: displayHint{Kind: "zones", Pane: "price", Group: "structure"}},
		{Name: "ranges", Type: "zones", Value: zonePrimitives(market.Ranges), Display: displayHint{Kind: "zones", Pane: "price", Group: "ranges"}},
		{Name: "key_levels", Type: "levels", Value: levelPrimitives(market.KeyLevels), Display: displayHint{Kind: "levels", Pane: "price", Group: "ranges"}},
		{Name: "fvgs", Type: "zones", Value: zonePrimitives(market.FVGs), Display: displayHint{Kind: "zones", Pane: "price", Group: "fvg"}},
		{Name: "order_blocks", Type: "zones", Value: zonePrimitives(market.OrderBlocks), Display: displayHint{Kind: "zones", Pane: "price", Group: "order_blocks"}},
		{Name: "liquidity", Type: "levels", Value: levelPrimitives(market.Liquidity), Display: displayHint{Kind: "levels", Pane: "price", Group: "liquidity"}},
		{Name: "liquidity_sweeps", Type: "markers", Value: patternPrimitives(market.LiquiditySweeps), Display: displayHint{Kind: "markers", Pane: "price", Group: "liquidity"}},
		{Name: "patterns", Type: "markers", Value: patternPrimitives(market.Patterns), Display: displayHint{Kind: "markers", Pane: "price", Group: "patterns"}},
		{Name: "premium_discount", Type: "zones", Value: premiumDiscountPrimitives(market.PremiumDiscount), Display: displayHint{Kind: "zones", Pane: "price", Group: "premium_discount"}},
	}
}

func swingPrimitives(values []model.Swing) []pointPrimitive {
	out := make([]pointPrimitive, 0, len(values))
	for _, value := range values {
		confirmedAt, price := value.ConfirmTime.UTC(), value.Price
		out = append(out, pointPrimitive{ID: primitiveID("swing", value.Type, value.Time, value.Price),
			Time: value.Time.UTC(), Value: &price, Label: value.Label,
			Direction: canonicalDirection(value.Type), ConfirmedAt: &confirmedAt, Attributes: map[string]any{"kind": value.Type}})
	}
	return out
}

func structurePrimitives(values []model.StructureEvent) []pointPrimitive {
	out := make([]pointPrimitive, 0, len(values))
	for _, value := range values {
		price := value.Price
		out = append(out, pointPrimitive{ID: primitiveID("structure", value.Kind+"|"+value.Side, value.Time, value.Price),
			Time: value.Time.UTC(), Value: &price, Label: value.Kind,
			Direction: canonicalDirection(value.Side), Attributes: map[string]any{"kind": value.Kind, "source_direction": value.Side}})
	}
	return out
}

func structureZonePrimitives(values []model.StructureEvent) []zonePrimitive {
	out := make([]zonePrimitive, 0)
	for _, value := range values {
		if value.Zone != nil {
			out = append(out, zonePrimitiveFrom(*value.Zone))
		}
	}
	return out
}

func patternPrimitives(values []model.PatternEvent) []pointPrimitive {
	out := make([]pointPrimitive, 0, len(values))
	for _, value := range values {
		out = append(out, pointPrimitive{ID: primitiveID("pattern", value.Kind+"|"+value.Side, value.Time, 0),
			Time: value.Time.UTC(), Label: value.Kind, Direction: canonicalDirection(value.Side),
			Attributes: map[string]any{"kind": value.Kind, "source_direction": value.Side}})
	}
	return out
}

func zonePrimitives(values []model.PriceZone) []zonePrimitive {
	out := make([]zonePrimitive, 0, len(values))
	for _, value := range values {
		out = append(out, zonePrimitiveFrom(value))
	}
	return out
}

func zonePrimitiveFrom(value model.PriceZone) zonePrimitive {
	return zonePrimitive{ID: value.ID, StartTime: value.FromTime.UTC(), EndTime: value.ToTime.UTC(),
		Upper: value.Top, Lower: value.Bottom, Label: value.Kind, Direction: canonicalDirection(value.Side), State: "active",
		Attributes: map[string]any{"grade": value.Grade, "source_direction": value.Side}}
}

func levelPrimitives(values []model.PriceLevel) []levelPrimitive {
	out := make([]levelPrimitive, 0, len(values))
	for _, value := range values {
		out = append(out, levelPrimitive{ID: primitiveID("level", value.Kind+"|"+value.Side, time.Time{}, value.Price),
			Value: value.Price, Label: value.Kind, Direction: canonicalDirection(value.Side),
			Attributes: map[string]any{"touches": value.Touches, "source_direction": value.Side}})
	}
	return out
}

func premiumDiscountPrimitives(value *model.PremiumDiscount) []zonePrimitive {
	if value == nil {
		return []zonePrimitive{}
	}
	base := func(id, label, direction string, upper, lower float64) zonePrimitive {
		return zonePrimitive{ID: id, StartTime: value.FromTime.UTC(), EndTime: value.ToTime.UTC(),
			Upper: upper, Lower: lower, Label: label, Direction: canonicalDirection(direction), State: "active",
			Attributes: map[string]any{"trend": value.Trend}}
	}
	return []zonePrimitive{
		base("premium", "premium", "down", value.High, value.Equilibrium),
		base("discount", "discount", "up", value.Equilibrium, value.Low),
		base("poi", "poi", value.Trend, value.POITop, value.POIBottom),
	}
}

func canonicalDirection(value string) string {
	switch value {
	case "H", "down", "sell_side", "bear", "bearish":
		return "bearish"
	case "L", "up", "buy_side", "bull", "bullish":
		return "bullish"
	default:
		return "neutral"
	}
}

func primitiveID(kind, identity string, at time.Time, value float64) string {
	raw := kind + "|" + identity + "|" + at.UTC().Format(time.RFC3339Nano) + "|" + strconv.FormatFloat(value, 'g', -1, 64)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func eventSource(dataset string) string {
	return "urn:ytstack:signals:dataset:" + base64.RawURLEncoding.EncodeToString([]byte(dataset))
}

func syntheticEventID(series model.SeriesKey, barTime time.Time, algorithm model.AlgorithmRef, suffix string) string {
	identity := series.String() + "|" + barTime.UTC().Format(time.RFC3339Nano) + "|" +
		algorithm.Name + "|" + algorithm.Version + "|" + algorithm.ConfigHash + "|" + suffix
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}

func analysisEventID(data analysisData) (string, error) {
	parameters, err := json.Marshal(data.Analysis.Parameters)
	if err != nil {
		return "", fmt.Errorf("encode analysis identity parameters: %w", err)
	}
	identity := data.Series.String() + "|" + data.Bar.OpenTime.UTC().Format(time.RFC3339Nano) + "|" +
		data.Analysis.Name + "|" + string(parameters) + "|" + data.Implementation.ConfigHash + "|" +
		data.Bar.Status + "|" + strconv.FormatUint(data.AnalysisRevision, 10)
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:]), nil
}

func eventCursor(event model.SignalEvent) string {
	if event.Cursor <= 0 {
		return ""
	}
	return strconv.FormatInt(event.Cursor, 10)
}

func indicatorConfigHash(name, version string, period int) string {
	config := name + "|" + version + "|" + strconv.Itoa(period) + "|sma-seed"
	sum := sha256.Sum256([]byte(config))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func marketOutputCatalog() []map[string]any {
	outputs := marketOutputs(model.MarketState{})
	catalog := make([]map[string]any, 0, len(outputs))
	for _, output := range outputs {
		catalog = append(catalog, map[string]any{
			"name": output.Name, "type": output.Type, "unit": output.Unit, "display": output.Display,
		})
	}
	return catalog
}
