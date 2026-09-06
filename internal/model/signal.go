package model

import "time"

type AlgorithmRef struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	ConfigHash string `json:"config_hash"`
}

type IndicatorPayload struct {
	Period  int     `json:"period"`
	Value   float64 `json:"value"`
	Samples uint64  `json:"samples"`
	Ready   bool    `json:"ready"`
}

type SignalEvent struct {
	Schema           string            `json:"schema"`
	Cursor           int64             `json:"cursor,omitempty"`
	EventID          string            `json:"event_id"`
	EventType        string            `json:"event_type"`
	Dataset          string            `json:"dataset"`
	Symbol           string            `json:"symbol"`
	Timeframe        string            `json:"timeframe"`
	BarOpenTime      time.Time         `json:"bar_open_time"`
	BarRevision      uint64            `json:"bar_revision"`
	AnalysisRevision uint64            `json:"analysis_revision"`
	Provisional      bool              `json:"provisional"`
	Algorithm        AlgorithmRef      `json:"algorithm"`
	Indicator        *IndicatorPayload `json:"indicator,omitempty"`
	Market           *MarketState      `json:"market,omitempty"`
}

type ReducerSnapshot struct {
	Algorithm    AlgorithmRef `json:"algorithm"`
	Period       int          `json:"period"`
	LastOpenTime time.Time    `json:"last_open_time"`
	State        []byte       `json:"state"`
}

type SeriesRecovery struct {
	Snapshots      []ReducerSnapshot
	CursorOpenTime time.Time
	CursorHash     string
	HasCursor      bool
}

type IndicatorState struct {
	Algorithm        AlgorithmRef `json:"algorithm"`
	Period           int          `json:"period"`
	Value            *float64     `json:"value,omitempty"`
	Samples          uint64       `json:"samples"`
	Ready            bool         `json:"ready"`
	AnalysisRevision uint64       `json:"analysis_revision"`
}

type SeriesSnapshot struct {
	Schema             string           `json:"schema"`
	Series             SeriesKey        `json:"series"`
	LastClosedTime     *time.Time       `json:"last_closed_time,omitempty"`
	LastClosedRevision uint64           `json:"last_closed_revision"`
	FormingOpenTime    *time.Time       `json:"forming_open_time,omitempty"`
	FormingRevision    uint64           `json:"forming_revision"`
	Ready              bool             `json:"ready"`
	Indicators         []IndicatorState `json:"indicators"`
	Provisional        []IndicatorState `json:"provisional,omitempty"`
	Market             *MarketState     `json:"market,omitempty"`
}
