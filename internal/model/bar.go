package model

import "time"

type Bar struct {
	Symbol    string    `json:"symbol"`
	Timeframe string    `json:"tf"`
	OpenTime  time.Time `json:"open_time"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    float64   `json:"volume"`
	Trades    uint64    `json:"trades"`
	Closed    bool      `json:"closed"`
}

type SeriesKey struct {
	Dataset   string `json:"dataset"`
	Symbol    string `json:"symbol"`
	Timeframe string `json:"timeframe"`
}

func (k SeriesKey) String() string {
	return k.Dataset + "|" + k.Symbol + "|" + k.Timeframe
}
