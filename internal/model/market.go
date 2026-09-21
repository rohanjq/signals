package model

import "time"

type PriceZone struct {
	ID       string    `json:"id"`
	Kind     string    `json:"kind"`
	Side     string    `json:"side"`
	Grade    string    `json:"grade,omitempty"`
	FromTime time.Time `json:"from_time"`
	ToTime   time.Time `json:"to_time"`
	Top      float64   `json:"top"`
	Bottom   float64   `json:"bottom"`
	State    string    `json:"state,omitempty"`
}

type ZoneTransition struct {
	Zone       PriceZone `json:"zone"`
	Transition string    `json:"transition"`
	At         time.Time `json:"at"`
}

type Swing struct {
	Type        string    `json:"type"`
	Label       string    `json:"label,omitempty"`
	Price       float64   `json:"price"`
	Time        time.Time `json:"time"`
	ConfirmTime time.Time `json:"confirm_time"`
}

type StructureEvent struct {
	Kind  string     `json:"kind"`
	Side  string     `json:"side"`
	Price float64    `json:"price"`
	Time  time.Time  `json:"time"`
	Zone  *PriceZone `json:"zone,omitempty"`
}

type PriceLevel struct {
	Kind    string  `json:"kind"`
	Side    string  `json:"side"`
	Price   float64 `json:"price"`
	Touches int     `json:"touches"`
}

type PatternEvent struct {
	Kind string    `json:"kind"`
	Side string    `json:"side"`
	Time time.Time `json:"time"`
}

type PremiumDiscount struct {
	FromTime    time.Time `json:"from_time"`
	ToTime      time.Time `json:"to_time"`
	Trend       string    `json:"trend"`
	High        float64   `json:"high"`
	Low         float64   `json:"low"`
	Equilibrium float64   `json:"equilibrium"`
	POITop      float64   `json:"poi_top"`
	POIBottom   float64   `json:"poi_bottom"`
}

type MarketState struct {
	Schema             string           `json:"schema"`
	Algorithm          AlgorithmRef     `json:"algorithm"`
	AsOf               time.Time        `json:"as_of"`
	Trend              string           `json:"trend"`
	Swings             []Swing          `json:"swings"`
	Structure          []StructureEvent `json:"structure"`
	Ranges             []PriceZone      `json:"ranges"`
	KeyLevels          []PriceLevel     `json:"key_levels"`
	FVGs               []PriceZone      `json:"fvgs"`
	OrderBlocks        []PriceZone      `json:"order_blocks"`
	Liquidity          []PriceLevel     `json:"liquidity"`
	LiquiditySweeps    []PatternEvent   `json:"liquidity_sweeps"`
	Patterns           []PatternEvent   `json:"patterns"`
	PatternOccurrences []PatternEvent   `json:"pattern_occurrences"`
	ZoneTransitions    []ZoneTransition `json:"zone_transitions"`
	PremiumDiscount    *PremiumDiscount `json:"premium_discount,omitempty"`
}
