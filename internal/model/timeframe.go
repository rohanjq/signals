package model

import (
	"fmt"
	"strconv"
	"time"
)

func TimeframeDuration(value string) (time.Duration, error) {
	if value == "1d" {
		return 24 * time.Hour, nil
	}
	if value == "1w" {
		return 7 * 24 * time.Hour, nil
	}
	if len(value) < 2 {
		return 0, fmt.Errorf("invalid timeframe %q", value)
	}
	amount, err := strconv.Atoi(value[:len(value)-1])
	if err != nil || amount <= 0 {
		return 0, fmt.Errorf("invalid timeframe %q", value)
	}
	var unit time.Duration
	switch value[len(value)-1] {
	case 's':
		unit = time.Second
	case 'm':
		unit = time.Minute
	case 'h':
		unit = time.Hour
	default:
		return 0, fmt.Errorf("invalid timeframe %q", value)
	}
	return time.Duration(amount) * unit, nil
}
