package indicator

import (
	"testing"
	"time"
)

func TestMarketSnapshotRestoreAndBoundedHistory(t *testing.T) {
	config := DefaultMarketConfig()
	config.RecentBars = 4
	config.FVGLookback = 4
	config.LiquidityLookback = 4
	reducer, err := NewMarket(config, 4)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1_700_000_000, 0).UTC()
	for index := range 6 {
		reducer.OnClosed(marketBar(start.Add(time.Duration(index)*time.Minute), 100, 102, 98, 101))
	}
	state, err := reducer.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := NewMarket(config, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(state); err != nil {
		t.Fatal(err)
	}
	if restored.Samples() != 6 || restored.State().AsOf != reducer.State().AsOf {
		t.Fatalf("restored samples/state = %d/%s, want %d/%s", restored.Samples(), restored.State().AsOf, reducer.Samples(), reducer.State().AsOf)
	}
}
