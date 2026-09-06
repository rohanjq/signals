package indicator

import (
	"math"
	"testing"
)

func TestEMAUsesSMASeedThenRecurses(t *testing.T) {
	ema, err := NewEMA(3)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		close float64
		want  float64
		ready bool
	}{
		{close: 10, ready: false},
		{close: 20, ready: false},
		{close: 30, want: 20, ready: true},
		{close: 40, want: 30, ready: true},
	}
	for index, test := range tests {
		got, ready, err := ema.OnClosed(test.close)
		if err != nil {
			t.Fatalf("sample %d: %v", index, err)
		}
		if ready != test.ready {
			t.Fatalf("sample %d: ready = %v, want %v", index, ready, test.ready)
		}
		if ready && math.Abs(got-test.want) > 1e-12 {
			t.Fatalf("sample %d: value = %v, want %v", index, got, test.want)
		}
	}
}

func TestEMAProjectionDoesNotMutateCanonicalState(t *testing.T) {
	ema, _ := NewEMA(2)
	_, _, _ = ema.OnClosed(10)
	_, _, _ = ema.OnClosed(20)

	projected, ready, err := ema.Project(30)
	if err != nil || !ready || projected != 25 {
		t.Fatalf("projected = %v, ready = %v, err = %v", projected, ready, err)
	}
	canonical, _ := ema.Value()
	if canonical != 15 || ema.Samples() != 2 {
		t.Fatalf("projection mutated canonical state: value=%v samples=%d", canonical, ema.Samples())
	}
}

func TestEMASnapshotRoundTrip(t *testing.T) {
	original, _ := NewEMA(3)
	for _, close := range []float64{10, 20, 30, 40} {
		_, _, _ = original.OnClosed(close)
	}
	snapshot, err := original.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	restored, _ := NewEMA(3)
	if err := restored.Restore(snapshot); err != nil {
		t.Fatal(err)
	}
	want, _, _ := original.OnClosed(50)
	got, ready, err := restored.OnClosed(50)
	if err != nil || !ready || math.Abs(got-want) > 1e-12 {
		t.Fatalf("restored next value = %v, want %v, ready=%v, err=%v", got, want, ready, err)
	}
}

func TestEMARejectsInvalidInput(t *testing.T) {
	if _, err := NewEMA(0); err == nil {
		t.Fatal("expected invalid period error")
	}
	ema, _ := NewEMA(9)
	if _, _, err := ema.OnClosed(math.NaN()); err == nil {
		t.Fatal("expected non-finite close error")
	}
}
