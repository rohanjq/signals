package broker

import (
	"strings"
	"testing"
	"time"

	"github.com/rohanjq/signals/internal/model"
)

func TestLiveGatePublishesTransitionsImmediatelyAndSteadyStateOncePerSecond(t *testing.T) {
	stream := &JetStream{live: make(map[string]liveCursor)}
	start := time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC)
	event := model.SignalEvent{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m", BarOpenTime: start, Provisional: true,
		SourceBar: &model.Bar{OpenTime: start, Open: 100, Close: 101}}
	if publish, replaceable, _ := stream.liveDecision(event, start.Add(time.Second)); !publish || replaceable {
		t.Fatal("first forming frame must publish")
	}
	stream.live[liveKey(event)] = liveCursor{openTime: start, color: "green", lastPublished: start.Add(time.Second)}
	if publish, _, _ := stream.liveDecision(event, start.Add(1500*time.Millisecond)); publish {
		t.Fatal("unchanged frame inside throttle interval must be coalesced")
	}
	event.SourceBar.Close = 99
	if publish, replaceable, _ := stream.liveDecision(event, start.Add(1600*time.Millisecond)); !publish || replaceable {
		t.Fatal("colour transition must publish immediately")
	}
	stream.live[liveKey(event)] = liveCursor{openTime: start, color: "red", lastPublished: start.Add(1600 * time.Millisecond)}
	if publish, replaceable, _ := stream.liveDecision(event, start.Add(2600*time.Millisecond)); !publish || !replaceable {
		t.Fatal("steady state must publish after one second")
	}
}

func TestEvaluationSubjectSeparatesReplaceableSnapshotsPerSeries(t *testing.T) {
	event := model.SignalEvent{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"}
	snapshot := evaluationSubject(event, "snapshot")
	confirmed := evaluationSubject(event, "confirmed")
	if snapshot == confirmed || !strings.HasPrefix(snapshot, "signals.evaluation.142.snapshot.") || !strings.HasPrefix(confirmed, "signals.evaluation.142.confirmed.") {
		t.Fatalf("snapshot=%q confirmed=%q", snapshot, confirmed)
	}
}
