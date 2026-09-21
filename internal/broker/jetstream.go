package broker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rohanjq/signals/internal/api"
	"github.com/rohanjq/signals/internal/model"
)

const (
	FactStream           = "MARKET_FACTS"
	FactSubjects         = "signals.facts.>"
	EvaluationStream     = "MARKET_EVALUATION"
	EvaluationSubjects   = "signals.evaluation.>"
	EvaluationPartitions = 256
)

const livePublishInterval = time.Second

var ErrReplaceableEvaluation = errors.New("replaceable evaluation snapshot publish failed")

type liveCursor struct {
	openTime      time.Time
	color         string
	lastPublished time.Time
}

type JetStream struct {
	connection *nats.Conn
	context    nats.JetStreamContext
	liveMu     sync.Mutex
	live       map[string]liveCursor
}

func Open(ctx context.Context, url, token string, replicas int) (*JetStream, error) {
	if replicas < 1 {
		replicas = 1
	}
	nc, err := nats.Connect(url, nats.Name("signald-fact-publisher"), nats.Token(token), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second), nats.Timeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(4096))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("open JetStream: %w", err)
	}
	configs := []*nats.StreamConfig{
		{Name: FactStream, Subjects: []string{FactSubjects}, Retention: nats.LimitsPolicy, Storage: nats.FileStorage, Replicas: replicas, MaxAge: 7 * 24 * time.Hour, MaxMsgSize: 2 << 20, Duplicates: 10 * time.Minute},
		{Name: EvaluationStream, Subjects: []string{EvaluationSubjects}, Retention: nats.LimitsPolicy, Storage: nats.FileStorage, Replicas: replicas, MaxAge: 7 * 24 * time.Hour, MaxMsgSize: 2 << 20, Duplicates: 10 * time.Minute, AllowRollup: true},
	}
	for _, config := range configs {
		if info, inspectErr := js.StreamInfo(config.Name, nats.Context(ctx)); errors.Is(inspectErr, nats.ErrStreamNotFound) {
			if _, err = js.AddStream(config, nats.Context(ctx)); err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
				nc.Close()
				return nil, fmt.Errorf("ensure %s stream: %w", config.Name, err)
			}
		} else if inspectErr != nil {
			nc.Close()
			return nil, fmt.Errorf("inspect %s stream: %w", config.Name, inspectErr)
		} else if !sameStreamConfig(info.Config, *config) {
			if _, err = js.UpdateStream(config, nats.Context(ctx)); err != nil {
				nc.Close()
				return nil, fmt.Errorf("reconcile %s stream: %w", config.Name, err)
			}
		}
	}
	return &JetStream{connection: nc, context: js, live: make(map[string]liveCursor)}, nil
}

func (j *JetStream) Close() {
	j.connection.Drain()
	j.connection.Close()
}

func (j *JetStream) Publish(ctx context.Context, event model.SignalEvent) error {
	// The market event is the complete, atomic facts frame. Publishing the
	// individual EMA events too would let consumers count one candle repeatedly.
	if event.Provisional || event.EventType != "market.state" {
		return nil
	}
	payload, err := api.EncodeAnalysisEvent(event, time.Now())
	if err != nil {
		return fmt.Errorf("encode market facts: %w", err)
	}
	kind := "confirmed"
	if event.Reset {
		kind = "reset"
	}
	if err = j.publish(ctx, evaluationSubject(event, kind), payload, event.EventID, event.Cursor, false); err != nil {
		return fmt.Errorf("publish confirmed evaluation fact: %w", err)
	}
	if err = j.publish(ctx, factSubject(event), payload, event.EventID, event.Cursor, false); err != nil {
		return fmt.Errorf("publish market facts: %w", err)
	}
	return nil
}

// PublishEvaluation sends forming and confirmed frames through the one ordering
// domain consumed by Alerts. Forming updates are coalesced to one per second;
// new candles and colour transitions are lossless transition events and bypass
// the coalescer.
func (j *JetStream) PublishEvaluation(ctx context.Context, event model.SignalEvent) error {
	if event.EventType != "market.state" {
		return nil
	}
	if event.SourceBar == nil {
		return fmt.Errorf("publish evaluation fact: source bar is required")
	}
	now := time.Now().UTC()
	replaceable := false
	var next liveCursor
	if event.Provisional {
		var publish bool
		publish, replaceable, next = j.liveDecision(event, now)
		if !publish {
			return nil
		}
	}
	payload, err := api.EncodeAnalysisEvent(event, now)
	if err != nil {
		return fmt.Errorf("encode evaluation fact: %w", err)
	}
	kind := "confirmed"
	if event.Reset {
		kind = "reset"
	} else if event.Provisional && replaceable {
		kind = "snapshot"
	} else if event.Provisional {
		kind = "transition"
	}
	if err = j.publish(ctx, evaluationSubject(event, kind), payload, event.EventID, event.Cursor, replaceable); err != nil {
		if replaceable {
			return fmt.Errorf("%w: %v", ErrReplaceableEvaluation, err)
		}
		return fmt.Errorf("publish evaluation fact: %w", err)
	}
	if event.Provisional {
		j.liveMu.Lock()
		j.live[liveKey(event)] = next
		j.liveMu.Unlock()
	} else {
		j.liveMu.Lock()
		delete(j.live, liveKey(event))
		j.liveMu.Unlock()
	}
	return nil
}

func (j *JetStream) publish(ctx context.Context, subject string, payload []byte, eventID string, cursor int64, rollup bool) error {
	message := nats.NewMsg(subject)
	message.Data = payload
	message.Header.Set(nats.MsgIdHdr, eventID)
	message.Header.Set("Content-Type", "application/cloudevents+json")
	if cursor > 0 {
		message.Header.Set("X-Signals-Cursor", fmt.Sprint(cursor))
	}
	if rollup {
		message.Header.Set("Nats-Rollup", "sub")
	}
	_, err := j.context.PublishMsg(message, nats.Context(ctx))
	return err
}

func (j *JetStream) liveDecision(event model.SignalEvent, now time.Time) (publish, replaceable bool, next liveCursor) {
	bar := event.SourceBar
	color := "doji"
	if bar.Close > bar.Open {
		color = "green"
	} else if bar.Close < bar.Open {
		color = "red"
	}
	key := liveKey(event)
	j.liveMu.Lock()
	defer j.liveMu.Unlock()
	previous, exists := j.live[key]
	transition := !exists || !previous.openTime.Equal(event.BarOpenTime) || previous.color != color
	publish = transition || now.Sub(previous.lastPublished) >= livePublishInterval
	return publish, publish && !transition, liveCursor{openTime: event.BarOpenTime, color: color, lastPublished: now}
}

func liveKey(event model.SignalEvent) string {
	return event.Dataset + "\x00" + event.Symbol + "\x00" + event.Timeframe
}

func factSubject(event model.SignalEvent) string {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(event.Dataset + "\x00" + event.Symbol + "\x00" + event.Timeframe))
	return fmt.Sprintf("signals.facts.%02d", hash.Sum32()%64)
}

func evaluationSubject(event model.SignalEvent, kind string) string {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(liveKey(event)))
	seriesHash := sha256.Sum256([]byte(liveKey(event)))
	return fmt.Sprintf("signals.evaluation.%03d.%s.%x", hash.Sum32()%EvaluationPartitions, kind, seriesHash[:8])
}

func sameStreamConfig(got, want nats.StreamConfig) bool {
	return got.Name == want.Name && len(got.Subjects) == len(want.Subjects) && len(got.Subjects) == 1 && got.Subjects[0] == want.Subjects[0] &&
		got.Retention == want.Retention && got.Storage == want.Storage && got.Replicas == want.Replicas && got.MaxAge == want.MaxAge &&
		got.MaxMsgsPerSubject == want.MaxMsgsPerSubject && got.MaxMsgSize == want.MaxMsgSize && got.Duplicates == want.Duplicates && got.AllowRollup == want.AllowRollup
}
