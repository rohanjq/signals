package monitor

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

type Metrics struct {
	startedAt         time.Time
	upstreamConnected atomic.Bool
	outboxHealthy     atomic.Bool
	lastInputUnixNano atomic.Int64
	seedMessages      atomic.Uint64
	closedMessages    atomic.Uint64
	formingMessages   atomic.Uint64
	reconnects        atomic.Uint64
	inputErrors       atomic.Uint64
	outboxErrors      atomic.Uint64
}

func NewMetrics() *Metrics { return &Metrics{startedAt: time.Now()} }

func (m *Metrics) SetConnected(value bool) { m.upstreamConnected.Store(value) }

func (m *Metrics) Connected() bool { return m.upstreamConnected.Load() }

func (m *Metrics) SetOutboxHealthy(value bool) { m.outboxHealthy.Store(value) }

func (m *Metrics) OutboxHealthy() bool { return m.outboxHealthy.Load() }

func (m *Metrics) RecordOutboxError() { m.outboxErrors.Add(1) }

func (m *Metrics) RecordInput(kind string) {
	m.lastInputUnixNano.Store(time.Now().UnixNano())
	switch kind {
	case "seed":
		m.seedMessages.Add(1)
	case "closed":
		m.closedMessages.Add(1)
	case "forming":
		m.formingMessages.Add(1)
	}
}

func (m *Metrics) RecordReconnect() { m.reconnects.Add(1) }

func (m *Metrics) RecordInputError() { m.inputErrors.Add(1) }

func (m *Metrics) WritePrometheus(writer io.Writer, ready bool, laneCount, readyLaneCount int) {
	connected := 0
	if m.Connected() {
		connected = 1
	}
	readyValue := 0
	if ready {
		readyValue = 1
	}
	outboxHealthy := 0
	if m.OutboxHealthy() {
		outboxHealthy = 1
	}
	lastInput := float64(m.lastInputUnixNano.Load()) / 1e9
	_, _ = fmt.Fprintf(writer, "# TYPE signald_up gauge\nsignald_up 1\n")
	_, _ = fmt.Fprintf(writer, "# TYPE signald_ready gauge\nsignald_ready %d\n", readyValue)
	_, _ = fmt.Fprintf(writer, "# TYPE signald_upstream_connected gauge\nsignald_upstream_connected %d\n", connected)
	_, _ = fmt.Fprintf(writer, "# TYPE signald_outbox_healthy gauge\nsignald_outbox_healthy %d\n", outboxHealthy)
	_, _ = fmt.Fprintf(writer, "# TYPE signald_uptime_seconds gauge\nsignald_uptime_seconds %.3f\n", time.Since(m.startedAt).Seconds())
	_, _ = fmt.Fprintf(writer, "# TYPE signald_last_input_timestamp_seconds gauge\nsignald_last_input_timestamp_seconds %.6f\n", lastInput)
	_, _ = fmt.Fprintf(writer, "# TYPE signald_input_messages_total counter\nsignald_input_messages_total{type=\"seed\"} %d\n", m.seedMessages.Load())
	_, _ = fmt.Fprintf(writer, "signald_input_messages_total{type=\"closed\"} %d\n", m.closedMessages.Load())
	_, _ = fmt.Fprintf(writer, "signald_input_messages_total{type=\"forming\"} %d\n", m.formingMessages.Load())
	_, _ = fmt.Fprintf(writer, "# TYPE signald_upstream_reconnects_total counter\nsignald_upstream_reconnects_total %d\n", m.reconnects.Load())
	_, _ = fmt.Fprintf(writer, "# TYPE signald_input_errors_total counter\nsignald_input_errors_total %d\n", m.inputErrors.Load())
	_, _ = fmt.Fprintf(writer, "# TYPE signald_outbox_errors_total counter\nsignald_outbox_errors_total %d\n", m.outboxErrors.Load())
	_, _ = fmt.Fprintf(writer, "# TYPE signald_lanes gauge\nsignald_lanes %d\n", laneCount)
	_, _ = fmt.Fprintf(writer, "# TYPE signald_ready_lanes gauge\nsignald_ready_lanes %d\n", readyLaneCount)
}
