# Signals Protocol V1

Signals V1 is the stable boundary between analysis producers and consumers. The
current producer is the Go `signald` service. A future TA-Lib, Node.js, or other
producer must emit this contract; consumers must not depend on native library
result formats.

Normative schemas:

- [`signals-v1.schema.json`](signals-v1.schema.json): analysis CloudEvent
- [`websocket-v1.schema.json`](websocket-v1.schema.json): subscription and delivery messages

## Domain boundary

Analysis events describe analytical facts. They do not contain orders, position
sizing, notification destinations, or buy/sell instructions. Strategy and rule
engines consume analysis events and may emit a separate event family such as
`io.ytstack.alert.triggered.v1`.

The core model is:

    Series -> Bar -> Analysis -> Outputs -> Semantic display hints

Display hints describe meaning such as `line`, `markers`, or `zones`; styling is
owned by each consumer.

## Authoritative snapshot semantics

Every `io.ytstack.signals.analysis.updated.v1` event is the complete
authoritative result for one logical analytical computation and revision. It is
never an implicit patch.

- `outputs` replaces the prior outputs for the same computation.
- `outputs: []` means the analysis currently has no outputs. It does not mean
  "unchanged".
- Removing an output from a newer revision deletes that output for the
  computation.
- Future incremental behavior requires a separate explicitly versioned event
  contract with patch/delete operations.

## Identity and revisions

The logical computation identity is:

    series.dataset
    + series.symbol
    + series.timeframe
    + bar.open_time
    + analysis.name
    + canonical(analysis.parameters)
    + implementation.config_hash

`analysis.name + canonical parameters + config_hash` identifies an analysis
configuration. `output.name` identifies one output within it. Stateful objects
inside points, markers, zones, and levels have stable `id` values.

`bar.revision` identifies the source OHLC revision. `analysis_revision`
identifies the analysis result revision. Both are non-negative and monotonic for
their respective identity. The current synchronous Go engine advances them
together, but consumers must treat them as separate fields. For the current OHLC
feed, a forming bar uses its durable accepted-trade count and confirmation uses
the next revision. This makes revisions deterministic across OHLC and Signals
restarts without depending on process-local counters.

For the same logical computation, a greater `analysis_revision` supersedes a
lower one. At the same revision, `confirmed` supersedes `provisional`; any other
duplicate or older event is ignored. The CloudEvent `id` is derived from the
logical computation, status, and analysis revision, so snapshot, history,
replay, and live delivery use the same ID for the same analytical occurrence.

## Analysis event

```json
{
  "specversion": "1.0",
  "id": "34a5...",
  "source": "urn:ytstack:signals:dataset:YmluYW5jZS1zcG90",
  "type": "io.ytstack.signals.analysis.updated.v1",
  "subject": "live/BTCUSDT/1m",
  "time": "2026-09-06T10:06:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:ytstack:signals:schema:analysis:v1",
  "correlationid": "deterministic-source-candle-id",
  "data": {
    "series": {
      "dataset": "binance-spot",
      "symbol": "BTCUSDT",
      "timeframe": "1m"
    },
    "bar": {
      "open_time": "2026-09-06T10:05:00Z",
      "close_time": "2026-09-06T10:06:00Z",
      "revision": 3,
      "status": "confirmed",
      "source_id": "deterministic-source-candle-id",
      "open": 107.5,
      "high": 109.0,
      "low": 107.0,
      "close": 108.4,
      "volume": 42.1,
      "trades": 2
    },
    "analysis": {
      "name": "bbands",
      "parameters": {
        "period": 20,
        "deviations": 2,
        "price_source": "close"
      }
    },
    "analysis_revision": 3,
    "outputs": [
      {"name": "upper", "type": "number", "unit": "price", "value": 111.2, "display": {"kind": "line", "pane": "price", "group": "bbands"}},
      {"name": "middle", "type": "number", "unit": "price", "value": 108.4, "display": {"kind": "line", "pane": "price", "group": "bbands"}},
      {"name": "lower", "type": "number", "unit": "price", "value": 105.6, "display": {"kind": "line", "pane": "price", "group": "bbands"}}
    ],
    "ready": true,
    "samples": 240,
    "required_samples": 20,
    "implementation": {
      "engine": "ta-lib",
      "engine_version": "0.6.4",
      "algorithm_version": "1",
      "config_hash": "sha256:..."
    }
  }
}
```

`time` is the bar close time for confirmed events and emission time for
provisional events. Confirmed events are durable and immutable. Provisional
events are replaceable and have no replay cursor.

`ready=false` means insufficient input history or upstream state exists for
normal consumption. `samples` reports available observations and optional
`required_samples` reports fixed warm-up requirements. The current EMA producer
emits no value while unready, so its `outputs` is empty.

## Output types

Every output has `name`, `type`, `value`, and `display`. `unit` is optional.
V1 supports exactly these generic types:

| Type | Value | Typical display |
|---|---|---|
| `number` | finite JSON number | `line`, `histogram`, `state` |
| `text` | string | `state`, `label` |
| `boolean` | boolean | `state` |
| `points` | point array | `polyline`, `points` |
| `markers` | point array | `markers` |
| `zones` | zone array | `zones` |
| `levels` | level array | `levels` |

New output names and display kinds may be added without changing V1. New value
shapes require a new contract version. Generic consumers retain or safely
ignore supported outputs whose display kind they do not render.

### Generic primitives

Points and markers require stable `id` and `time`; they may include `value`,
`label`, `direction`, `confirmed_at`, and `attributes`. Zones require stable
`id`, time bounds, and price bounds. Levels require stable `id` and `value`.

`direction` is one of `bullish`, `bearish`, or `neutral`. It communicates
analytical direction, never an instruction to buy or sell.

Generic zone lifecycle state is one of `pending`, `active`, `touched`, `mitigated`,
`invalidated`, or `expired`. Algorithm-specific state and metadata belong in
`attributes`. Fields needed for generic interpretation belong in the core
primitive. `attributes` may be ignored by generic consumers.

## WebSocket

Connect to `/v1/ws`. Browser clients may pass the bearer token using the
subprotocol described in the authentication section. The first application
message must be:

```json
{
  "protocol": "signals.v1",
  "type": "subscribe",
  "request_id": "1b88fb9c-...",
  "data": {
    "series": [
      {"dataset": "live", "symbol": "BTCUSDT", "timeframe": "1m"}
    ],
    "analyses": [{"name": "*"}],
    "event_types": ["io.ytstack.signals.analysis.updated.v1"],
    "statuses": ["confirmed", "provisional"],
    "resume_after": "184467",
    "history": {"limit": 1000}
  }
}
```

An empty dataset matches any configured dataset for the exact symbol/timeframe
pair. An analysis selector may pin `config_hash`; `{ "name": "*" }` selects all
analyses.

The server sends:

1. `subscribed` acknowledgement with subscription ID and delivery guarantee.
2. One `snapshot` batch per selected series.
3. One `history` batch.
4. Replayed `event` messages after `resume_after`.
5. Live `event` messages.

Every phase carries the same analysis event schema. Delivery is at least once.
`cursor` is an opaque decimal string present only on durable confirmed events.
Apply an event before persisting its cursor.

Errors use a typed envelope:

```json
{
  "protocol": "signals.v1",
  "type": "error",
  "request_id": "1b88fb9c-...",
  "error": {
    "code": "invalid_subscription",
    "message": "each series requires symbol and timeframe",
    "retryable": false
  }
}
```

## Consumer algorithm

1. Validate `protocol`, CloudEvent `type`, `datacontenttype`, and `dataschema`.
2. Build the logical computation identity using canonical parameter serialization.
3. Reject duplicate output names and unsupported V1 output types.
4. Ignore a lower `analysis_revision` for the same computation.
5. At equal revision, accept only a provisional-to-confirmed transition.
6. Replace the complete output set; delete outputs omitted by the replacement.
7. Upsert numeric points by output identity and `bar.open_time`.
8. Keep only the latest-bar snapshot for structured outputs.
9. Apply a durable event before saving its cursor and expect redelivery.
10. Expose staleness when analysis trails OHLC; never extend old values.

The reference implementation is
[`../../stream/demo/charts/analysis-consumer.js`](../../stream/demo/charts/analysis-consumer.js).

## REST and discovery

- `GET /v1/catalog`: output definitions, parameters, warm-up, lifecycle support, and implementation metadata.
- `GET /v1/snapshots?symbol=BTCUSDT&timeframe=1m`: current authoritative events.
- `GET /v1/events?dataset=...&symbol=...&timeframe=...&analysis=...&resume_after=...&limit=...`: durable event page.

REST responses use the same `signals.v1` envelope and opaque string cursor.

## Authentication and tracing

The current service token is suitable only for private operator deployment. A
commercial multi-user product must use an API gateway or backend-for-frontend
to authorize users, series, analyses, alert rules, and rate limits. Never ship
the service token in a public bundle.

`correlationid` and `bar.source_id` contain the deterministic candle identity
shared by every analysis of the same source revision. This allows downstream
assemblers and alert engines to reject mismatched facts without inventing
tracing ancestry.

The confirmed `smc.market_state` event is also the complete alert-fact frame:
it carries candle OHLC, current EMA outputs (`ema_<period>`), active zone state,
current-bar `pattern_occurrences`, and explicit `zone_transitions`. Consumers
must not join independent EMA messages to reconstruct a confirmed candle.
The production dispatcher publishes these frames to the JetStream
`MARKET_FACTS` stream only after they have been committed to the Signals
transactional outbox. The outbox row is marked published only after the broker
acknowledges durable storage.

Alert evaluators consume the single `MARKET_EVALUATION` stream through
`signals.evaluation.<000..255>.>`. Confirmed frames, provisional transitions,
and coalesced snapshots therefore share one authoritative sequence per logical
partition. Confirmed and transition subjects are retained for seven days.
Steady provisional snapshots are capped at one per second and published with a
per-series JetStream rollup header, so only the latest replaceable heartbeat is
kept while state transitions remain lossless.

An authoritative OHLC correction emits `data.reset=true` plus up to 500 prior
confirmed bars in `data.history`. The reset event time is the rebuild time (not
the corrected candle close). Downstream evaluators replace their future state
from that baseline but do not retract immutable alerts already delivered under
the documented live-as-known policy.

## Backend replacement rules

A replacement producer may use Go, Node.js, TA-Lib, `trading-signals`, or
another engine only if it:

1. Passes both JSON Schemas.
2. Preserves endpoint and WebSocket behavior.
3. Preserves snapshot, identity, revision, output, and primitive semantics.
4. Emits a new config hash or algorithm version when calculations change.
5. Passes golden-candle parity tests for intentionally compatible algorithms.
6. Never rewrites confirmed history under an existing analytical identity.

Payload compatibility does not imply numerical compatibility. Implementation
metadata makes intentional calculation changes explicit and auditable.
