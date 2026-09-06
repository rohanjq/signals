# Frontend integration: OHLC and Signals V1

The frontend consumes candles and canonical analysis from separate services:

| Service | Default endpoint | Responsibility |
|---|---|---|
| `ohlcd` | `ws://127.0.0.1:18081/ws` | Seed, forming, and closed OHLC bars |
| `signald` | `ws://127.0.0.1:8090/v1/ws` | Authoritative analysis outputs |

Join both streams on `dataset + symbol + timeframe + open_time`. An OHLC
`open_time` matches analysis `data.bar.open_time`. Never extend an old analysis
value onto a newer candle and never recompute canonical values in the browser.

The normative event, primitive, subscription, replay, and error behavior is in
[`protocol-v1.md`](protocol-v1.md). Validate integrations with:

- [`signals-v1.schema.json`](signals-v1.schema.json)
- [`websocket-v1.schema.json`](websocket-v1.schema.json)

## Startup

1. Connect to OHLC and request enough seed bars for the chart.
2. Connect to `/v1/ws` and send one `signals.v1` subscription.
3. Wait for `subscribed`.
4. Apply each `snapshot` and `history` item through the same event handler used
   for live events.
5. Upsert outputs by the identity rules in the protocol document.
6. Save a durable event cursor only after applying the event.
7. On reconnect, reseed OHLC independently and send the saved cursor as
   `resume_after` to Signals.

The reference implementation is:

- [`../../stream/demo/charts/signal-client.js`](../../stream/demo/charts/signal-client.js): transport, authentication, reconnect, and cursor persistence
- [`../../stream/demo/charts/analysis-consumer.js`](../../stream/demo/charts/analysis-consumer.js): generic event validation and lifecycle-aware state
- [`../../stream/demo/charts/renderer.js`](../../stream/demo/charts/renderer.js): dynamic numeric and primitive rendering

## Authentication

REST requests use `Authorization: Bearer <token>`. Browser WebSocket APIs cannot
set that header, so private operator clients encode it as a subprotocol:

```javascript
const bytes = new TextEncoder().encode(token);
let binary = "";
for (const byte of bytes) binary += String.fromCharCode(byte);
const encoded = btoa(binary)
  .replaceAll("+", "-")
  .replaceAll("/", "_")
  .replaceAll("=", "");
const socket = new WebSocket(
  "ws://127.0.0.1:8090/v1/ws",
  `bearer.${encoded}`,
);
```

Do not put the service token in a public bundle. A commercial multi-user
frontend must use an authenticated gateway or backend-for-frontend that checks
the user's allowed series, analyses, alert rules, and rate limits.

## Staleness

OHLC and Signals are independent connections. Continue rendering candles if
Signals is unavailable, but mark analysis stale when its newest
`data.bar.open_time` trails the newest candle. Do not shift, extrapolate, or
locally replace missing authoritative outputs.

Provisional outputs may change and have no replay cursor. Confirmed outputs are
immutable, durable, and replayable. Apply only a greater `analysis_revision`,
or a confirmed event replacing a provisional event at the same revision. Each
event is a complete output snapshot, so omitted outputs must be removed.
