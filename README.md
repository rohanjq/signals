# signald

`signald` is the server-side market intelligence service for this workspace. It
consumes the existing OHLC WebSocket contract and computes authoritative EMA
9, 21, 50, and 200 values plus market structure, FVGs, order blocks, liquidity,
candle patterns, key levels, ranges, and premium/discount state. Browsers
render snapshots and events; they do not recompute canonical values.

Frontend applications consume OHLC candles and signals through separate APIs.
See [`docs/frontend-integration.md`](docs/frontend-integration.md) for the
complete two-stream integration, authentication, reconnect, and chart-join
contract. The normative implementation-neutral contract and JSON Schemas are in
[`docs/protocol-v1.md`](docs/protocol-v1.md).

Each symbol/timeframe has one bounded actor. Live, replay, and seed bars use
the same deterministic reducers. Market analysis uses closed bars only and is
normalized into typed Signals V1 outputs at the public API boundary.

## Guarantees

- EMA is seeded with an SMA and emits no value before it is ready.
- Market state is evaluated from a bounded 500-bar closed history and restored from durable reducer snapshots.
- Forming-bar values are provisional projections and never mutate canonical state.
- A closed bar's points, cursor, snapshots, and outbox rows commit atomically.
- In-memory state advances only after the durable transaction commits.
- Event delivery is at least once; consumers deduplicate by `event_id`.
- WebSocket startup subscribes before snapshot/replay and removes cursor duplicates.
- One bounded mailbox per series applies backpressure instead of dropping bars.

## Run with Docker

Start `ohlcd` on port 8080 first. Then:

```bash
cd signals
cp .env.example .env
# Replace both credentials in .env.
docker compose up --build -d
curl http://127.0.0.1:8090/healthz
curl -H "Authorization: Bearer $SIGNALD_API_TOKEN" \
    http://127.0.0.1:8090/v1/catalog
```

PostgreSQL binds to `127.0.0.1:5433`; the API binds to
`127.0.0.1:8090`. Schema migration is automatic and serialized with a
PostgreSQL advisory lock. `/readyz` returns 200 only when PostgreSQL, OHLC, and
all configured reducer lanes are ready.

## Run locally

Use PostgreSQL from Compose while running the Go process on the host:

```bash
docker compose up -d postgres
export SIGNALD_API_TOKEN="$(openssl rand -hex 32)"
export SIGNALD_DATABASE_URL='postgres://signald:signald-local-only@127.0.0.1:5433/signald?sslmode=disable'
export SIGNALD_OHLC_WS_URL='ws://127.0.0.1:8080/ws'
make run
```

Volatile development mode is explicit:

```bash
SIGNALD_DATABASE_URL='' SIGNALD_ALLOW_MEMORY_STORE=true \
SIGNALD_INSECURE_NO_AUTH=true make run
```

Never use those two overrides in production.

## API

Public operations:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

Bearer-authenticated operations:

- `GET /v1/catalog`
- `GET /v1/snapshots?symbol=BTCUSDT&timeframe=1m`
- `GET /v1/events?resume_after=0&symbol=BTCUSDT&timeframe=1m&analysis=ema&limit=500`
- `GET /v1/ws`

WebSocket clients send one subscription after connecting:

```json
{
  "protocol": "signals.v1",
  "type": "subscribe",
  "request_id": "8cf6e2e4-...",
  "data": {
    "series": [{"dataset": "binance-spot", "symbol": "BTCUSDT", "timeframe": "1m"}],
    "analyses": [{"name": "*"}],
    "event_types": ["io.ytstack.signals.analysis.updated.v1"],
    "statuses": ["confirmed", "provisional"],
    "resume_after": "1234",
    "history": {"limit": 500}
  }
}
```

Native clients use `Authorization: Bearer <token>` on the upgrade request.
Browser clients encode the same token as a WebSocket subprotocol:

```js
const encoded = btoa(token).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
const socket = new WebSocket("ws://127.0.0.1:8090/v1/ws", `bearer.${encoded}`);
```

The server acknowledges with `subscribed`, then sends `snapshot`, `history`,
replayed `event`, and live `event` messages. Every phase carries the same
CloudEvents-compatible analysis schema. Cursors are opaque decimal strings.

Slow WebSocket clients are disconnected when their bounded buffer fills. They
must reconnect with the last durable cursor they processed; provisional events
are intentionally not replayed.

## Configuration

See [`.env.example`](.env.example). Production requires
`SIGNALD_API_TOKEN` and `SIGNALD_DATABASE_URL`; startup fails closed when either
is absent. `SIGNALD_SEED_BARS` must be at least the largest configured EMA
period. Keep PostgreSQL private, use a TLS database URL, terminate HTTPS/WSS at
the ingress, and restrict `SIGNALD_ALLOWED_ORIGINS` to actual consumers.

## Development checks

```bash
make test
make race
make vet
make build
```

The broader component boundaries and staged JetStream migration are documented
in [`../MARKET_INTELLIGENCE_DESIGN.md`](../MARKET_INTELLIGENCE_DESIGN.md).