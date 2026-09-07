# Signals restart and corrected-seed recovery

## Incident

Signals persisted a durable cursor and reducer snapshots so an ordinary
restart could resume efficiently. That recovery path assumed historical OHLC
bars never changed. After OHLC repaired history, Signals repeatedly rejected
the authoritative seed with errors such as:

```text
OHLC seed conflicts with durable cursor; correction protocol required
```

Two related cases existed:

- A newly started lane could have an incomplete snapshot set or a cursor hash
  that no longer matched corrected OHLC.
- An already-running lane could reconnect after an OHLC restart and reject the
  corrected current bar forever.

Historical indicator points also retained old revisions. Because live trade
counts can produce a numerically higher revision than REST history, a stale
point could incorrectly win over a corrected rebuilt point in consumers.

## Permanent fix

OHLC seeds are now authoritative in both startup and live-reconnect paths.
When snapshot restoration or catch-up conflicts with the seed, the lane:

1. Creates fresh EMA and market reducers.
2. Replays all closed seed bars deterministically.
3. Atomically rebuilds the affected series in storage.
4. Replaces in-memory state only after persistence succeeds.

The PostgreSQL rebuild takes a per-series advisory transaction lock, deletes
stale reducer snapshots and indicator points for that series, writes the
rebuilt points/snapshots, and advances the cursor in the same transaction.
Indicator-point upserts use the full algorithm identity constraint. The memory
store implements the same replacement semantics for tests and local use.

The outbox is not destructively cleared. Rebuilt history is served from the
indicator-point table, while new live events continue through normal outbox
ordering.

## Restart contract

- A normal restart restores matching snapshots and catches up efficiently.
- A corrected or incompatible seed triggers an automatic deterministic rebuild
  rather than requiring database deletion.
- An OHLC invalidation closes the upstream WebSocket; Signals reconnects,
  reseeds, and rebuilds if necessary.
- Repeated restarts and repeated identical seeds are idempotent.
- Readiness becomes true only when store, upstream, outbox, and all configured
  indicator lanes are healthy.

## Verification

Production was verified with all four configured series (`1m`, `5m`, `15m`,
`1h`) and EMA periods `9,21,50,200`. A live invalidation caused one reconnect,
then `/readyz` returned all components healthy without a retry loop.

Regression coverage is in `internal/engine/lane_test.go` and
`internal/store`. Run:

```sh
go test ./...
```

