# Changelog

## 2026-09-06 - Initial local release

- Add the Go `signald` service for authoritative EMA and market analysis from
  OHLC seed, forming, and closed-bar streams.
- Publish the strict Signals V1 REST, WebSocket, CloudEvents, and JSON Schema
  contract with generic numeric and structured output primitives.
- Separate durable bar revisions from analysis revisions and preserve revision
  ordering across service restarts.
- Persist canonical reducer snapshots, events, cursors, and outbox records
  atomically in PostgreSQL, with explicit in-memory development mode.
- Add bearer authentication, origin allowlisting, readiness and metrics
  endpoints, bounded per-series workers, and slow-client backpressure.
- Document consumer integration, deployment, production operations, snapshot
  replacement semantics, warm-up behavior, and reconnect/resume rules.
- Cover EMA, market analysis, durability, API authorization, WebSocket
  protocol, restart revisions, and serialization with unit and race tests.