CREATE TABLE IF NOT EXISTS signal_market_states (
    event_id text NOT NULL UNIQUE,
    dataset text NOT NULL,
    symbol text NOT NULL,
    timeframe text NOT NULL,
    bar_open_time timestamptz NOT NULL,
    bar_revision bigint NOT NULL DEFAULT 0,
    algorithm text NOT NULL,
    algorithm_version text NOT NULL,
    config_hash text NOT NULL,
    state jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset, symbol, timeframe, algorithm, algorithm_version, config_hash)
);

ALTER TABLE signal_event_outbox
    ADD COLUMN IF NOT EXISTS event_type text NOT NULL DEFAULT 'indicator.point';

CREATE INDEX IF NOT EXISTS signal_event_outbox_type_cursor_idx
    ON signal_event_outbox (event_type, cursor);