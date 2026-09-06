CREATE TABLE IF NOT EXISTS signal_series_cursors (
    dataset text NOT NULL,
    symbol text NOT NULL,
    timeframe text NOT NULL,
    last_open_time timestamptz NOT NULL,
    last_bar_hash text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset, symbol, timeframe)
);

CREATE TABLE IF NOT EXISTS signal_indicator_points (
    event_id text PRIMARY KEY,
    dataset text NOT NULL,
    symbol text NOT NULL,
    timeframe text NOT NULL,
    bar_open_time timestamptz NOT NULL,
    bar_revision bigint NOT NULL DEFAULT 0,
    indicator text NOT NULL,
    period integer NOT NULL,
    value double precision NOT NULL,
    samples bigint NOT NULL,
    algorithm_version text NOT NULL,
    config_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (dataset, symbol, timeframe, bar_open_time, bar_revision, indicator, period)
);

CREATE INDEX IF NOT EXISTS signal_indicator_points_series_time_idx
    ON signal_indicator_points (dataset, symbol, timeframe, bar_open_time DESC);

CREATE TABLE IF NOT EXISTS signal_reducer_snapshots (
    dataset text NOT NULL,
    symbol text NOT NULL,
    timeframe text NOT NULL,
    indicator text NOT NULL,
    period integer NOT NULL,
    algorithm_version text NOT NULL,
    config_hash text NOT NULL,
    last_open_time timestamptz NOT NULL,
    state jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset, symbol, timeframe, indicator, period, algorithm_version, config_hash)
);

CREATE TABLE IF NOT EXISTS signal_event_outbox (
    cursor bigserial PRIMARY KEY,
    event_id text NOT NULL UNIQUE,
    dataset text NOT NULL,
    symbol text NOT NULL,
    timeframe text NOT NULL,
    period integer NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX IF NOT EXISTS signal_event_outbox_pending_idx
    ON signal_event_outbox (cursor) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS signal_schema_migrations (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO signal_schema_migrations (version) VALUES (1)
ON CONFLICT (version) DO NOTHING;