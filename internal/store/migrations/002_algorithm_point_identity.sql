DO $$
DECLARE
    existing_constraint text;
BEGIN
    SELECT constraint_name
    INTO existing_constraint
    FROM information_schema.table_constraints
    WHERE table_schema = current_schema()
      AND table_name = 'signal_indicator_points'
      AND constraint_type = 'UNIQUE'
    LIMIT 1;

    IF existing_constraint IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE signal_indicator_points DROP CONSTRAINT %I',
            existing_constraint
        );
    END IF;
END $$;

ALTER TABLE signal_indicator_points
    ADD CONSTRAINT signal_indicator_points_algorithm_unique
    UNIQUE (
        dataset,
        symbol,
        timeframe,
        bar_open_time,
        bar_revision,
        indicator,
        period,
        algorithm_version,
        config_hash
    );