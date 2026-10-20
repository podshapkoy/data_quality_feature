CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS quality;

CREATE TABLE IF NOT EXISTS raw.trip_records (
    raw_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_row BIGINT NOT NULL,
    payload JSONB NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_file, source_row)
);
CREATE INDEX IF NOT EXISTS ix_raw_trip_loaded ON raw.trip_records (loaded_at);

CREATE TABLE IF NOT EXISTS raw.weather_hourly (
    observed_at TIMESTAMPTZ NOT NULL,
    latitude NUMERIC(8,5) NOT NULL,
    longitude NUMERIC(8,5) NOT NULL,
    temperature_2m NUMERIC,
    precipitation NUMERIC,
    wind_speed_10m NUMERIC,
    weather_code INTEGER,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (observed_at, latitude, longitude)
);

CREATE TABLE IF NOT EXISTS clean.trips (
    trip_id TEXT PRIMARY KEY,
    pickup_at TIMESTAMPTZ NOT NULL,
    dropoff_at TIMESTAMPTZ NOT NULL,
    duration_minutes NUMERIC(12,3) NOT NULL,
    passenger_count NUMERIC,
    trip_distance NUMERIC,
    pickup_zone_id INTEGER,
    dropoff_zone_id INTEGER,
    total_amount NUMERIC,
    source_file TEXT NOT NULL,
    cleaned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (dropoff_at > pickup_at),
    CHECK (duration_minutes > 0),
    CHECK (trip_distance IS NULL OR trip_distance >= 0)
);
CREATE INDEX IF NOT EXISTS ix_clean_trips_pickup ON clean.trips (pickup_at);

CREATE TABLE IF NOT EXISTS clean.quarantined_trips (
    raw_id BIGINT PRIMARY KEY REFERENCES raw.trip_records(raw_id),
    reasons TEXT[] NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS quality.check_results (
    check_id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_name TEXT NOT NULL,
    check_name TEXT NOT NULL,
    failed_rows BIGINT NOT NULL CHECK (failed_rows >= 0),
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS ix_quality_run ON quality.check_results (run_id);

CREATE TABLE IF NOT EXISTS quality.query_benchmarks (
    benchmark_id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    variant TEXT NOT NULL CHECK (variant IN ('before', 'after')),
    iteration INTEGER NOT NULL,
    query_text TEXT NOT NULL,
    plan JSONB NOT NULL,
    execution_time_ms DOUBLE PRECISION NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mart.hourly_demand_features (
    hour_start TIMESTAMPTZ NOT NULL,
    pickup_zone_id INTEGER NOT NULL,
    trip_count BIGINT NOT NULL,
    mean_duration_minutes NUMERIC,
    temperature_2m NUMERIC,
    precipitation NUMERIC,
    hour_of_day SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    demand_lag_1h BIGINT,
    demand_lag_24h BIGINT,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (hour_start, pickup_zone_id)
);
