CREATE DATABASE IF NOT EXISTS taxi;

CREATE TABLE IF NOT EXISTS taxi.trips
(
    trip_id String,
    pickup_at DateTime64(6, 'UTC'),
    dropoff_at DateTime64(6, 'UTC'),
    duration_minutes Float64,
    passenger_count Nullable(Float64),
    trip_distance Nullable(Float64),
    pickup_zone_id Nullable(Int32),
    dropoff_zone_id Nullable(Int32),
    total_amount Nullable(Float64),
    source_file LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(pickup_at)
ORDER BY (pickup_at, ifNull(pickup_zone_id, 0), trip_id);

CREATE TABLE IF NOT EXISTS taxi.hourly_demand
(
    hour_start DateTime('UTC'),
    pickup_zone_id Int32,
    trip_count AggregateFunction(count),
    mean_duration_minutes AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(hour_start)
ORDER BY (hour_start, pickup_zone_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS taxi.hourly_demand_mv
TO taxi.hourly_demand
AS SELECT
    toStartOfHour(pickup_at) AS hour_start,
    assumeNotNull(pickup_zone_id) AS pickup_zone_id,
    countState() AS trip_count,
    avgState(duration_minutes) AS mean_duration_minutes
FROM taxi.trips
WHERE isNotNull(pickup_zone_id)
GROUP BY hour_start, pickup_zone_id;
