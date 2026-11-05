import os
import time

from .db import connection


def clickhouse_client():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "taxi"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "taxi_local_only"),
        database=os.getenv("CLICKHOUSE_DATABASE", "taxi"),
    )


def sync_trips(batch_size: int = 10_000) -> int:
    """перенос партиями очищенные поездки из PostgreSQL в ClickHouse"""
    client = clickhouse_client()
    client.command("TRUNCATE TABLE taxi.hourly_demand")
    client.command("TRUNCATE TABLE taxi.trips")
    inserted = 0
    try:
        with connection() as conn:
            with conn.cursor(name="clickhouse_trip_export") as cursor:
                cursor.itersize = batch_size
                cursor.execute("""SELECT trip_id, pickup_at, dropoff_at, duration_minutes,
                                  passenger_count, trip_distance, pickup_zone_id,
                                  dropoff_zone_id, total_amount, source_file
                                  FROM clean.trips ORDER BY pickup_at, trip_id""")
                while rows := cursor.fetchmany(batch_size):
                    client.insert("trips", [tuple(row.values()) for row in rows],
                                  column_names=["trip_id", "pickup_at", "dropoff_at",
                                                "duration_minutes", "passenger_count",
                                                "trip_distance", "pickup_zone_id",
                                                "dropoff_zone_id", "total_amount", "source_file"])
                    inserted += len(rows)
                    print(f"Copied {inserted:,} trips", flush=True)
    finally:
        client.close()
    return inserted


POSTGRES_QUERY = """SELECT date_trunc('hour', pickup_at) AS hour_start,
    pickup_zone_id, count(*) AS trip_count, avg(duration_minutes) AS mean_duration_minutes
    FROM clean.trips
    WHERE pickup_at >= TIMESTAMPTZ '2024-01-15 00:00:00+00'
      AND pickup_at < TIMESTAMPTZ '2024-01-16 00:00:00+00'
    GROUP BY 1, 2 ORDER BY 1, 2"""

CLICKHOUSE_QUERY = """SELECT toStartOfHour(pickup_at) AS hour_start,
    pickup_zone_id, count() AS trip_count, avg(duration_minutes) AS mean_duration_minutes
    FROM taxi.trips
    WHERE pickup_at >= toDateTime('2024-01-15 00:00:00', 'UTC')
      AND pickup_at < toDateTime('2024-01-16 00:00:00', 'UTC')
    GROUP BY hour_start, pickup_zone_id ORDER BY hour_start, pickup_zone_id"""

MART_QUERY = """SELECT hour_start, pickup_zone_id, countMerge(trip_count) AS trip_count,
    avgMerge(mean_duration_minutes) AS mean_duration_minutes
    FROM taxi.hourly_demand
    WHERE hour_start >= toDateTime('2024-01-15 00:00:00', 'UTC')
      AND hour_start < toDateTime('2024-01-16 00:00:00', 'UTC')
    GROUP BY hour_start, pickup_zone_id ORDER BY hour_start, pickup_zone_id"""


def run_clickhouse_benchmark(repeats: int = 5) -> None:
    """сравнение скорости одного и того же аналитического запроса в PostgreSQL и ClickHouse"""
    client = clickhouse_client()
    try:
        with connection() as pg:
            pg_rows = pg.execute(POSTGRES_QUERY).fetchall()
            ch_rows = client.query(CLICKHOUSE_QUERY).result_rows
            mart_rows = client.query(MART_QUERY).result_rows
            print(f"Result rows (PostgreSQL/ClickHouse/ClickHouse mart): "
                  f"{len(pg_rows)} / {len(ch_rows)} / {len(mart_rows)}")
            if len(pg_rows) != len(ch_rows) or len(ch_rows) != len(mart_rows):
                raise RuntimeError("run clickhouse-sync before benchmarking")
            timings: dict[str, list[float]] = {"postgres": [], "clickhouse": [], "clickhouse_mart": []}
            for iteration in range(repeats):
                paths = (("postgres", "clickhouse", "clickhouse_mart") if iteration % 2 == 0
                         else ("clickhouse_mart", "clickhouse", "postgres"))
                for path in paths:
                    started = time.perf_counter()
                    if path == "postgres":
                        pg.execute(POSTGRES_QUERY).fetchall()
                    else:
                        client.query(CLICKHOUSE_QUERY if path == "clickhouse" else MART_QUERY)
                    elapsed_ms = (time.perf_counter() - started) * 1000
                    timings[path].append(elapsed_ms)
                    print(f"{path} run {iteration + 1}/{repeats}: {elapsed_ms:.2f} ms")
        for path, samples in timings.items():
            print(f"{path}: average {sum(samples) / len(samples):.2f} ms")
    finally:
        client.close()
