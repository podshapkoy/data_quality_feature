from datetime import datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import re
from zoneinfo import ZoneInfo

from .db import connection

NY = ZoneInfo("America/New_York")
MONTHLY_TLC_FILE = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})")


def _number(value):
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _pickup(value: str):
    return datetime.fromisoformat(value).replace(tzinfo=NY).astimezone(ZoneInfo("UTC"))


def _outside_source_month(source_file: str, pickup: datetime, inferred_month: str | None = None) -> bool:
    match = MONTHLY_TLC_FILE.search(source_file)
    expected_month = (f"{match.group(1)}-{match.group(2)}" if match else inferred_month)
    if not expected_month:
        return False
    local_pickup = pickup.astimezone(NY)
    return local_pickup.strftime("%Y-%m") != expected_month


def validate_and_clean(run_id: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    with connection() as conn, conn.cursor() as cur:
        cur.execute("""
            WITH monthly AS (
              SELECT source_file,
                     substring(payload->>'tpep_pickup_datetime' FROM '^[0-9]{4}-[0-9]{2}') AS year_month,
                     count(*) AS month_rows
              FROM raw.trip_records
              WHERE payload->>'tpep_pickup_datetime' ~ '^[0-9]{4}-[0-9]{2}'
              GROUP BY 1,2
            ), ranked AS (
              SELECT *, sum(month_rows) OVER (PARTITION BY source_file) AS total_rows,
                     row_number() OVER (PARTITION BY source_file ORDER BY month_rows DESC) AS rank
              FROM monthly
            )
            SELECT source_file, year_month FROM ranked
            WHERE rank=1 AND month_rows::numeric / total_rows >= 0.95
        """)
        inferred_months = {row["source_file"]: row["year_month"] for row in cur.fetchall()}
        clean_sql = (
            "INSERT INTO clean.trips(trip_id,pickup_at,dropoff_at,duration_minutes,passenger_count,trip_distance,"
            "pickup_zone_id,dropoff_zone_id,total_amount,source_file) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (trip_id) DO NOTHING"
        )
        quarantine_sql = (
            "INSERT INTO clean.quarantined_trips(raw_id,reasons) VALUES (%s,%s) "
            "ON CONFLICT (raw_id) DO UPDATE SET reasons=excluded.reasons, quarantined_at=now()"
        )
        with conn.cursor(name="raw_trip_stream") as reader:
            reader.itersize = 5000
            reader.execute("SELECT raw_id, source_file, payload FROM raw.trip_records ORDER BY raw_id")
            processed = 0
            while batch := reader.fetchmany(5000):
                clean_batch, quarantine_batch, remove_trip_ids = [], [], []
                for row in batch:
                    payload, reasons = row["payload"], []
                    try:
                        pickup = _pickup(payload["tpep_pickup_datetime"])
                        dropoff = _pickup(payload["tpep_dropoff_datetime"])
                    except (KeyError, ValueError, TypeError):
                        reasons.append("invalid_or_missing_datetime")
                        pickup = dropoff = None
                    if pickup and _outside_source_month(
                        row["source_file"], pickup, inferred_months.get(row["source_file"])
                    ):
                        reasons.append("outside_source_month")
                    if pickup and dropoff and dropoff <= pickup:
                        reasons.append("non_positive_duration")
                    try:
                        distance = _number(payload.get("trip_distance"))
                        if distance is not None and distance < 0:
                            reasons.append("negative_distance")
                        passengers = _number(payload.get("passenger_count"))
                        amount = _number(payload.get("total_amount"))
                    except InvalidOperation:
                        distance = None
                        passengers = amount = None
                        reasons.append("invalid_numeric_value")
                    duration = (None if pickup is None or dropoff is None else
                                Decimal(str((dropoff - pickup).total_seconds() / 60)))
                    if not payload.get("PULocationID"):
                        reasons.append("missing_pickup_zone")
                    try:
                        pickup_zone = int(payload["PULocationID"])
                        dropoff_zone = int(payload["DOLocationID"]) if payload.get("DOLocationID") else None
                    except (ValueError, TypeError, KeyError):
                        pickup_zone = dropoff_zone = None
                        reasons.append("invalid_zone_id")
                    if reasons:
                        for reason in set(reasons):
                            counts[reason] = counts.get(reason, 0) + 1
                        quarantine_batch.append((row["raw_id"], reasons))
                        if "outside_source_month" in reasons and pickup and dropoff:
                            key = "|".join((payload.get("VendorID", ""),
                                            payload["tpep_pickup_datetime"],
                                            payload["tpep_dropoff_datetime"],
                                            payload.get("PULocationID", ""),
                                            payload.get("DOLocationID", "")))
                            remove_trip_ids.append(sha256(key.encode()).hexdigest())
                        continue

                    key = "|".join((payload.get("VendorID", ""), payload["tpep_pickup_datetime"],
                                    payload["tpep_dropoff_datetime"], payload.get("PULocationID", ""),
                                    payload.get("DOLocationID", "")))
                    clean_batch.append((sha256(key.encode()).hexdigest(), pickup, dropoff, duration,
                                        passengers, distance, pickup_zone, dropoff_zone, amount,
                                        row["source_file"]))
                if clean_batch:
                    cur.executemany(clean_sql, clean_batch)
                if quarantine_batch:
                    cur.executemany(quarantine_sql, quarantine_batch)
                if remove_trip_ids:
                    cur.execute("DELETE FROM clean.trips WHERE trip_id = ANY(%s)", (remove_trip_ids,))
                processed += len(batch)
                if processed % 100_000 < len(batch):
                    print(f"Validated {processed:,} raw trip rows", flush=True)
        cur.execute("""SELECT coalesce(sum(n - 1), 0) AS duplicate_rows
                       FROM (SELECT count(*) AS n FROM raw.trip_records
                             GROUP BY payload->>'VendorID',
                                      payload->>'tpep_pickup_datetime',
                                      payload->>'tpep_dropoff_datetime',
                                      payload->>'PULocationID', payload->>'DOLocationID'
                             HAVING count(*) > 1) duplicates""")
        counts["duplicate_trip"] = cur.fetchone()["duplicate_rows"]
    with connection() as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO quality.check_results(run_id,source_name,check_name,failed_rows,details) "
            "VALUES (%s,'trip_records',%s,%s,'{}'::jsonb)",
            [(run_id, name, counts.get(name, 0)) for name in (
                "duplicate_trip", "invalid_or_missing_datetime", "outside_source_month",
                "non_positive_duration", "negative_distance", "invalid_numeric_value",
                "missing_pickup_zone", "invalid_zone_id")],
        )
    return counts


def build_features() -> int:
    query = """
    INSERT INTO mart.hourly_demand_features
      (hour_start,pickup_zone_id,trip_count,mean_duration_minutes,temperature_2m,precipitation,
       hour_of_day,day_of_week,is_weekend,demand_lag_1h,demand_lag_24h)
    WITH monthly_counts AS (
      SELECT source_file,
             date_trunc('month', pickup_at AT TIME ZONE 'America/New_York') AS month_start,
             count(*) AS month_rows
      FROM clean.trips GROUP BY 1,2
    ), ranked_months AS (
      SELECT *, sum(month_rows) OVER (PARTITION BY source_file) AS total_rows,
             row_number() OVER (PARTITION BY source_file ORDER BY month_rows DESC) AS rank
      FROM monthly_counts
    ), month_mode AS (
      SELECT source_file, month_start, month_rows::numeric / total_rows AS month_share
      FROM ranked_months WHERE rank=1
    ), observed AS (
      SELECT date_trunc('hour', pickup_at) AS hour_start, pickup_zone_id,
             count(*)::bigint AS trip_count, avg(duration_minutes) AS mean_duration_minutes
      FROM clean.trips t
      JOIN month_mode m USING (source_file)
      WHERE (source_file ~ 'yellow_tripdata_[0-9]{4}-[0-9]{2}'
             AND to_char(pickup_at AT TIME ZONE 'America/New_York', 'YYYY-MM') =
                 substring(source_file FROM 'yellow_tripdata_([0-9]{4}-[0-9]{2})'))
         OR (source_file !~ 'yellow_tripdata_[0-9]{4}-[0-9]{2}'
             AND (m.month_share < 0.95 OR
                  date_trunc('month', pickup_at AT TIME ZONE 'America/New_York') = m.month_start))
      GROUP BY 1,2
    ), bounds AS (
      SELECT min(hour_start) AS first_hour, max(hour_start) AS last_hour FROM observed
    ), zones AS (
      SELECT DISTINCT pickup_zone_id FROM observed
    ), hourly AS (
      SELECT hours.hour_start, zones.pickup_zone_id, coalesce(observed.trip_count, 0)::bigint AS trip_count,
             observed.mean_duration_minutes
      FROM bounds CROSS JOIN LATERAL generate_series(first_hour,last_hour,interval '1 hour') AS hours(hour_start)
      CROSS JOIN zones
      LEFT JOIN observed USING (hour_start,pickup_zone_id)
    ), enriched AS (
      SELECT h.*, w.temperature_2m, w.precipitation,
             extract(hour FROM h.hour_start AT TIME ZONE 'America/New_York')::smallint AS hour_of_day,
             extract(dow FROM h.hour_start AT TIME ZONE 'America/New_York')::smallint AS day_of_week
      FROM hourly h LEFT JOIN raw.weather_hourly w ON w.observed_at = h.hour_start
    )
    SELECT hour_start, pickup_zone_id, trip_count, mean_duration_minutes, temperature_2m, precipitation,
           hour_of_day, day_of_week, day_of_week IN (0,6),
           lag(trip_count,1) OVER (PARTITION BY pickup_zone_id ORDER BY hour_start),
           lag(trip_count,24) OVER (PARTITION BY pickup_zone_id ORDER BY hour_start)
    FROM enriched
    ON CONFLICT (hour_start,pickup_zone_id) DO UPDATE SET
      trip_count=excluded.trip_count, mean_duration_minutes=excluded.mean_duration_minutes,
      temperature_2m=excluded.temperature_2m, precipitation=excluded.precipitation,
      hour_of_day=excluded.hour_of_day, day_of_week=excluded.day_of_week,
      is_weekend=excluded.is_weekend, demand_lag_1h=excluded.demand_lag_1h,
      demand_lag_24h=excluded.demand_lag_24h, calculated_at=now()
    """
    with connection() as conn, conn.cursor() as cur:
        # The mart is derived data. Rebuild it atomically so corrected filters also
        # remove stale rows from a previous, overly broad date range.
        cur.execute("TRUNCATE TABLE mart.hourly_demand_features")
        cur.execute(query)
        return cur.rowcount


def check_features(run_id: str) -> dict[str, int]:
    with connection() as conn, conn.cursor() as cur:
        cur.execute("""SELECT count(*) AS rows,
                            count(*) FILTER (WHERE trip_count < 0) AS negative_demand,
                            count(*) FILTER (WHERE demand_lag_1h < 0 OR demand_lag_24h < 0) AS negative_lag
                     FROM mart.hourly_demand_features""")
        result = dict(cur.fetchone())
        cur.executemany(
            "INSERT INTO quality.check_results(run_id,source_name,check_name,failed_rows,details) "
            "VALUES (%s,'hourly_demand_features',%s,%s,%s::jsonb)",
            [(run_id, "feature_table_non_empty", int(result["rows"] == 0), '{"row_count": %d}' % result["rows"]),
             (run_id, "non_negative_demand", result["negative_demand"], "{}"),
             (run_id, "non_negative_lags", result["negative_lag"], "{}")],
        )
    if result["rows"] == 0 or result["negative_demand"] or result["negative_lag"]:
        raise ValueError(f"Feature mart checks failed: {result}")
    return result
