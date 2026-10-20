import csv
import calendar
from collections import Counter
import os
from datetime import datetime, timedelta
from pathlib import Path
import re

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .db import connection

TLC_MONTH_FILE = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})")


def iter_trip_rows(path: str):
    """Извлечение нормализованных словарей строк из файлов TLC CSV или Parquet"""
    file = Path(path)
    with file.open("rb") as probe:
        is_parquet = file.suffix.lower() == ".parquet" or probe.read(4) == b"PAR1"
    if is_parquet:
        import pyarrow.parquet as pq
        parquet = pq.ParquetFile(file)
        row_number = 2
        for batch in parquet.iter_batches(batch_size=10_000):
            columns = batch.to_pydict()
            names = list(columns)
            for values in zip(*(columns[name] for name in names), strict=True):
                yield row_number, {name: "" if value is None else str(value)
                                   for name, value in zip(names, values, strict=True)}
                row_number += 1
    else:
        with file.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames or "tpep_pickup_datetime" not in reader.fieldnames:
                raise ValueError("Expected a NYC TLC Yellow Taxi file")
            for row_number, row in enumerate(reader, start=2):
                yield row_number, row


def trip_date_bounds(path: str):
    """Возвращение месячного интервала, закодированного в имени файла TLC, или наблюдаемой границы CSV"""
    match = TLC_MONTH_FILE.search(Path(path).name)
    if match:
        year, month = map(int, match.groups())
        return datetime(year, month, 1).date(), datetime(
            year, month, calendar.monthrange(year, month)[1]
        ).date()
    first = last = None
    months = Counter()
    for _, row in iter_trip_rows(path):
        try:
            day = datetime.fromisoformat(row["tpep_pickup_datetime"]).date()
        except (ValueError, KeyError, TypeError):
            continue
        first = day if first is None else min(first, day)
        last = day if last is None else max(last, day)
        months[(day.year, day.month)] += 1
    if months and max(months.values()) / sum(months.values()) >= 0.95:
        year, month = months.most_common(1)[0][0]
        return datetime(year, month, 1).date(), datetime(
            year, month, calendar.monthrange(year, month)[1]
        ).date()
    return first, last


def ingest_weather_for_trips(path: str) -> int:
    """Загрузка данных о погоде за каждый месяц"""
    first, last = trip_date_bounds(path)
    if first is None or last is None:
        return 0

    inserted = 0
    month = first.replace(day=1)
    while month <= last:
        next_month = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_end = min(last, next_month - timedelta(days=1))
        inserted += ingest_weather(month.isoformat(), month_end.isoformat())
        month = next_month
    return inserted


def ingest_trips(path: str) -> int:
    """Добавление строк в формате TLC CSV/Parquet к исходному словарю"""
    from psycopg.types.json import Jsonb
    file = Path(path)
    inserted = 0
    with connection() as conn:
        with conn.cursor() as cur:
            for row_number, row in iter_trip_rows(path):
                cur.execute(
                    "INSERT INTO raw.trip_records(source_file, source_row, payload) VALUES (%s,%s,%s) "
                    "ON CONFLICT (source_file, source_row) DO NOTHING",
                    (file.name, row_number, Jsonb(row)),
                )
                inserted += cur.rowcount
    return inserted


def ingest_weather(start_date: str, end_date: str) -> int:
    """Загрузка архивных данных ежечасной погоды для заданной точки"""
    lat = float(os.getenv("WEATHER_LAT", "40.7580"))
    lon = float(os.getenv("WEATHER_LON", "-73.9855"))
    retry_policy = Retry(
        total=3, connect=3, read=3, status=3, backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(("GET",)),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry_policy))
    response = session.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={"latitude": lat, "longitude": lon, "start_date": start_date,
                "end_date": end_date,
                "hourly": "temperature_2m,precipitation,wind_speed_10m,weather_code",
                "timezone": "UTC"},
        timeout=(20, 90),
    )
    response.raise_for_status()
    hourly = response.json()["hourly"]
    rows = [(datetime.fromisoformat(ts), lat, lon, t, p, w, c)
            for ts, t, p, w, c in zip(hourly["time"], hourly["temperature_2m"],
                                        hourly["precipitation"], hourly["wind_speed_10m"],
                                        hourly["weather_code"], strict=True)]
    with connection() as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO raw.weather_hourly(observed_at,latitude,longitude,temperature_2m,precipitation,wind_speed_10m,weather_code) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (observed_at,latitude,longitude) DO UPDATE SET "
            "temperature_2m=excluded.temperature_2m, precipitation=excluded.precipitation, "
            "wind_speed_10m=excluded.wind_speed_10m, weather_code=excluded.weather_code, loaded_at=now()",
            rows,
        )
    return len(rows)
