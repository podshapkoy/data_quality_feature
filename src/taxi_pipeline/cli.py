import argparse
import os
import uuid

import requests

from .ingest import ingest_trips, ingest_weather_for_trips
from .transform import build_features, check_features, validate_and_clean
from .benchmark import run_query_benchmark


def download_sample(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    response = requests.get(url, timeout=180)
    response.raise_for_status()
    with open(path, "wb") as output:
        output.write(response.content)
    print(f"Downloaded {len(response.content):,} bytes to {path}")


def run_pipeline():
    path = os.getenv("TLC_CSV_PATH", "/app/data/raw/yellow_tripdata_2024-01.parquet")
    loaded = ingest_trips(path)
    print(f"New raw trip rows: {loaded}")
    weather_rows = ingest_weather_for_trips(path)
    print(f"Weather rows loaded or updated: {weather_rows}")
    run_id = str(uuid.uuid4())
    checks = validate_and_clean(run_id)
    features = build_features()
    result = check_features(run_id)
    print(f"Run {run_id}; quality errors={checks}; feature rows upserted={features}; checks={result}")


def main():
    parser = argparse.ArgumentParser(description="NYC taxi demand data pipeline")
    parser.add_argument("command", choices=("run", "download-sample", "benchmark"))
    args = parser.parse_args()
    if args.command == "run":
        run_pipeline()
    elif args.command == "benchmark":
        run_query_benchmark()
    else:
        download_sample(os.getenv("TLC_CSV_PATH", "/app/data/raw/yellow_tripdata_2024-01.parquet"))


if __name__ == "__main__":
    main()
