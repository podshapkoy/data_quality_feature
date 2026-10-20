from datetime import date
import csv

from taxi_pipeline.ingest import trip_date_bounds


def test_tlc_monthly_file_uses_expected_calendar_month_without_scanning_rows():
    assert trip_date_bounds("yellow_tripdata_2024-01.parquet") == (
        date(2024, 1, 1),
        date(2024, 1, 31),
    )


def test_generic_monthly_file_ignores_few_timestamp_outliers(tmp_path):
    path = tmp_path / "yellow_tripdata.csv"
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["tpep_pickup_datetime"])
        writer.writeheader()
        writer.writerows([{"tpep_pickup_datetime": "2024-01-15 12:00:00"}] * 100)
        writer.writerows([{"tpep_pickup_datetime": "2009-05-15 12:00:00"}] * 4)
    assert trip_date_bounds(str(path)) == (date(2024, 1, 1), date(2024, 1, 31))
