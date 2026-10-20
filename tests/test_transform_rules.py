from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from taxi_pipeline.transform import NY, _number, _outside_source_month, _pickup


def test_tlc_local_timestamp_is_converted_to_utc():
    assert _pickup("2024-01-15 12:00:00") == datetime(
        2024, 1, 15, 17, 0, tzinfo=ZoneInfo("UTC")
    )


def test_number_preserves_missing_values_without_imputing():
    assert _number("") is None
    assert _number("2.75") == Decimal("2.75")


def test_timezone_is_explicitly_new_york():
    assert str(NY) == "America/New_York"


def test_monthly_source_outlier_is_detected_after_timezone_conversion():
    assert _outside_source_month(
        "yellow_tripdata_2024-01.parquet", _pickup("2003-01-01 00:00:00")
    )
    assert not _outside_source_month(
        "yellow_tripdata_2024-01.parquet", _pickup("2024-01-31 23:30:00")
    )
