from pathlib import Path


def test_feature_query_uses_only_prior_observations_for_lags():
    source = (Path(__file__).parents[1] / "src/taxi_pipeline/transform.py").read_text()
    assert "lag(trip_count,1) OVER (PARTITION BY pickup_zone_id ORDER BY hour_start)" in source
    assert "lag(trip_count,24) OVER (PARTITION BY pickup_zone_id ORDER BY hour_start)" in source
    assert "month_share < 0.95" in source
    assert "TRUNCATE TABLE mart.hourly_demand_features" in source
