-- Целевые значения по часам/часовым поясам и безопасные для утечек лаги
-- PostgreSQL вычисляет оконные функции после агрегирования, поэтому лаги содержат только данные за предыдущие завершенные часы
WITH hourly AS (
    SELECT date_trunc('hour', pickup_at) AS hour_start,
           pickup_zone_id,
           count(*) AS trip_count,
           avg(duration_minutes) AS mean_duration_minutes
    FROM clean.trips
    GROUP BY 1, 2
)
SELECT *,
       lag(trip_count, 1) OVER (PARTITION BY pickup_zone_id ORDER BY hour_start) AS demand_lag_1h,
       lag(trip_count, 24) OVER (PARTITION BY pickup_zone_id ORDER BY hour_start) AS demand_lag_24h
FROM hourly;

-- Сравнение производительности: docker compose exec app python -m taxi_pipeline.cli benchmark
-- Результаты сохраняются в виде планов JSON в файле quality.query_benchmarks
-- Итоги последнего запуска:
SELECT run_id, variant, count(*) AS samples,
       round(avg(execution_time_ms)::numeric, 2) AS avg_execution_ms
FROM quality.query_benchmarks
GROUP BY run_id, variant
ORDER BY max(recorded_at) DESC, variant;

-- Проверка сохраненных планов для прогона:
SELECT variant, iteration, execution_time_ms, plan
FROM quality.query_benchmarks
WHERE run_id = '<run_id-from-summary>'::uuid
ORDER BY variant, iteration;
