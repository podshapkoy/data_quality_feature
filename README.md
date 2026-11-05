# NYC Taxi Demand Data Pipeline

Пайплайн загружает поездки NYC Yellow Taxi и историческую погоду Open-Meteo, проверяет и очищает данные, затем агрегирует спрос по часу и зоне. 
PostgreSQL хранит исходные данные и витрину признаков; 
ClickHouse - копию поездок и аналитическую витрину. Запуском управляет Airflow.

Витрина предназначена для прогноза числа посадок в следующем часу по зоне.
- В неё входят спрос, температура, осадки, календарные признаки и лаги спроса. 
- Погода загружается для одной точки в центре Манхэттена и используется для всех зон.

## Запуск

Из корневой папки проекта:

```bash
cp .env.example .env
docker compose up --build -d app airflow
docker compose exec app python -m taxi_pipeline.cli download-sample
```

Откройте [Airflow](http://localhost:8080), включите DAG `taxi_demand_pipeline` и нажмите **Trigger DAG**.

## Где находятся результаты

| Таблица | Содержимое |
|---|---|
| PostgreSQL `raw.trip_records` | Исходные поездки TLC |
| PostgreSQL `clean.trips` | Проверенные поездки |
| PostgreSQL `clean.quarantined_trips` | Строки, не прошедшие проверки |
| PostgreSQL `mart.hourly_demand_features` | Почасовой спрос, погода и признаки для модели |
| PostgreSQL `quality.check_results` | Результаты проверок качества |
| ClickHouse `taxi.trips` | Аналитическая копия проверенных поездок |
| ClickHouse `taxi.hourly_demand` | Почасовая агрегатная витрина |

## ClickHouse и замеры

После успешного запуска DAG выполните:

```bash
docker compose exec app python -m taxi_pipeline.cli clickhouse-sync
docker compose exec app python -m taxi_pipeline.cli clickhouse-benchmark
```

Данные хранятся в `taxi.trips` (`MergeTree`, месячные партиции), 
почасовая витрина — в `taxi.hourly_demand` (`AggregatingMergeTree`). Materialized View обновляет витрину при загрузке поездок.

| Запрос | Среднее время |
|---|---:|
| PostgreSQL | 65,07 мс |
| ClickHouse, таблица поездок | 16,45 мс |
| ClickHouse, агрегатная витрина | 13,22 мс |

## Остановка и очистка

Остановить контейнеры, сохранив данные:

```bash
docker compose down
```

Запустить снова:

```bash
docker compose up -d
```

Удалить контейнеры и данные PostgreSQL и ClickHouse:

```bash
docker compose down -v
```
