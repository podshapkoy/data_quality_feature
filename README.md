# NYC Taxi Demand Data Pipeline

Пайплайн загружает поездки NYC Yellow Taxi и архивную погоду, проверяет и очищает данные, затем формирует таблицу для последующего моделирования спроса на такси. Он демонстрирует путь от открытых источников до аналитической витрины PostgreSQL и автоматизацию через Airflow.

## Что делает проект

```text
NYC TLC Parquet/CSV ──┐
                      ├─ загрузка ─ raw ─ проверка ─ clean + quarantine ─ hourly features
Open-Meteo Archive ───┘                                      └─ quality.check_results
```

- **Поездки:** NYC TLC Yellow Taxi Trip Record Data, локальный пример за январь 2024 года
- **Погода:** Open-Meteo Historical Weather API, почасовые архивные данные для одной точки в центре Манхэттена
- **Хранилище:** PostgreSQL 16; Airflow 2.10 управляет задачами; Docker Compose запускает сервисы
- Все временные метки в БД — `timestamptz` в UTC. Локальное время поездок TLC интерпретируется как `America/New_York`

## Что именно готовится для прогноза

Предполагаемая задача модели — предсказать количество посадок в такси в следующем часу отдельно для каждой зоны посадки Нью-Йорка. Например: «сколько поездок начнётся в зоне 161 с 10:00 до 11:00?». Целевая величина — число поездок, сгруппированных по часу посадки и `PULocationID` (зоне посадки). В таблице `mart.hourly_demand_features` это поле называется `trip_count`.

Для каждой исходной поездки TLC пайплайн использует время посадки и высадки, зоны посадки и высадки, идентификатор перевозчика, число пассажиров, расстояние и сумму поездки. Эти поля нужны для проверки записей, расчёта длительности, определения дубликатов и группировки поездок по часу и зоне. Сама модель не анализирует каждую поездку отдельно: для прогноза спроса записи агрегируются в почасовые количества.

Погодный API возвращает температуру, осадки, скорость ветра и погодный код. Сейчас в витрину признаков переносятся температура и осадки; остальные значения сохраняются в `raw.weather_hourly`, но пока не используются для модели. Погода нужна как возможное объяснение изменения спроса: например, модель впоследствии сможет проверить, связаны ли осадки с числом посадок.

Погода сейчас загружается для одной точки в центре Манхэттена и присоединяется ко всем зонам.

## Быстрый запуск через Airflow

Выполняйте команды по одной из корневой папки проекта.

1. Создание локального `.env` из шаблона:

   ```bash
   cp .env.example .env
   ```

2. Сбор образов и запуск PostgreSQL, приложение и Airflow:

   ```bash
   docker compose up --build -d app airflow
   ```

3. Скачивание примера TLC внутрь общего каталога `data/raw`:

   ```bash
   docker compose exec app python -m taxi_pipeline.cli download-sample
   ```
4. Откройте Airflow: [http://localhost:8080](http://localhost:8080). Войдите с `AIRFLOW_ADMIN_USER` и `AIRFLOW_ADMIN_PASSWORD` из `.env`. 
Найдите DAG `taxi_demand_pipeline`, включите его переключателем и нажмите `Trigger DAG`.

5. Откройте запуск DAG. Граф должен завершиться тёмно-зелёным цветом в таком порядке:

   ```text
   load_trip_csv → load_weather → validate_and_clean → calculate_features → check_feature_mart
   ```

## Где находятся результаты

### Основные таблицы

| Таблица | Содержимое |
|---|---|
| `raw.trip_records` | Исходные строки TLC в JSONB; повторная загрузка файла идемпотентна |
| `raw.weather_hourly` | Почасовая погода Open-Meteo |
| `clean.trips` | Проверенные поездки с нормализованными типами; естественные дубликаты не размножаются|
| `clean.quarantined_trips` | Строки, которые нельзя безопасно использовать, с кодами причин |
| `mart.hourly_demand_features` | Спрос по часу и зоне, погода, календарные признаки и лаги 1/24 часа|
| `quality.check_results` | История результатов проверок качества и результата витрины по `run_id` |
| `quality.query_benchmarks` | Планы запросов `EXPLAIN` и время каждого performance-замера |

### Проверка объёма данных

```bash
docker compose exec db psql -U taxi -d taxi -c "SELECT (SELECT count(*) FROM raw.trip_records) AS raw_trips, (SELECT count(*) FROM clean.trips) AS clean_trips, (SELECT count(*) FROM clean.quarantined_trips) AS quarantined_trips, (SELECT count(*) FROM mart.hourly_demand_features) AS feature_rows;"
```

Причины карантина:

```bash
docker compose exec db psql -U taxi -d taxi -c "SELECT reasons, count(*) FROM clean.quarantined_trips GROUP BY reasons ORDER BY count(*) DESC;"
```

Последние часы спроса в часовом поясе Нью-Йорка:

```bash
docker compose exec db psql -U taxi -d taxi -c "SELECT hour_start AT TIME ZONE 'America/New_York' AS ny_hour, pickup_zone_id, trip_count, temperature_2m, precipitation, demand_lag_1h, demand_lag_24h FROM mart.hourly_demand_features ORDER BY hour_start DESC LIMIT 10;"
```

Сохранённые проверки качества:

```bash
docker compose exec db psql -U taxi -d taxi -c "SELECT run_id, source_name, check_name, failed_rows, checked_at FROM quality.check_results ORDER BY checked_at DESC, check_name LIMIT 30;"
```

## Замер производительности SQL

После успешной загрузки данных выполните:

```bash
docker compose exec app python -m taxi_pipeline.cli benchmark
```

Команда трижды выполняет одинаковый запрос за 15 января 2024 года в двух режимах. `before` имитирует планировщик без индексных путей; `after` использует обычный планировщик и индекс `ix_clean_trips_pickup`. В БД остаются JSON планы и времена каждого выполнения. Это сравнение режимов планирования на текущей таблице, а не буквальный замер двух исторических копий базы.

Команда напечатает `run_id`. Подставьте его вместо UUID в запрос:

```bash
docker compose exec db psql -U taxi -d taxi -c "SELECT variant, iteration, round(execution_time_ms::numeric, 2) AS ms, plan->'Plan'->>'Node Type' AS plan_type, plan->'Plan'->'Plans'->0->>'Node Type' AS child_plan_type FROM quality.query_benchmarks WHERE run_id = 'ВСТАВЬТЕ-RUN-ID'::uuid ORDER BY variant, iteration;"
```

Среднее время для конкретного запуска:

```bash
docker compose exec db psql -U taxi -d taxi -c "SELECT variant, count(*) AS samples, round(avg(execution_time_ms)::numeric, 2) AS avg_ms FROM quality.query_benchmarks WHERE run_id = 'ВСТАВЬТЕ-RUN-ID'::uuid GROUP BY variant ORDER BY variant;"
```

### Результат замера

Запрос агрегирует поездки по часу и зоне посадки за 15 января 2024 года. На локальном запуске получены следующие результаты:

| Режим | План чтения | Повтор 1 | Повтор 2 | Повтор 3 | Среднее |
|---|---|---:|---:|---:|---:|
| `before` | `Gather` (параллельное чтение) | 336,90 мс | 128,71 мс | 99,17 мс | **188,26 мс** |
| `after` | `Index Scan` по `ix_clean_trips_pickup` | 71,56 мс | 46,21 мс | 42,67 мс | **53,48 мс** |

В этом запуске вариант `after` выполнился в среднем примерно в 3,5 раза быстрее. Результаты относятся к локальному окружению и зависят от состояния кэша и ресурсов машины. Режим `before` имитирует отсутствие индексных путей планировщика; это не сравнение двух отдельных баз данных.

## Остановка и очистка

Остановить контейнеры, сохранив данные БД:

```bash
docker compose down
```

Запустить снова:

```bash
docker compose up -d
```

Полное удаление контейнеров и тома PostgreSQL со всеми загруженными данными:

```bash
docker compose down -v
```
