"""Сравнение планов запросов и каждого результата EXPLAIN в PostgreSQL"""

from .db import connection
from psycopg.types.json import Jsonb


BENCHMARK_SQL = """
SELECT date_trunc('hour', pickup_at), pickup_zone_id, count(*)
FROM clean.trips
WHERE pickup_at >= TIMESTAMPTZ '2024-01-15 00:00:00+00'
  AND pickup_at < TIMESTAMPTZ '2024-01-16 00:00:00+00'
GROUP BY 1, 2
"""


def run_query_benchmark(repeats: int = 3) -> None:
    with connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS quality.query_benchmarks (
                benchmark_id BIGSERIAL PRIMARY KEY,
                run_id UUID NOT NULL,
                variant TEXT NOT NULL CHECK (variant IN ('before', 'after')),
                iteration INTEGER NOT NULL,
                query_text TEXT NOT NULL,
                plan JSONB NOT NULL,
                execution_time_ms DOUBLE PRECISION NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)
        run_id = conn.execute("SELECT gen_random_uuid() AS id").fetchone()["id"]

        for variant in ("before", "after"):
            for iteration in range(1, repeats + 1):
                # Emulate the planner state before the pickup-time index. The index
                # remains intact; the after run uses PostgreSQL's normal planner.
                if variant == "before":
                    conn.execute("SET LOCAL enable_indexscan = off")
                    conn.execute("SET LOCAL enable_bitmapscan = off")
                else:
                    conn.execute("SET LOCAL enable_indexscan = on")
                    conn.execute("SET LOCAL enable_bitmapscan = on")

                result = conn.execute(
                    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + BENCHMARK_SQL
                ).fetchone()
                plan = result["QUERY PLAN"][0]
                execution_time_ms = plan["Execution Time"]
                conn.execute(
                    """INSERT INTO quality.query_benchmarks
                       (run_id, variant, iteration, query_text, plan, execution_time_ms)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (run_id, variant, iteration, BENCHMARK_SQL, Jsonb(plan), execution_time_ms),
                )
                print(f"{variant} run {iteration}/{repeats}: {execution_time_ms:.2f} ms")

        rows = conn.execute(
            """SELECT variant, round(avg(execution_time_ms)::numeric, 2) AS avg_ms
               FROM quality.query_benchmarks WHERE run_id = %s
               GROUP BY variant ORDER BY variant""",
            (run_id,),
        ).fetchall()
        print(f"Benchmark run_id: {run_id}")
        for row in rows:
            print(f"{row['variant']}: average {row['avg_ms']} ms")
