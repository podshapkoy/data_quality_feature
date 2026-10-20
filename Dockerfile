FROM apache/airflow:2.10.5-python3.11
USER airflow
WORKDIR /app
COPY --chown=airflow:root pyproject.toml README.md ./
COPY --chown=airflow:root src ./src
RUN pip install --no-cache-dir -e .
COPY --chown=airflow:root dags ./dags
ENV PYTHONPATH=/app/src
