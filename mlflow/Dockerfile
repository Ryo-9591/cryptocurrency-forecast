FROM ghcr.io/mlflow/mlflow:v2.9.2

# Install PostgreSQL driver for MLflow backend store
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir psycopg2-binary==2.9.9

COPY entrypoint.sh /mlflow-entrypoint.sh
RUN chmod +x /mlflow-entrypoint.sh

ENTRYPOINT ["/mlflow-entrypoint.sh"]

