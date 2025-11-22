FROM apache/airflow:2.9.0

USER root

# 必要なシステムパッケージをインストール
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Pythonパッケージをインストール
COPY ml_airflow/requirements.txt /requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip pip install -r /requirements.txt

# DAGs
COPY ml_airflow/dags /opt/airflow/dags

# Config
COPY ml_airflow/config /opt/airflow/config

# Utils (Shared + ML specific)
# まず共有コードをコピー
COPY shared /opt/airflow/utils
# 次にML固有のutilsを上書き/マージ
COPY ml_airflow/utils /opt/airflow/utils
