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
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

