"""
CoinGeckoからBTCの相場情報を取得し、1時間ごとにS3へ保存するDAG
"""

import base64
import os
from datetime import datetime, timedelta
from io import BytesIO
from typing import Optional

import pandas as pd
import pytz
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

JST = pytz.timezone("Asia/Tokyo")
UTC = pytz.UTC

COINGECKO_API_BASE_URL = (
    "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _fetch_market_chart_range(
    start_time_utc: datetime, end_time_utc: datetime, api_key: str
) -> dict:
    params = {
        "vs_currency": "usd",
        "from": int(start_time_utc.timestamp()),
        "to": int(end_time_utc.timestamp()),
    }
    headers = {"accept": "application/json", "x-cg-demo-api-key": api_key}

    response = requests.get(
        COINGECKO_API_BASE_URL, params=params, headers=headers, timeout=30
    )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise ValueError(
            f"CoinGecko API error: {exc} "
            f"(status_code={response.status_code}, body={response.text})"
        ) from exc

    data = response.json()
    if not isinstance(data, dict):
        raise ValueError("Unexpected CoinGecko response format")
    return data


def _series_to_df(series: list, value_column: str) -> Optional[pd.DataFrame]:
    if not series:
        return None
    df = pd.DataFrame(series, columns=["timestamp_ms", value_column])
    df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df = df.drop_duplicates(subset="timestamp")
    df = df.set_index("timestamp")
    return df[[value_column]]


def fetch_hourly_data(**context) -> dict:
    api_key = os.getenv("COINGECKO_API_KEY")
    if not api_key:
        raise ValueError("COINGECKO_API_KEY環境変数が設定されていません")

    logical_date = context.get("logical_date")
    if logical_date:
        end_time_jst = logical_date.in_timezone(JST)
    else:
        end_time_jst = datetime.now(JST)
    start_time_jst = end_time_jst - timedelta(hours=1)

    start_time_utc = start_time_jst.astimezone(UTC)
    end_time_utc = end_time_jst.astimezone(UTC)

    print(
        f"CoinGeckoからデータ取得: {start_time_utc.isoformat()} 〜 "
        f"{end_time_utc.isoformat()} (UTC)"
    )

    raw_data = _fetch_market_chart_range(
        start_time_utc=start_time_utc, end_time_utc=end_time_utc, api_key=api_key
    )

    price_df = _series_to_df(raw_data.get("prices", []), "usd_price")
    volume_df = _series_to_df(raw_data.get("total_volumes", []), "usd_24h_vol")
    market_cap_df = _series_to_df(raw_data.get("market_caps", []), "usd_market_cap")

    if price_df is None:
        raise AirflowSkipException("CoinGeckoから価格データが取得できませんでした")

    df = price_df
    for extra_df in (market_cap_df, volume_df):
        if extra_df is not None:
            df = df.join(extra_df, how="outer")

    df = df.reset_index().rename(columns={"index": "timestamp"})
    df = df.sort_values("timestamp")

    mask = (df["timestamp"] >= start_time_utc) & (df["timestamp"] <= end_time_utc)
    df = df[mask]

    if df.empty:
        raise AirflowSkipException("指定期間内のデータが存在しませんでした")

    df["timestamp_jst"] = df["timestamp"].dt.tz_convert(JST)
    df["retrieved_at"] = datetime.now(UTC)

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_data = parquet_buffer.getvalue()
    parquet_data_base64 = base64.b64encode(parquet_data).decode("utf-8")

    print(f"取得データ: {len(df)} レコード, Parquetサイズ: {len(parquet_data)} bytes")

    return {
        "parquet_data_base64": parquet_data_base64,
        "record_count": len(df),
        "start_time_utc": start_time_utc.isoformat(),
        "end_time_utc": end_time_utc.isoformat(),
    }


def upload_to_s3(**context) -> dict:
    ti = context["ti"]
    fetched_data = ti.xcom_pull(task_ids="fetch_hourly_data")

    if not fetched_data:
        raise ValueError("取得データがXComから取得できませんでした")

    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    start_time = datetime.fromisoformat(fetched_data["start_time_utc"])
    end_time = datetime.fromisoformat(fetched_data["end_time_utc"])

    start_time_jst = start_time.astimezone(JST)
    end_time_jst = end_time.astimezone(JST)

    year = start_time_jst.strftime("%Y")
    month = start_time_jst.strftime("%m")
    day = start_time_jst.strftime("%d")
    hour = start_time_jst.strftime("%H")

    parquet_key = (
        "btc-prices/hourly/"
        f"year={year}/month={month}/day={day}/hour={hour}/"
        f"btc_prices_{start_time_jst.strftime('%Y-%m-%dT%H-%M-%S')}"
        f"_to_{end_time_jst.strftime('%Y-%m-%dT%H-%M-%S')}.parquet"
    )

    parquet_data = base64.b64decode(fetched_data["parquet_data_base64"])

    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_hook.load_bytes(
        bytes_data=parquet_data,
        key=parquet_key,
        bucket_name=bucket_name,
        replace=True,
    )

    print(
        f"CoinGeckoデータをS3に保存しました: s3://{bucket_name}/{parquet_key} "
        f"(records={fetched_data['record_count']})"
    )

    return {
        "parquet_key": parquet_key,
        "bucket_name": bucket_name,
        "record_count": fetched_data["record_count"],
        "start_time_utc": fetched_data["start_time_utc"],
        "end_time_utc": fetched_data["end_time_utc"],
    }


dag = DAG(
    "fetch_hourly_data",
    default_args=default_args,
    description="CoinGeckoからBTCデータを取得し、1時間ごとにS3へ保存",
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=JST),
    catchup=True,
    tags=["cryptocurrency", "coingecko", "s3", "hourly"],
)

fetch_task = PythonOperator(
    task_id="fetch_hourly_data",
    python_callable=fetch_hourly_data,
    dag=dag,
)

upload_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    dag=dag,
)

fetch_task >> upload_task
