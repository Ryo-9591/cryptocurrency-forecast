"""
CoinGeckoからBTCの相場情報を取得し、日次でS3に蓄積するDAG（シンプル版）。
基本方針：過去365日分を取得。前回取得分がある場合は、その続きのみを追加保存。
保存先は単一のParquetファイル（追記マージ）
"""

import os
from datetime import datetime, timedelta
from io import BytesIO
import pandas as pd
import pendulum
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
PARQUET_SINGLE_KEY = os.getenv(
    "DAILY_PARQUET_KEY", "btc-prices/daily/btc_prices.parquet"
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
            f"CoinGecko API error: {exc} (status_code={response.status_code}, body={response.text})"
        ) from exc

    data = response.json()
    if not isinstance(data, dict):
        raise ValueError("Unexpected CoinGecko response format")
    return data


def _prices_to_df(prices: list) -> pd.DataFrame:
    """prices配列のみをDataFrameに変換（価格のみ取得）"""
    if not prices:
        return pd.DataFrame(columns=["timestamp", "usd_price"])
    df = pd.DataFrame(prices, columns=["timestamp_ms", "usd_price"])
    df["timestamp"] = pd.to_datetime(
        df["timestamp_ms"], unit="ms", utc=True, errors="coerce"
    )
    df = df.dropna(subset=["timestamp"])
    df = df.drop_duplicates(subset="timestamp").sort_values("timestamp")
    return df[["timestamp", "usd_price"]]


def _load_last_synced_timestamp(s3_hook: S3Hook, bucket_name: str):
    state_key = "btc-prices/state/last_synced_timestamp.txt"
    if not s3_hook.check_for_key(key=state_key, bucket_name=bucket_name):
        return None
    content = s3_hook.read_key(key=state_key, bucket_name=bucket_name).strip()
    if not content:
        return None
    return pendulum.parse(content)


def _store_last_synced_timestamp(
    s3_hook: S3Hook, bucket_name: str, timestamp: pendulum.DateTime
) -> None:
    state_key = "btc-prices/state/last_synced_timestamp.txt"
    s3_hook.load_string(
        string_data=timestamp.to_iso8601_string(),
        key=state_key,
        bucket_name=bucket_name,
        replace=True,
    )


def sync_daily_data(**context) -> dict:
    """
    シンプルな同期タスク：
    - COINGECKO_API_KEY, S3_BUCKET_NAME を使用
    - 過去365日分を上限に、前回同期以降の差分のみ取得
    - 取得データ（価格のみ）をParquetでS3に保存
    - last_synced_timestampを更新
    """
    api_key = os.getenv("COINGECKO_API_KEY")
    if not api_key:
        raise ValueError("COINGECKO_API_KEY環境変数が設定されていません")

    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    s3_hook = S3Hook(aws_conn_id="aws_default")
    last_synced = _load_last_synced_timestamp(s3_hook, bucket_name)

    data_interval_end = context.get("data_interval_end")
    logical_date = context.get("logical_date")
    if data_interval_end:
        end_time_utc = data_interval_end.in_timezone("UTC")
    elif logical_date:
        end_time_utc = logical_date.in_timezone("UTC")
    else:
        end_time_utc = pendulum.now("UTC")
    earliest_allowed_utc = pendulum.now("UTC").subtract(days=365)

    # 365日以前しか取得しない（無料プラン制限）
    if end_time_utc <= earliest_allowed_utc:
        raise AirflowSkipException(
            "CoinGecko無料プランでは過去365日より前のデータは取得できないためスキップします。"
        )

    if last_synced:
        start_time_utc = max(last_synced.add(seconds=1), earliest_allowed_utc)
    else:
        start_time_utc = earliest_allowed_utc

    if start_time_utc >= end_time_utc:
        raise AirflowSkipException("取得済みのデータのためスキップします。")

    print(
        f"CoinGeckoからデータ取得: {start_time_utc.isoformat()} 〜 {end_time_utc.isoformat()} (UTC)"
    )

    raw_data = _fetch_market_chart_range(
        start_time_utc=start_time_utc, end_time_utc=end_time_utc, api_key=api_key
    )

    df = _prices_to_df(raw_data.get("prices", []))
    if df.empty:
        raise AirflowSkipException("CoinGeckoから価格データが取得できませんでした")

    mask = (df["timestamp"] >= start_time_utc) & (df["timestamp"] <= end_time_utc)
    df = df[mask]

    if df.empty:
        raise AirflowSkipException("指定期間内のデータが存在しませんでした")

    # メタ列付与
    df = df.copy()
    df["timestamp_jst"] = df["timestamp"].dt.tz_convert(JST)
    df["retrieved_at"] = datetime.now(UTC)

    # 既存の単一Parquetを読み込み（存在すれば）
    existing_df = pd.DataFrame()
    if s3_hook.check_for_key(key=PARQUET_SINGLE_KEY, bucket_name=bucket_name):
        obj = s3_hook.get_key(key=PARQUET_SINGLE_KEY, bucket_name=bucket_name)
        existing_bytes = obj.get()["Body"].read()
        try:
            loaded = pd.read_parquet(BytesIO(existing_bytes))
            if "timestamp" in loaded.columns:
                loaded["timestamp"] = pd.to_datetime(
                    loaded["timestamp"], utc=True, errors="coerce"
                )
            existing_df = loaded
        except Exception:
            # 壊れた場合は無視して新規として扱う
            existing_df = pd.DataFrame()

    # 既存＋差分をマージし、重複削除・期間を365日に丸める
    combined = pd.concat([existing_df, df], ignore_index=True)
    if "timestamp" in combined.columns:
        combined["timestamp"] = pd.to_datetime(
            combined["timestamp"], utc=True, errors="coerce"
        )
    combined = combined.dropna(subset=["timestamp"])
    combined = combined.sort_values("timestamp").drop_duplicates(
        subset=["timestamp"], keep="last"
    )

    # 直近365日のみ保持（ファイル肥大化防止）
    cutoff_ts = pd.Timestamp.utcnow().tz_localize("UTC") - pd.Timedelta(days=365)
    combined = combined[combined["timestamp"] >= cutoff_ts]

    # S3へ単一ファイルとしてアップロード（上書き）
    parquet_key = PARQUET_SINGLE_KEY
    s3_hook.load_bytes(
        bytes_data=combined.to_parquet(index=False, engine="pyarrow"),
        key=parquet_key,
        bucket_name=bucket_name,
        replace=True,
    )

    latest_timestamp_utc = pendulum.instance(
        combined["timestamp"].max().to_pydatetime()
    )
    _store_last_synced_timestamp(s3_hook, bucket_name, latest_timestamp_utc)

    print(
        f"CoinGeckoデータをS3に保存しました: s3://{bucket_name}/{parquet_key} (records={len(combined)})"
    )
    return {
        "parquet_key": parquet_key,
        "bucket_name": bucket_name,
        "record_count": len(combined),
        "start_time_utc": start_time_utc.isoformat(),
        "end_time_utc": end_time_utc.isoformat(),
        "latest_timestamp_utc": latest_timestamp_utc.to_iso8601_string(),
    }


dag = DAG(
    "fetch_daily_data",
    default_args=default_args,
    description="CoinGeckoからBTCデータを取得し、日次でS3へ保存",
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=JST),
    catchup=True,
    tags=["cryptocurrency", "coingecko", "s3", "daily"],
)

sync_task = PythonOperator(
    task_id="sync_daily_data",
    python_callable=sync_daily_data,
    dag=dag,
)
