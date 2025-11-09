"""
リアルタイムデータ取得DAG

CoinGecko APIから現在のBTC価格情報を取得してS3にアップロード
"""

import requests
import os
from datetime import datetime, timedelta
import pytz
import pandas as pd
import base64
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import BytesIO

# タイムゾーン設定（東京時間）
JST = pytz.timezone("Asia/Tokyo")

# デフォルト引数
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def get_realtime_data():
    """
    CoinGecko APIからBTCの現在の価格情報を取得

    Returns:
        dict: BTC価格情報を含む辞書
    """
    # CoinGecko APIエンドポイント
    url = "https://api.coingecko.com/api/v3/simple/price"

    # パラメータ設定
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd,eur,jpy",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
        "include_last_updated_at": "true",
    }

    # APIキーが設定されている場合はヘッダーに追加
    api_key = os.getenv("COINGECKO_API_KEY", "")
    headers = {}
    if api_key:
        headers["x-cg-demo-api-key"] = api_key

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()

        data = response.json()

        if "bitcoin" not in data:
            raise ValueError("BTCデータが取得できませんでした")

        btc_data = data["bitcoin"]

        # タイムスタンプを追加（JSTタイムゾーン）
        jst_now = datetime.now(JST)
        result = {
            "timestamp": jst_now.isoformat(),
            "last_updated": btc_data.get("last_updated_at", ""),
            "usd_price": btc_data.get("usd", 0),
            "eur_price": btc_data.get("eur", 0),
            "jpy_price": btc_data.get("jpy", 0),
            "usd_market_cap": btc_data.get("usd_market_cap", 0),
            "usd_24h_vol": btc_data.get("usd_24h_vol", 0),
            "usd_24h_change": btc_data.get("usd_24h_change", 0),
        }

        print(f"BTC価格情報を取得しました: {result}")

        return result

    except requests.exceptions.RequestException as e:
        print(f"APIリクエストエラー: {e}")
        raise
    except Exception as e:
        print(f"データ取得エラー: {e}")
        raise


def transform_data(**context):
    """
    データを整形してParquet形式に変換

    Returns:
        dict: Parquetデータ（base64エンコード）とタイムスタンプを含む辞書
    """
    # 前のタスクからデータを取得
    ti = context["ti"]
    btc_data = ti.xcom_pull(task_ids="get_realtime_data")

    if not btc_data:
        raise ValueError("BTCデータが取得できませんでした")

    # DataFrameに変換
    df = pd.DataFrame([btc_data])

    # データ型の変換
    numeric_columns = [
        "usd_price",
        "eur_price",
        "jpy_price",
        "usd_market_cap",
        "usd_24h_vol",
        "usd_24h_change",
    ]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Parquet形式に変換
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_data = parquet_buffer.getvalue()

    # XComに保存するためにbase64エンコード
    parquet_data_base64 = base64.b64encode(parquet_data).decode("utf-8")

    print(f"データを整形しました。Parquetサイズ: {len(parquet_data)} bytes")

    return {
        "parquet_data_base64": parquet_data_base64,
        "timestamp": btc_data["timestamp"],
    }


def upload_to_s3(**context):
    """
    データをS3にアップロード
    """
    # 前のタスクからデータを取得
    ti = context["ti"]
    transformed_data = ti.xcom_pull(task_ids="transform_data")

    if not transformed_data:
        raise ValueError("変換済みデータが取得できませんでした")

    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    timestamp = transformed_data["timestamp"]

    # タイムスタンプをパース
    try:
        if timestamp.endswith("Z"):
            timestamp = timestamp.replace("Z", "+00:00")
        timestamp_dt = datetime.fromisoformat(timestamp)
        if timestamp_dt.tzinfo is None:
            timestamp_dt = JST.localize(timestamp_dt)
        else:
            timestamp_dt = timestamp_dt.astimezone(JST)
    except Exception as e:
        print(f"タイムスタンプのパースエラー: {e}, 現在時刻を使用します")
        timestamp_dt = datetime.now(JST)

    date_str = timestamp_dt.strftime("%Y-%m-%d")
    timestamp_str = timestamp_dt.strftime("%Y-%m-%dT%H-%M-%S")

    # S3Hookを使用してS3に接続
    s3_hook = S3Hook(aws_conn_id="aws_default")

    # base64エンコードされたデータをデコード
    parquet_data = base64.b64decode(transformed_data["parquet_data_base64"])

    # 1. 通常の保存先（日付別フォルダ）
    parquet_key = f"btc-prices/raw/{date_str}/btc_price_{timestamp_str}.parquet"
    s3_hook.load_bytes(
        bytes_data=parquet_data,
        key=parquet_key,
        bucket_name=bucket_name,
        replace=True,
    )
    print(
        f"ParquetファイルをS3にアップロードしました: s3://{bucket_name}/{parquet_key}"
    )

    # 2. リアルタイム用最新データ（常に上書き）
    latest_key = "btc-prices/realtime/latest/btc_latest.parquet"
    s3_hook.load_bytes(
        bytes_data=parquet_data,
        key=latest_key,
        bucket_name=bucket_name,
        replace=True,
    )
    print(f"最新データを更新しました: s3://{bucket_name}/{latest_key}")

    # 3. バッファ（過去60分分を保持）
    hour_str = timestamp_dt.strftime("%H")
    minute_str = timestamp_dt.strftime("%M")
    buffer_key = f"btc-prices/realtime/buffer/{date_str}/{hour_str}-{minute_str}/btc_price_{timestamp_str}.parquet"
    s3_hook.load_bytes(
        bytes_data=parquet_data,
        key=buffer_key,
        bucket_name=bucket_name,
        replace=True,
    )

    # 古いバッファデータを削除
    try:
        cutoff_time = timestamp_dt - timedelta(hours=1)
        cutoff_date_str = cutoff_time.strftime("%Y-%m-%d")

        # 古い日付のバッファを削除
        if cutoff_date_str < date_str:
            old_buffer_prefix = f"btc-prices/realtime/buffer/{cutoff_date_str}/"
            old_files = s3_hook.list_keys(
                bucket_name=bucket_name, prefix=old_buffer_prefix
            )
            if old_files:
                s3_hook.delete_objects(bucket=bucket_name, keys=old_files)
                print(f"古いバッファデータを削除しました: {len(old_files)} ファイル")

        # 同じ日付内で60分以上前のデータを削除
        if date_str == cutoff_date_str:
            old_buffer_prefix = f"btc-prices/realtime/buffer/{date_str}/"
            all_buffer_files = s3_hook.list_keys(
                bucket_name=bucket_name, prefix=old_buffer_prefix
            )
            for file_key in all_buffer_files:
                try:
                    filename = file_key.split("/")[-1]
                    if "btc_price_" in filename:
                        timestamp_str_file = filename.replace("btc_price_", "").replace(
                            ".parquet", ""
                        )
                        file_timestamp = datetime.strptime(
                            timestamp_str_file, "%Y-%m-%dT%H-%M-%S"
                        )
                        file_timestamp = JST.localize(file_timestamp)
                        if file_timestamp < cutoff_time:
                            s3_hook.delete_objects(bucket=bucket_name, keys=[file_key])
                except (ValueError, IndexError, KeyError) as parse_error:
                    print(f"ファイル名パースエラー（無視）: {parse_error}")
                    continue
    except Exception as e:
        print(f"バッファクリーンアップエラー（無視）: {e}")

    return {
        "parquet_key": parquet_key,
        "latest_key": latest_key,
        "buffer_key": buffer_key,
        "bucket_name": bucket_name,
    }


# DAG定義
dag = DAG(
    "coin_gecko_btc_to_s3",
    default_args=default_args,
    description="CoinGecko APIからBTC価格情報を取得してS3にアップロード（1分ごと）",
    schedule_interval="*/1 * * * *",  # 毎分実行
    start_date=datetime(2024, 1, 1, tzinfo=JST),
    catchup=False,  # 過去データは取得できないためFalse（CoinGecko APIの制限）
    tags=["cryptocurrency", "coingecko", "s3"],
)

# タスク定義
fetch_task = PythonOperator(
    task_id="get_realtime_data",
    python_callable=get_realtime_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)

upload_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    dag=dag,
)

# タスクの依存関係を設定
fetch_task >> transform_task >> upload_task
