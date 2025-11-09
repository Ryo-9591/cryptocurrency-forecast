"""
CoinGecko APIからBTC価格情報を取得してS3にアップロードするDAG

このDAGは以下の処理を実行します:
1. CoinGecko APIからBTCの価格情報を取得
2. データを整形（CSV/Parquet形式に変換）
3. Amazon S3にアップロード
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import pandas as pd
import os
from io import StringIO, BytesIO

# デフォルト引数
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# DAG定義
dag = DAG(
    "coin_gecko_btc_to_s3",
    default_args=default_args,
    description="CoinGecko APIからBTC価格情報を取得してS3にアップロード",
    schedule="@daily",  # 毎日実行（必要に応じて変更可能: '@hourly', '@weekly'など）
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cryptocurrency", "coingecko", "s3"],
)


def fetch_btc_price(**context):
    """
    CoinGecko APIからBTCの価格情報を取得

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

        # タイムスタンプを追加
        result = {
            "timestamp": datetime.now().isoformat(),
            "last_updated": btc_data.get("last_updated_at", ""),
            "usd_price": btc_data.get("usd", 0),
            "eur_price": btc_data.get("eur", 0),
            "jpy_price": btc_data.get("jpy", 0),
            "usd_market_cap": btc_data.get("usd_market_cap", 0),
            "usd_24h_vol": btc_data.get("usd_24h_vol", 0),
            "usd_24h_change": btc_data.get("usd_24h_change", 0),
        }

        print(f"BTC価格情報を取得しました: {result}")

        # XComに保存
        return result

    except requests.exceptions.RequestException as e:
        print(f"APIリクエストエラー: {e}")
        raise
    except Exception as e:
        print(f"データ取得エラー: {e}")
        raise


def transform_data(**context):
    """
    データを整形してCSVとParquet形式に変換

    Returns:
        tuple: (csv_data, parquet_data) のタプル
    """
    # 前のタスクからデータを取得
    ti = context["ti"]
    btc_data = ti.xcom_pull(task_ids="fetch_btc_price")

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

    # CSV形式に変換
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    # Parquet形式に変換
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_data = parquet_buffer.getvalue()

    print(
        f"データを整形しました。CSVサイズ: {len(csv_data)} bytes, Parquetサイズ: {len(parquet_data)} bytes"
    )

    # XComに保存（バイナリデータはbase64エンコードが必要な場合があるため、BytesIOを返す）
    return {
        "csv_data": csv_data,
        "parquet_data": parquet_data,
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

    # S3設定を取得
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    timestamp = transformed_data["timestamp"]
    date_str = datetime.fromisoformat(timestamp).strftime("%Y-%m-%d")

    # S3Hookを使用してS3に接続
    s3_hook = S3Hook(aws_conn_id="aws_default")

    # CSVファイルをアップロード
    csv_key = f"btc-prices/{date_str}/btc_price_{timestamp.replace(':', '-').split('.')[0]}.csv"
    s3_hook.load_string(
        string_data=transformed_data["csv_data"],
        key=csv_key,
        bucket_name=bucket_name,
        replace=True,
    )
    print(f"CSVファイルをS3にアップロードしました: s3://{bucket_name}/{csv_key}")

    # Parquetファイルをアップロード
    parquet_key = f"btc-prices/{date_str}/btc_price_{timestamp.replace(':', '-').split('.')[0]}.parquet"
    s3_hook.load_bytes(
        bytes_data=transformed_data["parquet_data"],
        key=parquet_key,
        bucket_name=bucket_name,
        replace=True,
    )
    print(
        f"ParquetファイルをS3にアップロードしました: s3://{bucket_name}/{parquet_key}"
    )

    return {"csv_key": csv_key, "parquet_key": parquet_key, "bucket_name": bucket_name}


# タスク定義
fetch_task = PythonOperator(
    task_id="fetch_btc_price",
    python_callable=fetch_btc_price,
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
