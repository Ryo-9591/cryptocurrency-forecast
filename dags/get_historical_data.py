"""
過去データ取得DAG

CoinGecko APIから過去365日分のBTC価格データを取得してS3にアップロード
"""

import requests
import os
from datetime import datetime, timedelta
import pytz
import time
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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def get_historical_data():
    """
    CoinGecko APIからBTCの過去価格データを取得

    CoinGecko APIの制限:
    - 無料プラン: 過去365日分のデータ
    - レート制限: 10-50 calls/minute（APIキーなしの場合）

    Returns:
        list: 過去のBTC価格情報のリスト
    """
    # CoinGecko APIエンドポイント（過去データ取得用）
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"

    # パラメータ設定（過去365日分のデータを取得）
    params = {
        "vs_currency": "usd",
        "days": "max",  # 最大期間（無料プランでは365日）
        "interval": "daily",  # 日次データ
    }

    # APIキーが設定されている場合はヘッダーに追加
    api_key = os.getenv("COINGECKO_API_KEY", "")
    headers = {}
    if api_key:
        headers["x-cg-demo-api-key"] = api_key

    try:
        print("CoinGecko APIから過去データを取得中...")
        response = requests.get(url, params=params, headers=headers, timeout=60)
        response.raise_for_status()

        data = response.json()

        # データ構造: {"prices": [[timestamp, price], ...], "market_caps": [...], "total_volumes": [...]}
        prices = data.get("prices", [])
        market_caps = data.get("market_caps", [])
        total_volumes = data.get("total_volumes", [])

        if not prices:
            raise ValueError("過去データが取得できませんでした")

        print(f"取得したデータポイント数: {len(prices)}")

        # データを整形
        historical_data = []
        for i, price_data in enumerate(prices):
            timestamp_ms = price_data[0]  # ミリ秒単位のタイムスタンプ
            timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=JST)

            # 市場データを取得（インデックスが一致する場合）
            market_cap = market_caps[i][1] if i < len(market_caps) else 0
            volume = total_volumes[i][1] if i < len(total_volumes) else 0

            # 前日の価格を計算（24時間変動率用）
            prev_price = prices[i - 1][1] if i > 0 else price_data[1]
            price_change_24h = (
                ((price_data[1] - prev_price) / prev_price * 100) if i > 0 else 0
            )

            result = {
                "timestamp": timestamp_dt.isoformat(),
                "date": timestamp_dt.strftime("%Y-%m-%d"),
                "usd_price": price_data[1],
                "usd_market_cap": market_cap,
                "usd_24h_vol": volume,
                "usd_24h_change": price_change_24h,
            }
            historical_data.append(result)

            # レート制限対策（APIキーがない場合）
            if not api_key and i % 10 == 0:
                time.sleep(1)  # 1秒待機

        print(f"データを整形しました。総レコード数: {len(historical_data)}")

        return historical_data

    except requests.exceptions.RequestException as e:
        print(f"APIリクエストエラー: {e}")
        raise
    except Exception as e:
        print(f"データ取得エラー: {e}")
        raise


def transform_data(**context):
    """
    過去データを整形してParquet形式に変換

    Returns:
        dict: Parquetデータ（base64エンコード）とメタデータを含む辞書
    """
    # 前のタスクからデータを取得
    ti = context["ti"]
    historical_data = ti.xcom_pull(task_ids="get_historical_data")

    if not historical_data:
        raise ValueError("過去データが取得できませんでした")

    # DataFrameに変換
    df = pd.DataFrame(historical_data)

    # データ型の変換
    numeric_columns = [
        "usd_price",
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
    print(f"データ期間: {df['date'].min()} から {df['date'].max()}")

    return {
        "parquet_data_base64": parquet_data_base64,
        "date_range": {
            "start": df["date"].min(),
            "end": df["date"].max(),
        },
        "record_count": len(df),
    }


def upload_to_s3(**context):
    """
    過去データをS3にアップロード
    """
    # 前のタスクからデータを取得
    ti = context["ti"]
    transformed_data = ti.xcom_pull(task_ids="transform_data")

    if not transformed_data:
        raise ValueError("変換済みデータが取得できませんでした")

    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    date_range = transformed_data["date_range"]

    parquet_key = f"btc-prices/historical/btc_historical_{date_range['start']}_to_{date_range['end']}.parquet"

    # S3Hookを使用してS3に接続
    s3_hook = S3Hook(aws_conn_id="aws_default")

    # base64エンコードされたデータをデコード
    parquet_data = base64.b64decode(transformed_data["parquet_data_base64"])

    # S3にアップロード
    s3_hook.load_bytes(
        bytes_data=parquet_data,
        key=parquet_key,
        bucket_name=bucket_name,
        replace=True,
    )

    print(f"過去データをS3にアップロードしました: s3://{bucket_name}/{parquet_key}")
    print(f"レコード数: {transformed_data['record_count']}")

    return {
        "parquet_key": parquet_key,
        "bucket_name": bucket_name,
        "date_range": date_range,
        "record_count": transformed_data["record_count"],
    }


# DAG定義
dag = DAG(
    "fetch_historical_btc_data",
    default_args=default_args,
    description="CoinGecko APIからBTCの過去価格データを取得してS3にアップロード（日次更新）",
    schedule_interval="0 1 * * *",  # 毎日午前1時（JST）に実行
    start_date=datetime(2024, 1, 1, tzinfo=JST),
    catchup=False,
    tags=["cryptocurrency", "coingecko", "s3", "historical"],
)

# タスク定義
fetch_task = PythonOperator(
    task_id="get_historical_data",
    python_callable=get_historical_data,
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
