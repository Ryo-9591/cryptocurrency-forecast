"""
日次データ取得DAG

S3から過去24時間分の時間単位データを読み込んで統合し、日次統計を計算してS3に保存
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import BytesIO
import pytz
import base64

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


def get_daily_data(**context):
    """
    S3から過去24時間分の時間単位データを読み込んで統合

    Args:
        context: Airflowコンテキスト

    Returns:
        pd.DataFrame: 統合されたデータフレーム
    """
    # S3設定を取得
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    # 実行日を取得
    execution_date = context.get("execution_date")
    if execution_date:
        if execution_date.tzinfo is None:
            target_date = JST.localize(execution_date)
        else:
            target_date = execution_date.astimezone(JST)
    else:
        # 手動実行の場合は前日
        target_date = datetime.now(JST) - timedelta(days=1)

    target_date_str = target_date.strftime("%Y-%m-%d")
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")

    print(f"統合対象日: {target_date_str}")

    # S3Hookを使用してS3に接続
    s3_hook = S3Hook(aws_conn_id="aws_default")

    all_dataframes = []

    try:
        # 時間単位データのファイルを取得
        files = s3_hook.list_keys(
            bucket_name=bucket_name,
            prefix=f"btc-prices/hourly/year={year}/month={month}/day={day}/",
        )

        for file_key in files:
            if not file_key.endswith(".parquet"):
                continue

            try:
                # ファイルを読み込む
                file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
                parquet_data = file_obj.get()["Body"].read()

                df = pd.read_parquet(BytesIO(parquet_data))
                all_dataframes.append(df)
                print(f"  読み込み: {file_key} ({len(df)} レコード)")
            except Exception as e:
                print(f"  ファイル処理エラー ({file_key}): {e}")
                continue

    except Exception as e:
        print(f"時間単位データ取得エラー: {e}")
        # フォールバック: リアルタイムデータから統合
        print("リアルタイムデータから統合を試みます...")
        raw_prefix = f"btc-prices/raw/{target_date_str}/"
        try:
            raw_files = s3_hook.list_keys(bucket_name=bucket_name, prefix=raw_prefix)
            for file_key in raw_files:
                if not file_key.endswith(".parquet"):
                    continue
                try:
                    file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
                    parquet_data = file_obj.get()["Body"].read()
                    df = pd.read_parquet(BytesIO(parquet_data))
                    all_dataframes.append(df)
                except Exception:
                    continue
        except Exception:
            pass

    if not all_dataframes:
        print("統合するデータが見つかりませんでした")
        return None

    # データを統合
    consolidated_df = pd.concat(all_dataframes, ignore_index=True)

    # タイムスタンプでソート
    consolidated_df["timestamp"] = pd.to_datetime(consolidated_df["timestamp"])
    consolidated_df = consolidated_df.sort_values("timestamp").reset_index(drop=True)

    return consolidated_df


def transform_data(**context):
    """
    日次データを統計計算してParquet形式に変換
    """
    # データを取得
    consolidated_df = get_daily_data(**context)

    if consolidated_df is None:
        raise ValueError("統合するデータが見つかりませんでした")

    # タイムスタンプでソート
    consolidated_df["timestamp"] = pd.to_datetime(consolidated_df["timestamp"])
    consolidated_df = consolidated_df.sort_values("timestamp").reset_index(drop=True)

    # 実行日を取得
    execution_date = context.get("execution_date")
    if execution_date:
        if execution_date.tzinfo is None:
            target_date = JST.localize(execution_date)
        else:
            target_date = execution_date.astimezone(JST)
    else:
        target_date = datetime.now(JST) - timedelta(days=1)

    target_date_str = target_date.strftime("%Y-%m-%d")

    # 日次統計を計算
    daily_stats = {
        "date": target_date_str,
        "timestamp": target_date.isoformat(),
        "open_price": consolidated_df["usd_price"].iloc[0]
        if len(consolidated_df) > 0
        else None,
        "close_price": consolidated_df["usd_price"].iloc[-1]
        if len(consolidated_df) > 0
        else None,
        "high_price": consolidated_df["usd_price"].max(),
        "low_price": consolidated_df["usd_price"].min(),
        "avg_price": consolidated_df["usd_price"].mean(),
        "volume": consolidated_df["usd_24h_vol"].sum()
        if "usd_24h_vol" in consolidated_df.columns
        else None,
        "market_cap_avg": consolidated_df["usd_market_cap"].mean()
        if "usd_market_cap" in consolidated_df.columns
        else None,
        "price_change": (
            (
                consolidated_df["usd_price"].iloc[-1]
                - consolidated_df["usd_price"].iloc[0]
            )
            / consolidated_df["usd_price"].iloc[0]
            * 100
        )
        if len(consolidated_df) > 1
        else None,
        "volatility": consolidated_df["usd_price"].std(),
        "record_count": len(consolidated_df),
    }

    print(f"日次統計を計算しました: {daily_stats}")

    # DataFrameに変換
    daily_df = pd.DataFrame([daily_stats])

    # Parquet形式に変換
    parquet_buffer = BytesIO()
    daily_df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_data = parquet_buffer.getvalue()

    # XComに保存するためにbase64エンコード
    parquet_data_base64 = base64.b64encode(parquet_data).decode("utf-8")

    return {
        "parquet_data_base64": parquet_data_base64,
        "date": target_date_str,
        "stats": daily_stats,
    }


def upload_to_s3(**context):
    """
    日次データをS3にアップロード
    """
    ti = context["ti"]
    transformed_data = ti.xcom_pull(task_ids="transform_data")

    if not transformed_data:
        raise ValueError("変換済みデータが取得できませんでした")

    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    target_date_str = transformed_data["date"]

    # 日付からパーティション情報を取得
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    target_date = JST.localize(target_date)
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")

    parquet_key = (
        f"btc-prices/daily/"
        f"year={year}/month={month}/day={day}/"
        f"btc_daily_{target_date_str}.parquet"
    )

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

    print(f"日次データをS3に保存しました: s3://{bucket_name}/{parquet_key}")

    return {
        "parquet_key": parquet_key,
        "bucket_name": bucket_name,
        "date": target_date_str,
        "stats": transformed_data["stats"],
    }


# DAG定義
dag = DAG(
    "consolidate_daily_data",
    default_args=default_args,
    description="日次データを統合（長期トレンド分析用）",
    schedule_interval="0 2 * * *",  # 毎日午前2時（JST）に実行
    start_date=datetime(2024, 1, 1, tzinfo=JST),
    catchup=False,
    tags=["cryptocurrency", "coingecko", "s3", "consolidation", "ml", "daily"],
)

# タスク定義
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
transform_task >> upload_task
