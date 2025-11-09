"""
時間単位データ取得DAG

S3から過去1時間分のリアルタイムデータを読み込んで統合し、S3に保存
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.exceptions import AirflowSkipException
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


def get_hourly_data(**context):
    """
    S3から過去1時間分のリアルタイムデータを読み込んで統合

    Args:
        context: Airflowコンテキスト

    Returns:
        pd.DataFrame: 統合されたデータフレーム
    """
    # S3設定を取得
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    # 実行時刻から前1時間の範囲を計算
    logical_date = context.get("logical_date")
    if logical_date:
        end_time = logical_date.in_timezone(JST)
    else:
        # 手動実行の場合
        end_time = datetime.now(JST)

    # 前1時間の開始時刻と終了時刻
    start_time = end_time - timedelta(hours=1)
    start_date_str = start_time.strftime("%Y-%m-%d")
    end_date_str = end_time.strftime("%Y-%m-%d")

    print(f"統合対象期間: {start_time} から {end_time}")

    # S3Hookを使用してS3に接続
    s3_hook = S3Hook(aws_conn_id="aws_default")

    # 対象期間のファイルを取得
    all_dataframes = []

    # 日付が変わる可能性があるため、両方の日付をチェック
    dates_to_check = set([start_date_str, end_date_str])

    for date_str in dates_to_check:
        prefix = f"btc-prices/raw/{date_str}/"
        print(f"S3からファイルを検索中: {prefix}")

        try:
            # 該当日のファイル一覧を取得
            files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

            for file_key in files:
                if not file_key.endswith(".parquet"):
                    continue

                # ファイル名からタイムスタンプを抽出
                try:
                    filename = file_key.split("/")[-1]
                    timestamp_str = filename.replace("btc_price_", "").replace(
                        ".parquet", ""
                    )
                    file_timestamp = datetime.strptime(
                        timestamp_str, "%Y-%m-%dT%H-%M-%S"
                    )
                    file_timestamp = JST.localize(file_timestamp)

                    # 対象期間内かチェック
                    if start_time <= file_timestamp < end_time:
                        # ファイルを読み込む
                        file_obj = s3_hook.get_key(
                            key=file_key, bucket_name=bucket_name
                        )
                        parquet_data = file_obj.get()["Body"].read()

                        df = pd.read_parquet(BytesIO(parquet_data))
                        all_dataframes.append(df)
                        print(f"  読み込み: {file_key}")
                except Exception as e:
                    print(f"  ファイル処理エラー ({file_key}): {e}")
                    continue

        except Exception as e:
            print(f"日付 {date_str} のファイル取得エラー: {e}")
            continue

    if not all_dataframes:
        print("統合するデータが見つかりませんでした")
        return None

    # データを統合
    consolidated_df = pd.concat(all_dataframes, ignore_index=True)

    # タイムスタンプでソート
    consolidated_df["timestamp"] = pd.to_datetime(consolidated_df["timestamp"])
    consolidated_df = consolidated_df.sort_values("timestamp").reset_index(drop=True)

    print(f"統合完了: {len(consolidated_df)} レコード")

    return consolidated_df


def transform_data(**context):
    """
    時間単位データをParquet形式に変換
    """
    # データを取得
    consolidated_df = get_hourly_data(**context)

    if consolidated_df is None:
        raise AirflowSkipException("統合するデータが見つかりませんでした")

    # Parquet形式に変換
    parquet_buffer = BytesIO()
    consolidated_df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_data = parquet_buffer.getvalue()

    # XComに保存するためにbase64エンコード
    parquet_data_base64 = base64.b64encode(parquet_data).decode("utf-8")

    print(f"データを整形しました。Parquetサイズ: {len(parquet_data)} bytes")

    # 実行時刻から前1時間の範囲を計算（S3キー生成用）
    logical_date = context.get("logical_date")
    if logical_date:
        end_time = logical_date.in_timezone(JST)
    else:
        end_time = datetime.now(JST)

    start_time = end_time - timedelta(hours=1)

    return {
        "parquet_data_base64": parquet_data_base64,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
    }


def upload_to_s3(**context):
    """
    時間単位データをS3にアップロード
    """
    ti = context["ti"]
    transformed_data = ti.xcom_pull(task_ids="transform_data")

    if not transformed_data:
        raise ValueError("変換済みデータが取得できませんでした")

    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    start_time = datetime.fromisoformat(transformed_data["start_time"])
    end_time = datetime.fromisoformat(transformed_data["end_time"])

    # Hive形式のパーティション構造で保存
    year = start_time.strftime("%Y")
    month = start_time.strftime("%m")
    day = start_time.strftime("%d")
    hour = start_time.strftime("%H")

    parquet_key = (
        f"btc-prices/hourly/"
        f"year={year}/month={month}/day={day}/hour={hour}/"
        f"btc_prices_{start_time.strftime('%Y-%m-%dT%H-%M-%S')}_to_{end_time.strftime('%Y-%m-%dT%H-%M-%S')}.parquet"
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

    print(f"統合データをS3に保存しました: s3://{bucket_name}/{parquet_key}")

    return {
        "parquet_key": parquet_key,
        "bucket_name": bucket_name,
        "start_time": transformed_data["start_time"],
        "end_time": transformed_data["end_time"],
    }


# DAG定義
dag = DAG(
    "consolidate_hourly_data",
    default_args=default_args,
    description="リアルタイムデータを時間単位で統合（機械学習用）",
    schedule_interval="0 * * * *",  # 毎時0分に実行（前1時間分を統合）
    start_date=datetime(2024, 1, 1, tzinfo=JST),
    catchup=False,
    tags=["cryptocurrency", "coingecko", "s3", "consolidation", "ml"],
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
