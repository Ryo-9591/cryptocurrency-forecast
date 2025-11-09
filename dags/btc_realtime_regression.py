"""
BTC価格リアルタイム回帰分析DAG（MLflow統合）

MLflow Model Registryから最新のモデルを読み込み、リアルタイムデータを使って予測を実行
1時間ごとに自動実行して予測結果をS3に保存
"""

import os
import sys
from datetime import datetime, timedelta
import pytz
import pandas as pd
import base64
import json
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException

# MLflow
try:
    import mlflow
    import mlflow.pyfunc

    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    print("Warning: mlflow is not available")

# utilsモジュールをインポート
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.feature_engineering import create_all_features

# タイムゾーン設定（東京時間）
JST = pytz.timezone("Asia/Tokyo")

# デフォルト引数
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def load_model_from_mlflow(**context) -> dict:
    """
    MLflow Model Registryから最新のモデルを読み込む

    Returns:
        dict: モデル情報を含む辞書
    """
    if not MLFLOW_AVAILABLE:
        raise ValueError("MLflow is not available")

    model_name = os.getenv("MLFLOW_MODEL_NAME", "btc-price-prediction")
    model_stage = os.getenv("MLFLOW_MODEL_STAGE", "Production")
    mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

    mlflow.set_tracking_uri(mlflow_tracking_uri)

    try:
        # Model Registryからモデルを読み込む
        model_uri = f"models:/{model_name}/{model_stage}"
        print(f"MLflowからモデルを読み込み: {model_uri}")

        _ = mlflow.pyfunc.load_model(model_uri)

        # モデル情報を取得
        client = mlflow.tracking.MlflowClient()
        latest_version = client.get_latest_versions(model_name, stages=[model_stage])[0]

        model_info = {
            "model_uri": model_uri,
            "model_name": model_name,
            "model_version": latest_version.version,
            "model_stage": model_stage,
            "model_timestamp": latest_version.creation_timestamp,
            "run_id": latest_version.run_id,
        }

        print(
            f"モデル読み込み完了: {model_name} v{latest_version.version} ({model_stage})"
        )
        return model_info

    except Exception as e:
        print(f"MLflowモデル読み込みエラー: {e}")
        # フォールバック: 最新バージョンを試す
        try:
            client = mlflow.tracking.MlflowClient()
            latest_versions = client.get_latest_versions(model_name, stages=[])
            if latest_versions:
                latest_version = latest_versions[0]
                model_uri = f"models:/{model_name}/{latest_version.version}"
                _ = mlflow.pyfunc.load_model(model_uri)
                model_info = {
                    "model_uri": model_uri,
                    "model_name": model_name,
                    "model_version": latest_version.version,
                    "model_stage": latest_version.current_stage or "None",
                    "model_timestamp": latest_version.creation_timestamp,
                    "run_id": latest_version.run_id,
                }
                print(
                    f"フォールバック: モデル読み込み完了: {model_name} v{latest_version.version}"
                )
                return model_info
        except Exception as fallback_error:
            raise ValueError(
                f"MLflowモデル読み込みに失敗しました: {e}, フォールバックも失敗: {fallback_error}"
            )


def _ensure_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    candidates = ["timestamp", "datetime", "date", "time", "ts"]
    for col in df.columns:
        if col.lower() in candidates:
            df = df.copy()
            df["timestamp"] = pd.to_datetime(df[col], utc=True)
            return df
    raise KeyError("timestamp")


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return pytz.UTC.localize(dt)
    return dt.astimezone(pytz.UTC)


def _load_hourly_data(
    s3_hook: S3Hook,
    bucket_name: str,
    start_time_utc: datetime,
    end_time_utc: datetime,
) -> list[pd.DataFrame]:
    dataframes: list[pd.DataFrame] = []

    current_time = start_time_utc
    while current_time < end_time_utc:
        current_jst = current_time.astimezone(JST)
        year = current_jst.strftime("%Y")
        month = current_jst.strftime("%m")
        day = current_jst.strftime("%d")
        hour = current_jst.strftime("%H")

        prefix = f"btc-prices/hourly/year={year}/month={month}/day={day}/hour={hour}/"
        files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix) or []

        for file_key in files:
            if not file_key.endswith(".parquet"):
                continue
            try:
                file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
                parquet_data = file_obj.get()["Body"].read()
                df = pd.read_parquet(BytesIO(parquet_data))
                df = _ensure_timestamp_column(df)
                mask = (df["timestamp"] >= start_time_utc) & (
                    df["timestamp"] <= end_time_utc
                )
                df = df[mask]
                if not df.empty:
                    dataframes.append(df)
                    print(f"  読み込み: {file_key} ({len(df)} レコード)")
            except Exception as e:
                print(f"  時間データ読み込みエラー ({file_key}): {e}")
                continue

        current_time += timedelta(hours=1)

    return dataframes


def _load_historical_data(
    s3_hook: S3Hook,
    bucket_name: str,
    start_time_utc: datetime,
    end_time_utc: datetime,
) -> list[pd.DataFrame]:
    dataframes: list[pd.DataFrame] = []

    try:
        prefix = "btc-prices/historical/"
        files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix) or []
        for file_key in files:
            if not file_key.endswith(".parquet"):
                continue
            try:
                file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
                parquet_data = file_obj.get()["Body"].read()
                df = pd.read_parquet(BytesIO(parquet_data))
                df = _ensure_timestamp_column(df)
                mask = (df["timestamp"] >= start_time_utc) & (
                    df["timestamp"] <= end_time_utc
                )
                df = df[mask]
                if not df.empty:
                    dataframes.append(df)
                    print(f"  読み込み: {file_key} ({len(df)} レコード)")
            except Exception as e:
                print(f"  過去データ読み込みエラー ({file_key}): {e}")
                continue
    except Exception as e:
        print(f"  過去データ一覧取得エラー: {e}")

    return dataframes


def load_realtime_data(**context) -> dict:
    """
    S3の時間単位データおよび過去データから最新のデータを読み込む
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    s3_hook = S3Hook(aws_conn_id="aws_default")

    logical_date = context.get("logical_date")
    if logical_date:
        end_time = logical_date.in_timezone(JST)
    else:
        end_time = datetime.now(JST)

    start_time = end_time - timedelta(hours=1)

    start_time_utc = _to_utc(start_time)
    end_time_utc = _to_utc(end_time)

    print(
        f"リアルタイム予測用データ取得期間: {start_time_utc.isoformat()} 〜 {end_time_utc.isoformat()} (UTC)"
    )

    dataframes: list[pd.DataFrame] = []

    # 時間単位データを優先的に読み込む
    dataframes.extend(
        _load_hourly_data(
            s3_hook=s3_hook,
            bucket_name=bucket_name,
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
        )
    )

    # データが見つからない場合は過去データを使用
    if not dataframes:
        print("時間単位データが見つからないため、過去データを検索します...")
        dataframes.extend(
            _load_historical_data(
                s3_hook=s3_hook,
                bucket_name=bucket_name,
                start_time_utc=start_time_utc - timedelta(days=1),
                end_time_utc=end_time_utc,
            )
        )

    if not dataframes:
        raise AirflowSkipException("予測に使用可能なデータが見つかりませんでした")

    consolidated_df = pd.concat(dataframes, ignore_index=True)
    consolidated_df = consolidated_df.sort_values("timestamp").drop_duplicates(
        subset=["timestamp"], keep="last"
    )
    consolidated_df = consolidated_df.reset_index(drop=True)

    print(f"統合データ: {len(consolidated_df)} レコード")
    print(
        f"データ期間: {consolidated_df['timestamp'].min()} 〜 {consolidated_df['timestamp'].max()}"
    )

    parquet_buffer = BytesIO()
    consolidated_df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_data = parquet_buffer.getvalue()
    parquet_data_base64 = base64.b64encode(parquet_data).decode("utf-8")

    return {
        "parquet_data_base64": parquet_data_base64,
        "record_count": len(consolidated_df),
        "latest_timestamp": consolidated_df["timestamp"].iloc[-1].isoformat(),
    }


def create_realtime_features(**context) -> dict:
    """
    リアルタイムデータから特徴量を作成

    Returns:
        dict: 特徴量が追加されたデータフレーム（base64エンコード）を含む辞書
    """
    ti = context["ti"]
    data_info = ti.xcom_pull(task_ids="load_realtime_data")

    if not data_info:
        raise ValueError("リアルタイムデータが取得できませんでした")

    # データをデコード
    parquet_data = base64.b64decode(data_info["parquet_data_base64"])
    df = pd.read_parquet(BytesIO(parquet_data))

    print(f"特徴量エンジニアリング開始: {len(df)} レコード")

    # 特徴量を作成
    df = create_all_features(df, target_col="usd_price", timestamp_col="timestamp")

    print(f"特徴量作成完了: {df.shape[1]} 列")

    # 最新のデータポイントのみを使用
    latest_data = df.iloc[-1:].copy()

    # Parquet形式に変換してbase64エンコード
    parquet_buffer = BytesIO()
    latest_data.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_data = parquet_buffer.getvalue()
    parquet_data_base64 = base64.b64encode(parquet_data).decode("utf-8")

    return {
        "parquet_data_base64": parquet_data_base64,
        "feature_count": latest_data.shape[1],
        "timestamp": latest_data["timestamp"].iloc[0].isoformat()
        if "timestamp" in latest_data.columns
        else None,
    }


def make_realtime_prediction(**context) -> dict:
    """
    リアルタイムデータを使って予測を実行（MLflowモデルを使用）

    Returns:
        dict: 予測結果を含む辞書
    """
    ti = context["ti"]
    features_info = ti.xcom_pull(task_ids="create_realtime_features")
    model_info = ti.xcom_pull(task_ids="load_model_from_mlflow")

    if not features_info or not model_info:
        raise ValueError("特徴量データまたはモデルが取得できませんでした")

    # データをデコード
    parquet_data = base64.b64decode(features_info["parquet_data_base64"])
    df = pd.read_parquet(BytesIO(parquet_data))

    # MLflowモデルを取得
    model_uri = model_info["model_uri"]
    mlflow_model = mlflow.pyfunc.load_model(model_uri)
    model_name = model_info["model_name"]
    model_version = model_info["model_version"]

    # 特徴量を準備（usd_price以外の数値列を特徴量として使用）
    feature_cols = [
        col
        for col in df.columns
        if col not in ["usd_price", "timestamp", "date", "last_updated"]
        and pd.api.types.is_numeric_dtype(df[col])
    ]
    X_latest = df[feature_cols].fillna(0)

    # MLflow pyfuncモデルで予測
    try:
        prediction = mlflow_model.predict(X_latest)[0]
    except Exception as e:
        print(f"予測エラー: {e}")
        raise

    actual_price = df["usd_price"].iloc[0]

    # 予測結果
    result = {
        "timestamp": features_info.get("timestamp", datetime.now(JST).isoformat()),
        "actual_price": float(actual_price),
        "predicted_price": float(prediction),
        "prediction_error": float(abs(prediction - actual_price)),
        "prediction_error_pct": float(
            abs(prediction - actual_price) / actual_price * 100
        ),
        "model_name": model_name,
        "model_version": model_version,
        "model_stage": model_info.get("model_stage"),
        "model_timestamp": datetime.fromtimestamp(
            model_info.get("model_timestamp", 0) / 1000
        ).isoformat()
        if model_info.get("model_timestamp")
        else None,
        "run_id": model_info.get("run_id"),
    }

    print("\nリアルタイム予測結果:")
    print(f"  タイムスタンプ: {result['timestamp']}")
    print(f"  実際の価格: ${actual_price:,.2f}")
    print(f"  予測価格: ${prediction:,.2f}")
    print(
        f"  誤差: ${result['prediction_error']:,.2f} ({result['prediction_error_pct']:.2f}%)"
    )
    print(
        f"  使用モデル: {model_name} v{model_version} ({model_info.get('model_stage')})"
    )

    return result


def save_realtime_prediction(**context) -> dict:
    """
    リアルタイム予測結果をS3に保存

    Returns:
        dict: 保存されたファイルの情報を含む辞書
    """
    ti = context["ti"]
    prediction_info = ti.xcom_pull(task_ids="make_realtime_prediction")

    if not prediction_info:
        raise ValueError("予測結果が取得できませんでした")

    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    s3_hook = S3Hook(aws_conn_id="aws_default")

    # 実行日時を取得
    execution_date = context.get("execution_date")
    if execution_date:
        if execution_date.tzinfo is None:
            run_date = JST.localize(execution_date)
        else:
            run_date = execution_date.astimezone(JST)
    else:
        run_date = datetime.now(JST)

    date_str = run_date.strftime("%Y-%m-%d")
    timestamp_str = run_date.strftime("%Y-%m-%dT%H-%M-%S")

    # 予測結果をJSON形式で保存
    prediction_json = json.dumps(prediction_info, indent=2, default=str)
    prediction_key = (
        f"btc-prices/ml/realtime_predictions/{date_str}/prediction_{timestamp_str}.json"
    )
    s3_hook.load_string(
        string_data=prediction_json,
        key=prediction_key,
        bucket_name=bucket_name,
        replace=True,
    )
    print(f"リアルタイム予測結果を保存: s3://{bucket_name}/{prediction_key}")

    # 最新の予測結果も保存（常に上書き）
    latest_prediction_key = (
        "btc-prices/ml/realtime_predictions/latest/prediction_latest.json"
    )
    s3_hook.load_string(
        string_data=prediction_json,
        key=latest_prediction_key,
        bucket_name=bucket_name,
        replace=True,
    )
    print(f"最新予測結果を更新: s3://{bucket_name}/{latest_prediction_key}")

    return {
        "prediction_key": prediction_key,
        "latest_prediction_key": latest_prediction_key,
        "bucket_name": bucket_name,
        "timestamp": timestamp_str,
    }


# DAG定義
dag = DAG(
    "btc_realtime_regression",
    default_args=default_args,
    description="BTC価格のリアルタイム回帰分析と予測（1時間ごと実行、MLflow統合）",
    schedule_interval="0 * * * *",  # 毎時0分に実行
    start_date=datetime(2024, 1, 1, tzinfo=JST),
    catchup=False,
    tags=["cryptocurrency", "ml", "regression", "realtime", "prediction", "mlflow"],
)

# タスク定義
load_model_task = PythonOperator(
    task_id="load_model_from_mlflow",
    python_callable=load_model_from_mlflow,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id="load_realtime_data",
    python_callable=load_realtime_data,
    dag=dag,
)

features_task = PythonOperator(
    task_id="create_realtime_features",
    python_callable=create_realtime_features,
    dag=dag,
)

predict_task = PythonOperator(
    task_id="make_realtime_prediction",
    python_callable=make_realtime_prediction,
    dag=dag,
)

save_task = PythonOperator(
    task_id="save_realtime_prediction",
    python_callable=save_realtime_prediction,
    dag=dag,
)

# タスクの依存関係を設定
[load_model_task, load_data_task] >> features_task >> predict_task >> save_task
