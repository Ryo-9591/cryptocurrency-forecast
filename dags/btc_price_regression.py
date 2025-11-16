"""
BTC価格回帰分析DAG

S3からBTC価格データを読み込み、特徴量エンジニアリングを行い、
複数の回帰モデルを訓練して予測を行う
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Optional
import pytz
import pandas as pd
import base64
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sklearn.model_selection import train_test_split
from mlflow.tracking import MlflowClient

# utilsモジュールをインポート
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.feature_engineering import create_all_features, prepare_features_for_training
from utils.models import ModelTrainer


# タイムゾーン設定（東京時間）
JST = pytz.timezone("Asia/Tokyo")

# 学習データの読込元（fetch_daily_dataで集約した単一Parquet）
DAILY_PARQUET_KEY = os.getenv(
    "DAILY_PARQUET_KEY", "btc-prices/daily/btc_prices.parquet"
)

# 予測する時間先（時間単位）
FORECAST_HORIZON_HOURS = int(os.getenv("FORECAST_HORIZON_HOURS", "4"))

# MLflowモデルの自動昇格設定
AUTO_PROMOTE_MLFLOW_MODEL = (
    os.getenv("AUTO_PROMOTE_MLFLOW_MODEL", "true").lower() == "true"
)
TARGET_MLFLOW_STAGE = os.getenv("MLFLOW_MODEL_STAGE", "Production")

# デフォルト引数
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _get_column_case_insensitive(df: pd.DataFrame, target: str) -> Optional[str]:
    target_lower = target.lower()
    for column in df.columns:
        if column.lower() == target_lower:
            return column
    return None


def _ensure_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the dataframe has a `timestamp` column."""
    df = df.copy()
    candidates = ["timestamp", "datetime", "date", "time", "ts"]
    for candidate in candidates:
        column = _get_column_case_insensitive(df, candidate)
        if column:
            ts = pd.to_datetime(df[column], utc=True, errors="coerce")
            df["timestamp"] = ts
            return df

    raise KeyError("timestamp")


def _promote_latest_mlflow_model(model_name: str, stage: str) -> Optional[str]:
    try:
        client = MlflowClient()
        versions = client.search_model_versions(f"name='{model_name}'")
        if not versions:
            print(f"Model {model_name} に登録済みバージョンがありません")
            return None

        latest = max(versions, key=lambda v: v.creation_timestamp)
        client.transition_model_version_stage(
            name=model_name,
            version=latest.version,
            stage=stage,
            archive_existing_versions=True,
        )
        print(
            f"Model {model_name} version {latest.version} を {stage} ステージに昇格しました"
        )
        return latest.version
    except Exception as exc:  # noqa: BLE001
        print(f"モデル昇格処理でエラーが発生しました: {exc}")
        return None


def load_data_from_s3(**context) -> dict:
    """
    S3からBTC価格データ（単一Parquet）を読み込む

    Returns:
        dict: データフレーム（base64エンコード）とメタデータを含む辞書
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    s3_hook = S3Hook(aws_conn_id="aws_default")

    key = DAILY_PARQUET_KEY
    print(f"S3から学習データを取得: s3://{bucket_name}/{key}")

    if not s3_hook.check_for_key(key=key, bucket_name=bucket_name):
        raise ValueError("S3から学習用データを取得できませんでした")

    try:
        file_obj = s3_hook.get_key(key=key, bucket_name=bucket_name)
        parquet_data = file_obj.get()["Body"].read()
        consolidated_df = pd.read_parquet(BytesIO(parquet_data))
    except Exception as e:
        raise ValueError(f"学習データParquetの読み込みに失敗しました: {e}") from e

    consolidated_df = _ensure_timestamp_column(consolidated_df)
    consolidated_df["timestamp"] = pd.to_datetime(
        consolidated_df["timestamp"], utc=True
    )
    consolidated_df = consolidated_df.sort_values("timestamp").drop_duplicates(
        subset=["timestamp"], keep="last"
    )
    consolidated_df = consolidated_df.reset_index(drop=True)

    print(f"読み込み完了: {len(consolidated_df)} レコード")
    print(
        f"データ期間: {consolidated_df['timestamp'].min()} から {consolidated_df['timestamp'].max()}"
    )

    parquet_buffer = BytesIO()
    consolidated_df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_data = parquet_buffer.getvalue()
    parquet_data_base64 = base64.b64encode(parquet_data).decode("utf-8")

    return {
        "parquet_data_base64": parquet_data_base64,
        "record_count": len(consolidated_df),
        "date_range": {
            "start": consolidated_df["timestamp"].min().isoformat(),
            "end": consolidated_df["timestamp"].max().isoformat(),
        },
        "new_records": len(consolidated_df),
    }


def create_features(**context) -> dict:
    """
    特徴量エンジニアリングを実行

    Returns:
        dict: 特徴量が追加されたデータフレーム（base64エンコード）を含む辞書
    """
    ti = context["ti"]
    data_info = ti.xcom_pull(task_ids="load_data_from_s3")

    if not data_info:
        raise ValueError("データが取得できませんでした")

    # データをデコード
    parquet_data = base64.b64decode(data_info["parquet_data_base64"])
    df = pd.read_parquet(BytesIO(parquet_data))

    print(f"特徴量エンジニアリング開始: {len(df)} レコード")

    # 特徴量を作成
    df = create_all_features(df, target_col="usd_price", timestamp_col="timestamp")

    print(f"特徴量作成完了: {df.shape[1]} 列")

    # Parquet形式に変換してbase64エンコード
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_data = parquet_buffer.getvalue()
    parquet_data_base64 = base64.b64encode(parquet_data).decode("utf-8")

    return {
        "parquet_data_base64": parquet_data_base64,
        "feature_count": df.shape[1],
        "record_count": len(df),
    }


def train_models(**context) -> dict:
    """
    回帰モデルを訓練

    Returns:
        dict: 訓練済みモデルと評価結果を含む辞書
    """
    ti = context["ti"]
    features_info = ti.xcom_pull(task_ids="create_features")

    if not features_info:
        raise ValueError("特徴量データが取得できませんでした")

    # データをデコード
    parquet_data = base64.b64decode(features_info["parquet_data_base64"])
    df = pd.read_parquet(BytesIO(parquet_data))

    print(f"モデル訓練開始: {len(df)} レコード, {df.shape[1]} 特徴量")

    # 特徴量とターゲットを準備
    X, y = prepare_features_for_training(
        df,
        target_col="usd_price",
        forecast_horizon=FORECAST_HORIZON_HOURS,
        drop_na=False,
    )

    time_columns = {"timestamp", "timestamp_jst", "retrieved_at"}
    X = X.drop(
        columns=[col for col in time_columns if col in X.columns], errors="ignore"
    )

    X = X.replace([pd.NA, float("inf"), float("-inf")], pd.NA)
    y = y.replace([pd.NA, float("inf"), float("-inf")], pd.NA)

    valid_mask = ~(X.isna().any(axis=1) | y.isna())
    X = X[valid_mask]
    y = y[valid_mask]

    if len(X) == 0 or len(y) == 0:
        raise ValueError("訓練データが準備できませんでした")

    print(f"訓練データ: {len(X)} サンプル, {X.shape[1]} 特徴量")

    # 訓練データと検証データに分割（80:20）
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, shuffle=False
    )

    print(f"訓練データ: {len(X_train)} サンプル")
    print(f"検証データ: {len(X_val)} サンプル")

    promoted_model_version: Optional[str] = None

    # モデルを訓練（MLflow統合）
    use_mlflow = os.getenv("USE_MLFLOW", "true").lower() == "true"
    trainer = ModelTrainer(random_state=42, use_mlflow=use_mlflow)

    # MLflow runを開始
    if use_mlflow:
        try:
            import mlflow

            mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
            mlflow.set_tracking_uri(mlflow_tracking_uri)
            experiment_name = os.getenv(
                "MLFLOW_EXPERIMENT_NAME", "btc-price-prediction"
            )
            mlflow.set_experiment(experiment_name)
        except Exception as e:
            print(f"MLflow setup error: {e}")
            use_mlflow = False

    if use_mlflow:
        import mlflow

        with mlflow.start_run(
            run_name=f"training_{datetime.now(JST).strftime('%Y%m%d_%H%M%S')}"
        ) as active_run:
            mlflow.log_params(
                {
                    "train_samples": len(X_train),
                    "val_samples": len(X_val),
                    "features": X_train.shape[1],
                    "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
                }
            )
            trainer.train_xgboost_model(
                X_train,
                y_train,
                X_val,
                y_val,
            )

            # モデルをModel Registryに登録（最新モデルを昇格対象にする）
            try:
                model_uri = trainer.register_best_model_to_mlflow()
                if model_uri:
                    mlflow.log_param("registered_model_uri", model_uri)

                if AUTO_PROMOTE_MLFLOW_MODEL:
                    client = MlflowClient()
                    model_name_env = os.getenv(
                        "MLFLOW_MODEL_NAME", "btc-price-prediction"
                    )
                    # 現在のrun_idに紐づくモデルバージョンを特定して昇格
                    run_id = active_run.info.run_id
                    versions = client.search_model_versions(f"name='{model_name_env}'")
                    current_run_versions = [
                        v for v in versions if getattr(v, "run_id", None) == run_id
                    ]
                    target_version = None
                    if current_run_versions:
                        # 念のため作成時刻で最新を選択
                        target_version = max(
                            current_run_versions, key=lambda v: v.creation_timestamp
                        )
                    else:
                        # フォールバック: 全体の最新を昇格
                        target_version = (
                            max(versions, key=lambda v: v.creation_timestamp)
                            if versions
                            else None
                        )

                    if target_version:
                        client.transition_model_version_stage(
                            name=model_name_env,
                            version=target_version.version,
                            stage=TARGET_MLFLOW_STAGE,
                            archive_existing_versions=True,
                        )
                        promoted_model_version = target_version.version
                        print(
                            f"最新モデルを昇格しました: {model_name_env} version {promoted_model_version} -> {TARGET_MLFLOW_STAGE}"
                        )
            except Exception as e:
                print(f"Model registration/promotion error (ignored): {e}")

    else:
        trainer.train_xgboost_model(X_train, y_train, X_val, y_val)

    # 評価結果を取得
    evaluation_summary = trainer.get_evaluation_summary()
    print("\n評価結果:")
    print(evaluation_summary)

    # 最良のモデルを取得
    try:
        best_model_name, best_model = trainer.get_trained_model(
            metric="val_rmse", lower_is_better=True
        )
        print(f"\n最良のモデル: {best_model_name}")

        # 最良のモデルをbase64エンコード
        best_model_base64 = trainer.save_model_base64(best_model_name)

        # すべてのモデルをbase64エンコード（オプション）
        all_models_base64 = {}
        for model_name in trainer.models.keys():
            all_models_base64[model_name] = trainer.save_model_base64(model_name)

        feature_names = [
            col
            for col in X.columns
            if col not in {"timestamp", "timestamp_jst", "retrieved_at"}
        ]
        return {
            "best_model_name": best_model_name,
            "best_model_base64": best_model_base64,
            "all_models_base64": all_models_base64,
            "evaluation_metrics": evaluation_summary.to_dict(),
            "feature_names": feature_names,
            "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
            "promoted_model_version": promoted_model_version,
        }
    except Exception as e:
        print(f"最良のモデル取得エラー: {e}")
        # エラーが発生した場合でも、最初のモデルを使用
        if trainer.models:
            first_model_name = list(trainer.models.keys())[0]
            feature_names = [
                col
                for col in X.columns
                if col not in {"timestamp", "timestamp_jst", "retrieved_at"}
            ]
            return {
                "best_model_name": first_model_name,
                "best_model_base64": trainer.save_model_base64(first_model_name),
                "all_models_base64": {
                    first_model_name: trainer.save_model_base64(first_model_name)
                },
                "evaluation_metrics": evaluation_summary.to_dict()
                if not evaluation_summary.empty
                else {},
                "feature_names": feature_names,
                "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
                "promoted_model_version": promoted_model_version,
            }
        else:
            raise ValueError("訓練されたモデルがありません")


# DAG定義
dag = DAG(
    "btc_price_regression",
    default_args=default_args,
    description="BTC価格の回帰分析と予測（日次実行）",
    schedule_interval="0 3 * * *",  # 毎日午前3時（JST）に実行
    start_date=datetime(2024, 1, 1, tzinfo=JST),
    catchup=False,
    tags=["cryptocurrency", "ml", "regression", "prediction"],
)

# タスク定義
load_task = PythonOperator(
    task_id="load_data_from_s3",
    python_callable=load_data_from_s3,
    dag=dag,
)

features_task = PythonOperator(
    task_id="create_features",
    python_callable=create_features,
    dag=dag,
)

train_task = PythonOperator(
    task_id="train_models",
    python_callable=train_models,
    dag=dag,
)

# タスクの依存関係を設定（学習・登録・昇格のみ）
load_task >> features_task >> train_task
