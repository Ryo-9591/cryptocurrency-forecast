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
import numpy as np
import base64
import json
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# utilsモジュールをインポート
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.feature_engineering import create_all_features, prepare_features_for_training
from utils.models import ModelTrainer

# sklearnのインポート
try:
    from sklearn.model_selection import train_test_split
except ImportError:
    raise ImportError("scikit-learn is required for model training")

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


def load_data_from_s3(**context) -> dict:
    """
    S3からBTC価格データを読み込む

    Returns:
        dict: データフレーム（base64エンコード）とメタデータを含む辞書
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    s3_hook = S3Hook(aws_conn_id="aws_default")
    all_dataframes = []

    # 実行日を取得
    logical_date = context.get("logical_date")
    if logical_date:
        end_date = logical_date.in_timezone(JST)
    else:
        end_date = datetime.now(JST)

    # 過去90日分のデータを取得（訓練用）
    start_date = end_date - timedelta(days=90)

    start_date_utc = start_date.astimezone(pytz.UTC)
    end_date_utc = end_date.astimezone(pytz.UTC)

    print(
        f"データ取得期間: {start_date_utc.date()} から {end_date_utc.date()} (UTC基準)"
    )

    # 1. 過去（historical）データを読み込む
    try:
        prefix = "btc-prices/historical/"
        files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

        for file_key in files:
            if not file_key.endswith(".parquet"):
                continue
            try:
                file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
                parquet_data = file_obj.get()["Body"].read()
                df = pd.read_parquet(BytesIO(parquet_data))

                df = _ensure_timestamp_column(df)
                mask = (df["timestamp"] >= start_date_utc) & (
                    df["timestamp"] <= end_date_utc
                )
                df = df[mask]

                if len(df) > 0:
                    all_dataframes.append(df)
                    print(f"  読み込み: {file_key} ({len(df)} レコード)")
            except Exception as e:
                print(f"  ファイル処理エラー ({file_key}): {e}")
                continue
    except Exception as e:
        print(f"過去データ取得エラー: {e}")

    # 2. 時間単位（hourly）データを読み込む
    try:
        current_date = start_date
        while current_date <= end_date:
            year = current_date.strftime("%Y")
            month = current_date.strftime("%m")
            day = current_date.strftime("%d")

            prefix = f"btc-prices/hourly/year={year}/month={month}/day={day}/"
            files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

            for file_key in files:
                if not file_key.endswith(".parquet"):
                    continue
                try:
                    file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
                    parquet_data = file_obj.get()["Body"].read()
                    df = pd.read_parquet(BytesIO(parquet_data))

                    df = _ensure_timestamp_column(df)
                    mask = (df["timestamp"] >= start_date_utc) & (
                        df["timestamp"] <= end_date_utc
                    )
                    df = df[mask]

                    if len(df) > 0:
                        all_dataframes.append(df)
                except Exception as e:
                    print(f"  ファイル処理エラー ({file_key}): {e}")
                    continue

            current_date += timedelta(days=1)
    except Exception as e:
        print(f"時間単位データ取得エラー: {e}")

    if not all_dataframes:
        raise ValueError("S3からデータを取得できませんでした")

    # データを統合
    consolidated_df = pd.concat(all_dataframes, ignore_index=True)

    # タイムスタンプでソート
    consolidated_df["timestamp"] = pd.to_datetime(
        consolidated_df["timestamp"], utc=True
    )
    consolidated_df = consolidated_df.sort_values("timestamp").reset_index(drop=True)

    # 重複を削除
    consolidated_df = consolidated_df.drop_duplicates(subset=["timestamp"], keep="last")

    print(f"統合完了: {len(consolidated_df)} レコード")
    print(
        f"データ期間: {consolidated_df['timestamp'].min()} から {consolidated_df['timestamp'].max()}"
    )

    # Parquet形式に変換してbase64エンコード
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
        df, target_col="usd_price", forecast_horizon=1, drop_na=True
    )

    if len(X) == 0 or len(y) == 0:
        raise ValueError("訓練データが準備できませんでした")

    print(f"訓練データ: {len(X)} サンプル, {X.shape[1]} 特徴量")

    # 訓練データと検証データに分割（80:20）
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, shuffle=False
    )

    print(f"訓練データ: {len(X_train)} サンプル")
    print(f"検証データ: {len(X_val)} サンプル")

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
        ):
            mlflow.log_params(
                {
                    "train_samples": len(X_train),
                    "val_samples": len(X_val),
                    "features": X_train.shape[1],
                }
            )
            results = trainer.train_all_models(X_train, y_train, X_val, y_val)

            # 最良のモデルをModel Registryに登録
            try:
                model_uri = trainer.register_best_model_to_mlflow()
                if model_uri:
                    mlflow.log_param("registered_model_uri", model_uri)
            except Exception as e:
                print(f"Model registration error: {e}")
    else:
        results = trainer.train_all_models(X_train, y_train, X_val, y_val)

    # 評価結果を取得
    evaluation_summary = trainer.get_evaluation_summary()
    print("\n評価結果:")
    print(evaluation_summary)

    # 最良のモデルを取得
    try:
        best_model_name, best_model = trainer.get_best_model(
            metric="val_rmse", lower_is_better=True
        )
        print(f"\n最良のモデル: {best_model_name}")

        # 最良のモデルをbase64エンコード
        best_model_base64 = trainer.save_model_base64(best_model_name)

        # すべてのモデルをbase64エンコード（オプション）
        all_models_base64 = {}
        for model_name in trainer.models.keys():
            all_models_base64[model_name] = trainer.save_model_base64(model_name)

        return {
            "best_model_name": best_model_name,
            "best_model_base64": best_model_base64,
            "all_models_base64": all_models_base64,
            "evaluation_metrics": evaluation_summary.to_dict(),
            "feature_names": list(X.columns),
        }
    except Exception as e:
        print(f"最良のモデル取得エラー: {e}")
        # エラーが発生した場合でも、最初のモデルを使用
        if trainer.models:
            first_model_name = list(trainer.models.keys())[0]
            return {
                "best_model_name": first_model_name,
                "best_model_base64": trainer.save_model_base64(first_model_name),
                "all_models_base64": {
                    first_model_name: trainer.save_model_base64(first_model_name)
                },
                "evaluation_metrics": evaluation_summary.to_dict()
                if not evaluation_summary.empty
                else {},
                "feature_names": list(X.columns),
            }
        else:
            raise ValueError("訓練されたモデルがありません")


def make_predictions(**context) -> dict:
    """
    最新データを使って予測を実行

    Returns:
        dict: 予測結果を含む辞書
    """
    ti = context["ti"]
    train_info = ti.xcom_pull(task_ids="train_models")
    features_info = ti.xcom_pull(task_ids="create_features")

    if not train_info or not features_info:
        raise ValueError("訓練済みモデルまたは特徴量データが取得できませんでした")

    # データをデコード
    parquet_data = base64.b64decode(features_info["parquet_data_base64"])
    df = pd.read_parquet(BytesIO(parquet_data))

    # 最新のデータポイントを使用して予測
    latest_data = df.iloc[-1:].copy()

    # 特徴量を準備
    feature_names = train_info["feature_names"]
    X_latest = latest_data[feature_names]

    # モデルを読み込む
    import pickle

    model_bytes = base64.b64decode(train_info["best_model_base64"])
    model = pickle.loads(model_bytes)

    # 予測を実行
    prediction = model.predict(X_latest)[0]
    actual_price = latest_data["usd_price"].iloc[0]

    # 予測結果
    result = {
        "timestamp": latest_data["timestamp"].iloc[0].isoformat(),
        "actual_price": float(actual_price),
        "predicted_price": float(prediction),
        "prediction_error": float(abs(prediction - actual_price)),
        "prediction_error_pct": float(
            abs(prediction - actual_price) / actual_price * 100
        ),
        "model_name": train_info["best_model_name"],
    }

    print(f"\n予測結果:")
    print(f"  実際の価格: ${actual_price:,.2f}")
    print(f"  予測価格: ${prediction:,.2f}")
    print(
        f"  誤差: ${result['prediction_error']:,.2f} ({result['prediction_error_pct']:.2f}%)"
    )

    return result


def save_results_to_s3(**context) -> dict:
    """
    訓練結果と予測結果をS3に保存

    Returns:
        dict: 保存されたファイルの情報を含む辞書
    """
    ti = context["ti"]
    train_info = ti.xcom_pull(task_ids="train_models")
    prediction_info = ti.xcom_pull(task_ids="make_predictions")

    if not train_info:
        raise ValueError("訓練結果が取得できませんでした")

    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    s3_hook = S3Hook(aws_conn_id="aws_default")

    # 実行日時を取得
    logical_date = context.get("logical_date")
    if logical_date:
        run_date = logical_date.in_timezone(JST)
    else:
        run_date = datetime.now(JST)

    date_str = run_date.strftime("%Y-%m-%d")
    timestamp_str = run_date.strftime("%Y-%m-%dT%H-%M-%S")

    saved_files = []

    # 1. 評価結果をJSON形式で保存
    evaluation_metrics = train_info.get("evaluation_metrics", {})
    evaluation_json = json.dumps(evaluation_metrics, indent=2, default=str)
    evaluation_key = (
        f"btc-prices/ml/evaluation/{date_str}/evaluation_{timestamp_str}.json"
    )
    s3_hook.load_string(
        string_data=evaluation_json,
        key=evaluation_key,
        bucket_name=bucket_name,
        replace=True,
    )
    saved_files.append(evaluation_key)
    print(f"評価結果を保存: s3://{bucket_name}/{evaluation_key}")

    # 2. 予測結果をJSON形式で保存
    if prediction_info:
        prediction_json = json.dumps(prediction_info, indent=2, default=str)
        prediction_key = (
            f"btc-prices/ml/predictions/{date_str}/prediction_{timestamp_str}.json"
        )
        s3_hook.load_string(
            string_data=prediction_json,
            key=prediction_key,
            bucket_name=bucket_name,
            replace=True,
        )
        saved_files.append(prediction_key)
        print(f"予測結果を保存: s3://{bucket_name}/{prediction_key}")

    # 3. 最良のモデルを保存（オプション）
    try:
        model_key = f"btc-prices/ml/models/{date_str}/model_{train_info['best_model_name']}_{timestamp_str}.pkl"
        model_bytes = base64.b64decode(train_info["best_model_base64"])
        s3_hook.load_bytes(
            bytes_data=model_bytes,
            key=model_key,
            bucket_name=bucket_name,
            replace=True,
        )
        saved_files.append(model_key)
        print(f"モデルを保存: s3://{bucket_name}/{model_key}")

        # 4. 特徴量名を保存（リアルタイム予測で使用）
        feature_names = train_info.get("feature_names", [])
        feature_info = {
            "feature_names": feature_names,
            "model_name": train_info["best_model_name"],
            "model_key": model_key,
            "timestamp": timestamp_str,
        }
        feature_info_json = json.dumps(feature_info, indent=2, default=str)
        feature_info_key = f"btc-prices/ml/models/{date_str}/features_{train_info['best_model_name']}_{timestamp_str}.json"
        s3_hook.load_string(
            string_data=feature_info_json,
            key=feature_info_key,
            bucket_name=bucket_name,
            replace=True,
        )
        saved_files.append(feature_info_key)
        print(f"特徴量情報を保存: s3://{bucket_name}/{feature_info_key}")

        # 最新の特徴量情報も保存（常に上書き）
        latest_feature_info_key = (
            f"btc-prices/ml/models/latest/features_{train_info['best_model_name']}.json"
        )
        s3_hook.load_string(
            string_data=feature_info_json,
            key=latest_feature_info_key,
            bucket_name=bucket_name,
            replace=True,
        )
        print(f"最新特徴量情報を更新: s3://{bucket_name}/{latest_feature_info_key}")
    except Exception as e:
        print(f"モデル保存エラー（無視）: {e}")

    return {
        "saved_files": saved_files,
        "bucket_name": bucket_name,
        "timestamp": timestamp_str,
    }


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

predict_task = PythonOperator(
    task_id="make_predictions",
    python_callable=make_predictions,
    dag=dag,
)

save_task = PythonOperator(
    task_id="save_results_to_s3",
    python_callable=save_results_to_s3,
    dag=dag,
)

# タスクの依存関係を設定
load_task >> features_task >> train_task >> predict_task >> save_task
