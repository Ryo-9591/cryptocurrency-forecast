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

# 訓練データを永続化するS3キー（環境変数で上書き可能）
TRAINING_DATA_S3_KEY = os.getenv(
    "TRAINING_DATA_S3_KEY",
    "btc-prices/ml/training_data/training_data.parquet",
)

# 予測する時間先（時間単位）
FORECAST_HORIZON_HOURS = int(os.getenv("FORECAST_HORIZON_HOURS", "4"))

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
    S3からBTC価格データを読み込み、前日分の差分を既存データに追加する

    Returns:
        dict: データフレーム（base64エンコード）とメタデータを含む辞書
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME環境変数が設定されていません")

    s3_hook = S3Hook(aws_conn_id="aws_default")
    existing_df = pd.DataFrame()
    try:
        if s3_hook.check_for_key(TRAINING_DATA_S3_KEY, bucket_name=bucket_name):
            training_obj = s3_hook.get_key(
                key=TRAINING_DATA_S3_KEY, bucket_name=bucket_name
            )
            training_data = training_obj.get()["Body"].read()
            existing_df = pd.read_parquet(BytesIO(training_data))
            existing_df = _ensure_timestamp_column(existing_df)
            print(
                f"既存訓練データを読み込み: {len(existing_df)} レコード "
                f"(最終タイムスタンプ: {existing_df['timestamp'].max()})"
            )
        else:
            print("既存訓練データは見つかりませんでした。新規に作成します。")
    except Exception as e:
        print(f"既存訓練データの読み込みに失敗しました（無視）: {e}")
        existing_df = pd.DataFrame()

    logical_date = context.get("logical_date")
    if logical_date:
        run_date = logical_date.in_timezone(JST)
    else:
        run_date = datetime.now(JST)

    current_day = run_date.date()
    end_date = JST.localize(datetime.combine(current_day, datetime.min.time()))
    start_date = end_date - timedelta(days=1)

    start_date_utc = start_date.astimezone(pytz.UTC)
    end_date_utc = end_date.astimezone(pytz.UTC)

    print(
        "差分データ取得期間 (UTC): "
        f"{start_date_utc.isoformat()} 〜 {end_date_utc.isoformat()}"
    )

    new_dataframes = []

    # 差分対象期間の時間単位（hourly）データのみを読み込む
    try:
        current_date = start_date
        while current_date < end_date:
            current_jst = current_date.astimezone(JST)
            year = current_jst.strftime("%Y")
            month = current_jst.strftime("%m")
            day = current_jst.strftime("%d")

            prefix = f"btc-prices/hourly/year={year}/month={month}/day={day}/"
            files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix) or []

            for file_key in files:
                if not file_key.endswith(".parquet"):
                    continue
                try:
                    file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
                    parquet_data = file_obj.get()["Body"].read()
                    df = pd.read_parquet(BytesIO(parquet_data))

                    df = _ensure_timestamp_column(df)
                    mask = (df["timestamp"] >= start_date_utc) & (
                        df["timestamp"] < end_date_utc
                    )
                    df = df[mask]

                    if len(df) > 0:
                        new_dataframes.append(df)
                except Exception as e:
                    print(f"  差分ファイル処理エラー ({file_key}): {e}")
                    continue

            current_date += timedelta(days=1)
    except Exception as e:
        print(f"差分時間データ取得エラー: {e}")

    if not new_dataframes and existing_df.empty:
        raise ValueError("S3からデータを取得できませんでした（差分も既存データも空）")

    new_data_df = (
        pd.concat(new_dataframes, ignore_index=True)
        if new_dataframes
        else pd.DataFrame()
    )

    if not new_data_df.empty:
        new_data_df["timestamp"] = pd.to_datetime(new_data_df["timestamp"], utc=True)
        if not existing_df.empty:
            latest_ts = existing_df["timestamp"].max()
            new_data_df = new_data_df[new_data_df["timestamp"] > latest_ts]
        print(f"差分データ数: {len(new_data_df)} レコード")
    else:
        print("新しい差分データは見つかりませんでした。既存データのみを使用します。")

    if existing_df.empty:
        consolidated_df = new_data_df.copy()
    elif new_data_df.empty:
        consolidated_df = existing_df.copy()
    else:
        consolidated_df = pd.concat([existing_df, new_data_df], ignore_index=True)

    if consolidated_df.empty:
        raise ValueError("結合後のデータが空です")

    consolidated_df["timestamp"] = pd.to_datetime(
        consolidated_df["timestamp"], utc=True
    )
    consolidated_df = consolidated_df.sort_values("timestamp").drop_duplicates(
        subset=["timestamp"], keep="last"
    )
    consolidated_df = consolidated_df.reset_index(drop=True)

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
        "new_records": len(new_data_df),
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
        drop_na=True,
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
                    "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
                }
            )
            trainer.train_xgboost_model(
                X_train,
                y_train,
                X_val,
                y_val,
            )

            # 最良のモデルをModel Registryに登録
            try:
                model_uri = trainer.register_best_model_to_mlflow()
                if model_uri:
                    mlflow.log_param("registered_model_uri", model_uri)
            except Exception as e:
                print(f"Model registration error: {e}")
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

        return {
            "best_model_name": best_model_name,
            "best_model_base64": best_model_base64,
            "all_models_base64": all_models_base64,
            "evaluation_metrics": evaluation_summary.to_dict(),
            "feature_names": list(X.columns),
            "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
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
                "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
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

    forecast_horizon = int(
        train_info.get("forecast_horizon_hours", FORECAST_HORIZON_HOURS)
    )

    # 学習時と同じ方法で特徴量とターゲットを準備
    X_prepared, y_prepared = prepare_features_for_training(
        df,
        target_col="usd_price",
        forecast_horizon=forecast_horizon,
        drop_na=True,
    )

    if X_prepared.empty or y_prepared.empty:
        raise ValueError("予測に使用できるデータポイントがありません")

    # 最新のサンプルを使用して予測
    latest_index = X_prepared.index[-1]
    X_latest = X_prepared.loc[[latest_index]]
    actual_future_price = y_prepared.loc[latest_index]

    feature_names = train_info["feature_names"]
    missing_features = set(feature_names) - set(X_latest.columns)
    if missing_features:
        raise ValueError(f"不足している特徴量があります: {missing_features}")
    X_latest = X_latest[feature_names]

    base_timestamp = df.loc[latest_index, "timestamp"]
    base_price = df.loc[latest_index, "usd_price"]

    # モデルを読み込む
    import pickle

    model_bytes = base64.b64decode(train_info["best_model_base64"])
    model = pickle.loads(model_bytes)

    # 予測を実行
    prediction = model.predict(X_latest)[0]

    target_timestamp = pd.to_datetime(base_timestamp, utc=True) + timedelta(
        hours=forecast_horizon
    )

    # 予測結果
    result = {
        "base_timestamp": pd.to_datetime(base_timestamp, utc=True).isoformat(),
        "target_timestamp": target_timestamp.isoformat(),
        "baseline_price": float(base_price),
        "actual_future_price": float(actual_future_price),
        "predicted_price": float(prediction),
        "prediction_error": float(abs(prediction - actual_future_price)),
        "prediction_error_pct": float(
            abs(prediction - actual_future_price) / actual_future_price * 100
        )
        if actual_future_price != 0
        else None,
        "forecast_horizon_hours": forecast_horizon,
        "model_name": train_info["best_model_name"],
    }

    print("\n予測結果:")
    print(f"  基準時刻: {result['base_timestamp']}")
    print(f"  予測対象時刻: {result['target_timestamp']}")
    print(f"  現在価格: ${base_price:,.2f}")
    print(f"  実際の将来価格: ${actual_future_price:,.2f}")
    print(f"  予測価格: ${prediction:,.2f}")
    print(
        f"  誤差: ${result['prediction_error']:,.2f} ({result['prediction_error_pct']:.2f}%)"
        if result["prediction_error_pct"] is not None
        else f"  誤差: ${result['prediction_error']:,.2f} (実際の価格が0のため割合計算不可)"
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
    raw_data_info = ti.xcom_pull(task_ids="load_data_from_s3")

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
    evaluation_payload = {
        "forecast_horizon_hours": train_info.get(
            "forecast_horizon_hours", FORECAST_HORIZON_HOURS
        ),
        "metrics": evaluation_metrics,
    }
    evaluation_json = json.dumps(evaluation_payload, indent=2, default=str)
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

    # 3. 訓練データを更新して保存
    if raw_data_info:
        try:
            training_bytes = base64.b64decode(raw_data_info["parquet_data_base64"])
            s3_hook.load_bytes(
                bytes_data=training_bytes,
                key=TRAINING_DATA_S3_KEY,
                bucket_name=bucket_name,
                replace=True,
            )
            saved_files.append(TRAINING_DATA_S3_KEY)
            print(f"訓練データを更新: s3://{bucket_name}/{TRAINING_DATA_S3_KEY}")
        except Exception as e:
            print(f"訓練データの保存に失敗しました（無視）: {e}")

    # 4. 最良のモデルを保存（オプション）
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

        # 5. 特徴量名を保存（リアルタイム予測で使用）
        feature_names = train_info.get("feature_names", [])
        feature_info = {
            "feature_names": feature_names,
            "model_name": train_info["best_model_name"],
            "model_key": model_key,
            "timestamp": timestamp_str,
            "forecast_horizon_hours": train_info.get(
                "forecast_horizon_hours", FORECAST_HORIZON_HOURS
            ),
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
