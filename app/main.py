import os
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from io import BytesIO
from typing import Optional, Sequence

import boto3
import mlflow
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from utils.feature_engineering import create_all_features, prepare_features_for_training

UTC = timezone.utc
JST = timezone(timedelta(hours=9))

TIME_COLUMNS = {"timestamp", "timestamp_jst", "retrieved_at"}

DEFAULT_FORECAST_HORIZON = int(os.getenv("FORECAST_HORIZON_HOURS", "4"))
MAX_HOURLY_FILES = int(os.getenv("FASTAPI_MAX_HOURLY_FILES", "96"))
S3_HOURLY_PREFIX = os.getenv("HOURLY_S3_PREFIX", "btc-prices/hourly/")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

if not S3_BUCKET_NAME:
    raise RuntimeError("S3_BUCKET_NAME 環境変数が設定されていません")


def _ensure_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    candidates = ["timestamp", "datetime", "date", "time", "ts"]
    for column in df.columns:
        if column.lower() in candidates:
            out = df.copy()
            out["timestamp"] = pd.to_datetime(out[column], utc=True, errors="coerce")
            return out
    raise KeyError("timestamp")


class PredictionResponse(BaseModel):
    model_name: str
    model_version: Optional[str]
    forecast_horizon_hours: int
    base_timestamp: str
    target_timestamp: str
    baseline_price: float
    predicted_price: float
    prediction_error: Optional[float] = None
    prediction_error_pct: Optional[float] = None


class HealthResponse(BaseModel):
    status: str
    model_name: str
    model_version: Optional[str]


class ModelService:
    def __init__(self) -> None:
        self.mlflow_tracking_uri = os.getenv(
            "MLFLOW_TRACKING_URI", "http://mlflow:5000"
        )
        self.model_name = os.getenv("MLFLOW_MODEL_NAME", "btc-price-prediction")
        self.model_stage = os.getenv("MLFLOW_MODEL_STAGE", "Production")

        mlflow.set_tracking_uri(self.mlflow_tracking_uri)
        self.model_uri = f"models:/{self.model_name}/{self.model_stage}"

        try:
            self.model = mlflow.pyfunc.load_model(self.model_uri)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"MLflowモデルの読み込みに失敗しました: {exc}") from exc

        self.model_version = None
        try:
            client = mlflow.tracking.MlflowClient()
            versions = client.get_latest_versions(
                self.model_name, stages=[self.model_stage]
            )
            if versions:
                self.model_version = versions[0].version
        except Exception:
            # バージョン情報が取得できなくても推論は継続できる
            self.model_version = None

        self.s3_client = boto3.client("s3")

    def _list_recent_keys(self) -> Sequence[str]:
        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=S3_HOURLY_PREFIX)
        keys: list[str] = []
        for page in pages:
            contents = page.get("Contents", [])
            for obj in contents:
                key = obj.get("Key")
                if key and key.endswith(".parquet"):
                    keys.append(key)
        if not keys:
            return []
        keys.sort()
        return keys[-MAX_HOURLY_FILES :]

    def _load_hourly_dataframe(self) -> pd.DataFrame:
        keys = self._list_recent_keys()
        if not keys:
            raise ValueError("S3に利用可能なhourlyデータが存在しません")

        frames: list[pd.DataFrame] = []
        for key in keys:
            try:
                obj = self.s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
                data = obj["Body"].read()
                df = pd.read_parquet(BytesIO(data))
                df = _ensure_timestamp_column(df)
                frames.append(df)
            except Exception as exc:  # noqa: BLE001
                print(f"[predict] S3読み込み失敗 ({key}): {exc}")
                continue

        if not frames:
            raise ValueError("S3から読み込んだhourlyデータが空です")

        df = pd.concat(frames, ignore_index=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.sort_values("timestamp").drop_duplicates(
            subset=["timestamp"], keep="last"
        )
        df = df.reset_index(drop=True)
        return df

    def _prepare_latest_sample(
        self, forecast_horizon: int
    ) -> tuple[pd.DataFrame, float, datetime]:
        raw_df = self._load_hourly_dataframe()
        features_df = create_all_features(
            raw_df, target_col="usd_price", timestamp_col="timestamp"
        )

        X, y = prepare_features_for_training(
            features_df,
            target_col="usd_price",
            forecast_horizon=forecast_horizon,
            drop_na=False,
        )

        X = X.drop(columns=[col for col in TIME_COLUMNS if col in X.columns])
        X = X.replace([pd.NA, float("inf"), float("-inf")], pd.NA)
        y = y.replace([pd.NA, float("inf"), float("-inf")], pd.NA)

        valid_mask = ~(X.isna().any(axis=1) | y.isna())
        X = X[valid_mask]
        y = y[valid_mask]

        if X.empty or y.empty:
            raise ValueError("特徴量の準備に失敗しました（有効サンプルがありません）")

        latest_index = X.index[-1]
        X_latest = X.loc[[latest_index]]
        baseline_price = float(features_df.loc[latest_index, "usd_price"])
        base_timestamp = pd.to_datetime(
            features_df.loc[latest_index, "timestamp"], utc=True
        )

        return X_latest, baseline_price, base_timestamp

    def predict(self, forecast_horizon: int) -> PredictionResponse:
        X_latest, baseline_price, base_timestamp = self._prepare_latest_sample(
            forecast_horizon
        )
        prediction = float(self.model.predict(X_latest)[0])

        target_timestamp = base_timestamp + timedelta(hours=forecast_horizon)
        error = prediction - baseline_price

        return PredictionResponse(
            model_name=self.model_name,
            model_version=self.model_version,
            forecast_horizon_hours=forecast_horizon,
            base_timestamp=base_timestamp.isoformat(),
            target_timestamp=target_timestamp.isoformat(),
            baseline_price=baseline_price,
            predicted_price=prediction,
            prediction_error=error,
            prediction_error_pct=abs(error) / baseline_price * 100
            if baseline_price
            else None,
        )


@lru_cache(maxsize=1)
def get_model_service() -> ModelService:
    return ModelService()


app = FastAPI(title="BTC Price Prediction API", version="1.0.0")


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    service = get_model_service()
    return HealthResponse(
        status="ok",
        model_name=service.model_name,
        model_version=service.model_version,
    )


@app.post("/predict", response_model=PredictionResponse)
def predict() -> PredictionResponse:
    service = get_model_service()
    forecast_horizon = DEFAULT_FORECAST_HORIZON
    try:
        return service.predict(forecast_horizon)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

