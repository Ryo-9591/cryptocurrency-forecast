import os
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Optional

import mlflow
import pandas as pd
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from utils.feature_engineering import create_all_features, prepare_features_for_training

UTC = timezone.utc
JST = timezone(timedelta(hours=9))

TIME_COLUMNS = {"timestamp", "timestamp_jst", "retrieved_at"}

DEFAULT_FORECAST_HORIZON = int(os.getenv("FORECAST_HORIZON_HOURS", "4"))
MAX_HOURLY_FILES = int(os.getenv("FASTAPI_MAX_HOURLY_FILES", "96"))
COINGECKO_API_BASE = os.getenv("COINGECKO_API_BASE", "https://api.coingecko.com/api/v3")
COINGECKO_COIN_ID = os.getenv("COINGECKO_COIN_ID", "bitcoin")
COINGECKO_VS_CURRENCY = os.getenv("COINGECKO_VS_CURRENCY", "usd")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")


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


class PricePoint(BaseModel):
    timestamp: str
    price: float


class TimeSeriesResponse(BaseModel):
    points: list[PricePoint]


def _parse_cors_origins() -> list[str]:
    raw = os.getenv(
        "FRONTEND_CORS_ORIGINS",
        "http://localhost:3000,http://localhost:8081",
    )
    origins = [origin.strip() for origin in raw.split(",") if origin.strip()]
    return origins or ["http://localhost:3000"]


class CoinGeckoClient:
    def __init__(
        self,
        base_url: str = COINGECKO_API_BASE,
        coin_id: str = COINGECKO_COIN_ID,
        vs_currency: str = COINGECKO_VS_CURRENCY,
        api_key: Optional[str] = COINGECKO_API_KEY,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.coin_id = coin_id
        self.vs_currency = vs_currency
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "cryptocurrency-forecast/1.0",
                "Accept": "application/json",
            }
        )
        if api_key:
            self.session.headers["x-cg-pro-api-key"] = api_key

    def fetch_recent_hours(self, hours: int) -> pd.DataFrame:
        end = datetime.now(tz=UTC)
        start = end - timedelta(hours=hours)
        return self._fetch_range(start, end)

    def _fetch_range(self, start: datetime, end: datetime) -> pd.DataFrame:
        params = {
            "vs_currency": self.vs_currency,
            "from": int(start.timestamp()),
            "to": int(end.timestamp()),
        }
        url = f"{self.base_url}/coins/{self.coin_id}/market_chart/range"

        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(f"CoinGecko APIリクエストに失敗しました: {exc}") from exc

        payload = response.json()
        prices = payload.get("prices")
        if not prices:
            raise ValueError("CoinGecko APIから価格データを取得できませんでした")

        df_prices = pd.DataFrame(prices, columns=["timestamp_ms", "usd_price"])
        df_prices["timestamp"] = pd.to_datetime(
            df_prices["timestamp_ms"], unit="ms", utc=True, errors="coerce"
        )
        df_prices = df_prices.dropna(subset=["timestamp"])
        df_prices = df_prices.drop(columns=["timestamp_ms"])
        df_prices = df_prices.sort_values("timestamp").drop_duplicates(
            subset=["timestamp"], keep="last"
        )

        df_prices = (
            df_prices.set_index("timestamp")
            .resample("1H")
            .last()
            .ffill()
            .dropna()
            .reset_index()
        )

        return df_prices


class ModelService:
    def __init__(self) -> None:
        self.mlflow_tracking_uri = os.getenv(
            "MLFLOW_TRACKING_URI", "http://mlflow:5000"
        )
        self.model_name = os.getenv("MLFLOW_MODEL_NAME", "btc-price-prediction")
        self.model_stage = os.getenv("MLFLOW_MODEL_STAGE", "Production")
        self.market_client = CoinGeckoClient()

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

    def _load_hourly_dataframe(self) -> pd.DataFrame:
        hours = MAX_HOURLY_FILES + 48
        df = self.market_client.fetch_recent_hours(hours)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.sort_values("timestamp").drop_duplicates(
            subset=["timestamp"], keep="last"
        )
        df = df.reset_index(drop=True)
        if "usd_price" not in df.columns:
            raise ValueError("価格データに usd_price 列が含まれていません")
        df["usd_price"] = df["usd_price"].astype(float)
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

    def get_recent_prices(self, hours: int) -> list[PricePoint]:
        if hours <= 0:
            raise ValueError("hours は1以上を指定してください")

        df = self._load_hourly_dataframe()
        cutoff = datetime.now(tz=UTC) - timedelta(hours=hours)
        df = df[df["timestamp"] >= cutoff]
        df = df.sort_values("timestamp")

        if df.empty:
            raise ValueError("指定された時間範囲に価格データが存在しません")

        if "usd_price" not in df.columns:
            raise ValueError("データセットに usd_price 列が存在しません")

        return [
            PricePoint(
                timestamp=pd.to_datetime(row["timestamp"], utc=True).isoformat(),
                price=float(row["usd_price"]),
            )
            for _, row in df.iterrows()
        ]


@lru_cache(maxsize=1)
def get_model_service() -> ModelService:
    return ModelService()


app = FastAPI(title="BTC Price Prediction API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_parse_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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


@app.get("/timeseries", response_model=TimeSeriesResponse)
def get_timeseries(hours: int = Query(96, ge=1)) -> TimeSeriesResponse:
    service = get_model_service()
    clamped_hours = min(hours, MAX_HOURLY_FILES)
    try:
        points = service.get_recent_prices(clamped_hours)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return TimeSeriesResponse(points=points)
