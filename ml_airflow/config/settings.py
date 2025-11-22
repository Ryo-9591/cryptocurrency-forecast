"""設定管理モジュール"""
import os
from typing import Optional


class Settings:
    """アプリケーション設定"""

    # AWS設定
    AWS_ACCESS_KEY_ID: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_DEFAULT_REGION: str = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-1")
    S3_BUCKET_NAME: Optional[str] = os.getenv("S3_BUCKET_NAME")

    # CoinGecko設定
    COINGECKO_API_KEY: Optional[str] = os.getenv("COINGECKO_API_KEY")
    COINGECKO_API_BASE: str = os.getenv(
        "COINGECKO_API_BASE", "https://api.coingecko.com/api/v3"
    )
    COINGECKO_COIN_ID: str = os.getenv("COINGECKO_COIN_ID", "bitcoin")
    COINGECKO_VS_CURRENCY: str = os.getenv("COINGECKO_VS_CURRENCY", "usd")

    # MLflow設定
    MLFLOW_TRACKING_URI: str = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    MLFLOW_EXPERIMENT_NAME: str = os.getenv(
        "MLFLOW_EXPERIMENT_NAME", "btc-price-prediction"
    )
    MLFLOW_MODEL_NAME: str = os.getenv("MLFLOW_MODEL_NAME", "btc-price-prediction")
    MLFLOW_MODEL_STAGE: str = os.getenv("MLFLOW_MODEL_STAGE", "Production")
    USE_MLFLOW: bool = os.getenv("USE_MLFLOW", "true").lower() == "true"
    AUTO_PROMOTE_MLFLOW_MODEL: bool = (
        os.getenv("AUTO_PROMOTE_MLFLOW_MODEL", "true").lower() == "true"
    )

    # データ設定
    DAILY_PARQUET_KEY: str = os.getenv(
        "DAILY_PARQUET_KEY", "btc-prices/daily/btc_prices.parquet"
    )
    FORECAST_HORIZON_HOURS: int = int(os.getenv("FORECAST_HORIZON_HOURS", "4"))

    # モデル訓練設定
    RANDOM_STATE: int = int(os.getenv("RANDOM_STATE", "42"))
    TRAIN_TEST_SPLIT: float = float(os.getenv("TRAIN_TEST_SPLIT", "0.2"))
    ENABLE_FEATURE_SELECTION: bool = (
        os.getenv("ENABLE_FEATURE_SELECTION", "true").lower() == "true"
    )
    ENABLE_HYPERPARAMETER_TUNING: bool = (
        os.getenv("ENABLE_HYPERPARAMETER_TUNING", "true").lower() == "true"
    )

    # CORS設定
    FRONTEND_CORS_ORIGINS: str = os.getenv(
        "FRONTEND_CORS_ORIGINS", "http://localhost:3000,http://localhost:8081"
    )
    FRONTEND_CORS_REGEX: Optional[str] = os.getenv("FRONTEND_CORS_REGEX")

