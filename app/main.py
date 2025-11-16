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

from utils.feature_engineering import (
    create_all_features,
    prepare_features_for_training,
)

TIME_COLUMNS = {"timestamp", "timestamp_jst", "retrieved_at"}

DEFAULT_FORECAST_HORIZON = int(os.getenv("FORECAST_HORIZON_HOURS", "4"))
COINGECKO_API_BASE = os.getenv("COINGECKO_API_BASE", "https://api.coingecko.com/api/v3")
COINGECKO_COIN_ID = os.getenv("COINGECKO_COIN_ID", "bitcoin")
COINGECKO_VS_CURRENCY = os.getenv("COINGECKO_VS_CURRENCY", "usd")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")


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
    # デバッグ用（任意）
    feature_alignment_ratio: Optional[float] = None
    num_zero_filled_features: Optional[int] = None
    total_expected_features: Optional[int] = None


class HealthResponse(BaseModel):
    status: str
    model_name: str
    model_version: Optional[str]


class PricePoint(BaseModel):
    timestamp: str
    price: float


class TimeSeriesResponse(BaseModel):
    points: list[PricePoint]


class ForecastSeriesResponse(BaseModel):
    model_name: str
    model_version: Optional[str] = None
    forecast_horizon_hours: int
    base_timestamp: str
    points: list[PricePoint]
    feature_alignment_ratio: Optional[float] = None
    num_zero_filled_features: Optional[int] = None
    total_expected_features: Optional[int] = None


class ModelEvaluationMetrics(BaseModel):
    train_rmse: Optional[float] = None
    train_mae: Optional[float] = None
    train_r2: Optional[float] = None
    val_rmse: Optional[float] = None
    val_mae: Optional[float] = None
    val_r2: Optional[float] = None


def _parse_cors_origins() -> list[str]:
    raw = os.getenv(
        "FRONTEND_CORS_ORIGINS",
        "http://localhost:3000,http://localhost:8081",
    )
    origins = [origin.strip() for origin in raw.split(",") if origin.strip()]
    return origins or ["http://localhost:3000"]


def _parse_cors_regex() -> Optional[str]:
    raw = os.getenv("FRONTEND_CORS_REGEX", "").strip()
    if raw:
        return raw
    return r"https?://localhost(:\d+)?$"


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

    def fetch_last_week(self) -> pd.DataFrame:
        end = datetime.now(tz=timezone.utc)
        start = end - timedelta(days=7)
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


@lru_cache(maxsize=1)
def get_market_client() -> CoinGeckoClient:
    return CoinGeckoClient()


class ModelService:
    def __init__(self) -> None:
        self.mlflow_tracking_uri = os.getenv(
            "MLFLOW_TRACKING_URI", "http://mlflow:5000"
        )
        self.model_name = os.getenv("MLFLOW_MODEL_NAME", "btc-price-prediction")
        self.model_stage = os.getenv("MLFLOW_MODEL_STAGE", "Production")
        self.market_client = get_market_client()

        mlflow.set_tracking_uri(self.mlflow_tracking_uri)
        self.model_uri = f"models:/{self.model_name}/{self.model_stage}"

        # モデル読み込み
        self.model = self._load_mlflow_model(self.model_uri)
        # 期待する特徴量名を推定
        self.expected_feature_names: list[str] | None = self._infer_expected_features(
            self.model
        )

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

    def _load_mlflow_model(self, uri: str):
        try:
            return mlflow.pyfunc.load_model(uri)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"MLflowモデルの読み込みに失敗しました: {exc}") from exc

    def _infer_expected_features(self, model) -> list[str] | None:
        """学習時の特徴量名を可能な限り推定"""
        try:
            # 方法1: 直接モデルから取得を試行
            names = getattr(model, "feature_names_in_", None)
            if names is not None:
                return [str(x) for x in list(names)]

            # 方法2: MLflow pyfuncモデルの内部構造を探索
            impl = getattr(model, "_model_impl", None)
            if impl is not None:
                py_model = getattr(impl, "python_model", None)
                if py_model is not None:
                    inner = getattr(py_model, "model", None)
                    if inner is not None:
                        names = getattr(inner, "feature_names_in_", None)
                        if names is not None:
                            return [str(x) for x in list(names)]
                        # XGBoostモデルの場合
                        if hasattr(inner, "get_booster"):
                            try:
                                booster = inner.get_booster()
                                feature_names = booster.feature_names
                                if feature_names:
                                    return [str(x) for x in feature_names]
                            except Exception:
                                pass
                        # LightGBMモデルの場合
                        if hasattr(inner, "booster_"):
                            try:
                                booster = inner.booster_
                                feature_names = booster.feature_name()
                                if feature_names:
                                    return [str(x) for x in feature_names]
                            except Exception:
                                pass
                        # sklearnラッパーの場合
                        if hasattr(inner, "estimator"):
                            estimator = inner.estimator
                            names = getattr(estimator, "feature_names_in_", None)
                            if names is not None:
                                return [str(x) for x in list(names)]

            # 方法3: MLflowのrunから特徴量数を取得（フォールバック）
            try:
                client = mlflow.tracking.MlflowClient()
                versions = client.get_latest_versions(
                    self.model_name, stages=[self.model_stage]
                )
                if versions:
                    run_id = versions[0].run_id
                    run = client.get_run(run_id)
                    # runのtagsやparamsから特徴量数を取得
                    if "features" in run.data.params:
                        feature_count = int(run.data.params["features"])
                        # 特徴量名は取得できないが、数だけでも有用
                        print(f"MLflow runから特徴量数を取得: {feature_count}")
            except Exception:
                pass

            # 方法4: 実際に予測を試みて特徴量数を推測（最後の手段）
            # これは後で実装する

        except Exception as e:
            print(f"特徴量名の推定に失敗しました: {e}")
            import traceback

            traceback.print_exc()
            return None
        return None

    def reload_model(self) -> None:
        """MLflowの指定ステージから最新モデルを再読込"""
        self.model = self._load_mlflow_model(self.model_uri)
        self.expected_feature_names = self._infer_expected_features(self.model)
        # バージョン情報更新
        try:
            client = mlflow.tracking.MlflowClient()
            versions = client.get_latest_versions(
                self.model_name, stages=[self.model_stage]
            )
            if versions:
                self.model_version = versions[0].version
        except Exception:
            self.model_version = None

    def get_evaluation_metrics(self) -> ModelEvaluationMetrics:
        """現在のProductionモデルの評価メトリクスを取得"""
        try:
            client = mlflow.tracking.MlflowClient()
            versions = client.get_latest_versions(
                self.model_name, stages=[self.model_stage]
            )
            if not versions:
                return ModelEvaluationMetrics()

            model_version = versions[0]
            run_id = model_version.run_id

            # runのメトリクスを取得
            run = client.get_run(run_id)
            metrics = run.data.metrics

            return ModelEvaluationMetrics(
                train_rmse=metrics.get("train_rmse"),
                train_mae=metrics.get("train_mae"),
                train_r2=metrics.get("train_r2"),
                val_rmse=metrics.get("val_rmse"),
                val_mae=metrics.get("val_mae"),
                val_r2=metrics.get("val_r2"),
            )
        except Exception:
            # メトリクス取得に失敗した場合は空のメトリクスを返す
            return ModelEvaluationMetrics()

    def _load_hourly_dataframe(self) -> pd.DataFrame:
        df = self.market_client.fetch_last_week()
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
    ) -> tuple[pd.DataFrame, float, datetime, dict]:
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

        diagnostics: dict = {}
        original_columns = list(X.columns)
        # 学習時の特徴量セットに整形（不足は0で補完、余分は削除）
        if self.expected_feature_names:
            inter = set(original_columns).intersection(self.expected_feature_names)
            alignment_ratio = len(inter) / max(len(self.expected_feature_names), 1)
            X = X.reindex(columns=self.expected_feature_names, fill_value=0)
            zero_filled = len(
                [c for c in self.expected_feature_names if c not in original_columns]
            )
            diagnostics = {
                "feature_alignment_ratio": alignment_ratio,
                "num_zero_filled_features": zero_filled,
                "total_expected_features": len(self.expected_feature_names),
            }
        else:
            diagnostics = {
                "feature_alignment_ratio": None,
                "num_zero_filled_features": None,
                "total_expected_features": None,
            }

        latest_index = X.index[-1]
        X_latest = X.loc[[latest_index]]
        baseline_price = float(features_df.loc[latest_index, "usd_price"])
        base_timestamp = pd.to_datetime(
            features_df.loc[latest_index, "timestamp"], utc=True
        )

        return X_latest, baseline_price, base_timestamp, diagnostics

    def predict(self, forecast_horizon: int) -> PredictionResponse:
        X_latest, baseline_price, base_timestamp, diag = self._prepare_latest_sample(
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
            feature_alignment_ratio=diag.get("feature_alignment_ratio"),
            num_zero_filled_features=diag.get("num_zero_filled_features"),
            total_expected_features=diag.get("total_expected_features"),
        )

    def predict_series(self, hours: int) -> tuple[list[PricePoint], str, dict]:
        """予測系列を生成し、評価情報も返す"""
        if hours <= 0:
            raise ValueError("hours は1以上を指定してください")
        raw_df = self._load_hourly_dataframe()

        if raw_df.empty:
            raise ValueError("価格データが取得できませんでした")

        features_df = create_all_features(
            raw_df, target_col="usd_price", timestamp_col="timestamp"
        )

        # ベースタイムスタンプを最初に1回だけ計算
        # 最新のデータポイントを使用（forecast_horizon=1で最後の行が有効になる）
        X_base, y_base = prepare_features_for_training(
            features_df,
            target_col="usd_price",
            forecast_horizon=1,
            drop_na=False,
        )
        X_base = X_base.drop(
            columns=[col for col in TIME_COLUMNS if col in X_base.columns]
        )
        X_base = X_base.replace([pd.NA, float("inf"), float("-inf")], pd.NA)

        # NaNを0で埋める（予測に必要な最小限の処理）
        # 完全にNaNの行のみ除外
        X_base = X_base.fillna(0)

        if X_base.empty:
            raise ValueError(
                f"予測系列を生成できませんでした（ベースデータがありません）。"
                f"特徴量データ: {features_df.shape}, 準備後: {X_base.shape}"
            )

        # 最新のインデックスを使用（元のfeatures_dfから）
        latest_index = features_df.index[-1]
        base_timestamp = pd.to_datetime(
            features_df.loc[latest_index, "timestamp"], utc=True
        )
        base_timestamp_str = base_timestamp.isoformat()

        # 評価情報を取得
        eval_info: dict = {}
        if self.expected_feature_names:
            # X_baseを期待する特徴量に合わせて調整
            X_base_aligned = X_base.reindex(
                columns=self.expected_feature_names, fill_value=0
            )
            if latest_index in X_base_aligned.index:
                X_latest = X_base_aligned.loc[[latest_index]]
            else:
                X_latest = X_base_aligned.iloc[[-1]]

            provided_cols = set(X_base.columns)
            expected_cols = set(self.expected_feature_names)
            aligned = len(provided_cols & expected_cols)
            total = len(expected_cols)
            zero_filled = len(expected_cols - provided_cols)
            eval_info = {
                "feature_alignment_ratio": aligned / total if total > 0 else 0.0,
                "num_zero_filled_features": zero_filled,
                "total_expected_features": total,
            }

        # 予測系列を生成（時系列予測：前の予測結果を次の予測に反映）
        out: list[PricePoint] = []
        # 予測結果を時系列データに追加するための一時的なDataFrame
        extended_df = features_df.copy()

        for h in range(1, hours + 1):
            # 現在の時点での特徴量を準備
            X, y = prepare_features_for_training(
                extended_df,
                target_col="usd_price",
                forecast_horizon=1,  # 常に1ステップ先を予測
                drop_na=False,
            )
            X = X.drop(columns=[col for col in TIME_COLUMNS if col in X.columns])
            X = X.replace([pd.NA, float("inf"), float("-inf")], pd.NA)

            # NaNを0で埋める
            X = X.fillna(0)

            if X.empty:
                continue

            # 最新のインデックスを使用
            latest_idx = extended_df.index[-1]
            if latest_idx in X.index:
                X_latest = X.loc[[latest_idx]].copy()
            else:
                # インデックスが一致しない場合は最後の行を使用
                X_latest = X.iloc[[-1]].copy()

            # モデルが期待する特徴量に合わせて調整
            if self.expected_feature_names:
                # 不足している特徴量は0で埋め、余分な特徴量は削除
                X_latest = X_latest.reindex(
                    columns=self.expected_feature_names, fill_value=0
                )

            # タイムスタンプはbase_timestampからh時間後
            target_timestamp = base_timestamp + timedelta(hours=h)
            try:
                pred = float(self.model.predict(X_latest)[0])
            except Exception as pred_exc:
                error_msg = str(pred_exc)
                # エラーメッセージから期待される特徴量数を抽出
                import re

                match = re.search(
                    r"expected[:\s]+(\d+)[,\s]+got[:\s]+(\d+)", error_msg, re.IGNORECASE
                )
                if match:
                    expected_count = int(match.group(1))
                    got_count = int(match.group(2))
                    raise RuntimeError(
                        f"特徴量数の不一致: 期待={expected_count}, 実際={got_count}. "
                        f"モデルを新しい特徴量セットで再訓練してください。"
                    ) from pred_exc
                else:
                    raise RuntimeError(
                        f"予測に失敗しました: {pred_exc}. "
                        f"特徴量数: {len(X_latest.columns)}"
                    ) from pred_exc

            out.append(PricePoint(timestamp=target_timestamp.isoformat(), price=pred))

            # 予測結果を時系列データに追加（次の予測のために）
            # 新しい行を作成
            new_row = extended_df.iloc[[-1]].copy()
            new_row.index = [len(extended_df)]  # 新しいインデックス
            new_row["timestamp"] = target_timestamp
            new_row["usd_price"] = pred  # 予測値を設定

            # 時系列データに追加
            extended_df = pd.concat([extended_df, new_row], ignore_index=False)

            # 特徴量を再計算（次の予測のために）
            # 最後の数行だけ再計算すれば効率的だが、簡易的に全体を再計算
            if h < hours:  # 最後の予測の後は再計算不要
                extended_df = create_all_features(
                    extended_df, target_col="usd_price", timestamp_col="timestamp"
                )

        if not out:
            raise ValueError("予測系列を生成できませんでした")
        return out, base_timestamp_str, eval_info


@lru_cache(maxsize=1)
def get_model_service() -> ModelService:
    return ModelService()


app = FastAPI(title="BTC Price Prediction API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_parse_cors_origins(),
    allow_origin_regex=_parse_cors_regex(),
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


@app.post("/model/reload")
def reload_model() -> dict:
    """MLflowの最新モデル（指定ステージ）を再ロード"""
    try:
        service = get_model_service()
        service.reload_model()
        return {
            "status": "reloaded",
            "model_name": service.model_name,
            "model_stage": service.model_stage,
            "model_version": service.model_version,
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/model/info")
def model_info() -> dict:
    """現在ロード済みモデルの情報"""
    service = get_model_service()
    return {
        "model_name": service.model_name,
        "model_stage": service.model_stage,
        "model_version": service.model_version,
        "expected_features": service.expected_feature_names or [],
    }


@app.get("/model/evaluation", response_model=ModelEvaluationMetrics)
def get_model_evaluation() -> ModelEvaluationMetrics:
    """現在のProductionモデルの評価メトリクスを取得"""
    try:
        service = get_model_service()
        return service.get_evaluation_metrics()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/predict", response_model=PredictionResponse)
def predict() -> PredictionResponse:
    try:
        service = get_model_service()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    forecast_horizon = DEFAULT_FORECAST_HORIZON
    try:
        resp = service.predict(forecast_horizon)
        return resp
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/timeseries", response_model=TimeSeriesResponse)
def get_timeseries(hours: int = Query(96, ge=1)) -> TimeSeriesResponse:
    try:
        client = get_market_client()
        df = client.fetch_last_week()
        df = df.sort_values("timestamp").drop_duplicates(
            subset=["timestamp"], keep="last"
        )
        if df.empty:
            raise ValueError("指定された時間範囲に価格データが存在しません")
        if "usd_price" not in df.columns:
            raise ValueError("取得したデータに usd_price 列が存在しません")
        points = [
            PricePoint(
                timestamp=pd.to_datetime(row["timestamp"], utc=True).isoformat(),
                price=float(row["usd_price"]),
            )
            for _, row in df.iterrows()
        ]
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return TimeSeriesResponse(points=points)


@app.get("/predict_series", response_model=ForecastSeriesResponse)
def predict_series(hours: int = Query(1, ge=1, le=168)) -> ForecastSeriesResponse:
    try:
        service = get_model_service()
        points, base_timestamp, eval_info = service.predict_series(hours)
        return ForecastSeriesResponse(
            model_name=service.model_name,
            model_version=service.model_version,
            forecast_horizon_hours=hours,
            base_timestamp=base_timestamp,
            points=points,
            feature_alignment_ratio=eval_info.get("feature_alignment_ratio"),
            num_zero_filled_features=eval_info.get("num_zero_filled_features"),
            total_expected_features=eval_info.get("total_expected_features"),
        )
    except ValueError as exc:
        # ValueErrorは400 Bad Requestとして返す（404ではない）
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        # RuntimeErrorは503 Service Unavailableとして返す
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
