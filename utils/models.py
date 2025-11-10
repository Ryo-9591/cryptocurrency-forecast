"""
回帰モデルモジュール

複数の回帰モデルを実装し、訓練と評価を行う
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
import pickle
from io import BytesIO
import base64
import os

try:
    import mlflow
    import mlflow.sklearn
    import mlflow.xgboost

    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    print("Warning: mlflow is not available")

try:
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("Warning: scikit-learn is not available")

try:
    import xgboost as xgb

    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False
    print("Warning: xgboost is not available")


class ModelTrainer:
    """モデル訓練クラス"""

    def __init__(self, random_state: int = 42, use_mlflow: bool = True):
        self.random_state = random_state
        self.models = {}
        self.scalers = {}
        self.evaluation_metrics = {}
        self.use_mlflow = use_mlflow and MLFLOW_AVAILABLE

        # MLflow設定
        if self.use_mlflow:
            mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
            mlflow.set_tracking_uri(mlflow_tracking_uri)
            self.experiment_name = os.getenv(
                "MLFLOW_EXPERIMENT_NAME", "btc-price-prediction"
            )
            try:
                mlflow.set_experiment(self.experiment_name)
            except Exception as e:
                print(f"MLflow experiment設定エラー: {e}")
                self.use_mlflow = False

    def train_xgboost_model(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame] = None,
        y_val: Optional[pd.Series] = None,
        n_estimators: int = 100,
        max_depth: int = 6,
        learning_rate: float = 0.1,
    ) -> Dict:
        """XGBoostモデルを訓練"""
        if not XGBOOST_AVAILABLE:
            raise ImportError("xgboost is required but not available")
        if not SKLEARN_AVAILABLE:
            raise ImportError("scikit-learn is required for evaluation metrics")

        print("Training XGBoost...")
        results = {}
        try:
            model = xgb.XGBRegressor(
                n_estimators=n_estimators,
                max_depth=max_depth,
                learning_rate=learning_rate,
                random_state=self.random_state,
                n_jobs=-1,
            )

            if X_val is not None and y_val is not None:
                model.fit(
                    X_train,
                    y_train,
                    eval_set=[(X_train, y_train), (X_val, y_val)],
                    verbose=False,
                )
            else:
                model.fit(X_train, y_train)

            train_pred = model.predict(X_train)
            metrics = {
                "train_rmse": float(np.sqrt(mean_squared_error(y_train, train_pred))),
                "train_mae": float(mean_absolute_error(y_train, train_pred)),
                "train_r2": float(r2_score(y_train, train_pred)),
            }

            if X_val is not None and y_val is not None:
                val_pred = model.predict(X_val)
                metrics["val_rmse"] = float(
                    np.sqrt(mean_squared_error(y_val, val_pred))
                )
                metrics["val_mae"] = float(mean_absolute_error(y_val, val_pred))
                metrics["val_r2"] = float(r2_score(y_val, val_pred))

            self.models["xgboost"] = model
            self.evaluation_metrics["xgboost"] = metrics

            if self.use_mlflow:
                try:
                    with mlflow.start_run(run_name="xgboost", nested=True):
                        mlflow.log_params(
                            {
                                "model_type": "xgboost",
                                "n_estimators": n_estimators,
                                "max_depth": max_depth,
                                "learning_rate": learning_rate,
                            }
                        )
                        mlflow.log_metrics(metrics)
                        mlflow.xgboost.log_model(model, "model")
                except Exception as e:
                    print(f"MLflow logging error: {e}")

            results["xgboost"] = metrics
        except Exception as e:
            print(f"Error training XGBoost: {e}")
            raise

        return results

    def get_trained_model(
        self, metric: str = "val_rmse", lower_is_better: bool = True
    ) -> Tuple[str, object]:
        """訓練済みXGBoostモデルを取得"""
        if "xgboost" not in self.models:
            raise ValueError("XGBoost model has not been trained yet")

        metrics = self.evaluation_metrics.get("xgboost", {})
        if metric not in metrics and metrics:
            raise ValueError(f"Metric {metric} not found in evaluation metrics")

        return "xgboost", self.models["xgboost"]

    def register_best_model_to_mlflow(
        self, metric: str = "val_rmse", model_name: str = "btc-price-prediction"
    ) -> Optional[str]:
        """最良のモデルをMLflow Model Registryに登録"""
        if not self.use_mlflow:
            print("MLflow is not available or not enabled")
            return None

        try:
            best_model_name, best_model = self.get_trained_model(metric=metric)

            # 最新のrunを検索
            experiment = mlflow.get_experiment_by_name(self.experiment_name)
            if experiment is None:
                print(f"Experiment {self.experiment_name} not found")
                return None

            runs = mlflow.search_runs(
                experiment_ids=[experiment.experiment_id],
                filter_string=f"tags.mlflow.runName = '{best_model_name}'",
                order_by=["start_time DESC"],
                max_results=1,
            )

            if runs.empty:
                print(f"No runs found for {best_model_name}")
                return None

            run_id = runs.iloc[0]["run_id"]
            model_uri = f"runs:/{run_id}/model"

            # Model Registryに登録
            registered_model_name = model_name
            try:
                result = mlflow.register_model(model_uri, registered_model_name)
                print(f"Model registered: {result.name} version {result.version}")

                # 最新バージョンをProductionに移行
                client = mlflow.tracking.MlflowClient()
                latest_version = client.get_latest_versions(
                    registered_model_name, stages=[]
                )[0]
                client.transition_model_version_stage(
                    name=registered_model_name,
                    version=latest_version.version,
                    stage="Production",
                )
                print(
                    f"Model version {latest_version.version} transitioned to Production"
                )

                return f"models:/{registered_model_name}/Production"
            except Exception as e:
                print(f"Model registration error: {e}")
                return None

        except Exception as e:
            print(f"MLflow model registration error: {e}")
            return None

    def predict(self, model_name: str, X: pd.DataFrame) -> np.ndarray:
        """指定されたモデルで予測"""
        if model_name not in self.models:
            raise ValueError(f"Model {model_name} not found")

        return self.models[model_name].predict(X)

    def save_model(self, model_name: str, filepath: str):
        """モデルを保存"""
        if model_name not in self.models:
            raise ValueError(f"Model {model_name} not found")

        with open(filepath, "wb") as f:
            pickle.dump(self.models[model_name], f)

    def save_model_base64(self, model_name: str) -> str:
        """モデルをbase64エンコードして返す（Airflow XCom用）"""
        if model_name not in self.models:
            raise ValueError(f"Model {model_name} not found")

        buffer = BytesIO()
        pickle.dump(self.models[model_name], buffer)
        buffer.seek(0)
        model_bytes = buffer.getvalue()
        return base64.b64encode(model_bytes).decode("utf-8")

    def load_model_base64(self, model_name: str, model_base64: str):
        """base64エンコードされたモデルを読み込む"""
        model_bytes = base64.b64decode(model_base64)
        buffer = BytesIO(model_bytes)
        self.models[model_name] = pickle.load(buffer)

    def get_evaluation_summary(self) -> pd.DataFrame:
        """評価結果のサマリーを取得"""
        if not self.evaluation_metrics:
            return pd.DataFrame()

        return pd.DataFrame(self.evaluation_metrics).T
