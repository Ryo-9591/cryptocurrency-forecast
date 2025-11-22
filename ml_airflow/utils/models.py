"""
最適化された回帰モデルモジュール

複数の回帰モデルを実装し、訓練と評価を行う（最適化版）
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional, List
import pickle
from io import BytesIO
import base64
import os
from functools import lru_cache

try:
    import mlflow
    import mlflow.sklearn
    import mlflow.xgboost
    import mlflow.lightgbm

    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    print("Warning: mlflow is not available")

try:
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.preprocessing import StandardScaler

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

try:
    import lightgbm as lgb

    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False
    print("Warning: lightgbm is not available")


class ModelTrainer:
    """モデル訓練クラス（最適化版）"""

    def __init__(
        self,
        random_state: int = 42,
        use_mlflow: bool = True,
        enable_scaling: bool = True,
        enable_feature_selection: bool = False,
    ):
        self.random_state = random_state
        self.models: Dict[str, any] = {}
        self.scalers: Dict[str, StandardScaler] = {}
        self.evaluation_metrics: Dict[str, Dict] = {}
        self.use_mlflow = use_mlflow and MLFLOW_AVAILABLE
        self.enable_scaling = enable_scaling and SKLEARN_AVAILABLE
        self.enable_feature_selection = enable_feature_selection

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

    def _scale_features(
        self, X_train: pd.DataFrame, X_val: Optional[pd.DataFrame] = None
    ) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """特徴量の標準化"""
        if not self.enable_scaling:
            return X_train, X_val

        scaler = StandardScaler()
        X_train_scaled = pd.DataFrame(
            scaler.fit_transform(X_train),
            columns=X_train.columns,
            index=X_train.index,
        )

        if X_val is not None:
            X_val_scaled = pd.DataFrame(
                scaler.transform(X_val),
                columns=X_val.columns,
                index=X_val.index,
            )
            return X_train_scaled, X_val_scaled

        return X_train_scaled, None

    def _hyperparameter_tuning_xgboost(
        self, X_train: pd.DataFrame, y_train: pd.Series
    ) -> Dict:
        """XGBoostのハイパーパラメータチューニング（簡易版）"""
        if not XGBOOST_AVAILABLE:
            return {}

        # 時系列交差検証
        tscv = TimeSeriesSplit(n_splits=3)
        best_score = float("inf")
        best_params = {}

        # パラメータグリッド（簡易版）
        param_grid = {
            "n_estimators": [100, 200, 300],
            "max_depth": [4, 6, 8],
            "learning_rate": [0.01, 0.1, 0.2],
            "subsample": [0.8, 0.9, 1.0],
        }

        # 簡易グリッドサーチ（全探索は時間がかかるため、ランダムサンプリング）
        import random

        random.seed(self.random_state)
        np.random.seed(self.random_state)

        n_trials = min(10, len(param_grid["n_estimators"]) * len(param_grid["max_depth"]))
        trials = random.sample(
            [
                (n, d, lr, ss)
                for n in param_grid["n_estimators"]
                for d in param_grid["max_depth"]
                for lr in param_grid["learning_rate"]
                for ss in param_grid["subsample"]
            ],
            n_trials,
        )

        for n_est, max_d, lr, ss in trials:
            scores = []
            for train_idx, val_idx in tscv.split(X_train):
                X_tr, X_v = X_train.iloc[train_idx], X_train.iloc[val_idx]
                y_tr, y_v = y_train.iloc[train_idx], y_train.iloc[val_idx]

                model = xgb.XGBRegressor(
                    n_estimators=n_est,
                    max_depth=max_d,
                    learning_rate=lr,
                    subsample=ss,
                    random_state=self.random_state,
                    n_jobs=-1,
                    verbosity=0,
                )
                model.fit(X_tr, y_tr)
                pred = model.predict(X_v)
                score = np.sqrt(mean_squared_error(y_v, pred))
                scores.append(score)

            avg_score = np.mean(scores)
            if avg_score < best_score:
                best_score = avg_score
                best_params = {
                    "n_estimators": n_est,
                    "max_depth": max_d,
                    "learning_rate": lr,
                    "subsample": ss,
                }

        return best_params

    def train_xgboost_model(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame] = None,
        y_val: Optional[pd.Series] = None,
        n_estimators: int = 200,
        max_depth: int = 6,
        learning_rate: float = 0.1,
        enable_tuning: bool = True,
    ) -> Dict:
        """XGBoostモデルを訓練（最適化版）"""
        if not XGBOOST_AVAILABLE:
            raise ImportError("xgboost is required but not available")
        if not SKLEARN_AVAILABLE:
            raise ImportError("scikit-learn is required for evaluation metrics")

        print("Training XGBoost...")
        results = {}

        # ハイパーパラメータチューニング
        if enable_tuning and X_val is None:
            print("Performing hyperparameter tuning...")
            best_params = self._hyperparameter_tuning_xgboost(X_train, y_train)
            if best_params:
                n_estimators = best_params.get("n_estimators", n_estimators)
                max_depth = best_params.get("max_depth", max_depth)
                learning_rate = best_params.get("learning_rate", learning_rate)
                subsample = best_params.get("subsample", 0.9)
            else:
                subsample = 0.9
        else:
            subsample = 0.9

        try:
            # 特徴量の標準化
            X_train_scaled, X_val_scaled = self._scale_features(X_train, X_val)

            model = xgb.XGBRegressor(
                n_estimators=n_estimators,
                max_depth=max_depth,
                learning_rate=learning_rate,
                subsample=subsample,
                random_state=self.random_state,
                n_jobs=-1,
                verbosity=0,
            )

            if X_val is not None and y_val is not None:
                model.fit(
                    X_train_scaled,
                    y_train,
                    eval_set=[(X_train_scaled, y_train), (X_val_scaled, y_val)],
                    verbose=False,
                )
            else:
                model.fit(X_train_scaled, y_train)

            train_pred = model.predict(X_train_scaled)
            metrics = {
                "train_rmse": float(np.sqrt(mean_squared_error(y_train, train_pred))),
                "train_mae": float(mean_absolute_error(y_train, train_pred)),
                "train_r2": float(r2_score(y_train, train_pred)),
            }

            if X_val is not None and y_val is not None:
                val_pred = model.predict(X_val_scaled)
                metrics["val_rmse"] = float(np.sqrt(mean_squared_error(y_val, val_pred)))
                metrics["val_mae"] = float(mean_absolute_error(y_val, val_pred))
                metrics["val_r2"] = float(r2_score(y_val, val_pred))

            self.models["xgboost"] = model
            if self.enable_scaling:
                self.scalers["xgboost"] = self.scalers.get("scaler", StandardScaler())
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
                                "subsample": subsample,
                                "scaling": self.enable_scaling,
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

    def train_lightgbm_model(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame] = None,
        y_val: Optional[pd.Series] = None,
        n_estimators: int = 200,
        max_depth: int = 6,
        learning_rate: float = 0.1,
    ) -> Dict:
        """LightGBMモデルを訓練"""
        if not LIGHTGBM_AVAILABLE:
            print("LightGBM is not available, skipping...")
            return {}
        if not SKLEARN_AVAILABLE:
            raise ImportError("scikit-learn is required for evaluation metrics")

        print("Training LightGBM...")
        results = {}

        try:
            # 特徴量の標準化
            X_train_scaled, X_val_scaled = self._scale_features(X_train, X_val)

            model = lgb.LGBMRegressor(
                n_estimators=n_estimators,
                max_depth=max_depth,
                learning_rate=learning_rate,
                random_state=self.random_state,
                n_jobs=-1,
                verbosity=-1,
            )

            if X_val is not None and y_val is not None:
                model.fit(
                    X_train_scaled,
                    y_train,
                    eval_set=[(X_train_scaled, y_train), (X_val_scaled, y_val)],
                    eval_names=["train", "val"],
                    callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)],
                )
            else:
                model.fit(X_train_scaled, y_train)

            train_pred = model.predict(X_train_scaled)
            metrics = {
                "train_rmse": float(np.sqrt(mean_squared_error(y_train, train_pred))),
                "train_mae": float(mean_absolute_error(y_train, train_pred)),
                "train_r2": float(r2_score(y_train, train_pred)),
            }

            if X_val is not None and y_val is not None:
                val_pred = model.predict(X_val_scaled)
                metrics["val_rmse"] = float(np.sqrt(mean_squared_error(y_val, val_pred)))
                metrics["val_mae"] = float(mean_absolute_error(y_val, val_pred))
                metrics["val_r2"] = float(r2_score(y_val, val_pred))

            self.models["lightgbm"] = model
            if self.enable_scaling:
                self.scalers["lightgbm"] = self.scalers.get("scaler", StandardScaler())
            self.evaluation_metrics["lightgbm"] = metrics

            if self.use_mlflow:
                try:
                    with mlflow.start_run(run_name="lightgbm", nested=True):
                        mlflow.log_params(
                            {
                                "model_type": "lightgbm",
                                "n_estimators": n_estimators,
                                "max_depth": max_depth,
                                "learning_rate": learning_rate,
                                "scaling": self.enable_scaling,
                            }
                        )
                        mlflow.log_metrics(metrics)
                        mlflow.lightgbm.log_model(model, "model")
                except Exception as e:
                    print(f"MLflow logging error: {e}")

            results["lightgbm"] = metrics
        except Exception as e:
            print(f"Error training LightGBM: {e}")
            # エラーが発生しても続行

        return results

    def train_all_models(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame] = None,
        y_val: Optional[pd.Series] = None,
    ) -> Dict:
        """すべてのモデルを訓練"""
        results = {}

        # XGBoost
        try:
            xgb_results = self.train_xgboost_model(
                X_train, y_train, X_val, y_val, enable_tuning=True
            )
            results.update(xgb_results)
        except Exception as e:
            print(f"XGBoost training failed: {e}")

        # LightGBM
        try:
            lgb_results = self.train_lightgbm_model(X_train, y_train, X_val, y_val)
            results.update(lgb_results)
        except Exception as e:
            print(f"LightGBM training failed: {e}")

        return results

    def get_trained_model(
        self, metric: str = "val_rmse", lower_is_better: bool = True
    ) -> Tuple[str, any]:
        """訓練済みモデルを取得（最良のモデル）"""
        if not self.models:
            raise ValueError("No models have been trained yet")

        best_model_name = None
        best_score = float("inf") if lower_is_better else float("-inf")

        for model_name, metrics in self.evaluation_metrics.items():
            if metric in metrics:
                score = metrics[metric]
                if (lower_is_better and score < best_score) or (
                    not lower_is_better and score > best_score
                ):
                    best_score = score
                    best_model_name = model_name

        if best_model_name is None:
            # フォールバック: 最初のモデルを使用
            best_model_name = list(self.models.keys())[0]

        return best_model_name, self.models[best_model_name]

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

        # スケーリングが必要な場合
        if self.enable_scaling and model_name in self.scalers:
            X = pd.DataFrame(
                self.scalers[model_name].transform(X),
                columns=X.columns,
                index=X.index,
            )

        return self.models[model_name].predict(X)

    def save_model(self, model_name: str, filepath: str):
        """モデルを保存"""
        if model_name not in self.models:
            raise ValueError(f"Model {model_name} not found")

        with open(filepath, "wb") as f:
            pickle.dump(
                {
                    "model": self.models[model_name],
                    "scaler": self.scalers.get(model_name),
                },
                f,
            )

    def save_model_base64(self, model_name: str) -> str:
        """モデルをbase64エンコードして返す（Airflow XCom用）"""
        if model_name not in self.models:
            raise ValueError(f"Model {model_name} not found")

        buffer = BytesIO()
        pickle.dump(
            {
                "model": self.models[model_name],
                "scaler": self.scalers.get(model_name),
            },
            buffer,
        )
        buffer.seek(0)
        model_bytes = buffer.getvalue()
        return base64.b64encode(model_bytes).decode("utf-8")

    def load_model_base64(self, model_name: str, model_base64: str):
        """base64エンコードされたモデルを読み込む"""
        model_bytes = base64.b64decode(model_base64)
        buffer = BytesIO(model_bytes)
        data = pickle.load(buffer)
        self.models[model_name] = data["model"]
        if "scaler" in data and data["scaler"]:
            self.scalers[model_name] = data["scaler"]

    def get_evaluation_summary(self) -> pd.DataFrame:
        """評価結果のサマリーを取得"""
        if not self.evaluation_metrics:
            return pd.DataFrame()

        return pd.DataFrame(self.evaluation_metrics).T
