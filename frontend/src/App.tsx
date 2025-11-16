import { useEffect, useMemo, useState } from "react";
import BtcLineChart from "./components/BtcLineChart";
import type { PredictionResponse, PricePoint, ModelEvaluationMetrics } from "./api";
import { fetchTimeSeries, fetchForecastSeries, fetchModelEvaluation } from "./api";

type HealthStatus = "unknown" | "online" | "offline";

function useHealthStatus(prediction: PredictionResponse | null): HealthStatus {
  if (!prediction) {
    return "unknown";
  }
  return prediction.model_name ? "online" : "unknown";
}

export default function App() {
  const [historical, setHistorical] = useState<PricePoint[]>([]);
  const [loadingInitial, setLoadingInitial] = useState(false);
  const [loadingPrediction, setLoadingPrediction] = useState(false);
  const [prediction, setPrediction] = useState<PredictionResponse | null>(null);
  const [forecast, setForecast] = useState<PricePoint[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showEvaluation, setShowEvaluation] = useState(false);
  const [evaluationMetrics, setEvaluationMetrics] = useState<ModelEvaluationMetrics | null>(null);
  const [loadingEvaluation, setLoadingEvaluation] = useState(false);

  useEffect(() => {
    (async () => {
      try {
        setLoadingInitial(true);
        const points = await fetchTimeSeries();
        setHistorical(points);
        setError(null);
      } catch (err) {
        console.error(err);
        const message =
          err instanceof Error
            ? err.message
            : "価格データの取得に失敗しました。APIを確認してください。";
        setError(message);
      } finally {
        setLoadingInitial(false);
      }
    })();
  }, []);

  const status = useHealthStatus(prediction);

  const lastUpdated = useMemo(() => {
    if (historical.length === 0) {
      return null;
    }
    return new Date(historical[historical.length - 1].timestamp);
  }, [historical]);

  const handlePredict = async () => {
    try {
      setLoadingPrediction(true);
      // 7日先まで（168時間）の予測系列を取得
      const seriesResponse = await fetchForecastSeries(168);
      const forecastPoints = seriesResponse.points || [];
      setForecast(forecastPoints);
      
      // 予測結果から評価情報を設定
      if (forecastPoints.length > 0) {
        setPrediction({
          model_name: seriesResponse.model_name,
          model_version: seriesResponse.model_version,
          forecast_horizon_hours: seriesResponse.forecast_horizon_hours,
          base_timestamp: seriesResponse.base_timestamp,
          target_timestamp: forecastPoints[forecastPoints.length - 1]?.timestamp || "",
          baseline_price: historical[historical.length - 1]?.price || 0,
          predicted_price: forecastPoints[forecastPoints.length - 1]?.price || 0,
          prediction_error: null,
          prediction_error_pct: null,
          feature_alignment_ratio: seriesResponse.feature_alignment_ratio,
          num_zero_filled_features: seriesResponse.num_zero_filled_features,
          total_expected_features: seriesResponse.total_expected_features,
        });
      }
      setError(null);
    } catch (err) {
      console.error("予測エラー:", err);
      const message =
        err instanceof Error
          ? err.message
          : "予測リクエストに失敗しました。APIログを確認してください。";
      setError(message);
    } finally {
      setLoadingPrediction(false);
    }
  };

  return (
    <div className="app">
      <header>
        <h1>BTC Price Forecast</h1>
        <p className="lead">
          直近のBTC価格と最新の予測結果を可視化します。まず履歴データを読み込み、必要に応じて「予測を実行」をクリックしてください。
        </p>
      </header>

      <div className="card">
        <BtcLineChart historical={historical} forecast={forecast || []} />

        {showEvaluation && prediction && (
          <div
            style={{
              marginTop: "16px",
              marginBottom: "16px",
              padding: "16px",
              background: "#f8fafc",
              borderRadius: "8px",
              border: "1px solid #e2e8f0"
            }}
          >
            <h3 style={{ marginTop: 0, marginBottom: "12px", fontSize: "1rem", fontWeight: 600 }}>
              モデル評価情報
            </h3>
            
            {/* 基本情報 */}
            <div style={{ marginBottom: "16px" }}>
              <h4 style={{ marginTop: 0, marginBottom: "8px", fontSize: "0.9rem", fontWeight: 600, color: "#475569" }}>
                基本情報
              </h4>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: "12px" }}>
                <div>
                  <div style={{ fontSize: "0.85rem", color: "#64748b", marginBottom: "4px" }}>モデル名</div>
                  <div style={{ fontWeight: 500 }}>{prediction.model_name}</div>
                </div>
                {prediction.model_version && (
                  <div>
                    <div style={{ fontSize: "0.85rem", color: "#64748b", marginBottom: "4px" }}>バージョン</div>
                    <div style={{ fontWeight: 500 }}>{prediction.model_version}</div>
                  </div>
                )}
                <div>
                  <div style={{ fontSize: "0.85rem", color: "#64748b", marginBottom: "4px" }}>予測期間</div>
                  <div style={{ fontWeight: 500 }}>{prediction.forecast_horizon_hours}時間</div>
                </div>
              </div>
            </div>

            {/* 評価メトリクス */}
            {evaluationMetrics && (
              <div style={{ marginBottom: "16px" }}>
                <h4 style={{ marginTop: 0, marginBottom: "8px", fontSize: "0.9rem", fontWeight: 600, color: "#475569" }}>
                  評価メトリクス
                </h4>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: "12px" }}>
                  {evaluationMetrics.train_rmse !== null && evaluationMetrics.train_rmse !== undefined && (
                    <div>
                      <div style={{ fontSize: "0.85rem", color: "#64748b", marginBottom: "4px" }}>訓練 RMSE</div>
                      <div style={{ fontWeight: 500 }}>{evaluationMetrics.train_rmse.toFixed(2)}</div>
                    </div>
                  )}
                  {evaluationMetrics.train_mae !== null && evaluationMetrics.train_mae !== undefined && (
                    <div>
                      <div style={{ fontSize: "0.85rem", color: "#64748b", marginBottom: "4px" }}>訓練 MAE</div>
                      <div style={{ fontWeight: 500 }}>{evaluationMetrics.train_mae.toFixed(2)}</div>
                    </div>
                  )}
                  {evaluationMetrics.train_r2 !== null && evaluationMetrics.train_r2 !== undefined && (
                    <div>
                      <div style={{ fontSize: "0.85rem", color: "#64748b", marginBottom: "4px" }}>訓練 R²</div>
                      <div style={{ fontWeight: 500 }}>{evaluationMetrics.train_r2.toFixed(4)}</div>
                    </div>
                  )}
                  {evaluationMetrics.val_rmse !== null && evaluationMetrics.val_rmse !== undefined && (
                    <div>
                      <div style={{ fontSize: "0.85rem", color: "#64748b", marginBottom: "4px" }}>検証 RMSE</div>
                      <div style={{ fontWeight: 500 }}>{evaluationMetrics.val_rmse.toFixed(2)}</div>
                    </div>
                  )}
                  {evaluationMetrics.val_mae !== null && evaluationMetrics.val_mae !== undefined && (
                    <div>
                      <div style={{ fontSize: "0.85rem", color: "#64748b", marginBottom: "4px" }}>検証 MAE</div>
                      <div style={{ fontWeight: 500 }}>{evaluationMetrics.val_mae.toFixed(2)}</div>
                    </div>
                  )}
                  {evaluationMetrics.val_r2 !== null && evaluationMetrics.val_r2 !== undefined && (
                    <div>
                      <div style={{ fontSize: "0.85rem", color: "#64748b", marginBottom: "4px" }}>検証 R²</div>
                      <div style={{ fontWeight: 500 }}>{evaluationMetrics.val_r2.toFixed(4)}</div>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* 予測情報 */}
            <div>
              <h4 style={{ marginTop: 0, marginBottom: "8px", fontSize: "0.9rem", fontWeight: 600, color: "#475569" }}>
                予測情報
              </h4>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: "12px" }}>
                {prediction.feature_alignment_ratio !== null && prediction.feature_alignment_ratio !== undefined && (
                  <div>
                    <div style={{ fontSize: "0.85rem", color: "#64748b", marginBottom: "4px" }}>
                      特徴量整合率
                    </div>
                    <div style={{ fontWeight: 500 }}>
                      {(prediction.feature_alignment_ratio * 100).toFixed(1)}%
                    </div>
                  </div>
                )}
                {prediction.num_zero_filled_features !== null &&
                  prediction.num_zero_filled_features !== undefined &&
                  prediction.total_expected_features !== null &&
                  prediction.total_expected_features !== undefined && (
                    <div>
                      <div style={{ fontSize: "0.85rem", color: "#64748b", marginBottom: "4px" }}>
                        0補完特徴量数
                      </div>
                      <div style={{ fontWeight: 500 }}>
                        {prediction.num_zero_filled_features} / {prediction.total_expected_features}
                      </div>
                    </div>
                  )}
                {prediction.prediction_error_pct !== null && prediction.prediction_error_pct !== undefined && (
                  <div>
                    <div style={{ fontSize: "0.85rem", color: "#64748b", marginBottom: "4px" }}>
                      予測誤差率
                    </div>
                    <div style={{ fontWeight: 500 }}>{prediction.prediction_error_pct.toFixed(2)}%</div>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        <div style={{ 
          display: "flex", 
          justifyContent: "flex-end", 
          alignItems: "center", 
          gap: "16px",
          marginTop: "16px",
          paddingTop: "16px",
          borderTop: "1px solid #e2e8f0"
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            <div className={`status ${status === "offline" ? "offline" : ""}`}>
              <span className="dot" />
              <span>
                {status === "unknown" && "予測未実行"}
                {status === "online" && "予測完了"}
                {status === "offline" && "オフライン"}
              </span>
            </div>

            {lastUpdated && (
              <div style={{ color: "#475569", fontSize: "0.9rem" }}>
                最終更新:{" "}
                {lastUpdated.toLocaleString("ja-JP", {
                  month: "numeric",
                  day: "numeric",
                  hour: "numeric",
                  minute: "2-digit"
                })}
              </div>
            )}
          </div>

          <div style={{ display: "flex", gap: "8px" }}>
            <button
              type="button"
              onClick={async () => {
                if (!showEvaluation && !evaluationMetrics) {
                  // 初回表示時にメトリクスを取得
                  try {
                    setLoadingEvaluation(true);
                    const metrics = await fetchModelEvaluation();
                    setEvaluationMetrics(metrics);
                    setShowEvaluation(true);
                  } catch (err) {
                    console.error("評価メトリクスの取得に失敗しました:", err);
                    setError("評価メトリクスの取得に失敗しました");
                  } finally {
                    setLoadingEvaluation(false);
                  }
                } else {
                  setShowEvaluation(!showEvaluation);
                }
              }}
              disabled={loadingEvaluation}
              style={{
                padding: "8px 16px",
                borderRadius: "8px",
                border: "1px solid #cbd5e1",
                background: showEvaluation ? "#e2e8f0" : "white",
                cursor: loadingEvaluation ? "not-allowed" : "pointer",
                fontSize: "0.9rem"
              }}
            >
              {loadingEvaluation ? "読み込み中..." : "モデル評価"}
            </button>
            <button
              type="button"
              onClick={handlePredict}
              disabled={loadingPrediction || loadingInitial}
              style={{
                padding: "8px 16px",
                borderRadius: "8px",
                border: "none",
                background: loadingPrediction ? "#94a3b8" : "#0284c7",
                color: "white",
                cursor: loadingPrediction || loadingInitial ? "not-allowed" : "pointer",
                fontSize: "0.9rem",
                fontWeight: 500
              }}
            >
              {loadingPrediction ? "予測中..." : "予測を実行"}
            </button>
          </div>
        </div>

        {error && <div className="error">{error}</div>}
      </div>
    </div>
  );
}

