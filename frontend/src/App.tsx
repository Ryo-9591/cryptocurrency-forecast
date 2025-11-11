import { useEffect, useMemo, useState } from "react";
import BtcLineChart from "./components/BtcLineChart";
import type { PredictionResponse, PricePoint } from "./api";
import { fetchTimeSeries, requestPrediction } from "./api";

type HealthStatus = "unknown" | "online" | "offline";

function useHealthStatus(prediction: PredictionResponse | null): HealthStatus {
  if (!prediction) {
    return "unknown";
  }
  return prediction.model_name ? "online" : "unknown";
}

export default function App() {
  const [historical, setHistorical] = useState<PricePoint[]>([]);
  const [hours, setHours] = useState(72);
  const [loadingInitial, setLoadingInitial] = useState(false);
  const [loadingPrediction, setLoadingPrediction] = useState(false);
  const [prediction, setPrediction] = useState<PredictionResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      try {
        setLoadingInitial(true);
        const points = await fetchTimeSeries(hours);
        setHistorical(points);
        setError(null);
      } catch (err) {
        console.error(err);
        setError("価格データの取得に失敗しました。APIを確認してください。");
      } finally {
        setLoadingInitial(false);
      }
    })();
  }, [hours]);

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
      const result = await requestPrediction();
      setPrediction(result);
      setError(null);
    } catch (err) {
      console.error(err);
      setError("予測リクエストに失敗しました。APIログを確認してください。");
    } finally {
      setLoadingPrediction(false);
    }
  };

  const handleHoursChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
    const value = Number.parseInt(event.target.value, 10);
    if (!Number.isNaN(value)) {
      setHours(value);
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
        <div className="controls">
          <button
            type="button"
            onClick={handlePredict}
            disabled={loadingPrediction || loadingInitial}
          >
            {loadingPrediction ? "予測中..." : "予測を実行"}
          </button>

          <label style={{ color: "#475569", fontSize: "0.9rem" }}>
            表示期間
            <select
              onChange={handleHoursChange}
              value={hours}
              style={{
                marginLeft: 8,
                borderRadius: 12,
                border: "1px solid #cbd5f5",
                padding: "8px 12px",
                fontSize: "0.9rem"
              }}
            >
              <option value={24}>24時間</option>
              <option value={48}>48時間</option>
              <option value={72}>72時間</option>
              <option value={96}>96時間</option>
            </select>
          </label>

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

        <BtcLineChart historical={historical} prediction={prediction} />

        {error && <div className="error">{error}</div>}
      </div>
    </div>
  );
}

