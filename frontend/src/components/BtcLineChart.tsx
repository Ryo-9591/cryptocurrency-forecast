import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import type { PricePoint } from "../api";

type Props = {
  historical: PricePoint[];
  forecast?: PricePoint[] | null;
};

function toLocalLabel(timestamp: string): string {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return timestamp;
  }
  return new Intl.DateTimeFormat("ja-JP", {
    month: "numeric",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit"
  }).format(date);
}

function buildChartData(
  historical: PricePoint[],
  forecast?: PricePoint[] | null
): Array<{ 
  timestamp: number; 
  timestampStr: string;
  actual: number | null; 
  forecast: number | null;
  forecastStart: boolean;
}> {
  // 現在時刻から24時間前までの実績データを取得
  const now = new Date();
  const yesterdaySameTime = new Date(now);
  yesterdaySameTime.setDate(yesterdaySameTime.getDate() - 1);
  const yesterdaySameTimeTime = yesterdaySameTime.getTime();
  
  // 次の日の同じ時刻を計算（X軸の終点）
  const tomorrowSameTime = new Date(now);
  tomorrowSameTime.setDate(tomorrowSameTime.getDate() + 1);
  const tomorrowSameTimeTime = tomorrowSameTime.getTime();
  
  // 実績データは過去24時間分のみ
  const filteredHistorical = historical.filter(
    (p) => {
      const pTime = new Date(p.timestamp).getTime();
      return pTime >= yesterdaySameTimeTime && pTime <= now.getTime();
    }
  );
  
  const histSorted = [...filteredHistorical].sort(
    (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
  );
  
  // 実績データを構築（timestampを数値に変換）
  const data = histSorted.map((p) => ({
    timestamp: new Date(p.timestamp).getTime(),
    timestampStr: p.timestamp, // ラベル表示用
    actual: p.price,
    forecast: null as number | null,
    forecastStart: false
  }));

  // 予測開始点のタイムスタンプを特定
  let forecastStartTimestamp: number | null = null;
  if (forecast && forecast.length > 0) {
    const futSorted = [...forecast].sort(
      (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    );

    // 実績の最後の時刻と価格を取得
    const lastHist = data.length > 0 ? data[data.length - 1] : null;
    const lastHistTs = lastHist ? lastHist.timestamp : NaN;
    const lastHistPrice = lastHist ? lastHist.actual : null;

    if (!isNaN(lastHistTs) && lastHistPrice !== null && futSorted.length > 0) {
      const firstForecast = futSorted[0];
      const firstForecastTs = new Date(firstForecast.timestamp).getTime();
      forecastStartTimestamp = lastHistTs;
      
      // 実績の最後の点に予測開始マーカーを設定
      if (data.length > 0) {
        data[data.length - 1] = {
          ...data[data.length - 1],
          forecastStart: true
        };
      }
      
      // 予測のすべての点を追加
      for (const p of futSorted) {
        const pTs = new Date(p.timestamp).getTime();
        // 実績の最後の点より後の時刻の予測点を追加
        if (pTs > lastHistTs) {
          const existingIndex = data.findIndex(
            (d) => d.timestamp === pTs
          );
          if (existingIndex >= 0) {
            data[existingIndex] = {
              ...data[existingIndex],
              forecast: p.price
            };
          } else {
            data.push({
              timestamp: new Date(p.timestamp).getTime(),
              timestampStr: p.timestamp, // ラベル表示用
              actual: null,
              forecast: p.price,
              forecastStart: false
            });
          }
        }
      }
    } else if (futSorted.length > 0) {
      // 実績がない場合は、予測のみを表示
      for (const p of futSorted) {
        data.push({
          timestamp: new Date(p.timestamp).getTime(),
          timestampStr: p.timestamp, // ラベル表示用
          actual: null,
          forecast: p.price,
          forecastStart: false
        });
      }
    }
  }

  // タイムスタンプでソート
  data.sort(
    (a, b) => a.timestamp - b.timestamp
  );
  
  // X軸の範囲を1日分にするため、次の日の同じ時刻の点を追加（データがない場合）
  if (data.length > 0) {
    const lastDataTime = data[data.length - 1].timestamp;
    if (lastDataTime < tomorrowSameTimeTime) {
      // 次の日の同じ時刻の点を追加（予測がない場合でもX軸の範囲を確保）
      data.push({
        timestamp: tomorrowSameTimeTime,
        timestampStr: tomorrowSameTime.toISOString(), // ラベル表示用
        actual: null,
        forecast: null,
        forecastStart: false
      });
    }
  }
  
  return data;
}

function renderTooltipContent({
  payload,
  label
}: {
  payload?: { value: number | null; dataKey: string }[];
  label?: string;
}) {
  if (!payload || payload.length === 0 || !label) {
    return null;
  }

  const actual = payload.find((item) => item.dataKey === "actual")?.value;
  const forecast = payload.find((item) => item.dataKey === "forecast")?.value;

  return (
    <div
      style={{
        background: "white",
        borderRadius: 12,
        boxShadow: "0 12px 30px rgba(15, 23, 42, 0.12)",
        padding: "12px 16px"
      }}
    >
      <div style={{ fontSize: "0.9rem", fontWeight: 600, marginBottom: 4 }}>
        {toLocalLabel(label)}
      </div>
      {actual != null && (
        <div style={{ color: "#0369a1", fontWeight: 500 }}>
          実績: {actual.toLocaleString("ja-JP", { maximumFractionDigits: 2 })} USD
        </div>
      )}
      {forecast != null && (
        <div style={{ color: "#16a34a", fontWeight: 500 }}>
          予測: {forecast.toLocaleString("ja-JP", { maximumFractionDigits: 2 })} USD
        </div>
      )}
      {actual != null && forecast != null && (
        <div style={{ fontSize: "0.85rem", color: "#64748b", marginTop: 4 }}>
          差: {Math.abs(forecast - actual).toLocaleString("ja-JP", { maximumFractionDigits: 2 })} USD
          ({((Math.abs(forecast - actual) / actual) * 100).toFixed(2)}%)
        </div>
      )}
    </div>
  );
}

export default function BtcLineChart({ historical, forecast }: Props) {
  const chartData = buildChartData(historical, forecast);
  
  // デバッグ用: データを確認
  if (forecast && forecast.length > 0) {
    const lastHist = historical.length > 0 ? historical[historical.length - 1] : null;
    const lastHistTs = lastHist ? new Date(lastHist.timestamp).getTime() : NaN;
    const firstForecastTs = forecast.length > 0 ? new Date(forecast[0].timestamp).getTime() : NaN;
    
    console.log("実績データ数:", historical.length);
    console.log("予測データ数:", forecast.length);
    console.log("チャートデータ数:", chartData.length);
    console.log("実績の最後の点:", lastHist);
    console.log("実績の最後のタイムスタンプ:", lastHist ? new Date(lastHist.timestamp).toISOString() : "N/A");
    console.log("予測の最初の点:", forecast[0]);
    console.log("予測の最初のタイムスタンプ:", forecast.length > 0 ? new Date(forecast[0].timestamp).toISOString() : "N/A");
    console.log("タイムスタンプ比較:", {
      lastHistTs,
      firstForecastTs,
      diff: firstForecastTs - lastHistTs,
      diffHours: (firstForecastTs - lastHistTs) / (1000 * 60 * 60)
    });
    console.log("予測の最後の点:", forecast[forecast.length - 1]);
    console.log("チャートデータの最後の10点:", chartData.slice(-10));
    console.log("予測データが含まれるチャートデータ:", chartData.filter(d => d.forecast !== null).length);
  }

  // 予測開始点を特定
  const forecastStartPoint = chartData.find(d => d.forecastStart);
  const forecastStartTimestamp = forecastStartPoint?.timestamp;

  return (
    <div className="chart-wrapper">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart 
          data={chartData}
          margin={{ top: 5, right: 30, left: 20, bottom: 80 }}
        >
          <CartesianGrid stroke="#e2e8f0" strokeDasharray="4 8" />
          <XAxis
            dataKey="timestamp"
            tickFormatter={(value) => {
              const date = new Date(value);
              return toLocalLabel(date.toISOString());
            }}
            type="number"
            scale="time"
            domain={(() => {
              const now = new Date();
              const yesterdaySameTime = new Date(now);
              yesterdaySameTime.setDate(yesterdaySameTime.getDate() - 1);
              const tomorrowSameTime = new Date(now);
              tomorrowSameTime.setDate(tomorrowSameTime.getDate() + 1);
              return [yesterdaySameTime.getTime(), tomorrowSameTime.getTime()];
            })()}
            tick={{ fill: "#64748b", fontSize: 12 }}
            stroke="#64748b"
            angle={-45}
            textAnchor="end"
            height={80}
            interval="preserveStartEnd"
          />
          <YAxis
            tickFormatter={(value) =>
              value.toLocaleString("ja-JP", { maximumFractionDigits: 0 })
            }
            stroke="#64748b"
            width={80}
          />
          <Tooltip content={renderTooltipContent} />
          {/* 予測開始点の縦線 */}
          {forecastStartTimestamp && (
            <ReferenceLine
              x={forecastStartTimestamp}
              stroke="#f59e0b"
              strokeWidth={2}
              strokeDasharray="5 5"
              label={{ value: "予測開始", position: "top", fill: "#f59e0b", fontSize: 12 }}
            />
          )}
          {/* 実績データ */}
          <Line
            type="monotone"
            dataKey="actual"
            stroke="#0284c7"
            strokeWidth={2.4}
            dot={false}
            connectNulls={false}
            name="実績"
            isAnimationActive={false}
          />
          {/* 予測データ（点線 + 点表示） */}
          <Line
            type="monotone"
            dataKey="forecast"
            stroke="#16a34a"
            strokeWidth={2.4}
            strokeDasharray="8 4"
            dot={{ fill: "#16a34a", r: 4, strokeWidth: 2, stroke: "#fff" }}
            connectNulls={false}
            name="予測"
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

