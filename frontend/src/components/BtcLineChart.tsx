import {
  CartesianGrid,
  Line,
  LineChart,
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
): Array<{ timestamp: string; actual: number | null; forecast: number | null }> {
  const histSorted = [...historical].sort(
    (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
  );
  
  // 実績データを構築
  const data = histSorted.map((p) => ({
    timestamp: p.timestamp,
    actual: p.price,
    forecast: null
  }));

  if (forecast && forecast.length > 0) {
    const futSorted = [...forecast].sort(
      (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    );

    // 実績の最後の時刻と価格を取得
    const lastHist = data.length > 0 ? data[data.length - 1] : null;
    const lastHistTs = lastHist ? new Date(lastHist.timestamp).getTime() : NaN;
    const lastHistPrice = lastHist ? lastHist.actual : null;

    if (!isNaN(lastHistTs) && lastHistPrice !== null && futSorted.length > 0) {
      // 実績の最後の点から予測の最初の点まで繋げる
      const firstForecast = futSorted[0];
      const firstForecastTs = new Date(firstForecast.timestamp).getTime();
      
      // 実績の最後の点に予測の最初の点の価格を設定（繋げるため）
      // これにより、実績の最後の点から予測の最初の点まで線が繋がる
      if (data.length > 0) {
        data[data.length - 1] = {
          ...data[data.length - 1],
          forecast: firstForecast.price
        };
      }
      
      // 予測のすべての点を追加
      // 実績の最後の点と同じ時刻またはそれより後の時刻の予測点を追加
      for (const p of futSorted) {
        const pTs = new Date(p.timestamp).getTime();
        // 実績の最後の点と同じ時刻またはそれより後の時刻の予測点を追加
        // 同じ時刻の場合は既に実績の最後の点に設定済みだが、予測点としても追加する
        if (pTs >= lastHistTs) {
          // 実績の最後の点と同じ時刻の場合は、予測点として追加（実績の点とは別のデータポイント）
          // これにより、予測線が確実に表示される
          const existingIndex = data.findIndex(
            (d) => new Date(d.timestamp).getTime() === pTs
          );
          if (existingIndex >= 0) {
            // 既存の点がある場合は、予測値を更新
            data[existingIndex] = {
              ...data[existingIndex],
              forecast: p.price
            };
          } else {
            // 新しい点として追加
            data.push({
              timestamp: p.timestamp,
              actual: null,
              forecast: p.price
            });
          }
        }
      }
    } else if (futSorted.length > 0) {
      // 実績がない場合は、予測のみを表示
      for (const p of futSorted) {
        data.push({
          timestamp: p.timestamp,
          actual: null,
          forecast: p.price
        });
      }
    }
  }

  // タイムスタンプでソート
  data.sort(
    (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
  );
  
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
            tickFormatter={toLocalLabel}
            minTickGap={forecast && forecast.length > 0 ? 24 : 12}
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
          <Line
            type="monotone"
            dataKey="forecast"
            stroke="#f59e0b"
            strokeWidth={2.4}
            dot={false}
            connectNulls={false}
            name="予測"
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

