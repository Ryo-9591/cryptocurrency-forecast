import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import type { PricePoint, PredictionResponse } from "../api";

type ChartDatum = {
  timestamp: string;
  actual: number | null;
  forecast: number | null;
};

type Props = {
  historical: PricePoint[];
  prediction?: PredictionResponse | null;
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
  prediction?: PredictionResponse | null
): ChartDatum[] {
  const sorted = [...historical].sort(
    (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
  );

  const data: ChartDatum[] = sorted.map((point) => ({
    timestamp: point.timestamp,
    actual: point.price,
    forecast: null
  }));

  if (!prediction) {
    return data;
  }

  const baseTimestamp = prediction.base_timestamp;
  const targetTimestamp = prediction.target_timestamp;
  const baselinePrice = prediction.baseline_price;
  const predictedPrice = prediction.predicted_price;

  const existingBaseIndex = data.findIndex(
    (entry) => entry.timestamp === baseTimestamp
  );

  if (existingBaseIndex >= 0) {
    data[existingBaseIndex].forecast = baselinePrice;
  } else {
    data.push({
      timestamp: baseTimestamp,
      actual: null,
      forecast: baselinePrice
    });
  }

  data.push({
    timestamp: targetTimestamp,
    actual: null,
    forecast: predictedPrice
  });

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

export default function BtcLineChart({ historical, prediction }: Props) {
  const chartData = buildChartData(historical, prediction);

  return (
    <div className="chart-wrapper">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={chartData}>
          <CartesianGrid stroke="#e2e8f0" strokeDasharray="4 8" />
          <XAxis
            dataKey="timestamp"
            tickFormatter={toLocalLabel}
            minTickGap={24}
            stroke="#64748b"
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
            connectNulls
            name="実績"
          />
          <Line
            type="monotone"
            dataKey="forecast"
            stroke="#22c55e"
            strokeWidth={2.4}
            dot={{ r: 4 }}
            connectNulls
            name="予測"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

