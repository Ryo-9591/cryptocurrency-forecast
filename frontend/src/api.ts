export type PricePoint = {
  timestamp: string;
  price: number;
};

export type PredictionResponse = {
  model_name: string;
  model_version?: string | null;
  forecast_horizon_hours: number;
  base_timestamp: string;
  target_timestamp: string;
  baseline_price: number;
  predicted_price: number;
  prediction_error?: number | null;
  prediction_error_pct?: number | null;
};

export type TimeSeriesResponse = {
  points: PricePoint[];
};

const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `request failed: ${response.status}`);
  }
  return (await response.json()) as T;
}

export async function fetchTimeSeries(
  hours: number = 96
): Promise<PricePoint[]> {
  const url = new URL("/timeseries", API_BASE_URL);
  url.searchParams.set("hours", `${hours}`);
  const response = await fetch(url.toString());
  const body = await handleResponse<TimeSeriesResponse>(response);
  return body.points;
}

export async function requestPrediction(): Promise<PredictionResponse> {
  const url = new URL("/predict", API_BASE_URL);
  const response = await fetch(url.toString(), {
    method: "POST",
    headers: { "Content-Type": "application/json" }
  });
  return handleResponse<PredictionResponse>(response);
}

