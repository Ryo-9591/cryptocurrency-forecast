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

function normalizeOrigin(url: URL) {
  return url.origin.replace(/\/$/, "");
}

function shouldTreatAsDockerHostname(hostname: string, currentHost: string): boolean {
  if (hostname === currentHost) {
    return false;
  }
  if (!hostname.includes(".")) {
    return true;
  }
  return false;
}

const API_BASE_URL = (() => {
  const fromEnvRaw = (import.meta.env.VITE_API_BASE_URL ?? "").trim();
  if (fromEnvRaw) {
    try {
      const envUrl = new URL(fromEnvRaw, window.location.origin);
      if (!shouldTreatAsDockerHostname(envUrl.hostname, window.location.hostname)) {
        return normalizeOrigin(envUrl);
      }
    } catch (error) {
      console.warn("VITE_API_BASE_URL が無効なため、クライアントホストにフォールバックします:", error);
    }
  }

  const { protocol, hostname } = window.location;
  const defaultPort = "8000";
  return `${protocol}//${hostname}:${defaultPort}`;
})();

async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const raw = await response.text();
    let message = raw || `request failed: ${response.status}`;
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object" && "detail" in parsed) {
        const detail = (parsed as { detail?: unknown }).detail;
        if (typeof detail === "string") {
          message = detail;
        }
      }
    } catch {
      // JSONではない場合はそのまま
    }
    throw new Error(message);
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

