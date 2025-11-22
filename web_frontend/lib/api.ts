// API設定
const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000'
const WS_BASE_URL = process.env.NEXT_PUBLIC_WS_BASE_URL || 'ws://localhost:8000'

export interface PricePoint {
  timestamp: string
  price: number
}

export interface PredictionResponse {
  model_name: string
  model_version?: string
  forecast_horizon_hours: number
  base_timestamp: string
  target_timestamp: string
  baseline_price: number
  predicted_price: number
  prediction_error?: number
  prediction_error_pct?: number
}

export interface TimeSeriesResponse {
  points: PricePoint[]
}

export interface ForecastSeriesResponse {
  model_name: string
  model_version?: string
  forecast_horizon_hours: number
  base_timestamp: string
  points: PricePoint[]
}

// 予測を実行
export async function executePrediction(): Promise<PredictionResponse> {
  const response = await fetch(`${API_BASE_URL}/predict`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
  })
  
  if (!response.ok) {
    let errorMessage = `予測の実行に失敗しました: ${response.statusText}`
    try {
      const errorData = await response.json()
      if (errorData.detail) {
        errorMessage = errorData.detail
      }
    } catch {
      // JSON解析に失敗した場合はデフォルトメッセージを使用
    }
    throw new Error(errorMessage)
  }
  
  return response.json()
}

// 時系列データを取得
export async function getTimeSeries(hours: number = 24): Promise<TimeSeriesResponse> {
  const response = await fetch(`${API_BASE_URL}/timeseries?hours=${hours}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })
  
  if (!response.ok) {
    let errorMessage = `時系列データの取得に失敗しました: ${response.statusText}`
    try {
      const errorData = await response.json()
      if (errorData.detail) {
        errorMessage = errorData.detail
      }
    } catch {
      // JSON解析に失敗した場合はデフォルトメッセージを使用
    }
    throw new Error(errorMessage)
  }
  
  return response.json()
}

// 予測シリーズを取得
export async function getPredictSeries(hours: number = 1): Promise<ForecastSeriesResponse> {
  const response = await fetch(`${API_BASE_URL}/predict_series?hours=${hours}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })
  
  if (!response.ok) {
    throw new Error(`予測シリーズの取得に失敗しました: ${response.statusText}`)
  }
  
  return response.json()
}

// 現在の価格を取得（簡易版）
export async function getCurrentPrice(): Promise<number> {
  const response = await getTimeSeries(1)
  if (response.points.length === 0) {
    throw new Error('価格データが取得できませんでした')
  }
  // 最新の価格を返す
  return response.points[response.points.length - 1].price
}

