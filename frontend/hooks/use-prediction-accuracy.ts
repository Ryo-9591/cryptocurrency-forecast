import { usePredictionContext } from '@/contexts/prediction-context'

// 予測精度を取得するカスタムフック
export function usePredictionAccuracy() {
  const { averageAccuracy } = usePredictionContext()
  return averageAccuracy
}

