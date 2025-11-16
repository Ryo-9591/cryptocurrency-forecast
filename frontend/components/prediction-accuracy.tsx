'use client'

import { useCallback, useEffect, useState, useRef } from 'react'
import { executePrediction as executePredictionAPI, getCurrentPrice, type PredictionResponse } from '@/lib/api'
import { usePredictionContext, type Prediction } from '@/contexts/prediction-context'

function formatTimeAgo(date: Date): string {
  const now = new Date()
  const diffMs = now.getTime() - date.getTime()
  const diffMinutes = Math.floor(diffMs / (1000 * 60))
  const diffHours = Math.floor(diffMs / (1000 * 60 * 60))
  
  if (diffMinutes < 1) {
    return '0分前'
  }
  if (diffHours < 1) {
    return `${diffMinutes}分前`
  }
  return `${diffHours}時間前`
}

// calculateAccuracy関数はコンテキストで定義されているが、
// ここでも使用するため、同じ実装を保持
function calculateAccuracy(predicted: number, actual: number): number {
  const error = Math.abs(predicted - actual)
  const accuracy = (1 - error / actual) * 100
  return Math.max(0, Math.min(100, accuracy))
}

function formatPrice(price: number): string {
  return `$${price.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`
}

export function PredictionAccuracy() {
  const { predictions, addPrediction, updatePrediction } = usePredictionContext()
  const [isRunning, setIsRunning] = useState(true)
  const lastPredictionTimeRef = useRef<number>(0)

  // 4時間ごとに予測を実行（常に4時間）
  const PREDICTION_INTERVAL_MS = 4 * 60 * 60 * 1000 // 4時間
  const ACTUAL_PRICE_CHECK_DELAY_MS = 4 * 60 * 60 * 1000 // 4時間（予測のターゲット時刻）

  // 実測値を取得する関数
  const fetchActualPrice = useCallback(async (predictionId: string, predictedPrice: number, targetTimestamp: Date) => {
    try {
      const actualPrice = await getCurrentPrice()
      const accuracy = calculateAccuracy(predictedPrice, actualPrice)
      updatePrediction(predictionId, {
        actual: actualPrice,
        accuracy,
      })
    } catch (error) {
      console.error('実測値の取得に失敗しました:', error)
    }
  }, [updatePrediction])

  // 予想を実行する関数
  const executePrediction = useCallback(async () => {
    // 重複実行を防ぐ：最後の実行から4時間経過していない場合は実行しない
    const now = Date.now()
    if (now - lastPredictionTimeRef.current < PREDICTION_INTERVAL_MS) {
      console.log('予想の実行をスキップしました（間隔が短すぎます）')
      return
    }

    try {
      const response: PredictionResponse = await executePredictionAPI()
      
      const predictionId = Date.now().toString()
      const targetTimestamp = new Date(response.target_timestamp)
      const newPrediction: Prediction = {
        id: predictionId,
        timestamp: new Date(response.base_timestamp),
        predicted: response.predicted_price,
        actual: null, // 後で実測値が入る
        accuracy: null,
        targetTimestamp,
      }

      // 予測を追加（コンテキストで5回分まで保持される）
      addPrediction(newPrediction)
      lastPredictionTimeRef.current = now

      // ターゲット時刻になったら実測値を取得して精度を計算
      const delay = targetTimestamp.getTime() - Date.now()
      if (delay > 0) {
        setTimeout(() => {
          fetchActualPrice(predictionId, response.predicted_price, targetTimestamp)
        }, delay)
      } else {
        // 既にターゲット時刻を過ぎている場合は即座に取得
        fetchActualPrice(predictionId, response.predicted_price, targetTimestamp)
      }
    } catch (error) {
      console.error('予想の実行に失敗しました:', error)
    }
  }, [addPrediction, fetchActualPrice, PREDICTION_INTERVAL_MS])

  // 過去の予想の実測値をチェックする関数
  useEffect(() => {
    const checkActualPrices = () => {
      const now = new Date()
      predictions.forEach((pred) => {
        // ターゲット時刻を過ぎていて、まだ実測値がない場合
        if (
          pred.actual === null &&
          now.getTime() >= pred.targetTimestamp.getTime()
        ) {
          // 実測値を取得
          fetchActualPrice(pred.id, pred.predicted, pred.targetTimestamp)
        }
      })
    }

    // 1分ごとにチェック
    const checkInterval = 60 * 1000
    const interval = setInterval(checkActualPrices, checkInterval)
    return () => clearInterval(interval)
  }, [predictions, fetchActualPrice])

  // 定期的に予想を実行
  useEffect(() => {
    if (!isRunning) return

    // 初回実行
    executePrediction()

    // 指定間隔ごとに実行
    const interval = setInterval(() => {
      executePrediction()
    }, PREDICTION_INTERVAL_MS)

    return () => clearInterval(interval)
  }, [isRunning, PREDICTION_INTERVAL_MS, executePrediction])

  return (
    <div className="h-full">
      <h3 className="text-lg font-bold text-foreground mb-6">予想履歴</h3>

      <div className="space-y-3">
        {predictions.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground">
            <p>予想履歴がありません</p>
            <p className="text-sm mt-2">4時間ごとに予想が実行されます</p>
          </div>
        ) : (
          predictions.map((pred) => (
            <div
              key={pred.id}
              className="relative rounded-lg bg-secondary/50 p-4 border border-border/50"
            >
              <div className="flex items-start justify-between gap-3">
                <div className="flex-1 min-w-0">
                  <p className="text-sm text-muted-foreground mb-3">
                    {formatTimeAgo(pred.timestamp)}
                  </p>
                  <div className="space-y-2">
                    <div className="flex items-center gap-2 text-sm">
                      <span className="text-muted-foreground whitespace-nowrap">予想:</span>
                      <span className="font-mono font-medium text-orange-500">
                        {formatPrice(pred.predicted)}
                      </span>
                    </div>
                    <div className="flex items-center gap-2 text-sm">
                      <span className="text-muted-foreground whitespace-nowrap">実際:</span>
                      <span className="font-mono font-medium text-foreground">
                        {pred.actual !== null
                          ? formatPrice(pred.actual)
                          : '測定中...'}
                      </span>
                    </div>
                  </div>
                </div>
                {pred.accuracy !== null && (
                  <div className="ml-auto rounded-md bg-green-500/20 px-3 py-1.5 flex-shrink-0">
                    <span className="text-xs font-bold text-green-400 whitespace-nowrap">
                      {pred.accuracy.toFixed(1)}%
                    </span>
                  </div>
                )}
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  )
}
