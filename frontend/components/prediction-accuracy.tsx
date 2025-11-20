'use client'

import { useCallback, useEffect, useState, useRef } from 'react'
import { executePrediction as executePredictionAPI, getCurrentPrice, type PredictionResponse } from '@/lib/api'
import { usePredictionContext, type Prediction } from '@/contexts/prediction-context'
import { AlertCircle, CheckCircle2, Clock, History, Zap } from 'lucide-react'
import { format } from 'date-fns'

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

  // 4時間ごとに予測を実行
  const PREDICTION_INTERVAL_MS = 4 * 60 * 60 * 1000 

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
        actual: null,
        accuracy: null,
        targetTimestamp,
      }

      addPrediction(newPrediction)
      lastPredictionTimeRef.current = now

      const delay = targetTimestamp.getTime() - Date.now()
      if (delay > 0) {
        setTimeout(() => {
          fetchActualPrice(predictionId, response.predicted_price, targetTimestamp)
        }, delay)
      } else {
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
        if (
          pred.actual === null &&
          now.getTime() >= pred.targetTimestamp.getTime()
        ) {
          fetchActualPrice(pred.id, pred.predicted, pred.targetTimestamp)
        }
      })
    }

    const checkInterval = 60 * 1000
    const interval = setInterval(checkActualPrices, checkInterval)
    return () => clearInterval(interval)
  }, [predictions, fetchActualPrice])

  // 定期的に予想を実行
  useEffect(() => {
    if (!isRunning) return

    executePrediction()

    const interval = setInterval(() => {
      executePrediction()
    }, PREDICTION_INTERVAL_MS)

    return () => clearInterval(interval)
  }, [isRunning, PREDICTION_INTERVAL_MS, executePrediction])

  return (
    <div className="h-full glass border-0 flex flex-col overflow-hidden relative rounded-xl">
       {/* Background Glow Effect */}
       <div className="absolute top-0 right-0 w-32 h-32 bg-primary/5 rounded-full blur-2xl pointer-events-none" />

      <div className="p-6 border-b border-white/5 relative z-10">
        <div className="flex items-center gap-2 mb-1">
          <History className="h-5 w-5 text-primary" />
          <h2 className="text-lg font-bold text-foreground tracking-tight">Prediction History</h2>
        </div>
        <p className="text-xs text-muted-foreground uppercase tracking-wider">Past Performance</p>
      </div>

      <div className="flex-1 overflow-auto p-4 space-y-3 custom-scrollbar">
        {predictions.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-40 text-muted-foreground">
            <p className="text-sm">No prediction history available</p>
          </div>
        ) : (
          predictions.map((pred) => {
            const isPending = pred.actual === null
            const date = new Date(pred.timestamp)
            const timeString = format(date, 'HH:mm')
            
            return (
              <div
                key={pred.id}
                className="group relative overflow-hidden rounded-xl bg-black/20 border border-white/5 p-4 transition-all hover:bg-white/5 hover:border-white/10 hover:shadow-lg hover:shadow-primary/5"
              >
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-2">
                    <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-white/5 text-xs font-bold text-muted-foreground group-hover:bg-primary/20 group-hover:text-primary transition-colors">
                      {timeString}
                    </div>
                    <div className="flex flex-col">
                        <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Target</span>
                        <span className="text-xs font-medium text-foreground">
                            {format(pred.targetTimestamp, 'HH:mm')}
                        </span>
                    </div>
                  </div>
                  
                  {!isPending && pred.accuracy !== null && (
                    <div className={`flex items-center gap-1 rounded-full px-2 py-1 text-[10px] font-bold border ${getAccuracyColor(pred.accuracy)}`}>
                      {pred.accuracy >= 90 && <CheckCircle2 className="h-3 w-3" />}
                      {pred.accuracy.toFixed(1)}%
                    </div>
                  )}
                  {isPending && (
                     <div className="flex items-center gap-1 rounded-full bg-yellow-500/10 px-2 py-1 text-[10px] font-bold text-yellow-500 border border-yellow-500/20">
                        <Clock className="h-3 w-3 animate-pulse" />
                        PENDING
                     </div>
                  )}
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div className="flex flex-col">
                    <span className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">Forecast</span>
                    <span className="text-sm font-mono font-bold text-accent">
                      {formatPrice(pred.predicted)}
                    </span>
                  </div>
                  <div className="flex flex-col items-end">
                    <span className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">Actual</span>
                    {pred.actual !== null ? (
                      <span className="text-sm font-mono font-bold text-foreground">
                        {formatPrice(pred.actual)}
                      </span>
                    ) : (
                      <span className="text-sm font-mono text-muted-foreground">---</span>
                    )}
                  </div>
                </div>
                
                {/* Progress bar for pending */}
                {isPending && (
                    <div className="absolute bottom-0 left-0 h-0.5 w-full bg-white/5">
                        <div className="h-full bg-primary/50 animate-progress-indeterminate" />
                    </div>
                )}
              </div>
            )
          })
        )}
      </div>
      
      <div className="p-4 border-t border-white/5 bg-black/20 backdrop-blur-sm">
        <button
            onClick={() => executePrediction()}
            className="w-full rounded-xl bg-primary p-3 text-sm font-bold text-primary-foreground shadow-lg shadow-primary/20 transition-all hover:bg-primary/90 hover:shadow-primary/40 hover:scale-[1.02] active:scale-[0.98] disabled:opacity-50 disabled:pointer-events-none flex items-center justify-center gap-2 cursor-pointer"
        >
            <Zap className="h-4 w-4 fill-current" />
            Generate New Prediction
        </button>
      </div>
    </div>
  )
}

function getAccuracyColor(accuracy: number) {
  if (accuracy >= 98) return 'bg-green-500/10 text-green-400 border-green-500/20 shadow-[0_0_10px_rgba(74,222,128,0.1)]'
  if (accuracy >= 95) return 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
  if (accuracy >= 90) return 'bg-blue-500/10 text-blue-400 border-blue-500/20'
  if (accuracy >= 80) return 'bg-yellow-500/10 text-yellow-400 border-yellow-500/20'
  return 'bg-red-500/10 text-red-400 border-red-500/20'
}
