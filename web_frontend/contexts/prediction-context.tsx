'use client'

import { createContext, useContext, useState, useCallback, useMemo, type ReactNode } from 'react'

export interface Prediction {
  id: string
  timestamp: Date
  predicted: number
  actual: number | null
  accuracy: number | null
  targetTimestamp: Date
}

interface PredictionContextType {
  predictions: Prediction[]
  averageAccuracy: number | null
  addPrediction: (prediction: Prediction) => void
  updatePrediction: (id: string, updates: Partial<Prediction>) => void
}

const PredictionContext = createContext<PredictionContextType | undefined>(undefined)

function calculateAccuracy(predicted: number, actual: number): number {
  const error = Math.abs(predicted - actual)
  const accuracy = (1 - error / actual) * 100
  return Math.max(0, Math.min(100, accuracy))
}

export function PredictionProvider({ children }: { children: ReactNode }) {
  const [predictions, setPredictions] = useState<Prediction[]>([])

  const addPrediction = useCallback((prediction: Prediction) => {
    setPredictions((prev) => [prediction, ...prev].slice(0, 5))
  }, [])

  const updatePrediction = useCallback((id: string, updates: Partial<Prediction>) => {
    setPredictions((prev) =>
      prev.map((p) => (p.id === id ? { ...p, ...updates } : p))
    )
  }, [])

  // 平均精度を計算（useMemoで最適化）
  const averageAccuracy = useMemo(() => {
    const accuracies = predictions
      .filter((p) => p.accuracy !== null)
      .map((p) => p.accuracy as number)
    
    return accuracies.length > 0
      ? accuracies.reduce((sum, acc) => sum + acc, 0) / accuracies.length
      : null
  }, [predictions])

  const value: PredictionContextType = {
    predictions,
    averageAccuracy,
    addPrediction,
    updatePrediction,
  }

  return (
    <PredictionContext.Provider value={value}>
      {children}
    </PredictionContext.Provider>
  )
}

export function usePredictionContext() {
  const context = useContext(PredictionContext)
  if (context === undefined) {
    throw new Error('usePredictionContext must be used within a PredictionProvider')
  }
  return context
}

