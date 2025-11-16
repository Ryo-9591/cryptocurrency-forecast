'use client'

import { TrendingUp } from 'lucide-react'
import { usePredictionAccuracy } from '@/hooks/use-prediction-accuracy'

export function Header() {
  const averageAccuracy = usePredictionAccuracy()

  return (
    <header className="border-b border-border bg-card">
      <div className="container mx-auto flex items-center justify-between px-4 py-4">
        <div className="flex items-center gap-2">
          <div className="flex size-10 items-center justify-center rounded-lg bg-primary">
            <TrendingUp className="size-6 text-primary-foreground" />
          </div>
          <div>
            <h1 className="text-xl font-bold text-foreground">cryptocurrency forecast</h1>
          </div>
        </div>
        <div className="flex items-center gap-4">
          <div className="hidden md:block text-right">
            <p className="text-sm font-medium text-foreground">予想精度</p>
            <p className="text-lg font-bold text-primary">
              {averageAccuracy !== null ? `${averageAccuracy.toFixed(1)}%` : '計算中...'}
            </p>
          </div>
        </div>
      </div>
    </header>
  )
}
