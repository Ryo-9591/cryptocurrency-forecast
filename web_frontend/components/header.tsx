'use client'

import { TrendingUp } from 'lucide-react'
import { usePredictionAccuracy } from '@/hooks/use-prediction-accuracy'

export function Header() {
  const averageAccuracy = usePredictionAccuracy()

  return (
    <header className="sticky top-0 z-50 w-full glass border-b-0">
      <div className="container mx-auto flex items-center justify-between px-4 py-4">
        <div className="flex items-center gap-3">
          <div className="flex size-10 items-center justify-center rounded-xl bg-primary/20 ring-1 ring-primary/50 shadow-[0_0_15px_rgba(var(--primary),0.3)]">
            <TrendingUp className="size-6 text-primary" />
          </div>
          <div>
            <h1 className="text-xl font-bold tracking-tight bg-gradient-to-r from-foreground to-muted-foreground bg-clip-text text-transparent">
              CryptoForecast
            </h1>
            <p className="text-[10px] font-medium text-muted-foreground tracking-widest uppercase">
              AI Powered Analytics
            </p>
          </div>
        </div>
        <div className="flex items-center gap-4">
          <div className="hidden md:block text-right">
            <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Avg. Accuracy</p>
            <div className="flex items-center justify-end gap-2">
                <div className={`h-2 w-2 rounded-full ${averageAccuracy !== null && averageAccuracy >= 90 ? 'bg-green-500 shadow-[0_0_10px_oklch(0.6_0.18_150)]' : 'bg-yellow-500'}`} />
                <p className="text-lg font-bold font-mono text-foreground">
                {averageAccuracy !== null ? `${averageAccuracy.toFixed(1)}%` : '---'}
                </p>
            </div>
          </div>
        </div>
      </div>
    </header>
  )
}
