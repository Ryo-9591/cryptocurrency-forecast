'use client'

import { PredictionProvider } from '@/contexts/prediction-context'

export function Providers({ children }: { children: React.ReactNode }) {
  return (
    <PredictionProvider>
      {children}
    </PredictionProvider>
  )
}

