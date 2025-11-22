'use client'

import { Card } from '@/components/ui/card'
import { Activity, TrendingUp, TrendingDown, Minus } from 'lucide-react'

interface SignalReasoningProps {
  signal: 'BUY' | 'SELL' | 'WAIT' | null
  trend: number | null
  currentPrice: number | null
  predictedPrice: number | null
  confidence?: number
}

export function SignalReasoning({ signal, trend, currentPrice, predictedPrice, confidence = 0 }: SignalReasoningProps) {
  if (!signal || trend === null || !currentPrice || !predictedPrice) return null

  return (
    <div className="grid gap-4 md:grid-cols-3 mt-6">
      <Card className="p-4 glass border-0 bg-white/5">
        <div className="flex items-center gap-3 mb-2">
            <div className={`p-2 rounded-lg ${
                signal === 'BUY' ? 'bg-green-500/20 text-green-400' :
                signal === 'SELL' ? 'bg-red-500/20 text-red-400' :
                'bg-yellow-500/20 text-yellow-400'
            }`}>
                {signal === 'BUY' ? <TrendingUp className="h-5 w-5" /> :
                 signal === 'SELL' ? <TrendingDown className="h-5 w-5" /> :
                 <Minus className="h-5 w-5" />}
            </div>
            <h3 className="font-bold text-foreground">AI判断</h3>
        </div>
        <p className="text-sm text-muted-foreground mb-1">推奨アクション</p>
        <p className={`text-2xl font-black ${
            signal === 'BUY' ? 'text-green-400' :
            signal === 'SELL' ? 'text-red-400' :
            'text-yellow-400'
        }`}>
            {signal}
        </p>
      </Card>

      <Card className="p-4 glass border-0 bg-white/5">
        <div className="flex items-center gap-3 mb-2">
            <div className="p-2 rounded-lg bg-primary/20 text-primary">
                <Activity className="h-5 w-5" />
            </div>
            <h3 className="font-bold text-foreground">市場分析</h3>
        </div>
        <div className="space-y-2">
            <div className="flex justify-between items-baseline">
                <p className="text-sm text-muted-foreground">予想収益率</p>
                <p className={`font-bold font-mono ${trend >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {trend > 0 ? '+' : ''}{trend.toFixed(2)}%
                </p>
            </div>
            <div className="flex justify-between items-baseline">
                <p className="text-sm text-muted-foreground">目標価格</p>
                <p className="font-bold font-mono text-foreground">
                    ${predictedPrice.toLocaleString()}
                </p>
            </div>
        </div>
      </Card>

        <Card className="p-4 glass border-0 bg-white/5 relative overflow-hidden">
             {/* Background Pattern */}
             <div className="absolute inset-0 bg-grid-white/[0.02] [mask-image:linear-gradient(0deg,transparent,black)]" />
             
            <div className="relative z-10">
                <h3 className="font-bold text-foreground mb-2">判断根拠</h3>
                <p className="text-sm text-muted-foreground leading-relaxed">
                    {signal === 'BUY' && `最近の市場モメンタムとテクニカル指標に基づき、AIモデルは${trend.toFixed(2)}%の価格上昇を予測しています。強い買いシグナルが検知されました。`}
                    {signal === 'SELL' && `AIモデルは${Math.abs(trend).toFixed(2)}%の価格下落を予測しています。買われすぎの状態による短期的な調整が予想されます。`}
                    {signal === 'WAIT' && `市場のボラティリティは低く、予測される変動はわずか${trend.toFixed(2)}%です。明確なトレンドが現れるまで、現在のポジションを維持することを推奨します。`}
                </p>
            </div>
        </Card>
    </div>
  )
}

