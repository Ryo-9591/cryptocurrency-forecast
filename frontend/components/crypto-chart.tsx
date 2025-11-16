'use client'

import { useState, useEffect, useMemo } from 'react'
import { Card } from '@/components/ui/card'
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { ChartContainer, ChartTooltip, ChartTooltipContent } from '@/components/ui/chart'
import { getTimeSeries, getPredictSeries } from '@/lib/api'
import { usePricePolling } from '@/hooks/use-price-polling'
import { format } from 'date-fns'

type TimeRange = '1h' | '24h' | '7d'

interface ChartDataPoint {
  time: string
  price: number
  prediction: number | null
  timestamp: string
}

export function CryptoChart() {
  // 24時間のみに固定
  const timeRange: TimeRange = '24h'
  const [chartData, setChartData] = useState<ChartDataPoint[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  
  // 価格を取得（1分ごとにポーリング）
  const { price: currentPrice } = usePricePolling(60000)

  // 24時間固定
  const hours = 24
  const timeRangeLabel = '過去24時間'

  // 価格変動率を計算（選択された時間範囲の最初の価格との比較）
  const priceChange = useMemo(() => {
    if (chartData.length < 2 || !currentPrice) return null
    const firstPrice = chartData[0].price
    const change = ((currentPrice - firstPrice) / firstPrice) * 100
    return change
  }, [chartData, currentPrice])

  // データを取得
  useEffect(() => {
    const fetchData = async () => {
      try {
        setIsLoading(true)
        setError(null)

        // 時系列データと予測データを並行取得
        const [timeSeriesData, predictData] = await Promise.all([
          getTimeSeries(hours),
          getPredictSeries(hours).catch(() => null), // 予測データは失敗しても続行
        ])

        // 予測データのbase_timestampを取得（予測が実行された時刻）
        const predictionBaseTimestamp = predictData ? new Date(predictData.base_timestamp).getTime() : null

        // 時系列データをチャート形式に変換
        const priceMap = new Map<string, number>()
        timeSeriesData.points.forEach((point) => {
          const date = new Date(point.timestamp)
          const timeKey = format(date, 'HH:mm')
          priceMap.set(timeKey, point.price)
        })

        // 予測データをマップに追加（base_timestamp以降のデータのみ）
        const predictionMap = new Map<string, number>()
        if (predictData && predictionBaseTimestamp !== null) {
          predictData.points.forEach((point) => {
            const pointTimestamp = new Date(point.timestamp).getTime()
            // base_timestamp以降のデータのみを追加
            if (pointTimestamp >= predictionBaseTimestamp) {
              const date = new Date(point.timestamp)
              const timeKey = format(date, 'HH:mm')
              predictionMap.set(timeKey, point.price)
            }
          })
        }

        // チャートデータを構築
        const data: ChartDataPoint[] = timeSeriesData.points.map((point) => {
          const date = new Date(point.timestamp)
          const timeKey = format(date, 'HH:mm')
          return {
            time: timeKey,
            price: point.price,
            prediction: predictionMap.get(timeKey) || null,
            timestamp: point.timestamp,
          }
        })

        setChartData(data)
      } catch (err) {
        console.error('データの取得に失敗しました:', err)
        setError(err instanceof Error ? err.message : 'データの取得に失敗しました')
      } finally {
        setIsLoading(false)
      }
    }

    fetchData()
  }, [hours])

  // 現在の価格が更新されたら、最新のデータポイントを更新
  useEffect(() => {
    if (currentPrice && chartData.length > 0) {
      setChartData((prev) => {
        const updated = [...prev]
        const lastIndex = updated.length - 1
        if (lastIndex >= 0) {
          updated[lastIndex] = {
            ...updated[lastIndex],
            price: currentPrice,
          }
        }
        return updated
      })
    }
  }, [currentPrice, chartData.length])

  // 表示価格（リアルタイム更新）
  const displayPrice = currentPrice || (chartData.length > 0 ? chartData[chartData.length - 1].price : 0)

  return (
    <Card className="p-6 bg-card">
      <div className="mb-6 flex items-start justify-between">
        <div>
          <div className="flex items-center gap-2">
            <h2 className="text-2xl font-bold text-foreground">BTC/USD</h2>
            <span className="rounded-md bg-primary/10 px-2 py-1 text-sm font-medium text-primary">
              LIVE
            </span>
          </div>
          <div className="mt-2 flex items-baseline gap-2">
            <span className="text-3xl font-bold text-foreground">
              ${displayPrice.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}
            </span>
            {priceChange !== null && (
              <span className={`text-lg font-medium ${priceChange >= 0 ? 'text-primary' : 'text-red-500'}`}>
                {priceChange >= 0 ? '+' : ''}{priceChange.toFixed(2)}%
              </span>
            )}
          </div>
          <p className="mt-1 text-sm text-muted-foreground">{timeRangeLabel}</p>
        </div>
      </div>

      {error ? (
        <div className="h-[400px] flex items-center justify-center text-muted-foreground">
          <p>エラー: {error}</p>
        </div>
      ) : isLoading ? (
        <div className="h-[400px] flex items-center justify-center text-muted-foreground">
          <p>読み込み中...</p>
        </div>
      ) : chartData.length === 0 ? (
        <div className="h-[400px] flex items-center justify-center text-muted-foreground">
          <p>データがありません</p>
        </div>
      ) : (
        <div className="h-[400px]">
          <ChartContainer
            config={{
              price: {
                label: '実際の価格',
                color: 'oklch(0.75 0.2 142)',
              },
              prediction: {
                label: 'AI予想',
                color: 'oklch(0.75 0.18 52)',
              },
            }}
          >
            <ResponsiveContainer width="100%" height="100%">
              <LineChart 
                data={chartData}
                margin={{ top: 5, right: 10, left: 10, bottom: 40 }}
              >
                <XAxis
                  dataKey="time"
                  stroke="oklch(0.65 0.01 264)"
                  fontSize={11}
                  tickLine={false}
                  axisLine={false}
                  interval="preserveStartEnd"
                  tickMargin={8}
                />
                <YAxis
                  stroke="oklch(0.65 0.01 264)"
                  fontSize={12}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={(value) => `$${(value / 1000).toFixed(0)}K`}
                />
                <ChartTooltip content={<ChartTooltipContent />} />
                <Line
                  type="monotone"
                  dataKey="price"
                  stroke="oklch(0.75 0.2 142)"
                  strokeWidth={3}
                  dot={false}
                  isAnimationActive={false}
                />
                {chartData.some((d) => d.prediction !== null) && (
                  <Line
                    type="monotone"
                    dataKey="prediction"
                    stroke="oklch(0.75 0.18 52)"
                    strokeWidth={2}
                    strokeDasharray="5 5"
                    dot={false}
                    isAnimationActive={false}
                  />
                )}
              </LineChart>
            </ResponsiveContainer>
          </ChartContainer>
        </div>
      )}

      <div className="mt-6 flex items-center gap-6">
        <div className="flex items-center gap-2">
          <div className="size-3 rounded-full bg-primary" />
          <span className="text-sm text-muted-foreground">実際の価格</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="size-3 rounded-full bg-accent" />
          <span className="text-sm text-muted-foreground">AI予想</span>
        </div>
      </div>
    </Card>
  )
}
