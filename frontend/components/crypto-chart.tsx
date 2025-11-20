'use client'

import { useState, useEffect, useMemo } from 'react'
import { Card } from '@/components/ui/card'
import { Area, AreaChart, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis, ReferenceLine } from 'recharts'
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
  const [predictionBaseTime, setPredictionBaseTime] = useState<string | null>(null)
  
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

  // 予測トレンドを計算
  const predictionTrend = useMemo(() => {
    if (chartData.length === 0) return null
    const lastData = chartData[chartData.length - 1]
    if (lastData.prediction === null || !currentPrice) return null
    
    const change = ((lastData.prediction - currentPrice) / currentPrice) * 100
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
        
        if (predictData) {
           const date = new Date(predictData.base_timestamp)
           setPredictionBaseTime(format(date, 'HH:mm'))
        }

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

        // 予測データが時系列データより未来にある場合、それを追加
        if (predictData && predictionBaseTimestamp !== null) {
             predictData.points.forEach((point) => {
                const pointTimestamp = new Date(point.timestamp).getTime()
                const date = new Date(point.timestamp)
                const timeKey = format(date, 'HH:mm')
                
                // 既存データにない場合のみ追加（未来の予測）
                if (!data.find(d => d.time === timeKey) && pointTimestamp >= predictionBaseTimestamp) {
                    data.push({
                        time: timeKey,
                        price: 0, // 未来なので実績なし（0またはnull、表示時に調整）
                        prediction: point.price,
                        timestamp: point.timestamp
                    })
                }
            })
        }

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
        // 実績データの最後のポイントを探す
        const lastPriceIndex = updated.map(d => d.price).lastIndexOf(updated.filter(d => d.price > 0).pop()?.price || 0)
        
        if (lastPriceIndex >= 0) {
          updated[lastPriceIndex] = {
            ...updated[lastPriceIndex],
            price: currentPrice,
          }
        }
        return updated
      })
    }
  }, [currentPrice, chartData.length])

  // 表示価格（リアルタイム更新）
  const displayPrice = currentPrice || (chartData.length > 0 ? chartData.filter(d => d.price > 0).pop()?.price || 0 : 0)

  return (
    <Card className="p-6 glass border-0 relative overflow-hidden">
      {/* Background Glow Effect */}
      <div className="absolute -top-20 -right-20 w-64 h-64 bg-primary/10 rounded-full blur-3xl pointer-events-none" />
      <div className="absolute -bottom-20 -left-20 w-64 h-64 bg-accent/10 rounded-full blur-3xl pointer-events-none" />

      <div className="mb-8 flex items-start justify-between relative z-10">
        <div>
          <div className="flex items-center gap-3">
            <h2 className="text-2xl font-bold text-foreground tracking-tight">BTC/USD</h2>
            <span className="rounded-full bg-primary/20 px-3 py-1 text-xs font-bold text-primary ring-1 ring-primary/50 animate-pulse">
              LIVE
            </span>
          </div>
          <div className="mt-3 flex items-baseline gap-4">
            <div>
                <span className="text-4xl font-black text-foreground tracking-tight">
                ${displayPrice.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}
                </span>
                {priceChange !== null && (
                <span className={`ml-3 text-lg font-bold ${priceChange >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {priceChange >= 0 ? '+' : ''}{priceChange.toFixed(2)}%
                </span>
                )}
            </div>
            
            {predictionTrend !== null && (
                <div className="flex flex-col border-l border-white/10 pl-4">
                    <span className="text-[10px] uppercase tracking-wider text-muted-foreground">AI Forecast</span>
                    <span className={`text-sm font-bold ${predictionTrend >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                        {predictionTrend >= 0 ? 'UPTREND' : 'DOWNTREND'} ({predictionTrend >= 0 ? '+' : ''}{predictionTrend.toFixed(2)}%)
                    </span>
                </div>
            )}
          </div>
          <p className="mt-1 text-xs text-muted-foreground uppercase tracking-widest">{timeRangeLabel}</p>
        </div>
      </div>

      {error ? (
        <div className="h-[400px] flex items-center justify-center text-muted-foreground">
          <p>エラー: {error}</p>
        </div>
      ) : isLoading ? (
        <div className="h-[400px] flex items-center justify-center text-muted-foreground">
          <div className="flex flex-col items-center gap-4">
            <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent" />
            <p className="animate-pulse text-sm font-medium">Loading Market Data...</p>
          </div>
        </div>
      ) : chartData.length === 0 ? (
        <div className="h-[400px] flex items-center justify-center text-muted-foreground">
          <p>データがありません</p>
        </div>
      ) : (
        <div className="h-[400px] w-full">
          <ChartContainer
            config={{
              price: {
                label: 'Actual Price',
                color: 'hsl(var(--primary))',
              },
              prediction: {
                label: 'AI Forecast',
                color: 'hsl(var(--accent))',
              },
            }}
          >
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart
                data={chartData}
                margin={{ top: 20, right: 10, left: 0, bottom: 0 }}
              >
                <defs>
                  <linearGradient id="colorPrice" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="hsl(var(--primary))" stopOpacity={0.4}/>
                    <stop offset="95%" stopColor="hsl(var(--primary))" stopOpacity={0}/>
                  </linearGradient>
                  <filter id="glow" x="-20%" y="-20%" width="140%" height="140%">
                    <feGaussianBlur stdDeviation="3" result="coloredBlur" />
                    <feMerge>
                      <feMergeNode in="coloredBlur" />
                      <feMergeNode in="SourceGraphic" />
                    </feMerge>
                  </filter>
                </defs>
                <XAxis
                  dataKey="time"
                  stroke="hsl(var(--muted-foreground))"
                  fontSize={11}
                  tickLine={false}
                  axisLine={false}
                  interval="preserveStartEnd"
                  minTickGap={30}
                  tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10 }}
                />
                <YAxis
                  stroke="hsl(var(--muted-foreground))"
                  fontSize={11}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={(value) => `$${(value / 1000).toFixed(0)}K`}
                  domain={['auto', 'auto']}
                  tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10 }}
                />
                <ChartTooltip 
                    cursor={{ stroke: 'hsl(var(--muted-foreground))', strokeWidth: 1, strokeDasharray: '4 4' }}
                    content={({ active, payload, label }) => {
                        if (active && payload && payload.length) {
                        return (
                            <div className="rounded-xl border border-white/10 bg-black/80 p-3 shadow-xl backdrop-blur-md">
                            <div className="grid grid-cols-2 gap-4">
                                <div className="flex flex-col">
                                <span className="text-[0.65rem] uppercase tracking-wider text-muted-foreground">
                                    Time
                                </span>
                                <span className="font-mono font-bold text-foreground">
                                    {label}
                                </span>
                                </div>
                                {payload.map((entry) => (
                                entry.value && entry.value > 0 ? (
                                    <div key={entry.name} className="flex flex-col">
                                    <span className="text-[0.65rem] uppercase tracking-wider text-muted-foreground">
                                        {entry.name === 'price' ? 'Actual' : 'Forecast'}
                                    </span>
                                    <span className={`font-mono font-bold ${entry.name === 'price' ? 'text-primary' : 'text-accent'}`}>
                                        ${Number(entry.value).toLocaleString()}
                                    </span>
                                    </div>
                                ) : null
                                ))}
                            </div>
                            </div>
                        )
                        }
                        return null
                    }}
                />
                <Area
                  type="monotone"
                  dataKey="price"
                  stroke="hsl(var(--primary))"
                  fillOpacity={1}
                  fill="url(#colorPrice)"
                  strokeWidth={3}
                  connectNulls
                  filter="url(#glow)"
                />
                {chartData.some((d) => d.prediction !== null) && (
                  <Line
                    type="monotone"
                    dataKey="prediction"
                    stroke="hsl(var(--accent))"
                    strokeWidth={3}
                    strokeDasharray="4 4"
                    dot={false}
                    connectNulls
                    activeDot={{ r: 6, strokeWidth: 0, fill: 'hsl(var(--accent))' }}
                    filter="url(#glow)"
                  />
                )}
                {predictionBaseTime && (
                    <ReferenceLine 
                        x={predictionBaseTime} 
                        stroke="hsl(var(--muted-foreground))" 
                        strokeDasharray="3 3" 
                        label={{ 
                            position: 'top',  
                            value: 'NOW', 
                            fill: 'hsl(var(--muted-foreground))', 
                            fontSize: 10,
                            fontWeight: 'bold'
                        }} 
                    />
                )}
              </AreaChart>
            </ResponsiveContainer>
          </ChartContainer>
        </div>
      )}

      <div className="mt-6 flex items-center justify-center gap-8">
        <div className="flex items-center gap-2">
          <div className="h-3 w-3 rounded-full bg-primary shadow-[0_0_10px_hsl(var(--primary))]" />
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Actual Price</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="h-0.5 w-6 border-t-2 border-dashed border-accent shadow-[0_0_10px_hsl(var(--accent))]" />
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">AI Forecast</span>
        </div>
      </div>
    </Card>
  )
}
