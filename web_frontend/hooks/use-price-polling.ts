import { useEffect, useState, useCallback, useRef } from 'react'
import { getCurrentPrice } from '@/lib/api'

// WebSocketが利用できない場合のポーリングフォールバック
// デフォルトは1分（60000ms）に設定
export function usePricePolling(intervalMs: number = 60000) {
  const [price, setPrice] = useState<number | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const intervalRef = useRef<NodeJS.Timeout | null>(null)

  const fetchPrice = useCallback(async () => {
    try {
      setIsLoading(true)
      const currentPrice = await getCurrentPrice()
      setPrice(currentPrice)
      setError(null)
    } catch (err) {
      console.error('価格の取得に失敗しました:', err)
      setError(err instanceof Error ? err.message : '価格の取得に失敗しました')
    } finally {
      setIsLoading(false)
    }
  }, [])

  useEffect(() => {
    // 初回実行
    fetchPrice()

    // 定期的に実行
    intervalRef.current = setInterval(() => {
      fetchPrice()
    }, intervalMs)

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
      }
    }
  }, [fetchPrice, intervalMs])

  return {
    price,
    isLoading,
    error,
    refetch: fetchPrice,
  }
}

