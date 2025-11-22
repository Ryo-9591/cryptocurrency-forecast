import { useEffect, useRef, useState, useCallback } from 'react'

const WS_BASE_URL = process.env.NEXT_PUBLIC_WS_BASE_URL || 'ws://localhost:8000'

export interface WebSocketMessage {
  type: 'price_update' | 'prediction_update' | 'error'
  data?: any
  timestamp?: string
}

export interface PriceUpdate {
  price: number
  timestamp: string
  change_24h?: number
}

export function useWebSocket() {
  const [price, setPrice] = useState<number | null>(null)
  const [isConnected, setIsConnected] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const wsRef = useRef<WebSocket | null>(null)
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null)
  const reconnectAttempts = useRef(0)
  const maxReconnectAttempts = 5

  const connect = useCallback(() => {
    try {
      // WebSocketエンドポイントが存在しない場合は、ポーリングで代替
      // バックエンドにWebSocketエンドポイントが実装されていない場合のフォールバック
      const wsUrl = `${WS_BASE_URL}/ws/price`
      const ws = new WebSocket(wsUrl)

      ws.onopen = () => {
        console.log('WebSocket接続が確立されました')
        setIsConnected(true)
        setError(null)
        reconnectAttempts.current = 0
      }

      ws.onmessage = (event) => {
        try {
          const message: WebSocketMessage = JSON.parse(event.data)
          
          if (message.type === 'price_update' && message.data) {
            const priceUpdate: PriceUpdate = message.data
            setPrice(priceUpdate.price)
          }
        } catch (err) {
          console.error('WebSocketメッセージの解析に失敗しました:', err)
        }
      }

      ws.onerror = (err) => {
        console.error('WebSocketエラー:', err)
        setError('WebSocket接続エラー')
        setIsConnected(false)
      }

      ws.onclose = () => {
        console.log('WebSocket接続が閉じられました')
        setIsConnected(false)
        
        // 再接続を試みる
        if (reconnectAttempts.current < maxReconnectAttempts) {
          reconnectAttempts.current++
          const delay = Math.min(1000 * Math.pow(2, reconnectAttempts.current), 30000)
          reconnectTimeoutRef.current = setTimeout(() => {
            console.log(`再接続を試みます (${reconnectAttempts.current}/${maxReconnectAttempts})...`)
            connect()
          }, delay)
        } else {
          setError('WebSocket接続に失敗しました。ポーリングモードに切り替えます。')
        }
      }

      wsRef.current = ws
    } catch (err) {
      console.error('WebSocket接続の確立に失敗しました:', err)
      setError('WebSocket接続に失敗しました')
      setIsConnected(false)
    }
  }, [])

  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current)
      reconnectTimeoutRef.current = null
    }
    
    if (wsRef.current) {
      wsRef.current.close()
      wsRef.current = null
    }
    
    setIsConnected(false)
  }, [])

  useEffect(() => {
    connect()
    
    return () => {
      disconnect()
    }
  }, [connect, disconnect])

  return {
    price,
    isConnected,
    error,
    reconnect: connect,
  }
}

