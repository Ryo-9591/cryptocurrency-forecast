import { CryptoChart } from '@/components/crypto-chart'
import { PredictionAccuracy } from '@/components/prediction-accuracy'
import { Header } from '@/components/header'

export default function Home() {
  return (
    <div className="min-h-screen bg-background">
      <Header />
      <main className="container mx-auto px-4 py-6 space-y-6">

        {/* Main Chart Section */}
        <div className="grid gap-6 lg:grid-cols-3">
          <div className="lg:col-span-2">
            <CryptoChart />
          </div>
          <div>
            <PredictionAccuracy />
          </div>
        </div>
      </main>
    </div>
  )
}
