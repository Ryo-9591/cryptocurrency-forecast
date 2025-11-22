import { CryptoChart } from '@/components/crypto-chart'
import { PredictionAccuracy } from '@/components/prediction-accuracy'
import { Header } from '@/components/header'

export default function Home() {
  return (
    <div className="min-h-screen flex flex-col">
      <Header />
      <main className="flex-1 container mx-auto px-4 py-8 md:py-12 space-y-8">
        
        {/* Hero / Intro Section could go here if needed */}
        
        {/* Main Chart Section */}
        <div className="grid gap-8 lg:grid-cols-12">
          <div className="lg:col-span-8 xl:col-span-9 space-y-6">
            <CryptoChart />
          </div>
          <div className="lg:col-span-4 xl:col-span-3">
            <PredictionAccuracy />
          </div>
        </div>
      </main>
    </div>
  )
}
