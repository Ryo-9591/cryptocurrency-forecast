# Cryptocurrency Forecast App

ビットコイン価格の予測を行うAIアプリケーションです。Airflowによる学習パイプライン、MLflowによるモデル管理、FastAPIによる推論API、Next.jsによるモダンなUIを備えています。

## 🚀 クイックスタート

### 開発環境の起動

Docker Composeを使用して簡単に開発環境を立ち上げることができます。

```bash
# 全てのサービスを起動（初回ビルド時）
docker compose --profile all up --build

# 以降の起動（ビルド済みの場合）
docker compose --profile all up
```

起動後、以下のURLにアクセスしてください。

*   **Web UI**: [http://localhost:3002](http://localhost:3002)
*   **推論 API**: [http://localhost:8000/docs](http://localhost:8000/docs)
*   **Airflow**: [http://localhost:8080](http://localhost:8080)
*   **MLflow**: [http://localhost:5000](http://localhost:5000)

### 高速起動（プロファイルの使用）

開発目的に合わせて必要なサービスのみを起動することで、リソースを節約し起動を高速化できます。

**Web開発のみ（Frontend + Backend + DB）**
```bash
docker compose --profile web up
```
*   Airflowなどの重い処理をスキップします。
*   Frontend/Backendのコード変更はホットリロードされます。

**ML開発のみ（Airflow + MLflow + DB）**
```bash
docker compose --profile ml up
```
*   Web UIを起動せず、モデル作成パイプラインの開発に集中できます。

## 📈 機能

*   **BTC価格予測**: 機械学習モデル（XGBoost/LightGBM）による短期価格予測
*   **売買シグナル**: AI判断による「買い」「売り」「様子見」のシグナル表示
*   **判断根拠の提示**: シグナルの理由と予測される収益率・目標価格を日本語で解説
*   **リアルタイムチャート**: 実績価格と予測価格を比較できるインタラクティブなチャート（ダークモード対応）
*   **MLOpsパイプライン**: 自動学習とモデル評価・管理

## 🛠️ 技術スタック

*   **Frontend**: Next.js, Tailwind CSS, Recharts
*   **Backend**: FastAPI, Python
*   **ML/Data**: Airflow, MLflow, Pandas, Scikit-learn, XGBoost
*   **Infrastructure**: Docker Compose

## 📝 開発ガイド

### ディレクトリ構成
```
.
├── ml_airflow/     # MLパイプライン
├── web_backend/    # 推論API
├── web_frontend/   # Web UI
├── shared/         # 共通コード
└── docker-compose.yml
```

### 新しいPythonパッケージの追加
`ml_airflow/requirements.txt` または `web_backend/requirements.txt` を編集し、再ビルドしてください。

```bash
docker compose build
```
