# BTC価格予測システム

CoinGecko APIからBTC価格情報を取得し、機械学習モデルで予測を行うシステムです。

## 機能

- CoinGecko APIからBTC価格情報を取得（USD）
- データをParquet形式でAmazon S3に自動保存
- 機械学習モデル（XGBoost、LightGBM）による価格予測
- MLflowによるモデル管理と追跡
- FastAPIによる推論エンドポイント
- React UIでのBTC価格可視化・予測実行

## 前提条件

- DockerとDocker Composeがインストールされていること
- AWSアカウントとS3バケットが作成済みであること
- AWS認証情報（アクセスキー、シークレットキー）を取得済みであること

## セットアップ

### 1. 環境変数の設定

`env.example`をコピーして`.env`ファイルを作成し、必要な値を設定してください：

```bash
cp env.example .env
```

必須の環境変数：
- `AWS_ACCESS_KEY_ID`: AWSアクセスキーID
- `AWS_SECRET_ACCESS_KEY`: AWSシークレットアクセスキー
- `AWS_DEFAULT_REGION`: AWSリージョン（例: ap-northeast-1）
- `S3_BUCKET_NAME`: S3バケット名
- `COINGECKO_API_KEY`: CoinGecko APIキー（必須）

その他の環境変数は`env.example`を参照してください。

### 2. サービスの起動

```bash
# Airflow UIDを設定（Linux/Macの場合）
export AIRFLOW_UID=$(id -u)

# Windows PowerShellの場合
$env:AIRFLOW_UID = 50000

# Docker Composeで主要サービスを起動
docker-compose up -d postgres mlflow fastapi frontend

# Airflowも含めて全てのサービスを起動する場合
docker-compose up -d
```

### 3. 各サービスへのアクセス

- **Airflow Web UI**: http://localhost:8080（デフォルト: `airflow`/`airflow`）
- **MLflow UI**: http://localhost:5000
- **FastAPI**: http://localhost:8000（APIドキュメント: http://localhost:8000/docs）
- **BTC予測UI**: http://localhost:8081

## DAGの実行

システムには2つの主要なDAGが含まれています：

1. **`fetch_daily_data`**: CoinGeckoからBTC価格データを取得し、S3に保存（毎日午前1時実行）
2. **`btc_price_regression`**: S3からデータを読み込み、機械学習モデルを訓練（毎日午前3時実行）

Airflow Web UIで各DAGを有効化し、手動実行する場合は「Trigger DAG」をクリックしてください。

## データ形式

データは以下のパスに単一のParquetファイルとして保存されます：
- `s3://<bucket-name>/btc-prices/daily/btc_prices.parquet`
- `s3://<bucket-name>/mlflow-artifacts/`（MLflowアーティファクト）

## トラブルシューティング

- **DAGが表示されない**: `docker-compose logs airflow-scheduler`でログを確認
- **S3アップロードエラー**: AWS認証情報とS3バケットの権限を確認
- **APIリクエストエラー**: CoinGecko APIキーとレート制限を確認
- **モデル予測エラー**: MLflowサーバーの起動状態とProductionステージのモデル存在を確認

## 停止とクリーンアップ

```bash
# サービスを停止
docker-compose down

# データベースも含めて完全に削除（注意: データが失われます）
docker-compose down -v
```

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。
