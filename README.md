# CoinGecko BTC価格データ取得・S3アップロードパイプライン

CoinGecko APIからBTC（ビットコイン）の価格情報を取得し、Amazon S3にアップロードするAirflow DAGです。

## 機能

- CoinGecko APIからBTC価格情報を取得（USD、EUR、JPY）
- データをCSVとParquet形式に変換
- Amazon S3に自動アップロード
- 定期実行（デフォルト: 毎日）

## 前提条件

- DockerとDocker Composeがインストールされていること
- AWSアカウントとS3バケットが作成済みであること
- AWS認証情報（アクセスキー、シークレットキー）を取得済みであること

## セットアップ

### 1. 環境変数の設定

`.env.example`をコピーして`.env`ファイルを作成し、必要な値を設定してください：

```bash
cp .env.example .env
```

`.env`ファイルを編集して、以下の値を設定：

- `AWS_ACCESS_KEY_ID`: AWSアクセスキーID
- `AWS_SECRET_ACCESS_KEY`: AWSシークレットアクセスキー
- `AWS_DEFAULT_REGION`: AWSリージョン（例: ap-northeast-1）
- `S3_BUCKET_NAME`: S3バケット名
- `COINGECKO_API_KEY`: CoinGecko APIキー（オプション、無料プランでも使用可能）

### 2. Airflowの初期化と起動

```bash
# Airflow UIDを設定（Linux/Macの場合）
export AIRFLOW_UID=$(id -u)

# Windows PowerShellの場合
$env:AIRFLOW_UID = 50000

# Docker Composeでサービスを起動
docker-compose up -d
```

### 3. Airflow Web UIへのアクセス

ブラウザで以下のURLにアクセス：
- URL: http://localhost:8080
- ユーザー名: `airflow`
- パスワード: `airflow`

### 4. AWS接続の設定

Airflow Web UIでAWS接続を設定：

1. **Admin** → **Connections** に移動
2. **+** ボタンをクリックして新しい接続を追加
3. 以下の情報を入力：
   - **Connection Id**: `aws_default`
   - **Connection Type**: `Amazon Web Services`
   - **Login**: AWSアクセスキーID
   - **Password**: AWSシークレットアクセスキー
   - **Extra**: `{"region_name": "ap-northeast-1"}`（リージョンを変更する場合）

または、環境変数で設定している場合は、接続設定は不要です。

## DAGの実行

1. Airflow Web UIで `coin_gecko_btc_to_s3` DAGを有効化
2. 手動実行する場合は、DAGを選択して「Trigger DAG」をクリック
3. 定期実行はデフォルトで毎日実行されます（`schedule_interval='@daily'`）

## データ形式

### 取得されるデータ

- `timestamp`: データ取得時刻
- `last_updated`: CoinGecko APIの最終更新時刻
- `usd_price`: USD価格
- `eur_price`: EUR価格
- `jpy_price`: JPY価格
- `usd_market_cap`: USD時価総額
- `usd_24h_vol`: 24時間取引量（USD）
- `usd_24h_change`: 24時間価格変動率（%）

### S3保存形式

データは以下のパスに保存されます：

```
s3://<bucket-name>/btc-prices/<YYYY-MM-DD>/btc_price_<timestamp>.csv
s3://<bucket-name>/btc-prices/<YYYY-MM-DD>/btc_price_<timestamp>.parquet
```

## スケジュール変更

DAGの実行頻度を変更する場合は、`dags/coin_gecko_btc_to_s3.py`の`schedule`を編集（Airflow 3.0では`schedule_interval`から`schedule`に変更されました）：

```python
# 毎時間実行
schedule='@hourly'

# 毎週実行
schedule='@weekly'

# Cron形式（例: 毎日午前9時）
schedule='0 9 * * *'
```

## トラブルシューティング

### DAGが表示されない

- `dags/`ディレクトリにファイルが正しく配置されているか確認
- Airflowのログを確認: `docker-compose logs airflow-scheduler`

### S3アップロードエラー

- AWS認証情報が正しく設定されているか確認
- S3バケットが存在し、適切な権限が設定されているか確認
- IAMポリシーでS3への書き込み権限があるか確認

### APIリクエストエラー

- CoinGecko APIのレート制限に達していないか確認
- インターネット接続を確認
- APIキーが正しく設定されているか確認（オプション）

## 停止とクリーンアップ

```bash
# サービスを停止
docker-compose down

# データベースも含めて完全に削除
docker-compose down -v
```

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。

