#!/usr/bin/env python3
"""
S3バケットの内容を確認するスクリプト
"""

import boto3
import os
from datetime import datetime

# 環境変数から設定を取得
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-1")
bucket_name = os.getenv("S3_BUCKET_NAME")

if not bucket_name:
    print("エラー: S3_BUCKET_NAME環境変数が設定されていません")
    exit(1)

print(f"S3バケット: {bucket_name}")
print(f"リージョン: {aws_region}")
print("-" * 60)

# S3クライアントを作成
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region,
)

try:
    # btc-pricesプレフィックスでオブジェクトを検索
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="btc-prices/")

    if "Contents" in response and len(response["Contents"]) > 0:
        print(f"\n見つかったオブジェクト数: {len(response['Contents'])}")
        print("\nファイル一覧:")
        print("-" * 60)

        # 日付ごとにグループ化
        files_by_date = {}
        for obj in response["Contents"]:
            key = obj["Key"]
            # 日付を抽出 (btc-prices/YYYY-MM-DD/...)
            parts = key.split("/")
            if len(parts) >= 2:
                date = parts[1]
                if date not in files_by_date:
                    files_by_date[date] = []
                files_by_date[date].append(
                    {
                        "key": key,
                        "size": obj["Size"],
                        "modified": obj["LastModified"],
                    }
                )

        # 日付順にソートして表示
        for date in sorted(files_by_date.keys(), reverse=True):
            print(f"\n📅 {date}:")
            for file_info in files_by_date[date]:
                size_kb = file_info["size"] / 1024
                print(
                    f"  - {file_info['key']} ({size_kb:.2f} KB) - {file_info['modified']}"
                )
    else:
        print("\n⚠️  S3バケットにデータが見つかりませんでした")
        print("   DAGがまだ実行されていないか、エラーが発生している可能性があります")

except Exception as e:
    print(f"\n❌ エラーが発生しました: {e}")
    print("   AWS認証情報とS3バケットの設定を確認してください")
