"""
特徴量エンジニアリングモジュール

仮想通貨価格予測のための特徴量を作成
"""

import pandas as pd
import numpy as np
from typing import Optional


def create_lag_features(df: pd.DataFrame, target_col: str = "usd_price", lags: list = [1, 2, 3, 7, 14, 30]) -> pd.DataFrame:
    """
    ラグ特徴量を作成
    
    Args:
        df: データフレーム
        target_col: ターゲット列名
        lags: ラグのリスト（例: [1, 2, 3, 7, 14, 30]）
    
    Returns:
        ラグ特徴量が追加されたデータフレーム
    """
    df = df.copy()
    
    for lag in lags:
        df[f"{target_col}_lag_{lag}"] = df[target_col].shift(lag)
    
    return df


def create_moving_averages(df: pd.DataFrame, target_col: str = "usd_price", windows: list = [7, 14, 30, 60]) -> pd.DataFrame:
    """
    移動平均特徴量を作成
    
    Args:
        df: データフレーム
        target_col: ターゲット列名
        windows: 移動平均のウィンドウサイズのリスト
    
    Returns:
        移動平均特徴量が追加されたデータフレーム
    """
    df = df.copy()
    
    for window in windows:
        df[f"{target_col}_ma_{window}"] = df[target_col].rolling(window=window, min_periods=1).mean()
        df[f"{target_col}_ema_{window}"] = df[target_col].ewm(span=window, adjust=False).mean()
    
    return df


def create_volatility_features(df: pd.DataFrame, target_col: str = "usd_price", windows: list = [7, 14, 30]) -> pd.DataFrame:
    """
    ボラティリティ特徴量を作成
    
    Args:
        df: データフレーム
        target_col: ターゲット列名
        windows: ボラティリティ計算のウィンドウサイズ
    
    Returns:
        ボラティリティ特徴量が追加されたデータフレーム
    """
    df = df.copy()
    
    # 価格変動率
    df[f"{target_col}_returns"] = df[target_col].pct_change()
    
    for window in windows:
        df[f"{target_col}_volatility_{window}"] = df[f"{target_col}_returns"].rolling(window=window, min_periods=1).std()
        df[f"{target_col}_max_{window}"] = df[target_col].rolling(window=window, min_periods=1).max()
        df[f"{target_col}_min_{window}"] = df[target_col].rolling(window=window, min_periods=1).min()
        df[f"{target_col}_range_{window}"] = df[f"{target_col}_max_{window}"] - df[f"{target_col}_min_{window}"]
    
    return df


def create_rsi(df: pd.DataFrame, target_col: str = "usd_price", period: int = 14) -> pd.DataFrame:
    """
    RSI (Relative Strength Index) を計算
    
    Args:
        df: データフレーム
        target_col: ターゲット列名
        period: RSI計算期間（デフォルト: 14）
    
    Returns:
        RSI特徴量が追加されたデータフレーム
    """
    df = df.copy()
    
    delta = df[target_col].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=1).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()
    
    rs = gain / loss
    df[f"{target_col}_rsi_{period}"] = 100 - (100 / (1 + rs))
    
    return df


def create_macd(df: pd.DataFrame, target_col: str = "usd_price", fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """
    MACD (Moving Average Convergence Divergence) を計算
    
    Args:
        df: データフレーム
        target_col: ターゲット列名
        fast: 短期EMA期間
        slow: 長期EMA期間
        signal: シグナルライン期間
    
    Returns:
        MACD特徴量が追加されたデータフレーム
    """
    df = df.copy()
    
    ema_fast = df[target_col].ewm(span=fast, adjust=False).mean()
    ema_slow = df[target_col].ewm(span=slow, adjust=False).mean()
    
    df[f"{target_col}_macd"] = ema_fast - ema_slow
    df[f"{target_col}_macd_signal"] = df[f"{target_col}_macd"].ewm(span=signal, adjust=False).mean()
    df[f"{target_col}_macd_histogram"] = df[f"{target_col}_macd"] - df[f"{target_col}_macd_signal"]
    
    return df


def create_bollinger_bands(df: pd.DataFrame, target_col: str = "usd_price", period: int = 20, num_std: float = 2.0) -> pd.DataFrame:
    """
    ボリンジャーバンドを計算
    
    Args:
        df: データフレーム
        target_col: ターゲット列名
        period: 移動平均期間
        num_std: 標準偏差の倍数
    
    Returns:
        ボリンジャーバンド特徴量が追加されたデータフレーム
    """
    df = df.copy()
    
    ma = df[target_col].rolling(window=period, min_periods=1).mean()
    std = df[target_col].rolling(window=period, min_periods=1).std()
    
    df[f"{target_col}_bb_upper"] = ma + (std * num_std)
    df[f"{target_col}_bb_lower"] = ma - (std * num_std)
    df[f"{target_col}_bb_middle"] = ma
    df[f"{target_col}_bb_width"] = df[f"{target_col}_bb_upper"] - df[f"{target_col}_bb_lower"]
    df[f"{target_col}_bb_position"] = (df[target_col] - df[f"{target_col}_bb_lower"]) / df[f"{target_col}_bb_width"]
    
    return df


def create_time_features(df: pd.DataFrame, timestamp_col: str = "timestamp") -> pd.DataFrame:
    """
    時系列特徴量を作成（時間、曜日、月など）
    
    Args:
        df: データフレーム
        timestamp_col: タイムスタンプ列名
    
    Returns:
        時系列特徴量が追加されたデータフレーム
    """
    df = df.copy()
    
    if timestamp_col in df.columns:
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        df["hour"] = df[timestamp_col].dt.hour
        df["day_of_week"] = df[timestamp_col].dt.dayofweek
        df["day_of_month"] = df[timestamp_col].dt.day
        df["month"] = df[timestamp_col].dt.month
        df["quarter"] = df[timestamp_col].dt.quarter
        
        # 周期的な特徴量（sin/cos変換）
        df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
        df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
        df["day_of_week_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
        df["day_of_week_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)
        df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
        df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    
    return df


def create_all_features(df: pd.DataFrame, target_col: str = "usd_price", timestamp_col: str = "timestamp") -> pd.DataFrame:
    """
    すべての特徴量を作成
    
    Args:
        df: データフレーム
        target_col: ターゲット列名
        timestamp_col: タイムスタンプ列名
    
    Returns:
        すべての特徴量が追加されたデータフレーム
    """
    df = df.copy()
    
    # 時系列特徴量
    df = create_time_features(df, timestamp_col)
    
    # ラグ特徴量
    df = create_lag_features(df, target_col)
    
    # 移動平均
    df = create_moving_averages(df, target_col)
    
    # ボラティリティ
    df = create_volatility_features(df, target_col)
    
    # 技術指標
    df = create_rsi(df, target_col)
    df = create_macd(df, target_col)
    df = create_bollinger_bands(df, target_col)
    
    # 追加の特徴量
    if "usd_market_cap" in df.columns:
        df["price_to_market_cap"] = df[target_col] / (df["usd_market_cap"] + 1e-10)
    
    if "usd_24h_vol" in df.columns:
        df["volume_to_price"] = df["usd_24h_vol"] / (df[target_col] + 1e-10)
    
    return df


def prepare_features_for_training(df: pd.DataFrame, target_col: str = "usd_price", 
                                   forecast_horizon: int = 1, drop_na: bool = True) -> tuple:
    """
    訓練用に特徴量とターゲットを準備
    
    Args:
        df: データフレーム
        target_col: ターゲット列名
        forecast_horizon: 予測期間（何ステップ先を予測するか）
        drop_na: NaNを含む行を削除するか
    
    Returns:
        (X, y) タプル
    """
    df = df.copy()
    
    # ターゲット変数を作成（forecast_horizonステップ先の価格）
    df["target"] = df[target_col].shift(-forecast_horizon)
    
    # 特徴量列を選択（ターゲット列とタイムスタンプ列を除く）
    exclude_cols = [target_col, "target", "timestamp", "date", "last_updated"]
    feature_cols = [col for col in df.columns if col not in exclude_cols]
    
    X = df[feature_cols]
    y = df["target"]
    
    if drop_na:
        # NaNを含む行を削除
        mask = ~(X.isna().any(axis=1) | y.isna())
        X = X[mask]
        y = y[mask]
    
    return X, y

