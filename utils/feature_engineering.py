"""
最適化された特徴量エンジニアリングモジュール

予測精度向上のための高度な特徴量作成と最適化
"""

import pandas as pd
import numpy as np
from typing import Optional, List


def create_lag_features(
    df: pd.DataFrame,
    target_col: str = "usd_price",
    lags: List[int] = [1, 2, 3, 6, 12, 24, 48, 72, 168],
) -> pd.DataFrame:
    """
    ラグ特徴量を作成（最適化版）

    Args:
        df: データフレーム
        target_col: ターゲット列名
        lags: ラグのリスト

    Returns:
        ラグ特徴量が追加されたデータフレーム
    """
    df = df.copy()

    for lag in lags:
        df[f"{target_col}_lag_{lag}"] = df[target_col].shift(lag)
        # ラグ特徴量の変化率も追加
        if lag > 1:
            df[f"{target_col}_lag_{lag}_pct"] = (
                df[target_col] / (df[f"{target_col}_lag_{lag}"] + 1e-10) - 1
            ) * 100

    return df


def create_moving_averages(
    df: pd.DataFrame,
    target_col: str = "usd_price",
    windows: List[int] = [6, 12, 24, 48, 72, 168],
) -> pd.DataFrame:
    """
    移動平均特徴量を作成（最適化版）

    Args:
        df: データフレーム
        target_col: ターゲット列名
        windows: 移動平均のウィンドウサイズのリスト

    Returns:
        移動平均特徴量が追加されたデータフレーム
    """
    df = df.copy()

    for window in windows:
        # 単純移動平均
        ma = df[target_col].rolling(window=window, min_periods=1).mean()
        df[f"{target_col}_ma_{window}"] = ma

        # 指数移動平均
        ema = df[target_col].ewm(span=window, adjust=False).mean()
        df[f"{target_col}_ema_{window}"] = ema

        # 価格と移動平均の比率
        df[f"{target_col}_ma_{window}_ratio"] = df[target_col] / (ma + 1e-10)
        df[f"{target_col}_ema_{window}_ratio"] = df[target_col] / (ema + 1e-10)

        # 移動平均の勾配（トレンド）
        if len(df) > window:
            df[f"{target_col}_ma_{window}_trend"] = ma.diff(window // 2)

    return df


def create_volatility_features(
    df: pd.DataFrame,
    target_col: str = "usd_price",
    windows: List[int] = [6, 12, 24, 48, 168],
) -> pd.DataFrame:
    """
    ボラティリティ特徴量を作成（最適化版）

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
    df[f"{target_col}_log_returns"] = np.log(df[target_col] / df[target_col].shift(1))

    for window in windows:
        # ボラティリティ（標準偏差）
        df[f"{target_col}_volatility_{window}"] = (
            df[f"{target_col}_returns"].rolling(window=window, min_periods=1).std()
        )

        # 最大値・最小値
        df[f"{target_col}_max_{window}"] = (
            df[target_col].rolling(window=window, min_periods=1).max()
        )
        df[f"{target_col}_min_{window}"] = (
            df[target_col].rolling(window=window, min_periods=1).min()
        )

        # レンジ
        df[f"{target_col}_range_{window}"] = (
            df[f"{target_col}_max_{window}"] - df[f"{target_col}_min_{window}"]
        )

        # 現在価格の位置（0-1の範囲）
        df[f"{target_col}_position_{window}"] = (
            df[target_col] - df[f"{target_col}_min_{window}"]
        ) / (df[f"{target_col}_range_{window}"] + 1e-10)

        # 平均絶対偏差
        df[f"{target_col}_mad_{window}"] = (
            df[target_col]
            .rolling(window=window, min_periods=1)
            .apply(lambda x: np.mean(np.abs(x - x.mean())), raw=True)
        )

    return df


def create_rsi(
    df: pd.DataFrame, target_col: str = "usd_price", periods: List[int] = [7, 14, 21]
) -> pd.DataFrame:
    """
    RSI (Relative Strength Index) を計算（複数期間）

    Args:
        df: データフレーム
        target_col: ターゲット列名
        periods: RSI計算期間のリスト

    Returns:
        RSI特徴量が追加されたデータフレーム
    """
    df = df.copy()

    for period in periods:
        delta = df[target_col].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()

        rs = gain / (loss + 1e-10)
        df[f"{target_col}_rsi_{period}"] = 100 - (100 / (1 + rs))

    return df


def create_macd(
    df: pd.DataFrame,
    target_col: str = "usd_price",
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.DataFrame:
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
    df[f"{target_col}_macd_signal"] = (
        df[f"{target_col}_macd"].ewm(span=signal, adjust=False).mean()
    )
    df[f"{target_col}_macd_histogram"] = (
        df[f"{target_col}_macd"] - df[f"{target_col}_macd_signal"]
    )
    df[f"{target_col}_macd_ratio"] = df[f"{target_col}_macd"] / (df[target_col] + 1e-10)

    return df


def create_bollinger_bands(
    df: pd.DataFrame,
    target_col: str = "usd_price",
    period: int = 20,
    num_std: float = 2.0,
) -> pd.DataFrame:
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
    df[f"{target_col}_bb_width"] = (
        df[f"{target_col}_bb_upper"] - df[f"{target_col}_bb_lower"]
    )
    df[f"{target_col}_bb_position"] = (
        df[target_col] - df[f"{target_col}_bb_lower"]
    ) / (df[f"{target_col}_bb_width"] + 1e-10)
    df[f"{target_col}_bb_squeeze"] = df[f"{target_col}_bb_width"] / (
        df[target_col] + 1e-10
    )

    return df


def create_adx(
    df: pd.DataFrame,
    high_col: Optional[str] = None,
    low_col: Optional[str] = None,
    close_col: str = "usd_price",
    period: int = 14,
) -> pd.DataFrame:
    """
    ADX (Average Directional Index) を計算
    高値・安値がない場合は価格から推定

    Args:
        df: データフレーム
        high_col: 高値列名
        low_col: 安値列名
        close_col: 終値列名
        period: ADX計算期間

    Returns:
        ADX特徴量が追加されたデータフレーム
    """
    df = df.copy()

    # 高値・安値がない場合は価格から推定
    if high_col is None or high_col not in df.columns:
        df["_high"] = df[close_col] * 1.01  # 簡易推定
    else:
        df["_high"] = df[high_col]

    if low_col is None or low_col not in df.columns:
        df["_low"] = df[close_col] * 0.99  # 簡易推定
    else:
        df["_low"] = df[low_col]

    # True Range
    tr1 = df["_high"] - df["_low"]
    tr2 = abs(df["_high"] - df[close_col].shift(1))
    tr3 = abs(df["_low"] - df[close_col].shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Directional Movement
    plus_dm = df["_high"].diff()
    minus_dm = -df["_low"].diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm < 0] = 0

    # ADX計算
    atr = tr.rolling(window=period, min_periods=1).mean()
    plus_di = 100 * (
        plus_dm.rolling(window=period, min_periods=1).mean() / (atr + 1e-10)
    )
    minus_di = 100 * (
        minus_dm.rolling(window=period, min_periods=1).mean() / (atr + 1e-10)
    )

    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10)
    df[f"{close_col}_adx"] = dx.rolling(window=period, min_periods=1).mean()
    df[f"{close_col}_plus_di"] = plus_di
    df[f"{close_col}_minus_di"] = minus_di

    # 一時列を削除
    df = df.drop(columns=["_high", "_low"], errors="ignore")

    return df


def create_time_features(
    df: pd.DataFrame, timestamp_col: str = "timestamp"
) -> pd.DataFrame:
    """
    時系列特徴量を作成（最適化版）

    Args:
        df: データフレーム
        timestamp_col: タイムスタンプ列名

    Returns:
        時系列特徴量が追加されたデータフレーム
    """
    df = df.copy()

    if timestamp_col in df.columns:
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])

        # 基本時間特徴量
        df["hour"] = df[timestamp_col].dt.hour
        df["day_of_week"] = df[timestamp_col].dt.dayofweek
        df["day_of_month"] = df[timestamp_col].dt.day
        df["week_of_year"] = df[timestamp_col].dt.isocalendar().week
        df["month"] = df[timestamp_col].dt.month
        df["quarter"] = df[timestamp_col].dt.quarter

        # 周期的な特徴量（sin/cos変換）
        df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
        df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
        df["day_of_week_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
        df["day_of_week_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)
        df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
        df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)
        df["week_of_year_sin"] = np.sin(2 * np.pi * df["week_of_year"] / 52)
        df["week_of_year_cos"] = np.cos(2 * np.pi * df["week_of_year"] / 52)

        # 週末フラグ
        df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

        # 月初・月末フラグ
        df["is_month_start"] = (df["day_of_month"] <= 3).astype(int)
        df["is_month_end"] = (df["day_of_month"] >= 28).astype(int)

    return df


def create_momentum_features(
    df: pd.DataFrame, target_col: str = "usd_price", periods: List[int] = [6, 12, 24]
) -> pd.DataFrame:
    """
    モメンタム特徴量を作成

    Args:
        df: データフレーム
        target_col: ターゲット列名
        periods: モメンタム計算期間

    Returns:
        モメンタム特徴量が追加されたデータフレーム
    """
    df = df.copy()

    for period in periods:
        # 価格変化率
        df[f"{target_col}_momentum_{period}"] = (
            df[target_col] / df[target_col].shift(period) - 1
        ) * 100

        # ROC (Rate of Change)
        df[f"{target_col}_roc_{period}"] = (
            (df[target_col] - df[target_col].shift(period))
            / (df[target_col].shift(period) + 1e-10)
        ) * 100

    return df


def create_all_features(
    df: pd.DataFrame, target_col: str = "usd_price", timestamp_col: str = "timestamp"
) -> pd.DataFrame:
    """
    すべての特徴量を作成（最適化版）

    Args:
        df: データフレーム
        target_col: ターゲット列名
        timestamp_col: タイムスタンプ列名

    Returns:
        すべての特徴量が追加されたデータフレーム
    """
    df = df.copy()

    # 時系列特徴量（最初に作成）
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
    df = create_adx(df, close_col=target_col)
    df = create_momentum_features(df, target_col)

    # 追加の特徴量
    if "usd_market_cap" in df.columns:
        df["price_to_market_cap"] = df[target_col] / (df["usd_market_cap"] + 1e-10)

    if "usd_24h_vol" in df.columns:
        df["volume_to_price"] = df["usd_24h_vol"] / (df[target_col] + 1e-10)
        df["volume_ma_24"] = df["usd_24h_vol"].rolling(window=24, min_periods=1).mean()
        df["volume_ratio"] = df["usd_24h_vol"] / (df["volume_ma_24"] + 1e-10)

    # 無限大とNaNを処理
    df = df.replace([np.inf, -np.inf], np.nan)

    return df


def prepare_features_for_training(
    df: pd.DataFrame,
    target_col: str = "usd_price",
    forecast_horizon: int = 1,
    drop_na: bool = True,
) -> tuple[pd.DataFrame, pd.Series]:
    """
    訓練用に特徴量とターゲットを準備（最適化版）

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
    exclude_cols = {
        target_col,
        "target",
        "timestamp",
        "date",
        "last_updated",
        "timestamp_jst",
        "retrieved_at",
    }
    feature_cols = [col for col in df.columns if col not in exclude_cols]

    X = df[feature_cols]
    y = df["target"]

    if drop_na:
        # NaNを含む行を削除
        mask = ~(X.isna().any(axis=1) | y.isna())
        X = X[mask]
        y = y[mask]

    return X, y


def select_features(
    X: pd.DataFrame,
    y: pd.Series,
    method: str = "correlation",
    top_k: int = 100,
) -> pd.DataFrame:
    """
    特徴量選択（相関または重要度ベース）

    Args:
        X: 特徴量データフレーム
        y: ターゲット
        method: 選択方法 ('correlation' または 'importance')
        top_k: 選択する特徴量数

    Returns:
        選択された特徴量データフレーム
    """
    if method == "correlation":
        # 相関ベースの特徴量選択
        correlations = X.corrwith(y).abs().sort_values(ascending=False)
        selected_features = correlations.head(top_k).index.tolist()
        return X[selected_features]
    else:
        # 重要度ベースの特徴量選択（モデルが必要）
        return X  # 実装はモデル訓練時に
