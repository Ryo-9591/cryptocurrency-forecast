"""設定モジュール"""

from .settings import Settings

__all__ = ["Settings", "get_settings"]

_settings_instance: Settings | None = None


def get_settings() -> Settings:
    """設定インスタンスを取得（シングルトン）"""
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance
