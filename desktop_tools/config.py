import json
import os
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Optional


@dataclass
class AppConfig:
    refresh_interval: int = 60
    auto_refresh: bool = True
    default_watchlist: List[str] = None
    news_count: int = 10
    screener_limit: int = 25

    def __post_init__(self):
        if self.default_watchlist is None:
            self.default_watchlist = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]


class ConfigManager:
    _instance: Optional['ConfigManager'] = None
    _config_file: Path = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._config = AppConfig()
        self._ensure_config_dir()
        self._load_config()

    def _ensure_config_dir(self):
        config_dir = Path.home() / ".stock_monitor"
        config_dir.mkdir(parents=True, exist_ok=True)
        self._config_file = config_dir / "config.json"
        self._watchlist_file = config_dir / "watchlist.json"

    def _load_config(self):
        if self._config_file.exists():
            try:
                with open(self._config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self._config.refresh_interval = data.get('refresh_interval', 60)
                    self._config.auto_refresh = data.get('auto_refresh', True)
                    self._config.news_count = data.get('news_count', 10)
                    self._config.screener_limit = data.get('screener_limit', 25)
            except (json.JSONDecodeError, IOError):
                pass

    def _save_config(self):
        try:
            with open(self._config_file, 'w', encoding='utf-8') as f:
                json.dump(asdict(self._config), f, indent=2, ensure_ascii=False)
        except IOError as e:
            print(f"保存配置失败: {e}")

    @property
    def refresh_interval(self) -> int:
        return self._config.refresh_interval

    @refresh_interval.setter
    def refresh_interval(self, value: int):
        if value < 10:
            value = 10
        self._config.refresh_interval = value
        self._save_config()

    @property
    def auto_refresh(self) -> bool:
        return self._config.auto_refresh

    @auto_refresh.setter
    def auto_refresh(self, value: bool):
        self._config.auto_refresh = value
        self._save_config()

    @property
    def news_count(self) -> int:
        return self._config.news_count

    @news_count.setter
    def news_count(self, value: int):
        self._config.news_count = max(1, min(50, value))
        self._save_config()

    @property
    def screener_limit(self) -> int:
        return self._config.screener_limit

    @screener_limit.setter
    def screener_limit(self, value: int):
        self._config.screener_limit = max(1, min(250, value))
        self._save_config()

    def load_watchlist(self) -> List[str]:
        if self._watchlist_file.exists():
            try:
                with open(self._watchlist_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return self._config.default_watchlist.copy()

    def save_watchlist(self, tickers: List[str]):
        try:
            with open(self._watchlist_file, 'w', encoding='utf-8') as f:
                json.dump(tickers, f, indent=2, ensure_ascii=False)
        except IOError as e:
            print(f"保存自选股失败: {e}")
