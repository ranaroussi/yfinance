"""
桌面端股票监控应用
基于 yfinance 和 PyQt5 开发

功能模块:
- config: 配置管理
- data_provider: 数据获取
- watchlist: 自选股管理
- news_manager: 新闻资讯
- screener: 选股工具
- gui: 图形界面
"""

__version__ = "1.0.0"

from .config import ConfigManager, AppConfig
from .data_provider import DataProvider
from .watchlist import WatchlistManager
from .news_manager import NewsManager
from .screener import StockScreener

__all__ = [
    'ConfigManager',
    'AppConfig',
    'DataProvider',
    'WatchlistManager',
    'NewsManager',
    'StockScreener',
]
