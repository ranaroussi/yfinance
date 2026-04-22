import threading
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from .config import ConfigManager
from .data_provider import DataProvider
from .watchlist import WatchlistManager


class NewsManager:
    _instance: Optional['NewsManager'] = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._config = ConfigManager()
        self._data_provider = DataProvider()
        self._watchlist_manager = WatchlistManager()
        self._news_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._cache_time: Dict[str, datetime] = {}
        self._cache_duration = 300
        self._max_workers = 5
        self._on_news_update_callbacks: List[Callable] = []

    def get_news_for_symbol(
        self,
        symbol: str,
        count: Optional[int] = None,
        force_refresh: bool = False
    ) -> List[Dict[str, Any]]:
        """
        获取指定股票的新闻资讯
        
        参数:
            symbol: 股票代码
            count: 获取的新闻数量，默认为配置中的数量
            force_refresh: 是否强制刷新缓存
            
        返回:
            新闻列表
        """
        symbol = symbol.upper().strip()
        if count is None:
            count = self._config.news_count

        cache_key = f"{symbol}_{count}"
        now = datetime.now()

        if not force_refresh and self._is_cache_valid(cache_key):
            return self._news_cache.get(cache_key, [])

        news_list = self._data_provider.get_stock_news(symbol, count)

        formatted_news = []
        for item in news_list:
            published_at = item.get('published_at', 0)
            formatted_date = ''
            
            if published_at:
                try:
                    if isinstance(published_at, (int, float)):
                        published_dt = datetime.fromtimestamp(published_at)
                        formatted_date = published_dt.strftime('%Y-%m-%d %H:%M')
                    elif isinstance(published_at, str):
                        if published_at.isdigit():
                            published_dt = datetime.fromtimestamp(int(published_at))
                            formatted_date = published_dt.strftime('%Y-%m-%d %H:%M')
                        else:
                            try:
                                from dateutil import parser
                                published_dt = parser.parse(published_at)
                                formatted_date = published_dt.strftime('%Y-%m-%d %H:%M')
                            except:
                                formatted_date = published_at
                except (ValueError, OSError):
                    formatted_date = ''
            else:
                formatted_date = ''

            formatted_news.append({
                'symbol': symbol,
                'title': item.get('title', ''),
                'link': item.get('link', ''),
                'publisher': item.get('publisher', ''),
                'published_at': published_at,
                'formatted_date': formatted_date,
                'type': item.get('type', ''),
                'related_tickers': item.get('related_tickers', []),
                'thumbnail': item.get('thumbnail', ''),
            })

        formatted_news.sort(
            key=lambda x: x.get('published_at', 0),
            reverse=True
        )

        self._news_cache[cache_key] = formatted_news
        self._cache_time[cache_key] = now

        return formatted_news

    def get_all_watchlist_news(
        self,
        count_per_symbol: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> List[Dict[str, Any]]:
        """
        获取自选股中所有股票的新闻并汇总
        
        参数:
            count_per_symbol: 每个股票获取的新闻数量
            progress_callback: 进度回调函数 (current, total, symbol)
            
        返回:
            按时间排序的新闻列表
        """
        watchlist = self._watchlist_manager.watchlist
        if not watchlist:
            return []

        if count_per_symbol is None:
            count_per_symbol = self._config.news_count

        all_news = []
        total = len(watchlist)

        def fetch_news(symbol: str) -> List[Dict[str, Any]]:
            return self.get_news_for_symbol(symbol, count_per_symbol)

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            future_to_symbol = {executor.submit(fetch_news, symbol): symbol for symbol in watchlist}

            for idx, future in enumerate(as_completed(future_to_symbol)):
                symbol = future_to_symbol[future]
                try:
                    news_list = future.result()
                    all_news.extend(news_list)
                except Exception as e:
                    print(f"获取股票 {symbol} 新闻时出错: {e}")

                if progress_callback:
                    progress_callback(idx + 1, total, symbol)

        all_news.sort(
            key=lambda x: x.get('published_at', 0),
            reverse=True
        )

        self._notify_news_update()
        return all_news

    def _is_cache_valid(self, cache_key: str) -> bool:
        """检查缓存是否有效"""
        if cache_key not in self._cache_time:
            return False
        elapsed = (datetime.now() - self._cache_time[cache_key]).total_seconds()
        return elapsed < self._cache_duration

    def clear_cache(self, symbol: Optional[str] = None):
        """
        清除新闻缓存
        
        参数:
            symbol: 指定股票代码，如果为 None 则清除所有缓存
        """
        if symbol:
            symbol = symbol.upper().strip()
            keys_to_remove = [k for k in self._news_cache.keys() if k.startswith(f"{symbol}_")]
            for key in keys_to_remove:
                if key in self._news_cache:
                    del self._news_cache[key]
                if key in self._cache_time:
                    del self._cache_time[key]
        else:
            self._news_cache.clear()
            self._cache_time.clear()

    def add_news_update_callback(self, callback: Callable):
        """添加新闻更新回调函数"""
        if callback not in self._on_news_update_callbacks:
            self._on_news_update_callbacks.append(callback)

    def remove_news_update_callback(self, callback: Callable):
        """移除新闻更新回调函数"""
        if callback in self._on_news_update_callbacks:
            self._on_news_update_callbacks.remove(callback)

    def _notify_news_update(self):
        """通知所有回调函数新闻已更新"""
        for callback in self._on_news_update_callbacks:
            try:
                callback()
            except Exception as e:
                print(f"调用新闻更新回调失败: {e}")

    def search_news(
        self,
        keyword: str,
        count: int = 20
    ) -> List[Dict[str, Any]]:
        """
        搜索新闻（通过搜索股票代码间接获取相关新闻）
        
        参数:
            keyword: 搜索关键词
            count: 返回的新闻数量
            
        返回:
            相关新闻列表
        """
        symbols = self._data_provider.search_symbol(keyword)
        if not symbols:
            return []

        all_news = []
        for symbol_info in symbols[:5]:
            symbol = symbol_info.get('symbol', '')
            if symbol:
                news = self.get_news_for_symbol(symbol, count // 5 + 1)
                all_news.extend(news)

        all_news.sort(
            key=lambda x: x.get('published_at', 0),
            reverse=True
        )

        return all_news[:count]
