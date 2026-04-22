import threading
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime
from .config import ConfigManager
from .data_provider import DataProvider


class WatchlistManager:
    _instance: Optional['WatchlistManager'] = None
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
        self._watchlist: List[str] = self._config.load_watchlist()
        self._quotes: Dict[str, Optional[Dict[str, Any]]] = {}
        self._last_update: Optional[datetime] = None
        self._refresh_thread: Optional[threading.Thread] = None
        self._stop_refresh = threading.Event()
        self._on_update_callbacks: List[Callable] = []

    @property
    def watchlist(self) -> List[str]:
        """获取当前自选股列表"""
        return self._watchlist.copy()

    @property
    def quotes(self) -> Dict[str, Optional[Dict[str, Any]]]:
        """获取当前行情数据"""
        return self._quotes.copy()

    @property
    def last_update(self) -> Optional[datetime]:
        """获取最后更新时间"""
        return self._last_update

    def add_stock(self, symbol: str) -> bool:
        """
        添加股票到自选股
        
        参数:
            symbol: 股票代码
            
        返回:
            是否成功添加
        """
        symbol = symbol.upper().strip()
        if not symbol:
            return False

        if symbol in self._watchlist:
            return False

        self._watchlist.append(symbol)
        self._config.save_watchlist(self._watchlist)

        quote = self._data_provider.get_stock_quote(symbol)
        self._quotes[symbol] = quote

        self._notify_update()
        return True

    def remove_stock(self, symbol: str) -> bool:
        """
        从自选股移除股票
        
        参数:
            symbol: 股票代码
            
        返回:
            是否成功移除
        """
        symbol = symbol.upper().strip()
        if symbol not in self._watchlist:
            return False

        self._watchlist.remove(symbol)
        self._config.save_watchlist(self._watchlist)

        if symbol in self._quotes:
            del self._quotes[symbol]

        self._notify_update()
        return True

    def is_in_watchlist(self, symbol: str) -> bool:
        """检查股票是否在自选股中"""
        return symbol.upper().strip() in self._watchlist

    def refresh_all(self) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        刷新所有自选股的行情数据
        
        返回:
            更新后的行情数据
        """
        for symbol in self._watchlist:
            quote = self._data_provider.get_stock_quote(symbol)
            self._quotes[symbol] = quote

        self._last_update = datetime.now()
        self._notify_update()
        return self._quotes.copy()

    def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取指定股票的行情数据
        
        参数:
            symbol: 股票代码
            
        返回:
            行情数据字典，如果不存在则返回 None
        """
        return self._quotes.get(symbol.upper().strip())

    def get_sorted_quotes(
        self,
        sort_by: str = 'symbol',
        ascending: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取排序后的行情数据列表
        
        参数:
            sort_by: 排序字段 ('symbol', 'name', 'current_price', 'change_percent', 'volume', 'market_cap')
            ascending: 是否升序
            
        返回:
            排序后的行情数据列表
        """
        valid_sort_fields = [
            'symbol', 'name', 'current_price', 'change', 'change_percent',
            'volume', 'avg_volume', 'market_cap', 'pe_ratio'
        ]

        if sort_by not in valid_sort_fields:
            sort_by = 'symbol'

        result = []
        for symbol in self._watchlist:
            quote = self._quotes.get(symbol)
            if quote:
                result.append(quote)

        result.sort(
            key=lambda x: x.get(sort_by, 0) if sort_by != 'symbol' else x.get(sort_by, ''),
            reverse=not ascending
        )

        return result

    def start_auto_refresh(self, interval: Optional[int] = None):
        """
        启动自动刷新线程
        
        参数:
            interval: 刷新间隔（秒），如果为 None 则使用配置中的间隔
        """
        if self._refresh_thread and self._refresh_thread.is_alive():
            return

        if interval is None:
            interval = self._config.refresh_interval

        self._stop_refresh.clear()
        self._refresh_thread = threading.Thread(
            target=self._auto_refresh_loop,
            args=(interval,),
            daemon=True
        )
        self._refresh_thread.start()

    def stop_auto_refresh(self):
        """停止自动刷新"""
        self._stop_refresh.set()
        if self._refresh_thread:
            self._refresh_thread.join(timeout=2)
            self._refresh_thread = None

    def _auto_refresh_loop(self, interval: int):
        """自动刷新循环"""
        while not self._stop_refresh.is_set():
            if self._config.auto_refresh:
                try:
                    self.refresh_all()
                except Exception as e:
                    print(f"自动刷新失败: {e}")

            self._stop_refresh.wait(interval)

    def add_update_callback(self, callback: Callable):
        """
        添加更新回调函数
        
        参数:
            callback: 回调函数，当数据更新时会被调用
        """
        if callback not in self._on_update_callbacks:
            self._on_update_callbacks.append(callback)

    def remove_update_callback(self, callback: Callable):
        """移除更新回调函数"""
        if callback in self._on_update_callbacks:
            self._on_update_callbacks.remove(callback)

    def _notify_update(self):
        """通知所有回调函数数据已更新"""
        for callback in self._on_update_callbacks:
            try:
                callback()
            except Exception as e:
                print(f"调用更新回调失败: {e}")

    def get_total_value(self) -> Dict[str, float]:
        """
        计算自选股的总市值等统计信息
        
        返回:
            包含总市值、平均涨跌幅等统计信息的字典
        """
        total_market_cap = 0.0
        total_change_percent = 0.0
        count = 0
        up_count = 0
        down_count = 0
        flat_count = 0

        for symbol in self._watchlist:
            quote = self._quotes.get(symbol)
            if quote:
                total_market_cap += quote.get('market_cap', 0)
                change_pct = quote.get('change_percent', 0)
                total_change_percent += change_pct
                count += 1

                if change_pct > 0:
                    up_count += 1
                elif change_pct < 0:
                    down_count += 1
                else:
                    flat_count += 1

        avg_change_percent = total_change_percent / count if count > 0 else 0

        return {
            'total_stocks': len(self._watchlist),
            'valid_quotes': count,
            'total_market_cap': total_market_cap,
            'avg_change_percent': round(avg_change_percent, 2),
            'up_count': up_count,
            'down_count': down_count,
            'flat_count': flat_count,
        }
