import threading
from typing import List, Dict, Any, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from .data_provider import DataProvider


class StockScreener:
    _instance: Optional['StockScreener'] = None
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
        self._data_provider = DataProvider()
        self._default_stock_pool = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM',
            'V', 'JNJ', 'WMT', 'PG', 'MA', 'UNH', 'HD', 'DIS', 'PYPL', 'BAC',
            'ADBE', 'NFLX', 'CRM', 'INTC', 'VZ', 'KO', 'PEP', 'NKE', 'MRK',
            'T', 'PFE', 'BA', 'CSCO', 'XOM', 'CVX', 'ORCL', 'ABBV', 'ACN',
            'AVGO', 'COST', 'TMO', 'TXN', 'QCOM', 'LIN', 'NEE', 'LOW',
            'MCD', 'UNP', 'IBM', 'AMD', 'AMGN', 'CAT', 'GS', 'NOW',
            'RTX', 'DE', 'SPGI', 'BLK', 'BKNG', 'DUK', 'MDT', 'ISRG',
            'GILD', 'SYK', 'PLD', 'AXP', 'MO', 'ZTS', 'TGT', 'CI',
            'EL', 'REGN', 'ADP', 'LRCX', 'ITW', 'SO', 'CME', 'HON',
            'USB', 'CL', 'NSC', 'BIIB', 'SBUX', 'MMC', 'ILMN', 'GE',
            'GD', 'SHW', 'TJX', 'PNC', 'FIS', 'AMAT', 'MDLZ', 'AON'
        ]
        self._max_workers = 10

    def screen_by_recommendation(
        self,
        stock_pool: Optional[List[str]] = None,
        min_buy_ratio: float = 0.5,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        推荐评级选股：筛选分析师评级为 strongBuy 或 buy 比例较高的股票
        
        参数:
            stock_pool: 股票池，默认为默认股票池
            min_buy_ratio: 最低买入比例 (0-1)，默认为 0.5
            progress_callback: 进度回调函数 (current, total, symbol)
            limit: 最大返回数量
        
        返回:
            符合条件的股票列表，按买入评分排序
        """
        if stock_pool is None:
            stock_pool = self._default_stock_pool.copy()

        results = []
        total = len(stock_pool)

        def check_stock(symbol: str) -> Optional[Dict[str, Any]]:
            try:
                rec = self._data_provider.get_latest_recommendation(symbol)
                if rec is None:
                    return None

                total_analysts = rec['total_analysts']
                if total_analysts == 0:
                    return None

                buy_count = rec['strongBuy'] + rec['buy']
                buy_ratio = buy_count / total_analysts

                if buy_ratio >= min_buy_ratio:
                    quote = self._data_provider.get_stock_quote(symbol)
                    current_price = quote['current_price'] if quote else 0
                    change_percent = quote['change_percent'] if quote else 0

                    return {
                        'symbol': symbol,
                        'name': quote['name'] if quote else symbol,
                        'strongBuy': rec['strongBuy'],
                        'buy': rec['buy'],
                        'hold': rec['hold'],
                        'sell': rec['sell'],
                        'strongSell': rec['strongSell'],
                        'total_analysts': total_analysts,
                        'buy_ratio': round(buy_ratio * 100, 2),
                        'buy_score': buy_count * 100 + rec['strongBuy'] * 50,
                        'current_price': current_price,
                        'change_percent': round(change_percent, 2),
                    }
                return None
            except Exception as e:
                print(f"检查股票 {symbol} 推荐评级失败: {e}")
                return None

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            future_to_symbol = {executor.submit(check_stock, symbol): symbol for symbol in stock_pool}

            for idx, future in enumerate(as_completed(future_to_symbol)):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    print(f"处理股票 {symbol} 时出错: {e}")

                if progress_callback:
                    progress_callback(idx + 1, total, symbol)

        results.sort(key=lambda x: x['buy_score'], reverse=True)
        return results[:limit]

    def screen_by_price_target(
        self,
        stock_pool: Optional[List[str]] = None,
        min_upside: float = 10.0,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        目标价选股：筛选当前价格低于分析师目标价的股票
        
        参数:
            stock_pool: 股票池，默认为默认股票池
            min_upside: 最低上涨空间百分比，默认为 10%
            progress_callback: 进度回调函数 (current, total, symbol)
            limit: 最大返回数量
        
        返回:
            符合条件的股票列表，按上涨空间排序
        """
        if stock_pool is None:
            stock_pool = self._default_stock_pool.copy()

        results = []
        total = len(stock_pool)

        def check_stock(symbol: str) -> Optional[Dict[str, Any]]:
            try:
                targets = self._data_provider.get_analyst_price_targets(symbol)
                if targets is None:
                    return None

                current = targets.get('current', 0)
                mean_target = targets.get('mean', 0)

                if current <= 0 or mean_target <= 0:
                    return None

                upside_potential = ((mean_target - current) / current) * 100

                if upside_potential >= min_upside:
                    quote = self._data_provider.get_stock_quote(symbol)
                    name = quote['name'] if quote else symbol
                    change_percent = quote['change_percent'] if quote else 0

                    return {
                        'symbol': symbol,
                        'name': name,
                        'current_price': round(current, 2),
                        'target_low': round(targets.get('low', 0), 2),
                        'target_high': round(targets.get('high', 0), 2),
                        'target_mean': round(mean_target, 2),
                        'target_median': round(targets.get('median', 0), 2),
                        'upside_potential': round(upside_potential, 2),
                        'change_percent': round(change_percent, 2),
                    }
                return None
            except Exception as e:
                print(f"检查股票 {symbol} 目标价失败: {e}")
                return None

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            future_to_symbol = {executor.submit(check_stock, symbol): symbol for symbol in stock_pool}

            for idx, future in enumerate(as_completed(future_to_symbol)):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    print(f"处理股票 {symbol} 时出错: {e}")

                if progress_callback:
                    progress_callback(idx + 1, total, symbol)

        results.sort(key=lambda x: x['upside_potential'], reverse=True)
        return results[:limit]

    def screen_by_insider_buys(
        self,
        stock_pool: Optional[List[str]] = None,
        days: int = 30,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        内部人买卖选股：筛选近期有内部人买入的股票
        
        参数:
            stock_pool: 股票池，默认为默认股票池
            days: 检查的天数，默认为 30 天
            progress_callback: 进度回调函数 (current, total, symbol)
            limit: 最大返回数量
        
        返回:
            符合条件的股票列表
        """
        if stock_pool is None:
            stock_pool = self._default_stock_pool.copy()

        results = []
        total = len(stock_pool)

        def check_stock(symbol: str) -> Optional[Dict[str, Any]]:
            try:
                purchases = self._data_provider.get_insider_purchases(symbol)
                if purchases is None or purchases.empty:
                    return None

                from datetime import datetime, timedelta
                cutoff_date = datetime.now() - timedelta(days=days)

                recent_purchases = purchases[purchases.index >= cutoff_date]
                if recent_purchases.empty:
                    return None

                quote = self._data_provider.get_stock_quote(symbol)
                name = quote['name'] if quote else symbol
                current_price = quote['current_price'] if quote else 0
                change_percent = quote['change_percent'] if quote else 0

                total_shares = int(recent_purchases['Shares'].sum())
                total_value = int(recent_purchases['Value'].sum())
                transaction_count = len(recent_purchases)

                latest = recent_purchases.iloc[0]
                latest_insider = latest.get('Insider', 'Unknown')
                latest_position = latest.get('Position', 'Unknown')
                latest_transaction = latest.get('Transaction', 'Unknown')
                latest_shares = int(latest.get('Shares', 0))
                latest_value = int(latest.get('Value', 0))

                return {
                    'symbol': symbol,
                    'name': name,
                    'current_price': round(current_price, 2),
                    'change_percent': round(change_percent, 2),
                    'transaction_count': transaction_count,
                    'total_shares': total_shares,
                    'total_value': total_value,
                    'latest_insider': latest_insider,
                    'latest_position': latest_position,
                    'latest_transaction': latest_transaction,
                    'latest_shares': latest_shares,
                    'latest_value': latest_value,
                    'latest_date': recent_purchases.index[0].strftime('%Y-%m-%d') if len(recent_purchases) > 0 else '',
                }
            except Exception as e:
                print(f"检查股票 {symbol} 内部人交易失败: {e}")
                return None

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            future_to_symbol = {executor.submit(check_stock, symbol): symbol for symbol in stock_pool}

            for idx, future in enumerate(as_completed(future_to_symbol)):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    print(f"处理股票 {symbol} 时出错: {e}")

                if progress_callback:
                    progress_callback(idx + 1, total, symbol)

        results.sort(key=lambda x: x['total_value'], reverse=True)
        return results[:limit]

    @property
    def default_stock_pool(self) -> List[str]:
        """获取默认股票池"""
        return self._default_stock_pool.copy()

    def set_default_stock_pool(self, stocks: List[str]):
        """设置默认股票池"""
        self._default_stock_pool = [s.upper() for s in stocks]
