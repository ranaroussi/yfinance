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

    def screen_by_piotroski_fscore(
        self,
        stock_pool: Optional[List[str]] = None,
        min_fscore: int = 7,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        皮奥特罗斯基 F-Score 选股策略
        
        核心算法(计分):
        盈利能力(4分):
        - ROA > 0 (1分)
        - 经营现金流 > 0 (1分)
        - 经营现金流 > ROA (1分)
        - ROA 同比改善 (1分)
        
        杠杆与流动性(3分):
        - 长期负债/总资产同比下降 (1分)
        - 流动比率同比上升 (1分)
        - 总股本未增加 (1分)
        
        经营效率(2分):
        - 毛利率同比上升 (1分)
        - 资产周转率同比上升 (1分)
        
        参数:
            stock_pool: 股票池，默认为默认股票池
            min_fscore: 最低 F-Score 分数 (1-9)，默认为 7
            progress_callback: 进度回调函数 (current, total, symbol)
            limit: 最大返回数量
        
        返回:
            符合条件的股票列表，按 F-Score 降序排列
        """
        if stock_pool is None:
            stock_pool = self._default_stock_pool.copy()

        results = []
        total = len(stock_pool)

        def calculate_fscore(financials: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            """计算 F-Score 并返回详细得分"""
            current = financials['current']
            previous = financials['previous']

            scores = {
                'total': 0,
                'profitability': 0,
                'leverage': 0,
                'efficiency': 0,
                'details': {}
            }

            def safe_divide(a, b):
                if b is None or b == 0:
                    return None
                if a is None:
                    return None
                return a / b

            current_roa = safe_divide(current['net_income'], current['total_assets'])
            previous_roa = safe_divide(previous['net_income'], previous['total_assets'])

            if current_roa is not None and current_roa > 0:
                scores['profitability'] += 1
                scores['details']['roa_positive'] = 1
            else:
                scores['details']['roa_positive'] = 0

            if current['operating_cashflow'] is not None and current['operating_cashflow'] > 0:
                scores['profitability'] += 1
                scores['details']['cfo_positive'] = 1
            else:
                scores['details']['cfo_positive'] = 0

            if (current['operating_cashflow'] is not None and current['net_income'] is not None and
                    current['operating_cashflow'] > current['net_income']):
                scores['profitability'] += 1
                scores['details']['cfo_gt_net_income'] = 1
            else:
                scores['details']['cfo_gt_net_income'] = 0

            if (current_roa is not None and previous_roa is not None and
                    current_roa > previous_roa):
                scores['profitability'] += 1
                scores['details']['roa_improving'] = 1
            else:
                scores['details']['roa_improving'] = 0

            current_leverage = safe_divide(current['long_term_debt'], current['total_assets'])
            previous_leverage = safe_divide(previous['long_term_debt'], previous['total_assets'])

            if (current_leverage is not None and previous_leverage is not None and
                    current_leverage < previous_leverage):
                scores['leverage'] += 1
                scores['details']['leverage_decreasing'] = 1
                scores['details']['leverage_change'] = '下降'
            else:
                scores['details']['leverage_decreasing'] = 0
                scores['details']['leverage_change'] = '上升' if (current_leverage and previous_leverage and current_leverage > previous_leverage) else '不变'

            current_current_ratio = safe_divide(current['current_assets'], current['current_liabilities'])
            previous_current_ratio = safe_divide(previous['current_assets'], previous['current_liabilities'])

            if (current_current_ratio is not None and previous_current_ratio is not None and
                    current_current_ratio > previous_current_ratio):
                scores['leverage'] += 1
                scores['details']['current_ratio_improving'] = 1
                scores['details']['current_ratio_change'] = '上升'
            else:
                scores['details']['current_ratio_improving'] = 0
                scores['details']['current_ratio_change'] = '下降' if (current_current_ratio and previous_current_ratio and current_current_ratio < previous_current_ratio) else '不变'

            if (current['shares_outstanding'] is not None and previous['shares_outstanding'] is not None and
                    current['shares_outstanding'] <= previous['shares_outstanding']):
                scores['leverage'] += 1
                scores['details']['shares_not_increasing'] = 1
            else:
                scores['details']['shares_not_increasing'] = 0

            current_gross_margin = None
            previous_gross_margin = None
            if (current['total_revenue'] is not None and current['total_revenue'] > 0 and
                    current['cost_of_revenue'] is not None):
                current_gross_margin = (current['total_revenue'] - current['cost_of_revenue']) / current['total_revenue']
            if (previous['total_revenue'] is not None and previous['total_revenue'] > 0 and
                    previous['cost_of_revenue'] is not None):
                previous_gross_margin = (previous['total_revenue'] - previous['cost_of_revenue']) / previous['total_revenue']

            if (current_gross_margin is not None and previous_gross_margin is not None and
                    current_gross_margin > previous_gross_margin):
                scores['efficiency'] += 1
                scores['details']['gross_margin_improving'] = 1
                scores['details']['gross_margin_change'] = '上升'
            else:
                scores['details']['gross_margin_improving'] = 0
                scores['details']['gross_margin_change'] = '下降' if (current_gross_margin and previous_gross_margin and current_gross_margin < previous_gross_margin) else '不变'

            current_asset_turnover = safe_divide(current['total_revenue'], current['total_assets'])
            previous_asset_turnover = safe_divide(previous['total_revenue'], previous['total_assets'])

            if (current_asset_turnover is not None and previous_asset_turnover is not None and
                    current_asset_turnover > previous_asset_turnover):
                scores['efficiency'] += 1
                scores['details']['asset_turnover_improving'] = 1
                scores['details']['asset_turnover_change'] = '上升'
            else:
                scores['details']['asset_turnover_improving'] = 0
                scores['details']['asset_turnover_change'] = '下降' if (current_asset_turnover and previous_asset_turnover and current_asset_turnover < previous_asset_turnover) else '不变'

            scores['total'] = scores['profitability'] + scores['leverage'] + scores['efficiency']
            scores['current_roa'] = current_roa
            scores['current_cfo'] = current['operating_cashflow']

            return scores

        def check_stock(symbol: str) -> Optional[Dict[str, Any]]:
            try:
                financials = self._data_provider.get_latest_two_years_financials(symbol)
                if financials is None:
                    return None

                fscore = calculate_fscore(financials)
                if fscore is None:
                    return None

                if fscore['total'] < min_fscore:
                    return None

                quote = self._data_provider.get_stock_quote(symbol)
                name = quote['name'] if quote else symbol
                current_price = quote['current_price'] if quote else 0
                change_percent = quote['change_percent'] if quote else 0

                return {
                    'symbol': symbol,
                    'name': name,
                    'fscore_total': fscore['total'],
                    'fscore_profitability': fscore['profitability'],
                    'fscore_leverage': fscore['leverage'],
                    'fscore_efficiency': fscore['efficiency'],
                    'current_roa': fscore['current_roa'],
                    'current_cfo': fscore['current_cfo'],
                    'leverage_change': fscore['details']['leverage_change'],
                    'current_ratio_change': fscore['details']['current_ratio_change'],
                    'gross_margin_change': fscore['details']['gross_margin_change'],
                    'asset_turnover_change': fscore['details']['asset_turnover_change'],
                    'current_price': round(current_price, 2) if current_price else 0,
                    'change_percent': round(change_percent, 2) if change_percent else 0,
                    'current_year': financials.get('current_year', ''),
                    'previous_year': financials.get('previous_year', ''),
                }
            except Exception as e:
                print(f"检查股票 {symbol} F-Score 失败: {e}")
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

        results.sort(key=lambda x: x['fscore_total'], reverse=True)
        return results[:limit]

    @property
    def default_stock_pool(self) -> List[str]:
        """获取默认股票池"""
        return self._default_stock_pool.copy()

    def set_default_stock_pool(self, stocks: List[str]):
        """设置默认股票池"""
        self._default_stock_pool = [s.upper() for s in stocks]
