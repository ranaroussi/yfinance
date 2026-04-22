import yfinance as yf
import pandas as pd
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import threading


class DataProvider:
    _instance: Optional['DataProvider'] = None
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
        self._ticker_cache: Dict[str, yf.Ticker] = {}
        self._cache_time: Dict[str, datetime] = {}
        self._cache_duration = timedelta(seconds=30)

    def _get_ticker(self, symbol: str) -> yf.Ticker:
        """获取或创建 Ticker 对象"""
        symbol = symbol.upper()
        if symbol not in self._ticker_cache:
            self._ticker_cache[symbol] = yf.Ticker(symbol)
        return self._ticker_cache[symbol]

    def _is_cache_valid(self, symbol: str) -> bool:
        """检查缓存是否有效"""
        if symbol not in self._cache_time:
            return False
        return datetime.now() - self._cache_time[symbol] < self._cache_duration

    def _update_cache_time(self, symbol: str):
        """更新缓存时间"""
        self._cache_time[symbol.upper()] = datetime.now()

    def get_stock_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取股票实时行情数据
        返回: 包含价格、涨跌幅、成交量等信息的字典
        """
        try:
            ticker = self._get_ticker(symbol)
            info = ticker.info

            if not info:
                return None

            quote = {
                'symbol': symbol.upper(),
                'name': info.get('longName', info.get('shortName', symbol)),
                'current_price': info.get('currentPrice', info.get('regularMarketPrice', 0)),
                'previous_close': info.get('previousClose', 0),
                'change': 0,
                'change_percent': 0,
                'open': info.get('open', 0),
                'high': info.get('dayHigh', 0),
                'low': info.get('dayLow', 0),
                'volume': info.get('volume', 0),
                'avg_volume': info.get('averageVolume', 0),
                'market_cap': info.get('marketCap', 0),
                'pe_ratio': info.get('trailingPE', 0),
                'currency': info.get('currency', 'USD'),
                'exchange': info.get('exchange', ''),
            }

            if quote['current_price'] and quote['previous_close']:
                quote['change'] = quote['current_price'] - quote['previous_close']
                if quote['previous_close'] > 0:
                    quote['change_percent'] = (quote['change'] / quote['previous_close']) * 100

            self._update_cache_time(symbol)
            return quote

        except Exception as e:
            print(f"获取股票 {symbol} 行情数据失败: {e}")
            return None

    def get_multiple_quotes(self, symbols: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        批量获取多个股票的行情数据
        """
        results = {}
        for symbol in symbols:
            results[symbol] = self.get_stock_quote(symbol)
        return results

    def get_stock_news(self, symbol: str, count: int = 10) -> List[Dict[str, Any]]:
        """
        获取股票相关新闻
        """
        try:
            ticker = self._get_ticker(symbol)
            news_list = ticker.news

            if not news_list:
                print(f"[调试] 股票 {symbol} 没有获取到新闻数据")
                return []

            print(f"[调试] 股票 {symbol} 获取到 {len(news_list)} 条新闻")

            formatted_news = []
            for idx, item in enumerate(news_list[:count]):
                if not isinstance(item, dict):
                    print(f"[调试] 新闻项 {idx} 不是字典类型: {type(item)}")
                    continue

                print(f"[调试] 新闻项 {idx} 键名: {list(item.keys())}")

                title = item.get('title', '') or item.get('headline', '')
                link = item.get('link', '') or item.get('url', '')
                publisher = item.get('publisher', '') or item.get('source', '')

                published_at = 0
                if 'providerPublishTime' in item:
                    published_at = item.get('providerPublishTime', 0)
                elif 'publishTime' in item:
                    published_at = item.get('publishTime', 0)
                elif 'datetime' in item:
                    published_at = item.get('datetime', 0)

                news_type = item.get('type', '') or item.get('newsType', '')

                related_tickers = item.get('relatedTickers', []) or item.get('related', [])
                if isinstance(related_tickers, str):
                    related_tickers = [related_tickers]

                thumbnail = ''
                if item.get('thumbnail'):
                    thumb = item.get('thumbnail', {})
                    if isinstance(thumb, dict):
                        resolutions = thumb.get('resolutions', [])
                        if resolutions and len(resolutions) > 0:
                            thumbnail = resolutions[0].get('url', '')
                    elif isinstance(thumb, str):
                        thumbnail = thumb

                print(f"[调试] 新闻 {idx}: title='{title[:50] if title else '空'}...', publisher='{publisher}'")

                formatted_news.append({
                    'title': title,
                    'link': link,
                    'publisher': publisher,
                    'published_at': published_at,
                    'type': news_type,
                    'related_tickers': related_tickers,
                    'thumbnail': thumbnail
                })

            print(f"[调试] 格式化完成，共 {len(formatted_news)} 条新闻")
            return formatted_news

        except Exception as e:
            print(f"获取股票 {symbol} 新闻失败: {e}")
            import traceback
            traceback.print_exc()
            return []

    def get_recommendations(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        获取分析师推荐评级
        返回 DataFrame，包含 period, strongBuy, buy, hold, sell, strongSell
        """
        try:
            ticker = self._get_ticker(symbol)
            return ticker.recommendations
        except Exception as e:
            print(f"获取股票 {symbol} 推荐评级失败: {e}")
            return None

    def get_latest_recommendation(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取最新的分析师推荐评级
        返回包含 strongBuy, buy, hold, sell, strongSell 的字典
        """
        try:
            recs = self.get_recommendations(symbol)
            if recs is None or recs.empty:
                return None

            latest = recs.iloc[-1]
            return {
                'symbol': symbol.upper(),
                'period': latest.get('period', ''),
                'strongBuy': int(latest.get('strongBuy', 0)),
                'buy': int(latest.get('buy', 0)),
                'hold': int(latest.get('hold', 0)),
                'sell': int(latest.get('sell', 0)),
                'strongSell': int(latest.get('strongSell', 0)),
                'total_analysts': int(latest.get('strongBuy', 0)) + int(latest.get('buy', 0)) +
                                  int(latest.get('hold', 0)) + int(latest.get('sell', 0)) +
                                  int(latest.get('strongSell', 0))
            }
        except Exception as e:
            print(f"获取股票 {symbol} 最新推荐评级失败: {e}")
            return None

    def get_analyst_price_targets(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取分析师目标价
        返回: current, low, high, mean, median
        """
        try:
            ticker = self._get_ticker(symbol)
            targets = ticker.analyst_price_targets

            if not targets:
                return None

            info = ticker.info
            current_price = info.get('currentPrice', info.get('regularMarketPrice', 0))

            result = {
                'symbol': symbol.upper(),
                'current': current_price,
                'low': targets.get('low', 0),
                'high': targets.get('high', 0),
                'mean': targets.get('mean', 0),
                'median': targets.get('median', 0),
            }

            if result['mean'] and result['mean'] > 0:
                result['upside_potential'] = ((result['mean'] - result['current']) / result['current']) * 100 if result['current'] > 0 else 0
            else:
                result['upside_potential'] = 0

            return result

        except Exception as e:
            print(f"获取股票 {symbol} 目标价失败: {e}")
            return None

    def get_insider_transactions(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        获取内部人交易记录
        """
        try:
            ticker = self._get_ticker(symbol)
            return ticker.insider_transactions
        except Exception as e:
            print(f"获取股票 {symbol} 内部人交易失败: {e}")
            return None

    def get_insider_purchases(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        获取内部人买入记录
        """
        try:
            ticker = self._get_ticker(symbol)
            return ticker.insider_purchases
        except Exception as e:
            print(f"获取股票 {symbol} 内部人买入失败: {e}")
            return None

    def has_recent_insider_buys(self, symbol: str, days: int = 30) -> bool:
        """
        检查股票在指定天数内是否有内部人买入
        """
        try:
            purchases = self.get_insider_purchases(symbol)
            if purchases is None or purchases.empty:
                return False

            cutoff_date = datetime.now() - timedelta(days=days)
            return (purchases.index >= cutoff_date).any()

        except Exception as e:
            print(f"检查股票 {symbol} 内部人买入失败: {e}")
            return False

    def search_symbol(self, keyword: str) -> List[Dict[str, Any]]:
        """
        搜索股票代码
        """
        try:
            results = yf.search(keyword, quotes_count=10)
            formatted = []
            for item in results.get('quotes', []):
                formatted.append({
                    'symbol': item.get('symbol', ''),
                    'name': item.get('shortname', item.get('longname', '')),
                    'type': item.get('quoteType', ''),
                    'exchange': item.get('exchange', ''),
                })
            return formatted
        except Exception as e:
            print(f"搜索股票失败: {e}")
            return []

    def get_stock_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取股票详细信息
        """
        try:
            ticker = self._get_ticker(symbol)
            info = ticker.info

            if not info:
                return None

            return {
                'symbol': symbol.upper(),
                'name': info.get('longName', info.get('shortName', symbol)),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'country': info.get('country', ''),
                'website': info.get('website', ''),
                'description': info.get('longBusinessSummary', ''),
                'employees': info.get('fullTimeEmployees', 0),
                'market_cap': info.get('marketCap', 0),
                'pe_ratio': info.get('trailingPE', 0),
                'forward_pe': info.get('forwardPE', 0),
                'dividend_yield': info.get('dividendYield', 0),
                '52_week_high': info.get('fiftyTwoWeekHigh', 0),
                '52_week_low': info.get('fiftyTwoWeekLow', 0),
            }

        except Exception as e:
            print(f"获取股票 {symbol} 详细信息失败: {e}")
            return None
