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
                return []

            formatted_news = []
            for item in news_list[:count]:
                if not isinstance(item, dict):
                    continue

                if 'content' in item:
                    content = item.get('content', {})
                    if not isinstance(content, dict):
                        content = item
                else:
                    content = item

                title = content.get('title', '') or content.get('headline', '')
                
                link = ''
                click_through = content.get('clickThroughUrl')
                if isinstance(click_through, dict):
                    link = click_through.get('url', '')
                if not link:
                    link = (content.get('link', '') or 
                            content.get('url', '') or 
                            content.get('canonicalUrl', '') or
                            content.get('previewUrl', ''))
                
                publisher = ''
                provider_data = content.get('provider')
                
                if isinstance(provider_data, dict):
                    publisher = provider_data.get('displayName', '') or provider_data.get('name', '') or provider_data.get('publisher', '')
                elif isinstance(provider_data, str):
                    publisher = provider_data
                
                if not publisher:
                    publisher = content.get('publisher', '') or content.get('source', '')

                published_at = 0
                if 'providerPublishTime' in content:
                    published_at = content.get('providerPublishTime', 0)
                elif 'publishTime' in content:
                    published_at = content.get('publishTime', 0)
                elif 'pubDate' in content:
                    pub_date = content.get('pubDate')
                    
                    if pub_date:
                        if isinstance(pub_date, (int, float)):
                            published_at = pub_date
                        elif isinstance(pub_date, str):
                            if pub_date.isdigit():
                                published_at = int(pub_date)
                            else:
                                try:
                                    import dateutil.parser
                                    parsed = dateutil.parser.parse(pub_date)
                                    published_at = parsed.timestamp()
                                except:
                                    pass
                elif 'displayTime' in content:
                    display_time = content.get('displayTime', 0)
                    if isinstance(display_time, (int, float)):
                        published_at = display_time
                elif 'datetime' in content:
                    published_at = content.get('datetime', 0)

                news_type = content.get('type', '') or content.get('contentType', '') or content.get('newsType', '')

                related_tickers = content.get('relatedTickers', []) or content.get('related', [])
                if content.get('finance'):
                    finance = content.get('finance', {})
                    if isinstance(finance, dict):
                        related_tickers = finance.get('relatedTickers', related_tickers)
                
                if isinstance(related_tickers, str):
                    related_tickers = [related_tickers]

                thumbnail = ''
                if content.get('thumbnail'):
                    thumb = content.get('thumbnail', {})
                    if isinstance(thumb, dict):
                        resolutions = thumb.get('resolutions', [])
                        if resolutions and len(resolutions) > 0:
                            thumbnail = resolutions[0].get('url', '')
                    elif isinstance(thumb, str):
                        thumbnail = thumb

                formatted_news.append({
                    'title': title,
                    'link': link,
                    'publisher': publisher,
                    'published_at': published_at,
                    'type': news_type,
                    'related_tickers': related_tickers,
                    'thumbnail': thumbnail
                })

            return formatted_news

        except Exception as e:
            print(f"获取股票 {symbol} 新闻失败: {e}")
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

    def get_financial_statements(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取股票财务报表数据，用于计算 F-Score
        返回: 包含利润表、资产负债表、现金流量表的字典
        """
        try:
            ticker = self._get_ticker(symbol)
            
            income_stmt = ticker.financials
            balance_sheet = ticker.balance_sheet
            cashflow = ticker.cashflow
            
            if income_stmt is None or balance_sheet is None or cashflow is None:
                return None
            
            if income_stmt.empty or balance_sheet.empty or cashflow.empty:
                return None
            
            return {
                'income_stmt': income_stmt,
                'balance_sheet': balance_sheet,
                'cashflow': cashflow
            }

        except Exception as e:
            print(f"获取股票 {symbol} 财务报表失败: {e}")
            return None

    def get_latest_two_years_financials(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取最近两年的财务数据，用于同比比较
        返回: {
            'current': {...},  # 最近一年
            'previous': {...}  # 前一年
        }
        """
        try:
            financials = self.get_financial_statements(symbol)
            if not financials:
                return None
            
            income_stmt = financials['income_stmt']
            balance_sheet = financials['balance_sheet']
            cashflow = financials['cashflow']
            
            if len(income_stmt.columns) < 2 or len(balance_sheet.columns) < 2 or len(cashflow.columns) < 2:
                return None
            
            current_year = income_stmt.columns[0]
            previous_year = income_stmt.columns[1]
            
            def get_value(df, key, year):
                try:
                    if key in df.index:
                        val = df.loc[key, year]
                        if pd.isna(val):
                            return None
                        return float(val)
                    return None
                except Exception:
                    return None
            
            current = {
                'net_income': get_value(income_stmt, 'Net Income', current_year),
                'total_assets': get_value(balance_sheet, 'Total Assets', current_year),
                'operating_cashflow': get_value(cashflow, 'Operating Cash Flow', current_year),
                'long_term_debt': get_value(balance_sheet, 'Long Term Debt', current_year),
                'current_assets': get_value(balance_sheet, 'Current Assets', current_year),
                'current_liabilities': get_value(balance_sheet, 'Current Liabilities', current_year),
                'total_revenue': get_value(income_stmt, 'Total Revenue', current_year),
                'cost_of_revenue': get_value(income_stmt, 'Cost Of Revenue', current_year),
                'shares_outstanding': get_value(balance_sheet, 'Ordinary Shares Number', current_year),
                'retained_earnings': get_value(balance_sheet, 'Retained Earnings', current_year),
                'ebit': get_value(income_stmt, 'EBIT', current_year),
                'total_liabilities': get_value(balance_sheet, 'Total Liabilities Net Minority Interest', current_year),
            }
            
            previous = {
                'net_income': get_value(income_stmt, 'Net Income', previous_year),
                'total_assets': get_value(balance_sheet, 'Total Assets', previous_year),
                'operating_cashflow': get_value(cashflow, 'Operating Cash Flow', previous_year),
                'long_term_debt': get_value(balance_sheet, 'Long Term Debt', previous_year),
                'current_assets': get_value(balance_sheet, 'Current Assets', previous_year),
                'current_liabilities': get_value(balance_sheet, 'Current Liabilities', previous_year),
                'total_revenue': get_value(income_stmt, 'Total Revenue', previous_year),
                'cost_of_revenue': get_value(income_stmt, 'Cost Of Revenue', previous_year),
                'shares_outstanding': get_value(balance_sheet, 'Ordinary Shares Number', previous_year),
                'retained_earnings': get_value(balance_sheet, 'Retained Earnings', previous_year),
                'ebit': get_value(income_stmt, 'EBIT', previous_year),
                'total_liabilities': get_value(balance_sheet, 'Total Liabilities Net Minority Interest', previous_year),
            }
            
            return {
                'current': current,
                'previous': previous,
                'current_year': current_year.strftime('%Y-%m-%d') if hasattr(current_year, 'strftime') else str(current_year),
                'previous_year': previous_year.strftime('%Y-%m-%d') if hasattr(previous_year, 'strftime') else str(previous_year)
            }

        except Exception as e:
            print(f"获取股票 {symbol} 两年财务数据失败: {e}")
            return None
