import threading
from typing import List, Dict, Any, Optional, Callable, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import math

from .data_provider import DataProvider


class ValuationMetrics:
    def __init__(self):
        self.symbol = ''
        self.name = ''
        
        self.pe_trailing = None
        self.pe_forward = None
        self.pb_ratio = None
        self.peg_ratio = None
        self.price_to_sales = None
        self.ev_to_ebitda = None
        self.dividend_yield = None
        self.free_cash_flow = None
        self.fcf_per_share = None
        
        self.sector = ''
        self.industry = ''
        self.current_price = None
        self.market_cap = None
        self.shares_outstanding = None
        
        self.industry_avg_pe = None
        self.industry_avg_pb = None
        self.industry_avg_ps = None
        
        self.dcf_intrinsic_value = None
        self.safe_price = None
        self.margin_of_safety = None
        
        self.price_reasonableness_score = 0
        self.growth_score = 0
        self.safety_score = 0
        self.total_score = 0
        
        self.valuation_status = ''
        self.reasonable_price_low = None
        self.reasonable_price_high = None
        
        self.growth_rate_5y = None
        self.revenue_growth = None
        self.earnings_growth = None
        self.debt_to_equity = None
        self.current_ratio = None
        self.roe = None
        self.roa = None
        
        self.fcf_growth = None
        self.net_income = None
        self.total_debt = None
        self.cash_and_equivalents = None
        self.operating_cash_flow = None
        self.capital_expenditures = None


class ValuationAnalyzer:
    _instance: Optional['ValuationAnalyzer'] = None
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
        self._max_workers = 5
        
        self._industry_avg_cache: Dict[str, Dict[str, float]] = {}
        self._cache_expiry = timedelta(hours=24)
        self._cache_time: Dict[str, datetime] = {}

    def _safe_get(self, data: Dict, key: str, default=None):
        value = data.get(key)
        if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
            return default
        return value

    def _safe_divide(self, a, b) -> Optional[float]:
        if a is None or b is None or b == 0:
            return None
        try:
            result = float(a) / float(b)
            if math.isnan(result) or math.isinf(result):
                return None
            return result
        except (TypeError, ValueError):
            return None

    def get_valuation_metrics(self, symbol: str) -> Optional[ValuationMetrics]:
        try:
            symbol = symbol.upper().strip()
            metrics = ValuationMetrics()
            metrics.symbol = symbol
            
            ticker = self._data_provider._get_ticker(symbol)
            info = ticker.info
            
            if not info:
                return None
            
            metrics.name = self._safe_get(info, 'longName', self._safe_get(info, 'shortName', symbol))
            metrics.current_price = self._safe_get(info, 'currentPrice', self._safe_get(info, 'regularMarketPrice'))
            metrics.market_cap = self._safe_get(info, 'marketCap')
            metrics.sector = self._safe_get(info, 'sector', '')
            metrics.industry = self._safe_get(info, 'industry', '')
            
            metrics.pe_trailing = self._safe_get(info, 'trailingPE')
            metrics.pe_forward = self._safe_get(info, 'forwardPE')
            metrics.pb_ratio = self._safe_get(info, 'priceToBook')
            metrics.peg_ratio = self._safe_get(info, 'pegRatio')
            metrics.price_to_sales = self._safe_get(info, 'priceToSalesTrailing12Months')
            metrics.ev_to_ebitda = self._safe_get(info, 'enterpriseToEbitda')
            
            dividend_yield = self._safe_get(info, 'dividendYield')
            if dividend_yield is not None:
                metrics.dividend_yield = dividend_yield * 100
            
            metrics.shares_outstanding = self._safe_get(info, 'sharesOutstanding')
            metrics.debt_to_equity = self._safe_get(info, 'debtToEquity')
            metrics.current_ratio = self._safe_get(info, 'currentRatio')
            metrics.roe = self._safe_get(info, 'returnOnEquity')
            if metrics.roe is not None:
                metrics.roe = metrics.roe * 100
            metrics.roa = self._safe_get(info, 'returnOnAssets')
            if metrics.roa is not None:
                metrics.roa = metrics.roa * 100
            
            metrics.growth_rate_5y = self._safe_get(info, 'fiveYearAvgDividendYield')
            if metrics.growth_rate_5y is None:
                metrics.growth_rate_5y = self._safe_get(info, 'earningsQuarterlyGrowth')
                if metrics.growth_rate_5y is not None:
                    metrics.growth_rate_5y = metrics.growth_rate_5y * 100
            
            try:
                balance_sheet = ticker.balance_sheet
                if balance_sheet is not None and not balance_sheet.empty:
                    latest_col = balance_sheet.columns[0]
                    
                    cash = self._safe_get_from_df(balance_sheet, 'Cash And Cash Equivalents', latest_col)
                    if cash is None:
                        cash = self._safe_get_from_df(balance_sheet, 'Cash Cash Equivalents And Short Term Investments', latest_col)
                    metrics.cash_and_equivalents = cash
                    
                    total_debt = self._safe_get_from_df(balance_sheet, 'Total Debt', latest_col)
                    if total_debt is None:
                        long_term_debt = self._safe_get_from_df(balance_sheet, 'Long Term Debt', latest_col)
                        current_debt = self._safe_get_from_df(balance_sheet, 'Current Debt And Capital Lease Obligation', latest_col)
                        if long_term_debt is not None and current_debt is not None:
                            total_debt = long_term_debt + current_debt
                        elif long_term_debt is not None:
                            total_debt = long_term_debt
                    metrics.total_debt = total_debt
                    
                    shares = self._safe_get_from_df(balance_sheet, 'Ordinary Shares Number', latest_col)
                    if shares is not None and metrics.shares_outstanding is None:
                        metrics.shares_outstanding = shares
            except Exception as e:
                print(f"获取 {symbol} 资产负债表数据失败: {e}")
            
            try:
                cashflow = ticker.cashflow
                if cashflow is not None and not cashflow.empty:
                    latest_col = cashflow.columns[0]
                    
                    operating_cf = self._safe_get_from_df(cashflow, 'Operating Cash Flow', latest_col)
                    metrics.operating_cash_flow = operating_cf
                    
                    capex = self._safe_get_from_df(cashflow, 'Capital Expenditure', latest_col)
                    metrics.capital_expenditures = capex
                    
                    if operating_cf is not None and capex is not None:
                        metrics.free_cash_flow = operating_cf + capex
                        
                        if metrics.shares_outstanding and metrics.shares_outstanding > 0:
                            metrics.fcf_per_share = self._safe_divide(metrics.free_cash_flow, metrics.shares_outstanding)
            except Exception as e:
                print(f"获取 {symbol} 现金流量表数据失败: {e}")
            
            try:
                financials = ticker.financials
                if financials is not None and not financials.empty:
                    latest_col = financials.columns[0]
                    metrics.net_income = self._safe_get_from_df(financials, 'Net Income', latest_col)
            except Exception as e:
                print(f"获取 {symbol} 利润表数据失败: {e}")
            
            try:
                growth_estimates = ticker.growth_estimates
                if growth_estimates is not None and not growth_estimates.empty:
                    if '+5y' in growth_estimates.index:
                        stock_growth = growth_estimates.loc['+5y', 'stock']
                        if stock_growth is not None and not pd.isna(stock_growth):
                            metrics.growth_rate_5y = stock_growth * 100
            except Exception as e:
                print(f"获取 {symbol} 增长预测数据失败: {e}")
            
            return metrics
            
        except Exception as e:
            print(f"获取股票 {symbol} 估值数据失败: {e}")
            return None

    def _safe_get_from_df(self, df, key: str, col) -> Optional[float]:
        try:
            if key in df.index:
                val = df.loc[key, col]
                if pd.isna(val):
                    return None
                return float(val)
            return None
        except Exception:
            return None

    def calculate_dcf(self, metrics: ValuationMetrics, 
                      growth_rate: Optional[float] = None,
                      terminal_growth: float = 2.5,
                      discount_rate: float = 10.0,
                      margin_of_safety_pct: float = 25.0) -> Dict[str, Any]:
        result = {
            'intrinsic_value': None,
            'safe_price': None,
            'margin_of_safety': None,
            'assumptions': {
                'growth_rate': growth_rate,
                'terminal_growth': terminal_growth,
                'discount_rate': discount_rate,
                'margin_of_safety_pct': margin_of_safety_pct
            }
        }
        
        if metrics.fcf_per_share is None or metrics.fcf_per_share <= 0:
            if metrics.net_income is not None and metrics.shares_outstanding and metrics.shares_outstanding > 0:
                eps = self._safe_divide(metrics.net_income, metrics.shares_outstanding)
                if eps and eps > 0:
                    metrics.fcf_per_share = eps * 0.8
        
        if metrics.fcf_per_share is None or metrics.fcf_per_share <= 0:
            if metrics.pe_trailing and metrics.pe_trailing > 0 and metrics.current_price:
                eps = self._safe_divide(metrics.current_price, metrics.pe_trailing)
                if eps and eps > 0:
                    metrics.fcf_per_share = eps
        
        if metrics.fcf_per_share is None or metrics.fcf_per_share <= 0:
            return result
        
        if growth_rate is None:
            growth_rate = metrics.growth_rate_5y
        
        if growth_rate is None or growth_rate <= 0:
            if metrics.roe and metrics.roe > 0:
                if metrics.dividend_yield and metrics.dividend_yield > 0:
                    payout_ratio = self._safe_divide(metrics.dividend_yield, 100)
                    if payout_ratio:
                        growth_rate = metrics.roe * (1 - payout_ratio)
                else:
                    growth_rate = metrics.roe * 0.6
        
        if growth_rate is None or growth_rate <= 0:
            growth_rate = 5.0
        
        growth_rate = min(growth_rate, 15.0)
        result['assumptions']['growth_rate'] = growth_rate
        
        stages = [
            {'years': 5, 'growth': growth_rate},
            {'years': 5, 'growth': growth_rate * 0.6},
            {'years': 5, 'growth': growth_rate * 0.3},
        ]
        
        fcf = metrics.fcf_per_share
        total_pv = 0.0
        discount_rate_decimal = discount_rate / 100
        terminal_growth_decimal = terminal_growth / 100
        
        year = 0
        for stage in stages:
            for _ in range(stage['years']):
                year += 1
                fcf = fcf * (1 + stage['growth'] / 100)
                pv_fcf = fcf / ((1 + discount_rate_decimal) ** year)
                total_pv += pv_fcf
        
        terminal_value = (fcf * (1 + terminal_growth_decimal)) / (discount_rate_decimal - terminal_growth_decimal)
        pv_terminal = terminal_value / ((1 + discount_rate_decimal) ** year)
        
        intrinsic_value = total_pv + pv_terminal
        
        result['intrinsic_value'] = round(intrinsic_value, 2)
        
        if metrics.current_price and metrics.current_price > 0:
            safe_price = intrinsic_value * (1 - margin_of_safety_pct / 100)
            result['safe_price'] = round(safe_price, 2)
            
            margin = ((intrinsic_value - metrics.current_price) / metrics.current_price) * 100
            result['margin_of_safety'] = round(margin, 2)
        
        return result

    def calculate_scores(self, metrics: ValuationMetrics) -> Dict[str, Any]:
        scores = {
            'price_reasonableness': {
                'total': 0,
                'components': {}
            },
            'growth': {
                'total': 0,
                'components': {}
            },
            'safety': {
                'total': 0,
                'components': {}
            },
            'total': 0,
            'valuation_status': '',
            'reasonable_price_range': {
                'low': None,
                'high': None
            }
        }
        
        pr_score = 0
        pr_count = 0
        
        if metrics.pe_trailing and metrics.pe_trailing > 0:
            if metrics.pe_trailing < 10:
                pr_score += 25
            elif metrics.pe_trailing < 15:
                pr_score += 20
            elif metrics.pe_trailing < 20:
                pr_score += 15
            elif metrics.pe_trailing < 25:
                pr_score += 10
            elif metrics.pe_trailing < 30:
                pr_score += 5
            else:
                pr_score += 0
            pr_count += 1
            scores['price_reasonableness']['components']['pe_trailing'] = {
                'value': metrics.pe_trailing,
                'score': pr_score if pr_count == 1 else 0
            }
        
        if metrics.pe_forward and metrics.pe_forward > 0:
            pe_score = 0
            if metrics.pe_forward < 10:
                pe_score = 25
            elif metrics.pe_forward < 15:
                pe_score = 20
            elif metrics.pe_forward < 20:
                pe_score = 15
            elif metrics.pe_forward < 25:
                pe_score = 10
            elif metrics.pe_forward < 30:
                pe_score = 5
            pr_score += pe_score
            pr_count += 1
            scores['price_reasonableness']['components']['pe_forward'] = {
                'value': metrics.pe_forward,
                'score': pe_score
            }
        
        if metrics.pb_ratio and metrics.pb_ratio > 0:
            pb_score = 0
            if metrics.pb_ratio < 1:
                pb_score = 20
            elif metrics.pb_ratio < 2:
                pb_score = 15
            elif metrics.pb_ratio < 3:
                pb_score = 10
            elif metrics.pb_ratio < 5:
                pb_score = 5
            pr_score += pb_score
            pr_count += 1
            scores['price_reasonableness']['components']['pb_ratio'] = {
                'value': metrics.pb_ratio,
                'score': pb_score
            }
        
        if metrics.peg_ratio and metrics.peg_ratio > 0:
            peg_score = 0
            if metrics.peg_ratio < 1:
                peg_score = 20
            elif metrics.peg_ratio < 1.5:
                peg_score = 15
            elif metrics.peg_ratio < 2:
                peg_score = 10
            elif metrics.peg_ratio < 2.5:
                peg_score = 5
            pr_score += peg_score
            pr_count += 1
            scores['price_reasonableness']['components']['peg_ratio'] = {
                'value': metrics.peg_ratio,
                'score': peg_score
            }
        
        if metrics.price_to_sales and metrics.price_to_sales > 0:
            ps_score = 0
            if metrics.price_to_sales < 1:
                ps_score = 15
            elif metrics.price_to_sales < 2:
                ps_score = 10
            elif metrics.price_to_sales < 3:
                ps_score = 5
            pr_score += ps_score
            pr_count += 1
            scores['price_reasonableness']['components']['price_to_sales'] = {
                'value': metrics.price_to_sales,
                'score': ps_score
            }
        
        if metrics.ev_to_ebitda and metrics.ev_to_ebitda > 0:
            ev_score = 0
            if metrics.ev_to_ebitda < 5:
                ev_score = 15
            elif metrics.ev_to_ebitda < 8:
                ev_score = 10
            elif metrics.ev_to_ebitda < 10:
                ev_score = 5
            pr_score += ev_score
            pr_count += 1
            scores['price_reasonableness']['components']['ev_to_ebitda'] = {
                'value': metrics.ev_to_ebitda,
                'score': ev_score
            }
        
        if metrics.margin_of_safety is not None:
            mos_score = 0
            if metrics.margin_of_safety >= 50:
                mos_score = 25
            elif metrics.margin_of_safety >= 30:
                mos_score = 20
            elif metrics.margin_of_safety >= 15:
                mos_score = 15
            elif metrics.margin_of_safety >= 0:
                mos_score = 10
            elif metrics.margin_of_safety >= -20:
                mos_score = 5
            pr_score += mos_score
            pr_count += 1
            scores['price_reasonableness']['components']['margin_of_safety'] = {
                'value': metrics.margin_of_safety,
                'score': mos_score
            }
        
        scores['price_reasonableness']['total'] = min(pr_score, 100)
        
        growth_score = 0
        growth_count = 0
        
        if metrics.growth_rate_5y and metrics.growth_rate_5y > 0:
            gr_score = 0
            if metrics.growth_rate_5y >= 20:
                gr_score = 35
            elif metrics.growth_rate_5y >= 15:
                gr_score = 30
            elif metrics.growth_rate_5y >= 10:
                gr_score = 25
            elif metrics.growth_rate_5y >= 5:
                gr_score = 20
            elif metrics.growth_rate_5y >= 0:
                gr_score = 10
            growth_score += gr_score
            growth_count += 1
            scores['growth']['components']['growth_rate_5y'] = {
                'value': metrics.growth_rate_5y,
                'score': gr_score
            }
        
        if metrics.roe and metrics.roe > 0:
            roe_score = 0
            if metrics.roe >= 20:
                roe_score = 30
            elif metrics.roe >= 15:
                roe_score = 25
            elif metrics.roe >= 10:
                roe_score = 20
            elif metrics.roe >= 5:
                roe_score = 15
            growth_score += roe_score
            growth_count += 1
            scores['growth']['components']['roe'] = {
                'value': metrics.roe,
                'score': roe_score
            }
        
        if metrics.roa and metrics.roa > 0:
            roa_score = 0
            if metrics.roa >= 15:
                roa_score = 20
            elif metrics.roa >= 10:
                roa_score = 15
            elif metrics.roa >= 5:
                roa_score = 10
            elif metrics.roa >= 0:
                roa_score = 5
            growth_score += roa_score
            growth_count += 1
            scores['growth']['components']['roa'] = {
                'value': metrics.roa,
                'score': roa_score
            }
        
        if metrics.revenue_growth and metrics.revenue_growth > 0:
            rg_score = 0
            if metrics.revenue_growth >= 20:
                rg_score = 25
            elif metrics.revenue_growth >= 15:
                rg_score = 20
            elif metrics.revenue_growth >= 10:
                rg_score = 15
            elif metrics.revenue_growth >= 5:
                rg_score = 10
            growth_score += rg_score
            growth_count += 1
            scores['growth']['components']['revenue_growth'] = {
                'value': metrics.revenue_growth,
                'score': rg_score
            }
        
        scores['growth']['total'] = min(growth_score, 100)
        
        safety_score = 0
        safety_count = 0
        
        if metrics.dividend_yield and metrics.dividend_yield > 0:
            dy_score = 0
            if metrics.dividend_yield >= 5:
                dy_score = 25
            elif metrics.dividend_yield >= 3:
                dy_score = 20
            elif metrics.dividend_yield >= 2:
                dy_score = 15
            elif metrics.dividend_yield >= 1:
                dy_score = 10
            safety_score += dy_score
            safety_count += 1
            scores['safety']['components']['dividend_yield'] = {
                'value': metrics.dividend_yield,
                'score': dy_score
            }
        
        if metrics.debt_to_equity is not None:
            de_score = 0
            if metrics.debt_to_equity < 0.5:
                de_score = 25
            elif metrics.debt_to_equity < 1:
                de_score = 20
            elif metrics.debt_to_equity < 1.5:
                de_score = 15
            elif metrics.debt_to_equity < 2:
                de_score = 10
            elif metrics.debt_to_equity < 3:
                de_score = 5
            safety_score += de_score
            safety_count += 1
            scores['safety']['components']['debt_to_equity'] = {
                'value': metrics.debt_to_equity,
                'score': de_score
            }
        
        if metrics.current_ratio and metrics.current_ratio > 0:
            cr_score = 0
            if metrics.current_ratio >= 2:
                cr_score = 25
            elif metrics.current_ratio >= 1.5:
                cr_score = 20
            elif metrics.current_ratio >= 1:
                cr_score = 15
            elif metrics.current_ratio >= 0.8:
                cr_score = 10
            safety_score += cr_score
            safety_count += 1
            scores['safety']['components']['current_ratio'] = {
                'value': metrics.current_ratio,
                'score': cr_score
            }
        
        if metrics.free_cash_flow and metrics.free_cash_flow > 0:
            fcf_score = 25
            safety_score += fcf_score
            safety_count += 1
            scores['safety']['components']['free_cash_flow'] = {
                'value': metrics.free_cash_flow,
                'score': fcf_score
            }
        elif metrics.free_cash_flow is not None:
            fcf_score = 0
            scores['safety']['components']['free_cash_flow'] = {
                'value': metrics.free_cash_flow,
                'score': fcf_score
            }
        
        if metrics.cash_and_equivalents is not None and metrics.total_debt is not None:
            net_debt = metrics.total_debt - metrics.cash_and_equivalents
            if net_debt <= 0:
                nd_score = 15
                safety_score += nd_score
                safety_count += 1
                scores['safety']['components']['net_cash'] = {
                    'value': -net_debt,
                    'score': nd_score
                }
        
        scores['safety']['total'] = min(safety_score, 100)
        
        pr_weight = 0.40
        growth_weight = 0.35
        safety_weight = 0.25
        
        total_score = (
            scores['price_reasonableness']['total'] * pr_weight +
            scores['growth']['total'] * growth_weight +
            scores['safety']['total'] * safety_weight
        )
        scores['total'] = round(total_score, 1)
        
        if scores['total'] >= 80:
            scores['valuation_status'] = '显著低估'
        elif scores['total'] >= 65:
            scores['valuation_status'] = '合理偏低'
        elif scores['total'] >= 50:
            scores['valuation_status'] = '估值合理'
        elif scores['total'] >= 35:
            scores['valuation_status'] = '合理偏高'
        else:
            scores['valuation_status'] = '高估'
        
        if metrics.current_price and metrics.current_price > 0:
            if metrics.dcf_intrinsic_value and metrics.dcf_intrinsic_value > 0:
                scores['reasonable_price_range']['low'] = round(metrics.dcf_intrinsic_value * 0.7, 2)
                scores['reasonable_price_range']['high'] = round(metrics.dcf_intrinsic_value * 1.3, 2)
            else:
                if metrics.pe_trailing and metrics.pe_trailing > 0:
                    eps = metrics.current_price / metrics.pe_trailing
                    reasonable_pe = min(metrics.pe_trailing, 15) if scores['total'] >= 50 else 12
                    scores['reasonable_price_range']['low'] = round(eps * 10, 2)
                    scores['reasonable_price_range']['high'] = round(eps * reasonable_pe * 1.2, 2)
        
        return scores

    def analyze_stock(self, symbol: str) -> Optional[ValuationMetrics]:
        metrics = self.get_valuation_metrics(symbol)
        if metrics is None:
            return None
        
        dcf_result = self.calculate_dcf(metrics)
        metrics.dcf_intrinsic_value = dcf_result['intrinsic_value']
        metrics.safe_price = dcf_result['safe_price']
        metrics.margin_of_safety = dcf_result['margin_of_safety']
        
        scores = self.calculate_scores(metrics)
        
        metrics.price_reasonableness_score = scores['price_reasonableness']['total']
        metrics.growth_score = scores['growth']['total']
        metrics.safety_score = scores['safety']['total']
        metrics.total_score = scores['total']
        metrics.valuation_status = scores['valuation_status']
        metrics.reasonable_price_low = scores['reasonable_price_range']['low']
        metrics.reasonable_price_high = scores['reasonable_price_range']['high']
        
        return metrics

    def _prepare_radar_data(self, metrics: ValuationMetrics, scores: Dict) -> Dict[str, Any]:
        labels = [
            'PE估值',
            'PB估值', 
            'PEG估值',
            'PS估值',
            'EV/EBITDA',
            '股息收益',
            '增长潜力',
            '盈利能力',
            '财务健康',
            '安全边际'
        ]
        
        values = []
        
        pe_score = 0
        if metrics.pe_trailing and metrics.pe_trailing > 0:
            if metrics.pe_trailing < 10:
                pe_score = 100
            elif metrics.pe_trailing < 15:
                pe_score = 80
            elif metrics.pe_trailing < 20:
                pe_score = 60
            elif metrics.pe_trailing < 25:
                pe_score = 40
            elif metrics.pe_trailing < 30:
                pe_score = 20
        values.append(pe_score)
        
        pb_score = 0
        if metrics.pb_ratio and metrics.pb_ratio > 0:
            if metrics.pb_ratio < 1:
                pb_score = 100
            elif metrics.pb_ratio < 2:
                pb_score = 80
            elif metrics.pb_ratio < 3:
                pb_score = 60
            elif metrics.pb_ratio < 5:
                pb_score = 40
        values.append(pb_score)
        
        peg_score = 0
        if metrics.peg_ratio and metrics.peg_ratio > 0:
            if metrics.peg_ratio < 1:
                peg_score = 100
            elif metrics.peg_ratio < 1.5:
                peg_score = 80
            elif metrics.peg_ratio < 2:
                peg_score = 60
            elif metrics.peg_ratio < 2.5:
                peg_score = 40
        values.append(peg_score)
        
        ps_score = 0
        if metrics.price_to_sales and metrics.price_to_sales > 0:
            if metrics.price_to_sales < 1:
                ps_score = 100
            elif metrics.price_to_sales < 2:
                ps_score = 80
            elif metrics.price_to_sales < 3:
                ps_score = 60
        values.append(ps_score)
        
        ev_score = 0
        if metrics.ev_to_ebitda and metrics.ev_to_ebitda > 0:
            if metrics.ev_to_ebitda < 5:
                ev_score = 100
            elif metrics.ev_to_ebitda < 8:
                ev_score = 80
            elif metrics.ev_to_ebitda < 10:
                ev_score = 60
        values.append(ev_score)
        
        dy_score = 0
        if metrics.dividend_yield and metrics.dividend_yield > 0:
            if metrics.dividend_yield >= 5:
                dy_score = 100
            elif metrics.dividend_yield >= 3:
                dy_score = 80
            elif metrics.dividend_yield >= 2:
                dy_score = 60
            elif metrics.dividend_yield >= 1:
                dy_score = 40
        values.append(dy_score)
        
        growth_score = 0
        if metrics.growth_rate_5y and metrics.growth_rate_5y > 0:
            if metrics.growth_rate_5y >= 20:
                growth_score = 100
            elif metrics.growth_rate_5y >= 15:
                growth_score = 80
            elif metrics.growth_rate_5y >= 10:
                growth_score = 60
            elif metrics.growth_rate_5y >= 5:
                growth_score = 40
        values.append(growth_score)
        
        profit_score = 0
        if metrics.roe and metrics.roe > 0:
            if metrics.roe >= 20:
                profit_score = 100
            elif metrics.roe >= 15:
                profit_score = 80
            elif metrics.roe >= 10:
                profit_score = 60
            elif metrics.roe >= 5:
                profit_score = 40
        values.append(profit_score)
        
        health_score = 0
        if metrics.debt_to_equity is not None:
            if metrics.debt_to_equity < 0.5:
                health_score = 100
            elif metrics.debt_to_equity < 1:
                health_score = 80
            elif metrics.debt_to_equity < 1.5:
                health_score = 60
            elif metrics.debt_to_equity < 2:
                health_score = 40
        values.append(health_score)
        
        mos_score = 0
        if metrics.margin_of_safety is not None:
            if metrics.margin_of_safety >= 50:
                mos_score = 100
            elif metrics.margin_of_safety >= 30:
                mos_score = 80
            elif metrics.margin_of_safety >= 15:
                mos_score = 60
            elif metrics.margin_of_safety >= 0:
                mos_score = 40
        values.append(mos_score)
        
        return {
            'labels': labels,
            'values': values,
            'dimension_scores': {
                'price_reasonableness': scores['price_reasonableness']['total'],
                'growth': scores['growth']['total'],
                'safety': scores['safety']['total'],
            }
        }

    def analyze_multiple(self, symbols: List[str], 
                         progress_callback: Optional[Callable[[int, int, str], None]] = None) -> List[Dict[str, Any]]:
        results = []
        total = len(symbols)
        
        def analyze_single(symbol: str) -> Optional[Dict[str, Any]]:
            try:
                return self.analyze_stock(symbol)
            except Exception as e:
                print(f"分析股票 {symbol} 失败: {e}")
                return None
        
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            future_to_symbol = {executor.submit(analyze_single, symbol): symbol for symbol in symbols}
            
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
        
        results.sort(key=lambda x: x.total_score if x and x.total_score else 0, reverse=True)
        return results


import pandas as pd
