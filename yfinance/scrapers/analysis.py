import pandas as pd

from yfinance import utils
from yfinance.data import TickerData


class Analysis:

    def __init__(self, data: TickerData, proxy=None):
        self._data = data
        self.proxy = proxy

        self._earnings_trend = None
        self._analyst_trend_details = None
        self._analyst_price_target = None
        self._rev_est = None
        self._eps_est = None
        self._already_scraped = False

    @property
    def earnings_trend(self) -> pd.DataFrame:
        if self._earnings_trend is None:
            self._scrape(self.proxy)
        return self._earnings_trend

    @property
    def analyst_trend_details(self) -> pd.DataFrame:
        if self._analyst_trend_details is None:
            self._scrape(self.proxy)
        return self._analyst_trend_details

    @property
    def analyst_price_target(self) -> pd.DataFrame:
        if self._analyst_price_target is None:
            self._scrape(self.proxy)
        return self._analyst_price_target

    @property
    def rev_est(self) -> pd.DataFrame:
        if self._rev_est is None:
            self._scrape(self.proxy)
        return self._rev_est

    @property
    def eps_est(self) -> pd.DataFrame:
        if self._eps_est is None:
            self._scrape(self.proxy)
        return self._eps_est

    def _scrape(self, proxy):
        if self._already_scraped:
            return
        self._already_scraped = True

        # Analysis Data/Analyst Forecasts
        analysis_data = self._data.get_json_data_stores("analysis", proxy=proxy)
        try:
            analysis_data = analysis_data['QuoteSummaryStore']
        except KeyError as e:
            err_msg = "No analysis data found, symbol may be delisted"
            print('- %s: %s' % (self._data.ticker, err_msg))
            return

        if isinstance(analysis_data.get('earningsTrend'), dict):
            try:
                analysis = pd.DataFrame(analysis_data['earningsTrend']['trend'])
                analysis['endDate'] = pd.to_datetime(analysis['endDate'])
                analysis.set_index('period', inplace=True)
                analysis.index = analysis.index.str.upper()
                analysis.index.name = 'Period'
                analysis.columns = utils.camel2title(analysis.columns)

                dict_cols = []

                for idx, row in analysis.iterrows():
                    for colname, colval in row.items():
                        if isinstance(colval, dict):
                            dict_cols.append(colname)
                            for k, v in colval.items():
                                new_colname = colname + ' ' + \
                                              utils.camel2title([k])[0]
                                analysis.loc[idx, new_colname] = v

                self._earnings_trend = analysis[[
                    c for c in analysis.columns if c not in dict_cols]]
            except Exception:
                pass

        try:
            self._analyst_trend_details = pd.DataFrame(analysis_data['recommendationTrend']['trend'])
        except Exception as e:
            self._analyst_trend_details = None
        try:
            self._analyst_price_target = pd.DataFrame(analysis_data['financialData'], index=[0])[
                ['targetLowPrice', 'currentPrice', 'targetMeanPrice', 'targetHighPrice', 'numberOfAnalystOpinions']].T
        except Exception as e:
            self._analyst_price_target = None
        earnings_estimate = []
        revenue_estimate = []
        if self._analyst_trend_details is not None :
            for key in analysis_data['earningsTrend']['trend']:
                try:
                    earnings_dict = key['earningsEstimate']
                    earnings_dict['period'] = key['period']
                    earnings_dict['endDate'] = key['endDate']
                    earnings_estimate.append(earnings_dict)

                    revenue_dict = key['revenueEstimate']
                    revenue_dict['period'] = key['period']
                    revenue_dict['endDate'] = key['endDate']
                    revenue_estimate.append(revenue_dict)
                except Exception as e:
                    pass
            self._rev_est = pd.DataFrame(revenue_estimate)
            self._eps_est = pd.DataFrame(earnings_estimate)
        else:
            self._rev_est = pd.DataFrame()
            self._eps_est = pd.DataFrame()
