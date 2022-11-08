import pandas as pd

from yfinance import utils
from yfinance.data import TickerData


class Fundamentals:
    _SCRAPE_URL_ = 'https://finance.yahoo.com/quote'

    def __init__(self, data: TickerData, proxy=None):
        self._data = data
        self.proxy = proxy

        self.ticker_url = "{}/{}".format(self._SCRAPE_URL_, self._data.ticker)

        self._earnings = None
        self._financials = None
        self._shares = None

        self._financials_data = None
        self._fin_data_quote = None
        self._basics_already_scraped = False
        self._already_scraped_financials = False

    @property
    def earnings(self):
        if self._earnings is None:
            self._scrape_earnings(self.proxy)
        return self._earnings

    @property
    def financials(self):
        if self._financials is None:
            self._scrape_financials(self.proxy)
        return self._financials

    @property
    def shares(self):
        if self._shares is None:
            self._scrape_shares(self.proxy)
        return self._shares

    def _scrape_basics(self, proxy):
        if self._basics_already_scraped:
            return
        self._basics_already_scraped = True

        self._financials_data = self._data.get_json_data_stores(self.ticker_url + '/financials', proxy)
        try:
            self._fin_data_quote = self._financials_data['QuoteSummaryStore']
        except KeyError:
            err_msg = "No financials data found, symbol may be delisted"
            print('- %s: %s' % (self._data.ticker, err_msg))
            return None

    def _scrape_earnings(self, proxy):
        self._scrape_basics(proxy)
        # earnings
        self._earnings = {"yearly": pd.DataFrame(), "quarterly": pd.DataFrame()}
        if self._fin_data_quote is None:
            return
        if isinstance(self._fin_data_quote.get('earnings'), dict):
            try:
                earnings = self._fin_data_quote['earnings']['financialsChart']
                earnings['financialCurrency'] = self._fin_data_quote['earnings'].get('financialCurrency', 'USD')
                self._earnings['financialCurrency'] = earnings['financialCurrency']
                df = pd.DataFrame(earnings['yearly']).set_index('date')
                df.columns = utils.camel2title(df.columns)
                df.index.name = 'Year'
                self._earnings['yearly'] = df

                df = pd.DataFrame(earnings['quarterly']).set_index('date')
                df.columns = utils.camel2title(df.columns)
                df.index.name = 'Quarter'
                self._earnings['quarterly'] = df
            except Exception:
                pass

    def _scrape_shares(self, proxy):
        self._scrape_basics(proxy)
        # shares outstanding
        try:
            # keep only years with non None data
            available_shares = [shares_data for shares_data in
                                self._financials_data['QuoteTimeSeriesStore']['timeSeries']['annualBasicAverageShares']
                                if
                                shares_data]
            shares = pd.DataFrame(available_shares)
            shares['Year'] = shares['asOfDate'].agg(lambda x: int(x[:4]))
            shares.set_index('Year', inplace=True)
            shares.drop(columns=['dataId', 'asOfDate',
                                 'periodType', 'currencyCode'], inplace=True)
            shares.rename(
                columns={'reportedValue': "BasicShares"}, inplace=True)
            self._shares = shares
        except Exception:
            pass

    def _scrape_financials(self, proxy):
        self._scrape_basics(proxy)
        if self._already_scraped_financials:
            return
        self._already_scraped_financials = True

        # get fundamentals
        self._financials = {}
        for name in ["income", "balance-sheet", "cash-flow"]:
            self._financials[name] = {"yearly": pd.DataFrame(), "quarterly": pd.DataFrame()}
            annual, qtr = self._create_financials_table(name, proxy)
            if annual is not None:
                self._financials[name]["yearly"] = annual
            if qtr is not None:
                self._financials[name]["quarterly"] = qtr

    def _create_financials_table(self, name, proxy):
        acceptable_names = ["income", "balance-sheet", "cash-flow"]
        if name not in acceptable_names:
            raise ValueError("name '{}' must be one of: {}".format(name, acceptable_names))

        if name == "income":
            # Yahoo stores the 'income' table internally under 'financials' key
            name = "financials"

        data_stores = self._data.get_json_data_stores(self.ticker_url + '/' + name, proxy)
        _stmt_annual = None
        _stmt_qtr = None
        try:
            # Developers note: TTM and template stuff allows for reproducing the nested structure
            # visible on Yahoo website. But more work needed to make it user-friendly! Ideally
            # return a tree data structure instead of Pandas MultiIndex
            # So until this is implemented, just return simple tables
            _stmt_annual = self._data.get_financials_time_series("annual", data_stores, proxy)
            _stmt_qtr = self._data.get_financials_time_series("quarterly", data_stores, proxy)

            # template_ttm_order, template_annual_order, template_order, level_detail = utils.build_template(data_store["FinancialTemplateStore"])
            # TTM_dicts, Annual_dicts = utils.retreive_financial_details(data_store['QuoteTimeSeriesStore'])
            # if name == "balance-sheet":
            #     # Note: balance sheet is the only financial statement with no ttm detail
            #     _stmt_annual = utils.format_annual_financial_statement(level_detail, Annual_dicts, template_annual_order)
            # else:
            #     _stmt_annual = utils.format_annual_financial_statement(level_detail, Annual_dicts, template_annual_order, TTM_dicts, template_ttm_order)

            # Data store doesn't contain quarterly data, so retrieve using different url:
            # _qtr_data = utils.get_financials_time_series(self._ticker.ticker, name, "quarterly", ticker_url, proxy, self.session)
            # _stmt_qtr = utils.format_quarterly_financial_statement(_qtr_data, level_detail, template_order)

        except Exception as e:
            pass

        return _stmt_annual, _stmt_qtr
