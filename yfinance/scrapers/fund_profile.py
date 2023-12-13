import pandas as pd

from yfinance.data import YfData
from yfinance.const import _BASE_URL_
from yfinance.exceptions import YFinanceDataException

_QUOTE_SUMMARY_URL_ = f"{_BASE_URL_}/v10/finance/quoteSummary/"


class FundProfile:
    def __init__(self, data: YfData, symbol: str, proxy=None):
        self._data = data
        self._symbol = symbol
        self.proxy = proxy

        self._description = None
        self._top_holdings = None
        self._equity_holdings = None
        self._bond_holdings = None
        self._bond_ratings = None
        self._sector_weightings = None
        self._profile = None
        self._perfomance = None

    @property
    def description(self) -> str:
        if self._description is None:
            self._fetch_and_parse()
        return self._description

    @property
    def top_holdings(self) -> pd.DataFrame:
        if self._top_holdings is None:
            self._fetch_and_parse()
        return self._top_holdings

    @property
    def equity_holdings(self) -> pd.DataFrame:
        if self._equity_holdings is None:
            self._fetch_and_parse()
        return self._equity_holdings

    @property
    def bond_holdings(self) -> pd.DataFrame:
        if self._bond_holdings is None:
            self._fetch_and_parse()
        return self._bond_holdings

    @property
    def bond_ratings(self) -> pd.DataFrame:
        if self._bond_ratings is None:
            self._fetch_and_parse()
        return self._bond_ratings

    @property
    def sector_weightings(self) -> pd.DataFrame:
        if self._sector_weightings is None:
            self._fetch_and_parse()
        return self._sector_weightings

    @property
    def profile(self) -> pd.DataFrame:
        if self._profile is None:
            self._fetch_and_parse()
        return self._profile

    @property
    def perfomance(self) -> pd.DataFrame:
        if self._perfomance is None:
            self._fetch_and_parse()
        return self._perfomance

    def _fetch(self, proxy):
        modules = ','.join(["quoteType", "summaryProfile", "topHoldings", "fundPerformance", "fundProfile"])
        params_dict = {"modules": modules, "corsDomain": "finance.yahoo.com", "symbol": self._symbol, "formatted": "false"}
        result = self._data.get_raw_json(_QUOTE_SUMMARY_URL_, user_agent_headers=self._data.user_agent_headers, params=params_dict, proxy=proxy)
        return result

    def _fetch_and_parse(self):
        result = self._fetch(self.proxy)
        try:
            data = result["quoteSummary"]["result"][0]
            # check quote type
            try:
                quote_type = data["quoteType"]["quoteType"]
                if quote_type != "ETF":
                    raise YFinanceDataException("Only ETFs are supported.")
            except KeyError:
                raise YFinanceDataException("Failed to parse quote type. No ETF data found.")
            # parse "summaryProfile", "topHoldings", "fundPerformance", "fundProfile",
            self._parse_description(data["summaryProfile"])
            self._parse_top_holdings(data["topHoldings"])
            self._parse_fund_performance(data["fundPerformance"])
            self._parse_fund_profile(data["fundProfile"])
        except (KeyError, IndexError):
            raise YFinanceDataException("Failed to parse fund json data.")

    @staticmethod
    def _parse_raw_values(data):
        if isinstance(data, dict) and "raw" in data:
            return data["raw"]
        return data

    def _parse_description(self, data):
        self._description = data.get("longBusinessSummary", "")

    def _parse_top_holdings(self, data):
        if "maxAge" in data:
            del data["maxAge"]
        df = pd.DataFrame.from_dict(data, orient="index")
        if not df.empty:
            df.columns.name = "Symbol"
            df.rename(columns={df.columns[0]: 'Name'}, inplace=True)
        self._top_holdings = df

    def _parse_fund_performance(self, data):
        if "maxAge" in data:
            del data["maxAge"]
        df = pd.DataFrame.from_dict(data, orient="index")
        if not df.empty:
            df.columns.name = "Performance"
            df.rename(columns={df.columns[0]: 'Value'}, inplace=True)
        self._perfomance = df

    def _parse_fund_profile(self, data):
        if "maxAge" in data:
            del data["maxAge"]
        df = pd.DataFrame.from_dict(data, orient="index")
        if not df.empty:
            df.columns.name = "Profile"
            df.rename(columns={df.columns[0]: 'Value'}, inplace=True)
        self._profile = df

"""
Ticker: IVV
{'quoteSummary': {'error': None,
                  'result': [{'fundPerformance': {'annualTotalReturns': {'returns': [{'annualValue': -0.18157841,
                                                                                      'year': '2022'},
                                                                                     {'annualValue': 0.2875674,
                                                                                      'year': '2021'},
                                                                                     {'annualValue': 0.18398939,
                                                                                      'year': '2020'},
                                                                                     {'annualValue': 0.3125169,
                                                                                      'year': '2019'},
                                                                                     {'annualValue': -0.044671997,
                                                                                      'year': '2018'},
                                                                                     {'annualValue': 0.217602,
                                                                                      'year': '2017'},
                                                                                     {'annualValue': 0.1215578,
                                                                                      'year': '2016'},
                                                                                     {'annualValue': 0.0129512,
                                                                                      'year': '2015'},
                                                                                     {'annualValue': 0.13564551,
                                                                                      'year': '2014'},
                                                                                     {'annualValue': 0.3229967,
                                                                                      'year': '2013'},
                                                                                     {'annualValue': 0.1605743,
                                                                                      'year': '2012'},
                                                                                     {'annualValue': 0.018587299,
                                                                                      'year': '2011'},
                                                                                     {'annualValue': 0.1508797,
                                                                                      'year': '2010'},
                                                                                     {'annualValue': 0.2642158,
                                                                                      'year': '2009'},
                                                                                     {'annualValue': -0.36929902,
                                                                                      'year': '2008'},
                                                                                     {'annualValue': 0.0531544,
                                                                                      'year': '2007'},
                                                                                     {'annualValue': 0.1594825,
                                                                                      'year': '2006'},
                                                                                     {'annualValue': 0.047479,
                                                                                      'year': '2005'},
                                                                                     {'annualValue': 0.1088997,
                                                                                      'year': '2004'},
                                                                                     {'annualValue': 0.2819183,
                                                                                      'year': '2003'},
                                                                                     {'annualValue': -0.2166225,
                                                                                      'year': '2002'},
                                                                                     {'annualValue': -0.119517,
                                                                                      'year': '2001'}],
                                                                         'returnsCat': [{'annualValue': -0.0063223,
                                                                                         'year': '2015'},
                                                                                        {'annualValue': 0.1178567,
                                                                                         'year': '2014'},
                                                                                        {'annualValue': 0.3143698,
                                                                                         'year': '2013'},
                                                                                        {'annualValue': 0.1421203,
                                                                                         'year': '2012'},
                                                                                        {'annualValue': 0.016827399,
                                                                                         'year': '2011'},
                                                                                        {'annualValue': 0.17790991,
                                                                                         'year': '2010'},
                                                                                        {'annualValue': 0.322644,
                                                                                         'year': '2009'},
                                                                                        {'annualValue': -0.38047272,
                                                                                         'year': '2008'},
                                                                                        {'annualValue': 0.044018798,
                                                                                         'year': '2007'},
                                                                                        {'annualValue': 0.1554867,
                                                                                         'year': '2006'},
                                                                                        {'annualValue': 0.0468473,
                                                                                         'year': '2005'},
                                                                                        {'annualValue': 0.1255366,
                                                                                         'year': '2004'},
                                                                                        {'annualValue': 0.2909224,
                                                                                         'year': '2003'},
                                                                                        {'annualValue': -0.17842369,
                                                                                         'year': '2002'},
                                                                                        {'annualValue': -0.057511102,
                                                                                         'year': '2001'},
                                                                                        {'annualValue': -0.052431203,
                                                                                         'year': '2000'},
                                                                                        {'annualValue': 0.1598289,
                                                                                         'year': '1999'},
                                                                                        {'annualValue': 0.2835127,
                                                                                         'year': '1998'},
                                                                                        {'annualValue': 0.33063492,
                                                                                         'year': '1997'},
                                                                                        {'annualValue': 0.22740379,
                                                                                         'year': '1996'},
                                                                                        {'annualValue': 0.3722677,
                                                                                         'year': '1995'},
                                                                                        {'annualValue': 0.011552599,
                                                                                         'year': '1994'}]},
                                                  'maxAge': 1,
                                                  'pastQuarterlyReturns': {'returns': []},
                                                  'performanceOverview': {'asOfDate': 1702252800,
                                                                          'oneYearTotalReturn': 0.1942313,
                                                                          'threeYearTotalReturn': 0.0973332,
                                                                          'ytdReturnPct': 0.22255899},
                                                  'performanceOverviewCat': {'fiveYrAvgReturnPct': 0.1576,
                                                                             'ytdReturnPct': 0.075},
                                                  'riskOverviewStatistics': {'riskStatistics': [{'alpha': -0.03,
                                                                                                 'beta': 1.0,
                                                                                                 'meanAnnualReturn': 1.13,
                                                                                                 'rSquared': 100.0,
                                                                                                 'sharpeRatio': 0.61,
                                                                                                 'stdDev': 19.04,
                                                                                                 'treynorRatio': 10.52,
                                                                                                 'year': '5y'},
                                                                                                {'alpha': -0.03,
                                                                                                 'beta': 1.0,
                                                                                                 'meanAnnualReturn': 0.9,
                                                                                                 'rSquared': 100.0,
                                                                                                 'sharpeRatio': 0.48,
                                                                                                 'stdDev': 17.49,
                                                                                                 'treynorRatio': 7.37,
                                                                                                 'year': '3y'},
                                                                                                {'alpha': -0.04,
                                                                                                 'beta': 1.0,
                                                                                                 'meanAnnualReturn': 1.03,
                                                                                                 'rSquared': 100.0,
                                                                                                 'sharpeRatio': 0.73,
                                                                                                 'stdDev': 15.16,
                                                                                                 'treynorRatio': 10.48,
                                                                                                 'year': '10y'}]},
                                                  'riskOverviewStatisticsCat': {'riskStatisticsCat': [{'alpha': -0.48,
                                                                                                       'beta': 1.0,
                                                                                                       'meanAnnualReturn': 1.28,
                                                                                                       'rSquared': 95.34,
                                                                                                       'sharpeRatio': 1.34,
                                                                                                       'stdDev': 11.38,
                                                                                                       'treynorRatio': 15.73,
                                                                                                       'year': '5y'},
                                                                                                      {'alpha': -0.65,
                                                                                                       'beta': 0.98,
                                                                                                       'meanAnnualReturn': 0.86,
                                                                                                       'rSquared': 94.06,
                                                                                                       'sharpeRatio': 0.94,
                                                                                                       'stdDev': 10.9,
                                                                                                       'treynorRatio': 10.38,
                                                                                                       'year': '3y'},
                                                                                                      {'alpha': 0.15,
                                                                                                       'beta': 1.0,
                                                                                                       'meanAnnualReturn': 0.69,
                                                                                                       'rSquared': 96.99,
                                                                                                       'sharpeRatio': 0.48,
                                                                                                       'stdDev': 15.42,
                                                                                                       'treynorRatio': 6.54,
                                                                                                       'year': '10y'}]},
                                                  'trailingReturns': {'asOfDate': 1702252800,
                                                                      'fiveYear': 0.1377521,
                                                                      'lastBearMkt': 0.0,
                                                                      'lastBullMkt': 0.0,
                                                                      'oneMonth': 0.048523698,
                                                                      'oneYear': 0.1942313,
                                                                      'tenYear': 0.1205057,
                                                                      'threeMonth': 0.0341672,
                                                                      'threeYear': 0.0973332,
                                                                      'ytd': 0.22255899},
                                                  'trailingReturnsCat': {'fiveYear': 0.1576,
                                                                         'lastBearMkt': 0.0,
                                                                         'lastBullMkt': 0.0,
                                                                         'oneMonth': -0.0025,
                                                                         'oneYear': 0.1345,
                                                                         'tenYear': 0.0733,
                                                                         'threeMonth': 0.032,
                                                                         'threeYear': 0.1014,
                                                                         'ytd': 0.075},
                                                  'trailingReturnsNav': {'fiveYear': 0.1377521,
                                                                         'oneMonth': 0.048523698,
                                                                         'oneYear': 0.1942313,
                                                                         'tenYear': 0.1205057,
                                                                         'threeMonth': 0.0341672,
                                                                         'threeYear': 0.0973332,
                                                                         'ytd': 0.22255899}},
                              'fundProfile': {'categoryName': 'Large Blend',
                                              'family': 'iShares',
                                              'feesExpensesInvestment': {'annualHoldingsTurnover': 0.03,
                                                                         'annualReportExpenseRatio': 0.00029999999,
                                                                         'projectionValues': {},
                                                                         'totalNetAssets': 237535.88},
                                              'feesExpensesInvestmentCat': {'annualHoldingsTurnover': 50.76,
                                                                            'annualReportExpenseRatio': 0.0036000002,
                                                                            'projectionValuesCat': {},
                                                                            'totalNetAssets': 237535.88},
                                              'legalType': 'Exchange Traded '
                                                           'Fund',
                                              'maxAge': 1,
                                              'styleBoxUrl': 'https://s.yimg.com/lq/i/fi/3_0stylelargeeq2.gif'},
                              'quoteType': {'exchange': 'PCX',
                                            'firstTradeDateEpochUtc': 958743000,
                                            'gmtOffSetMilliseconds': -18000000,
                                            'longName': 'iShares Core S&P 500 '
                                                        'ETF',
                                            'maxAge': 1,
                                            'quoteType': 'ETF',
                                            'shortName': 'iShares Core S&P 500 '
                                                         'ETF',
                                            'symbol': 'IVV',
                                            'timeZoneFullName': 'America/New_York',
                                            'timeZoneShortName': 'EST',
                                            'underlyingSymbol': 'IVV',
                                            'uuid': '17732e4a-0401-33ba-977d-a9848ab99a1e'},
                              'summaryProfile': {'companyOfficers': [],
                                                 'longBusinessSummary': 'The '
                                                                        'index '
                                                                        'measures '
                                                                        'the '
                                                                        'performance '
                                                                        'of '
                                                                        'the '
                                                                        'large-capitalization '
                                                                        'sector '
                                                                        'of '
                                                                        'the '
                                                                        'U.S. '
                                                                        'equity '
                                                                        'market, '
                                                                        'as '
                                                                        'determined '
                                                                        'by '
                                                                        'SPDJI. '
                                                                        'The '
                                                                        'fund '
                                                                        'generally '
                                                                        'will '
                                                                        'invest '
                                                                        'at '
                                                                        'least '
                                                                        '80% '
                                                                        'of '
                                                                        'its '
                                                                        'assets '
                                                                        'in '
                                                                        'the '
                                                                        'component '
                                                                        'securities '
                                                                        'of '
                                                                        'its '
                                                                        'index '
                                                                        'and '
                                                                        'in '
                                                                        'investments '
                                                                        'that '
                                                                        'have '
                                                                        'economic '
                                                                        'characteristics '
                                                                        'that '
                                                                        'are '
                                                                        'substantially '
                                                                        'identical '
                                                                        'to '
                                                                        'the '
                                                                        'component '
                                                                        'securities '
                                                                        'of '
                                                                        'its '
                                                                        'index '
                                                                        'and '
                                                                        'may '
                                                                        'invest '
                                                                        'up to '
                                                                        '20% '
                                                                        'of '
                                                                        'its '
                                                                        'assets '
                                                                        'in '
                                                                        'certain '
                                                                        'futures, '
                                                                        'options '
                                                                        'and '
                                                                        'swap '
                                                                        'contracts, '
                                                                        'cash '
                                                                        'and '
                                                                        'cash '
                                                                        'equivalents.',
                                                 'maxAge': 86400,
                                                 'phone': '1-800-474-2737'},
                              'topHoldings': {'bondHoldings': {},
                                              'bondPosition': 0.0,
                                              'bondRatings': [{'us_government': 0.0}],
                                              'equityHoldings': {'priceToBook': 0.24986,
                                                                 'priceToCashflow': 0.06365,
                                                                 'priceToEarnings': 0.04416,
                                                                 'priceToSales': 0.38772},
                                              'holdings': [{'holdingName': 'Microsoft '
                                                                           'Corp',
                                                            'holdingPercent': 0.0732494,
                                                            'symbol': 'MSFT'},
                                                           {'holdingName': 'Apple '
                                                                           'Inc',
                                                            'holdingPercent': 0.072633296,
                                                            'symbol': 'AAPL'},
                                                           {'holdingName': 'Amazon.com '
                                                                           'Inc',
                                                            'holdingPercent': 0.034512803,
                                                            'symbol': 'AMZN'},
                                                           {'holdingName': 'NVIDIA '
                                                                           'Corp',
                                                            'holdingPercent': 0.0300576,
                                                            'symbol': 'NVDA'},
                                                           {'holdingName': 'Alphabet '
                                                                           'Inc '
                                                                           'Class '
                                                                           'A',
                                                            'holdingPercent': 0.0204588,
                                                            'symbol': 'GOOGL'},
                                                           {'holdingName': 'Meta '
                                                                           'Platforms '
                                                                           'Inc '
                                                                           'Class '
                                                                           'A',
                                                            'holdingPercent': 0.018918999,
                                                            'symbol': 'META'},
                                                           {'holdingName': 'Alphabet '
                                                                           'Inc '
                                                                           'Class '
                                                                           'C',
                                                            'holdingPercent': 0.0175856,
                                                            'symbol': 'GOOG'},
                                                           {'holdingName': 'Tesla '
                                                                           'Inc',
                                                            'holdingPercent': 0.017249301,
                                                            'symbol': 'TSLA'},
                                                           {'holdingName': 'Berkshire '
                                                                           'Hathaway '
                                                                           'Inc '
                                                                           'Class '
                                                                           'B',
                                                            'holdingPercent': 0.0170823,
                                                            'symbol': 'BRK-B'},
                                                           {'holdingName': 'UnitedHealth '
                                                                           'Group '
                                                                           'Inc',
                                                            'holdingPercent': 0.0133278,
                                                            'symbol': 'UNH'}],
                                              'maxAge': 1,
                                              'sectorWeightings': [{'realestate': 0.024300002},
                                                                   {'consumer_cyclical': 0.1091},
                                                                   {'basic_materials': 0.022},
                                                                   {'consumer_defensive': 0.0627},
                                                                   {'technology': 0.29709998},
                                                                   {'communication_services': 0.0861},
                                                                   {'financial_services': 0.1249},
                                                                   {'utilities': 0.024},
                                                                   {'industrials': 0.081599995},
                                                                   {'energy': 0.040999997},
                                                                   {'healthcare': 0.1272}],
                                              'stockPosition': 0.9969}}]}}



Ticker: ILTB
{'quoteSummary': {'error': None,
                  'result': [{'fundPerformance': {'annualTotalReturns': {'returns': [{'annualValue': -0.2663794,
                                                                                      'year': '2022'},
                                                                                     {'annualValue': -0.026731702,
                                                                                      'year': '2021'},
                                                                                     {'annualValue': 0.16101351,
                                                                                      'year': '2020'},
                                                                                     {'annualValue': 0.19610491,
                                                                                      'year': '2019'},
                                                                                     {'annualValue': -0.0509226,
                                                                                      'year': '2018'},
                                                                                     {'annualValue': 0.1124594,
                                                                                      'year': '2017'},
                                                                                     {'annualValue': 0.0782001,
                                                                                      'year': '2016'},
                                                                                     {'annualValue': -0.042186,
                                                                                      'year': '2015'},
                                                                                     {'annualValue': 0.2028438,
                                                                                      'year': '2014'},
                                                                                     {'annualValue': -0.0989118,
                                                                                      'year': '2013'},
                                                                                     {'annualValue': 0.0872509,
                                                                                      'year': '2012'},
                                                                                     {'annualValue': 0.2148577,
                                                                                      'year': '2011'},
                                                                                     {'annualValue': 0.0912168,
                                                                                      'year': '2010'}],
                                                                         'returnsCat': [{'annualValue': -0.0150185,
                                                                                         'year': '2015'},
                                                                                        {'annualValue': 0.18469709,
                                                                                         'year': '2014'},
                                                                                        {'annualValue': -0.0745889,
                                                                                         'year': '2013'},
                                                                                        {'annualValue': 0.1091477,
                                                                                         'year': '2012'},
                                                                                        {'annualValue': 0.1873323,
                                                                                         'year': '2011'},
                                                                                        {'annualValue': 0.1003784,
                                                                                         'year': '2010'},
                                                                                        {'annualValue': 0.0697499,
                                                                                         'year': '2009'},
                                                                                        {'annualValue': 0.0417112,
                                                                                         'year': '2008'},
                                                                                        {'annualValue': 0.0373175,
                                                                                         'year': '2007'},
                                                                                        {'annualValue': 0.0396022,
                                                                                         'year': '2006'},
                                                                                        {'annualValue': 0.0100174,
                                                                                         'year': '2005'},
                                                                                        {'annualValue': 0.059131097,
                                                                                         'year': '2004'}]},
                                                  'maxAge': 1,
                                                  'pastQuarterlyReturns': {'returns': []},
                                                  'performanceOverview': {'asOfDate': 1702252800,
                                                                          'oneYearTotalReturn': -0.0193081,
                                                                          'threeYearTotalReturn': -0.096361704,
                                                                          'ytdReturnPct': 0.0297735},
                                                  'performanceOverviewCat': {'fiveYrAvgReturnPct': 0.0636,
                                                                             'ytdReturnPct': 0.1439},
                                                  'riskOverviewStatistics': {'riskStatistics': [{'alpha': 1.87,
                                                                                                 'beta': 2.18,
                                                                                                 'meanAnnualReturn': 0.13,
                                                                                                 'rSquared': 92.94,
                                                                                                 'sharpeRatio': -0.03,
                                                                                                 'stdDev': 13.51,
                                                                                                 'treynorRatio': -0.62,
                                                                                                 'year': '5y'},
                                                                                                {'alpha': 1.58,
                                                                                                 'beta': 2.08,
                                                                                                 'meanAnnualReturn': -0.83,
                                                                                                 'rSquared': 94.3,
                                                                                                 'sharpeRatio': -0.85,
                                                                                                 'stdDev': 14.55,
                                                                                                 'treynorRatio': -6.16,
                                                                                                 'year': '3y'},
                                                                                                {'alpha': 1.41,
                                                                                                 'beta': 2.24,
                                                                                                 'meanAnnualReturn': 0.26,
                                                                                                 'rSquared': 91.86,
                                                                                                 'sharpeRatio': 0.17,
                                                                                                 'stdDev': 10.8,
                                                                                                 'treynorRatio': 0.56,
                                                                                                 'year': '10y'}]},
                                                  'riskOverviewStatisticsCat': {'riskStatisticsCat': [{'alpha': -1.24,
                                                                                                       'beta': 2.55,
                                                                                                       'meanAnnualReturn': 0.54,
                                                                                                       'rSquared': 85.61,
                                                                                                       'sharpeRatio': 0.91,
                                                                                                       'stdDev': 7.28,
                                                                                                       'treynorRatio': 2.61,
                                                                                                       'year': '5y'},
                                                                                                      {'alpha': 0.16,
                                                                                                       'beta': 2.4,
                                                                                                       'meanAnnualReturn': 0.8,
                                                                                                       'rSquared': 87.35,
                                                                                                       'sharpeRatio': 1.49,
                                                                                                       'stdDev': 6.68,
                                                                                                       'treynorRatio': 4.25,
                                                                                                       'year': '3y'},
                                                                                                      {'year': '10y'}]},
                                                  'trailingReturns': {'asOfDate': 1702252800,
                                                                      'fiveYear': 0.0068278997,
                                                                      'lastBearMkt': 0.0,
                                                                      'lastBullMkt': 0.0,
                                                                      'oneMonth': 0.0740724,
                                                                      'oneYear': -0.0193081,
                                                                      'tenYear': 0.0292781,
                                                                      'threeMonth': 0.033599302,
                                                                      'threeYear': -0.096361704,
                                                                      'ytd': 0.0297735},
                                                  'trailingReturnsCat': {'fiveYear': 0.0636,
                                                                         'lastBearMkt': 0.0,
                                                                         'lastBullMkt': 0.0,
                                                                         'oneMonth': -0.0076,
                                                                         'oneYear': 0.14,
                                                                         'tenYear': 0.0,
                                                                         'threeMonth': 0.0095,
                                                                         'threeYear': 0.097600006,
                                                                         'ytd': 0.1439},
                                                  'trailingReturnsNav': {'fiveYear': 0.0068278997,
                                                                         'oneMonth': 0.0740724,
                                                                         'oneYear': -0.0193081,
                                                                         'tenYear': 0.0292781,
                                                                         'threeMonth': 0.033599302,
                                                                         'threeYear': -0.096361704,
                                                                         'ytd': 0.0297735}},
                              'fundProfile': {'categoryName': 'Long-Term Bond',
                                              'family': 'iShares',
                                              'feesExpensesInvestment': {'annualHoldingsTurnover': 0.1,
                                                                         'annualReportExpenseRatio': 0.00059999997,
                                                                         'projectionValues': {},
                                                                         'totalNetAssets': 0.0},
                                              'feesExpensesInvestmentCat': {'annualHoldingsTurnover': 14.5,
                                                                            'annualReportExpenseRatio': 0.0017,
                                                                            'projectionValuesCat': {},
                                                                            'totalNetAssets': 0.0},
                                              'legalType': 'Exchange Traded '
                                                           'Fund',
                                              'maxAge': 1,
                                              'styleBoxUrl': 'https://s.yimg.com/lq/i/fi/3_0stylelargeeq.gif'},
                              'quoteType': {'exchange': 'PCX',
                                            'firstTradeDateEpochUtc': 1260369000,
                                            'gmtOffSetMilliseconds': -18000000,
                                            'longName': 'iShares Core 10+ Year '
                                                        'USD Bond ETF',
                                            'maxAge': 1,
                                            'quoteType': 'ETF',
                                            'shortName': 'iShares Core 10  '
                                                         'Year USD Bond ',
                                            'symbol': 'ILTB',
                                            'timeZoneFullName': 'America/New_York',
                                            'timeZoneShortName': 'EST',
                                            'underlyingSymbol': 'ILTB',
                                            'uuid': '0d410db8-3e43-3ded-840c-ed89a65fe0ba'},
                              'summaryProfile': {'companyOfficers': [],
                                                 'longBusinessSummary': 'The '
                                                                        'underlying '
                                                                        'index '
                                                                        'measures '
                                                                        'the '
                                                                        'performance '
                                                                        'of '
                                                                        'U.S. '
                                                                        'dollar-denominated '
                                                                        'taxable '
                                                                        'bonds '
                                                                        'that '
                                                                        'are '
                                                                        'rated '
                                                                        'either '
                                                                        'investment-grade '
                                                                        'or '
                                                                        'high '
                                                                        'yield '
                                                                        'with '
                                                                        'remaining '
                                                                        'maturities '
                                                                        'greater '
                                                                        'than '
                                                                        'ten '
                                                                        'years. '
                                                                        'The '
                                                                        'fund '
                                                                        'will '
                                                                        'invest '
                                                                        'at '
                                                                        'least '
                                                                        '80% '
                                                                        'of '
                                                                        'its '
                                                                        'assets '
                                                                        'in '
                                                                        'the '
                                                                        'component '
                                                                        'securities '
                                                                        'of '
                                                                        'the '
                                                                        'underlying '
                                                                        'index '
                                                                        'and '
                                                                        'TBAs '
                                                                        'that '
                                                                        'have '
                                                                        'economic '
                                                                        'characteristics '
                                                                        'that '
                                                                        'are '
                                                                        'substantially '
                                                                        'identical '
                                                                        'to '
                                                                        'the '
                                                                        'economic '
                                                                        'characteristics '
                                                                        'of '
                                                                        'the '
                                                                        'component '
                                                                        'securities '
                                                                        'of '
                                                                        'the '
                                                                        'underlying '
                                                                        'index.',
                                                 'maxAge': 86400,
                                                 'phone': '1-800-474-2737'},
                              'topHoldings': {'bondHoldings': {'duration': 4.07,
                                                               'maturity': 81.91},
                                              'bondPosition': 0.9947,
                                              'bondRatings': [{'bb': 0.020499999},
                                                              {'aa': 0.4746},
                                                              {'aaa': 0.0176},
                                                              {'a': 0.2189},
                                                              {'other': 0.00029999999},
                                                              {'b': 0.0128},
                                                              {'bbb': 0.2482},
                                                              {'below_b': 0.0070999996},
                                                              {'us_government': 0.48630002}],
                                              'equityHoldings': {'priceToBook': 0.0,
                                                                 'priceToCashflow': 0.0,
                                                                 'priceToEarnings': 0.0,
                                                                 'priceToSales': 0.0},
                                              'holdings': [],
                                              'maxAge': 1,
                                              'sectorWeightings': [],
                                              'stockPosition': 0.0}}]}}
"""