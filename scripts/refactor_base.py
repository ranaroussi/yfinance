"""Refactor TickerBase lazy attributes into _lazy dict."""
from pathlib import Path

p = Path(__file__).resolve().parent.parent / "yfinance/base.py"
text = p.read_text(encoding="utf-8")

old_init = """        self.ticker = ticker.upper()
        self.session = session or new_session()
        self._tz = None

        self._isin = None
        self._news = []
        self._shares = None

        self._earnings_dates = {}

        self._earnings = None
        self._financials = None"""

new_init = """        self.ticker = ticker.upper()
        self.session = session or new_session()
        self._lazy = {
            'tz': None, 'isin': None, 'news': [], 'shares': None,
            'earnings_dates': {}, 'earnings': None, 'financials': None,
            'price_history': None, 'funds_data': None, 'fast_info': None,
            'message_handler': None, 'ws': None,
        }"""

text = text.replace(old_init, new_init)
text = text.replace("        # self._price_history = PriceHistory(self._data, self.ticker)\n        self._price_history = None  # lazy-load", "")
text = text.replace("        self._funds_data = None\n\n        self._fast_info = None\n\n        self._message_handler = None\n        self.ws = None", "")

replacements = [
    ('self._price_history', "self._lazy['price_history']"),
    ('self._funds_data', "self._lazy['funds_data']"),
    ('self._fast_info', "self._lazy['fast_info']"),
    ('self._message_handler', "self._lazy['message_handler']"),
    ('self.ws', "self._lazy['ws']"),
    ('self._earnings_dates', "self._lazy['earnings_dates']"),
    ('self._earnings', "self._lazy['earnings']"),
    ('self._financials', "self._lazy['financials']"),
    ('self._shares', "self._lazy['shares']"),
    ('self._news', "self._lazy['news']"),
    ('self._isin', "self._lazy['isin']"),
    ('self._tz', "self._lazy['tz']"),
]
for old, new in replacements:
    text = text.replace(old, new)

# get_shares_full refactor
old_gsf = """    @utils.log_indent_decorator
    def get_shares_full(self, start=None, end=None):
        logger = utils.get_yf_logger()


        # Process dates
        tz = self._get_ticker_tz(timeout=10)
        dt_now = pd.Timestamp.now('UTC').tz_convert(tz)
        if start is not None:
            start = utils._parse_user_dt(start, tz)
        if end is not None:
            end = utils._parse_user_dt(end, tz)
        if end is None:
            end = dt_now
        if start is None:
            start = end - pd.Timedelta(days=548)  # 18 months
        if start >= end:
            logger.error("Start date must be before end")
            return None
        start = start.floor("D")
        end = end.ceil("D")

        # Fetch
        ts_url_base = f"https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{self.ticker}?symbol={self.ticker}"
        shares_url = f"{ts_url_base}&period1={int(start.timestamp())}&period2={int(end.timestamp())}"
        try:
            json_data = self._data.cache_get(url=shares_url)
            json_data = json_data.json()
        except (_json.JSONDecodeError, requests.exceptions.RequestException):
            if not YfConfig.debug.hide_exceptions:
                raise
            logger.error(f"{self.ticker}: Yahoo web request for share count failed")
            return None
        try:
            fail = json_data["finance"]["error"]["code"] == "Bad Request"
        except KeyError:
            fail = False
        if fail:
            if not YfConfig.debug.hide_exceptions:
                raise requests.exceptions.HTTPError("Yahoo web request for share count returned 'Bad Request'")
            logger.error(f"{self.ticker}: Yahoo web request for share count failed")
            return None

        shares_data = json_data["timeseries"]["result"]
        if "shares_out" not in shares_data[0]:
            return None
        try:
            df = pd.Series(shares_data[0]["shares_out"], index=pd.to_datetime(shares_data[0]["timestamp"], unit="s"))
        except Exception as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            logger.error(f"{self.ticker}: Failed to parse shares count data: {e}")
            return None

        df.index = df.index.tz_localize(tz)
        df = df.sort_index()
        return df"""

new_gsf = """    def _parse_shares_date_range(self, start, end, logger):
        tz = self._get_ticker_tz(timeout=10)
        dt_now = pd.Timestamp.now('UTC').tz_convert(tz)
        if start is not None:
            start = utils._parse_user_dt(start, tz)
        if end is not None:
            end = utils._parse_user_dt(end, tz)
        if end is None:
            end = dt_now
        if start is None:
            start = end - pd.Timedelta(days=548)
        if start >= end:
            logger.error("Start date must be before end")
            return None, None
        return start.floor("D"), end.ceil("D")

    def _fetch_shares_json(self, start, end, logger):
        ts_url_base = (
            f"https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/"
            f"{self.ticker}?symbol={self.ticker}"
        )
        shares_url = f"{ts_url_base}&period1={int(start.timestamp())}&period2={int(end.timestamp())}"
        try:
            return self._data.cache_get(url=shares_url).json()
        except (_json.JSONDecodeError, requests.exceptions.RequestException):
            if not YfConfig.debug.hide_exceptions:
                raise
            logger.error(f"{self.ticker}: Yahoo web request for share count failed")
            return None

    def _parse_shares_series(self, json_data, logger):
        try:
            fail = json_data["finance"]["error"]["code"] == "Bad Request"
        except KeyError:
            fail = False
        if fail:
            if not YfConfig.debug.hide_exceptions:
                raise requests.exceptions.HTTPError("Yahoo web request for share count returned 'Bad Request'")
            logger.error(f"{self.ticker}: Yahoo web request for share count failed")
            return None
        shares_data = json_data["timeseries"]["result"]
        if "shares_out" not in shares_data[0]:
            return None
        try:
            return pd.Series(
                shares_data[0]["shares_out"],
                index=pd.to_datetime(shares_data[0]["timestamp"], unit="s"),
            )
        except Exception as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            logger.error(f"{self.ticker}: Failed to parse shares count data: {e}")
            return None

    @utils.log_indent_decorator
    def get_shares_full(self, start=None, end=None):
        logger = utils.get_yf_logger()
        start, end = self._parse_shares_date_range(start, end, logger)
        if start is None:
            return None
        json_data = self._fetch_shares_json(start, end, logger)
        if json_data is None:
            return None
        df = self._parse_shares_series(json_data, logger)
        if df is None:
            return None
        tz = self._get_ticker_tz(timeout=10)
        df.index = df.index.tz_localize(tz)
        return df.sort_index()"""

text = text.replace(old_gsf, new_gsf)
p.write_text(text, encoding="utf-8")
print("base.py updated")
