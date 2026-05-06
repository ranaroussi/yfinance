import datetime
import math

from ..response import MockResponse

# Tickers that always return a chart error response (no data found).
# is_error_ticker() is used by the router so both exact matches and substring
# patterns are defined here rather than scattered across router.py.
_ERROR_TICKERS = frozenset({"ATVI"})
_ERROR_TICKER_SUBSTRINGS = frozenset({"DOES_NOT_EXIST"})

# Per-ticker dividend schedules for tests that assert exact dividend dates.
# Each entry is a list of (date, amount) tuples in chronological order.
_TICKER_FIXED_DIVIDENDS = {
    "BHP.AX": [
        (datetime.date(2022, 2, 24), 1.00),
        (datetime.date(2022, 9, 1),  1.00),
    ],
    "IMP.JO": [
        (datetime.date(2022, 3, 16), 1.00),
        (datetime.date(2022, 9, 21), 1.00),
    ],
    "BP.L": [
        (datetime.date(2022, 2, 17), 1.00),
        (datetime.date(2022, 5, 12), 1.00),
        (datetime.date(2022, 8, 11), 1.00),
        (datetime.date(2022, 11, 10), 1.00),
    ],
    "INTC": [
        (datetime.date(2022, 2, 4),  1.00),
        (datetime.date(2022, 5, 5),  1.00),
        (datetime.date(2022, 8, 4),  1.00),
        (datetime.date(2022, 11, 4), 1.00),
    ],
}


def is_error_ticker(ticker: str) -> bool:
    u = ticker.upper()
    return u in _ERROR_TICKERS or any(s in u for s in _ERROR_TICKER_SUBSTRINGS)


# (exchangeName, exchangeTimezoneName, currency, instrumentType)
_TICKER_META = {
    "AAPL":     ("NMS",  "America/New_York",    "USD",  "EQUITY"),
    "GOOGL":    ("NMS",  "America/New_York",    "USD",  "EQUITY"),
    "MSFT":     ("NMS",  "America/New_York",    "USD",  "EQUITY"),
    "INTC":     ("NMS",  "America/New_York",    "USD",  "EQUITY"),
    "IBM":      ("NYSE", "America/New_York",    "USD",  "EQUITY"),
    "AMZN":     ("NMS",  "America/New_York",    "USD",  "EQUITY"),
    "0Q3.DE":   ("GER",  "Europe/Berlin",       "EUR",  "EQUITY"),
    "BP.L":     ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "PNL.L":    ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "AET.L":    ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "ABDP.L":   ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "BBIL.L":   ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "ADIG.L":   ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "BHP.AX":   ("ASX",  "Australia/Sydney",    "AUD",  "EQUITY"),
    "IMP.JO":   ("JSE",  "Africa/Johannesburg", "ZAc",  "EQUITY"),
    "BHG.JO":   ("JSE",  "Africa/Johannesburg", "ZAc",  "EQUITY"),
    "SSW.JO":   ("JSE",  "Africa/Johannesburg", "ZAc",  "EQUITY"),
    "ESLT.TA":  ("TLV",  "Asia/Jerusalem",      "ILS",  "EQUITY"),
    "GLEN.L":   ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "2330.TW":  ("TAI",  "Asia/Taipei",         "TWD",  "EQUITY"),
    "4063.T":   ("TYO",  "Asia/Tokyo",          "JPY",  "EQUITY"),
    "ALPHA.PA": ("PAR",  "Europe/Paris",        "EUR",  "EQUITY"),
    "AV.L":     ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "8TRA.DE":  ("GER",  "Europe/Berlin",       "EUR",  "EQUITY"),
    "1398.HK":  ("HKG",  "Asia/Hong_Kong",      "HKD",  "EQUITY"),
    "3988.HK":  ("HKG",  "Asia/Hong_Kong",      "HKD",  "EQUITY"),
    "^GSPC":    ("SNP",  "America/New_York",    "USD",  "INDEX"),
    "^DJI":     ("DJI",  "America/New_York",    "USD",  "INDEX"),
    "BTC-USD":  ("CCC",  "UTC",                 "USD",  "CRYPTOCURRENCY"),
    "SPY":      ("NYSE", "America/New_York",    "USD",  "ETF"),
    "JNK":      ("NYSE", "America/New_York",    "USD",  "ETF"),
    "VFINX":    ("NMS",  "America/New_York",    "USD",  "MUTUALFUND"),
    "VTSAX":    ("NMS",  "America/New_York",    "USD",  "MUTUALFUND"),
    "EXTO":     ("NMS",  "America/New_York",    "USD",  "EQUITY"),
    "DJI":      ("DJI",  "America/New_York",    "USD",  "INDEX"),
    "QCSTIX":   ("NMS",  "America/New_York",    "USD",  "MUTUALFUND"),
    "VALE":     ("NYSE", "America/New_York",    "USD",  "EQUITY"),
    "BSE.AX":   ("ASX",  "Australia/Sydney",    "AUD",  "EQUITY"),
    "NVDA":     ("NMS",  "America/New_York",    "USD",  "EQUITY"),
    "IWO":      ("NYSE", "America/New_York",    "USD",  "ETF"),
    "SOKE.IS":  ("IST",  "Europe/Istanbul",     "TRY",  "EQUITY"),
    "ADS.DE":   ("GER",  "Europe/Berlin",       "EUR",  "EQUITY"),
    # Additional tickers for price repair tests
    "LSC.L":    ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "TEM.L":    ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "TENT.L":   ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "REL.L":    ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "CLC.L":    ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "RGL.L":    ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "SERE.L":   ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "ELCO.L":   ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "PSH.L":    ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "NVT.L":    ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "KMR.L":    ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "CNE.L":    ("LSE",  "Europe/London",       "GBp",  "EQUITY"),
    "KME.MI":   ("MIL",  "Europe/Rome",         "EUR",  "EQUITY"),
    "SPM.MI":   ("MIL",  "Europe/Rome",         "EUR",  "EQUITY"),
    "TISG.MI":  ("MIL",  "Europe/Rome",         "EUR",  "EQUITY"),
    "MOB.ST":   ("STO",  "Europe/Stockholm",    "SEK",  "EQUITY"),
    "BOL.ST":   ("STO",  "Europe/Stockholm",    "SEK",  "EQUITY"),
    "TUI1.DE":  ("GER",  "Europe/Berlin",       "EUR",  "EQUITY"),
    "SCR.TO":   ("TOR",  "America/Toronto",     "CAD",  "EQUITY"),
    "LA.V":     ("VAN",  "America/Vancouver",   "CAD",  "EQUITY"),
    "DEX.AX":   ("ASX",  "Australia/Sydney",    "AUD",  "EQUITY"),
    "KEN.TA":   ("TLV",  "Asia/Jerusalem",      "ILS",  "EQUITY"),
    "KAP.IL":   ("TLV",  "Asia/Jerusalem",      "ILS",  "EQUITY"),
    "HSBK.IL":  ("TLV",  "Asia/Jerusalem",      "ILS",  "EQUITY"),
    "TEP.PA":   ("PAR",  "Europe/Paris",        "EUR",  "EQUITY"),
    "IBE.MC":   ("MCE",  "Europe/Madrid",       "EUR",  "EQUITY"),
    "DODFX":    ("NMS",  "America/New_York",    "USD",  "MUTUALFUND"),
    "VWILX":    ("NMS",  "America/New_York",    "USD",  "MUTUALFUND"),
    "JENYX":    ("NMS",  "America/New_York",    "USD",  "MUTUALFUND"),
    "AGRO3.SA": ("SAO",  "America/Sao_Paulo",   "BRL",  "EQUITY"),
    "ABBV":     ("NYSE", "America/New_York",    "USD",  "EQUITY"),
    "QQQ":      ("NYSE", "America/New_York",    "USD",  "ETF"),
    "GDX":      ("NYSE", "America/New_York",    "USD",  "ETF"),
    "FXAIX":    ("NMS",  "America/New_York",    "USD",  "MUTUALFUND"),
    "_DEFAULT": ("NMS",  "America/New_York",    "USD",  "EQUITY"),
}

_RANGE_TO_DAYS = {
    "1d": 1, "5d": 5, "1mo": 21, "3mo": 63, "6mo": 126,
    "1y": 252, "2y": 504, "5y": 1260, "10y": 2520, "ytd": 100, "max": 3000,
}


def _range_to_n_rows(range_str):
    if range_str in _RANGE_TO_DAYS:
        return _RANGE_TO_DAYS[range_str]
    import re
    m = re.match(r"^(\d+)(d|mo|y|wk)$", range_str)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit == "d":
            return n
        if unit == "wk":
            return n * 5
        if unit == "mo":
            return n * 21
        if unit == "y":
            return n * 252
    return 252


def _get_meta(ticker):
    return _TICKER_META.get(ticker, _TICKER_META.get(ticker.upper(), _TICKER_META["_DEFAULT"]))


def _business_days_before(end_date, n):
    """Return list of n business day dates ending on or before end_date."""
    dates = []
    d = end_date
    while len(dates) < n:
        if d.weekday() < 5:
            dates.append(d)
        d -= datetime.timedelta(days=1)
    dates.reverse()
    return dates


def _mondays_before(end_date, n):
    """Return list of n Monday dates ending on or before end_date."""
    dates = []
    d = end_date - datetime.timedelta(days=end_date.weekday())  # snap to Monday
    while len(dates) < n:
        dates.append(d)
        d -= datetime.timedelta(weeks=1)
    dates.reverse()
    return dates


def _month_starts_before(end_date, n):
    """Return list of n first-of-month business dates ending on or before end_date."""
    dates = []
    y, m = end_date.year, end_date.month
    while len(dates) < n:
        d = datetime.date(y, m, 1)
        if d.weekday() == 5:  # Saturday -> Monday
            d += datetime.timedelta(days=2)
        elif d.weekday() == 6:  # Sunday -> Monday
            d += datetime.timedelta(days=1)
        dates.append(d)
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    dates.reverse()
    return dates


def _quarter_starts_before(end_date, n):
    """Return list of n first-of-quarter business dates ending on or before end_date."""
    dates = []
    y, m = end_date.year, end_date.month
    qm = ((m - 1) // 3) * 3 + 1  # snap to quarter start month
    while len(dates) < n:
        d = datetime.date(y, qm, 1)
        if d.weekday() == 5:
            d += datetime.timedelta(days=2)
        elif d.weekday() == 6:
            d += datetime.timedelta(days=1)
        dates.append(d)
        qm -= 3
        if qm <= 0:
            qm += 12
            y -= 1
    dates.reverse()
    return dates


def _to_unix(d):
    """Convert date to Unix timestamp at noon UTC (avoids DST edge cases)."""
    dt = datetime.datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=datetime.timezone.utc)
    return int(dt.timestamp())


def _intraday_timestamps(n_days, interval, end_date):
    """Generate intraday timestamps for a given interval."""
    minutes_per_bar = {
        "1m": 1, "2m": 2, "5m": 5, "15m": 15, "30m": 30,
        "60m": 60, "90m": 90, "1h": 60,
    }.get(interval, 60)

    bars_per_day = 390 // minutes_per_bar  # ~390 trading minutes/day for US market
    dates = _business_days_before(end_date, n_days)
    timestamps = []
    for d in dates:
        # Market opens at 14:30 UTC (9:30 ET)
        open_ts = int(datetime.datetime(d.year, d.month, d.day, 14, 30, 0).timestamp())
        for bar in range(bars_per_day):
            timestamps.append(open_ts + bar * minutes_per_bar * 60)
    return timestamps


def _make_events(instrument_type, tz_name, timestamps, base, intraday=False,
                 intraday_div_dates=None, fixed_dividends=None):
    """Synthetic dividend events for EQUITY/ETF tickers.

    Only generates dividends for US-timezone tickers (America/New_York) to
    avoid date-boundary mismatches when intraday mock uses 14:30 UTC open
    times (= 9:30 ET) which map to the wrong calendar day on non-US exchanges.

    Pass fixed_dividends=[(date, amount), ...] to emit exact dates instead of
    synthetic ones — bypasses the US-only and instrument-type guards.
    """
    if fixed_dividends is not None:
        ts_set = set(timestamps)
        divs = {}
        for d, amount in fixed_dividends:
            ts = _to_unix(d)
            if ts in ts_set:
                divs[str(ts)] = {"amount": amount, "date": ts}
        return {"dividends": divs} if divs else {}

    if instrument_type not in ("EQUITY", "ETF") or len(timestamps) < 4:
        return {}
    if tz_name not in ("America/New_York",):
        return {}
    div_amount = round(base * 0.001, 4)
    if intraday:
        if not intraday_div_dates:
            return {}
        divs = {}
        for d in intraday_div_dates:
            # Use 14:30 UTC (= 9:30 ET market open) so the timestamp lands on
            # an actual intraday bar.  _to_unix() uses noon UTC, which falls
            # before the first bar of the day and gets dropped by yfinance.
            ts = int(datetime.datetime(d.year, d.month, d.day, 14, 30, 0).timestamp())
            divs[str(ts)] = {"amount": div_amount, "date": ts}
        return {"dividends": divs} if divs else {}
    t1 = timestamps[len(timestamps) // 4]
    t2 = timestamps[3 * len(timestamps) // 4]
    return {
        "dividends": {
            str(t1): {"amount": div_amount, "date": t1},
            str(t2): {"amount": div_amount, "date": t2},
        }
    }


def _intraday_div_dates(timestamps, params, intraday, has_explicit_range):
    """Compute dividend dates for intraday charts with explicit date ranges.

    For an intraday request over [period1, period2), we mirror the daily-chart
    dividend placement: put dividends on the calendar date of period1 (= d1)
    and the calendar date of period2-1day (= d2).  These exactly match the
    noon-UTC timestamps that the daily chart places dividends at, so the
    floor('D') comparison in test_intraDayWithEvents passes.
    """
    if not intraday or not has_explicit_range:
        return []
    try:
        d1 = datetime.datetime.fromtimestamp(
            int(params["period1"]), tz=datetime.timezone.utc
        ).date()
        d2 = (datetime.datetime.fromtimestamp(
            int(params["period2"]), tz=datetime.timezone.utc
        ) - datetime.timedelta(days=1)).date()
    except (ValueError, TypeError, OSError):
        return []
    if d1 >= d2:
        return []
    return [d1, d2]


def make_response(ticker, params):
    params = params or {}
    range_str = params.get("range", "1y")
    interval = params.get("interval", "1d")

    exchange, tz_name, currency, instrument_type = _get_meta(ticker)

    end_date = datetime.date.today() - datetime.timedelta(days=1)

    # Determine date range from params
    if "period1" in params and "period2" in params:
        try:
            dt1 = datetime.datetime.fromtimestamp(int(params["period1"]), tz=datetime.timezone.utc).replace(tzinfo=None)
            dt2 = datetime.datetime.fromtimestamp(int(params["period2"]), tz=datetime.timezone.utc).replace(tzinfo=None)
            calendar_days = max(1, (dt2 - dt1).days)
            n_rows = max(1, math.ceil(calendar_days * 5 / 7))
            end_date = dt2.date()
        except (ValueError, TypeError, OSError):
            n_rows = _RANGE_TO_DAYS.get(range_str, 252)
    else:
        n_rows = _range_to_n_rows(range_str)

    intraday = interval[-1] in ("m", "h")

    # For explicit date-range requests, intraday data covers the full range.
    # For range-string requests (e.g. "5d"), cap at 7 days to stay realistic.
    has_explicit_range = "period1" in params and "period2" in params

    if intraday:
        if has_explicit_range:
            n_days = max(1, n_rows)
            # period2 is exclusive, so the last trading day is period2 - 1.
            # Using period2 itself as end_date shifts the window one day forward
            # and leaves the first requested day (period1's date) without bars.
            intraday_end = end_date - datetime.timedelta(days=1)
            while intraday_end.weekday() >= 5:  # snap weekend back to Friday
                intraday_end -= datetime.timedelta(days=1)
        else:
            n_days = max(1, min(n_rows, 7))
            intraday_end = end_date
        timestamps = _intraday_timestamps(n_days, interval, intraday_end)
    else:
        if interval == "1wk":
            n_rows = max(2, n_rows // 5)
            dates = _mondays_before(end_date, n_rows)
        elif interval == "1mo":
            n_rows = max(2, n_rows // 21)
            dates = _month_starts_before(end_date, n_rows)
        elif interval == "3mo":
            n_rows = max(2, n_rows // 63)
            dates = _quarter_starts_before(end_date, n_rows)
        else:
            dates = _business_days_before(end_date, n_rows)
        timestamps = [_to_unix(d) for d in dates]

    n = len(timestamps)
    if n == 0:
        timestamps = [_to_unix(end_date)]
        n = 1

    # Synthetic OHLCV — deterministic, non-trivial values
    seed = sum(ord(c) for c in ticker)
    base = 100.0 + (seed % 900)
    prices = [base * (1 + 0.001 * math.sin(i)) for i in range(n)]
    opens   = [round(p * 0.999, 4) for p in prices]
    closes  = [round(p * 1.001, 4) for p in prices]
    highs   = [round(p * 1.005, 4) for p in prices]
    lows    = [round(p * 0.995, 4) for p in prices]
    volumes = [1_000_000 + (seed * i) % 500_000 for i in range(n)]

    last_ts = timestamps[-1]

    return MockResponse({
        "chart": {
            "result": [{
                "meta": {
                    "currency": currency,
                    "symbol": ticker,
                    "exchangeName": exchange,
                    "fullExchangeName": exchange,
                    "instrumentType": instrument_type,
                    "firstTradeDate": 345479400,
                    "regularMarketTime": last_ts,
                    "hasPrePostMarketData": True,
                    "gmtoffset": -18000,
                    "timezone": "EST",
                    "exchangeTimezoneName": tz_name,
                    "regularMarketPrice": closes[-1],
                    "chartPreviousClose": closes[-2] if n > 1 else closes[-1],
                    "priceHint": 2,
                    "dataGranularity": interval,
                    "range": range_str,
                    "validRanges": ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"],
                    "currentTradingPeriod": {
                        "pre":     {"timezone": "EST", "start": last_ts - 6600, "end": last_ts - 4200, "gmtoffset": -18000},
                        "regular": {"timezone": "EST", "start": last_ts - 4200, "end": last_ts,        "gmtoffset": -18000},
                        "post":    {"timezone": "EST", "start": last_ts,        "end": last_ts + 4800, "gmtoffset": -18000},
                    },
                },
                "timestamp": timestamps,
                "indicators": {
                    "quote": [{
                        "open":   opens,
                        "high":   highs,
                        "low":    lows,
                        "close":  closes,
                        "volume": volumes,
                    }],
                    "adjclose": [{"adjclose": closes}],
                },
                "events": _make_events(
                    instrument_type, tz_name, timestamps, base, intraday,
                    intraday_div_dates=_intraday_div_dates(
                        timestamps, params, intraday, has_explicit_range
                    ),
                    fixed_dividends=_TICKER_FIXED_DIVIDENDS.get(ticker),
                ),
            }],
            "error": None,
        }
    })


def make_error_response(ticker, description="No data found"):
    """Return a chart response indicating no data (ticker not found etc.)."""
    return MockResponse({
        "chart": {
            "result": None,
            "error": {"code": "Not Found", "description": description},
        }
    })


def csv_to_chart_response(csv_path, ticker, interval="1d"):
    """Convert a tests/data CSV fixture to a chart JSON mock response."""
    import pandas as pd
    import json

    df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    df = df.sort_index()

    exchange, tz_name, currency, instrument_type = _get_meta(ticker)

    # Convert index to Unix timestamps
    if hasattr(df.index, "tz") and df.index.tz is not None:
        timestamps = [int(ts.timestamp()) for ts in df.index]
    else:
        timestamps = [int(pd.Timestamp(ts).timestamp()) for ts in df.index]

    n = len(timestamps)

    def col(name, default=0.0):
        if name in df.columns:
            return [float(v) if v == v else None for v in df[name]]
        return [default] * n

    opens   = col("Open")
    highs   = col("High")
    lows    = col("Low")
    closes  = col("Close")
    adj_cl  = col("Adj Close", closes[-1] if closes else 0.0)
    volumes = col("Volume", 0)

    last_ts = timestamps[-1] if timestamps else 0

    data = {
        "chart": {
            "result": [{
                "meta": {
                    "currency": currency,
                    "symbol": ticker,
                    "exchangeName": exchange,
                    "fullExchangeName": exchange,
                    "instrumentType": instrument_type,
                    "firstTradeDate": timestamps[0] if timestamps else 0,
                    "regularMarketTime": last_ts,
                    "hasPrePostMarketData": False,
                    "gmtoffset": -18000,
                    "timezone": "EST",
                    "exchangeTimezoneName": tz_name,
                    "regularMarketPrice": closes[-1] if closes else 0.0,
                    "chartPreviousClose": closes[-2] if n > 1 else (closes[-1] if closes else 0.0),
                    "priceHint": 2,
                    "dataGranularity": interval,
                    "range": "max",
                    "validRanges": ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"],
                    "currentTradingPeriod": {
                        "pre":     {"timezone": "EST", "start": last_ts - 6600, "end": last_ts - 4200, "gmtoffset": -18000},
                        "regular": {"timezone": "EST", "start": last_ts - 4200, "end": last_ts,        "gmtoffset": -18000},
                        "post":    {"timezone": "EST", "start": last_ts,        "end": last_ts + 4800, "gmtoffset": -18000},
                    },
                    "tradingPeriods": [],
                },
                "timestamp": timestamps,
                "indicators": {
                    "quote": [{"open": opens, "high": highs, "low": lows, "close": closes, "volume": volumes}],
                    "adjclose": [{"adjclose": adj_cl}],
                },
                "events": {},
            }],
            "error": None,
        }
    }
    return MockResponse(data, text=json.dumps(data))
