"""Extract helpers from utils.py to fix R0915/R0912."""
from pathlib import Path

p = Path(__file__).resolve().parent.parent / "yfinance/utils.py"
text = p.read_text(encoding="utf-8")

# Insert helpers before fix_Yahoo_returning_live_separate
marker = "def fix_Yahoo_returning_live_separate"
helpers = '''
def _fix_yahoo_daily_duplicate_row(quotes):
    dt1 = quotes.index[-1]
    dt2 = quotes.index[-2]
    if quotes.index.tz is None:
        dt1 = dt1.tz_localize("UTC")
        dt2 = dt2.tz_localize("UTC")
    if dt1.date() != dt2.date():
        return quotes, None
    dropped_row = quotes.iloc[-2]
    return _pd.concat([quotes.iloc[:-2], quotes.iloc[-1:]]), dropped_row


def _fix_yahoo_repair_currency_ratio(quotes, idx1, idx2, ss, currency):
    if currency == 'KWF':
        currency_divide = 1000
    else:
        currency_divide = 100
    if abs(ss / currency_divide - 1) <= 0.25:
        return
    ratio = quotes.loc[idx1, const._PRICE_COLNAMES_] / quotes.loc[idx2, const._PRICE_COLNAMES_]
    if ((ratio / currency_divide - 1).abs() < 0.05).all():
        for c in const._PRICE_COLNAMES_:
            quotes.loc[idx2, c] *= 100
    elif ((ratio * currency_divide - 1).abs() < 0.05).all():
        for c in const._PRICE_COLNAMES_:
            quotes.loc[idx2, c] *= 0.01


def _fix_yahoo_merge_intraday_rows(quotes, interval, prepost, repair, currency):
    dt1 = quotes.index[-1]
    dt2 = quotes.index[-2]
    if quotes.index.tz is None:
        dt1 = dt1.tz_localize("UTC")
        dt2 = dt2.tz_localize("UTC")
    if not _dts_in_same_interval(dt2, dt1, interval):
        return quotes, None
    idx1 = quotes.index[-1]
    idx2 = quotes.index[-2]
    if idx1 == idx2:
        return quotes, None
    if prepost and dt1.second == 0:
        return quotes, None
    ss = quotes['Stock Splits'].iloc[-2:].replace(0, 1).prod()
    if repair:
        _fix_yahoo_repair_currency_ratio(quotes, idx1, idx2, ss, currency)
    if _np.isnan(quotes.loc[idx2, "Open"]):
        quotes.loc[idx2, "Open"] = quotes["Open"].iloc[-1]
    if not _np.isnan(quotes["High"].iloc[-1]):
        quotes.loc[idx2, "High"] = _np.nanmax([quotes["High"].iloc[-1], quotes["High"].iloc[-2]])
        if "Adj High" in quotes.columns:
            quotes.loc[idx2, "Adj High"] = _np.nanmax([quotes["Adj High"].iloc[-1], quotes["Adj High"].iloc[-2]])
    if not _np.isnan(quotes["Low"].iloc[-1]):
        quotes.loc[idx2, "Low"] = _np.nanmin([quotes["Low"].iloc[-1], quotes["Low"].iloc[-2]])
        if "Adj Low" in quotes.columns:
            quotes.loc[idx2, "Adj Low"] = _np.nanmin([quotes["Adj Low"].iloc[-1], quotes["Adj Low"].iloc[-2]])
    quotes.loc[idx2, "Close"] = quotes["Close"].iloc[-1]
    if "Adj Close" in quotes.columns:
        quotes.loc[idx2, "Adj Close"] = quotes["Adj Close"].iloc[-1]
    quotes.loc[idx2, "Volume"] += quotes["Volume"].iloc[-1]
    quotes.loc[idx2, "Dividends"] += quotes["Dividends"].iloc[-1]
    if ss != 1.0:
        quotes.loc[idx2, "Stock Splits"] = ss
    dropped_row = quotes.iloc[-1]
    return quotes.drop(quotes.index[-1]), dropped_row


'''

if "_fix_yahoo_daily_duplicate_row" not in text:
    text = text.replace(marker, helpers + marker)

new_fix_yahoo = '''def fix_Yahoo_returning_live_separate(quotes, interval, tz_exchange, prepost, repair=False, currency=None):
    # Yahoo bug fix. If market is open today then Yahoo normally returns
    # todays data as a separate row from rest-of week/month interval in above row.
    # Seems to depend on what exchange e.g. crypto OK.
    # Fix = merge them together

    if interval[-1] not in ['m', 'h']:
        prepost = False

    dropped_row = None
    if len(quotes) > 1:
        if interval == "1d":
            quotes, dropped_row = _fix_yahoo_daily_duplicate_row(quotes)
        else:
            quotes, dropped_row = _fix_yahoo_merge_intraday_rows(
                quotes, interval, prepost, repair, currency)

    return quotes, dropped_row'''

import re
text = re.sub(
    r'def fix_Yahoo_returning_live_separate\(quotes.*?return quotes, dropped_row\n',
    new_fix_yahoo + '\n',
    text,
    count=1,
    flags=re.DOTALL,
)

p.write_text(text, encoding="utf-8")
print("utils fix_Yahoo refactored")
