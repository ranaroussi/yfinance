"""
Factory for POST /v1/finance/visualization (calendar endpoint).
"""

import datetime
from ..response import MockResponse

# Use a date well in the future so datetime comparisons in tests pass
def _future_date(days_ahead):
    d = datetime.date.today() + datetime.timedelta(days=days_ahead)
    return d.strftime("%Y-%m-%dT00:00:00.000Z")


def _col(label, type_="STRING"):
    return {"label": label, "type": type_}


def make_response(body):
    body = body or {}
    entity_type = body.get("entityIdType", "sp_earnings")
    size = int(body.get("size", 5))

    if entity_type == "sp_earnings":
        return _earnings_response(size)
    if entity_type == "ipo_info":
        return _ipo_response(size)
    if entity_type == "economic_event":
        return _economic_events_response(size)
    if entity_type == "splits":
        return _splits_response(size)
    return _earnings_response(size)


_EARNINGS_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
                     "BRK-B", "JPM", "V", "UNH", "XOM", "JNJ", "PG", "HD"]


def _earnings_response(size):
    # "Event Start Date" with type "DATETIME" stays as-is (not renamed to "Timing")
    columns = [
        _col("Symbol"),
        _col("Company Name"),
        _col("Market Cap (Intraday)"),
        _col("Event Start Date", "DATETIME"),
        _col("Call Time"),
        _col("EPS Estimate"),
        _col("Reported EPS"),
        _col("Surprise (%)"),
    ]
    rows = []
    for i in range(size):
        ticker = _EARNINGS_TICKERS[i % len(_EARNINGS_TICKERS)]
        rows.append({
            "Symbol": ticker,
            "Company Name": f"{ticker} Inc.",
            "Market Cap (Intraday)": "2500000000000",
            "Event Start Date": _future_date(7 + i),
            "Call Time": "AMC",
            "EPS Estimate": "2.10",
            "Reported EPS": "0",
            "Surprise (%)": "0",
        })
    return _wrap(columns, rows)


def _ipo_response(size):
    columns = [
        _col("Symbol"),
        _col("Company"),
        _col("Exchange Short Name"),
        _col("Filing Date", "DATETIME"),
        _col("Date", "DATETIME"),
        _col("Amended Date", "DATETIME"),
        _col("Price From"),
        _col("Price To"),
        _col("Price"),
        _col("Shares"),
    ]
    rows = []
    for i in range(size):
        rows.append({
            "Symbol": f"IPO{i+1}",
            "Company": f"New Company {i+1}",
            "Exchange Short Name": "NMS",
            "Filing Date": _future_date(14 + i),
            "Date": _future_date(14 + i),
            "Amended Date": _future_date(14 + i),
            "Price From": "18",
            "Price To": "22",
            "Price": "0",
            "Shares": "10000000",
        })
    return _wrap(columns, rows)


_ECON_EVENTS = ["CPI YoY", "PPI MoM", "NFP", "Unemployment Rate", "FOMC Rate",
                "GDP QoQ", "Retail Sales", "Consumer Confidence", "ISM Mfg", "CPI MoM"]


def _economic_events_response(size):
    columns = [
        _col("Event"),
        _col("Country Code"),
        _col("Event Time", "DATETIME"),
        _col("Actual"),
        _col("Market Expectation"),
        _col("Prior to This"),
        _col("Revised from"),
    ]
    rows = []
    for i in range(size):
        rows.append({
            "Event": _ECON_EVENTS[i % len(_ECON_EVENTS)],
            "Country Code": "US",
            "Event Time": _future_date(3 + i),
            "Actual": "0",
            "Market Expectation": "2.6",
            "Prior to This": "2.4",
            "Revised from": "0",
        })
    return _wrap(columns, rows)


_SPLIT_TICKERS = ["NVDA", "TSLA", "AMZN", "AAPL", "GOOGL",
                  "META", "MSFT", "NFLX", "UBER", "LYFT"]


def _splits_response(size):
    columns = [
        _col("Symbol"),
        _col("Company Name"),
        _col("Payable On", "DATETIME"),
        _col("Optionable"),
        _col("Ratio"),
    ]
    rows = []
    for i in range(size):
        ticker = _SPLIT_TICKERS[i % len(_SPLIT_TICKERS)]
        rows.append({
            "Symbol": ticker,
            "Company Name": f"{ticker} Inc.",
            "Payable On": _future_date(30 + i * 7),
            "Optionable": "Yes",
            "Ratio": "2:1",
        })
    return _wrap(columns, rows)


def _wrap(columns, rows):
    return MockResponse({
        "finance": {
            "result": [{
                "documents": [{
                    "columns": columns,
                    "rows": rows,
                    "total": len(rows),
                }],
                "total": len(rows),
            }],
            "error": None,
        }
    })
