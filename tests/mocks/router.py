"""
URL router - maps Yahoo Finance GET/POST URLs to fixture factories.
"""

import re
from urllib.parse import urlparse, parse_qs

from .fixtures import chart, quote_summary, timeseries, search, lookup, calendar, screener
from .fixtures import options, news, html
from .response import MockResponse


def _extract_ticker(url, pattern):
    m = re.search(pattern, url)
    return m.group(1) if m else "AAPL"


def route_get(url, params=None, timeout=30):
    params = params or {}

    if "/v8/finance/chart/" in url:
        ticker = _extract_ticker(url, r"/v8/finance/chart/([^/?]+)")
        if chart.is_error_ticker(ticker):
            return chart.make_error_response(ticker)
        return chart.make_response(ticker, params)

    if "/v10/finance/quoteSummary/" in url:
        ticker = _extract_ticker(url, r"/v10/finance/quoteSummary/([^/?]+)")
        return quote_summary.make_response(ticker, params)

    if "/v7/finance/quote" in url:
        symbols = params.get("symbols", params.get("symbol", "AAPL"))
        ticker = symbols.split(",")[0].strip() if symbols else "AAPL"
        return quote_summary.make_v7_quote_response(ticker, params)

    if "/v1/finance/timeseries/" in url:
        ticker = _extract_ticker(url, r"/v1/finance/timeseries/([^/?]+)")
        url_params = parse_qs(urlparse(url).query)
        url_type_list = url_params.get("type", [""])[0]
        enriched = {**params, "_url_types": url_type_list}
        if url_type_list == "":
            return timeseries.make_shares_response(ticker)
        if url_type_list.startswith("trailing") and "PegRatio" in url_type_list:
            return timeseries.make_complementary_response(ticker, enriched)
        return timeseries.make_response(ticker, enriched)

    if "/v1/finance/screener/predefined" in url:
        return screener.make_predefined_response(params)

    if "/v1/finance/search" in url:
        return search.make_response(params)

    if "/v1/finance/lookup" in url:
        return lookup.make_response(params)

    if "/v7/finance/options/" in url:
        ticker = _extract_ticker(url, r"/v7/finance/options/([^/?]+)")
        return options.make_response(ticker)

    if "finance.yahoo.com/calendar/earnings" in url:
        url_params = parse_qs(urlparse(url).query)
        size = int(url_params.get("size", ["25"])[0])
        symbol = url_params.get("symbol", ["AAPL"])[0]
        return html.earnings_calendar_response(symbol, size)

    if "finance.yahoo.com/quote/" in url and "key-statistics" in url:
        return html.valuation_response()

    if "finance.yahoo.com" in url:
        return MockResponse(text="<html><body></body></html>")

    raise NotImplementedError(f"No mock for GET: {url}")


def route_post(url, body=None, params=None, timeout=30, data=None):
    if "/v1/finance/visualization" in url:
        return calendar.make_response(body or {})

    if "/v1/finance/screener" in url:
        return screener.make_response(body or {})

    if "/xhr/ncp" in url:
        return news.make_response(body or {})

    raise NotImplementedError(f"No mock for POST: {url}")
