from ..response import MockResponse

_KNOWN_TICKERS = {"AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA"}

# ISIN -> Yahoo symbol mapping for tests that resolve tickers from ISIN strings.
# ISINs absent from this dict return empty results -> ValueError in Ticker constructor.
_ISIN_TO_SYMBOL = {
    "ES0137650018": "IBE.MC",
    "INF209K01EN2": "0P0000XVYZ.BO",
    "INF846K01K35": "0P0001JC30.BO",
}


def make_response(params):
    params = params or {}
    query = params.get("q", "").strip()
    max_results = int(params.get("quotesCount", 5))
    news_count = int(params.get("newsCount", 1))
    enable_research = str(params.get("enableResearchReports", "false")).lower() == "true"

    if query in _ISIN_TO_SYMBOL:
        symbol = _ISIN_TO_SYMBOL[query]
        return MockResponse({
            "quotes": [{"symbol": symbol, "shortname": "Fund", "exchange": "BSE",
                        "quoteType": "EQUITY", "isYahooFinance": True, "score": 1}],
            "news": [], "lists": [], "researchReports": [], "nav": [],
        })

    # Return empty results for blank queries or long all-caps strings that don't
    # match a known ticker (e.g. "XYZXYZQQQQQQ") — simulates Yahoo returning no hits.
    if not query or (query.upper() not in _KNOWN_TICKERS and len(query) > 6 and query.isupper()):
        return MockResponse({
            "quotes": [], "news": [], "lists": [], "researchReports": [], "nav": [],
        })

    quotes = [
        {"symbol": "AAPL",  "shortname": "Apple Inc.",           "exchange": "NMS", "quoteType": "EQUITY", "isYahooFinance": True, "score": 1722507},
        {"symbol": "AAPL.BA","shortname": "APPLE INC",           "exchange": "BUE", "quoteType": "EQUITY", "isYahooFinance": True, "score": 20449},
        {"symbol": "MSFT",  "shortname": "Microsoft Corporation", "exchange": "NMS", "quoteType": "EQUITY", "isYahooFinance": True, "score": 1500000},
        {"symbol": "GOOGL", "shortname": "Alphabet Inc.",         "exchange": "NMS", "quoteType": "EQUITY", "isYahooFinance": True, "score": 1400000},
        {"symbol": "AMZN",  "shortname": "Amazon.com Inc.",       "exchange": "NMS", "quoteType": "EQUITY", "isYahooFinance": True, "score": 1300000},
        {"symbol": "META",  "shortname": "Meta Platforms Inc.",   "exchange": "NMS", "quoteType": "EQUITY", "isYahooFinance": True, "score": 1200000},
    ][:max_results]

    news = [
        {
            "uuid": f"news-{i:04d}",
            "title": f"Market Update {i+1}: {query}",
            "publisher": "Reuters",
            "link": "https://finance.yahoo.com/news/market-update.html",
            "providerPublishTime": 1699387200 + i * 3600,
            "type": "STORY",
        }
        for i in range(news_count)
    ]

    research = [
        {
            "reportTitle": f"Research Report {i+1}: {query}",
            "author": "Analyst",
            "reportDate": "2024-11-01",
            "provider": "Goldman Sachs",
            "tickers": ["AAPL"],
        }
        for i in range(3)  # Always return 3 research reports when research is requested
    ]

    return MockResponse({
        "quotes": quotes,
        "news": news,
        "lists": [],
        "researchReports": research if enable_research else [],
        "nav": [],
        "totalTime": 35,
    })
