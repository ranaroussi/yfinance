from ..response import MockResponse

_QUOTES = [
    {"symbol": "AAPL",  "regularMarketPrice": 150.0, "marketCap": 2_500_000_000_000},
    {"symbol": "MSFT",  "regularMarketPrice": 420.0, "marketCap": 3_100_000_000_000},
    {"symbol": "GOOGL", "regularMarketPrice": 175.0, "marketCap": 2_200_000_000_000},
    {"symbol": "AMZN",  "regularMarketPrice": 185.0, "marketCap": 1_900_000_000_000},
    {"symbol": "NVDA",  "regularMarketPrice": 875.0, "marketCap": 2_150_000_000_000},
]


def _result(quotes):
    return MockResponse({
        "finance": {
            "result": [{"start": 0, "count": len(quotes), "total": len(quotes), "quotes": quotes}],
            "error": None,
        }
    })


def make_response(body):
    """Handles POST to /v1/finance/screener."""
    size = int((body or {}).get("size", len(_QUOTES)))
    return _result(_QUOTES[:size])


def make_predefined_response(params):
    """Handles GET to /v1/finance/screener/predefined/saved."""
    count = int((params or {}).get("count", len(_QUOTES)))
    return _result(_QUOTES[:min(count, len(_QUOTES))])
