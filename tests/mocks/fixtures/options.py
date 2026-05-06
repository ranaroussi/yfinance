import datetime

from ..response import MockResponse


def make_response(ticker):
    today = datetime.date.today()
    expirations = [
        int((today + datetime.timedelta(days=30)).strftime("%s")),
        int((today + datetime.timedelta(days=60)).strftime("%s")),
        int((today + datetime.timedelta(days=90)).strftime("%s")),
    ]
    return MockResponse({
        "optionChain": {
            "result": [{
                "underlyingSymbol": ticker,
                "expirationDates": expirations,
                "strikes": [140.0, 145.0, 150.0, 155.0, 160.0],
                "hasMiniOptions": False,
                "quote": {"symbol": ticker, "regularMarketPrice": 150.0},
                "options": [],
            }],
            "error": None,
        }
    })
