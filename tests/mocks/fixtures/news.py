from ..response import MockResponse


def make_response(body):
    tickers = (body.get("serviceConfig", {}).get("s") or ["AAPL"])
    ticker = tickers[0] if tickers else "AAPL"
    return MockResponse({
        "data": {
            "tickerStream": {
                "stream": [{
                    "id": "abc123",
                    "contentType": "STORY",
                    "story": {
                        "title": f"{ticker} quarterly earnings beat estimates",
                        "pubTime": "2024-11-01T12:00:00Z",
                        "summary": "A strong quarter for the company.",
                        "canonicalUrl": {"url": "https://finance.yahoo.com/news/story"},
                        "provider": {"displayName": "Reuters"},
                        "thumbnailUrl": None,
                    },
                }]
            }
        }
    })
