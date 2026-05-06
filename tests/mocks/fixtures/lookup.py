from ..response import MockResponse

_BASE_SAMPLES = {
    "equity":         [("AAPL", "Apple Inc.", "NMS"), ("MSFT", "Microsoft Corp.", "NMS"),
                       ("GOOGL", "Alphabet Inc.", "NMS"), ("AMZN", "Amazon.com Inc.", "NMS"),
                       ("TSLA", "Tesla Inc.", "NMS")],
    "mutualfund":     [("VFINX", "Vanguard 500 Index", "NMS"), ("VTSAX", "Vanguard Total Stock Mkt", "NMS"),
                       ("FXAIX", "Fidelity 500 Index", "NMS"), ("SWPPX", "Schwab S&P 500 Index", "NMS"),
                       ("SWTSX", "Schwab Total Stock Market", "NMS")],
    "etf":            [("SPY", "SPDR S&P 500 ETF", "PCX"), ("QQQ", "Invesco QQQ Trust", "NMS"),
                       ("IVV", "iShares Core S&P 500", "PCX"), ("VTI", "Vanguard Total Stock ETF", "PCX"),
                       ("VOO", "Vanguard S&P 500 ETF", "PCX")],
    "index":          [("^GSPC", "S&P 500", "SNP"), ("^DJI", "Dow Jones", "DJI"),
                       ("^IXIC", "NASDAQ Composite", "NMS"), ("^RUT", "Russell 2000", "RUT"),
                       ("^VIX", "CBOE Volatility Index", "SNP")],
    "future":         [("ES=F", "E-Mini S&P 500", "CME"), ("NQ=F", "E-Mini NASDAQ-100", "CME"),
                       ("YM=F", "Mini Dow Jones", "CBT"), ("RTY=F", "E-Mini Russell 2000", "CME"),
                       ("GC=F", "Gold Futures", "CMX")],
    "currency":       [("EURUSD=X", "EUR/USD", "CCY"), ("GBPUSD=X", "GBP/USD", "CCY"),
                       ("USDJPY=X", "USD/JPY", "CCY"), ("AUDUSD=X", "AUD/USD", "CCY"),
                       ("USDCAD=X", "USD/CAD", "CCY")],
    "cryptocurrency": [("BTC-USD", "Bitcoin USD", "CCC"), ("ETH-USD", "Ethereum USD", "CCC"),
                       ("SOL-USD", "Solana USD", "CCC"), ("BNB-USD", "BNB USD", "CCC"),
                       ("XRP-USD", "XRP USD", "CCC")],
}
_BASE_SAMPLES["all"] = (
    _BASE_SAMPLES["equity"] + _BASE_SAMPLES["mutualfund"] + _BASE_SAMPLES["etf"] +
    _BASE_SAMPLES["index"] + _BASE_SAMPLES["cryptocurrency"]
)


def _make_doc(symbol, name, exchange, type_):
    return {"symbol": symbol, "name": name, "exchange": exchange, "type": type_, "exchDisp": exchange}


def make_response(params):
    params = params or {}
    lookup_type = params.get("type", "equity")
    count = int(params.get("count", 25))

    base = _BASE_SAMPLES.get(lookup_type, _BASE_SAMPLES["equity"])

    # Generate enough documents to satisfy count
    documents = []
    for i in range(count):
        sym, name, exch = base[i % len(base)]
        # Disambiguate repeated symbols when count > len(base)
        suffix = f"_{i // len(base)}" if i >= len(base) else ""
        doc = _make_doc(sym + suffix, name, exch, lookup_type.upper())
        documents.append(doc)

    return MockResponse({
        "finance": {
            "result": [{"documents": documents, "total": count}],
            "error": None,
        }
    })
