import yfinance as yf


def test_invalid_query():
    result = yf.Search(query="XYZXYZXYZ")
    assert len(result.quotes) == 0
    assert len(result.news) == 0
    assert len(result.lists) == 0
    assert len(result.nav) == 0
    assert len(result.research) == 0


def test_empty_query():
    result = yf.Search(query="")
    assert len(result.quotes) == 0
    assert len(result.news) == 0


def test_fuzzy_query():
    result = yf.Search(query="Appel", enable_fuzzy_query=True)
    assert len(result.quotes) > 0
    assert result.quotes[0]["symbol"] == "AAPL"


def test_quotes():
    result = yf.Search(query="AAPL", max_results=5)
    assert len(result.quotes) == 5
    assert result.quotes[0]["symbol"] == "AAPL"


def test_news():
    result = yf.Search(query="AAPL", news_count=3)
    assert len(result.news) == 3


def test_research_reports():
    result = yf.Search(query="AAPL", include_research=True)
    assert len(result.research) == 3
