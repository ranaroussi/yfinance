import yfinance as yf

# get list of quotes
quotes = yf.Search("AAPL", max_results=10).quotes

# get list of news
news = yf.Search("Google", news_count=10).news

# get list of related research
research = yf.Search("apple", include_research=True).research