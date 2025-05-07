import yfinance as yf

# Ticker to Sector and Industry
msft = yf.Ticker("MSFT")
tech = yf.Sector(msft.info.get("sectorKey"))
software = yf.Industry(msft.info.get("industryKey"))

# Sector and Industry to Ticker
tech_ticker = tech.ticker
tech_ticker.info
software_ticker = software.ticker
software_ticker.history()
