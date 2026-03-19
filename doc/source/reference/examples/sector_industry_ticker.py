import yfinance as yf
# Ticker to Sector and Industry
msft = yf.Ticker('MSFT')
sector_key = msft.info.get('sectorKey')
industry_key = msft.info.get('industryKey')
if not isinstance(sector_key, str) or not isinstance(industry_key, str):
    raise ValueError("Expected sectorKey and industryKey to be present in ticker info")

tech = yf.Sector(sector_key)
software = yf.Industry(industry_key)

# Sector and Industry to Ticker
tech_ticker = tech.ticker
tech_ticker.info
software_ticker = software.ticker
software_ticker.history()
