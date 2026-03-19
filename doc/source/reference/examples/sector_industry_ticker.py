"""Ticker to sector and industry example."""

import yfinance.client as yf


def main():
    """Navigate between a ticker, its sector, and its industry."""
    msft = yf.Ticker('MSFT')
    sector_key = msft.info.get('sectorKey')
    industry_key = msft.info.get('industryKey')
    if not isinstance(sector_key, str) or not isinstance(industry_key, str):
        raise ValueError("Expected sectorKey and industryKey to be present in ticker info")

    tech = yf.Sector(sector_key)
    software = yf.Industry(industry_key)
    tech_ticker = tech.ticker
    software_ticker = software.ticker
    return {
        "tech_ticker_info": tech_ticker.info,
        "software_history": software_ticker.history(),
    }
