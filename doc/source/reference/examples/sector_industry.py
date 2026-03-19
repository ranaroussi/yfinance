"""Sector and industry lookup examples."""

import yfinance.client as yf


def main():
    """Fetch sector and industry metadata."""
    tech = yf.Sector('technology')
    software = yf.Industry('software-infrastructure')
    return {
        "sector_key": tech.key,
        "sector_name": tech.name,
        "sector_symbol": tech.symbol,
        "sector_ticker": tech.ticker,
        "sector_overview": tech.overview,
        "top_companies": tech.top_companies,
        "research_reports": tech.research_reports,
        "top_etfs": tech.top_etfs,
        "top_mutual_funds": tech.top_mutual_funds,
        "industries": tech.industries,
        "industry_sector_key": software.sector_key,
        "industry_sector_name": software.sector_name,
        "top_performing_companies": software.top_performing_companies,
        "top_growth_companies": software.top_growth_companies,
    }
