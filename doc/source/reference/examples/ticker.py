"""Single ticker example."""

import yfinance as yf


def main():
    """Fetch common ticker resources."""
    dat = yf.Ticker("MSFT")
    dat.live()
    return {
        "history": dat.history(period='1mo'),
        "option_calls": dat.option_chain(dat.options[0]).calls,
        "balance_sheet": dat.balance_sheet,
        "quarterly_income_stmt": dat.quarterly_income_stmt,
        "calendar": dat.calendar,
        "info": dat.info,
        "analyst_price_targets": dat.analyst_price_targets,
        "ticker": dat,
    }
