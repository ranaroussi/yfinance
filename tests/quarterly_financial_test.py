from data.valid import msft_quarterly
import sys
sys.path.append("..")
from yfinance.ticker import Ticker

class TickerMock(Ticker):
    def _get_fundamentals(self, proxy=None):
        self._financials['quarterly'] = msft_quarterly

def test_quarterly_financials_is_in_correct_format():
    testTicker = TickerMock("MSFT")
    info = testTicker.quarterly_financials
    assert(info is msft_quarterly)

if __name__ == "__main__":
    test_quarterly_financials_is_in_correct_format()