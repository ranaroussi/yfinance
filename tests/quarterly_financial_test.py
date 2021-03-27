import data.valid
import sys
sys.path.append("..")
from yfinance import ticker

class TickerMock(ticker.Ticker):
    def _get_fundamentals(self, proxy=None):
        self._financials['quarterly'] = data.valid.msft_quarterly

def test_quarterly_financials_is_in_correct_format():
    testTicker = TickerMock("MSFT")
    info = testTicker.quarterly_financials
    assert(info is data.valid.msft_quarterly)

if __name__ == "__main__":
    test_quarterly_financials_is_in_correct_format()