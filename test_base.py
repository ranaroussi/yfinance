import unittest
from base.py import generic_patterns

tickers = [yf.Ticker(symbol) for symbol in symbols]

class TestGenericPatterns(unittest.TestCae):
    data = {}
    
    def test_generic_pattern(self, data):
        for key in (
                (self._cashflow, 'cashflowStatement', 'cashflowStatements'),
                (self._balancesheet, 'balanceSheet', 'balanceSheetStatements'),
                (self._financials, 'incomeStatement', 'incomeStatementHistory')
            ):
                print(key)
                item = key[1] + 'History'
                if isinstance(data.get(item), dict):
                    try:
                        key[0]['yearly'] = cleanup(data[item][key[2]])
                    except Exception as e:
                        pass

                item = key[1]+'HistoryQuarterly'
                if isinstance(data.get(item), dict):
                    try:
                        key[0]['quarterly'] = cleanup(data[item][key[2]])
                    except Exception as e:
                        pass
    
if __name__ == 'generic_patterns()':
    unittest.generic_patterns()