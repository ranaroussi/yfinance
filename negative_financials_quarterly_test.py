import yfinance as yf
from unittest import TestCase
from unittest.mock import patch
import pandas as _pd
import numpy as _np
 
 
class FinancialsTestCase(TestCase):
  def setUp(self):
    with patch('yf.Ticker(' ')') as MockTicker:
    self.ticker = MockTicker()
    index =[]
    data = _pd.DataFrame(index =index,data={
    'Open': _np.nan, 'High': _np.nan, 'Low': _np.nan,
    'Close': _np.nan, 'Adj Close': _np.nan, 'Volume': _np.nan})
    ticker.quarterly_financials.return_value = data
    self.response = ticker.quarterly_financials
  def test_get_financials_quarterly_invalid_data_returns_empty_dataframe(self):
    self.assertIsNone(self.response)
            
    
            
            
            
if __name__ == "__main__":
    unittest.main()
