import unittest
import base


class TestGenericPatterns(unittest.TestCase):
    
    def test_is_dict(self):
        tickerBase = base.TickerBase(ticker="MSFT")
        empty_dict = dict()
        data = tickerBase.generic_patterns(empty_dict)
        self.assertTrue(isinstance(data, dict))
    
    
if __name__ == '__main__':
    unittest.main()