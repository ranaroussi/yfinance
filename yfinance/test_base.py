import unittest
import base


class TestGenericPatterns(unittest.TestCase):
    
    def test_test(self):
        tickerBase = base.TickerBase(ticker="MSFT")
        empty_dict = dict()
        data = tickerBase.generic_patterns(empty_dict)
        self.assertTrue(isinstance(data, dict))
        self.assertEquals(tickerBase.get_info()['zip'], '98052-6399')


if __name__ == '__main__':
    unittest.main()