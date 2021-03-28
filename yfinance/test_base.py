import unittest
import base


class TestGenericPatterns(unittest.TestCase):
    
    def test_test(self):
        msft = base.TickerBase(ticker="MSFT")
        empty_dict = dict()
        data = msft.generic_patterns(empty_dict)
        self.assertTrue(isinstance(data, dict))
        self.assertEquals(msft.get_info()['zip'], '98052-6399')


if __name__ == '__main__':
    unittest.main()