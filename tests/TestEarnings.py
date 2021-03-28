# Get parent directory to import the yfinancial library
import os,sys,inspect
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir) 

import unittest
import json
from unittest.mock import MagicMock, Mock
import yfinance.base as yfBase


class TestEarnings(unittest.TestCase):
    def get_json_side_effect(*args, **kwargs):
        if (not args[0].endswith("/financials")):
            # Return data for the first get_json call
            data = '''{"banana":"Funny"}'''
            print(type(json.dumps(data)))
            return json.loads(data)
        else:
            # Return data for the second get_json call
            data = '''{"banana":"Funny"}'''
            print(type(json.dumps(data)))
            return json.loads(data)


    def set_up(self):
        self.tb = yfBase.TickerBase("stub_ticker_base")
        self.tb._info = {'regularMarketPrice':100.0, 'regularMarketOpen':100.0}
        self.data = None
        yfBase.utils.get_json = Mock(side_effect=TestEarnings.get_json_side_effect)
        yfBase._pd.read_html = MagicMock(return_value=[None,])

    def test_yearly_earnings(self):
        self.set_up()
        self.tb._get_fundamentals()

    def test_quarterly_earnings(self):
        self.set_up()
        pass

    def test_yearly_and_quarterly_earnings(self):
        self.set_up()
        pass

if __name__ == '__main__':
    unittest.main()