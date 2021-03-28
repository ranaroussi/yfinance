# Get parent directory to import the yfinancial library
import os
import sys
import inspect
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir) 

import unittest
import pandas
from unittest.mock import MagicMock, Mock
import yfinance.base as yfBase


class TestEarnings(unittest.TestCase):
    def mock_get_json(self, earnings_data):
        """
        Returns the sum of two decimal numbers in binary digits.

                Parameters:
                        earnings_data (dict): Earnings dictionary with a financialChart containing yearly/quarterly earnings

        """
        
        first_return = {"financialData":{"regularMarketPrice":100, "regularMarketOpen":100}}
        second_return = earnings_data
        yfBase.utils.get_json = Mock(side_effect=[first_return, second_return])

    def set_up(self):
        """
        Create a TickerBase instance and mock methods that prevent exceptions
        when _get_fundamentals() is called.
        """
        
        self.tb = yfBase.TickerBase("stub_ticker_base")
        yfBase._pd.read_html = MagicMock(return_value=[None,])

    def test_yearly_earnings(self):
        """
        Test that TickerBase saves yearly earnings using data
        obtained from a request to ticker_url/financials.
        """
        
        self.set_up()
        
        earnings_data = {"earnings":{
                                "financialsChart":{
                                    "yearly":{
                                        "date":["December 1, 2021"],
                                        "total":[100]
                                    },
                                    "quarterly":{
                                        "date":["November 2, 2021"],
                                        "total":[500]
                                    }
                                }
                            }
                        }
        self.mock_get_json(earnings_data)
        self.tb._get_fundamentals()

        financials_chart = earnings_data["earnings"]["financialsChart"]
        expected_yearly_frame = pandas.DataFrame(financials_chart["yearly"])
        
        self.assertEqual(expected_yearly_frame.sort_index(inplace=True), 
                         self.tb._earnings["yearly"].sort_index(inplace=True))

    def test_quarterly_earnings(self):
        """
        Test that TickerBase saves quarterly earnings using data
        obtained from a request to ticker_url/financials.
        """
        
        self.set_up()
        
        earnings_data = {"earnings":{
                                "financialsChart":{
                                    "yearly":{
                                        "date":["December 1, 2021"],
                                        "total":[100]
                                    },
                                    "quarterly":{
                                        "date":["November 2, 2021"],
                                        "total":[500]
                                    }
                                }
                            }
                        }
        self.mock_get_json(earnings_data)
        self.tb._get_fundamentals()
        
        financials_chart = earnings_data["earnings"]["financialsChart"]
        expected_quarterly_frame = pandas.DataFrame(financials_chart["quarterly"])
        
        self.assertEqual(expected_quarterly_frame.sort_index(inplace=True), 
                         self.tb._earnings["quarterly"].sort_index(inplace=True))
        

    def test_yearly_and_quarterly_earnings(self):
        """
        Test that TickerBase saves both yearly and quarterly earnings using data
        obtained from a request to ticker_url/financials.
        """
        self.set_up()
        
        earnings_data = {"earnings":{
                                "financialsChart":{
                                    "yearly":{
                                        "date":["December 1, 2021"],
                                        "total":[100]
                                    },
                                    "quarterly":{
                                        "date":["November 2, 2021"],
                                        "total":[500]
                                    }
                                }
                            }
                        }
        self.mock_get_json(earnings_data)
        self.tb._get_fundamentals()

        financials_chart = earnings_data["earnings"]["financialsChart"]
        expected_yearly_frame = pandas.DataFrame(financials_chart["yearly"])
        expected_quarterly_frame = pandas.DataFrame(financials_chart["quarterly"])
        
        self.assertEqual(expected_quarterly_frame.sort_index(inplace=True), 
                         self.tb._earnings["quarterly"].sort_index(inplace=True))
        self.assertEqual(expected_yearly_frame.sort_index(inplace=True), 
                         self.tb._earnings["yearly"].sort_index(inplace=True))

if __name__ == '__main__':
    unittest.main()