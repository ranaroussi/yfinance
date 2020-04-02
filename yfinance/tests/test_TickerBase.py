"""
TODO: add docstring
"""
# --- Imports

# Standard library
import logging
import unittest

# External packages
import pandas as pd

# yfinance
from yfinance.base import TickerBase

# --- Constants


# --- Disable logging during unit testing

logging.disable(logging.CRITICAL)


# --- Class definition

class TickerBase_tests(unittest.TestCase):  # pylint: disable=invalid-name
    """
    Unit tests for TickerBase class.
    """
    # --- Test preparation and clean up

    def setUp(self):  # pylint: disable=invalid-name
        """
        Perform preparations required by most tests.
        """
        self.ticker = 'MSFT'

    # --- Test cases

    def test_constructor_1(self):
        """
        Test constructor. Typical parameters.
        """
        # pylint: disable=protected-access

        # Construct TickerBase object
        ticker = TickerBase(self.ticker)

        # Check properties
        assert ticker.ticker == self.ticker
        assert ticker._history is None
        assert ticker._base_url == 'https://query1.finance.yahoo.com'
        assert ticker._scrape_url == 'https://finance.yahoo.com/quote'

        assert ticker._fundamentals is False
        assert ticker._info is None
        assert ticker._sustainability is None
        assert ticker._recommendations is None
        assert ticker._major_holders is None
        assert ticker._institutional_holders is None
        assert ticker._isin is None

        assert ticker._calendar is None
        assert ticker._expirations == {}

        expected_frequencies = ['yearly', 'quarterly']
        financial_statement_types = [ticker._financials,
                                     ticker._balancesheet,
                                     ticker._cashflow,
                                     ticker._earnings,
                                     ]
        for financial_statement_type in financial_statement_types:
            for frequency in expected_frequencies:
                assert frequency in financial_statement_type
                assert financial_statement_type[frequency].empty

    def test_constructor_2(self):
        """
        Test constructor. Lower case ticker.
        """
        # Construct TickerBase object
        ticker = TickerBase(self.ticker.lower())

        # Check properties
        assert ticker.ticker == self.ticker

    def test_get_fundamentals(self):
        """
        Test _get_fundamentals().
        """
        # pylint: disable=protected-access

        # --- Preparations

        # Construct TickerBase object
        ticker = TickerBase(self.ticker.lower())

        # Get fundamentals
        ticker._get_fundamentals()

        # --- Check results

        # ------ Check company financials

        expected_frequencies = ['yearly', 'quarterly']

        # Check ticker._financials
        for frequency in expected_frequencies:
            assert frequency in ticker._financials

            financial_statement = ticker._financials[frequency]
            assert isinstance(financial_statement, pd.DataFrame)

            expected_num_columns = 4
            assert len(financial_statement.columns) == expected_num_columns

            assert len(financial_statement.index) > 0

        # Check ticker._balancesheet
        for frequency in expected_frequencies:
            assert frequency in ticker._balancesheet

            financial_statement = ticker._balancesheet[frequency]
            assert isinstance(financial_statement, pd.DataFrame)

            expected_num_columns = 4
            assert len(financial_statement.columns) == expected_num_columns

            assert len(financial_statement.index) > 0

        # Check ticker._cashflow
        for frequency in expected_frequencies:
            assert frequency in ticker._cashflow

            financial_statement = ticker._cashflow[frequency]
            assert isinstance(financial_statement, pd.DataFrame)

            expected_num_columns = 4
            assert len(financial_statement.columns) == expected_num_columns

            assert len(financial_statement.index) > 0

        # Check ticker._earnings
        for frequency in expected_frequencies:
            assert frequency in ticker._earnings

            financial_statement = ticker._earnings[frequency]
            assert isinstance(financial_statement, pd.DataFrame)

            expected_num_columns = 2
            assert len(financial_statement.columns) == expected_num_columns

            assert len(financial_statement.index) > 0

        # ------ Check other retrieved fundamental data

        # TODO
