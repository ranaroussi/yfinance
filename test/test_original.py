#!/usr/bin/env python

from __future__ import print_function

import unittest
import yfinance as yf


class TestOriginal(unittest.TestCase):

    def setUp(self):
        print("\n! Running TestOriginal !\n")

    def test_us_tickets(self):
        default_tickets = ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']
        self._ticketListTest(default_tickets)

    def test_swe_tickets(self):
        additional_swedish_tickets = ['HTRO.ST', 'FING-B.ST', 'TELIA.ST', 'AZA.ST', 'NDA-SE.ST']
        self._ticketListTest(additional_swedish_tickets)

    def _ticketListTest(self, ticketList):
        for symbol in ticketList:
            print(">>", symbol, end=' ... ')
            ticker = yf.Ticker(symbol)

            # always should have info and history for valid symbols
            self.assertTrue(ticker.info is not None and ticker.info != {})
            self.assertTrue(ticker.history(period="max").empty is False)

            # following should always gracefully handled, no crashes
            print(ticker.cashflow)
            print(ticker.balance_sheet)
            print(ticker.financials)
            print(ticker.sustainability)
            print(ticker.major_holders)
            print(ticker.institutional_holders)
