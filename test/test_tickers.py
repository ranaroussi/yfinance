'''
Module for testings earnings property
'''

import sys
sys.path.insert(1, '../') # Allows us to import yfinance

import unittest
import yfinance as yf

from unittest import mock
from pathlib import Path

from http.client import InvalidURL
from urllib.error import HTTPError
from mock import get_mocked_get_json

class TickerTesting(unittest.TestCase):

    def test_invalid_ticker(self):
        invalid = yf.Ticker('InvalidTickerName')
        with self.assertRaises(ValueError):
            invalid.earnings

        invalid = yf.Ticker('') #test empty
        with self.assertRaises(HTTPError):
            invalid.earnings

        invalid = yf.Ticker('LEHLQ') #test debunct
        with self.assertRaises(KeyError):
            invalid.earnings

        invalid = yf.Ticker('GOOE') #test misspelled (like GOOE when we want GOOG)
        with self.assertRaises(KeyError):
            invalid.earnings

        invalid = yf.Ticker(' ') # test white space
        with self.assertRaises(InvalidURL):
            invalid.earnings

        invalid = yf.Ticker('123') # test with numbers
        with self.assertRaises(KeyError):
            invalid.earnings


if __name__ == '__main__':
  unittest.main()
