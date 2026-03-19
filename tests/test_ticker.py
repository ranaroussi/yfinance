"""Loader module for ticker tests."""

from tests.ticker_core_cases import TestTickerCore, TestTickerHistoryCases
from tests.ticker_financial_cases import TestTickerFinancialCases
from tests.ticker_info_cases import TestTickerInfoCases

__all__ = [
	"TestTickerCore",
	"TestTickerHistoryCases",
	"TestTickerFinancialCases",
	"TestTickerInfoCases",
]
