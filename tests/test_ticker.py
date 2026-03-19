"""Loader module for ticker tests."""

import unittest


def load_tests(loader, standard_tests, pattern):
    """Load the split ticker case modules."""
    del standard_tests, pattern
    return loader.loadTestsFromNames(
        [
            "tests.ticker_core_cases",
            "tests.ticker_financial_cases",
            "tests.ticker_info_cases",
        ]
    )


if __name__ == '__main__':
    unittest.main()
