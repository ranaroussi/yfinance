"""Loader module for price repair tests."""

import unittest


def load_tests(loader, standard_tests, pattern):
    """Load the split price repair test modules."""
    del standard_tests, pattern
    return loader.loadTestsFromNames(
        [
            "tests.price_repair_assumptions_cases",
            "tests.price_repair_cases",
        ]
    )


if __name__ == '__main__':
    unittest.main()
