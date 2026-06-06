"""Tests for yfinance.data internal helpers."""
import unittest
from functools import lru_cache

from yfinance.data import SingletonMeta, YfData, lru_cache_freezeargs
from yfinance.utils import frozendict


class TestFrozenDict(unittest.TestCase):
    def test_is_dict_subclass(self):
        d = frozendict({"a": 1, "b": 2})
        self.assertEqual(d["a"], 1)
        self.assertEqual(d.get("b"), 2)
        self.assertEqual(set(d.keys()), {"a", "b"})
        self.assertIsInstance(d, dict)

    def test_is_hashable(self):
        d1 = frozendict({"a": 1, "b": 2})
        d2 = frozendict({"b": 2, "a": 1})
        self.assertEqual(hash(d1), hash(d2))
        self.assertEqual(len({d1, d2}), 1)

    def test_mutation_raises(self):
        d = frozendict({"a": 1})
        with self.assertRaises(TypeError):
            d["b"] = 2
        with self.assertRaises(TypeError):
            del d["a"]
        with self.assertRaises(AttributeError):
            d.pop("a")
        with self.assertRaises(AttributeError):
            d.clear()
        with self.assertRaises(AttributeError):
            d.update({"x": 9})

    def test_lru_cache_integration(self):
        call_count = {"n": 0}

        @lru_cache_freezeargs
        @lru_cache(maxsize=8)
        def fn(params):
            call_count["n"] += 1
            return sum(params.values())

        self.assertEqual(fn({"a": 1, "b": 2}), 3)
        self.assertEqual(fn({"a": 1, "b": 2}), 3)
        self.assertEqual(call_count["n"], 1)
        self.assertEqual(fn({"a": 1, "b": 3}), 4)
        self.assertEqual(call_count["n"], 2)


class TestYfDataLoginCookies(unittest.TestCase):
    def test_set_login_cookies_invalidates_crumb(self):
        # Logging in (or switching accounts) must drop a crumb minted under a
        # different login state so the next request re-mints a matched crumb.
        # Use a throwaway instance (YfData is a singleton) to avoid leaking the
        # test cookies into the shared session other tests rely on.
        SingletonMeta._instances.pop(YfData, None)
        try:
            data = YfData()
            data._crumb = "stale-crumb-sentinel"
            data.set_login_cookies("T-test-value", "Y-test-value")
            self.assertIsNone(data._crumb)
        finally:
            SingletonMeta._instances.pop(YfData, None)


if __name__ == "__main__":
    unittest.main()
