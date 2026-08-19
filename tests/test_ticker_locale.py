"""Verify YfConfig.locale switches the lang/region forwarded to Yahoo."""
from tests.context import yfinance as yf

import unittest


class TestTickerLocale(unittest.TestCase):
    def setUp(self):
        self._backup_lang = yf.config.locale.lang
        self._backup_region = yf.config.locale.region

    def tearDown(self):
        yf.config.locale.lang = self._backup_lang
        yf.config.locale.region = self._backup_region

    def test_default_locale_is_en_us(self):
        self.assertEqual(yf.config.locale.lang, "en-US")
        self.assertEqual(yf.config.locale.region, "US")

    def test_default_returns_english(self):
        # Default locale (en-US/US) returns the U.S. English long name.
        self.assertEqual(yf.Ticker("1810.HK").info["longName"], "Xiaomi Corporation")

    def test_locale_zh_hant_hk(self):
        yf.config.locale.lang = "zh-Hant-HK"
        yf.config.locale.region = "HK"
        # 小米集團－Ｗ
        self.assertEqual(yf.Ticker("1810.HK").info["longName"], "小米集團－Ｗ")

    def test_locale_ja_jp(self):
        yf.config.locale.lang = "ja-JP"
        yf.config.locale.region = "JP"
        # トヨタ自動車
        self.assertEqual(yf.Ticker("7203.T").info["longName"], "トヨタ自動車")

    def test_locale_ru_ru(self):
        yf.config.locale.lang = "ru-RU"
        yf.config.locale.region = "RU"
        # Публичное акционерное общество Газпром
        name = yf.Ticker("GAZP.ME").info.get("longName")
        if name is None:
            self.skipTest("GAZP.ME longName unavailable from Yahoo")
        self.assertIn("Газпром", name)  # contains "Газпром"

    def test_locale_us_listing_not_translated(self):
        # Yahoo only translates fields for natively-listed companies.
        # Apple's U.S. listing has no Japanese translation.
        yf.config.locale.lang = "ja-JP"
        yf.config.locale.region = "JP"
        self.assertEqual(yf.Ticker("AAPL").info["longName"], "Apple Inc.")


if __name__ == "__main__":
    unittest.main()
