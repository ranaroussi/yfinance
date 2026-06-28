"""Convert FastInfo and Quote cache classes to dicts in quote.py."""
from pathlib import Path

p = Path(__file__).resolve().parent.parent / "yfinance/scrapers/quote.py"
text = p.read_text(encoding="utf-8")

# Remove helper class definitions before FastInfo
start = text.index("class _FastInfoPrices:")
end = text.index("class FastInfo:")
text = text[:start] + text[end:]

# Replace FastInfo __init__ cache setup
old_init = """        self._tkr = tickerBaseObject
        self._prices = _FastInfoPrices()
        self._market = _FastInfoMarket()"""
new_init = """        self._tkr = tickerBaseObject
        self._prices = {k: None for k in (
            'prices_1y', 'prices_1wk_1h_prepost', 'prices_1wk_1h_reg', 'md',
            'today_open', 'today_close', 'today_midnight',
        )}
        self._market = {k: None for k in (
            'currency', 'quote_type', 'exchange', 'timezone', 'shares', 'mcap',
            'open', 'day_high', 'day_low', 'last_price', 'last_volume', 'prev_close',
            'reg_prev_close', 'fifty_day_average', 'two_hundred_day_average',
            'year_high', 'year_low', 'year_change', 'ten_day_average_volume',
            'three_month_average_volume',
        )}"""
text = text.replace(old_init, new_init)

text = text.replace("self._keymaps = _FastInfoKeyMaps(sc_to_cc, cc_to_sc, public_keys, keys)", 
                    "self._keymaps = {'sc_to_cc_key': sc_to_cc, 'cc_to_sc_key': cc_to_sc, 'public_keys': public_keys, 'keys': keys}")

price_keys = ['prices_1y', 'prices_1wk_1h_prepost', 'prices_1wk_1h_reg', 'md',
              'today_open', 'today_close', 'today_midnight']
market_keys = [
    'currency', 'quote_type', 'exchange', 'timezone', 'shares', 'mcap',
    'open', 'day_high', 'day_low', 'last_price', 'last_volume', 'prev_close',
    'reg_prev_close', 'fifty_day_average', 'two_hundred_day_average',
    'year_high', 'year_low', 'year_change', 'ten_day_average_volume',
    'three_month_average_volume',
]
keymap_keys = ['sc_to_cc_key', 'cc_to_sc_key', 'public_keys', 'keys']

for k in price_keys:
    text = text.replace(f"self._prices.{k}", f"self._prices['{k}']")
for k in market_keys:
    text = text.replace(f"self._market.{k}", f"self._market['{k}']")
for k in keymap_keys:
    text = text.replace(f"self._keymaps.{k}", f"self._keymaps['{k}']")

# Quote cache class -> dict
qstart = text.index("class _QuoteCache:")
qend = text.index("class Quote:")
text = text[:qstart] + text[qend:]

text = text.replace(
    "        self._cache = _QuoteCache()",
    "        self._cache = {k: None if k not in ('already_scraped', 'already_fetched', 'already_fetched_complementary') else False for k in (\n"
    "            'info', 'retired_info', 'sustainability', 'recommendations',\n"
    "            'upgrades_downgrades', 'calendar', 'sec_filings', 'valuation_measures',\n"
    "            'already_scraped', 'already_fetched', 'already_fetched_complementary',\n"
    "        )}",
)

quote_keys = [
    'info', 'retired_info', 'sustainability', 'recommendations',
    'upgrades_downgrades', 'calendar', 'sec_filings', 'valuation_measures',
    'already_scraped', 'already_fetched', 'already_fetched_complementary',
]
for k in quote_keys:
    text = text.replace(f"self._cache.{k}", f"self._cache['{k}']")

p.write_text(text, encoding="utf-8")
print("quote.py updated")
