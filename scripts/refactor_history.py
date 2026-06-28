"""Refactor yfinance/scrapers/history.py for pylint R0902/R0912/R0915."""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent.parent
HISTORY = ROOT / "yfinance" / "scrapers" / "history.py"

OLD_INIT = """        self._history_cache = {}
        self._history_metadata = None
        self._history_metadata_formatted = False

        self._dividends = None
        self._splits = None
        self._capital_gains = None

        # Limit recursion depth when repairing prices
        self._reconstruct_start_interval = None

        self._last_error = None"""

NEW_INIT = """        self._cache = {
            'history_cache': {},
            'history_metadata': None,
            'history_metadata_formatted': False,
            'dividends': None,
            'splits': None,
            'capital_gains': None,
            'reconstruct_start_interval': None,
            'last_error': None,
        }"""

CACHE_REPLACEMENTS = [
    ("self._history_metadata_formatted", "self._cache['history_metadata_formatted']"),
    ("self._history_metadata", "self._cache['history_metadata']"),
    ("self._history_cache", "self._cache['history_cache']"),
    ("self._reconstruct_start_interval", "self._cache['reconstruct_start_interval']"),
    ("self._capital_gains", "self._cache['capital_gains']"),
    ("self._last_error", "self._cache['last_error']"),
    ("self._splits", "self._cache['splits']"),
    ("self._dividends =", "self._cache['dividends'] ="),
    ("'dividends': self._dividends", "'dividends': self._cache['dividends']"),
]


def apply_cache_conversion(text: str) -> str:
    text = text.replace(OLD_INIT, NEW_INIT)
    for old, new in CACHE_REPLACEMENTS:
        text = text.replace(old, new)
    return text


def extract_method(class_text: str, method_name: str, new_methods: list[str], new_body: str) -> str:
    """Replace method body and insert new helper methods before it."""
    pattern = rf"(    def {re.escape(method_name)}\([^)]*\).*?:\n)(.*?)(?=\n    def |\nclass |\Z)"
    match = re.search(pattern, class_text, re.DOTALL)
    if not match:
        raise ValueError(f"Method not found: {method_name}")
    header = match.group(1)
    insert = "\n".join(new_methods) + "\n\n" if new_methods else ""
    replacement = insert + header + new_body
    return class_text[:match.start()] + replacement + class_text[match.end():]


def main():
    text = HISTORY.read_text(encoding="utf-8")
    text = apply_cache_conversion(text)

    # --- _resample helpers ---
    resample_helpers = '''    def _resample_period_config(self, target_interval, period, df):
        if target_interval == '1wk':
            if period == 'ytd':
                resample_period = '7D'
                origin = pd.Timestamp(f"{_datetime.datetime.now().year}-01-01").tz_localize(df.index.tz)
            else:
                resample_period = 'W-MON'
                origin = 'epoch'
        elif target_interval == '5d':
            resample_period = '5D'
            if period == 'ytd':
                origin = pd.Timestamp(f"{_datetime.datetime.now().year}-01-01").tz_localize(df.index.tz)
            else:
                origin = 'epoch'
        elif target_interval == '1mo':
            resample_period, origin = 'MS', 'epoch'
        elif target_interval == '3mo':
            align_month = 'JAN' if period == 'ytd' else _datetime.datetime.now().strftime('%b').upper()
            resample_period, origin = f"QS-{align_month}", 'epoch'
        else:
            raise ValueError(f"Not implemented resampling to interval '{target_interval}'")
        return resample_period, origin

    def _resample_fill_ohlc(self, df2):
        prev_close = df2['Close'].shift(1).ffill()
        for c in ['Open', 'High', 'Low', 'Close']:
            df2[c] = df2[c].fillna(prev_close)
        return df2'''

    resample_body = '''        if df_interval == target_interval:
            return df
        resample_period, origin = self._resample_period_config(target_interval, period, df)
        resample_map = {
            'Open': 'first', 'Low': 'min', 'High': 'max', 'Close': 'last',
            'Volume': 'sum', 'Dividends': 'sum', 'Stock Splits': 'prod'}
        if 'Repaired?' in df.columns:
            resample_map['Repaired?'] = 'any'
        if 'Adj Close' in df.columns:
            resample_map['Adj Close'] = resample_map['Close']
        if 'Capital Gains' in df.columns:
            resample_map['Capital Gains'] = 'sum'
        df.loc[df['Stock Splits']==0.0, 'Stock Splits'] = 1.0
        if origin != 'epoch':
            df2 = df.resample(resample_period, label='left', closed='left', origin=origin).agg(resample_map)
        else:
            df2 = df.resample(resample_period, label='left', closed='left').agg(resample_map)
        df2.loc[df2['Stock Splits']==1.0, 'Stock Splits'] = 0.0
        return self._resample_fill_ohlc(df2)
'''

    text = extract_method(text, "_resample", [resample_helpers], resample_body)

    # --- _standardise_currency helpers ---
    std_helpers = '''    @staticmethod
    def _subunit_currency_factor(currency):
        factors = {'GBp': ('GBP', 0.01), 'ZAc': ('ZAR', 0.01), 'ILA': ('ILS', 0.01)}
        return factors.get(currency)

    def _prices_already_in_major_units(self, df, m, last_row):
        if last_row.name <= (pd.Timestamp.now('UTC') - _datetime.timedelta(days=30)):
            return False
        try:
            ratio = self._cache['history_metadata']['regularMarketPrice'] / last_row['Close']
            return abs((ratio * m) - 1) < 0.1
        except Exception:
            if not YfConfig.debug.hide_exceptions:
                raise
            return False

    def _convert_subunit_dividends(self, df, m):
        f_div = df['Dividends'] != 0.0
        if not f_div.any():
            return
        divs = df[['Close', 'Dividends']].copy()
        divs['Close'] = divs['Close'].ffill().shift(1, fill_value=divs['Close'].iloc[0])
        divs = divs[f_div]
        div_pcts = (divs['Dividends'] / divs['Close']).to_numpy()
        if len(div_pcts) > 0 and np.average(div_pcts) > 1:
            df['Dividends'] *= m'''

    std_body = '''        factor = self._subunit_currency_factor(currency)
        if factor is None:
            return df, currency
        currency2, m = factor
        f_volume = df['Volume'] > 0
        if not f_volume.any():
            return df, currency
        last_row = df.iloc[np.where(f_volume)[0][-1]]
        if not self._prices_already_in_major_units(df, m, last_row):
            for c in _PRICE_COLNAMES_:
                df[c] *= m
        self._cache['history_metadata']["currency"] = currency2
        self._convert_subunit_dividends(df, m)
        return df, currency2
'''

    text = extract_method(text, "_standardise_currency", [std_helpers], std_body)

    HISTORY.write_text(text, encoding="utf-8")
    print("Phase 1 done: cache + _resample + _standardise_currency")


if __name__ == "__main__":
    main()
