import threading
import time
import unittest
from datetime import date
from unittest.mock import patch

import pandas as pd

from tests.context import yfinance as yf
from yfinance import multi
from yfinance.exceptions import YFRateLimitError


class TestDownloadThreadSafety(unittest.TestCase):

    def test_concurrent_downloads_return_only_own_tickers(self):
        """Concurrent download() calls must not mix results via shared state."""
        idx = pd.DatetimeIndex(['2024-01-02', '2024-01-03'], tz='America/New_York')
        aapl_df = pd.DataFrame(
            {'Open': [185.0, 186.0], 'Close': [185.5, 186.5]},
            index=idx,
        )
        msft_df = pd.DataFrame(
            {'Open': [375.0, 376.0], 'Close': [375.5, 376.5]},
            index=idx,
        )

        def mock_download_one(ctx, ticker, *args, **kwargs):
            time.sleep(0.05)
            sym = ticker.upper()
            df = aapl_df if sym == 'AAPL' else msft_df
            with ctx.lock:
                ctx.dfs[sym] = df
            return df

        results = {}
        errors = {}

        def do_download(tickers, key):
            try:
                results[key] = yf.download(
                    tickers, threads=False, progress=False,
                )
            except Exception as e:
                errors[key] = e

        with patch('yfinance.multi._download_one', side_effect=mock_download_one), \
             patch('yfinance.multi.YfData'):
            t1 = threading.Thread(target=do_download, args=(['AAPL'], 'aapl'))
            t2 = threading.Thread(target=do_download, args=(['MSFT'], 'msft'))
            t1.start()
            t2.start()
            t1.join(timeout=30)
            t2.join(timeout=30)

        self.assertFalse(errors, f"Download raised: {errors}")

        aapl_tickers = results['aapl'].columns.get_level_values('Ticker').unique().tolist()
        msft_tickers = results['msft'].columns.get_level_values('Ticker').unique().tolist()

        self.assertEqual(aapl_tickers, ['AAPL'])
        self.assertEqual(msft_tickers, ['MSFT'])

    def test_threads_int_caps_concurrency(self):
        """download(threads=N) must run at most N fetches at once."""
        idx = pd.DatetimeIndex(['2024-01-02'], tz='America/New_York')
        lock = threading.Lock()
        state = {'active': 0, 'peak': 0}

        def mock_download_one(ctx, ticker, *args, **kwargs):
            with lock:
                state['active'] += 1
                state['peak'] = max(state['peak'], state['active'])
            time.sleep(0.05)
            with lock:
                state['active'] -= 1
            df = pd.DataFrame({'Close': [1.0]}, index=idx)
            with ctx.lock:
                ctx.dfs[ticker.upper()] = df
            return df

        tickers = [f'T{i}' for i in range(8)]
        with patch('yfinance.multi._download_one', side_effect=mock_download_one), \
             patch('yfinance.multi.YfData'):
            df = yf.download(tickers, threads=2, progress=False)

        self.assertLessEqual(state['peak'], 2)
        self.assertEqual(set(df.columns.get_level_values('Ticker')), set(tickers))

    def test_default_threads_when_cpu_count_unknown(self):
        """threads=True must still work when os.cpu_count() returns None."""
        idx = pd.DatetimeIndex(['2024-01-02'], tz='America/New_York')

        def mock_download_one(ctx, ticker, *args, **kwargs):
            df = pd.DataFrame({'Close': [1.0]}, index=idx)
            with ctx.lock:
                ctx.dfs[ticker.upper()] = df
            return df

        tickers = ['AAPL', 'MSFT', 'NVDA']
        with patch('yfinance.multi._download_one', side_effect=mock_download_one), \
             patch('yfinance.multi.YfData'), \
             patch('yfinance.multi.os.cpu_count', return_value=None):
            df = yf.download(tickers, threads=True, progress=False)

        self.assertEqual(set(df.columns.get_level_values('Ticker')), set(tickers))


class _FakeTickerAttr:
    """Stands in for a Ticker property without network. Caches per Ticker like
    the real one, counts fetches and concurrency, and raises when a symbol's
    value is an exception."""

    def __init__(self, name, values, delay=0.0):
        self.name = name
        self.values = values
        self.delay = delay
        self.calls = []
        self.active = 0
        self.peak = 0
        self.lock = threading.Lock()

    def patch(self):
        return patch.object(yf.Ticker, self.name, property(self._get))

    def _get(self, tkr):
        cache = tkr.__dict__.setdefault('_fake_cache', {})
        if self.name in cache:
            return cache[self.name]
        with self.lock:
            self.calls.append(tkr.ticker)
            self.active += 1
            self.peak = max(self.peak, self.active)
        time.sleep(self.delay)
        with self.lock:
            self.active -= 1
        value = self.values[tkr.ticker]
        if isinstance(value, Exception):
            raise value
        cache[self.name] = value
        return value


class TestTickersAttributes(unittest.TestCase):

    def test_info_is_dict_per_ticker(self):
        fake = _FakeTickerAttr('info', {
            'AAPL': {'symbol': 'AAPL', 'currency': 'USD'},
            'MC.PA': {'symbol': 'MC.PA', 'currency': 'EUR'},
            'BAD': ValueError('boom'),
        })
        with fake.patch(), self.assertLogs('yfinance', level='ERROR') as logs:
            info = yf.Tickers('AAPL MC.PA BAD').info

        self.assertEqual(list(info), ['AAPL', 'MC.PA', 'BAD'])
        self.assertEqual(info['MC.PA']['currency'], 'EUR')
        # A failed ticker keeps the type of Ticker.info
        self.assertEqual(info['BAD'], {})
        self.assertIn("['BAD']: ValueError('boom')", '\n'.join(logs.output))

    def test_calendar_is_one_dataframe(self):
        fake = _FakeTickerAttr('calendar', {
            'AAPL': {'Ex-Dividend Date': date(2026, 8, 10), 'Earnings Average': 1.98},
            'MSFT': {'Ex-Dividend Date': date(2026, 11, 19), 'Earnings Average': 4.72},
            'SPY': {},
        })
        with fake.patch():
            cal = yf.Tickers('AAPL MSFT SPY').calendar

        self.assertIsInstance(cal, pd.DataFrame)
        self.assertEqual(cal.index.name, 'Ticker')
        self.assertEqual(cal.index.tolist(), ['AAPL', 'MSFT', 'SPY'])
        self.assertEqual(cal.loc['MSFT', 'Earnings Average'], 4.72)
        self.assertTrue(cal.loc['SPY'].isna().all())

    def test_values_stay_cached_on_tickers(self):
        fake = _FakeTickerAttr('info', {'AAPL': {'a': 1}, 'MSFT': {'m': 1}})
        with fake.patch():
            tickers = yf.Tickers('AAPL MSFT')
            tickers.info
            tickers.info
            tickers.tickers['AAPL'].info

        self.assertEqual(sorted(fake.calls), ['AAPL', 'MSFT'])

    def test_default_threads_are_capped(self):
        symbols = [f'T{i}' for i in range(3 * multi._ATTR_MAX_THREADS)]
        fake = _FakeTickerAttr('info', {s: {} for s in symbols}, delay=0.05)
        with fake.patch():
            yf.Tickers(symbols).info

        self.assertEqual(fake.peak, multi._ATTR_MAX_THREADS)

    def test_one_thread_runs_serially(self):
        symbols = [f'T{i}' for i in range(4)]
        fake = _FakeTickerAttr('info', {s: {} for s in symbols}, delay=0.02)
        with fake.patch():
            yf.Tickers(symbols).get_info(threads=1)

        self.assertEqual(fake.peak, 1)

    def test_rate_limit_stops_requests_then_retry_fetches_rest(self):
        symbols = [f'T{i}' for i in range(6)]
        values = {s: {'symbol': s} for s in symbols}
        values['T2'] = YFRateLimitError()
        fake = _FakeTickerAttr('info', values)

        with fake.patch():
            tickers = yf.Tickers(symbols)
            with self.assertLogs('yfinance', level='ERROR') as logs:
                info = tickers.get_info(threads=False)

            # No request after Yahoo's refusal
            self.assertEqual(fake.calls, ['T0', 'T1', 'T2'])
            self.assertEqual([s for s in symbols if info[s]], ['T0', 'T1'])
            self.assertIn("['T3', 'T4', 'T5']: Skipped", '\n'.join(logs.output))

            values['T2'] = {'symbol': 'T2'}
            fake.calls.clear()
            info = tickers.get_info(threads=False)

        # The retry only fetches what is missing
        self.assertEqual(fake.calls, ['T2', 'T3', 'T4', 'T5'])
        self.assertTrue(all(info.values()))

    def test_unknown_attribute_raises(self):
        with self.assertRaises(ValueError):
            multi._attr_impl(multi._AttrCtx(), {}, 'history')


if __name__ == '__main__':
    unittest.main()
