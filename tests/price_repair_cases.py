"""Case modules for price repair integration tests."""

import datetime as _dt
import os
from typing import cast

import numpy as _np
import pandas as _pd

from tests.price_repair_support import (
    PriceRepairTestCase,
    as_datetime_index,
    call_private,
    to_dataframe,
)


class TestPriceRepair(PriceRepairTestCase):
    """Verify price repair behavior across known data issues."""

    def _assert_random_mixup_case(
        self,
        frame: _pd.DataFrame,
        broken: _pd.DataFrame,
        repair_context,
    ):
        interval, timezone, history = repair_context
        data_columns = ["Low", "High", "Open", "Close", "Adj Close"]
        repaired = to_dataframe(
            call_private(
                history,
                "_fix_unit_random_mixups",
                broken,
                interval,
                timezone,
                prepost=False,
            )
        )
        self.assert_close_columns(repaired, frame, data_columns, rtol=1e-2)
        self.assert_ratio_matches_expected(broken, frame, data_columns)
        self.assert_repaired_flag(repaired)

    def _assert_zero_repair(
        self,
        broken: _pd.DataFrame,
        correct: _pd.DataFrame,
        repair_context,
        validation,
    ):
        interval, timezone, history = repair_context
        columns, rtol = validation
        repaired = to_dataframe(
            call_private(history, "_fix_zeroes", broken, interval, timezone, prepost=False)
        )
        self.assert_close_columns(repaired, correct, columns, rtol=rtol)
        self.assert_repaired_flag(repaired)

    def _assert_block_switch_case(self, ticker_symbol: str, interval: str):
        ticker, timezone, history = self.get_history_parts(ticker_symbol)
        _ = ticker
        data_columns = ["Low", "High", "Open", "Close", "Adj Close"]
        base_name = ticker_symbol.replace('.', '-') + f'-{interval}-100x-error'
        broken_path = os.path.join(self.dp, "data", f"{base_name}.csv")
        if not os.path.isfile(broken_path):
            return
        fixed_path = os.path.join(self.dp, "data", f"{base_name}-fixed.csv")
        broken = self.read_csv_frame(broken_path, index_col="Date", timezone=timezone)
        correct = self.read_csv_frame(fixed_path, index_col="Date", timezone=timezone)
        repaired = to_dataframe(
            call_private(history, "_fix_unit_switch", broken, interval, timezone)
        )
        repaired = repaired.sort_index()
        self.assert_close_columns(
            repaired,
            correct,
            data_columns,
            rtol=1e-2,
        )
        self.assert_ratio_matches_expected(
            broken,
            correct,
            data_columns,
            include_inverse=True,
        )
        self.assert_repaired_flag(repaired)

    def _assert_stock_split_unchanged(self, ticker_symbol: str, interval: str, **history_args):
        ticker, timezone, history = self.get_history_parts(ticker_symbol)
        frame = ticker.history(interval=interval, auto_adjust=False, **history_args).sort_index()
        repaired = to_dataframe(
            call_private(history, "_fix_bad_stock_splits", frame, interval, timezone)
        )
        repaired = repaired.sort_index()
        for column in ["Open", "Low", "High", "Close", "Adj Close", "Volume"]:
            self.assertTrue((repaired[column].to_numpy() == frame[column].to_numpy()).all())

    def _assert_bad_stock_split_case(self, ticker_symbol: str):
        _, timezone, history = self.get_history_parts(ticker_symbol)
        interval = '1d'
        broken_path = os.path.join(
            self.dp,
            "data",
            f"{ticker_symbol.replace('.', '-')}-{interval}-bad-stock-split.csv",
        )
        if not os.path.isfile(broken_path):
            interval = '1wk'
            broken_path = os.path.join(
                self.dp,
                "data",
                f"{ticker_symbol.replace('.', '-')}-{interval}-bad-stock-split.csv",
            )
        broken = self.read_csv_frame(broken_path, index_col="Date")
        repaired = to_dataframe(
            call_private(history, "_fix_bad_stock_splits", broken, "1d", timezone)
        )
        fixed_path = os.path.join(
            self.dp,
            "data",
            f"{ticker_symbol.replace('.', '-')}-{interval}-bad-stock-split-fixed.csv",
        )
        correct = self.read_csv_frame(fixed_path, index_col="Date")
        self.assert_close_columns(
            repaired.sort_index(),
            correct.sort_index(),
            ["Open", "Low", "High", "Close", "Adj Close", "Volume"],
            rtol=5e-6,
        )

    def _history_metadata_parts(self, ticker_symbol: str):
        ticker = self.get_history_parts(ticker_symbol)[0]
        history = call_private(ticker, "_lazy_load_price_history")
        history.history(period='1mo')
        metadata = self.history_metadata(history)
        return history, metadata['currency'], metadata['exchangeTimezoneName']

    def _assert_div_adjust_false_positive(self, ticker_symbol: str, interval: str):
        history, currency, timezone = self._history_metadata_parts(ticker_symbol)
        frame_path = os.path.join(
            self.dp,
            "data",
            f"{ticker_symbol.replace('.', '-')}-{interval}-no-bad-divs.csv",
        )
        if not os.path.isfile(frame_path):
            return
        frame = self.read_csv_frame(frame_path, index_col='Datetime', timezone=timezone)
        repaired = to_dataframe(
            call_private(history, "_fix_bad_div_adjust", frame, interval, currency)
        )
        self.assertTrue(
            _np.isclose(
                repaired['Dividends'].to_numpy(),
                frame['Dividends'].to_numpy(),
                rtol=1e-12,
                equal_nan=True,
            ).all()
        )
        self.assertTrue(
            _np.isclose(
                repaired['Adj Close'].to_numpy(),
                frame['Adj Close'].to_numpy(),
                rtol=1e-12,
                equal_nan=True,
            ).all()
        )

    def _assert_bad_div_adjust_case(self, ticker_symbol: str, interval: str):
        history, currency, timezone = self._history_metadata_parts(ticker_symbol)
        broken_path = os.path.join(
            self.dp,
            "data",
            f"{ticker_symbol.replace('.', '-')}-{interval}-bad-div.csv",
        )
        if not os.path.isfile(broken_path):
            return
        fixed_path = os.path.join(
            self.dp,
            "data",
            f"{ticker_symbol.replace('.', '-')}-{interval}-bad-div-fixed.csv",
        )
        broken = self.read_csv_frame(broken_path, index_col='Datetime', timezone=timezone)
        correct = self.read_csv_frame(fixed_path, index_col='Datetime', timezone=timezone)
        repaired = to_dataframe(
            call_private(history, "_fix_bad_div_adjust", broken, interval, currency)
        )
        self.assertTrue(
            _np.isclose(
                repaired['Dividends'].to_numpy(),
                correct['Dividends'].to_numpy(),
                rtol=1e-12,
                equal_nan=True,
            ).all()
        )
        self.assertTrue(
            _np.isclose(
                repaired['Adj Close'].to_numpy(),
                correct['Adj Close'].to_numpy(),
                rtol=5e-7,
                equal_nan=True,
            ).all()
        )

    def test_types(self):
        """Return dataframes from repair entry points."""
        ticker = self.get_history_parts('INTC')[0]
        data = ticker.history(period="3mo", interval="1d", prepost=True, repair=True)
        self.assertIsInstance(data, _pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        history = call_private(ticker, "_lazy_load_price_history")
        reconstructed = to_dataframe(
            call_private(history, "_reconstruct_intervals_batch", data, "1wk", True)
        )
        self.assertFalse(reconstructed.empty, "data is empty")

    def test_reconstruct_2m(self):
        """Handle 2m repair when the repair path needs 1m source data."""
        end_dt = _pd.Timestamp.now('UTC').ceil("1h")
        start_dt = end_dt - _dt.timedelta(days=60)
        for ticker_symbol in ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]:
            ticker = self.get_history_parts(ticker_symbol)[0]
            ticker.history(start=start_dt, end=end_dt, interval="2m", repair=True)

    def test_repair_100x_random_weekly(self):
        """Repair sporadic 100x errors on weekly data."""
        _, timezone, history = self.get_history_parts("PNL.L")
        frame = _pd.DataFrame(
            data={
                "Open": [470.5, 473.5, 474.5, 470],
                "High": [476, 476.5, 477, 480],
                "Low": [470.5, 470, 465.5, 468.26],
                "Close": [475, 473.5, 472, 473.5],
                "Adj Close": [474.865, 468.6, 467.1, 468.6],
                "Volume": [2295613, 2245604, 3000287, 2635611],
            },
            index=_pd.to_datetime(
                [
                    _dt.date(2022, 10, 24),
                    _dt.date(2022, 10, 17),
                    _dt.date(2022, 10, 10),
                    _dt.date(2022, 10, 3),
                ]
            ),
        ).sort_index()
        frame.index.name = "Date"
        broken = cast(_pd.DataFrame, frame.copy())
        broken.loc["2022-10-24", "Close"] *= 100
        broken.loc["2022-10-17", "Low"] *= 100
        broken.loc["2022-10-03", "Open"] *= 100
        frame.index = as_datetime_index(frame.index).tz_localize(timezone)
        broken.index = as_datetime_index(broken.index).tz_localize(timezone)
        self._assert_random_mixup_case(frame, broken, ("1wk", timezone, history))

    def test_repair_100x_random_weekly_presplit(self):
        """Repair sporadic 100x errors on pre-split weekly data."""
        _, timezone, history = self.get_history_parts("PNL.L")
        frame = cast(
            _pd.DataFrame,
            _pd.DataFrame(
            data={
                "Open": [400, 398, 392.5, 417],
                "High": [421, 425, 419, 420.5],
                "Low": [400, 380.5, 376.5, 396],
                "Close": [410, 409.5, 402, 399],
                "Adj Close": [409.75, 393.43, 386.22, 383.34],
                "Volume": [3232600, 3773900, 10835000, 4257900],
            },
            index=_pd.to_datetime(
                [
                    _dt.date(2020, 3, 30),
                    _dt.date(2020, 3, 23),
                    _dt.date(2020, 3, 16),
                    _dt.date(2020, 3, 9),
                ]
            ),
            ).sort_index(),
        )
        frame_df = _pd.DataFrame(frame.copy())
        data_columns = ["Low", "High", "Open", "Close", "Adj Close"]
        for column in data_columns:
            frame_df.loc[:, column] = frame_df.loc[:, column] * 100.0
        frame_df.loc[:, "Volume"] = frame_df.loc[:, "Volume"] * 0.01
        frame = frame_df
        frame.index.name = "Date"
        broken = cast(_pd.DataFrame, frame.copy())
        broken.loc["2020-03-30", "Close"] *= 100
        broken.loc["2020-03-23", "Low"] *= 100
        broken.loc["2020-03-09", "Open"] *= 100
        frame.index = as_datetime_index(frame.index).tz_localize(timezone)
        broken.index = as_datetime_index(broken.index).tz_localize(timezone)
        self._assert_random_mixup_case(frame, broken, ("1wk", timezone, history))

    def test_repair_100x_random_daily(self):
        """Repair sporadic 100x errors on daily data."""
        _, timezone, history = self.get_history_parts("PNL.L")
        frame = _pd.DataFrame(
            data={
                "Open": [478, 476, 476, 472],
                "High": [478, 477.5, 477, 475],
                "Low": [474.02, 474, 473, 470.75],
                "Close": [475.5, 475.5, 474.5, 475],
                "Adj Close": [475.5, 475.5, 474.5, 475],
                "Volume": [436414, 485947, 358067, 287620],
            },
            index=_pd.to_datetime(
                [
                    _dt.date(2022, 11, 1),
                    _dt.date(2022, 10, 31),
                    _dt.date(2022, 10, 28),
                    _dt.date(2022, 10, 27),
                ]
            ),
        )
        for column in ["Low", "High", "Open", "Close", "Adj Close"]:
            frame[column] = frame[column].astype(float)
        frame = frame.sort_index()
        frame.index.name = "Date"
        broken = cast(_pd.DataFrame, frame.copy())
        broken.loc["2022-11-01", "Close"] *= 100
        broken.loc["2022-10-31", "Low"] *= 100
        broken.loc["2022-10-27", "Open"] *= 100
        frame.index = as_datetime_index(frame.index).tz_localize(timezone)
        broken.index = as_datetime_index(broken.index).tz_localize(timezone)
        self._assert_random_mixup_case(frame, broken, ("1d", timezone, history))

    def test_repair_100x_block_daily(self):
        """Repair sustained 100x unit switch errors."""
        for ticker_symbol in ['AET.L', 'SSW.JO']:
            self._assert_block_switch_case(ticker_symbol, '1d')

    def test_repair_zeroes_daily(self):
        """Repair a daily row of missing OHLCV values."""
        ticker, timezone, history = self.get_history_parts("BBIL.L")
        correct = ticker.history(period='1mo', auto_adjust=False)
        bad_timestamp = correct.index[len(correct) // 2]
        broken = cast(_pd.DataFrame, correct.copy())
        for column in broken.columns:
            broken.loc[bad_timestamp, column] = _np.nan
        self._assert_zero_repair(
            broken,
            correct,
            ("1d", timezone, history),
            (["Open", "Low", "High", "Close"], 1e-7),
        )

    def test_repair_zeroes_daily_adj_close(self):
        """Repair an Adj Close hole around a dividend boundary."""
        self.skipTest(
            "Currently failing because Yahoo returning slightly different data for "
            "interval 1d vs 1h on day Aug 6 2024"
        )
        ticker_symbol = "INTC"
        frame = _pd.DataFrame(
            data={
                "Open": [2.020000e+01, 2.032000e+01, 1.992000e+01, 1.910000e+01, 2.008000e+01],
                "High": [2.039000e+01, 2.063000e+01, 2.025000e+01, 2.055000e+01, 2.015000e+01],
                "Low": [1.929000e+01, 1.975000e+01, 1.895000e+01, 1.884000e+01, 1.950000e+01],
                "Close": [2.011000e+01, 1.983000e+01, 1.899000e+01, 2.049000e+01, 1.971000e+01],
                "Adj Close": [1.998323e+01, 1.970500e+01, 1.899000e+01, 2.049000e+01, 1.971000e+01],
                "Volume": [1.473857e+08, 1.066704e+08, 9.797230e+07, 9.683680e+07, 7.639450e+07],
                "Dividends": [0.0, 0.0, 1.25e-01, 0.0, 0.0],
            },
            index=_pd.to_datetime(
                [
                    _dt.datetime(2024, 8, 9),
                    _dt.datetime(2024, 8, 8),
                    _dt.datetime(2024, 8, 7),
                    _dt.datetime(2024, 8, 6),
                    _dt.datetime(2024, 8, 5),
                ]
            ),
        ).sort_index()
        frame.index.name = "Date"
        _, timezone, history = self.get_history_parts(ticker_symbol)
        frame.index = frame.index.tz_localize(timezone)
        for start in [0, 1, 2]:
            frame_slice = frame.iloc[start:start + 3]
            for bad_row in range(3):
                broken = cast(_pd.DataFrame, frame_slice.copy())
                broken.loc[broken.index[bad_row], "Adj Close"] = 0.0
                self._assert_zero_repair(
                    broken,
                    frame_slice,
                    ("1d", timezone, history),
                    (["Close", "Adj Close"], 5e-3),
                )

    def test_repair_zeroes_hourly(self):
        """Repair a missing hourly OHLCV bar."""
        _, timezone, history = self.get_history_parts("INTC")
        correct = to_dataframe(
            history.history(period="5d", interval="1h", auto_adjust=False, repair=True)
        )
        broken = cast(_pd.DataFrame, correct.copy())
        bad_index = correct.index[10]
        for column in ["Open", "High", "Low", "Close", "Adj Close"]:
            broken.loc[bad_index, column] = _np.nan
        broken.loc[bad_index, "Volume"] = 0
        self._assert_zero_repair(
            broken,
            correct,
            ("1h", timezone, history),
            (["Open", "Low", "High", "Close"], 1e-7),
        )

    def test_repair_bad_stock_splits(self):
        """Repair known bad stock split histories while leaving good cases unchanged."""
        unchanged_tickers = [
            'AMZN',
            'DXCM',
            'FTNT',
            'GOOG',
            'GME',
            'PANW',
            'SHOP',
            'TSLA',
        ]
        unchanged_tickers += ['AEI', 'GHI', 'IRON', 'LXU', 'TISI', 'BOL.ST', 'TUI1.DE']
        for ticker_symbol in unchanged_tickers:
            for interval in ['1d', '1wk', '1mo', '3mo']:
                self._assert_stock_split_unchanged(
                    ticker_symbol,
                    interval,
                    start='2020-01-01',
                    end=_dt.date.today(),
                )

        for ticker_symbol in ['4063.T', 'AV.L', 'CNE.L', 'MOB.ST', 'SPM.MI', 'LA.V']:
            self._assert_bad_stock_split_case(ticker_symbol)

        false_positives = {
            'FIZZ': {'interval': '1d', 'start': '2020-11-30', 'end': '2021-04-01'},
            'GME': {'interval': '1d', 'start': '2007-01-01', 'end': '2023-01-01'},
            'NVDA': {'interval': '1d', 'start': '2001-07-01', 'end': '2007-09-15'},
        }
        for ticker_symbol, history_args in false_positives.items():
            interval = cast(str, history_args['interval'])
            self._assert_stock_split_unchanged(ticker_symbol, interval, **history_args)

    def test_repair_bad_div_adjusts(self):
        """Repair dividend adjustment issues without changing clean histories."""
        interval = '1d'
        false_positives = [
            'CALM',
            'EWG',
            'HSBK.IL',
            'IBE.MC',
            'KMR.L',
            'TISG.MI',
        ]
        for ticker_symbol in false_positives:
            self._assert_div_adjust_false_positive(ticker_symbol, interval)

        broken_tickers = [
            'LSC.L',
            'TEM.L',
            'KME.MI',
            'REL.L',
            '4063.T',
            'ADIG.L',
            'CLC.L',
            'RGL.L',
            'SERE.L',
            'ABDP.L',
            'ELCO.L',
            'PSH.L',
            'SCR.TO',
            'NVT.L',
            '1398.HK',
            '3988.HK',
            'KEN.TA',
            'KAP.IL',
            'TEM.L',
            'TEP.PA',
        ]
        for ticker_symbol in broken_tickers:
            self._assert_bad_div_adjust_case(ticker_symbol, interval)

    def test_repair_capital_gains_double_count(self):
        """Repair histories where capital gains were double counted."""
        for ticker_symbol in ['DODFX', 'VWILX', 'JENYX']:
            _, _, history = self.get_history_parts(ticker_symbol)
            broken_path = os.path.join(
                self.dp,
                "data",
                f"{ticker_symbol.replace('.', '-')}-1d-cg-double-count.csv",
            )
            fixed_path = os.path.join(
                self.dp,
                "data",
                f"{ticker_symbol.replace('.', '-')}-1d-cg-double-count-fixed.csv",
            )
            broken = self.read_csv_frame(broken_path, index_col="Date")
            correct = self.read_csv_frame(fixed_path, index_col="Date")
            repaired = to_dataframe(
                call_private(history, "_repair_capital_gains", broken)
            ).sort_index()
            self.assert_close_columns(
                repaired,
                correct.sort_index(),
                ["Open", "Low", "High", "Close", "Adj Close", "Volume"],
                rtol=5e-6,
            )
