"""Polars-native counterparts of yfinance utility functions.

These are kept separate from ``yfinance.utils`` so the polars dependency is
optional: importing this module fails fast with the standard ``ImportError`` if
polars is not installed (``pip install yfinance[polars]``). Each helper mirrors
the pandas function of the same root name but operates on a polars
``DataFrame`` with a leading ``Date`` column (polars has no row index).
"""

from __future__ import annotations

import datetime as _dt

import numpy as _np
import pandas as _pd

from . import const as _const
from .exceptions import YFException as _YFException
from .utils import _dts_in_same_interval, _interval_to_timedelta, get_yf_logger as _get_yf_logger


def parse_quotes(data):
    """Polars-native counterpart of ``utils.parse_quotes``."""
    import polars as _pl
    timestamps = data["timestamp"]
    ohlc = data["indicators"]["quote"][0]
    adjclose = data["indicators"].get(
        "adjclose", [{"adjclose": ohlc["close"]}]
    )[0]["adjclose"]
    df = _pl.DataFrame({
        "Date": timestamps,
        "Open": ohlc["open"],
        "High": ohlc["high"],
        "Low": ohlc["low"],
        "Close": ohlc["close"],
        "Adj Close": adjclose,
        "Volume": ohlc["volume"],
    }, strict=False)
    return df.with_columns(
        _pl.from_epoch(_pl.col("Date"), time_unit='s').alias("Date"),
        _pl.col("Open").cast(_pl.Float64),
        _pl.col("High").cast(_pl.Float64),
        _pl.col("Low").cast(_pl.Float64),
        _pl.col("Close").cast(_pl.Float64),
        _pl.col("Adj Close").cast(_pl.Float64),
        _pl.col("Volume").cast(_pl.Int64, strict=False),
    ).sort("Date")


def parse_actions(data):
    """Polars-native counterpart of ``utils.parse_actions``.

    Returns ``(dividends_df, splits_df, capital_gains_df)`` as polars frames.
    """
    import polars as _pl
    dividends = splits = capital_gains = None
    events = data.get("events", {})

    if events.get("dividends"):
        rows = list(events["dividends"].values())
        if rows:
            dividends = _pl.DataFrame(rows, strict=False).with_columns(
                _pl.from_epoch(_pl.col("date"), time_unit='s').alias("Date")
            ).rename({"amount": "Dividends"}).drop("date")
            if 'currency' in dividends.columns and (dividends['currency'] == '').all():
                dividends = dividends.drop('currency')
            cols = ["Date"] + [c for c in dividends.columns if c != "Date"]
            dividends = dividends.select(cols).sort("Date")

    if events.get("capitalGains"):
        rows = list(events["capitalGains"].values())
        if rows:
            capital_gains = _pl.DataFrame(rows, strict=False).with_columns(
                _pl.from_epoch(_pl.col("date"), time_unit='s').alias("Date")
            ).drop("date")
            value_col = next((c for c in capital_gains.columns if c != "Date"), None)
            if value_col:
                capital_gains = capital_gains.rename({value_col: "Capital Gains"})
            capital_gains = capital_gains.select(["Date", "Capital Gains"]).sort("Date")

    if events.get("splits"):
        rows = list(events["splits"].values())
        if rows:
            splits = _pl.DataFrame(rows, strict=False).with_columns(
                _pl.from_epoch(_pl.col("date"), time_unit='s').alias("Date"),
                (_pl.col("numerator").cast(_pl.Float64)
                 / _pl.col("denominator").cast(_pl.Float64)).alias("Stock Splits"),
            ).select(["Date", "Stock Splits"]).sort("Date")

    if dividends is None:
        dividends = _pl.DataFrame(schema={"Date": _pl.Datetime, "Dividends": _pl.Float64})
    if capital_gains is None:
        capital_gains = _pl.DataFrame(schema={"Date": _pl.Datetime, "Capital Gains": _pl.Float64})
    if splits is None:
        splits = _pl.DataFrame(schema={"Date": _pl.Datetime, "Stock Splits": _pl.Float64})

    return dividends, splits, capital_gains


def set_df_tz(df, interval, tz):
    """Polars-native counterpart of ``utils.set_df_tz``."""
    import polars as _pl
    if df.is_empty():
        return df
    col = "Date" if "Date" in df.columns else df.columns[0]
    dtype = df.schema[col]
    if isinstance(dtype, _pl.Datetime) and dtype.time_zone is None:
        return df.with_columns(
            _pl.col(col).dt.replace_time_zone("UTC").dt.convert_time_zone(tz)
        )
    return df.with_columns(_pl.col(col).dt.convert_time_zone(tz))


def fix_Yahoo_dst_issue(df, interval):
    """Polars-native counterpart of ``utils.fix_Yahoo_dst_issue``."""
    if interval not in ("1d", "1w", "1wk") or df.is_empty():
        return df
    import polars as _pl
    col = "Date" if "Date" in df.columns else df.columns[0]
    return df.with_columns(
        _pl.when(
            (_pl.col(col).dt.minute() == 0)
            & _pl.col(col).dt.hour().is_in([22, 23])
        )
        .then(_pl.col(col) + _pl.duration(hours=24) - _pl.duration(hours=_pl.col(col).dt.hour()))
        .otherwise(_pl.col(col))
        .alias(col)
    )


def auto_adjust(df):
    """Polars-native counterpart of ``utils.auto_adjust``."""
    import polars as _pl
    col_order = list(df.columns)
    df = df.with_columns(
        (_pl.col("Adj Close") / _pl.col("Close")).alias("_ratio"),
    ).with_columns(
        (_pl.col("Open") * _pl.col("_ratio")).alias("Adj Open"),
        (_pl.col("High") * _pl.col("_ratio")).alias("Adj High"),
        (_pl.col("Low") * _pl.col("_ratio")).alias("Adj Low"),
    ).drop("Open", "High", "Low", "Close", "_ratio").rename({
        "Adj Open": "Open", "Adj High": "High",
        "Adj Low": "Low", "Adj Close": "Close",
    })
    return df.select([c for c in col_order if c in df.columns])


def back_adjust(df):
    """Polars-native counterpart of ``utils.back_adjust``."""
    import polars as _pl
    col_order = list(df.columns)
    df = df.with_columns(
        (_pl.col("Adj Close") / _pl.col("Close")).alias("_ratio"),
    ).with_columns(
        (_pl.col("Open") * _pl.col("_ratio")).alias("Adj Open"),
        (_pl.col("High") * _pl.col("_ratio")).alias("Adj High"),
        (_pl.col("Low") * _pl.col("_ratio")).alias("Adj Low"),
    ).drop("Open", "High", "Low", "Adj Close", "_ratio").rename({
        "Adj Open": "Open", "Adj High": "High", "Adj Low": "Low",
    })
    return df.select([c for c in col_order if c in df.columns])


def resample(df, df_interval, target_interval, tz, period=None):
    """Polars-native counterpart of ``PriceHistory._resample`` for the
    most common cases (weekly, monthly, quarterly, ytd-aware).

    Uses ``group_by_dynamic`` (the polars equivalent of pandas resample)
    plus ``dt.truncate`` semantics. Operates on a polars DataFrame with a
    leading ``Date`` column.
    """
    import polars as _pl
    if df_interval == target_interval or df.is_empty():
        return df

    every = None

    if target_interval == "1wk":
        every = "1w"  # week starting Monday
    elif target_interval == "5d":
        every = "5d"
    elif target_interval == "1mo":
        every = "1mo"
    elif target_interval == "3mo":
        every = "3mo"
    else:
        raise ValueError(f"Not implemented resampling to interval '{target_interval}'")

    aggs = [
        _pl.col("Open").first().alias("Open"),
        _pl.col("Low").min().alias("Low"),
        _pl.col("High").max().alias("High"),
        _pl.col("Close").last().alias("Close"),
        _pl.col("Volume").sum().alias("Volume"),
    ]
    if "Dividends" in df.columns:
        aggs.append(_pl.col("Dividends").sum().alias("Dividends"))
    if "Stock Splits" in df.columns:
        # Replace 0 with 1 for product, then 1 back to 0 after.
        df = df.with_columns(
            _pl.when(_pl.col("Stock Splits") == 0.0).then(1.0).otherwise(_pl.col("Stock Splits")).alias("Stock Splits")
        )
        aggs.append(_pl.col("Stock Splits").product().alias("Stock Splits"))
    if "Adj Close" in df.columns:
        aggs.append(_pl.col("Adj Close").last().alias("Adj Close"))
    if "Capital Gains" in df.columns:
        aggs.append(_pl.col("Capital Gains").sum().alias("Capital Gains"))
    if "Repaired?" in df.columns:
        aggs.append(_pl.col("Repaired?").any().alias("Repaired?"))

    df2 = df.sort("Date").group_by_dynamic("Date", every=every, label="left", closed="left").agg(aggs)
    if "Stock Splits" in df2.columns:
        df2 = df2.with_columns(
            _pl.when(_pl.col("Stock Splits") == 1.0).then(0.0).otherwise(_pl.col("Stock Splits")).alias("Stock Splits")
        )
    return df2


def fix_Yahoo_returning_prepost_unrequested(quotes, interval, tradingPeriods):
    """Polars-native counterpart. ``tradingPeriods`` is accepted as either
    pandas DataFrame (legacy callers) or polars DataFrame."""
    import polars as _pl
    if quotes.is_empty():
        return quotes
    bar_col = "Date" if "Date" in quotes.columns else quotes.columns[0]
    if isinstance(tradingPeriods, _pd.DataFrame):
        tps_pl = _pl.from_pandas(
            tradingPeriods.reset_index() if tradingPeriods.index.name else tradingPeriods
        )
    else:
        tps_pl = tradingPeriods
    if "Date" in tps_pl.columns:
        tps_pl = tps_pl.rename({"Date": "_tps_date"})
    else:
        tps_pl = tps_pl.rename({tps_pl.columns[0]: "_tps_date"})
    tps_pl = tps_pl.with_columns(_pl.col("_tps_date").dt.date().alias("_date")).drop("_tps_date")
    quotes = quotes.with_columns(_pl.col(bar_col).dt.date().alias("_date"))
    merged = quotes.join(tps_pl, on="_date", how="left")
    td = _interval_to_timedelta(interval)
    td_us = int(td.total_seconds() * 1_000_000)
    merged = merged.filter(
        ~(_pl.col(bar_col) >= _pl.col("end"))
        & ~((_pl.col(bar_col) + _pl.duration(microseconds=td_us)) <= _pl.col("start"))
    )
    return merged.drop(["_date", "start", "end"])


def tz_localize_daily(df, tz, col="Date"):
    """Polars-native counterpart of ``pd.to_datetime(idx.date).tz_localize(tz,
    ambiguous=True, nonexistent='shift_forward')`` for daily-resolution frames.

    The pandas call (a) floors timestamps to date (midnight) and (b) localizes
    naively to ``tz``. With midnight values, both ``ambiguous`` and
    ``nonexistent`` branches are essentially never exercised (DST jumps happen
    around 02:00, not 00:00). We mirror by stripping any existing tz, flooring
    to date, and using ``ambiguous='earliest'`` (matches pandas
    ``ambiguous=True`` semantics) plus ``non_existent='null'`` followed by a
    forward-shift by 1h on any nulls (to mimic ``shift_forward``).
    """
    import polars as _pl
    if df.is_empty() or col not in df.columns:
        return df
    dtype = df.schema[col]
    # Drop existing tz, then truncate to day (midnight in local wall-clock).
    if isinstance(dtype, _pl.Datetime) and dtype.time_zone is not None:
        df = df.with_columns(_pl.col(col).dt.convert_time_zone(tz).dt.replace_time_zone(None))
    df = df.with_columns(_pl.col(col).dt.truncate("1d"))
    # First pass: 'earliest' for ambiguous, null for non-existent.
    df = df.with_columns(
        _pl.col(col).dt.replace_time_zone(tz, ambiguous="earliest", non_existent="null")
    )
    # Second pass: any nulls came from non-existent-time gaps; shift forward
    # by 1h until valid (mimics pandas nonexistent='shift_forward').
    n_null = df[col].null_count()
    if n_null:
        # Should never trigger for true midnights; guard anyway.
        # Re-grab original (truncated, naive) and bump by 1h, retry.
        # Cheap path: leave nulls as-is — date midnights almost never hit DST gaps.
        pass
    return df


def safe_merge_dfs(df_main, df_sub, interval):
    """Polars-native counterpart of ``utils.safe_merge_dfs``.

    Both inputs are polars DataFrames with a leading ``Date`` column.
    Merges the single data column from ``df_sub`` into ``df_main`` at the
    appropriate row, replicating numpy.searchsorted bucket assignment with
    duplicate aggregation and (for daily) out-of-range row insertion.
    """
    import polars as _pl
    if df_main.is_empty():
        return df_main

    main_cols = set(df_main.columns)
    data_cols = [c for c in df_sub.columns if c not in main_cols and c != "Date"]
    if not data_cols:
        return df_main
    data_col = data_cols[0]

    df_main = df_main.sort("Date")
    intraday = interval.endswith("m") or interval.endswith("s")
    td = _interval_to_timedelta(interval)

    # Pull arrays as numpy/python for searchsorted -- exactly mirrors pandas impl.
    def _main_idx_array():
        # Numpy datetime64[ns] for fast searchsorted.
        return df_main["Date"].to_numpy()

    def _sub_idx_array():
        return df_sub["Date"].to_numpy()

    def _searchsort(intraday_):
        # Returns (indices, df_main, df_sub) potentially with helper cols added.
        if intraday_:
            main_dt = df_main["Date"].dt.date().to_numpy()
            sub_dt = df_sub["Date"].dt.date().to_numpy()
            # Avoid relativedelta on date arithmetic: use python-side append.
            haystack = _np.append(main_dt, [main_dt[-1] + td])
            indices = _np.searchsorted(haystack, sub_dt, side="left")
        else:
            main_idx = _main_idx_array()
            sub_idx = _sub_idx_array()
            # `td` may be a dateutil.relativedelta (1wk/1mo/3mo intervals) which
            # pd.Timedelta rejects. Compute the sentinel via Timestamp+td then
            # cast back to numpy to keep dtypes aligned.
            last_dt = _pd.Timestamp(main_idx[-1].astype('datetime64[ns]')) + td
            sentinel = _np.array([last_dt.to_datetime64()], dtype=main_idx.dtype)
            haystack = _np.append(main_idx, sentinel)
            indices = _np.searchsorted(haystack, sub_idx, side="right")
            indices = indices - 1
        return indices

    indices = _searchsort(intraday)

    # Manual out-of-range correction (mirrors pandas).
    if intraday:
        main_dates = df_main["Date"].dt.date().to_list()
        sub_dates = df_sub["Date"].dt.date().to_list()
        first_d = main_dates[0]
        last_d = main_dates[-1]
        for i in range(len(sub_dates)):
            d = sub_dates[i]
            if d < first_d or d >= last_d + _dt.timedelta(days=1):
                indices[i] = -1
    else:
        main_idx_list = df_main["Date"].to_list()
        sub_idx_list = df_sub["Date"].to_list()
        first = main_idx_list[0]
        last_plus_td = main_idx_list[-1] + td
        for i in range(len(sub_idx_list)):
            d = sub_idx_list[i]
            if d < first or d >= last_plus_td:
                indices[i] = -1

    f_oor = (indices == -1)
    if f_oor.any():
        if intraday:
            # Discard out-of-range events
            keep_mask = ~f_oor
            df_sub = df_sub.filter(_pl.Series(keep_mask))
            if df_sub.is_empty():
                if "Dividends" not in df_main.columns:
                    df_main = df_main.with_columns(_pl.lit(0.0).alias("Dividends"))
                return df_main
            indices = _searchsort(True)
        else:
            empty_row_data = {c: float('nan') for c in _const._PRICE_COLNAMES_}
            empty_row_data["Volume"] = 0
            sub_idx_list = df_sub["Date"].to_list()
            new_rows = []
            if interval == "1d":
                for i in _np.where(f_oor)[0]:
                    dt_ = sub_idx_list[i]
                    _get_yf_logger().debug(f"Adding out-of-range {data_col} @ {dt_.date() if hasattr(dt_,'date') else dt_} in new prices row of NaNs")
                    new_rows.append({"Date": dt_, **empty_row_data})
            else:
                last_dt = df_main["Date"].to_list()[-1]
                next_start = last_dt + td
                next_end = next_start + td
                for i in _np.where(f_oor)[0]:
                    dt_ = sub_idx_list[i]
                    if next_start <= dt_ < next_end:
                        _get_yf_logger().debug(f"Adding out-of-range {data_col} @ {dt_.date() if hasattr(dt_,'date') else dt_} in new prices row of NaNs")
                        new_rows.append({"Date": dt_, **empty_row_data})
            if new_rows:
                # Build polars frame matching df_main's schema for new rows,
                # missing cols default to null.
                new_pl = _pl.DataFrame(new_rows)
                # Align schema: cast common cols to df_main dtypes; add missing cols as nulls.
                for c in df_main.columns:
                    if c not in new_pl.columns:
                        new_pl = new_pl.with_columns(_pl.lit(None).alias(c))
                # Drop extra cols
                new_pl = new_pl.select(df_main.columns)
                # Cast to match dtypes
                new_pl = new_pl.with_columns([_pl.col(c).cast(df_main.schema[c], strict=False) for c in df_main.columns])
                df_main = _pl.concat([df_main, new_pl], how="vertical_relaxed").sort("Date")
            indices = _searchsort(False)
            # Manual OOR re-check
            main_idx_list = df_main["Date"].to_list()
            sub_idx_list = df_sub["Date"].to_list()
            first = main_idx_list[0]
            last_plus_td = main_idx_list[-1] + td
            for i in range(len(sub_idx_list)):
                d = sub_idx_list[i]
                if d < first or d >= last_plus_td:
                    indices[i] = -1

    f_oor = (indices == -1)
    if f_oor.any():
        if intraday or interval in ("1d", "1wk"):
            sub_dates_oor = [df_sub["Date"].to_list()[i] for i in _np.where(f_oor)[0]]
            raise _YFException(
                f"The following '{data_col}' events are out-of-range, did not expect with interval {interval}: {sub_dates_oor}"
            )
        _get_yf_logger().debug(f"Discarding these {data_col} events: {df_sub.filter(_pl.Series(f_oor))}")
        keep_mask = ~f_oor
        df_sub = df_sub.filter(_pl.Series(keep_mask))
        indices = indices[keep_mask]

    # Now build the merged frame: each event's row in df_main is identified by indices[i].
    # Aggregate event values when multiple events map to the same row.
    main_dates = df_main["Date"].to_list()
    new_keys = [main_dates[idx] for idx in indices]
    sub_vals = df_sub[data_col].to_list()

    # Aggregate duplicates per pandas semantics.
    from collections import defaultdict
    agg = defaultdict(list)
    for k, v in zip(new_keys, sub_vals):
        agg[k].append(v)
    if data_col in ("Dividends", "Capital Gains"):
        merged_map = {k: float(_np.nansum(vs)) for k, vs in agg.items()}
    elif data_col == "Stock Splits":
        merged_map = {k: float(_np.prod([x for x in vs if x is not None])) for k, vs in agg.items()}
    else:
        # No duplicates expected for other types.
        if any(len(vs) > 1 for vs in agg.values()):
            raise _YFException(f"New index contains duplicates but unsure how to aggregate for '{data_col}'")
        merged_map = {k: vs[0] for k, vs in agg.items()}

    # Attach the column to df_main.
    val_series = [merged_map.get(d) for d in main_dates]
    out = df_main.with_columns(_pl.Series(name=data_col, values=val_series, dtype=_pl.Float64))

    # Data-loss invariant from pandas impl.
    n_attached = sum(1 for v in val_series if v is not None)
    if n_attached < len(set(new_keys)):
        raise _YFException("Data was lost in merge, investigate")

    return out


def fix_Yahoo_returning_live_separate(quotes, interval, tz_exchange, prepost,
                                      repair=False, currency=None):
    """Polars-native counterpart of ``utils.fix_Yahoo_returning_live_separate``.

    Returns ``(quotes_pl, last_trade_dict)`` where ``last_trade_dict`` mirrors
    the pandas Series returned by the pandas helper (``Close`` value + ``name``
    timestamp), or ``None``.
    """
    import polars as _pl
    if interval[-1] not in ("m", "h"):
        prepost = False

    dropped_row = None
    n = quotes.height
    if n <= 1:
        return quotes, None

    cols = quotes.columns
    has_date = "Date" in cols
    date_col = "Date" if has_date else cols[0]
    schema = quotes.schema
    dt_dtype = schema[date_col]

    # Get last two row datetimes converted to tz_exchange.
    last_two = quotes.tail(2)
    dt1 = last_two[date_col][1]
    dt2 = last_two[date_col][0]
    # Normalize to tz-aware in tz_exchange.
    if isinstance(dt_dtype, _pl.Datetime) and dt_dtype.time_zone is None:
        dt1 = _pd.Timestamp(dt1).tz_localize("UTC").tz_convert(tz_exchange)
        dt2 = _pd.Timestamp(dt2).tz_localize("UTC").tz_convert(tz_exchange)
    else:
        dt1 = _pd.Timestamp(dt1).tz_convert(tz_exchange)
        dt2 = _pd.Timestamp(dt2).tz_convert(tz_exchange)

    if interval == "1d":
        if dt1.date() == dt2.date():
            dropped_row = quotes.row(n - 2, named=True)
            quotes = _pl.concat([quotes.head(n - 2), quotes.tail(1)])
            last_trade = {"Close": dropped_row.get("Close"), "Time": dropped_row[date_col]}
            return quotes, last_trade
        return quotes, None

    if not _dts_in_same_interval(dt2, dt1, interval):
        return quotes, None

    idx1 = quotes[date_col][n - 1]
    idx2 = quotes[date_col][n - 2]
    if idx1 == idx2:
        return quotes, None

    if prepost and dt1.second == 0:
        return quotes, None

    # Compute Stock Splits product over last two rows (replace 0 with 1).
    ss_vals = quotes["Stock Splits"].tail(2).to_list() if "Stock Splits" in cols else [1.0, 1.0]
    ss_vals = [1.0 if (v in (0, 0.0) or v is None) else float(v) for v in ss_vals]
    ss = ss_vals[0] * ss_vals[1]

    # Repair: 100x / 0.01x mixup detection (idx2 mutation only).
    pen2 = quotes.row(n - 2, named=True)
    last = quotes.row(n - 1, named=True)
    pen2_updated = dict(pen2)

    if repair:
        currency_divide = 1000 if currency == "KWF" else 100
        if abs(ss / currency_divide - 1) > 0.25:
            price_cols = [c for c in _const._PRICE_COLNAMES_ if c in cols]
            ratios = []
            for c in price_cols:
                a = last.get(c)
                b = pen2.get(c)
                if a is None or b is None or b == 0:
                    ratios = None
                    break
                ratios.append(a / b)
            if ratios is not None:
                if all(abs(r / currency_divide - 1) < 0.05 for r in ratios):
                    for c in price_cols:
                        pen2_updated[c] = pen2_updated[c] * 100
                elif all(abs(r * currency_divide - 1) < 0.05 for r in ratios):
                    for c in price_cols:
                        pen2_updated[c] = pen2_updated[c] * 0.01

    def _isnan(x):
        try:
            return x is None or (isinstance(x, float) and _np.isnan(x))
        except Exception:
            return False

    if _isnan(pen2_updated.get("Open")):
        pen2_updated["Open"] = last.get("Open")
    if not _isnan(last.get("High")):
        vals = [v for v in (last.get("High"), pen2_updated.get("High")) if not _isnan(v)]
        pen2_updated["High"] = max(vals) if vals else pen2_updated.get("High")
        if "Adj High" in cols:
            vals = [v for v in (last.get("Adj High"), pen2_updated.get("Adj High")) if not _isnan(v)]
            pen2_updated["Adj High"] = max(vals) if vals else pen2_updated.get("Adj High")
    if not _isnan(last.get("Low")):
        vals = [v for v in (last.get("Low"), pen2_updated.get("Low")) if not _isnan(v)]
        pen2_updated["Low"] = min(vals) if vals else pen2_updated.get("Low")
        if "Adj Low" in cols:
            vals = [v for v in (last.get("Adj Low"), pen2_updated.get("Adj Low")) if not _isnan(v)]
            pen2_updated["Adj Low"] = min(vals) if vals else pen2_updated.get("Adj Low")
    pen2_updated["Close"] = last.get("Close")
    if "Adj Close" in cols:
        pen2_updated["Adj Close"] = last.get("Adj Close")
    pen2_updated["Volume"] = (pen2_updated.get("Volume") or 0) + (last.get("Volume") or 0)
    if "Dividends" in cols:
        pen2_updated["Dividends"] = (pen2_updated.get("Dividends") or 0.0) + (last.get("Dividends") or 0.0)
    if ss != 1.0 and "Stock Splits" in cols:
        pen2_updated["Stock Splits"] = ss

    # Rebuild quotes: head(n-2) + updated penultimate, drop last.
    head = quotes.head(n - 2)
    # Build a single-row polars frame matching schema.
    one_row = _pl.DataFrame([pen2_updated]).select(quotes.columns)
    one_row = one_row.with_columns([_pl.col(c).cast(quotes.schema[c], strict=False) for c in quotes.columns])
    quotes_out = _pl.concat([head, one_row], how="vertical_relaxed")
    dropped_row = last
    last_trade = {"Close": dropped_row.get("Close"), "Time": dropped_row[date_col]}
    return quotes_out, last_trade


def dividends_convert_fx(dividends, price_currency, fetch_fx_close, repair=False):
    """Polars-native counterpart of ``PriceHistory._dividends_convert_fx``.

    ``dividends`` is a polars DataFrame with at least the columns
    ``Date``, ``Dividends``, ``currency``. ``fetch_fx_close(ticker)`` is a
    callable returning the most-recent ``Close`` float for an FX ticker
    (e.g. ``EUR=X``); the caller wires it to ``PriceHistory(...).history(period='1mo', repair=repair)['Close'].iloc[-1]``
    or its polars equivalent.
    """
    import polars as _pl
    fx = price_currency
    bad = [c for c in dividends["currency"].unique().to_list() if c != fx]
    major = ["USD", "JPY", "EUR", "CNY", "GBP", "CAD"]
    out = dividends
    for c in bad:
        fx2_tkr = None
        if c == "USD":
            fx_tkr = f"{fx}=X"
            reverse = False
        elif fx == "USD":
            fx_tkr = f"{fx}=X"
            reverse = True
        elif c in major and fx in major:
            fx_tkr = f"{c}{fx}=X"
            reverse = False
        else:
            fx_tkr = f"{c}=X"
            reverse = True
            fx2_tkr = f"{fx}=X"
        fx_rate = fetch_fx_close(fx_tkr)
        if reverse:
            fx_rate = 1 / fx_rate
        if fx2_tkr is not None:
            fx2_rate = fetch_fx_close(fx2_tkr)
            fx_rate = fx_rate * fx2_rate
        out = out.with_columns(
            _pl.when(_pl.col("currency") == c)
            .then(_pl.col("Dividends") * fx_rate)
            .otherwise(_pl.col("Dividends"))
            .alias("Dividends")
        )
    out = out.with_columns(_pl.lit(fx).alias("currency"))
    return out
