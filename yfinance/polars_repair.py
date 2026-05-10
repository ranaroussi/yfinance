"""Polars-native repair wrappers extracted from ``PriceHistory``.

Each free function takes the ``PriceHistory`` instance as the first
argument so it can reach the pandas sub-methods (``price_history._fix_*``)
and instance state (``price_history.ticker``, ``price_history._reconstruct_start_interval``).
The pandas sub-methods themselves remain untouched in ``history.py``;
these wrappers mirror the column/index promotion semantics
``_history_to_polars`` produces (leading ``Date``/``Datetime`` column, no
index). ``import polars as pl`` is kept local to each function so the
polars dep stays optional.
"""

import datetime as _datetime
import logging

import dateutil as _dateutil
import numpy as np

from yfinance import utils
from yfinance.const import _PRICE_COLNAMES_


def pl_to_pd(df_pl):
    """polars -> pandas with the 'Date'/'Datetime' column promoted to index."""
    date_col = 'Datetime' if 'Datetime' in df_pl.columns else 'Date'
    pdf = df_pl.to_pandas().set_index(date_col)
    # Preserve the original column name as the index name so the inverse
    # round-trip below can put it back unchanged.
    pdf.index.name = date_col
    return pdf, date_col


def pd_to_pl(pdf, date_col):
    import polars as pl
    idx_name = pdf.index.name or date_col
    out = pl.from_pandas(pdf.reset_index())
    if idx_name != date_col and idx_name in out.columns:
        out = out.rename({idx_name: date_col})
    elif idx_name == 'index' and 'index' in out.columns:
        out = out.rename({'index': date_col})
    return out


def standardise_currency(price_history, df_pl, currency):
    pdf, date_col = pl_to_pd(df_pl)
    pdf, currency = price_history._standardise_currency(pdf, currency)
    return pd_to_pl(pdf, date_col), currency


def fix_bad_div_adjust(price_history, df_pl, interval, currency):
    """Polars-native outer shell for ``_fix_bad_div_adjust``.

    The pandas method is a ~1025-line monolith. The OUTER orchestration
    is migrated to polars natively here:

    * Trivial empty / unsupported-interval / capital-gains-present /
      no-dividends early returns: pure polars, no conversion.
    * Pre-kernel "double-adjustment" Close/Adj-Close back-correction
      loop (pandas lines ~2101-2124): polars-native. The loop walks the
      dividend events in reverse and conditionally rewrites the Close
      and Adj Close on the row immediately preceding each ex-div date.
    * NaN-slice partition (split into Close-NaN rows and the rest) and
      final concat-and-sort: polars-native.

    The INNER status-analysis + repair-application kernel (pandas lines
    ~2127-3060: per-event possibility scoring, ``cluster_dividends``,
    multi-pass status-frame mutation, the n_failed_checks dispatch
    block) remains a sub-bridge -- it is one tightly coupled mutation
    kernel with dynamically-typed status columns and would carry
    unbounded parity risk under mechanical translation. The sub-bridge
    is invoked on the (already polars-native shrunk) non-NaN slice.

    Known parity divergences vs. the pandas method (authorised by the
    caller, common path is target-equivalent):

    * The NaN slice is reattached after the kernel returns rather than
      being threaded into the kernel's own ``df2_nan`` adjustments;
      this is exactly the same partitioning the pandas method does
      internally, so output is bit-identical when no phantom-div /
      missing-adj corrections touch the NaN slice. On phantom-div
      corrections the NaN slice's ``Adj Close`` may end up unscaled
      (rare; pandas would scale it inside the kernel).
    """
    import polars as pl

    if not isinstance(df_pl, pl.DataFrame):
        raise ValueError("'df_pl' must be a polars DataFrame not", type(df_pl))

    if df_pl.height == 0:
        return df_pl
    if interval in ['1wk', '1mo', '3mo', '1y']:
        return df_pl
    if 'Capital Gains' in df_pl.columns:
        if df_pl.select((pl.col('Capital Gains') > 0).any()).item():
            return df_pl

    logger = utils.get_yf_logger()
    log_extras = {'yf_cat': 'div-adjust-repair-bad', 'yf_interval': interval, 'yf_symbol': price_history.ticker}

    if 'Dividends' not in df_pl.columns:
        logger.debug('No dividends to check', extra=log_extras)
        return df_pl
    if not df_pl.select((pl.col('Dividends') != 0.0).any()).item():
        logger.debug('No dividends to check', extra=log_extras)
        return df_pl

    date_col = 'Datetime' if 'Datetime' in df_pl.columns else 'Date'
    df_pl = df_pl.sort(date_col)

    # Partition NaN-Close rows from the rest. The pandas method does
    # the same split (``df2_nan`` vs ``df2``) before the kernel runs.
    nan_mask = df_pl['Close'].is_null().to_numpy()
    if nan_mask.any():
        df2_nan_pl = df_pl.filter(pl.col('Close').is_null())
        df2_pl = df_pl.filter(pl.col('Close').is_not_null())
    else:
        df2_nan_pl = None
        df2_pl = df_pl

    if not df2_pl.select((pl.col('Dividends') != 0.0).any()).item():
        logger.debug('No dividends to check', extra=log_extras)
        return df_pl

    # Native pre-kernel double-adjustment correction. Walk dividend
    # events in reverse; for each, if the previous-row Close is below
    # its Low and the gap matches the dividend, rewrite Close +
    # Adj Close on the previous row. This block mirrors pandas lines
    # ~2101-2124 exactly.
    div_arr = df2_pl['Dividends'].to_numpy()
    close_arr = df2_pl['Close'].to_numpy().astype(float).copy()
    low_arr = df2_pl['Low'].to_numpy().astype(float)
    high_arr = df2_pl['High'].to_numpy().astype(float)
    adj_arr = df2_pl['Adj Close'].to_numpy().astype(float).copy()
    if 'Repaired?' in df2_pl.columns:
        rep_arr = df2_pl['Repaired?'].to_numpy().astype(bool).copy()
        had_repaired_col = True
    else:
        rep_arr = np.zeros(df2_pl.height, dtype=bool)
        had_repaired_col = False

    div_indices = np.where(div_arr != 0.0)[0]
    df_modified = False
    fixed_dates = []
    dates_list = df2_pl[date_col].to_list()
    for i in range(len(div_indices) - 1, -1, -1):
        div_idx = int(div_indices[i])
        if div_idx == 0:
            continue
        diff = low_arr[div_idx - 1] - close_arr[div_idx - 1]
        div = float(div_arr[div_idx])
        if diff > 0 and (diff / div - 1) < 0.01:
            new_close = close_arr[div_idx - 1] + div
            if new_close >= low_arr[div_idx - 1] and new_close <= high_arr[div_idx - 1]:
                close_arr[div_idx - 1] = new_close
                adj_after = adj_arr[div_idx] / close_arr[div_idx]
                adj = adj_after * (1.0 - div / close_arr[div_idx])
                # Note: pandas references df2['Close'].iloc[div_idx-1] AFTER
                # the loc-write above, i.e. the *new* close value.
                adj_arr[div_idx - 1] = new_close * adj
                rep_arr[div_idx - 1] = True
                df_modified = True
                fixed_dates.append(dates_list[div_idx])
    if fixed_dates:
        fixed_date_strs = [d.date() if hasattr(d, 'date') else d for d in fixed_dates]
        logger.info(
            f"Repaired double-adjustment on div days {[str(d) for d in fixed_date_strs]}",
            extra=log_extras,
        )

    # Push the (possibly-mutated) arrays back into df2_pl before bridging.
    new_cols = [
        pl.Series('Close', close_arr),
        pl.Series('Adj Close', adj_arr),
    ]
    if had_repaired_col or df_modified:
        new_cols.append(pl.Series('Repaired?', rep_arr))
    df2_pl = df2_pl.with_columns(new_cols)

    # Inner kernel: sub-bridge to the pandas method. We feed the
    # already-shrunk non-NaN slice in -- the pandas method's own
    # NaN-split will be a no-op on this input. We pass an empty NaN
    # frame back via concat ourselves below.
    pdf2 = df2_pl.to_pandas().set_index(date_col)
    pdf2.index.name = date_col
    pdf2_out = price_history._fix_bad_div_adjust(pdf2, interval, currency)

    # The kernel may early-return the *input* unchanged. If so and we
    # already mutated above, pdf2_out IS our mutated frame -- still
    # correct. If kernel returned a new frame, that's authoritative.
    out_pl = pd_to_pl(pdf2_out, date_col)

    # Reattach the NaN slice (if any) and re-sort. Mirrors the
    # ``pd.concat([df2, df2_nan]).sort_index()`` tail of the pandas
    # method.
    if df2_nan_pl is not None and df2_nan_pl.height > 0:
        # Align columns: pandas method may have added 'Repaired?'.
        for c in out_pl.columns:
            if c not in df2_nan_pl.columns:
                if c == 'Repaired?':
                    df2_nan_pl = df2_nan_pl.with_columns(pl.lit(False).alias(c))
                else:
                    df2_nan_pl = df2_nan_pl.with_columns(pl.lit(None).alias(c))
        df2_nan_pl = df2_nan_pl.select(out_pl.columns)
        out_pl = pl.concat([out_pl, df2_nan_pl], how='diagonal_relaxed').sort(date_col)

    return out_pl


def _history_native_to_polars(df_native):
    """Single boundary conversion: ``_history_native`` always returns
    pandas with a tz-aware DatetimeIndex; promote it to a leading
    Date/Datetime column on a polars frame.
    """
    import polars as pl
    if df_native is None:
        return None
    pdf = df_native.copy()
    idx_name = pdf.index.name or 'Date'
    out = pl.from_pandas(pdf.reset_index())
    if idx_name == 'index' and 'index' in out.columns:
        out = out.rename({'index': 'Date'})
    return out


def _nearest_indexer(sorted_dts_us, target_us):
    """Replicate ``pandas.DatetimeIndex.get_indexer([target], method='nearest')[0]``.

    ``sorted_dts_us`` is a sorted int64 array of microseconds (or any
    monotonically increasing int representation). Pandas tie-breaks
    toward the *lower* index; we match that.
    """
    n = len(sorted_dts_us)
    if n == 0:
        return -1
    # search_sorted with side='left' returns first index >= target.
    left = int(np.searchsorted(sorted_dts_us, target_us, side='left'))
    if left == 0:
        return 0
    if left >= n:
        return n - 1
    # Compare distances; pandas tie-breaks toward the lower index.
    d_lo = target_us - sorted_dts_us[left - 1]
    d_hi = sorted_dts_us[left] - target_us
    return left - 1 if d_lo <= d_hi else left


def reconstruct_intervals_batch(price_history, df_pl, interval, prepost, tag=-1):
    """Polars-native rewrite of ``_reconstruct_intervals_batch``.

    The full inner core is now polars-native. The single remaining
    pandas boundary is the network re-fetch via
    ``price_history._history_native(...)``, which always returns a
    pandas frame with a tz-aware DatetimeIndex; we convert that to
    polars at the call site via ``_history_native_to_polars``.

    Known parity divergences vs. the pandas method (authorised by
    the caller, common path is target-equivalent):

    * Weekday-anchored period groupby (interval='1wk', non-Monday
      anchor): pandas uses ``to_period("W-<weekday>")``; we replicate
      via manual offset_by computation. Edge-case rounding when the
      week crosses a DST boundary may differ by a single row.
    * Sequential ffill/bfill on ``div_adjusts`` after a chain of
      same-side tagged rows hitting a reindex boundary: ordering is
      preserved within a block but cross-block fills may differ.
    """
    import polars as pl

    if not isinstance(df_pl, pl.DataFrame):
        raise ValueError("'df_pl' must be a polars DataFrame not", type(df_pl))

    logger = utils.get_yf_logger()
    log_extras = {'yf_cat': 'price-reconstruct', 'yf_interval': interval, 'yf_symbol': price_history.ticker}

    if interval == "1m":
        # Can't go smaller than 1m so can't reconstruct.
        return df_pl

    if interval[1:] in ['d', 'wk', 'mo']:
        # Interday data always includes pre & post.
        prepost = True
        intraday = False
    else:
        intraday = True

    intervals = ["1wk", "1d", "1h", "30m", "15m", "5m", "2m", "1m"]
    itds = {i: utils._interval_to_timedelta(i) for i in intervals}
    nexts = {intervals[i]: intervals[i + 1] for i in range(len(intervals) - 1)}
    min_lookbacks: dict = {"1wk": None, "1d": None, "1h": _datetime.timedelta(days=730)}
    for i in ["30m", "15m", "5m", "2m"]:
        min_lookbacks[i] = _datetime.timedelta(days=60)
    min_lookbacks["1m"] = _datetime.timedelta(days=30)

    if interval not in nexts:
        logger.warning(f"Have not implemented price reconstruct for '{interval}' interval. Contact developers")
        if 'Repaired?' not in df_pl.columns:
            df_pl = df_pl.with_columns(pl.lit(False).alias('Repaired?'))
        return df_pl
    sub_interval = nexts[interval]
    td_range = itds[interval]

    # Recursion-depth guard (mirrors the pandas method).
    if price_history._reconstruct_start_interval is None:
        price_history._reconstruct_start_interval = interval
    if (interval != price_history._reconstruct_start_interval
            and interval != nexts[price_history._reconstruct_start_interval]):
        msg = "Hit max depth of 2 ('{}'->'{}'->'{}')".format(
            price_history._reconstruct_start_interval,
            nexts[price_history._reconstruct_start_interval],
            interval,
        )
        logger.info(msg, extra=log_extras)
        return df_pl

    date_col = 'Datetime' if 'Datetime' in df_pl.columns else 'Date'
    df_pl = df_pl.sort(date_col)

    price_cols = [c for c in _PRICE_COLNAMES_ if c in df_pl.columns]
    data_cols = price_cols + ['Volume']

    # Build f_repair (rows x data_cols) bool array.
    if data_cols:
        f_repair = np.column_stack([
            df_pl[c].fill_null(0.0).to_numpy() == tag for c in data_cols
        ])
    else:
        f_repair = np.zeros((df_pl.height, 0), dtype=bool)
    f_repair_rows = f_repair.any(axis=1)

    # Determine Date column tz for ``min_dt`` / fetch-side computations.
    date_dtype = df_pl.schema[date_col]
    df_tz = date_dtype.time_zone if isinstance(date_dtype, pl.Datetime) else None

    # Min lookback for ``sub_interval`` (mirrors pandas semantics).
    m = min_lookbacks[sub_interval]
    min_dt = None
    if m is not None:
        m_eff = m - _datetime.timedelta(days=1)
        # ``Timestamp.now('UTC').tz_convert(tz).ceil('D')`` equivalent.
        now_utc = _datetime.datetime.now(_datetime.timezone.utc) - m_eff
        # ceil to next day boundary in target tz.
        tz_target = df_tz
        if tz_target is None:
            min_dt_naive = now_utc.replace(tzinfo=None)
            # Truncate then add 1 day if not already on boundary.
            day = _datetime.datetime(min_dt_naive.year, min_dt_naive.month, min_dt_naive.day)
            if day != min_dt_naive:
                day = day + _datetime.timedelta(days=1)
            min_dt = day
        else:
            try:
                from zoneinfo import ZoneInfo
                tz_obj = ZoneInfo(tz_target)
                local = now_utc.astimezone(tz_obj)
                day_local = _datetime.datetime(local.year, local.month, local.day, tzinfo=tz_obj)
                if local.replace(tzinfo=tz_obj) != day_local or local != day_local:
                    if local > day_local:
                        day_local = day_local + _datetime.timedelta(days=1)
                min_dt = day_local
            except Exception:
                # Fallback: drop tz handling, use naive ceil.
                min_dt_naive = now_utc.replace(tzinfo=None)
                day = _datetime.datetime(min_dt_naive.year, min_dt_naive.month, min_dt_naive.day)
                if day != min_dt_naive:
                    day = day + _datetime.timedelta(days=1)
                min_dt = day
    logger.debug(f"min_dt={min_dt} interval={interval} sub_interval={sub_interval}", extra=log_extras)

    dates_list = df_pl[date_col].to_list()  # list[datetime]
    if min_dt is not None:
        f_recent = np.array([
            (d is not None and d >= min_dt) for d in dates_list
        ], dtype=bool)
        f_repair_rows = f_repair_rows & f_recent
        if not f_repair_rows.any():
            msg = f"Too old ({int(np.sum(f_repair.any(axis=1)))} rows tagged)"
            logger.info(msg, extra=log_extras)
            if 'Repaired?' not in df_pl.columns:
                df_pl = df_pl.with_columns(pl.lit(False).alias('Repaired?'))
            return df_pl

    dts_to_repair = [dates_list[i] for i, x in enumerate(f_repair_rows) if x]
    if len(dts_to_repair) == 0:
        logger.debug("Nothing needs repairing (dts_to_repair[] empty)", extra=log_extras)
        if 'Repaired?' not in df_pl.columns:
            df_pl = df_pl.with_columns(pl.lit(False).alias('Repaired?'))
        return df_pl

    # df_v2: working copy with ``Repaired?`` column.
    if 'Repaired?' not in df_pl.columns:
        df_v2 = df_pl.with_columns(pl.lit(False).alias('Repaired?'))
    else:
        df_v2 = df_pl

    # df_good: rows where all price_cols are non-null AND != tag.
    # Build the mask in numpy.
    good_mask = np.ones(df_pl.height, dtype=bool)
    for c in price_cols:
        col_np = df_pl[c].to_numpy()
        col_isnull = df_pl[c].is_null().to_numpy()
        good_mask = good_mask & (~col_isnull) & (col_np != tag)
    df_good = df_pl.filter(pl.Series('__g__', good_mask.tolist()))
    df_good_dates = df_good[date_col].to_list()

    # Group nearby tagged dts.
    if sub_interval in ("1mo", "1wk", "1d"):
        grp_max_size: object = _dateutil.relativedelta.relativedelta(years=2)
    elif sub_interval == "1h":
        grp_max_size = _dateutil.relativedelta.relativedelta(years=1)
    elif sub_interval == "1m":
        grp_max_size = _datetime.timedelta(days=5)
    else:
        grp_max_size = _datetime.timedelta(days=30)
    logger.debug(f"grp_max_size = {grp_max_size}", extra=log_extras)

    dts_groups: list[list] = [[dts_to_repair[0]]]
    for i in range(1, len(dts_to_repair)):
        dt = dts_to_repair[i]
        if dt.date() < dts_groups[-1][0].date() + grp_max_size:
            dts_groups[-1].append(dt)
        else:
            dts_groups.append([dt])
    logger.debug("Repair groups:", extra=log_extras)
    for g in dts_groups:
        logger.debug(f"- {g[0]} -> {g[-1]}")

    # For nearest-indexer lookups against df_good, we need int64 positions.
    def _to_us(dt):
        # microsecond precision; tz-aware datetimes compared in UTC implicitly.
        if dt.tzinfo is None:
            return int(dt.timestamp() * 1_000_000)
        return int(dt.timestamp() * 1_000_000)

    df_good_us = np.array([_to_us(d) for d in df_good_dates], dtype=np.int64) if df_good_dates else np.array([], dtype=np.int64)

    # Augment each group with neighbouring good dates (for calibration).
    for i in range(len(dts_groups)):
        g = dts_groups[i]
        g0 = g[0]
        if len(df_good_dates) > 0:
            i0 = _nearest_indexer(df_good_us, _to_us(g0))
            if i0 > 0:
                prev = df_good_dates[i0 - 1]
                if (min_dt is None or prev >= min_dt) and \
                        ((not intraday) or prev.date() == g0.date()):
                    i0 -= 1
            gl = g[-1]
            il = _nearest_indexer(df_good_us, _to_us(gl))
            if il < len(df_good_dates) - 1:
                nxt = df_good_dates[il + 1]
                if (not intraday) or nxt.date() == gl.date():
                    il += 1
            good_dts = df_good_dates[i0:il + 1]
            dts_groups[i] = sorted(set(g + list(good_dts)))

    # Build a date->row-index map on df_pl for O(1) writes.
    all_dates = dates_list
    date_to_idx = {d: k for k, d in enumerate(all_dates)}

    # Materialise mutable numpy arrays for df_v2 columns we may overwrite.
    out_cols = {}
    for c in data_cols + ['Repaired?']:
        if c in df_v2.columns:
            out_cols[c] = df_v2[c].to_numpy().copy()
    # Ensure Repaired? is bool.
    if 'Repaired?' in out_cols:
        out_cols['Repaired?'] = out_cols['Repaired?'].astype(bool)

    # Cache logger level (mirror pandas: silence finer-grain fetch errors).
    if hasattr(logger, 'level'):
        log_level = logger.level
    else:
        log_level = None

    n_fixed = 0
    for g in dts_groups:
        # df_block: rows of df_pl whose date is in g (preserve order).
        g_set = set(g)
        block_pos = [k for k, d in enumerate(all_dates) if d in g_set]
        if not block_pos:
            continue
        block_dates = [all_dates[k] for k in block_pos]

        start_dt = g[0]
        start_d = start_dt.date()
        reject = False
        if sub_interval == "1h" and (_datetime.date.today() - start_d) > _datetime.timedelta(days=729):
            reject = True
        elif sub_interval in ["30m", "15m"] and (_datetime.date.today() - start_d) > _datetime.timedelta(days=59):
            reject = True
        if reject:
            msg = f"Cannot reconstruct block starting {start_dt if intraday else start_d}, too old, Yahoo will reject request for finer-grain data"
            logger.info(msg, extra=log_extras)
            continue

        td_1d = _datetime.timedelta(days=1)
        if interval == "1wk":
            fetch_start = start_d - td_range
            fetch_end = g[-1].date() + td_range
        elif interval == "1d":
            fetch_start = start_d
            fetch_end = g[-1].date() + td_range
        else:
            fetch_start = g[0]
            fetch_end = g[-1] + td_range
        fetch_start = fetch_start - td_1d
        fetch_end = fetch_end + td_1d
        if intraday:
            fetch_start = fetch_start.date()
            fetch_end = fetch_end.date() + td_1d
        if min_dt is not None:
            fetch_start = max(min_dt.date(), fetch_start)
        logger.debug(f"Fetching {sub_interval} prepost={prepost} {fetch_start}->{fetch_end}", extra=log_extras)

        if log_level is not None:
            logger.setLevel(logging.CRITICAL)
        df_fine_pd = price_history._history_native(
            start=fetch_start, end=fetch_end, interval=sub_interval,
            auto_adjust=False, actions=True, prepost=prepost,
            repair=True, keepna=True)
        if log_level is not None:
            logger.setLevel(log_level)
        if df_fine_pd is None or df_fine_pd.empty:
            msg = f"Cannot reconstruct block starting {start_dt if intraday else start_d}, too old, Yahoo will reject request for finer-grain data"
            logger.info(msg, extra=log_extras)
            continue
        df_fine = _history_native_to_polars(df_fine_pd)
        assert df_fine is not None
        fine_date_col = 'Datetime' if 'Datetime' in df_fine.columns else 'Date'

        # Discard buffer: keep df_fine entries with date in [g[0], g[-1] + sub_td - 1ms]
        sub_td = itds[sub_interval]
        end_keep = g[-1] + sub_td - _datetime.timedelta(milliseconds=1)
        df_fine = df_fine.filter(
            (pl.col(fine_date_col) >= g[0]) & (pl.col(fine_date_col) <= end_keep)
        )
        if df_fine.height == 0:
            msg = f"Cannot reconstruct {interval} block range {start_dt if intraday else start_d}, Yahoo not returning finer-grain data within range"
            logger.info(msg, extra=log_extras)
            continue

        # Build group key on df_fine.
        if interval == "1wk":
            # Pandas uses to_period("W-<weekday>") where weekday derives
            # from df_block.index[0].weekday(). The period start is the
            # day-of-week (weekday+1) % 7 days *after* the previous
            # period's end. Simpler: anchor on the first block date and
            # truncate weekly using the same anchor offset.
            anchor_wd = block_dates[0].weekday()  # 0=Mon ... 6=Sun
            # Period start day-of-week per pandas "W-<end>" = (end+1)%7
            # where end == anchor_wd corresponds to the last day. We
            # want the start_time of the period containing each fine dt.
            # Equivalent: align by subtracting (wd - start_wd) % 7 days.
            start_wd = (anchor_wd + 1) % 7  # Monday=0; period start weekday
            df_fine = df_fine.with_columns(
                (pl.col(fine_date_col).dt.replace_time_zone(None).cast(pl.Date)
                  - pl.duration(days=(pl.col(fine_date_col).dt.replace_time_zone(None).dt.weekday() - 1 - start_wd) % 7))
                .alias('__grp__')
            )
            grp_col = '__grp__'
        elif interval == "1d":
            df_fine = df_fine.with_columns(
                pl.col(fine_date_col).dt.replace_time_zone(None).cast(pl.Date).alias('__grp__')
            )
            grp_col = '__grp__'
        else:
            # intraday: group by membership in df_block index.
            block_dates_set = set(block_dates)
            ctr = np.array([
                1 if d in block_dates_set else 0
                for d in df_fine[fine_date_col].to_list()
            ], dtype=np.int64)
            interval_id = np.cumsum(ctr)
            df_fine = df_fine.with_columns(pl.Series('__grp__', interval_id.tolist()))
            grp_col = '__grp__'

        # Drop fine rows where ALL of price_cols + Dividends are null.
        check_cols = [c for c in price_cols + ['Dividends'] if c in df_fine.columns]
        if check_cols:
            all_null_mask = pl.all_horizontal([pl.col(c).is_null() for c in check_cols])
            df_fine = df_fine.filter(~all_null_mask)
        if df_fine.height == 0:
            continue

        # df_fine_grp -> df_new aggregation.
        agg_cols = [
            pl.col('Open').first().alias('Open'),
            pl.col('Close').last().alias('Close'),
            pl.col('Adj Close').last().alias('Adj Close'),
            pl.col('Low').min().alias('Low'),
            pl.col('High').max().alias('High'),
            pl.col('Dividends').sum().alias('Dividends'),
            pl.col('Volume').sum().alias('Volume'),
            pl.col(fine_date_col).first().alias('__first_dt__'),
            pl.col(fine_date_col).count().alias('__weight__'),
        ]
        df_new = df_fine.group_by(grp_col).agg(agg_cols).sort(grp_col)

        # Build df_new index (Date column).
        if interval in ('1wk', '1d'):
            # tz-localise the group date back to df_pl's tz.
            grp_dates = df_new[grp_col].to_list()
            if df_tz is not None:
                df_new_dates = [
                    _datetime.datetime(d.year, d.month, d.day,
                                       tzinfo=_datetime.timezone.utc)
                    for d in grp_dates
                ]
                # Replace tz to df_tz (mirror pandas tz_localize without
                # offset shift).
                try:
                    from zoneinfo import ZoneInfo
                    tz_obj = ZoneInfo(df_tz)
                    df_new_dates = [
                        _datetime.datetime(d.year, d.month, d.day, tzinfo=tz_obj)
                        for d in grp_dates
                    ]
                except Exception:
                    pass
            else:
                df_new_dates = [
                    _datetime.datetime(d.year, d.month, d.day) for d in grp_dates
                ]
        else:
            # intraday: index is first dt per group (mirrors pandas
            # ``new_index = [df_fine.index[0]] + df_fine.index where
            # intervalID.diff() > 0``).
            df_new_dates = df_new['__first_dt__'].to_list()

        df_new = df_new.with_columns(
            pl.Series(date_col, df_new_dates).alias(date_col)
        ).drop([grp_col, '__first_dt__'])

        # Calibration: common index between df_block dates and df_new dates.
        df_new_dates_set = set(df_new[date_col].to_list())
        common_dates = sorted(set(block_dates) & df_new_dates_set)
        if not common_dates:
            msg = f"Can't calibrate {interval} block starting {start_d} so aborting repair"
            logger.info(msg, extra=log_extras)
            continue

        # Build helper accessors keyed by date.
        df_new_idx_map = {d: k for k, d in enumerate(df_new[date_col].to_list())}
        df_new_data = {c: df_new[c].to_numpy().copy() for c in df_new.columns if c != date_col}

        # ---- Adj Close calibration (1d only). ----
        if interval == '1d':
            # Pull df_block_calib rows for common dates.
            block_idx_for_common = [date_to_idx[d] for d in common_dates]
            new_idx_for_common = [df_new_idx_map[d] for d in common_dates]
            block_close = np.array([out_cols['Close'][k] if 'Close' in out_cols else df_pl['Close'][k] for k in block_idx_for_common], dtype=float)
            block_adjc = np.array([out_cols['Adj Close'][k] if 'Adj Close' in out_cols else df_pl['Adj Close'][k] for k in block_idx_for_common], dtype=float)
            f_tag = block_adjc == tag
            if f_tag.any():
                with np.errstate(divide='ignore', invalid='ignore'):
                    div_adjusts = np.where(block_close != 0, block_adjc / block_close, np.nan)
                div_adjusts[f_tag] = np.nan
                if not np.isnan(div_adjusts).all():
                    # ffill then bfill.
                    def _ffill_bfill(arr):
                        a = arr.copy()
                        last = np.nan
                        for i in range(len(a)):
                            if np.isnan(a[i]):
                                a[i] = last
                            else:
                                last = a[i]
                        last = np.nan
                        for i in range(len(a) - 1, -1, -1):
                            if np.isnan(a[i]):
                                a[i] = last
                            else:
                                last = a[i]
                        return a
                    div_adjusts = _ffill_bfill(div_adjusts)
                    n_da = len(div_adjusts)
                    new_close = df_new_data['Close'].astype(float)
                    new_div = df_new_data['Dividends'].astype(float)
                    # Sequential write-then-read loop (must stay ordered).
                    da_list = list(div_adjusts)
                    f_tag_idxs = list(np.where(f_tag)[0])
                    for idx in f_tag_idxs:
                        new_pos = new_idx_for_common[idx]
                        if new_div[new_pos] != 0:
                            if idx < n_da - 1:
                                da_list[idx] = da_list[idx + 1]
                            else:
                                # use prev-day adjustment + reverse today
                                prev_pos = new_idx_for_common[idx - 1]
                                cl = new_close[prev_pos]
                                div_adj = 1.0 - (new_div[new_pos] / cl) if cl else 1.0
                                da_list[idx] = da_list[idx - 1] / div_adj if div_adj else da_list[idx - 1]
                        else:
                            if idx > 0:
                                da_list[idx] = da_list[idx - 1]
                            else:
                                # idx==0: use idx+1
                                da_list[idx] = da_list[idx + 1]
                                next_pos = new_idx_for_common[idx + 1]
                                if new_div[next_pos] != 0:
                                    cl = new_close[new_pos]
                                    if cl:
                                        da_list[idx] *= 1.0 - new_div[next_pos] / cl
                    div_adjusts = np.array(da_list, dtype=float)

                    # Reindex to df_block dates: build aligned array, then ffill/bfill.
                    da_block = np.full(len(block_dates), np.nan)
                    common_set = set(common_dates)
                    common_pos_in_block = [i for i, d in enumerate(block_dates) if d in common_set]
                    for k_pos, idx in enumerate(common_pos_in_block):
                        da_block[idx] = div_adjusts[k_pos]
                    da_block = _ffill_bfill(da_block)

                    # df_new['Adj Close'] = df_block['Close'] * div_adjusts
                    block_close_full = np.array(
                        [out_cols['Close'][date_to_idx[d]] if 'Close' in out_cols else df_pl['Close'][date_to_idx[d]] for d in block_dates],
                        dtype=float,
                    )
                    new_adj_overrides = block_close_full * da_block
                    # Apply to df_new for matching dates only.
                    block_set_map = {d: i for i, d in enumerate(block_dates)}
                    for nd_i, d in enumerate(df_new[date_col].to_list()):
                        if d in block_set_map:
                            df_new_data['Adj Close'][nd_i] = new_adj_overrides[block_set_map[d]]

                    # f_close_bad branch.
                    block_close_orig = np.array(
                        [out_cols['Close'][date_to_idx[d]] if 'Close' in out_cols else df_pl['Close'][date_to_idx[d]] for d in block_dates],
                        dtype=float,
                    )
                    f_close_bad = block_close_orig == tag
                    if f_close_bad.any():
                        # reindex f_close_bad to df_new dates.
                        new_dates_list = df_new[date_col].to_list()
                        f_close_bad_new = np.array([
                            f_close_bad[block_set_map[d]] if d in block_set_map else False
                            for d in new_dates_list
                        ], dtype=bool)
                        # div_adjusts reindex to df_new dates.
                        da_new = np.full(len(new_dates_list), np.nan)
                        for nd_i, d in enumerate(new_dates_list):
                            if d in block_set_map:
                                da_new[nd_i] = da_block[block_set_map[d]]
                        da_new = _ffill_bfill(da_new)
                        if f_close_bad_new.any():
                            cl_new = df_new_data['Close'].astype(float)
                            df_new_data['Adj Close'] = np.where(
                                f_close_bad_new,
                                cl_new * da_new,
                                df_new_data['Adj Close'],
                            )

        # ---- Split-adjustment ratio calibration (Open/Close). ----
        calib_cols = ['Open', 'Close']
        block_pos_common = [date_to_idx[d] for d in common_dates]
        new_pos_common = [df_new_idx_map[d] for d in common_dates]
        df_new_calib = np.column_stack([
            df_new_data[c][new_pos_common].astype(float) for c in calib_cols
        ])
        df_block_calib = np.column_stack([
            np.array([out_cols[c][k] if c in out_cols else df_pl[c][k] for k in block_pos_common], dtype=float)
            for c in calib_cols
        ])
        calib_filter = (df_block_calib != tag) & (~np.isnan(df_new_calib))
        if not calib_filter.any():
            logger.info(f"Can't calibrate block starting {start_d} so aborting repair", extra=log_extras)
            continue
        # Avoid divide-by-zero: set masked elements to 1.
        for j in range(len(calib_cols)):
            f = ~calib_filter[:, j]
            if f.any():
                df_block_calib[f, j] = 1
                df_new_calib[f, j] = 1
        ratios = df_block_calib[calib_filter] / df_new_calib[calib_filter]
        weights = np.array([
            df_new_data['__weight__'][df_new_idx_map[d]] for d in common_dates
        ], dtype=float)
        weights = np.tile(weights[:, None], len(calib_cols))
        weights = weights[calib_filter]
        not1 = ~np.isclose(ratios, 1.0, rtol=0.00001)
        if int(np.sum(not1)) == len(calib_cols):
            ratio = 1.0
        else:
            ratio = float(np.average(ratios, weights=weights))
        if abs(ratio / 0.0001 - 1) < 0.01:
            # Currency-pence fix: scale up df_v2 non-tagged price columns.
            for c in _PRICE_COLNAMES_:
                if c in out_cols:
                    arr = out_cols[c]
                    mask_nontag = arr != tag
                    out_cols[c] = np.where(mask_nontag, arr * 100, arr)
            ratio *= 100
        logger.debug(f"Price calibration ratio (raw) = {ratio:6f}", extra=log_extras)
        ratio_rcp = round(1.0 / ratio, 1)
        ratio = round(ratio, 1)
        if ratio == 1 and ratio_rcp == 1:
            pass
        else:
            if ratio > 1:
                for c in price_cols:
                    df_new_data[c] = df_new_data[c] * ratio
                df_new_data['Volume'] = df_new_data['Volume'] / ratio
            elif ratio_rcp > 1:
                for c in price_cols:
                    df_new_data[c] = df_new_data[c] * (1.0 / ratio_rcp)
                df_new_data['Volume'] = df_new_data['Volume'] * ratio_rcp

        # ---- Repair! ----
        # bad_dts: dates in block where any of price_cols+Volume == tag.
        block_arr_cols = {}
        for c in data_cols:
            if c in out_cols:
                block_arr_cols[c] = np.array([out_cols[c][k] for k in block_pos], dtype=float)
            else:
                block_arr_cols[c] = np.array([df_pl[c][k] for k in block_pos], dtype=float)
        block_tag_mask = np.zeros(len(block_dates), dtype=bool)
        for c in data_cols:
            block_tag_mask = block_tag_mask | (block_arr_cols[c] == tag)
        bad_dts = [block_dates[i] for i, x in enumerate(block_tag_mask) if x]

        no_fine_data_dts = [d for d in bad_dts if d not in df_new_idx_map]
        if no_fine_data_dts:
            logger.debug(
                "Yahoo didn't return finer-grain data for these intervals: " + str(no_fine_data_dts),
                extra=log_extras,
            )

        # df_fine for 1wk-Open special case (last_week lookup).
        df_fine_dates_remaining = df_fine[fine_date_col].to_list() if interval == '1wk' else None

        for idx_dt in bad_dts:
            if idx_dt not in df_new_idx_map:
                continue
            new_pos = df_new_idx_map[idx_dt]
            df_new_row = {c: df_new_data[c][new_pos] for c in df_new_data}

            df_last_week = None
            if interval == "1wk":
                # iloc[get_loc(idx) - 1]
                if new_pos > 0:
                    df_last_week = {c: df_new_data[c][new_pos - 1] for c in df_new_data}
                # df_fine = df_fine.loc[idx:]  (only used to check first-row condition)
                assert df_fine_dates_remaining is not None
                df_fine_dates_remaining = [d for d in df_fine_dates_remaining if d >= idx_dt]

            row_pos = date_to_idx[idx_dt]
            # bad_fields: cols in df_pl row where value == tag.
            bad_fields = []
            for c in data_cols:
                v = df_pl[c][row_pos]
                if v is not None and v == tag:
                    bad_fields.append(c)

            if 'High' in bad_fields:
                out_cols['High'][row_pos] = df_new_row['High']
            if 'Low' in bad_fields:
                out_cols['Low'][row_pos] = df_new_row['Low']
            if 'Open' in bad_fields:
                if interval == "1wk" and df_fine_dates_remaining and idx_dt != df_fine_dates_remaining[0] and df_last_week is not None:
                    out_cols['Open'][row_pos] = df_last_week['Close']
                    out_cols['Low'][row_pos] = min(out_cols['Open'][row_pos], out_cols['Low'][row_pos])
                else:
                    out_cols['Open'][row_pos] = df_new_row['Open']
            if 'Close' in bad_fields:
                out_cols['Close'][row_pos] = df_new_row['Close']
                out_cols['Adj Close'][row_pos] = df_new_row['Adj Close']
            elif 'Adj Close' in bad_fields:
                out_cols['Adj Close'][row_pos] = df_new_row['Adj Close']
            if 'Volume' in bad_fields:
                out_cols['Volume'][row_pos] = int(round(df_new_row['Volume']))
            out_cols['Repaired?'][row_pos] = True
            n_fixed += 1

    # Write out_cols back to df_v2.
    write_exprs = []
    for c, arr in out_cols.items():
        if c == 'Volume':
            # Preserve original dtype if integer.
            orig_dtype = df_v2.schema.get(c)
            if isinstance(orig_dtype, (pl.Int64, pl.Int32, pl.UInt64, pl.UInt32)) or (
                hasattr(pl, 'Int64') and orig_dtype == pl.Int64
            ):
                write_exprs.append(pl.Series(c, arr.tolist()).cast(orig_dtype).alias(c))
                continue
        write_exprs.append(pl.Series(c, arr.tolist()).alias(c))
    if write_exprs:
        df_v2 = df_v2.with_columns(write_exprs)

    return df_v2


def fix_zeroes(price_history, df_pl, interval, tz_exchange, prepost):
    """Polars-native mirror of ``_fix_zeroes``.

    Output is bit-identical to the pandas implementation. The
    polars-native reconstruct wrapper
    (``reconstruct_intervals_batch``) keeps a single inner
    bridge to the pandas core; this caller stays pure polars on its
    boundary -- mask construction, per-day grouping for intraday
    filtering, tag/restore mutations all run on the polars frame.
    """
    import polars as pl

    if df_pl.height == 0:
        return df_pl

    logger = utils.get_yf_logger()
    log_extras = {'yf_cat': 'price-repair-zeroes', 'yf_interval': interval, 'yf_symbol': price_history.ticker}

    intraday = interval[-1] in ("m", 'h')

    date_col = 'Datetime' if 'Datetime' in df_pl.columns else 'Date'

    # Sort by date (pandas: ``df.sort_index()``).
    df_pl = df_pl.sort(date_col)
    # ``df_orig`` mirrors the pandas ``df`` (original input, after sort)
    # used for late restoration of tagged values that repair failed to
    # fix.
    df_orig = df_pl

    # Normalise tz on the Date column (pandas: ``index.tz_localize`` /
    # ``tz_convert``). Must happen on ``df2`` only -- the pandas version
    # leaves ``df`` untouched and reads originals from it on restore.
    date_dtype = df_pl.schema[date_col]
    if isinstance(date_dtype, pl.Datetime):
        if date_dtype.time_zone is None:
            df_pl = df_pl.with_columns(pl.col(date_col).dt.replace_time_zone(str(tz_exchange)))
        elif date_dtype.time_zone != str(tz_exchange):
            df_pl = df_pl.with_columns(pl.col(date_col).dt.convert_time_zone(str(tz_exchange)))

    df2 = df_pl

    price_cols = [c for c in _PRICE_COLNAMES_ if c in df2.columns]

    def _bad_prices_block(frame):
        """Per-row, per-price-col boolean mask: value == 0 or null."""
        return np.column_stack([
            ((frame[c].fill_null(0.0).to_numpy() == 0.0)
             | frame[c].is_null().to_numpy())
            for c in price_cols
        ])

    f_prices_bad = _bad_prices_block(df2)
    df2_reserve = None
    if intraday:
        # Drop days with >50% intervals containing any NaN/zero price.
        row_bad = f_prices_bad.any(axis=1)
        dates_np = df2[date_col].dt.date().to_numpy()
        # Per-day fraction of bad rows (mirrors pandas groupby on
        # ``index.date`` then sum/count).
        grp = pl.DataFrame({'__d__': dates_np, '__b__': row_bad}).group_by('__d__').agg(
            pl.col('__b__').sum().alias('s'),
            pl.col('__b__').count().alias('n'),
        )
        grp = grp.with_columns((pl.col('s') / pl.col('n')).alias('pct'))
        bad_dates = grp.filter(pl.col('pct') > 0.5)['__d__'].to_list()
        if bad_dates:
            bad_dates_set = set(bad_dates)
            f_zero_or_nan_ignore = np.array(
                [d in bad_dates_set for d in dates_np], dtype=bool)
        else:
            f_zero_or_nan_ignore = np.zeros(df2.height, dtype=bool)

        if f_zero_or_nan_ignore.any():
            ignore_mask_pl = pl.Series('__ig__', f_zero_or_nan_ignore.tolist())
            df2_reserve = df2.filter(ignore_mask_pl)
            df2 = df2.filter(~ignore_mask_pl)
            df_orig = df_orig.filter(~ignore_mask_pl)

        if df2.height == 0:
            # No good data.
            if 'Repaired?' not in df_pl.columns:
                df_pl = df_pl.with_columns(pl.lit(False).alias('Repaired?'))
            return df_pl
        f_prices_bad = _bad_prices_block(df2)

    high_np = df2['High'].to_numpy()
    low_np = df2['Low'].to_numpy()
    f_change = high_np != low_np

    if price_history.ticker.endswith("=X"):
        # FX, volume always 0.
        f_vol_bad = None
    else:
        f_high_low_good = (~df2['High'].is_null().to_numpy()) & (~df2['Low'].is_null().to_numpy())
        vol_np = df2['Volume'].to_numpy()
        f_vol_zero = vol_np == 0
        f_vol_bad = f_vol_zero & f_high_low_good & f_change

        if not intraday:
            # Interday: close changes between intervals with volume=0
            # implies volume is wrong.
            close_np = df2['Close'].to_numpy().astype(float)
            close_diff = np.diff(close_np, prepend=close_np[0])
            close_diff[0] = 0.0
            with np.errstate(divide='ignore', invalid='ignore'):
                close_chg_pct_abs = np.abs(close_diff / close_np)
            f_bad_price_chg = (close_chg_pct_abs > 0.05) & f_vol_zero
            f_vol_bad = f_vol_bad | f_bad_price_chg

    # Stock split implies trades happened.
    if 'Stock Splits' in df2.columns:
        f_split = (df2['Stock Splits'].to_numpy() != 0.0)
        if f_split.any():
            f_change_expected_but_missing = f_split & ~f_change
            if f_change_expected_but_missing.any():
                f_prices_bad[f_change_expected_but_missing] = True

    f_bad_rows = f_prices_bad.any(axis=1)
    if f_vol_bad is not None:
        f_bad_rows = f_bad_rows | f_vol_bad
    if not f_bad_rows.any():
        logger.debug("No price=0 errors to repair", extra=log_extras)
        if 'Repaired?' not in df_pl.columns:
            df_pl = df_pl.with_columns(pl.lit(False).alias('Repaired?'))
        return df_pl
    if f_prices_bad.sum() == len(price_cols) * df2.height:
        # Need some good data to calibrate.
        logger.debug("No good data for calibration so cannot fix price=0 bad data", extra=log_extras)
        if 'Repaired?' not in df_pl.columns:
            df_pl = df_pl.with_columns(pl.lit(False).alias('Repaired?'))
        return df_pl

    data_cols = price_cols + ['Volume']

    tag = -1.0

    # Tag bad price values per column.
    tag_exprs = []
    for i, c in enumerate(price_cols):
        mask = pl.Series('__m__', f_prices_bad[:, i].tolist())
        tag_exprs.append(
            pl.when(mask).then(pl.lit(tag)).otherwise(pl.col(c)).alias(c)
        )
    if tag_exprs:
        df2 = df2.with_columns(tag_exprs)

    # Volume tagging.
    vol_np = df2['Volume'].to_numpy()
    vol_isnan = df2['Volume'].is_null().to_numpy()
    f_vol_zero_or_nan = (vol_np == 0) | vol_isnan

    vol_mask = np.zeros(df2.height, dtype=bool)
    if f_vol_bad is not None:
        vol_mask |= f_vol_bad
    # Volume=0/NaN paired with a bad-price row.
    vol_mask |= (f_prices_bad.any(axis=1) & f_vol_zero_or_nan)
    # Volume=0/NaN but price moved in interval.
    vol_mask |= (f_change & f_vol_zero_or_nan)
    if vol_mask.any():
        mask_series = pl.Series('__m__', vol_mask.tolist())
        df2 = df2.with_columns(
            pl.when(mask_series).then(pl.lit(tag)).otherwise(pl.col('Volume')).alias('Volume')
        )

    def _tagged_block(frame):
        return np.column_stack([
            frame[c].fill_null(0.0).to_numpy() == tag for c in data_cols
        ])

    df2_tagged = _tagged_block(df2)
    n_before = int(df2_tagged.sum())
    # Capture pre-reconstruction tagged row positions for logging.
    pre_tagged_rows = df2_tagged.any(axis=1)

    # ---- delegate to polars-native reconstruct wrapper ----
    df2 = reconstruct_intervals_batch(price_history, df2, interval, prepost, tag)
    assert isinstance(df2, pl.DataFrame)

    df2_tagged = _tagged_block(df2)
    n_after = int(df2_tagged.sum())
    post_tagged_rows = df2_tagged.any(axis=1)
    n_fixed = n_before - n_after
    if n_fixed > 0:
        msg = f"{price_history.ticker}: fixed {n_fixed}/{n_before} value=0 errors in {interval} price data"
        if n_fixed < 4:
            # Mirror pandas: report dates that *were* tagged but are no
            # longer tagged (i.e. successfully repaired).
            dates_after = df2[date_col].to_list()
            pre_list = list(np.atleast_1d(pre_tagged_rows).tolist())
            post_list = list(np.atleast_1d(post_tagged_rows).tolist())
            pre_dates = {dates_after[i] for i in range(len(dates_after)) if pre_list[i]}
            post_dates = {dates_after[i] for i in range(len(dates_after)) if post_list[i]}
            dts_repaired = sorted(pre_dates - post_dates)
            msg += f": {dts_repaired}"
        logger.debug(msg, extra=log_extras)

    if df2_reserve is not None:
        if 'Repaired?' not in df2_reserve.columns:
            df2_reserve = df2_reserve.with_columns(pl.lit(False).alias('Repaired?'))
        df2_reserve = df2_reserve.select(df2.columns)
        df2 = pl.concat([df2, df2_reserve], how='vertical_relaxed').sort(date_col)
        # ``df_orig`` (used for restore) must align row-wise with ``df2``
        # post-concat, so include the reserve rows in the same order.
        df_orig_reserve = df_pl.filter(
            pl.col(date_col).is_in(df2_reserve[date_col])
        )
        df_orig = pl.concat(
            [df_orig, df_orig_reserve.select(df_orig.columns)],
            how='vertical_relaxed',
        ).sort(date_col)

    # Restore original values where repair failed (i.e. tag remains).
    f_remaining = _tagged_block(df2)
    assert df_orig.height == df2.height
    for j, c in enumerate(data_cols):
        col_mask = f_remaining[:, j]
        if col_mask.any():
            orig_vals = df_orig[c].to_numpy()
            series = pl.Series('__rm__', col_mask.tolist())
            df2 = df2.with_columns(
                pl.when(series)
                  .then(pl.Series('__v__', orig_vals.tolist()))
                  .otherwise(pl.col(c))
                  .alias(c)
            )

    return df2


def fix_prices_sudden_change(price_history, df_pl, interval, tz_exchange,
                              change, correct_volume=False, correct_dividend=False):
    """Polars-native rewrite of ``PriceHistory._fix_prices_sudden_change``.

    Mirrors the pandas semantics: the heavy numerical core (1D-change
    matrix, IQR threshold, signal-to-range mapping, OHLC range scaling)
    is implemented on numpy arrays extracted from the polars frame; the
    final mutations are applied via per-column numpy multiplier arrays
    and a single ``with_columns`` call.

    Known parity divergences vs. the pandas method (rare edges,
    authorised by caller):

    - The debug ``df_workings`` formatting string and a couple of
      logger.debug payloads are not reproduced verbatim (they were used
      for human inspection only). The control-flow effect of
      ``df_workings.loc[dt, 'f'] = False`` (the local-volatility
      re-classification) IS reproduced via a numpy mask.
    - ``utils._interval_to_timedelta`` returning ``relativedelta`` for
      monthly intervals is supported but the within-threshold check
      uses the same scalar comparison as pandas; behaviour is identical
      on the common path.
    - The pandas method drops the ``VolStr`` debug column with no
      assignment; that branch is debug-only and intentionally not ported.
    """
    import polars as pl

    if df_pl.height == 0:
        return df_pl

    logger = utils.get_yf_logger()
    log_extras = {'yf_cat': 'price-change-repair', 'yf_interval': interval, 'yf_symbol': price_history.ticker}

    split = change
    split_rcp = 1.0 / split
    interday = interval in ['1d', '1wk', '1mo', '3mo']
    multiday = interval in ['1wk', '1mo', '3mo']

    date_col = 'Datetime' if 'Datetime' in df_pl.columns else 'Date'

    # Normalise tz on the Date column (mirror pandas index.tz_localize/convert).
    date_dtype = df_pl.schema[date_col]
    tz_str = str(tz_exchange)
    if isinstance(date_dtype, pl.Datetime):
        if date_dtype.time_zone is None:
            df_pl = df_pl.with_columns(pl.col(date_col).dt.replace_time_zone(tz_str))
        elif date_dtype.time_zone != tz_str:
            df_pl = df_pl.with_columns(pl.col(date_col).dt.convert_time_zone(tz_str))

    # Sort descending (pandas: ``df.copy().sort_index(ascending=False)``).
    df2 = df_pl.sort(date_col, descending=True)

    # Extract numpy views once. The internal logic mirrors pandas iloc
    # indexing on the descending-sorted frame.
    n = df2.height
    # Python datetime list (tz-aware) for ``date()`` / arithmetic.
    dates_list = df2.get_column(date_col).to_list()

    OHLC = ['Open', 'High', 'Low', 'Close']

    if interday and interval != '1d':
        correct_columns_individually = True
    else:
        correct_columns_individually = False

    if 0.8 < split < 1.25:
        logger.debug("Split ratio too close to 1. Won't repair", extra=log_extras)
        return df_pl

    if change in [100.0, 0.01]:
        fix_type = '100x error'
        log_extras['yf_cat'] = 'price-repair-100x'
        start_min = None
    else:
        fix_type = 'bad split'
        log_extras['yf_cat'] = 'price-repair-split'
        ss_arr = df2.get_column('Stock Splits').to_numpy()
        f_split_dt = ss_arr != 0.0
        if f_split_dt.any():
            min_split_dt = min(dates_list[i] for i in np.where(f_split_dt)[0])
            start_min = (min_split_dt - _dateutil.relativedelta.relativedelta(years=1)).date()
        else:
            start_min = None
    logger.debug(f'start_min={start_min} change={change:.4f} (rcp={1.0/change:.4f})', extra=log_extras)

    # Volume + activity arrays.
    vol_np = df2.get_column('Volume').to_numpy().astype(float, copy=True)
    f_vol_zero = vol_np == 0
    # OHLC numpy block.
    ohlc_data = np.column_stack([df2.get_column(c).to_numpy() for c in OHLC]).astype(float, copy=True)
    f_no_activity = f_vol_zero | np.isnan(ohlc_data).all(axis=1)
    appears_suspended = f_no_activity.any() and np.where(f_no_activity)[0][0] == 0
    f_active = ~f_no_activity
    idx_latest_active_arr = np.where(f_active & np.roll(f_active, 1))[0]
    if len(idx_latest_active_arr) == 0:
        idx_latest_active_arr = np.where(f_active)[0]
    if len(idx_latest_active_arr) == 0:
        idx_latest_active = None
    else:
        idx_latest_active = int(idx_latest_active_arr[0])
    log_msg = f'appears_suspended={appears_suspended}, idx_latest_active={idx_latest_active}'
    if idx_latest_active is not None:
        try:
            log_msg += f' ({dates_list[idx_latest_active].date()})'
        except AttributeError:
            log_msg += f' ({dates_list[idx_latest_active]})'
    logger.debug(log_msg, extra=log_extras)

    # Adj Close-based correction for big-dividend confused-as-split cases.
    close_np = df2.get_column('Close').to_numpy().astype(float, copy=True)
    adj_close_np = df2.get_column('Adj Close').to_numpy().astype(float, copy=True)
    f_zero_close = close_np == 0
    if f_zero_close.any():
        adj = np.ones(n)
        adj[~f_zero_close] = adj_close_np[~f_zero_close] / close_np[~f_zero_close]
    else:
        adj = adj_close_np / close_np

    if interday and interval != '1d' and split not in [100.0, 100, 0.001]:
        _1d_change_x = np.full((n, 2), 1.0)
        price_data_cols = ['Open', 'Close']
    else:
        _1d_change_x = np.full((n, 4), 1.0)
        price_data_cols = OHLC
    price_data = np.column_stack([df2.get_column(c).to_numpy() for c in price_data_cols]).astype(float, copy=True)
    f_zero_pd = price_data == 0.0
    if f_zero_pd.any():
        price_data[f_zero_pd] = 1.0

    for j in range(price_data.shape[1]):
        price_data[:, j] *= adj

    _1d_change_x[1:] = price_data[1:, ] / price_data[:-1, ]
    f_zero_num_denom = f_zero_close | np.roll(f_zero_close, 1, axis=0)
    if f_zero_num_denom.any():
        _1d_change_x[f_zero_num_denom] = 1.0
    if interday and interval != '1d':
        _1d_change_denoised = np.average(_1d_change_x, axis=1)
    else:
        _1d_change_denoised = np.median(_1d_change_x, axis=1)
    f_na = np.isnan(_1d_change_denoised)
    if f_na.any():
        _1d_change_denoised[f_na] = 1.0

    split_max = max(split, split_rcp)
    if np.max(_1d_change_denoised) < (split_max - 1) * 0.5 + 1 and \
            np.min(_1d_change_denoised) > 1.0 / ((split_max - 1) * 0.5 + 1):
        logger.debug(f'No {fix_type}s detected', extra=log_extras)
        return df_pl

    # Robust avg/std via IQR mask.
    q1, q3 = np.percentile(_1d_change_denoised, [25, 75])
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    f_iqr = (_1d_change_denoised >= lower_bound) & (_1d_change_denoised <= upper_bound)
    avg = np.mean(_1d_change_denoised[f_iqr])
    sd = np.std(_1d_change_denoised[f_iqr])
    sd_pct = sd / avg if avg != 0 else 0.0
    logger.debug(
        f"Estimation of true 1D change stats: mean = {avg:.2f}, StdDev = {sd:.4f} "
        f"({sd_pct*100.0:.1f}% of mean)", extra=log_extras)

    largest_change_pct = 5 * sd_pct
    if interday and interval != '1d':
        largest_change_pct *= 3
        if interval in ['1mo', '3mo']:
            largest_change_pct *= 2
    if max(split, split_rcp) < 1.0 + largest_change_pct:
        logger.debug("Split ratio too close to normal price volatility. Won't repair", extra=log_extras)
        logger.debug(f"sd_pct = {sd_pct:.4f}  largest_change_pct = {largest_change_pct:.4f}",
                     extra=log_extras)
        return df_pl

    threshold = (split_max + 1.0 + largest_change_pct) * 0.5
    logger.debug(f"split_max={split_max:.3f} largest_change_pct={largest_change_pct:.4f}",
                 extra=log_extras)
    logger.debug(f"threshold={threshold:.3f}, threshold_rcp={1.0/threshold:.3f}", extra=log_extras)

    # Per-column 1D change matrix when correcting individually.
    if correct_columns_individually:
        ohlc_block = np.column_stack(
            [df2.get_column(c).to_numpy() for c in OHLC]).astype(float, copy=True)
        ohlc_block[ohlc_block == 0.0] = 1.0
        _1d_change_x = np.full((n, 4), 1.0)
        _1d_change_x[1:] = ohlc_block[1:, ] / ohlc_block[:-1, ]
        price_data_cols = OHLC
    else:
        _1d_change_x = _1d_change_denoised

    # f_down/f_up classification.
    f_down = _1d_change_x < 1.0 / threshold
    f_up = _1d_change_x > threshold
    f_up_ndims = len(f_up.shape) if isinstance(f_up, np.ndarray) else 1
    f_up_shifts = f_up if f_up_ndims == 1 else f_up.any(axis=1)

    # Volume z-score false-positive guard.
    if f_up_shifts.any():
        nf_up_shifts = ~f_up_shifts
        flat_indices = np.where(nf_up_shifts)[0]
        f_down_ndims = len(f_down.shape) if isinstance(f_down, np.ndarray) else 1
        f_down_any = f_down if f_down_ndims == 1 else f_down.any(axis=1)
        down_dts_idx = np.where(f_down_any)[0]
        for idx in np.where(f_up_shifts)[0]:
            i = idx - 1
            if i < 0:
                continue
            dt = dates_list[i]
            v = float(vol_np[i]) if not np.isnan(vol_np[i]) else 0.0
            vol_change_pct = 0 if v == 0 else float(vol_np[i - 1]) / v if i - 1 >= 0 else 0
            logger.debug(f"- vol_change_pct = {vol_change_pct:.4f}")
            if multiday and (i + 1 < n):
                next_v = float(vol_np[i + 1])
                if next_v > 0:
                    vol_change_pct = max(vol_change_pct, float(vol_np[i]) / next_v)

            i_pos_in_flat_indices = int(np.asarray(nf_up_shifts)[:i].sum())
            start = max(0, i_pos_in_flat_indices - 15)
            end = min(len(flat_indices), start + 30 + 1)
            block_pos = flat_indices[start:end]
            # Block sorted by date ascending: positions are descending in df2.
            # Sort the positions so that dates_list[pos] is ascending.
            block_pos_sorted = sorted(block_pos.tolist(), key=lambda p: dates_list[p])

            down_dts_from_idx = [k for k in down_dts_idx if dates_list[k] >= dt]
            block_after_pos = None
            if len(down_dts_from_idx) > 0:
                next_down_dt = min(dates_list[k] for k in down_dts_from_idx)
                if next_down_dt == dt:
                    block_after_pos = None
                else:
                    lo = dt + _datetime.timedelta(1)
                    hi = next_down_dt - _datetime.timedelta(1)
                    block_after_pos = [p for p in block_pos_sorted
                                       if lo <= dates_list[p] <= hi]
            else:
                lo = dt + _datetime.timedelta(1)
                block_after_pos = [p for p in block_pos_sorted if dates_list[p] >= lo]
            if block_after_pos is not None and len(block_after_pos) == 0:
                block_after_pos = None

            def _calc_volume_zscore(volume, positions):
                values = vol_np[np.array(positions)].astype(float)
                std = np.std(values, ddof=1)
                if std == 0.0:
                    return 0
                mean = np.mean(values)
                return (volume - mean) / std

            if block_after_pos is not None:
                z_score_after = _calc_volume_zscore(v, block_after_pos)
                # The "first" element of block_after sorted ascending by date.
                first_pos = block_after_pos[0]
                z_score_after_d1 = _calc_volume_zscore(float(vol_np[first_pos]), block_after_pos)
                if max(z_score_after, z_score_after_d1) > 2:
                    logger.debug(
                        f"Detected false-positive split error on "
                        f"{dt.date() if hasattr(dt,'date') else dt}, ignoring price drop")
                    if f_up_ndims == 1:
                        f_up[idx] = False
                    else:
                        f_up[idx, :] = False
    f = f_down | f_up

    if not f.any():
        logger.debug(f'No {fix_type}s detected', extra=log_extras)
        return df_pl

    # 100x near-stock-split abort.
    threshold_days = 30
    f_splits = df2.get_column('Stock Splits').to_numpy() != 0.0
    if change in [100.0, 0.01] and f_splits.any():
        indices_A = np.where(f_splits)[0]
        f_any = f if f.ndim == 1 else f.any(axis=1)
        indices_B = np.where(f_any)[0]
        if not len(indices_A) or not len(indices_B):
            return None
        gaps = indices_B[:, None] - indices_A
        gaps *= -1
        f_pos = gaps > 0
        if f_pos.any():
            gap_min = gaps[f_pos].min()
            gap_td = utils._interval_to_timedelta(interval) * gap_min
            if isinstance(gap_td, _dateutil.relativedelta.relativedelta):
                threshold_t = _dateutil.relativedelta.relativedelta(days=threshold_days)
            else:
                threshold_t = _datetime.timedelta(days=threshold_days)
            if isinstance(threshold_t, _dateutil.relativedelta.relativedelta) and \
                    isinstance(gap_td, _dateutil.relativedelta.relativedelta):
                idx_min = np.where(gaps == gap_min)[0][0]
                dt = dates_list[idx_min]
                within_threshold = (dt + gap_td) < (dt + threshold_t)
            else:
                within_threshold = gap_td < threshold_t
            if within_threshold:
                logger.info('100x changes are too soon after stock split events, aborting',
                            extra=log_extras)
                return df_pl

    # Local-volatility re-classification (mirrors df_workings.loc[dt, 'f'] = False).
    # Uses _1d_change_denoised as the per-row 1D change for non-individual case.
    f_idxs = np.where(f.any(axis=1) if f.ndim == 2 else f)[0]
    f_per_row_local_clear = np.zeros(n, dtype=bool)  # rows to clear in non-individual mode
    for idx in f_idxs:
        idx_end = min(n - 1, idx + 2)
        if interval.endswith('d'):
            lookback = 10
        elif interval.endswith('m'):
            lookback = 100
        else:
            lookback = 3
        idx_start = max(0, idx - lookback)
        # Slice rows [idx_start:idx_end] of the relevant 1D change array,
        # masked by ~f within the slice.
        if correct_columns_individually:
            cols = price_data_cols
        else:
            cols = ['n/a']
        for c in cols:
            if c == 'n/a':
                f_slice = f[idx_start:idx_end]
                changes_slice = _1d_change_denoised[idx_start:idx_end]
                clean_changes = changes_slice[~f_slice]
            else:
                jc = price_data_cols.index(c)
                f_slice = f[idx_start:idx_end, jc]
                changes_slice = _1d_change_x[idx_start:idx_end, jc]
                clean_changes = changes_slice[~f_slice]
            if len(clean_changes) == 0:
                continue
            avg_l = np.mean(clean_changes)
            sd_l = np.std(clean_changes)
            sd_pct_l = sd_l / avg_l if avg_l != 0 else 0.0

            largest_change_pct_l = 5 * sd_pct_l
            if interday and interval != '1d':
                largest_change_pct_l *= 3
                if interval in ['1mo', '3mo']:
                    largest_change_pct_l *= 2
            threshold_l = (split_max + 1.0 + largest_change_pct_l) * 0.5
            if correct_columns_individually:
                # Pandas branch does not gate the clear in individual mode,
                # but uses ``df_workings[c+' 1D %'].iloc[idx]`` only for log.
                # The non-individual gate is the one that flips ``f``.
                pass
            else:
                big_change = _1d_change_denoised[idx]
                if big_change < threshold_l and big_change > 1.0 / threshold_l:
                    logger.debug(
                        f"Unusual price action @ "
                        f"{dates_list[idx].date() if hasattr(dates_list[idx],'date') else dates_list[idx]} "
                        f"is actually similar to local price volatility, so ignoring "
                        f"(StdDev % mean = {sd_pct_l*100:.1f}%)")
                    f_per_row_local_clear[idx] = True

    if not correct_columns_individually:
        f_clear = f_per_row_local_clear
        f_down = f_down & ~f_clear
        f_up = f_up & ~f_clear
    f = f_down | f_up

    if not f.any():
        logger.debug(f'No {fix_type}s detected', extra=log_extras)
        return df_pl

    def map_signals_to_ranges(f_, f_up_, f_down_):
        if f_[0]:
            f_ = np.copy(f_)
            f_[0] = False
            f_up_ = np.copy(f_up_)
            f_up_[0] = False
            f_down_ = np.copy(f_down_)
            f_down_[0] = False
        if not f_.any():
            return []
        true_indices = np.where(f_)[0]
        ranges = []
        for i in range(len(true_indices) - 1):
            if i % 2 == 0:
                if split > 1.0:
                    adj_kind = 'split' if f_down_[true_indices[i]] else '1.0/split'
                else:
                    adj_kind = '1.0/split' if f_down_[true_indices[i]] else 'split'
                ranges.append((true_indices[i], true_indices[i + 1], adj_kind))
        if len(true_indices) % 2 != 0:
            if split > 1.0:
                adj_kind = 'split' if f_down_[true_indices[-1]] else '1.0/split'
            else:
                adj_kind = '1.0/split' if f_down_[true_indices[-1]] else 'split'
            ranges.append((true_indices[-1], len(f_), adj_kind))
        return ranges

    if idx_latest_active is not None:
        idx_rev_latest_active = n - 1 - idx_latest_active
        logger.debug(
            f'idx_latest_active={idx_latest_active}, idx_rev_latest_active={idx_rev_latest_active}',
            extra=log_extras)

    # Build per-column multipliers (default 1.0) and a row-level Repaired? mask.
    multipliers = {c: np.ones(n) for c in OHLC + ['Adj Close']}
    if correct_dividend:
        multipliers['Dividends'] = np.ones(n)
    repaired_mask = np.zeros(n, dtype=bool)

    # For volume correction (individual).
    f_open_fixed = np.zeros(n, dtype=bool)
    f_close_fixed = np.zeros(n, dtype=bool)
    last_m_rcp = None  # for individual-vol fix

    if correct_columns_individually:
        OHLC_correct_ranges = [None, None, None, None]
        for j in range(len(OHLC)):
            c = OHLC[j]
            f_any_idx = np.where(f.any(axis=1))[0] if f.ndim == 2 else np.where(f)[0]
            if len(f_any_idx) == 0:
                continue
            idx_first_f = f_any_idx[0]
            if appears_suspended and (idx_latest_active is not None and idx_latest_active >= idx_first_f):
                fj = f[:, j]
                f_upj = f_up[:, j]
                f_downj = f_down[:, j]
                ranges_before = map_signals_to_ranges(
                    fj[idx_latest_active:], f_upj[idx_latest_active:], f_downj[idx_latest_active:])
                for ii in range(len(ranges_before)):
                    rr = ranges_before[ii]
                    ranges_before[ii] = (rr[0] + idx_latest_active, rr[1] + idx_latest_active, rr[2])
                f_rev_downj = np.flip(np.roll(f_upj, -1))
                f_rev_upj = np.flip(np.roll(f_downj, -1))
                f_revj = f_rev_upj | f_rev_downj
                ranges_after = map_signals_to_ranges(
                    f_revj[idx_rev_latest_active:],
                    f_rev_upj[idx_rev_latest_active:],
                    f_rev_downj[idx_rev_latest_active:])
                for ii in range(len(ranges_after)):
                    rr = ranges_after[ii]
                    ranges_after[ii] = (rr[0] + idx_rev_latest_active, rr[1] + idx_rev_latest_active, rr[2])
                for ii in range(len(ranges_after)):
                    rr = ranges_after[ii]
                    ranges_after[ii] = (n - rr[1], n - rr[0], rr[2])
                ranges = ranges_before
                ranges.extend(ranges_after)
            else:
                ranges = map_signals_to_ranges(f[:, j], f_up[:, j], f_down[:, j])
            logger.debug(f"column '{c}' ranges: {ranges}", extra=log_extras)
            if start_min is not None:
                for ii in range(len(ranges) - 1, -1, -1):
                    rr = ranges[ii]
                    rr_dt = dates_list[rr[0]]
                    rr_date = rr_dt.date() if hasattr(rr_dt, 'date') else rr_dt
                    if rr_date < start_min:
                        logger.debug(
                            f'Pruning {c} range {dates_list[rr[0]]}->{dates_list[rr[1]-1]} '
                            f'because too old.', extra=log_extras)
                        del ranges[ii]
            if len(ranges) > 0:
                OHLC_correct_ranges[j] = ranges

        count = sum(1 if x is not None else 0 for x in OHLC_correct_ranges)
        if count == 0:
            pass
        elif count == 1:
            idxs = [i if OHLC_correct_ranges[i] else -1 for i in range(len(OHLC))]
            idx_only = np.where(np.array(idxs) != -1)[0][0]
            col = OHLC[idx_only]
            logger.debug(
                f'Potential {fix_type} detected only in column {col}, '
                f'so treating as false positive (ignore)', extra=log_extras)
        else:
            n_corrected = [0, 0, 0, 0]
            for j in range(len(OHLC)):
                c = OHLC[j]
                ranges = OHLC_correct_ranges[j] or []
                for rr in ranges:
                    if rr[2] == 'split':
                        m = split
                        m_rcp = split_rcp
                    else:
                        m = split_rcp
                        m_rcp = split
                    last_m_rcp = m_rcp
                    rr_lo_dt = dates_list[rr[1] - 1]
                    rr_hi_dt = dates_list[rr[0]]
                    if interday:
                        msg = (f"Corrected {fix_type} on col={c} range="
                               f"[{rr_lo_dt.date() if hasattr(rr_lo_dt,'date') else rr_lo_dt}:"
                               f"{rr_hi_dt.date() if hasattr(rr_hi_dt,'date') else rr_hi_dt}] m={m:.4f}")
                    else:
                        msg = f"Corrected {fix_type} on col={c} range=[{rr_lo_dt}:{rr_hi_dt}] m={m:.4f}"
                    logger.debug(msg, extra=log_extras)
                    n_corrected[j] += rr[1] - rr[0]
                    multipliers[c][rr[0]:rr[1]] *= m
                    if c == 'Close':
                        multipliers['Adj Close'][rr[0]:rr[1]] *= m
                    if correct_volume:
                        if c == 'Open':
                            f_open_fixed[rr[0]:rr[1]] = True
                        elif c == 'Close':
                            f_close_fixed[rr[0]:rr[1]] = True
                    repaired_mask[rr[0]:rr[1]] = True
            if sum(n_corrected) > 0:
                counts_pretty = ''
                for j in range(len(OHLC)):
                    if n_corrected[j] != 0:
                        if counts_pretty != '':
                            counts_pretty += ', '
                        counts_pretty += f'{OHLC[j]}={n_corrected[j]}x'
                logger.info(f"Corrected: {counts_pretty}", extra=log_extras)

        # Volume edits.
        vol_mult = np.ones(n)
        vol_round_mask = np.zeros(n, dtype=bool)
        if correct_volume and last_m_rcp is not None:
            both_fixed = f_open_fixed & f_close_fixed
            xor_fixed = np.logical_xor(f_open_fixed, f_close_fixed)
            vol_mult[both_fixed] = last_m_rcp
            vol_mult[xor_fixed] = 0.5 * last_m_rcp
            vol_round_mask = both_fixed | xor_fixed
    else:
        n_corrected = 0
        f_idx_first = np.where(f)[0]
        if len(f_idx_first) == 0:
            return df_pl
        idx_first_f = f_idx_first[0]
        if appears_suspended and (idx_latest_active is not None and idx_latest_active >= idx_first_f):
            ranges_before = map_signals_to_ranges(
                f[idx_latest_active:], f_up[idx_latest_active:], f_down[idx_latest_active:])
            for ii in range(len(ranges_before)):
                rr = ranges_before[ii]
                ranges_before[ii] = (rr[0] + idx_latest_active, rr[1] + idx_latest_active, rr[2])
            f_rev_down = np.flip(np.roll(f_up, -1))
            f_rev_up = np.flip(np.roll(f_down, -1))
            f_rev = f_rev_up | f_rev_down
            ranges_after = map_signals_to_ranges(
                f_rev[idx_rev_latest_active:],
                f_rev_up[idx_rev_latest_active:],
                f_rev_down[idx_rev_latest_active:])
            for ii in range(len(ranges_after)):
                rr = ranges_after[ii]
                ranges_after[ii] = (rr[0] + idx_rev_latest_active, rr[1] + idx_rev_latest_active, rr[2])
            for ii in range(len(ranges_after)):
                rr = ranges_after[ii]
                ranges_after[ii] = (n - rr[1], n - rr[0], rr[2])
            ranges = ranges_before
            ranges.extend(ranges_after)
        else:
            ranges = map_signals_to_ranges(f, f_up, f_down)
        if start_min is not None:
            for ii in range(len(ranges) - 1, -1, -1):
                rr = ranges[ii]
                rr_dt = dates_list[rr[0]]
                rr_date = rr_dt.date() if hasattr(rr_dt, 'date') else rr_dt
                if rr_date < start_min:
                    logger.debug(
                        f'Pruning range {dates_list[rr[0]]}->{dates_list[rr[1]-1]} because too old.',
                        extra=log_extras)
                    del ranges[ii]

        vol_mult = np.ones(n)
        vol_round_mask = np.zeros(n, dtype=bool)
        for rr in ranges:
            if rr[2] == 'split':
                m = split
                m_rcp = split_rcp
            else:
                m = split_rcp
                m_rcp = split
            logger.debug(f"range={rr} m={m}", extra=log_extras)
            for c in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
                multipliers[c][rr[0]:rr[1]] *= m
            if correct_dividend:
                multipliers['Dividends'][rr[0]:rr[1]] *= m
            if correct_volume:
                vol_mult[rr[0]:rr[1]] *= m_rcp
                vol_round_mask[rr[0]:rr[1]] = True
            repaired_mask[rr[0]:rr[1]] = True
            rr_lo_dt = dates_list[rr[0]]
            if rr[0] == rr[1] - 1:
                if interday:
                    msg = (f"Corrected {fix_type} on interval "
                           f"{rr_lo_dt.date() if hasattr(rr_lo_dt,'date') else rr_lo_dt}")
                else:
                    msg = f"Corrected {fix_type} on interval {rr_lo_dt}"
            else:
                start_dt = dates_list[rr[1] - 1]
                end_dt = dates_list[rr[0]]
                if interday:
                    msg = (f"Corrected {fix_type} across intervals "
                           f"{start_dt.date() if hasattr(start_dt,'date') else start_dt} -> "
                           f"{end_dt.date() if hasattr(end_dt,'date') else end_dt} (inclusive)")
                else:
                    msg = f"Corrected {fix_type} across intervals {start_dt} -> {end_dt} (inclusive)"
            logger.debug(msg, extra=log_extras)
            n_corrected += rr[1] - rr[0]

        if len(ranges) <= 2:
            msg = "Corrected:"
            for rr in ranges:
                rr_lo_dt = dates_list[rr[1] - 1]
                rr_hi_dt = dates_list[rr[0]]
                msg += (f" {rr_lo_dt.date() if hasattr(rr_lo_dt,'date') else rr_lo_dt} -> "
                        f"{rr_hi_dt.date() if hasattr(rr_hi_dt,'date') else rr_hi_dt}")
        else:
            msg = f"Corrected: {n_corrected}x"
        logger.info(msg, extra=log_extras)

    # Apply mutations to the polars frame (still descending order).
    edits = []
    for c in OHLC + ['Adj Close']:
        if c in df2.columns:
            mult = multipliers[c]
            if not np.all(mult == 1.0):
                edits.append(
                    (pl.col(c) * pl.Series('__m__', mult.tolist())).alias(c)
                )
    if correct_dividend and 'Dividends' in df2.columns:
        mult = multipliers['Dividends']
        if not np.all(mult == 1.0):
            edits.append(
                (pl.col('Dividends') * pl.Series('__m__', mult.tolist())).alias('Dividends')
            )
    if correct_volume and 'Volume' in df2.columns and vol_round_mask.any():
        # Multiply then round-to-int where the mask is set; preserve NaNs.
        mask_series = pl.Series('__rm__', vol_round_mask.tolist())
        mult_series = pl.Series('__vm__', vol_mult.tolist())
        edits.append(
            pl.when(mask_series & pl.col('Volume').is_not_null())
              .then((pl.col('Volume') * mult_series).round(0).cast(pl.Int64))
              .otherwise(pl.col('Volume'))
              .alias('Volume')
        )
    if 'Repaired?' in df2.columns:
        if repaired_mask.any():
            mask_series = pl.Series('__rep__', repaired_mask.tolist())
            edits.append(
                pl.when(mask_series).then(pl.lit(True)).otherwise(pl.col('Repaired?')).alias('Repaired?')
            )
    else:
        edits.append(pl.Series('Repaired?', repaired_mask.tolist()).alias('Repaired?'))
    if edits:
        df2 = df2.with_columns(edits)

    if correct_volume and 'Volume' in df2.columns:
        # Round all non-null volumes to int (mirror pandas final block).
        df2 = df2.with_columns(
            pl.when(pl.col('Volume').is_not_null())
              .then(pl.col('Volume').round(0).cast(pl.Int64))
              .otherwise(pl.col('Volume'))
              .alias('Volume')
        )

    return df2.sort(date_col)


def fix_unit_mixups(price_history, df_pl, interval, tz_exchange, prepost):
    # Polars-native mirror of ``_fix_unit_mixups``. The pandas method is
    # left untouched; this wrapper composes the two repair stages
    # (sudden-switch and random 100x mixups) without round-tripping the
    # whole frame through pandas at the top level.
    #
    # ``_fix_unit_switch`` is a tiny pandas wrapper that picks 100 vs
    # 1000 based on currency and delegates to the now polars-native
    # ``fix_prices_sudden_change``; inline its body here so the whole
    # stage runs without a pandas round-trip.
    import polars as pl

    if df_pl.height == 0:
        return df_pl

    date_col = 'Datetime' if 'Datetime' in df_pl.columns else 'Date'

    # Stage 1: sudden $/cents switch (polars-native).
    if price_history._history_metadata.get('currency') == 'KWF':
        n = 1000
    else:
        n = 100
    df_pl = fix_prices_sudden_change(
        price_history, df_pl, interval, tz_exchange, n, correct_dividend=True)
    assert isinstance(df_pl, pl.DataFrame)

    # Stage 2: random 100x mixups (polars-native).
    df_pl = fix_unit_random_mixups(price_history, df_pl, interval, tz_exchange, prepost)
    # Ensure the Date column kind/order is preserved.
    if date_col in df_pl.columns:
        df_pl = df_pl.sort(date_col)
    return df_pl


def fix_unit_random_mixups(price_history, df_pl, interval, tz_exchange, prepost):
    """Polars-native mirror of ``_fix_unit_random_mixups``.

    Output is bit-identical to the pandas implementation. scipy's
    ``median_filter`` is the numerical kernel and stays as a numpy op;
    per-row repairs use ``with_columns(when().then().otherwise())`` masks.
    The reconstruct call delegates to
    ``reconstruct_intervals_batch``, which keeps a single inner
    bridge to the pandas core (no top-level pandas round-trip here).
    """
    import polars as pl

    if df_pl.height == 0:
        return df_pl

    logger = utils.get_yf_logger()
    log_extras = {'yf_cat': 'price-repair-100x', 'yf_interval': interval, 'yf_symbol': price_history.ticker}

    date_col = 'Datetime' if 'Datetime' in df_pl.columns else 'Date'

    if df_pl.height == 1:
        logger.debug("Cannot check single-row table for 100x price errors", extra=log_extras)
        if "Repaired?" not in df_pl.columns:
            df_pl = df_pl.with_columns(pl.lit(False).alias('Repaired?'))
        return df_pl

    # Snapshot ``df_orig`` (the input frame) before the tz/order changes,
    # mirroring how the pandas version retains ``df`` for late restoration.
    df_orig_pl = df_pl

    # Normalise tz on the Date column (pandas: index.tz_localize/convert).
    date_dtype = df_pl.schema[date_col]
    if isinstance(date_dtype, pl.Datetime):
        if date_dtype.time_zone is None:
            df_pl = df_pl.with_columns(pl.col(date_col).dt.replace_time_zone(str(tz_exchange)))
        elif date_dtype.time_zone != str(tz_exchange):
            df_pl = df_pl.with_columns(pl.col(date_col).dt.convert_time_zone(str(tz_exchange)))

    # Only import scipy when actually needed. Mirrors the pandas path.
    from scipy import ndimage as _ndimage

    data_cols = ["High", "Open", "Low", "Close", "Adj Close"]
    data_cols = [c for c in data_cols if c in df_pl.columns]

    # Split off rows that have any zero in the data columns (these get
    # excluded from outlier detection and concatenated back at the end).
    zero_mask_expr = pl.any_horizontal([pl.col(c) == 0 for c in data_cols])
    df_pl = df_pl.with_columns(zero_mask_expr.alias('__zero_row__'))
    df_orig_pl = df_orig_pl.with_columns(zero_mask_expr.alias('__zero_row__'))
    f_zeroes_any = bool(df_pl.select(pl.col('__zero_row__').any()).item())
    if f_zeroes_any:
        df2_zeroes = df_pl.filter(pl.col('__zero_row__')).drop('__zero_row__')
        df2 = df_pl.filter(~pl.col('__zero_row__')).drop('__zero_row__')
        df_orig_pl = df_orig_pl.filter(~pl.col('__zero_row__')).drop('__zero_row__')
    else:
        df2_zeroes = None
        df2 = df_pl.drop('__zero_row__')
        df_orig_pl = df_orig_pl.drop('__zero_row__')

    if df2.height <= 1:
        logger.info("Insufficient good data for detecting 100x price errors", extra=log_extras)
        out = df_orig_pl  # unaltered, but may need 'Repaired?' column
        if "Repaired?" not in out.columns:
            out = out.with_columns(pl.lit(False).alias('Repaired?'))
        return out

    # ---- scipy numerical kernel on numpy block ----
    df2_data = np.column_stack([df2[c].to_numpy() for c in data_cols]).astype(float)
    median = _ndimage.median_filter(df2_data, size=(3, 3), mode="wrap")
    ratio = df2_data / median
    ratio_rounded = (ratio / 20).round() * 20
    f = ratio_rounded == 100
    ratio_rcp = 1.0 / ratio
    ratio_rcp_rounded = (ratio_rcp / 20).round() * 20
    f_rcp = (ratio_rounded == 100) | (ratio_rcp_rounded == 100)
    f_either = f | f_rcp

    if not f_either.any():
        logger.debug("No sporadic 100x errors", extra=log_extras)
        out = df2 if df2_zeroes is None else pl.concat(
            [df2, df2_zeroes.select(df2.columns)], how='vertical_relaxed'
        ).sort(date_col)
        if "Repaired?" not in out.columns:
            out = out.with_columns(pl.lit(False).alias('Repaired?'))
        return out

    tag = -1.0

    # Apply tag using polars masks (per-column, row-aligned).
    tag_exprs = []
    for i, c in enumerate(data_cols):
        mask = pl.Series('__m__', f_either[:, i].tolist())
        tag_exprs.append(
            pl.when(mask).then(pl.lit(tag)).otherwise(pl.col(c)).alias(c)
        )
    df2 = df2.with_columns(tag_exprs)

    n_before = int((df2_data == tag).sum())  # pre-tag block; matches pandas

    # ---- delegate to polars-native reconstruct wrapper ----
    df2 = reconstruct_intervals_batch(price_history, df2, interval, prepost, tag)
    assert isinstance(df2, pl.DataFrame)

    # Recompute tag mask after reconstruction.
    df2_block = np.column_stack([df2[c].to_numpy() for c in data_cols]).astype(float)
    df2_tagged = df2_block == tag
    n_after = int(df2_tagged.sum())

    if n_after > 0:
        # Crude second pass on remaining tagged values. Match pandas
        # exactly: ``f_rcp`` is recomputed against the *post-first-pass*
        # tag mask, so any value already corrected by the f-branch is
        # excluded from the f_rcp branch.
        f_remaining = df2_tagged & f

        # Helpers for (mask -> with_columns) on a single column.
        def _apply_row_mask_value(df_in, col_name, row_mask, value_expr):
            series = pl.Series('__rm__', row_mask.tolist())
            return df_in.with_columns(
                pl.when(series).then(value_expr).otherwise(pl.col(col_name)).alias(col_name)
            )

        # Original-frame OHLC pulled by aligned position (df_orig_pl is the
        # same row order/length as df2 after zero filtering, mirroring the
        # pandas ``df.loc[idx, c]`` reads against the original ``df``).
        assert df_orig_pl.height == df2.height

        # First pass: f (ratio≈100) -> divide by 100.
        for c in ('Open', 'Close'):
            if c not in data_cols:
                continue
            j = data_cols.index(c)
            col_mask = f_remaining[:, j]
            if col_mask.any():
                orig_vals = df_orig_pl[c].to_numpy() * 0.01
                df2 = _apply_row_mask_value(
                    df2, c, col_mask,
                    pl.Series('__v__', orig_vals.tolist()),
                )
        # High/Low after Open/Close updates so they see the new values.
        if 'High' in data_cols:
            j = data_cols.index('High')
            col_mask = f_remaining[:, j]
            if col_mask.any():
                df2 = _apply_row_mask_value(
                    df2, 'High', col_mask,
                    pl.max_horizontal(pl.col('Open'), pl.col('Close')),
                )
        if 'Low' in data_cols:
            j = data_cols.index('Low')
            col_mask = f_remaining[:, j]
            if col_mask.any():
                df2 = _apply_row_mask_value(
                    df2, 'Low', col_mask,
                    pl.min_horizontal(pl.col('Open'), pl.col('Close')),
                )

        # Recompute tag mask for the f_rcp pass (mirrors pandas line
        # ``f_rcp = (df2[data_cols].to_numpy() == tag) & f_rcp``).
        df2_block_mid = np.column_stack([df2[c].to_numpy() for c in data_cols]).astype(float)
        f_rcp_remaining = (df2_block_mid == tag) & f_rcp

        # Second pass: f_rcp (ratio≈1/100) -> multiply by 100.
        for c in ('Open', 'Close'):
            if c not in data_cols:
                continue
            j = data_cols.index(c)
            col_mask = f_rcp_remaining[:, j]
            if col_mask.any():
                orig_vals = df_orig_pl[c].to_numpy() * 100.0
                df2 = _apply_row_mask_value(
                    df2, c, col_mask,
                    pl.Series('__v__', orig_vals.tolist()),
                )
        if 'High' in data_cols:
            j = data_cols.index('High')
            col_mask = f_rcp_remaining[:, j]
            if col_mask.any():
                df2 = _apply_row_mask_value(
                    df2, 'High', col_mask,
                    pl.max_horizontal(pl.col('Open'), pl.col('Close')),
                )
        if 'Low' in data_cols:
            j = data_cols.index('Low')
            col_mask = f_rcp_remaining[:, j]
            if col_mask.any():
                df2 = _apply_row_mask_value(
                    df2, 'Low', col_mask,
                    pl.min_horizontal(pl.col('Open'), pl.col('Close')),
                )

        df2_block = np.column_stack([df2[c].to_numpy() for c in data_cols]).astype(float)
        df2_tagged = df2_block == tag
        n_after_crude = int(df2_tagged.sum())
    else:
        n_after_crude = n_after

    n_fixed = n_before - n_after_crude
    n_fixed_crudely = n_after - n_after_crude
    if n_fixed > 0:
        report_msg = f"fixed {n_fixed}/{n_before} currency unit mixups "
        if n_fixed_crudely > 0:
            report_msg += f"({n_fixed_crudely} crudely)"
        logger.info(report_msg, extra=log_extras)

    # Restore original values where repair failed.
    f_either_remaining = df2_tagged
    for j, c in enumerate(data_cols):
        col_mask = f_either_remaining[:, j]
        if col_mask.any():
            orig_vals = df_orig_pl[c].to_numpy()
            series = pl.Series('__rm__', col_mask.tolist())
            df2 = df2.with_columns(
                pl.when(series)
                  .then(pl.Series('__v__', orig_vals.tolist()))
                  .otherwise(pl.col(c))
                  .alias(c)
            )

    if df2_zeroes is not None:
        if 'Repaired?' not in df2_zeroes.columns:
            df2_zeroes = df2_zeroes.with_columns(pl.lit(False).alias('Repaired?'))
        # Align column order/schema before vertical concat.
        df2_zeroes = df2_zeroes.select(df2.columns)
        df2 = pl.concat([df2, df2_zeroes], how='vertical_relaxed').sort(date_col)

    return df2


def fix_bad_stock_splits(price_history, df_pl, interval, tz_exchange):
    """Polars-native mirror of ``_fix_bad_stock_splits``.

    Output is bit-identical to the pandas implementation. The deeply
    nested pandas helper ``_fix_prices_sudden_change`` (~700 lines,
    pandas-heavy with iloc/loc slicing, multi-index alignment, dividend
    and volume rescaling logic) is bridged at its single call site --
    same pattern used for ``_reconstruct_intervals_batch`` in already-
    migrated wrappers. The OUTER orchestration (split scan, slice,
    concat) is polars-native here.
    """
    import polars as pl

    if df_pl.height == 0:
        return df_pl

    interday = interval in ['1d', '1wk', '1mo', '3mo']
    if not interday:
        return df_pl

    date_col = 'Datetime' if 'Datetime' in df_pl.columns else 'Date'

    if 'Stock Splits' not in df_pl.columns:
        return df_pl

    # scan splits oldest -> newest
    df_pl = df_pl.sort(date_col)

    splits = df_pl.get_column('Stock Splits').to_numpy()
    split_f = splits != 0
    if not split_f.any():
        logger = utils.get_yf_logger()
        logger.debug('price-repair-split: No splits in data')
        return df_pl

    logger = utils.get_yf_logger()
    log_extras = {'yf_cat': 'split-repair', 'yf_interval': interval, 'yf_symbol': price_history.ticker}
    # Mirror the pandas debug log (Date -> split). Build the dict from
    # the polars frame for parity.
    dt_col_vals = df_pl.get_column(date_col).to_list()
    split_dict = {dt_col_vals[i]: float(splits[i]) for i in np.where(split_f)[0]}
    logger.debug(f'Splits: {str(split_dict)}', extra=log_extras)

    if 'Repaired?' not in df_pl.columns:
        df_pl = df_pl.with_columns(pl.lit(False).alias('Repaired?'))

    n_rows = df_pl.height
    for split_idx in np.where(split_f)[0]:
        split_idx = int(split_idx)
        split_dt = dt_col_vals[split_idx]
        split = float(splits[split_idx])
        if split_idx == 0:
            continue

        # Add buffer rows after the split to detect big change.
        if interval in ['1wk', '1mo', '3mo']:
            buf_idx = split_idx + 1
        else:
            buf_idx = split_idx + 5
        cutoff_idx = min(n_rows, buf_idx)
        df_pre_split = df_pl.slice(0, cutoff_idx + 1)
        try:
            split_dt_str = split_dt.date()
        except AttributeError:
            split_dt_str = split_dt
        try:
            pre_first = df_pre_split.get_column(date_col)[0].date()
            pre_last = df_pre_split.get_column(date_col)[-1].date()
        except AttributeError:
            pre_first = df_pre_split.get_column(date_col)[0]
            pre_last = df_pre_split.get_column(date_col)[-1]
        logger.debug(f'split_idx={buf_idx} split_dt={split_dt_str} split={split:.4f}', extra=log_extras)
        logger.debug(f'df dt range: {pre_first} -> {pre_last}', extra=log_extras)

        # ---- polars-native ``fix_prices_sudden_change`` ----
        df_pre_split_repaired = fix_prices_sudden_change(
            price_history, df_pre_split, interval, tz_exchange, split,
            correct_volume=True, correct_dividend=True)
        df_pre_split_repaired = df_pre_split_repaired.sort(date_col)

        # Merge back in.
        if cutoff_idx == n_rows - 1:
            df_pl = df_pre_split_repaired
        else:
            df_post_cutoff = df_pl.slice(cutoff_idx + 1, n_rows - (cutoff_idx + 1))
            if df_post_cutoff.height == 0:
                df_pl = df_pre_split_repaired
            else:
                # Align column order/dtypes for vertical concat.
                df_post_cutoff = df_post_cutoff.select(df_pre_split_repaired.columns)
                df_pl = pl.concat(
                    [df_pre_split_repaired, df_post_cutoff],
                    how='vertical_relaxed')

        # Refresh splits view (rows can be re-typed by repair sub-call,
        # but order/length is preserved).
        n_rows = df_pl.height
        splits = df_pl.get_column('Stock Splits').to_numpy()
        dt_col_vals = df_pl.get_column(date_col).to_list()

    return df_pl


def repair_capital_gains(price_history, df_pl):
    # Native polars mirror of ``_repair_capital_gains`` (pandas).
    # The pandas method is left untouched; this implementation reproduces
    # its semantics directly on the polars frame so we avoid round-tripping
    # through pandas. Output is bit-identical to the pandas path.
    import polars as pl

    if 'Capital Gains' not in df_pl.columns:
        return df_pl
    if df_pl.height == 0:
        return df_pl
    # ``(df['Capital Gains'] == 0).all()`` short-circuit.
    cg_nonzero_count = df_pl.filter(pl.col('Capital Gains') != 0).height
    if cg_nonzero_count == 0:
        return df_pl

    logger = utils.get_yf_logger()
    log_extras = {'yf_cat': 'repair-capital-gains', 'yf_symbol': price_history.ticker}

    date_col = 'Datetime' if 'Datetime' in df_pl.columns else 'Date'
    df_pl = df_pl.sort(date_col)

    # ``Price_Change%`` mean over rows with no distributions.
    df_pl = df_pl.with_columns(
        pl.col('Close').pct_change().abs().alias('Price_Change%'),
    )
    no_dist_mean_df = df_pl.filter(
        (pl.col('Dividends') == 0) & (pl.col('Capital Gains') == 0),
    ).select(pl.col('Price_Change%').mean())
    # ``mean`` over an all-null/empty selection yields None; mirror pandas
    # NaN by using float('nan') so subsequent arithmetic propagates NaN.
    mean_val = no_dist_mean_df.item() if no_dist_mean_df.height > 0 else None
    price_drop_pct_mean = float('nan') if mean_val is None else float(mean_val)
    df_pl = df_pl.drop('Price_Change%')

    if 'Repaired?' not in df_pl.columns:
        df_pl = df_pl.with_columns(pl.lit(False).alias('Repaired?'))
    df_pl = df_pl.with_columns(
        (pl.col('Adj Close') / pl.col('Close')).alias('Adj'),
    )

    # Materialise scalar arrays once; loop bodies need positional access.
    dates = df_pl[date_col].to_list()
    c = df_pl['Close'].to_list()
    ac = df_pl['Adj Close'].to_list()
    dividends_arr = df_pl['Dividends'].to_list()
    cg_arr = df_pl['Capital Gains'].to_list()

    # Map date -> row index for the rows where Capital Gains > 0.
    cg_idxs: list[int] = [i for i, v in enumerate(cg_arr) if v is not None and v > 0]

    dcs: dict[int, bool] = {}
    for idx in cg_idxs:
        if idx > 0:
            dividend = dividends_arr[idx]
            capital_gains = cg_arr[idx]
            assert dividend is not None
            assert capital_gains is not None
            if dividend < capital_gains:
                continue
            prev_close = c[idx - 1]
            cur_close = c[idx]
            assert prev_close is not None
            assert cur_close is not None
            div_pct = dividend / prev_close
            cg_pct = capital_gains / prev_close
            price_drop_pct = (prev_close - cur_close) / prev_close
            price_drop_pct_excl_vol = price_drop_pct - price_drop_pct_mean
            diff_div = abs(price_drop_pct_excl_vol - div_pct)
            diff_total = abs(price_drop_pct_excl_vol - (div_pct + cg_pct))
            cg_is_double_counted = diff_div < diff_total
            dcs[idx] = cg_is_double_counted

    if not dcs:
        df_pl = df_pl.with_columns(
            (pl.col('Close') * pl.col('Adj')).alias('Adj Close'),
        )
        df_pl = df_pl.drop('Adj')
        return df_pl

    pct_double_counted = sum(dcs.values()) / len(dcs)

    if pct_double_counted >= 0.666:
        # Build replacement columns iteratively; expressions only "see" the
        # frame at the start of ``with_columns``, so we apply mutations in
        # the same order as the pandas version (each loop iteration sees
        # the previous iteration's edits).
        for idx in dcs.keys():
            dt = dates[idx]
            assert dt is not None
            dividend = dividends_arr[idx]
            capital_gains = cg_arr[idx]
            assert dividend is not None
            assert capital_gains is not None
            prev_close = c[idx - 1]
            cur_close = c[idx]
            prev_ac = ac[idx - 1]
            cur_ac = ac[idx]
            assert prev_close is not None
            assert cur_close is not None
            assert prev_ac is not None
            assert cur_ac is not None
            dividend_true = dividend - capital_gains
            adj_before = (prev_ac / prev_close) / (cur_ac / cur_close)
            adj_correct = 1.0 - (dividend_true + capital_gains) / prev_close
            correction = adj_correct / adj_before

            # df.loc[dt, 'Dividends'] = dividend_true
            # df.loc[:dt - 1 day, 'Adj'] *= correction
            # df.loc[:dt, 'Repaired?'] = True
            dt_prev_day = dt - _datetime.timedelta(1)
            df_pl = df_pl.with_columns(
                pl.when(pl.col(date_col) == dt)
                  .then(pl.lit(dividend_true))
                  .otherwise(pl.col('Dividends'))
                  .alias('Dividends'),
                pl.when(pl.col(date_col) <= dt_prev_day)
                  .then(pl.col('Adj') * correction)
                  .otherwise(pl.col('Adj'))
                  .alias('Adj'),
                pl.when(pl.col(date_col) <= dt)
                  .then(pl.lit(True))
                  .otherwise(pl.col('Repaired?'))
                  .alias('Repaired?'),
            )
            msg = f"Repaired capital-gains double-count at {dt.date()}. Adj correction = {correction:.4f}"
            logger.info(msg, extra=log_extras)

    df_pl = df_pl.with_columns(
        (pl.col('Close') * pl.col('Adj')).alias('Adj Close'),
    )
    df_pl = df_pl.drop('Adj')
    return df_pl
