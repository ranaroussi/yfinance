"""Shared helpers for the historical price scraper and repair logic."""

import datetime as _datetime
import importlib
import logging
from dataclasses import dataclass
from typing import Any, Optional, Union, cast

import dateutil as _dateutil
from dateutil.relativedelta import relativedelta as _relativedelta
import numpy as np
import pandas as pd

from ...options import HISTORY_REQUEST_ARG_NAMES, HISTORY_REQUEST_DEFAULTS, bind_options
from ... import utils


@dataclass(frozen=True)
class _HistoryRequest:
    period: Optional[str] = None
    interval: str = "1d"
    start: Any = None
    end: Any = None
    prepost: bool = False
    actions: bool = True
    auto_adjust: bool = True
    back_adjust: bool = False
    repair: bool = False
    keepna: bool = False
    rounding: bool = False
    timeout: Optional[float] = 10
    raise_errors: bool = False


def _parse_options(function_name, arg_names, defaults, args, kwargs):
    options, remaining_kwargs = bind_options(function_name, arg_names, defaults, args, kwargs)
    if remaining_kwargs:
        unexpected = next(iter(remaining_kwargs))
        raise TypeError(f"{function_name}() got an unexpected keyword argument '{unexpected}'")

    return options


def _parse_history_request(function_name, args, kwargs) -> _HistoryRequest:
    options = _parse_options(
        function_name,
        HISTORY_REQUEST_ARG_NAMES,
        HISTORY_REQUEST_DEFAULTS,
        args,
        kwargs,
    )
    return _HistoryRequest(**options)


def _interval_to_supported_delta(interval: str) -> Union[_datetime.timedelta, _relativedelta]:
    delta = utils.interval_to_timedelta(interval)
    if isinstance(delta, (_datetime.timedelta, _relativedelta)):
        return delta
    if isinstance(delta, pd.Timedelta):
        return _datetime.timedelta(seconds=float(delta.total_seconds()))
    raise ValueError(f"Unsupported interval delta for '{interval}'")


def _interval_to_timedelta(interval: str) -> _datetime.timedelta:
    delta = utils.interval_to_timedelta(interval)
    if isinstance(delta, _datetime.timedelta):
        return delta
    if isinstance(delta, _relativedelta):
        if delta.years or delta.months:
            raise ValueError(f"Interval '{interval}' does not map to a fixed timedelta")
        anchor = _datetime.datetime(2000, 1, 1)
        return (anchor + delta) - anchor
    if isinstance(delta, pd.Timedelta):
        return _datetime.timedelta(seconds=float(delta.total_seconds()))
    raise ValueError(f"Interval '{interval}' does not map to a timedelta")


def _safe_timestamp(ts_value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(ts_value)
    if pd.isna(ts):
        raise ValueError(f"Cannot convert {ts_value!r} to timestamp")
    return cast(pd.Timestamp, ts)


def _index_date(index: pd.Index, position: int) -> _datetime.date:
    return _safe_timestamp(index[position]).date()


def _get_scipy_ndimage():
    return importlib.import_module("scipy.ndimage")


@dataclass(frozen=True)
class _SplitRangeContext:
    split: float
    size: int
    idx_latest_active: Optional[int]
    idx_rev_latest_active: Optional[int]


@dataclass(frozen=True)
class _SplitRangePruneContext:
    index: pd.Index
    start_min: Optional[_datetime.date]
    logger: Any
    log_extras: dict[str, Any]
    column: Optional[str] = None


@dataclass(frozen=True)
class _PriceChangeRepairSettings:
    interval: str
    tz_exchange: str
    change: float
    correct_volume: bool = False
    correct_dividend: bool = False


@dataclass(frozen=True)
class _SplitRepairContext:
    df_index: pd.Index
    split: float
    split_rcp: float
    start_min: Optional[_datetime.date]
    logger: Any
    log_extras: dict[str, Any]
    interday: bool
    fix_type: str
    correct_volume: bool
    correct_dividend: bool
    appears_suspended: bool
    idx_latest_active: Optional[int]
    split_range_context: _SplitRangeContext


@dataclass(frozen=True)
class _SplitColumnSignals:
    ohlc: list[str]
    f: np.ndarray
    f_up: np.ndarray
    f_down: np.ndarray


@dataclass(frozen=True)
class _LocalVolatilityContext:
    interval: str
    interday: bool
    split_max: float
    correct_columns_individually: bool
    price_data_cols: list[str]
    debug_cols: list[str]
    logger: Any


@dataclass(frozen=True)
class _SplitAbortContext:
    change: float
    interval: str
    logger: Any
    log_extras: dict[str, Any]


@dataclass(frozen=True)
class _SplitChangeMatrixContext:
    ohlc: list[str]
    interday: bool
    interval: str
    split: float
    n: int
    correct_columns_individually: bool


@dataclass(frozen=True)
class _SplitThresholdContext:
    split: float
    split_rcp: float
    interday: bool
    interval: str
    logger: Any
    log_extras: dict[str, Any]


def _map_split_signals_to_ranges(
    signals: np.ndarray,
    up_signals: np.ndarray,
    down_signals: np.ndarray,
    split: float,
) -> list[tuple[int, int, str]]:
    if signals[0]:
        signals = np.copy(signals)
        signals[0] = False
        up_signals = np.copy(up_signals)
        up_signals[0] = False
        down_signals = np.copy(down_signals)
        down_signals[0] = False

    if not signals.any():
        return []

    down_adj, up_adj = (
        ("split", "1.0/split") if split > 1.0 else ("1.0/split", "split")
    )
    true_indices = np.where(signals)[0]
    ranges = []

    for range_index in range(0, len(true_indices) - 1, 2):
        start = true_indices[range_index]
        adj = down_adj if down_signals[start] else up_adj
        ranges.append((start, true_indices[range_index + 1], adj))

    if len(true_indices) % 2 != 0:
        start = true_indices[-1]
        adj = down_adj if down_signals[start] else up_adj
        ranges.append((start, len(signals), adj))

    return ranges


def _shift_split_ranges(
    ranges: list[tuple[int, int, str]], offset: int
) -> list[tuple[int, int, str]]:
    return [(start + offset, end + offset, adj) for start, end, adj in ranges]


def _flip_split_ranges(
    ranges: list[tuple[int, int, str]], size: int
) -> list[tuple[int, int, str]]:
    return [(size - end, size - start, adj) for start, end, adj in ranges]


def _build_split_correction_ranges(
    signals: np.ndarray,
    up_signals: np.ndarray,
    down_signals: np.ndarray,
    context: _SplitRangeContext,
) -> list[tuple[int, int, str]]:
    if context.idx_latest_active is None or context.idx_rev_latest_active is None:
        return _map_split_signals_to_ranges(signals, up_signals, down_signals, context.split)

    ranges_before = _shift_split_ranges(
        _map_split_signals_to_ranges(
            signals[context.idx_latest_active :],
            up_signals[context.idx_latest_active :],
            down_signals[context.idx_latest_active :],
            context.split,
        ),
        context.idx_latest_active,
    )
    rev_down_signals = np.flip(np.roll(up_signals, -1))
    rev_up_signals = np.flip(np.roll(down_signals, -1))
    rev_signals = rev_up_signals | rev_down_signals
    ranges_after = _shift_split_ranges(
        _map_split_signals_to_ranges(
            rev_signals[context.idx_rev_latest_active :],
            rev_up_signals[context.idx_rev_latest_active :],
            rev_down_signals[context.idx_rev_latest_active :],
            context.split,
        ),
        context.idx_rev_latest_active,
    )
    return ranges_before + _flip_split_ranges(ranges_after, context.size)


def _prune_split_correction_ranges(
    ranges: list[tuple[int, int, str]],
    context: _SplitRangePruneContext,
) -> list[tuple[int, int, str]]:
    if context.start_min is None:
        return ranges

    kept_ranges = []
    prefix = "" if context.column is None else f"{context.column} "
    for range_item in ranges:
        if _index_date(context.index, range_item[0]) < context.start_min:
            context.logger.debug(
                f"Pruning {prefix}range {context.index[range_item[0]]}"
                f"->{context.index[range_item[1] - 1]} because too old.",
                extra=context.log_extras,
            )
            continue
        kept_ranges.append(range_item)
    return kept_ranges


def _split_adjustment_multipliers(adj: str, split: float, split_rcp: float) -> tuple[float, float]:
    if adj == "split":
        return split, split_rcp
    return split_rcp, split


def _calc_volume_zscore(volume, block) -> float:
    values = block["Volume"].to_numpy()
    std = np.std(values, ddof=1)
    if std == 0.0:
        return 0.0
    mean = np.mean(values)
    return float((volume - mean) / std)


def _split_up_shift_flags(f_up: np.ndarray) -> np.ndarray:
    return np.asarray(f_up if len(f_up.shape) == 1 else f_up.any(axis=1), dtype=bool)


def _split_down_dates(df2, f_down: np.ndarray) -> pd.Index:
    down_mask = f_down if len(f_down.shape) == 1 else f_down.any(axis=1)
    return df2.index[down_mask]


def _volume_change_pct(df2, idx: int, multiday: bool) -> float:
    volume = df2["Volume"].iloc[idx]
    if volume == 0:
        return 0.0

    change_pct = df2["Volume"].iloc[idx - 1] / volume
    if multiday and idx + 1 < len(df2):
        next_volume = df2["Volume"].iloc[idx + 1]
        if next_volume > 0:
            change_pct = max(change_pct, volume / next_volume)
    return float(change_pct)


def _followup_volume_block(df2, f_up_shifts: np.ndarray, down_dts: pd.Index, idx: int):
    price_idx = idx - 1
    dt = _safe_timestamp(df2.index[price_idx])
    i_pos_in_flat_indices = np.logical_not(f_up_shifts)[:price_idx].sum()
    flat_indices = np.where(np.logical_not(f_up_shifts))[0]
    start = max(0, i_pos_in_flat_indices - 15)
    end = min(len(flat_indices), start + 31)
    block = df2.iloc[flat_indices[start:end]].sort_index()
    down_dts_from = down_dts[down_dts >= dt]
    if len(down_dts_from) > 0:
        next_down_dt = min(down_dts_from)
        if next_down_dt == dt:
            return dt, price_idx, None
        block_after = block.loc[
            dt + _datetime.timedelta(1) : next_down_dt - _datetime.timedelta(1)
        ]
    else:
        block_after = block.loc[dt + _datetime.timedelta(1) :]

    if block_after.empty:
        block_after = None
    return dt, price_idx, block_after


def _has_false_positive_followup_volume(volume, block_after) -> bool:
    if block_after is None:
        return False

    z_score_after = _calc_volume_zscore(volume, block_after)
    z_score_after_d1 = _calc_volume_zscore(block_after["Volume"].iloc[0], block_after)
    return max(z_score_after, z_score_after_d1) > 2


def _clear_up_signal(f_up: np.ndarray, idx: int) -> None:
    if len(f_up.shape) == 1:
        f_up[idx] = False
    else:
        f_up[idx, :] = False


def _filter_split_false_positives(df2, f_up, f_down, multiday: bool, logger):
    f_up_shifts = _split_up_shift_flags(f_up)
    if not f_up_shifts.any():
        return f_up

    down_dts = _split_down_dates(df2, f_down)

    for idx in np.where(f_up_shifts)[0]:
        dt, price_idx, block_after = _followup_volume_block(df2, f_up_shifts, down_dts, idx)
        volume = df2["Volume"].iloc[price_idx]
        vol_change_pct = _volume_change_pct(df2, price_idx, multiday)
        logger.debug(f"- vol_change_pct = {vol_change_pct:.4f}")

        if not _has_false_positive_followup_volume(volume, block_after):
            continue
        logger.debug(f"Detected false-positive split error on {dt.date()}, ignoring price drop")
        _clear_up_signal(f_up, idx)

    return f_up


def _local_volatility_lookback(interval: str) -> int:
    if interval.endswith("d"):
        return 10
    if interval.endswith("m"):
        return 100
    return 3


def _clean_local_changes(changes_local: pd.DataFrame, column: str) -> np.ndarray:
    if column == "n/a":
        return cast(pd.Series, changes_local["1D %"][~changes_local["f"]]).to_numpy()
    return cast(
        pd.Series,
        changes_local[column + " 1D %"][~changes_local[column + "_f"]],
    ).to_numpy()


def _largest_change_pct(sd_pct: float, context: _LocalVolatilityContext) -> float:
    largest_change_pct = 5 * sd_pct
    if context.interday and context.interval != "1d":
        largest_change_pct *= 3
        if context.interval in ["1mo", "3mo"]:
            largest_change_pct *= 2
    return largest_change_pct


def _big_change_value(df_workings: pd.DataFrame, idx: int, column: str):
    if column == "n/a":
        return df_workings["1D %"].iloc[idx]
    return df_workings[column + " 1D %"].iloc[idx]


def _matches_local_volatility(
    df_workings: pd.DataFrame,
    changes_local: pd.DataFrame,
    idx: int,
    column: str,
    context: _LocalVolatilityContext,
) -> tuple[bool, float]:
    clean_changes = _clean_local_changes(changes_local, column)
    avg = np.mean(clean_changes)
    sd = np.std(clean_changes)
    sd_pct = float(sd / avg)
    largest_change_pct = _largest_change_pct(sd_pct, context)
    threshold = (context.split_max + 1.0 + largest_change_pct) * 0.5
    big_change = _big_change_value(df_workings, idx, column)
    matches = (1.0 / threshold) < big_change < threshold
    return matches, sd_pct


def _mark_local_volatility_match(
    df_workings: pd.DataFrame,
    dt: pd.Timestamp,
    column: str,
    sd_pct: float,
    logger,
) -> None:
    if column == "n/a":
        logger.debug(
            f"Unusual price action @ {dt.date()} is actually similar to local "
            f"price volatility, so ignoring (StdDev % mean = {sd_pct * 100:.1f}%)"
        )
        df_workings.loc[dt, "f"] = False
        return

    logger.debug(
        f"Unusual '{column}' price action @ {dt.date()} is actually similar "
        f"to local price volatility, so ignoring (StdDev % mean = {sd_pct * 100:.1f}%)"
    )
    df_workings.loc[dt, column + "_f"] = False


def _local_volatility_window(df2, df_workings: pd.DataFrame, idx: int, interval: str):
    dt = _safe_timestamp(df2.index[idx])
    idx_end = min(len(df2) - 1, idx + 2)
    lookback = _local_volatility_lookback(interval)
    idx_start = max(0, idx - lookback)
    return dt, df_workings.iloc[idx_start:idx_end]


def _filter_local_volatility_false_positives(
    df2,
    df_workings: pd.DataFrame,
    f_down: np.ndarray,
    f_up: np.ndarray,
    context: _LocalVolatilityContext,
) -> tuple[np.ndarray, np.ndarray]:
    f = f_down | f_up
    for idx in np.where(f)[0]:
        dt, changes_local = _local_volatility_window(df2, df_workings, idx, context.interval)
        columns = context.price_data_cols if context.correct_columns_individually else ["n/a"]
        for column in columns:
            matches, sd_pct = _matches_local_volatility(
                df_workings,
                changes_local,
                idx,
                column,
                context,
            )
            if not matches:
                continue
            _mark_local_volatility_match(df_workings, dt, column, sd_pct, context.logger)

    if not context.correct_columns_individually:
        mask = df_workings["f"].to_numpy()
        return f_down & mask, f_up & mask

    for column_index, column in enumerate(context.price_data_cols):
        if column in context.debug_cols:
            f_down[:, column_index] = f_down[:, column_index] & df_workings[column + "_f"]
            f_up[:, column_index] = f_up[:, column_index] & df_workings[column + "_f"]
    return f_down, f_up


def _should_abort_due_to_nearby_split(
    df2,
    f_splits: np.ndarray,
    f: np.ndarray,
    context: _SplitAbortContext,
) -> bool:
    if context.change not in [100.0, 0.01] or not f_splits.any():
        return False

    indices_a = np.where(f_splits)[0]
    indices_b = np.where(f)[0]
    if indices_a.size == 0 or indices_b.size == 0:
        return False

    gaps = (indices_b[:, None] - indices_a) * -1
    f_pos = gaps > 0
    if not f_pos.any():
        return False

    gap_min = gaps[f_pos].min()
    interval_delta = utils.interval_to_timedelta(context.interval)
    if isinstance(interval_delta, pd.Timedelta):
        interval_delta = _datetime.timedelta(seconds=float(interval_delta.total_seconds()))

    if isinstance(interval_delta, _dateutil.relativedelta.relativedelta):
        gap_delta = interval_delta * int(gap_min)
        threshold_delta = _dateutil.relativedelta.relativedelta(days=30)
        idx = np.where(gaps == gap_min)[0][0]
        dt = _safe_timestamp(df2.index[idx])
        within_threshold = (dt + gap_delta) < (dt + threshold_delta)
    else:
        gap_delta = interval_delta * int(gap_min)
        threshold_delta = _datetime.timedelta(days=30)
        within_threshold = gap_delta < threshold_delta

    if not within_threshold:
        return False

    context.logger.info(
        "100x changes are too soon after stock split events, aborting",
        extra=context.log_extras,
    )
    return True


def _log_split_repair_workings(
    df_workings: pd.DataFrame,
    correct_columns_individually: bool,
    debug_cols: list[str],
    logger,
) -> None:
    if not logger.isEnabledFor(logging.DEBUG):
        return

    df_workings["i"] = list(range(0, df_workings.shape[0]))
    df_workings["i_rev"] = df_workings.shape[0] - 1 - df_workings["i"]
    if correct_columns_individually:
        f_change = df_workings[[c + "_down" for c in debug_cols]].any(axis=1) | df_workings[
            [c + "_up" for c in debug_cols]
        ].any(axis=1)
    else:
        f_change = df_workings["down"] | df_workings["up"]
    f_change = (
        f_change
        | np.roll(f_change, -1)
        | np.roll(f_change, 1)
        | np.roll(f_change, -2)
        | np.roll(f_change, 2)
    )
    with pd.option_context(
        "display.max_rows", None, "display.max_columns", 10, "display.width", 1000
    ):
        logger.debug("price-repair-split: my workings:" + "\n" + str(df_workings[f_change]))


def _prepare_split_change_matrix(
    df2,
    df_workings: pd.DataFrame,
    change_denoised: np.ndarray,
    context: _SplitChangeMatrixContext,
) -> tuple[np.ndarray, list[str]]:
    if "Repaired?" not in df2.columns:
        df2["Repaired?"] = False

    if context.correct_columns_individually:
        change_matrix = np.full((context.n, 4), 1.0)
        price_data = df2[context.ohlc].replace(0.0, 1.0).to_numpy()
        price_data_cols = context.ohlc
        change_matrix[1:] = price_data[1:,] / price_data[:-1,]
        for column_index, column in enumerate(price_data_cols):
            df_workings[column + " 1D %"] = change_matrix[:, column_index]
            df_workings[column + " 1D %"] = df_workings[column + " 1D %"].round(3)
        return change_matrix, price_data_cols

    df_workings["1D %"] = change_denoised
    df_workings["1D %"] = df_workings["1D %"].round(3)
    return change_denoised, ["Open", "Close"] if (
        context.interday and context.interval != "1d" and context.split not in [100.0, 100, 0.001]
    ) else context.ohlc


def _estimate_split_repair_threshold(
    change_denoised: np.ndarray,
    context: _SplitThresholdContext,
) -> Optional[tuple[float, float]]:
    q1 = np.quantile(change_denoised, 0.25)
    q3 = np.quantile(change_denoised, 0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    f = (change_denoised >= lower_bound) & (change_denoised <= upper_bound)
    avg = np.mean(change_denoised[f])
    sd = np.std(change_denoised[f])
    sd_pct = sd / avg
    context.logger.debug(
        "Estimation of true 1D change stats: "
        f"mean = {avg:.2f}, StdDev = {sd:.4f} ({sd_pct * 100.0:.1f}% of mean)",
        extra=context.log_extras,
    )

    largest_change_pct = 5 * sd_pct
    if context.interday and context.interval != "1d":
        largest_change_pct *= 3
        if context.interval in ["1mo", "3mo"]:
            largest_change_pct *= 2
    if max(context.split, context.split_rcp) < 1.0 + largest_change_pct:
        context.logger.debug(
            "Split ratio too close to normal price volatility. Won't repair",
            extra=context.log_extras,
        )
        context.logger.debug(
            f"sd_pct = {sd_pct:.4f}  largest_change_pct = {largest_change_pct:.4f}",
            extra=context.log_extras,
        )
        return None

    split_max = max(context.split, context.split_rcp)
    context.logger.debug(
        f"split_max={split_max:.3f} largest_change_pct={largest_change_pct:.4f}",
        extra=context.log_extras,
    )
    threshold = float((split_max + 1.0 + largest_change_pct) * 0.5)
    context.logger.debug(
        f"threshold={threshold:.3f}, threshold_rcp={1.0 / threshold:.3f}",
        extra=context.log_extras,
    )
    return split_max, threshold


def _build_pruned_split_ranges(
    signals: np.ndarray,
    up_signals: np.ndarray,
    down_signals: np.ndarray,
    context: _SplitRepairContext,
    column: Optional[str] = None,
) -> list[tuple[int, int, str]]:
    if (
        context.appears_suspended
        and context.idx_latest_active is not None
        and signals.any()
        and context.idx_latest_active >= np.where(signals)[0][0]
    ):
        ranges = _build_split_correction_ranges(
            signals,
            up_signals,
            down_signals,
            context.split_range_context,
        )
    else:
        ranges = _map_split_signals_to_ranges(
            signals,
            up_signals,
            down_signals,
            context.split,
        )
    return _prune_split_correction_ranges(
        ranges,
        _SplitRangePruneContext(
            context.df_index,
            context.start_min,
            context.logger,
            context.log_extras,
            column=column,
        ),
    )


def _collect_ohlc_split_ranges(
    signals: _SplitColumnSignals,
    context: _SplitRepairContext,
) -> list[Optional[list[tuple[int, int, str]]]]:
    ohlc_correct_ranges: list[Optional[list[tuple[int, int, str]]]] = [None, None, None, None]

    for column_index, column in enumerate(signals.ohlc):
        ranges = _build_pruned_split_ranges(
            signals.f[:, column_index],
            signals.f_up[:, column_index],
            signals.f_down[:, column_index],
            context,
            column=column,
        )
        context.logger.debug(f"column '{column}' ranges: {ranges}", extra=context.log_extras)
        if ranges:
            ohlc_correct_ranges[column_index] = ranges

    return ohlc_correct_ranges


def _log_ohlc_split_range(
    df2: pd.DataFrame,
    range_item: tuple[int, int, str],
    column: str,
    multiplier: float,
    context: _SplitRepairContext,
) -> None:
    if context.interday:
        msg = (
            f"Corrected {context.fix_type} on col={column} range="
            f"[{_index_date(df2.index, range_item[1] - 1)}:{_index_date(df2.index, range_item[0])}] "
            f"m={multiplier:.4f}"
        )
    else:
        msg = (
            f"Corrected {context.fix_type} on col={column} range="
            f"[{df2.index[range_item[1] - 1]}:{df2.index[range_item[0]]}] "
            f"m={multiplier:.4f}"
        )
    context.logger.debug(msg, extra=context.log_extras)


def _multiply_column_slice(
    df2: pd.DataFrame,
    range_item: tuple[int, int, str],
    column: str,
    multiplier: float,
) -> None:
    start, end, _ = range_item
    df2.loc[df2.index[start:end], column] *= multiplier


def _apply_ohlc_split_range(
    df2: pd.DataFrame,
    range_item: tuple[int, int, str],
    column: str,
    context: _SplitRepairContext,
) -> float:
    multiplier, m_rcp = _split_adjustment_multipliers(
        range_item[2], context.split, context.split_rcp
    )
    _log_ohlc_split_range(df2, range_item, column, multiplier, context)
    _multiply_column_slice(df2, range_item, column, multiplier)
    if column == "Close":
        _multiply_column_slice(df2, range_item, "Adj Close", multiplier)
    return m_rcp


def _mark_ohlc_volume_fix_flags(
    f_open_fixed: np.ndarray,
    f_close_fixed: np.ndarray,
    range_item: tuple[int, int, str],
    column: str,
) -> None:
    start, end, _ = range_item
    if column == "Open":
        f_open_fixed[start:end] = True
    elif column == "Close":
        f_close_fixed[start:end] = True


def _apply_split_volume_corrections(
    df2: pd.DataFrame,
    f_open_fixed: np.ndarray,
    f_close_fixed: np.ndarray,
    m_rcp: float,
) -> None:
    f_open_and_closed_fixed = f_open_fixed & f_close_fixed
    f_open_xor_closed_fixed = np.logical_xor(f_open_fixed, f_close_fixed)
    if f_open_and_closed_fixed.any():
        df2.loc[f_open_and_closed_fixed, "Volume"] = (
            (df2.loc[f_open_and_closed_fixed, "Volume"] * m_rcp).round().astype("int")
        )
    if f_open_xor_closed_fixed.any():
        df2.loc[f_open_xor_closed_fixed, "Volume"] = (
            (df2.loc[f_open_xor_closed_fixed, "Volume"] * 0.5 * m_rcp)
            .round()
            .astype("int")
        )


def _log_full_split_range(
    df2: pd.DataFrame,
    range_item: tuple[int, int, str],
    context: _SplitRepairContext,
) -> None:
    if range_item[0] == range_item[1] - 1:
        if context.interday:
            msg = (
                f"Corrected {context.fix_type} on interval "
                f"{_index_date(df2.index, range_item[0])}"
            )
        else:
            msg = f"Corrected {context.fix_type} on interval {df2.index[range_item[0]]}"
    else:
        start = df2.index[range_item[1] - 1]
        end = df2.index[range_item[0]]
        if context.interday:
            msg = (
                f"Corrected {context.fix_type} across intervals "
                f"{_safe_timestamp(start).date()} -> {_safe_timestamp(end).date()} (inclusive)"
            )
        else:
            msg = (
                f"Corrected {context.fix_type} across intervals "
                f"{start} -> {end} (inclusive)"
            )
    context.logger.debug(msg, extra=context.log_extras)


def _apply_ohlc_split_ranges(
    df2: pd.DataFrame,
    signals: _SplitColumnSignals,
    ohlc_correct_ranges: list[Optional[list[tuple[int, int, str]]]],
    context: _SplitRepairContext,
) -> None:
    f_corrected = np.full(df2.shape[0], False)
    f_open_fixed = np.full(df2.shape[0], False)
    f_close_fixed = np.full(df2.shape[0], False)
    m_rcp = 1.0
    corrected_counts = [0, 0, 0, 0]

    for column_index, column in enumerate(signals.ohlc):
        ranges = ohlc_correct_ranges[column_index] or []
        for range_item in ranges:
            m_rcp = _apply_ohlc_split_range(df2, range_item, column, context)
            corrected_counts[column_index] += range_item[1] - range_item[0]
            if context.correct_volume:
                _mark_ohlc_volume_fix_flags(f_open_fixed, f_close_fixed, range_item, column)
            f_corrected[range_item[0] : range_item[1]] = True

    if sum(corrected_counts) > 0:
        counts_pretty = []
        for column_index, column in enumerate(signals.ohlc):
            if corrected_counts[column_index] != 0:
                counts_pretty.append(f"{column}={corrected_counts[column_index]}x")
        context.logger.info(f"Corrected: {', '.join(counts_pretty)}", extra=context.log_extras)

    if context.correct_volume:
        _apply_split_volume_corrections(df2, f_open_fixed, f_close_fixed, m_rcp)

    df2.loc[f_corrected, "Repaired?"] = True


def _repair_split_ranges_by_column(
    df2: pd.DataFrame,
    signals: _SplitColumnSignals,
    context: _SplitRepairContext,
) -> None:
    ohlc_correct_ranges = _collect_ohlc_split_ranges(signals, context)
    count = sum(1 for ranges in ohlc_correct_ranges if ranges is not None)
    if count == 1:
        indices = [i if ohlc_correct_ranges[i] else -1 for i in range(len(signals.ohlc))]
        only_index = np.where(np.array(indices) != -1)[0][0]
        only_column = signals.ohlc[only_index]
        context.logger.debug(
            f"Potential {context.fix_type} detected only in column {only_column}, so "
            "treating as false positive (ignore)",
            extra=context.log_extras,
        )
        return
    if count <= 1:
        return
    _apply_ohlc_split_ranges(df2, signals, ohlc_correct_ranges, context)


def _repair_split_ranges(
    df2: pd.DataFrame,
    f: np.ndarray,
    f_up: np.ndarray,
    f_down: np.ndarray,
    context: _SplitRepairContext,
) -> None:
    ranges = _build_pruned_split_ranges(f, f_up, f_down, context)
    n_corrected = 0

    for range_item in ranges:
        multiplier, m_rcp = _split_adjustment_multipliers(
            range_item[2], context.split, context.split_rcp
        )
        context.logger.debug(f"range={range_item} m={multiplier}", extra=context.log_extras)
        for column in ["Open", "High", "Low", "Close", "Adj Close"]:
            _multiply_column_slice(df2, range_item, column, multiplier)
        if context.correct_dividend:
            _multiply_column_slice(df2, range_item, "Dividends", multiplier)
        if context.correct_volume:
            volume_index = df2.index[range_item[0] : range_item[1]]
            df2.loc[volume_index, "Volume"] = (
                (df2.loc[volume_index, "Volume"] * m_rcp).round().astype("int")
            )
        df2.loc[df2.index[range_item[0] : range_item[1]], "Repaired?"] = True
        _log_full_split_range(df2, range_item, context)
        n_corrected += range_item[1] - range_item[0]

    if len(ranges) <= 2:
        msg = "Corrected:"
        for range_item in ranges:
            msg += (
                f" {_index_date(df2.index, range_item[1] - 1)}"
                f" -> {_index_date(df2.index, range_item[0])}"
            )
    else:
        msg = f"Corrected: {n_corrected}x"
    context.logger.info(msg, extra=context.log_extras)
