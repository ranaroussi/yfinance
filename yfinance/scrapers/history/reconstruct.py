"""Interval reconstruction helpers extracted from the history package."""

from __future__ import annotations

import datetime as _datetime
import logging
from dataclasses import dataclass
from typing import Any, Iterable, cast

import dateutil as _dateutil
import numpy as np
import pandas as pd

from yfinance.const import _PRICE_COLNAMES_
from yfinance.scrapers.history.helpers import _interval_to_timedelta
from ... import utils


_RECONSTRUCT_INTERVALS = ["1wk", "1d", "1h", "30m", "15m", "5m", "2m", "1m"]
_RECONSTRUCT_ITDS = {
    interval: _interval_to_timedelta(interval)
    for interval in _RECONSTRUCT_INTERVALS
}
_RECONSTRUCT_NEXTS = {
    _RECONSTRUCT_INTERVALS[index]: _RECONSTRUCT_INTERVALS[index + 1]
    for index in range(len(_RECONSTRUCT_INTERVALS) - 1)
}
_MIN_LOOKBACKS = {
    "1wk": None,
    "1d": None,
    "1h": _datetime.timedelta(days=730),
    "30m": _datetime.timedelta(days=60),
    "15m": _datetime.timedelta(days=60),
    "5m": _datetime.timedelta(days=60),
    "2m": _datetime.timedelta(days=60),
    "1m": _datetime.timedelta(days=30),
}


@dataclass
class _ReconstructContext:
    price_history: Any
    history_obj: Any
    interval: str
    prepost: bool
    intraday: bool
    sub_interval: str
    td_range: Any
    min_lookback: _datetime.timedelta | None
    itds: dict[str, _datetime.timedelta]
    nexts: dict[str, str]
    logger: Any
    log_extras: dict[str, Any]


@dataclass
class _TargetInfo:
    df_index: pd.DatetimeIndex
    f_repair: np.ndarray
    f_repair_rows: np.ndarray
    min_dt: pd.Timestamp | None


@dataclass
class _CalibrationBlock:
    df_new: pd.DataFrame
    df_block: pd.DataFrame
    common_index: pd.DatetimeIndex
    df_fine_grp: Any
    price_cols: list[str]
    tag: float


@dataclass
class _BatchRepairState:
    source_df: pd.DataFrame
    repaired_df: pd.DataFrame
    price_cols: list[str]
    tag: float
    min_dt: pd.Timestamp | None


@dataclass
class _BatchRepairPlan:
    state: _BatchRepairState
    df_good: pd.DataFrame
    dts_to_repair: pd.DatetimeIndex


@dataclass
class _RowRepairContext:
    state: _BatchRepairState
    df_new: pd.DataFrame
    df_fine: pd.DataFrame
    interval: str


def _ensure_repaired_flag(df: pd.DataFrame) -> pd.DataFrame:
    if "Repaired?" not in df.columns:
        df["Repaired?"] = False
    return df


def _is_interday_interval(interval: str) -> bool:
    return interval[1:] in ["d", "wk", "mo"]


def _build_reconstruct_context(
    price_history,
    interval,
    prepost,
    log_state: tuple[Any, dict[str, Any]],
):
    logger, log_extras = log_state
    if interval not in _RECONSTRUCT_NEXTS:
        logger.warning(
            f"Have not implemented price reconstruct for '{interval}' interval. Contact developers"
        )
        return None
    intraday = not _is_interday_interval(interval)
    sub_interval = _RECONSTRUCT_NEXTS[interval]
    logger.debug(
        f"Using sub-interval {sub_interval} for {interval} reconstruction",
        extra=log_extras,
    )
    return _ReconstructContext(
        price_history=price_history,
        history_obj=cast(Any, price_history),
        interval=interval,
        prepost=prepost if intraday else True,
        intraday=intraday,
        sub_interval=sub_interval,
        td_range=_RECONSTRUCT_ITDS[interval],
        min_lookback=_MIN_LOOKBACKS[sub_interval],
        itds=_RECONSTRUCT_ITDS,
        nexts=_RECONSTRUCT_NEXTS,
        logger=logger,
        log_extras=log_extras,
    )


def _prepare_targets(df, data_cols, tag, context: _ReconstructContext) -> _TargetInfo:
    df_index = pd.DatetimeIndex(df.index)
    f_repair = df[data_cols].to_numpy() == tag
    f_repair_rows = f_repair.any(axis=1)
    min_dt = None
    if context.min_lookback is not None:
        min_dt = pd.Timestamp.now("UTC") - (
            context.min_lookback - _datetime.timedelta(days=1)
        )
        df_tz = df_index.tz if df_index.tz is not None else "UTC"
        min_dt = min_dt.tz_convert(df_tz).ceil("D")
        f_repair_rows = f_repair_rows & (df_index >= min_dt)
    context.logger.debug(
        f"min_dt={min_dt} interval={context.interval} sub_interval={context.sub_interval}",
        extra=context.log_extras,
    )
    return _TargetInfo(
        df_index=df_index,
        f_repair=f_repair,
        f_repair_rows=f_repair_rows,
        min_dt=min_dt,
    )


def _group_max_size(sub_interval: str):
    if sub_interval in ["1wk", "1d"]:
        return _dateutil.relativedelta.relativedelta(years=2)
    if sub_interval == "1h":
        return _dateutil.relativedelta.relativedelta(years=1)
    if sub_interval == "1m":
        return _datetime.timedelta(days=5)
    return _datetime.timedelta(days=30)


def _extend_group_with_neighbors(
    group: list[pd.Timestamp],
    df_good_index: pd.DatetimeIndex,
    intraday: bool,
    min_dt: pd.Timestamp | None,
) -> list[pd.Timestamp]:
    first_group_dt = group[0]
    start_index = int(df_good_index.get_indexer([first_group_dt], method="nearest")[0])
    if start_index > 0:
        prev_dt = cast(pd.Timestamp, df_good_index[start_index - 1])
        if (min_dt is None or prev_dt >= min_dt) and (
            (not intraday) or prev_dt.date() == first_group_dt.date()
        ):
            start_index -= 1
    last_group_dt = group[-1]
    end_index = int(df_good_index.get_indexer([last_group_dt], method="nearest")[0])
    if end_index < len(df_good_index) - 1:
        next_dt = cast(pd.Timestamp, df_good_index[end_index + 1])
        if (not intraday) or next_dt.date() == last_group_dt.date():
            end_index += 1
    good_dts = [cast(pd.Timestamp, dt) for dt in df_good_index[start_index : end_index + 1]]
    extended_group = group + good_dts
    extended_group.sort()
    return extended_group


def _build_reconstruct_groups(
    dts_to_repair: pd.DatetimeIndex,
    df_good_index: pd.DatetimeIndex,
    context: _ReconstructContext,
    min_dt: pd.Timestamp | None,
) -> list[list[pd.Timestamp]]:
    dts_groups: list[list[pd.Timestamp]] = [[cast(pd.Timestamp, dts_to_repair[0])]]
    grp_max_size = _group_max_size(context.sub_interval)
    context.logger.debug(f"grp_max_size = {grp_max_size}", extra=context.log_extras)
    for index in range(1, len(dts_to_repair)):
        dt = cast(pd.Timestamp, dts_to_repair[index])
        if dt.date() < dts_groups[-1][0].date() + grp_max_size:
            dts_groups[-1].append(dt)
        else:
            dts_groups.append([dt])
    context.logger.debug("Repair groups:", extra=context.log_extras)
    for group in dts_groups:
        context.logger.debug(f"- {group[0]} -> {group[-1]}")
    return [
        _extend_group_with_neighbors(group, df_good_index, context.intraday, min_dt)
        for group in dts_groups
    ]


def _resolve_start_interval(context: _ReconstructContext) -> str | None:
    if context.history_obj.get_reconstruct_start_interval() is None:
        context.history_obj.set_reconstruct_start_interval(context.interval)
    return context.history_obj.get_reconstruct_start_interval()


def _within_reconstruct_depth(
    context: _ReconstructContext,
    start_interval: str,
) -> bool:
    if context.interval in (start_interval, context.nexts[start_interval]):
        return True
    context.logger.info(
        (
            f"Hit max depth of 2 ('{start_interval}'"
            f"->'{context.nexts[start_interval]}'->'{context.interval}')"
        ),
        extra=context.log_extras,
    )
    return False


def _build_fetch_window(
    group: list[pd.Timestamp],
    context: _ReconstructContext,
    min_dt: pd.Timestamp | None,
) -> tuple[_datetime.date | pd.Timestamp, _datetime.date | pd.Timestamp]:
    td_1d = _datetime.timedelta(days=1)
    if context.interval == "1wk":
        fetch_start = group[0].date() - context.td_range
        fetch_end = group[-1].date() + context.td_range
    elif context.interval == "1d":
        fetch_start = group[0].date()
        fetch_end = group[-1].date() + context.td_range
    else:
        fetch_start = group[0]
        fetch_end = group[-1] + context.td_range
    fetch_start -= td_1d
    fetch_end += td_1d
    if context.intraday:
        if isinstance(fetch_start, pd.Timestamp):
            fetch_start = fetch_start.date()
        if isinstance(fetch_end, pd.Timestamp):
            fetch_end = fetch_end.date() + td_1d
    if min_dt is not None:
        fetch_start = max(min_dt.date(), fetch_start)
    return fetch_start, fetch_end


def _group_too_old(context: _ReconstructContext, start_d: _datetime.date) -> bool:
    age = _datetime.date.today() - start_d
    if context.sub_interval == "1h":
        return age > _datetime.timedelta(days=729)
    if context.sub_interval in ["30m", "15m"]:
        return age > _datetime.timedelta(days=59)
    return False


def _fetch_fine_history(context: _ReconstructContext, fetch_start, fetch_end) -> pd.DataFrame:
    temp_logger = utils.get_yf_logger()
    raw_logger = (
        temp_logger.logger
        if isinstance(temp_logger, logging.LoggerAdapter)
        else temp_logger
    )
    log_level = raw_logger.level
    raw_logger.setLevel(logging.CRITICAL)
    try:
        return context.price_history.history(
            start=fetch_start,
            end=fetch_end,
            interval=context.sub_interval,
            auto_adjust=False,
            actions=True,
            prepost=context.prepost,
            repair=True,
            keepna=True,
        )
    finally:
        raw_logger.setLevel(log_level)


def _add_grouping_column(df_fine: pd.DataFrame, df_block: pd.DataFrame, interval: str) -> str:
    df_fine_index = pd.DatetimeIndex(df_fine.index)
    if interval == "1wk":
        weekdays = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
        week_end_day = weekdays[(cast(pd.Timestamp, df_block.index[0]).weekday() + 6) % 7]
        week_periods = df_fine_index.tz_localize(None).to_period("W-" + week_end_day)
        df_fine["Week Start"] = week_periods.to_timestamp(how="start")
        return "Week Start"
    if interval == "1d":
        df_fine["Day Start"] = pd.to_datetime(
            [cast(pd.Timestamp, dt).date() for dt in df_fine_index]
        )
        return "Day Start"
    df_fine.loc[df_fine.index.isin(df_block.index), "ctr"] = 1
    df_fine["intervalID"] = df_fine["ctr"].cumsum()
    df_fine.drop("ctr", axis=1, inplace=True)
    return "intervalID"


def _build_df_new_index(
    df_fine: pd.DataFrame,
    current_index: pd.Index,
    grp_col: str,
) -> pd.DatetimeIndex:
    df_fine_index = pd.DatetimeIndex(df_fine.index)
    if grp_col in ["Week Start", "Day Start"]:
        return pd.DatetimeIndex(current_index).tz_localize(df_fine_index.tz)
    interval_ids = cast(pd.Series, df_fine["intervalID"])
    interval_diffs = cast(pd.Series, interval_ids.diff())
    extra_index = [
        cast(pd.Timestamp, df_fine_index[index])
        for index, is_new in enumerate(interval_diffs.to_numpy() > 0)
        if is_new
    ]
    return pd.DatetimeIndex([cast(pd.Timestamp, df_fine_index[0]), *extra_index])


def _prepare_grouped_fine_data(
    df_fine: pd.DataFrame,
    df_block: pd.DataFrame,
    context: _ReconstructContext,
    price_cols: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, Any]:
    df_fine = df_fine.loc[
        df_block.index[0] : df_block.index[-1]
        + context.itds[context.sub_interval]
        - _datetime.timedelta(milliseconds=1)
    ].copy()
    df_fine["ctr"] = 0
    grp_col = _add_grouping_column(df_fine, df_block, context.interval)
    df_fine = df_fine[~df_fine[price_cols + ["Dividends"]].isna().all(axis=1)]
    df_fine_grp = df_fine.groupby(grp_col)
    df_new = cast(
        pd.DataFrame,
        df_fine_grp.agg(
            Open=("Open", "first"),
            Close=("Close", "last"),
            AdjClose=("Adj Close", "last"),
            Low=("Low", "min"),
            High=("High", "max"),
            Dividends=("Dividends", "sum"),
            Volume=("Volume", "sum"),
        ).rename(columns={"AdjClose": "Adj Close"}),
    )
    df_new.index = _build_df_new_index(df_fine, df_new.index, grp_col)
    return df_fine, df_new, df_fine_grp


def _common_index(df_block: pd.DataFrame, df_new: pd.DataFrame) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(np.intersect1d(df_block.index, df_new.index))


def _repair_tagged_div_adjusts(
    div_adjusts: pd.Series,
    df_new: pd.DataFrame,
    df_new_calib: pd.DataFrame,
    tagged_adj_close: pd.Series,
) -> pd.Series:
    count = len(div_adjusts)
    for raw_index in np.flatnonzero(tagged_adj_close.to_numpy()):
        index = int(raw_index)
        dt = cast(pd.Timestamp, df_new_calib.index[index])
        if df_new.loc[dt, "Dividends"] != 0:
            if index < count - 1:
                div_adjusts.iloc[index] = div_adjusts.iloc[index + 1]
            else:
                div_adj = (
                    1.0
                    - df_new_calib["Dividends"].iloc[index]
                    / df_new_calib["Close"].iloc[index - 1]
                )
                div_adjusts.iloc[index] = div_adjusts.iloc[index - 1] / div_adj
            continue
        if index > 0:
            div_adjusts.iloc[index] = div_adjusts.iloc[index - 1]
            continue
        div_adjusts.iloc[index] = div_adjusts.iloc[index + 1]
        if df_new_calib["Dividends"].iloc[index + 1] != 0:
            div_adjusts.iloc[index] *= (
                1.0
                - df_new_calib["Dividends"].iloc[index + 1]
                / df_new_calib["Close"].iloc[index]
            )
    return div_adjusts


def _calibrate_daily_adj_close(
    df_new: pd.DataFrame,
    df_block: pd.DataFrame,
    common_index: pd.DatetimeIndex,
    tag: float,
) -> pd.DataFrame:
    common_new_index = df_new.index.intersection(common_index)
    common_block_index = df_block.index.intersection(common_index)
    df_new_calib = cast(pd.DataFrame, df_new.loc[common_new_index])
    df_block_calib = cast(pd.DataFrame, df_block.loc[common_block_index])
    tagged_adj_close = cast(pd.Series, df_block_calib["Adj Close"] == tag)
    if not tagged_adj_close.any():
        return df_new
    div_adjusts = cast(pd.Series, df_block_calib["Adj Close"] / df_block_calib["Close"])
    div_adjusts[tagged_adj_close] = np.nan
    if div_adjusts.isna().all():
        return df_new
    div_adjusts = div_adjusts.ffill().bfill()
    div_adjusts = _repair_tagged_div_adjusts(
        div_adjusts,
        df_new,
        df_new_calib,
        tagged_adj_close,
    )
    div_adjusts = cast(
        pd.Series,
        div_adjusts.reindex(df_block.index, fill_value=np.nan).ffill().bfill(),
    )
    df_new = df_new.copy()
    df_new["Adj Close"] = df_block["Close"] * div_adjusts
    close_bad = cast(pd.Series, df_block_calib["Close"] == tag)
    if close_bad.any():
        close_bad_new = cast(
            pd.Series,
            close_bad.reindex(df_new.index, fill_value=False),
        )
        div_adjusts_new = cast(
            pd.Series,
            div_adjusts.reindex(df_new.index, fill_value=np.nan).ffill().bfill(),
        )
        close_bad_mask = close_bad_new.to_numpy(dtype=bool)
        df_new.loc[close_bad_mask, "Adj Close"] = (
            df_new.loc[close_bad_mask, "Close"] * div_adjusts_new[close_bad_mask]
        )
    return df_new


def _replace_invalid_calibration_values(
    values: np.ndarray,
    calib_filter: np.ndarray,
) -> np.ndarray:
    writable = values.copy()
    for column_index in range(calib_filter.shape[1]):
        invalid = ~calib_filter[:, column_index]
        if invalid.any():
            writable[invalid, column_index] = 1
    return writable


def _calibrate_price_block(
    block: _CalibrationBlock,
) -> tuple[pd.DataFrame, float]:
    calib_cols = ["Open", "Close"]
    df_new = block.df_new
    df_block = block.df_block
    common_new_index = df_new.index.intersection(block.common_index)
    common_block_index = df_block.index.intersection(block.common_index)
    df_new_calib = cast(
        np.ndarray,
        df_new.loc[common_new_index, calib_cols].to_numpy(),
    )
    df_block_calib = cast(
        np.ndarray,
        df_block.loc[common_block_index, calib_cols].to_numpy(),
    )
    calib_filter = (df_block_calib != block.tag) & (~np.isnan(df_new_calib))
    if not calib_filter.any():
        return df_new, np.nan
    df_new_calib = _replace_invalid_calibration_values(df_new_calib, calib_filter)
    df_block_calib = _replace_invalid_calibration_values(df_block_calib, calib_filter)
    ratios = df_block_calib[calib_filter] / df_new_calib[calib_filter]
    weights = cast(pd.Series, block.df_fine_grp.size())
    weights.index = df_new.index
    weight_values = (
        weights.loc[weights.index.intersection(block.common_index)]
        .to_numpy()
        .astype(float)
    )
    weight_values = np.tile(weight_values[:, None], len(calib_cols))[calib_filter]
    not_one = ~np.isclose(ratios, 1.0, rtol=0.00001)
    ratio = (
        1.0
        if np.sum(not_one) == len(calib_cols)
        else float(np.average(ratios, weights=weight_values))
    )
    ratio_rcp = round(1.0 / ratio, 1)
    ratio = round(ratio, 1)
    if ratio == 1 and ratio_rcp == 1:
        return df_new, ratio
    df_new = df_new.copy()
    if ratio > 1:
        df_new[block.price_cols] *= ratio
        df_new["Volume"] /= ratio
    elif ratio_rcp > 1:
        df_new[block.price_cols] *= 1.0 / ratio_rcp
        df_new["Volume"] *= ratio_rcp
    return df_new, ratio


def _apply_hundred_x_block_fix(df_v2: pd.DataFrame, ratio: float, tag: float) -> float:
    if abs(ratio / 0.0001 - 1) >= 0.01:
        return ratio
    for column in _PRICE_COLNAMES_:
        df_v2.loc[df_v2[column] != tag, column] *= 100
    return ratio * 100


def _iter_bad_datetimes(
    df_block: pd.DataFrame,
    columns: Iterable[str],
    tag: float,
) -> pd.DatetimeIndex:
    bad_rows = (df_block[list(columns)] == tag).to_numpy().any(axis=1)
    return pd.DatetimeIndex(df_block.index[bad_rows.tolist()])


def _previous_week_close(
    df_new: pd.DataFrame,
    df_fine: pd.DataFrame,
    idx: pd.Timestamp,
    interval: str,
):
    if interval != "1wk":
        return None
    df_new_index = pd.DatetimeIndex(df_new.index)
    new_loc = df_new_index.get_loc(idx)
    if not isinstance(new_loc, (int, np.integer)):
        return None
    if int(new_loc) <= 0 or idx == df_fine.index[0]:
        return None
    return df_new.iloc[int(new_loc) - 1]["Close"]


def _repair_open_field(
    df_v2: pd.DataFrame,
    df_new_row: pd.Series,
    idx: pd.Timestamp,
    interval: str,
    previous_week,
) -> None:
    if interval == "1wk" and previous_week is not None:
        df_v2.loc[idx, "Open"] = previous_week
        df_v2.loc[idx, "Low"] = df_v2.loc[idx, ["Open", "Low"]].min()
        return
    df_v2.loc[idx, "Open"] = df_new_row["Open"]


def _repair_one_bad_row(
    context: _RowRepairContext,
    idx: pd.Timestamp,
) -> None:
    df_new_row = cast(pd.Series, context.df_new.loc[idx])
    previous_week = _previous_week_close(context.df_new, context.df_fine, idx, context.interval)
    source_row = cast(pd.Series, context.state.source_df.loc[idx])
    bad_fields = source_row[source_row == context.state.tag].index.to_numpy()
    if "High" in bad_fields:
        context.state.repaired_df.loc[idx, "High"] = df_new_row["High"]
    if "Low" in bad_fields:
        context.state.repaired_df.loc[idx, "Low"] = df_new_row["Low"]
    if "Open" in bad_fields:
        _repair_open_field(
            context.state.repaired_df,
            df_new_row,
            idx,
            context.interval,
            previous_week,
        )
    if "Close" in bad_fields:
        context.state.repaired_df.loc[idx, ["Close", "Adj Close"]] = df_new_row[
            ["Close", "Adj Close"]
        ]
    elif "Adj Close" in bad_fields:
        context.state.repaired_df.loc[idx, "Adj Close"] = df_new_row["Adj Close"]
    if "Volume" in bad_fields:
        context.state.repaired_df.loc[idx, "Volume"] = round(float(df_new_row["Volume"]))
    context.state.repaired_df.loc[idx, "Repaired?"] = True


def _repair_bad_rows(
    state: _BatchRepairState,
    df_new: pd.DataFrame,
    df_fine: pd.DataFrame,
    interval: str,
) -> None:
    row_context = _RowRepairContext(
        state=state,
        df_new=df_new,
        df_fine=df_fine,
        interval=interval,
    )
    bad_dts = _iter_bad_datetimes(
        state.repaired_df.loc[df_new.index.intersection(state.repaired_df.index)],
        _PRICE_COLNAMES_ + ["Volume"],
        state.tag,
    )
    for idx in bad_dts:
        if idx in df_new.index:
            _repair_one_bad_row(row_context, idx)


def _reconstruct_group(
    state: _BatchRepairState,
    group: list[pd.Timestamp],
    context: _ReconstructContext,
) -> None:
    df_block = state.source_df[state.source_df.index.isin(group)]
    context.logger.debug("df_block:\n" + str(df_block))
    start_dt = group[0]
    start_d = start_dt.date()
    if _group_too_old(context, start_d):
        context.logger.info(
            "Cannot reconstruct block starting "
            f"{start_dt if context.intraday else start_d}, too old, "
            "Yahoo will reject request for finer-grain data",
            extra=context.log_extras,
        )
        return
    fetch_start, fetch_end = _build_fetch_window(group, context, state.min_dt)
    context.logger.debug(
        f"Fetching {context.sub_interval} prepost={context.prepost} {fetch_start}->{fetch_end}",
        extra=context.log_extras,
    )
    df_fine = _fetch_fine_history(context, fetch_start, fetch_end)
    if df_fine.empty:
        context.logger.info(
            "Cannot reconstruct block starting "
            f"{start_dt if context.intraday else start_d}, too old, "
            "Yahoo will reject request for finer-grain data",
            extra=context.log_extras,
        )
        return
    df_fine, df_new, df_fine_grp = _prepare_grouped_fine_data(
        df_fine,
        df_block,
        context,
        state.price_cols,
    )
    if df_fine.empty:
        context.logger.info(
            "Cannot reconstruct "
            f"{context.interval} block range {start_dt if context.intraday else start_d}, "
            "Yahoo not returning finer-grain data within range",
            extra=context.log_extras,
        )
        return
    context.logger.debug("df_new:\n" + str(df_new))
    common_index = _common_index(df_block, df_new)
    if len(common_index) == 0:
        context.logger.info(
            f"Can't calibrate {context.interval} block starting {start_d} so aborting repair",
            extra=context.log_extras,
        )
        return
    if context.interval == "1d":
        df_new = _calibrate_daily_adj_close(df_new, df_block, common_index, state.tag)
    df_new, ratio = _calibrate_price_block(
        _CalibrationBlock(
            df_new=df_new,
            df_block=df_block,
            common_index=common_index,
            df_fine_grp=df_fine_grp,
            price_cols=state.price_cols,
            tag=state.tag,
        )
    )
    if np.isnan(ratio):
        context.logger.info(
            f"Can't calibrate block starting {start_d} so aborting repair",
            extra=context.log_extras,
        )
        return
    ratio = _apply_hundred_x_block_fix(state.repaired_df, ratio, state.tag)
    context.logger.debug(
        f"Price calibration ratio (raw) = {ratio:6f}",
        extra=context.log_extras,
    )
    missing_dts = [
        idx for idx in _iter_bad_datetimes(df_block, state.price_cols + ["Volume"], state.tag)
        if idx not in df_new.index
    ]
    if missing_dts:
        context.logger.debug(
            "Yahoo didn't return finer-grain data for these intervals: " + str(missing_dts),
            extra=context.log_extras,
        )
    _repair_bad_rows(state, df_new, df_fine, context.interval)


def _build_batch_repair_plan(
    df: pd.DataFrame,
    context: _ReconstructContext,
    tag: float,
) -> _BatchRepairPlan | None:
    price_cols = [column for column in _PRICE_COLNAMES_ if column in df]
    data_cols = price_cols + ["Volume"]
    source_df = df.sort_index()
    target_info = _prepare_targets(source_df, data_cols, tag, context)
    if not target_info.f_repair_rows.any():
        if target_info.min_dt is not None:
            context.logger.info(
                f"Too old ({np.sum(target_info.f_repair.any(axis=1))} rows tagged)",
                extra=context.log_extras,
            )
        return None
    dts_to_repair = pd.DatetimeIndex(target_info.df_index[target_info.f_repair_rows])
    if len(dts_to_repair) == 0:
        context.logger.debug(
            "Nothing needs repairing (dts_to_repair[] empty)",
            extra=context.log_extras,
        )
        return None
    repaired_df = _ensure_repaired_flag(source_df.copy())
    good_rows = ~(source_df[price_cols].isna().any(axis=1))
    good_rows = good_rows & (source_df[price_cols].to_numpy() != tag).all(axis=1)
    return _BatchRepairPlan(
        state=_BatchRepairState(
            source_df=source_df,
            repaired_df=repaired_df,
            price_cols=price_cols,
            tag=tag,
            min_dt=target_info.min_dt,
        ),
        df_good=source_df[good_rows],
        dts_to_repair=dts_to_repair,
    )


def _can_reconstruct(context: _ReconstructContext) -> bool:
    start_interval = _resolve_start_interval(context)
    if start_interval is None:
        return False
    return _within_reconstruct_depth(context, start_interval)


def reconstruct_intervals_batch(price_history, df, interval, prepost, tag=-1.0):
    """Repair tagged rows by rebuilding them from the next finer Yahoo interval."""
    if not isinstance(df, pd.DataFrame):
        raise ValueError("'df' must be a Pandas DataFrame not", type(df))
    if interval == "1m":
        return df
    log_state = (
        utils.get_yf_logger(),
        {
            "yf_cat": "price-reconstruct",
            "yf_interval": interval,
            "yf_symbol": price_history.ticker,
        },
    )
    context = _build_reconstruct_context(price_history, interval, prepost, log_state)
    if context is None or not _can_reconstruct(context):
        return _ensure_repaired_flag(df)
    plan = _build_batch_repair_plan(df, context, tag)
    if plan is None:
        return _ensure_repaired_flag(df)
    if plan.df_good.empty:
        return plan.state.repaired_df
    groups = _build_reconstruct_groups(
        plan.dts_to_repair,
        pd.DatetimeIndex(plan.df_good.index),
        context,
        plan.state.min_dt,
    )
    for group in groups:
        _reconstruct_group(plan.state, group, context)
    return plan.state.repaired_df
