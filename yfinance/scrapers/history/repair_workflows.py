"""Price repair workflows for the history package."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import numpy as np
import pandas as pd

from yfinance import utils
from yfinance.const import _PRICE_COLNAMES_

from yfinance.scrapers.history.helpers import _get_scipy_ndimage


@dataclass
class _RepairWorkflowContext:
    price_history: Any
    interval: str
    prepost: bool
    tz_exchange: Any
    intraday: bool
    logger: Any
    log_extras: dict[str, Any]


@dataclass
class _TaggedRepairData:
    source_df: pd.DataFrame
    data_cols: list[str]
    tag: float


@dataclass
class _UnitMixupInputs:
    df_work: pd.DataFrame
    zero_rows: pd.DataFrame | None
    repair: _TaggedRepairData


@dataclass
class _UnitMixupPlan:
    tagged_df: pd.DataFrame
    repair: _TaggedRepairData
    hundred_mask: np.ndarray
    reciprocal_mask: np.ndarray
    n_before: int


@dataclass
class _ZeroRepairInputs:
    df_work: pd.DataFrame
    reserve_df: pd.DataFrame | None
    price_cols: list[str]
    price_mask: np.ndarray


@dataclass
class _ZeroRepairPlan:
    tagged_df: pd.DataFrame
    repair: _TaggedRepairData
    tagged_dates: pd.Index
    n_before: int


def _build_workflow_context(
    price_history,
    interval,
    tz_exchange,
    prepost,
    category: str,
) -> _RepairWorkflowContext:
    logger = utils.get_yf_logger()
    return _RepairWorkflowContext(
        price_history=price_history,
        interval=interval,
        prepost=prepost,
        tz_exchange=tz_exchange,
        intraday=interval[-1] in ("m", "h"),
        logger=logger,
        log_extras={
            "yf_cat": category,
            "yf_interval": interval,
            "yf_symbol": price_history.ticker,
        },
    )


def _ensure_repaired_flag(df: pd.DataFrame) -> pd.DataFrame:
    if "Repaired?" not in df.columns:
        df["Repaired?"] = False
    return df


def _localize_index(df: pd.DataFrame, tz_exchange) -> pd.DataFrame:
    df_index = pd.DatetimeIndex(df.index)
    if df_index.tz is None:
        df.index = df_index.tz_localize(tz_exchange)
    elif df_index.tz != tz_exchange:
        df.index = df_index.tz_convert(tz_exchange)
    return df


def _split_zero_rows(
    df: pd.DataFrame,
    original_df: pd.DataFrame,
    data_cols: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
    zero_mask = (df[data_cols] == 0).any(axis=1).to_numpy()
    if not zero_mask.any():
        return df, original_df, None
    return df[~zero_mask], original_df[~zero_mask], df[zero_mask]


def _get_unit_mixup_masks(
    df: pd.DataFrame,
    data_cols: list[str],
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    ndimage = _get_scipy_ndimage()
    df_data = df[data_cols].to_numpy()
    median = ndimage.median_filter(df_data, size=(3, 3), mode="wrap")
    ratio = df_data / median
    ratio_rounded = (ratio / 20).round() * 20
    ratio_rcp = 1.0 / ratio
    ratio_rcp_rounded = (ratio_rcp / 20).round() * 20
    hundred_mask = ratio_rounded == 100
    reciprocal_mask = (ratio_rounded == 100) | (ratio_rcp_rounded == 100)
    return hundred_mask, reciprocal_mask, hundred_mask | reciprocal_mask


def _tag_unit_mixup_candidates(
    df: pd.DataFrame,
    repair: _TaggedRepairData,
) -> _UnitMixupPlan | None:
    data_cols = repair.data_cols
    hundred_mask, reciprocal_mask, either_mask = _get_unit_mixup_masks(df, data_cols)
    if not either_mask.any():
        return None
    tagged_df = df.copy()
    for index, column in enumerate(data_cols):
        tagged_df.loc[either_mask[:, index], column] = repair.tag
    return _UnitMixupPlan(
        tagged_df=tagged_df,
        repair=repair,
        hundred_mask=hundred_mask,
        reciprocal_mask=reciprocal_mask,
        n_before=int((tagged_df[data_cols].to_numpy() == repair.tag).sum()),
    )


def _crudely_fill_remaining_unit_mixups(
    df: pd.DataFrame,
    repair: _TaggedRepairData,
    mask: np.ndarray,
    multiplier: float,
) -> None:
    data_cols = repair.data_cols
    source_df = repair.source_df
    remaining_mask = (df[data_cols].to_numpy() == repair.tag) & mask
    if not remaining_mask.any():
        return
    for row_index in range(remaining_mask.shape[0]):
        row_mask = remaining_mask[row_index, :]
        if not row_mask.any():
            continue
        idx = df.index[row_index]
        for column in ["Open", "Close"]:
            column_index = data_cols.index(column)
            if row_mask[column_index]:
                df.loc[idx, column] = cast(Any, source_df.loc[idx, column]) * multiplier
        for column, agg in (("High", "max"), ("Low", "min")):
            column_index = data_cols.index(column)
            if row_mask[column_index]:
                df.loc[idx, column] = df.loc[idx, ["Open", "Close"]].agg(agg)


def _restore_tagged_values(
    df: pd.DataFrame,
    repair: _TaggedRepairData,
) -> pd.DataFrame:
    data_cols = repair.data_cols
    tagged_mask = df[data_cols].to_numpy() == repair.tag
    for index, column in enumerate(data_cols):
        column_mask = tagged_mask[:, index]
        if column_mask.any():
            df.loc[column_mask, column] = repair.source_df.loc[column_mask, column]
    return df


def _prepare_unit_mixup_inputs(
    df: pd.DataFrame,
    context: _RepairWorkflowContext,
) -> _UnitMixupInputs | None:
    df_work = _localize_index(df.copy(), context.tz_exchange)
    data_cols = [
        column
        for column in ["High", "Open", "Low", "Close", "Adj Close"]
        if column in df_work.columns
    ]
    df_work, df_orig, zero_rows = _split_zero_rows(df_work, df, data_cols)
    if df_work.shape[0] <= 1:
        context.logger.info(
            "Insufficient good data for detecting 100x price errors",
            extra=context.log_extras,
        )
        return None
    return _UnitMixupInputs(
        df_work=df_work,
        zero_rows=zero_rows,
        repair=_TaggedRepairData(source_df=df_orig, data_cols=data_cols, tag=-1.0),
    )


def _merge_reserved_rows(
    repaired_df: pd.DataFrame,
    reserved_df: pd.DataFrame | None,
) -> pd.DataFrame:
    if reserved_df is None:
        return repaired_df
    _ensure_repaired_flag(reserved_df)
    merged = pd.concat([repaired_df, reserved_df]).sort_index()
    merged.index = pd.to_datetime(merged.index)
    return merged


def _log_unit_mixup_result(
    logger,
    log_extras: dict[str, Any],
    n_before: int,
    n_after: int,
    n_after_crude: int,
) -> None:
    n_fixed = n_before - n_after_crude
    n_fixed_crudely = n_after - n_after_crude
    if n_fixed <= 0:
        return
    report_msg = f"fixed {n_fixed}/{n_before} currency unit mixups "
    if n_fixed_crudely > 0:
        report_msg += f"({n_fixed_crudely} crudely)"
    logger.info(report_msg, extra=log_extras)


def fix_unit_random_mixups(price_history, df, interval, tz_exchange, prepost):
    """Fix sporadic 100x unit mixups in Yahoo price data."""
    if df.empty:
        return df

    context = _build_workflow_context(
        price_history,
        interval,
        tz_exchange,
        prepost,
        "price-repair-100x",
    )
    if df.shape[0] <= 1:
        if df.shape[0] == 1:
            context.logger.debug(
                "Cannot check single-row table for 100x price errors",
                extra=context.log_extras,
            )
        return _ensure_repaired_flag(df)

    inputs = _prepare_unit_mixup_inputs(df, context)
    if inputs is None:
        return _ensure_repaired_flag(df)

    plan = _tag_unit_mixup_candidates(inputs.df_work, inputs.repair)
    if plan is None:
        context.logger.debug("No sporadic 100x errors", extra=context.log_extras)
        return _ensure_repaired_flag(df)

    repaired_df = cast(Any, price_history).reconstruct_intervals_batch(
        plan.tagged_df,
        interval,
        prepost,
        plan.repair.tag,
    )
    n_after = int((repaired_df[plan.repair.data_cols].to_numpy() == plan.repair.tag).sum())
    n_after_crude = n_after
    if n_after > 0:
        _crudely_fill_remaining_unit_mixups(
            repaired_df,
            plan.repair,
            plan.hundred_mask,
            0.01,
        )
        _crudely_fill_remaining_unit_mixups(
            repaired_df,
            plan.repair,
            plan.reciprocal_mask,
            100.0,
        )
        n_after_crude = int(
            (repaired_df[plan.repair.data_cols].to_numpy() == plan.repair.tag).sum()
        )

    _log_unit_mixup_result(
        context.logger,
        context.log_extras,
        plan.n_before,
        n_after,
        n_after_crude,
    )
    repaired_df = _restore_tagged_values(repaired_df, plan.repair)
    return _merge_reserved_rows(repaired_df, inputs.zero_rows)


def _filter_intraday_reserve_rows(
    df: pd.DataFrame,
    price_cols: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame | None, np.ndarray]:
    df_index = pd.DatetimeIndex(df.index)
    df_dates = np.array([dt.date() for dt in df_index])
    price_mask = (df[price_cols] == 0.0) | df[price_cols].isna()
    grp = pd.Series(price_mask.any(axis=1), name="nan").groupby(df_dates)
    nan_pct = grp.sum() / grp.count()
    ignored_dates = nan_pct.index[nan_pct > 0.5]
    reserve_mask = np.isin(df_dates, ignored_dates)
    reserve_df = df[reserve_mask]
    filtered_df = df[~reserve_mask].copy()
    filtered_mask = (
        (filtered_df[price_cols] == 0.0) | filtered_df[price_cols].isna()
    ).to_numpy()
    return filtered_df, reserve_df, filtered_mask


def _get_zero_repair_flags(
    price_history,
    df: pd.DataFrame,
    intraday: bool,
) -> tuple[np.ndarray, np.ndarray | None]:
    price_change_mask = df["High"].to_numpy() != df["Low"].to_numpy()
    if price_history.ticker.endswith("=X"):
        return price_change_mask, None
    high_low_good = (~df["High"].isna().to_numpy()) & (~df["Low"].isna().to_numpy())
    volume_zero = (df["Volume"] == 0).to_numpy()
    volume_bad = volume_zero & high_low_good & price_change_mask
    if not intraday:
        close_diff = df["Close"].diff()
        close_diff.iloc[0] = 0
        close_change_pct = np.abs(close_diff / df["Close"])
        volume_bad = volume_bad | (np.asarray(close_change_pct > 0.05) & volume_zero)
    return price_change_mask, volume_bad


def _apply_split_expectation_mask(
    df: pd.DataFrame,
    price_mask: np.ndarray,
    price_change_mask: np.ndarray,
) -> np.ndarray:
    if "Stock Splits" not in df.columns:
        return price_mask
    split_mask = (df["Stock Splits"] != 0.0).to_numpy()
    if split_mask.any():
        price_mask[split_mask & ~price_change_mask] = True
    return price_mask


def _tag_zero_repair_targets(
    df: pd.DataFrame,
    price_cols: list[str],
    price_mask: np.ndarray,
    price_change_mask: np.ndarray,
    volume_bad: np.ndarray | None,
) -> _ZeroRepairPlan:
    data_cols = price_cols + ["Volume"]
    repair = _TaggedRepairData(source_df=df, data_cols=data_cols, tag=-1.0)
    tagged_df = df.copy()
    for index, column in enumerate(price_cols):
        tagged_df.loc[price_mask[:, index], column] = repair.tag
    if volume_bad is not None:
        tagged_df.loc[volume_bad, "Volume"] = repair.tag
    volume_zero_or_nan = (
        (tagged_df["Volume"].to_numpy() == 0)
        | tagged_df["Volume"].isna().to_numpy()
    )
    tagged_df.loc[price_mask.any(axis=1) & volume_zero_or_nan, "Volume"] = repair.tag
    tagged_df.loc[price_change_mask & volume_zero_or_nan, "Volume"] = repair.tag
    tagged_mask = tagged_df[data_cols].to_numpy() == repair.tag
    return _ZeroRepairPlan(
        tagged_df=tagged_df,
        repair=repair,
        tagged_dates=tagged_df.index[tagged_mask.any(axis=1)],
        n_before=int(tagged_mask.sum()),
    )


def _log_zero_repair_result(
    context: _RepairWorkflowContext,
    n_before: int,
    n_after: int,
    tagged_dates: pd.Index,
    unrepaired_dates: pd.Index,
) -> None:
    n_fixed = n_before - n_after
    if n_fixed <= 0:
        return
    msg = (
        f"{context.price_history.ticker}: fixed {n_fixed}/{n_before} "
        f"value=0 errors in {context.interval} price data"
    )
    if n_fixed < 4:
        repaired_dates = sorted(list(set(tagged_dates).difference(unrepaired_dates)))
        msg += f": {repaired_dates}"
    context.logger.debug(msg, extra=context.log_extras)


def _prepare_zero_repair_inputs(
    context: _RepairWorkflowContext,
    df: pd.DataFrame,
) -> _ZeroRepairInputs | None:
    df_work = _localize_index(df.sort_index().copy(), context.tz_exchange)
    price_cols = [column for column in _PRICE_COLNAMES_ if column in df_work.columns]
    price_mask = ((df_work[price_cols] == 0.0) | df_work[price_cols].isna()).to_numpy()
    reserve_df = None
    if context.intraday:
        df_work, reserve_df, price_mask = _filter_intraday_reserve_rows(df_work, price_cols)
        if df_work.empty:
            return None
    return _ZeroRepairInputs(
        df_work=df_work,
        reserve_df=reserve_df,
        price_cols=price_cols,
        price_mask=price_mask,
    )


def _has_zero_repair_targets(
    context: _RepairWorkflowContext,
    df_work: pd.DataFrame,
    price_cols: list[str],
    price_mask: np.ndarray,
    volume_bad: np.ndarray | None,
) -> bool:
    bad_rows = price_mask.any(axis=1)
    if volume_bad is not None:
        bad_rows = bad_rows | volume_bad
    if not bad_rows.any():
        context.logger.debug("No price=0 errors to repair", extra=context.log_extras)
        return False
    if int(price_mask.sum()) == len(price_cols) * len(df_work):
        context.logger.debug(
            "No good data for calibration so cannot fix price=0 bad data",
            extra=context.log_extras,
        )
        return False
    return True


def _finalize_zero_repair(
    repaired_df: pd.DataFrame,
    inputs: _ZeroRepairInputs,
    repair: _TaggedRepairData,
) -> pd.DataFrame:
    repaired_df = _merge_reserved_rows(repaired_df, inputs.reserve_df)
    return _restore_tagged_values(repaired_df, repair)


def fix_zeroes(price_history, df, interval, tz_exchange, prepost):
    """Repair zero or missing prices where Yahoo likely returned bad data."""
    if df.empty:
        return df

    context = _build_workflow_context(
        price_history,
        interval,
        tz_exchange,
        prepost,
        "price-repair-zeroes",
    )
    inputs = _prepare_zero_repair_inputs(context, df)
    if inputs is None:
        return _ensure_repaired_flag(df)

    price_change_mask, volume_bad = _get_zero_repair_flags(
        price_history,
        inputs.df_work,
        context.intraday,
    )
    price_mask = _apply_split_expectation_mask(
        inputs.df_work,
        inputs.price_mask,
        price_change_mask,
    )
    if not _has_zero_repair_targets(
        context,
        inputs.df_work,
        inputs.price_cols,
        price_mask,
        volume_bad,
    ):
        return _ensure_repaired_flag(df)

    plan = _tag_zero_repair_targets(
        inputs.df_work,
        inputs.price_cols,
        price_mask,
        price_change_mask,
        volume_bad,
    )
    repaired_df = cast(Any, price_history).reconstruct_intervals_batch(
        plan.tagged_df,
        interval,
        prepost,
        plan.repair.tag,
    )
    repaired_mask = repaired_df[plan.repair.data_cols].to_numpy() == plan.repair.tag
    n_after = int(repaired_mask.sum())
    unrepaired_dates = repaired_df.index[repaired_mask.any(axis=1)]
    _log_zero_repair_result(
        context,
        plan.n_before,
        n_after,
        plan.tagged_dates,
        unrepaired_dates,
    )
    return _finalize_zero_repair(repaired_df, inputs, plan.repair)
