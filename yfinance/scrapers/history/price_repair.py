"""Shared price-repair operations for the history package."""

import dateutil as _dateutil
import numpy as np

from yfinance.scrapers.history.helpers import (
    _LocalVolatilityContext,
    _PriceChangeRepairSettings,
    _SplitAbortContext,
    _SplitChangeMatrixContext,
    _SplitColumnSignals,
    _SplitRangeContext,
    _SplitRepairContext,
    _SplitThresholdContext,
    _estimate_split_repair_threshold,
    _filter_local_volatility_false_positives,
    _filter_split_false_positives,
    _log_split_repair_workings,
    _prepare_split_change_matrix,
    _repair_split_ranges,
    _repair_split_ranges_by_column,
    _should_abort_due_to_nearby_split,
)
from ... import utils


def _price_repair_setup(price_history, df, change, interval):
    logger = utils.get_yf_logger()
    log_extras = {
        "yf_cat": "price-change-repair",
        "yf_interval": interval,
        "yf_symbol": price_history.ticker,
    }

    split = change
    split_rcp = 1.0 / split
    if change in [100.0, 0.01]:
        fix_type = "100x error"
        log_extras["yf_cat"] = "price-repair-100x"
        start_min = None
    else:
        fix_type = "bad split"
        log_extras["yf_cat"] = "price-repair-split"
        f = df["Stock Splits"].to_numpy() != 0.0
        start_min = (df.index[f].min() - _dateutil.relativedelta.relativedelta(years=1)).date()
    logger.debug(
        f"start_min={start_min} change={change:.4f} (rcp={1.0 / change:.4f})",
        extra=log_extras,
    )
    return logger, log_extras, split, split_rcp, fix_type, start_min


def _price_repair_setup_context(price_history, df, settings):
    logger, log_extras, split, split_rcp, fix_type, start_min = _price_repair_setup(
        price_history,
        df,
        settings.change,
        settings.interval,
    )
    return {
        "logger": logger,
        "log_extras": log_extras,
        "split": split,
        "split_rcp": split_rcp,
        "fix_type": fix_type,
        "start_min": start_min,
        "correct_volume": settings.correct_volume,
        "correct_dividend": settings.correct_dividend,
        "change": settings.change,
        "interval": settings.interval,
    }


def _latest_active_index(df2, ohlc):
    f_no_activity = df2["Volume"] == 0
    f_no_activity = f_no_activity | df2[ohlc].isna().all(axis=1)
    appears_suspended = f_no_activity.any() and np.where(f_no_activity)[0][0] == 0
    f_active = ~f_no_activity
    idx_latest_active = np.where(f_active & np.roll(f_active, 1))[0]
    if len(idx_latest_active) == 0:
        idx_latest_active = np.where(f_active)[0]
    if len(idx_latest_active) == 0:
        return appears_suspended, None
    return appears_suspended, int(idx_latest_active[0])


def _build_debug_frame(df2, ohlc, debug_cols):
    df_workings = df2.copy()
    df_workings = df_workings.drop(
        ["Adj Close", "Dividends", "Stock Splits", "Repaired?"], axis=1, errors="ignore"
    )
    df_workings = df_workings.rename(columns={"Volume": "Vol"})
    fna = df_workings["Vol"].isna()
    if fna.any():
        df_workings["VolStr"] = ""
        df_workings.loc[fna, "VolStr"] = "NaN"
        df_workings.loc[~fna, "VolStr"] = (df_workings["Vol"][~fna] / 1e6).astype("int").astype(
            "str"
        ) + "m"
        df_workings["Vol"] = df_workings["VolStr"]
        df_workings.drop("VolStr", axis=1)
    else:
        df_workings["Vol"] = (df_workings["Vol"] / 1e6).astype("int").astype("str") + "m"
    return df_workings.drop([c for c in ohlc if c not in debug_cols], axis=1, errors="ignore")


def _change_matrix_shape(interday, interval, split, n):
    if interday and interval != "1d" and split not in [100.0, 100, 0.001]:
        return np.full((n, 2), 1.0), ["Open", "Close"]
    return np.full((n, 4), 1.0), ["Open", "High", "Low", "Close"]


def _prepare_adjusted_price_data(df2, df_workings, price_data_cols):
    price_data = df2[price_data_cols].to_numpy()
    f_zero = price_data == 0.0
    if not price_data.flags.writeable:
        price_data = price_data.copy()
    if f_zero.any():
        price_data[f_zero] = 1.0

    f_zero_close = df2["Close"] == 0
    if f_zero_close.any():
        adj = np.ones(len(df2))
        adj[~f_zero_close] = (
            df2["Adj Close"].to_numpy()[~f_zero_close]
            / df2["Close"].to_numpy()[~f_zero_close]
        )
    else:
        adj = df2["Adj Close"].to_numpy() / df2["Close"].to_numpy()

    df_dtype = price_data.dtype
    if df_dtype == np.int64:
        price_data = price_data.astype("float")
    for column_index, column in enumerate(price_data_cols):
        price_data[:, column_index] *= adj
        if column in df_workings.columns:
            df_workings[column] *= adj
    if df_dtype == np.int64:
        price_data = price_data.astype("int")
    return price_data, f_zero


def _denoise_price_changes(price_data, f_zero, interday, interval):
    change_matrix = np.full((price_data.shape[0], price_data.shape[1]), 1.0)
    change_matrix[1:] = price_data[1:,] / price_data[:-1,]
    f_zero_num_denom = f_zero | np.roll(f_zero, 1, axis=0)
    if f_zero_num_denom.any():
        change_matrix[f_zero_num_denom] = 1.0
    if interday and interval != "1d":
        denoised = np.average(change_matrix, axis=1)
    else:
        denoised = np.median(change_matrix, axis=1)
    f_na = np.isnan(denoised)
    if f_na.any():
        denoised[f_na] = 1.0
    return change_matrix, denoised


def _price_change_inputs(df2, df_workings, interday, interval, split):
    change_shape, price_data_cols = _change_matrix_shape(interday, interval, split, df2.shape[0])
    price_data, f_zero = _prepare_adjusted_price_data(df2, df_workings, price_data_cols)
    change_matrix, denoised = _denoise_price_changes(price_data, f_zero, interday, interval)
    change_shape[:, :] = change_matrix
    return change_shape, denoised, price_data_cols


def _price_repair_mode(interval):
    return {
        "interval": interval,
        "interday": interval in ["1d", "1wk", "1mo", "3mo"],
        "multiday": interval in ["1wk", "1mo", "3mo"],
        "ohlc": ["Open", "High", "Low", "Close"],
        "debug_cols": ["Close"],
        "correct_columns_individually": bool(interval in ["1wk", "1mo", "3mo"]),
    }


def _prepare_price_repair_frame(df, tz_exchange):
    df2 = df.copy().sort_index(ascending=False)
    if df2.index.tz is None:
        df2.index = df2.index.tz_localize(tz_exchange)
    elif df2.index.tz != tz_exchange:
        df2.index = df2.index.tz_convert(tz_exchange)
    return df2


def _analyse_price_repair_state(df2, mode, split, logger, log_extras):
    appears_suspended, idx_latest_active = _latest_active_index(df2, mode["ohlc"])
    log_msg = f"appears_suspended={appears_suspended}, idx_latest_active={idx_latest_active}"
    if idx_latest_active is not None:
        log_msg += f" ({df2.index[idx_latest_active].date()})"
    logger.debug(log_msg, extra=log_extras)

    df_workings = _build_debug_frame(df2, mode["ohlc"], mode["debug_cols"])
    change_x, change_denoised, price_data_cols = _price_change_inputs(
        df2,
        df_workings,
        mode["interday"],
        mode["interval"],
        split,
    )
    return {
        "appears_suspended": appears_suspended,
        "idx_latest_active": idx_latest_active,
        "df_workings": df_workings,
        "change_x": change_x,
        "change_denoised": change_denoised,
        "price_data_cols": price_data_cols,
    }


def _log_latest_active_index(df, idx_latest_active, logger, log_extras):
    idx_rev_latest_active = None
    if idx_latest_active is not None:
        idx_rev_latest_active = df.shape[0] - 1 - idx_latest_active
        logger.debug(
            f"idx_latest_active={idx_latest_active}, idx_rev_latest_active={idx_rev_latest_active}",
            extra=log_extras,
        )
    return idx_rev_latest_active


def _estimate_price_repair_thresholds(change_denoised, mode, setup):
    split_max = max(setup["split"], setup["split_rcp"])
    min_change = 1.0 / ((split_max - 1) * 0.5 + 1)
    max_change = (split_max - 1) * 0.5 + 1
    if np.max(change_denoised) < max_change and np.min(change_denoised) > min_change:
        return None
    return _estimate_split_repair_threshold(
        change_denoised,
        _SplitThresholdContext(
            split=setup["split"],
            split_rcp=setup["split_rcp"],
            interday=mode["interday"],
            interval=mode["interval"],
            logger=setup["logger"],
            log_extras=setup["log_extras"],
        ),
    )


def _build_price_repair_signals(df2, analysis, mode, setup, threshold):
    change_x, price_data_cols = _prepare_split_change_matrix(
        df2,
        analysis["df_workings"],
        analysis["change_denoised"],
        _SplitChangeMatrixContext(
            ohlc=mode["ohlc"],
            interday=mode["interday"],
            interval=mode["interval"],
            split=setup["split"],
            n=df2.shape[0],
            correct_columns_individually=mode["correct_columns_individually"],
        ),
    )
    r = change_x / setup["split_rcp"]
    f_down = change_x < 1.0 / threshold
    f_up = change_x > threshold
    f_up = _filter_split_false_positives(
        df2,
        f_up,
        f_down,
        mode["multiday"],
        setup["logger"],
    )
    f = f_down | f_up
    return {
        "change_x": change_x,
        "price_data_cols": price_data_cols,
        "r": r,
        "f_down": f_down,
        "f_up": f_up,
        "f": f,
    }


def _record_price_repair_signals(df_workings, signals, mode):
    if not mode["correct_columns_individually"]:
        df_workings["r"] = signals["r"]
        df_workings["down"] = signals["f_down"]
        df_workings["up"] = signals["f_up"]
        df_workings["r"] = df_workings["r"].round(2).astype("str")
        df_workings["f"] = signals["f"]
        return
    for j, column in enumerate(signals["price_data_cols"]):
        df_workings[column + "_r"] = signals["r"][:, j]
        df_workings[column + "_r"] = df_workings[column + "_r"].round(2).astype("str")
        df_workings[column + "_down"] = signals["f_down"][:, j]
        df_workings[column + "_up"] = signals["f_up"][:, j]
        df_workings[column + "_f"] = signals["f"][:, j]


def _filter_price_repair_signals(df2, repair_state, split_max, setup):
    analysis = repair_state["analysis"]
    signals = repair_state["signals"]
    mode = repair_state["mode"]
    f_down, f_up = _filter_local_volatility_false_positives(
        df2,
        analysis["df_workings"],
        signals["f_down"],
        signals["f_up"],
        _LocalVolatilityContext(
            interval=mode["interval"],
            interday=mode["interday"],
            split_max=split_max,
            correct_columns_individually=mode["correct_columns_individually"],
            price_data_cols=signals["price_data_cols"],
            debug_cols=mode["debug_cols"],
            logger=setup["logger"],
        ),
    )
    signals["f_down"] = f_down
    signals["f_up"] = f_up
    signals["f"] = f_down | f_up
    repair_state["signals"] = signals
    return repair_state


def _apply_price_repair(df2, repair_state, setup):
    analysis = repair_state["analysis"]
    signals = repair_state["signals"]
    mode = repair_state["mode"]
    logger = setup["logger"]
    log_extras = setup["log_extras"]
    _log_split_repair_workings(
        analysis["df_workings"],
        mode["correct_columns_individually"],
        mode["debug_cols"],
        logger,
    )
    idx_rev_latest_active = _log_latest_active_index(
        df2,
        analysis["idx_latest_active"],
        logger,
        log_extras,
    )
    split_range_context = _SplitRangeContext(
        split=setup["split"],
        size=df2.shape[0],
        idx_latest_active=analysis["idx_latest_active"],
        idx_rev_latest_active=idx_rev_latest_active,
    )
    repair_context = _SplitRepairContext(
        df_index=df2.index,
        split=setup["split"],
        split_rcp=setup["split_rcp"],
        start_min=setup["start_min"],
        logger=logger,
        log_extras=log_extras,
        interday=mode["interday"],
        fix_type=setup["fix_type"],
        correct_volume=setup["correct_volume"],
        correct_dividend=setup["correct_dividend"],
        appears_suspended=analysis["appears_suspended"],
        idx_latest_active=analysis["idx_latest_active"],
        split_range_context=split_range_context,
    )
    if mode["correct_columns_individually"]:
        _repair_split_ranges_by_column(
            df2=df2,
            signals=_SplitColumnSignals(
                ohlc=mode["ohlc"],
                f=signals["f"],
                f_up=signals["f_up"],
                f_down=signals["f_down"],
            ),
            context=repair_context,
        )
    else:
        _repair_split_ranges(
            df2,
            signals["f"],
            signals["f_up"],
            signals["f_down"],
            repair_context,
        )


def _finalise_repaired_prices(df2, correct_volume):
    if correct_volume:
        f_na = df2["Volume"].isna()
        if f_na.any():
            df2.loc[~f_na, "Volume"] = df2["Volume"][~f_na].round(0).astype("int")
        else:
            df2["Volume"] = df2["Volume"].round(0).astype("int")
    return df2.sort_index()


def fix_prices_sudden_change(price_history, df, settings: _PriceChangeRepairSettings):
    """Repair sudden price changes that match missing split or 100x adjustments."""
    if df.empty:
        return df

    mode = _price_repair_mode(settings.interval)
    setup = _price_repair_setup_context(price_history, df, settings)

    if 0.8 < setup["split"] < 1.25:
        setup["logger"].debug(
            "Split ratio too close to 1. Won't repair",
            extra=setup["log_extras"],
        )
        return df

    df2 = _prepare_price_repair_frame(df, settings.tz_exchange)
    analysis = _analyse_price_repair_state(
        df2,
        mode,
        setup["split"],
        setup["logger"],
        setup["log_extras"],
    )
    threshold_result = _estimate_price_repair_thresholds(
        analysis["change_denoised"],
        mode,
        setup,
    )
    if threshold_result is None:
        setup["logger"].debug(
            f"No {setup['fix_type']}s detected",
            extra=setup["log_extras"],
        )
        return df
    split_max, threshold = threshold_result

    signals = _build_price_repair_signals(df2, analysis, mode, setup, threshold)
    repair_state = {"analysis": analysis, "signals": signals, "mode": mode}

    _record_price_repair_signals(analysis["df_workings"], signals, mode)
    repair_state = _filter_price_repair_signals(df2, repair_state, split_max, setup)
    signals = repair_state["signals"]
    if not signals["f"].any():
        setup["logger"].debug(
            f"No {setup['fix_type']}s detected",
            extra=setup["log_extras"],
        )
        return df

    f_splits = df2["Stock Splits"].to_numpy() != 0.0
    if _should_abort_due_to_nearby_split(
        df2,
        f_splits,
        signals["f"],
        _SplitAbortContext(
            change=setup["change"],
            interval=setup["interval"],
            logger=setup["logger"],
            log_extras=setup["log_extras"],
        ),
    ):
        return df

    _apply_price_repair(df2, repair_state, setup)
    return _finalise_repaired_prices(df2, setup["correct_volume"])
