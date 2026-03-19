"""Stock-split repair helpers for the history package."""

import numpy as np
import pandas as pd

from yfinance.scrapers.history.helpers import _PriceChangeRepairSettings, _safe_timestamp
from yfinance.scrapers.history.price_repair import fix_prices_sudden_change
from ... import utils


def _split_scan_cutoff(interval, split_idx, row_count):
    extra_rows = 1 if interval in ["1wk", "1mo", "3mo"] else 5
    return min(row_count, split_idx + extra_rows)


def _log_split_window(logger, df_pre_split, split_details, log_extras):
    first_dt = _safe_timestamp(df_pre_split.index[0]).date()
    last_dt = _safe_timestamp(df_pre_split.index[-1]).date()
    logger.debug(
        "split_idx=%s split_dt=%s split=%.4f",
        split_details["idx"],
        split_details["dt"].date(),
        split_details["ratio"],
        extra=log_extras,
    )
    logger.debug(f"df dt range: {first_dt} -> {last_dt}", extra=log_extras)


def _repair_pre_split_window(price_history, df_pre_split, interval, tz_exchange, split):
    return fix_prices_sudden_change(
        price_history,
        df_pre_split,
        _PriceChangeRepairSettings(
            interval=interval,
            tz_exchange=tz_exchange,
            change=float(split),
            correct_volume=True,
            correct_dividend=True,
        ),
    )


def _merge_repaired_split_window(df, repaired_window, cutoff_idx):
    if cutoff_idx == df.shape[0] - 1:
        return repaired_window

    df_post_cutoff = df.iloc[cutoff_idx + 1 :]
    if df_post_cutoff.empty:
        return repaired_window.sort_index()
    return pd.concat([repaired_window.sort_index(), df_post_cutoff])


def fix_bad_stock_splits(price_history, df, interval, tz_exchange):
    """Repair historical rows that missed a future stock-split adjustment."""
    # Original logic only considered latest split adjustment could be missing, but
    # actually **any** split adjustment can be missing. So check all splits in df.
    #
    # Improved logic looks for BIG daily price changes that closely match the
    # nearest future stock split ratio. This indicates Yahoo failed to apply a new
    # stock split to old price data.

    if df.empty:
        return df

    logger = utils.get_yf_logger()
    log_extras = {
        "yf_cat": "split-repair",
        "yf_interval": interval,
        "yf_symbol": price_history.ticker,
    }

    interday = interval in ["1d", "1wk", "1mo", "3mo"]
    if not interday:
        return df

    df = df.sort_index()  # scan splits oldest -> newest
    split_f = df["Stock Splits"].to_numpy() != 0
    if not split_f.any():
        logger.debug("price-repair-split: No splits in data")
        return df

    logger.debug(f"Splits: {str(df['Stock Splits'][split_f].to_dict())}", extra=log_extras)

    if "Repaired?" not in df.columns:
        df["Repaired?"] = False
    for split_idx in np.where(split_f)[0]:
        split_dt_raw = df.index[split_idx]
        split_dt = _safe_timestamp(split_dt_raw)
        split = df.loc[split_dt_raw, "Stock Splits"]
        if split_dt_raw == df.index[0]:
            continue

        cutoff_idx = _split_scan_cutoff(interval, split_idx, df.shape[0])
        df_pre_split = df.iloc[0 : cutoff_idx + 1]
        _log_split_window(
            logger,
            df_pre_split,
            {"idx": split_idx, "dt": split_dt, "ratio": split},
            log_extras,
        )
        repaired_window = _repair_pre_split_window(
            price_history,
            df_pre_split,
            interval,
            tz_exchange,
            split,
        )
        df = _merge_repaired_split_window(df, repaired_window, cutoff_idx)
    return df
