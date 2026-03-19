"""Capital-gains repair helpers for the history package."""

import datetime as _datetime

import numpy as np

from yfinance import utils


def _capital_gains_debug_frame(df, price_drop_pct_mean):
    df_debug = df.copy()
    df_debug["ScaleFactor"] = np.nan
    df_debug["correction"] = np.nan
    df_debug["AdjYahoo"] = (df_debug["Adj Close"] / df_debug["Close"]).round(4)
    print(f"# price_drop_pct_mean = {price_drop_pct_mean:.4f}")
    return df_debug


def _capital_gains_indices(df, close_values, debug):
    double_counted_by_index = {}
    for dt in df[df["Capital Gains"] > 0].index:
        idx = df.index.get_loc(dt)
        if idx <= 0:
            continue

        dividend = df["Dividends"].iloc[idx]
        capital_gains = df["Capital Gains"].iloc[idx]
        if dividend < capital_gains:
            continue

        div_pct = dividend / close_values[idx - 1]
        cg_pct = capital_gains / close_values[idx - 1]
        price_drop_pct = (close_values[idx - 1] - close_values[idx]) / close_values[idx - 1]
        price_drop_pct_excl_vol = price_drop_pct - df.attrs["price_drop_pct_mean"]
        diff_div = abs(price_drop_pct_excl_vol - div_pct)
        diff_total = abs(price_drop_pct_excl_vol - (div_pct + cg_pct))
        cg_is_double_counted = diff_div < diff_total
        double_counted_by_index[idx] = cg_is_double_counted

        if debug:
            print(f"# {dt.date()}: div = {div_pct * 100:.1f}%, cg = {cg_pct * 100:.1f}%")
            print(f"- price_drop_pct = {price_drop_pct * 100:.1f}%")
            print(f"- price_drop_pct_excl_vol = {price_drop_pct_excl_vol * 100:.1f}%")
            print(f"- diff_div = {diff_div:.4f}")
            print(f"- diff_total = {diff_total:.4f}")
            print(f"- cg_is_double_counted = {cg_is_double_counted}")
    return double_counted_by_index


def _apply_capital_gains_repairs(df, double_counted_by_index, calibration, repair_context):
    for idx in double_counted_by_index:
        dt = df.index[idx]
        dividend = df["Dividends"].iloc[idx]
        capital_gains = df["Capital Gains"].iloc[idx]
        dividend_true = dividend - capital_gains

        df.loc[dt, "Dividends"] = dividend_true

        adj_before = (calibration["adj_close"][idx - 1] / calibration["close"][idx - 1]) / (
            calibration["adj_close"][idx] / calibration["close"][idx]
        )
        adj_correct = 1.0 - (dividend_true + capital_gains) / calibration["close"][idx - 1]
        correction = adj_correct / adj_before
        df.loc[: dt - _datetime.timedelta(1), "Adj"] *= correction
        df.loc[:dt, "Repaired?"] = True
        repair_context["logger"].info(
            (
                f"Repaired capital-gains double-count at {dt.date()}. "
                f"Adj correction = {correction:.4f}"
            ),
            extra=repair_context["log_extras"],
        )

        if repair_context["debug"]:
            df.loc[dt, "correction"] = correction


def repair_capital_gains(price_history, df):
    """Repair capital gains that Yahoo double-counted in dividend adjustments."""
    # Yahoo has started double-counting capital gains in Adj Close,
    # by pre-adding it to dividends column.

    if "Capital Gains" not in df.columns:
        return df
    if (df["Capital Gains"] == 0).all():
        return df

    debug = False
    # debug = True

    logger = utils.get_yf_logger()
    log_extras = {"yf_cat": "repair-capital-gains", "yf_symbol": price_history.ticker}

    df = df.copy()
    df = df.sort_index()

    # Consider price drop to decide if Yahoo double-counted -
    #   drop should = true dividend + capital gains
    # But need to account for normal price volatility:
    df["Price_Change%"] = df["Close"].pct_change(fill_method=None).abs()
    no_distributions = (df["Dividends"] == 0) & (df["Capital Gains"] == 0)
    price_drop_pct_mean = df.loc[no_distributions, "Price_Change%"].mean()
    df = df.drop("Price_Change%", axis=1)

    # Add columns if not present
    if "Repaired?" not in df.columns:
        df["Repaired?"] = False
    df["Adj"] = df["Adj Close"] / df["Close"]

    if debug:
        df = _capital_gains_debug_frame(df, price_drop_pct_mean)

    df.attrs["price_drop_pct_mean"] = price_drop_pct_mean

    close_values = df["Close"].to_numpy()
    adj_close_values = df["Adj Close"].to_numpy()
    double_counted_by_index = _capital_gains_indices(
        df,
        close_values,
        debug,
    )

    pct_double_counted = sum(double_counted_by_index.values()) / len(double_counted_by_index)
    if debug:
        print(f"- pct_double_counted = {pct_double_counted * 100:.1f}%")

    if pct_double_counted >= 0.666:
        _apply_capital_gains_repairs(
            df,
            double_counted_by_index,
            {"close": close_values, "adj_close": adj_close_values},
            {"logger": logger, "log_extras": log_extras, "debug": debug},
        )

    df["Adj Close"] = df["Close"] * df["Adj"]

    if debug:
        df["Adj"] = df["Adj"].round(4)
    else:
        df = df.drop("Adj", axis=1)

    return df
