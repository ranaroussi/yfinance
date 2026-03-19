"""Price-repair and metadata normalization helpers for yfinance."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
import datetime as _datetime
from typing import Any, cast

import numpy as _np
import pandas as _pd


@dataclass(frozen=True)
class _LiveSeparateContext:
    interval: str
    tz_exchange: str
    prepost: bool
    repair: bool
    currency: str | None
    price_colnames: Sequence[str]


@dataclass(frozen=True)
class _MergeContext:
    interval_to_timedelta: Callable[[str], Any]
    logger_getter: Callable[[], Any]
    price_colnames: Sequence[str]
    exception_cls: type[Exception]


@dataclass(frozen=True)
class _OutOfRangeContext:
    interval: str
    intraday: bool
    td: Any
    data_col: str
    merge_context: _MergeContext


def _dts_in_same_interval(dt1, dt2, interval):
    """Return whether two datetimes fall within the same Yahoo interval bucket."""
    if interval == "1d":
        return dt1.date() == dt2.date()
    if interval == "1wk":
        return (dt2 - dt1).days < 7
    if interval == "1mo":
        return dt1.month == dt2.month
    if interval == "3mo":
        shift = (dt1.month % 3) - 1
        q1 = (dt1.month - shift - 1) // 3 + 1
        q2 = (dt2.month - shift - 1) // 3 + 1
        year_diff = dt2.year - dt1.year
        quarter_diff = q2 - q1 + 4 * year_diff
        return quarter_diff == 0

    _interval = interval[:-1] + "D" if interval.endswith("d") else interval
    return (dt2 - dt1) < _pd.Timedelta(_interval)


def _normalize_live_dt_pair(quotes, tz_exchange):
    dt_live = quotes.index[-1]
    dt_previous = quotes.index[-2]
    if quotes.index.tz is None:
        dt_live = dt_live.tz_localize("UTC")
        dt_previous = dt_previous.tz_localize("UTC")
    return dt_live.tz_convert(tz_exchange), dt_previous.tz_convert(tz_exchange)


def _fix_live_separate_currency_mixup(quotes, idx_previous, idx_live, context, stock_split):
    if not context.repair:
        return

    currency_divide = 1000 if context.currency == "KWF" else 100
    if abs(stock_split / currency_divide - 1) <= 0.25:
        return

    ratio = quotes.loc[idx_live, context.price_colnames] / quotes.loc[
        idx_previous, context.price_colnames
    ]
    if ((ratio / currency_divide - 1).abs() < 0.05).all():
        multiplier = 100
    elif ((ratio * currency_divide - 1).abs() < 0.05).all():
        multiplier = 0.01
    else:
        return

    for col in context.price_colnames:
        quotes.loc[idx_previous, col] *= multiplier


def _merge_live_row_into_previous(quotes, idx_previous, idx_live, context):
    stock_split = quotes["Stock Splits"].iloc[-2:].replace(0, 1).prod()
    _fix_live_separate_currency_mixup(quotes, idx_previous, idx_live, context, stock_split)

    if _np.isnan(quotes.loc[idx_previous, "Open"]):
        quotes.loc[idx_previous, "Open"] = quotes["Open"].iloc[-1]

    if not _np.isnan(quotes["High"].iloc[-1]):
        quotes.loc[idx_previous, "High"] = _np.nanmax(
            [quotes["High"].iloc[-1], quotes["High"].iloc[-2]]
        )
        if "Adj High" in quotes.columns:
            quotes.loc[idx_previous, "Adj High"] = _np.nanmax(
                [quotes["Adj High"].iloc[-1], quotes["Adj High"].iloc[-2]]
            )

    if not _np.isnan(quotes["Low"].iloc[-1]):
        quotes.loc[idx_previous, "Low"] = _np.nanmin(
            [quotes["Low"].iloc[-1], quotes["Low"].iloc[-2]]
        )
        if "Adj Low" in quotes.columns:
            quotes.loc[idx_previous, "Adj Low"] = _np.nanmin(
                [quotes["Adj Low"].iloc[-1], quotes["Adj Low"].iloc[-2]]
            )

    quotes.loc[idx_previous, "Close"] = quotes["Close"].iloc[-1]
    if "Adj Close" in quotes.columns:
        quotes.loc[idx_previous, "Adj Close"] = quotes["Adj Close"].iloc[-1]
    quotes.loc[idx_previous, "Volume"] += quotes["Volume"].iloc[-1]
    quotes.loc[idx_previous, "Dividends"] += quotes["Dividends"].iloc[-1]
    if stock_split != 1.0:
        quotes.loc[idx_previous, "Stock Splits"] = stock_split
    dropped_row = quotes.iloc[-1]
    return quotes.drop(idx_live), dropped_row


def _get_live_separate_merge_action(quotes, context):
    if len(quotes) <= 1:
        return None

    dt_live, dt_previous = _normalize_live_dt_pair(quotes, context.tz_exchange)
    if context.interval == "1d":
        return "keep-live-row" if dt_live.date() == dt_previous.date() else None

    idx_live = quotes.index[-1]
    idx_previous = quotes.index[-2]
    should_merge = _dts_in_same_interval(
        dt1=dt_previous,
        dt2=dt_live,
        interval=context.interval,
    )
    should_merge = should_merge and idx_live != idx_previous
    should_merge = should_merge and not (context.prepost and dt_live.second == 0)
    if should_merge:
        return "merge-live-row"
    return None


def fix_yahoo_returning_live_separate_impl(quotes, context):
    """Merge Yahoo's split live row into the preceding candle when appropriate."""
    if context.interval[-1] not in ["m", "h"]:
        context = _LiveSeparateContext(
            interval=context.interval,
            tz_exchange=context.tz_exchange,
            prepost=False,
            repair=context.repair,
            currency=context.currency,
            price_colnames=context.price_colnames,
        )

    dropped_row = None
    merge_action = _get_live_separate_merge_action(quotes, context)
    if merge_action == "keep-live-row":
        dropped_row = quotes.iloc[-2]
        quotes = _pd.concat([quotes.iloc[:-2], quotes.iloc[-1:]])
    elif merge_action == "merge-live-row":
        quotes, dropped_row = _merge_live_row_into_previous(
            quotes,
            quotes.index[-2],
            quotes.index[-1],
            context,
        )

    return quotes, dropped_row


def _calculate_event_indices(df_main, df_sub, td, intraday):
    if intraday:
        df_main = df_main.copy()
        df_sub = df_sub.copy()
        df_main["_date"] = df_main.index.date
        df_sub["_date"] = df_sub.index.date
        indices = _np.searchsorted(
            _np.append(df_main["_date"], [df_main["_date"].iloc[-1] + td]),
            df_sub["_date"],
            side="left",
        )
        return df_main.drop("_date", axis=1), df_sub.drop("_date", axis=1), indices

    indices = _np.searchsorted(
        _np.append(df_main.index, df_main.index[-1] + td),
        df_sub.index,
        side="right",
    )
    return df_main, df_sub, indices - 1


def _mark_out_of_range_indices(df_main, df_sub, indices, intraday, td):
    if intraday:
        max_dt = df_main.index[-1].date() + _datetime.timedelta(days=1)
        for index, sub_index in enumerate(df_sub.index):
            dt = sub_index.date()
            if dt < df_main.index[0].date() or dt >= max_dt:
                indices[index] = -1
        return

    for index, dt in enumerate(df_sub.index):
        if dt < df_main.index[0] or dt >= df_main.index[-1] + td:
            indices[index] = -1


def _get_empty_row_data(price_colnames):
    return {**{col: [_np.nan] for col in price_colnames}, "Volume": [0]}


def _append_out_of_range_rows(df_main, df_sub, f_out_of_range, range_context):
    empty_row_data = _get_empty_row_data(range_context.merge_context.price_colnames)
    if range_context.interval == "1d":
        candidate_indexes = _np.where(f_out_of_range)[0]
    else:
        last_dt = df_main.index[-1]
        next_interval_start_dt = last_dt + range_context.td
        next_interval_end_dt = next_interval_start_dt + range_context.td
        candidate_indexes = [
            index
            for index in _np.where(f_out_of_range)[0]
            if next_interval_start_dt <= df_sub.index[index] < next_interval_end_dt
        ]

    for index in candidate_indexes:
        dt = df_sub.index[index]
        range_context.merge_context.logger_getter().debug(
            "Adding out-of-range %s @ %s in new prices row of NaNs",
            range_context.data_col,
            dt.date(),
        )
        empty_row = _pd.DataFrame(data=empty_row_data, index=[dt])
        df_main = _pd.concat([df_main, empty_row], sort=True)
    return df_main.sort_index()


def _handle_out_of_range_events(df_main, df_sub, indices, range_context):
    f_out_of_range = indices == -1
    if not f_out_of_range.any():
        return df_main, df_sub, indices

    if range_context.intraday:
        df_sub = df_sub[~f_out_of_range]
        if df_sub.empty:
            df_main["Dividends"] = 0.0
            return df_main, df_sub, indices[~f_out_of_range]
        df_main, df_sub, indices = _calculate_event_indices(
            df_main,
            df_sub,
            range_context.td,
            intraday=True,
        )
        _mark_out_of_range_indices(df_main, df_sub, indices, intraday=True, td=range_context.td)
        return df_main, df_sub, indices

    df_main = _append_out_of_range_rows(df_main, df_sub, f_out_of_range, range_context)
    df_main, df_sub, indices = _calculate_event_indices(
        df_main,
        df_sub,
        range_context.td,
        intraday=False,
    )
    _mark_out_of_range_indices(
        df_main,
        df_sub,
        indices,
        intraday=False,
        td=range_context.td,
    )
    return df_main, df_sub, indices


def _reindex_events(df, new_index, data_col_name, exception_cls):
    if len(new_index) == len(set(new_index)):
        df.index = new_index
        return df

    df["_NewIndex"] = new_index
    if data_col_name in ["Dividends", "Capital Gains"]:
        df = df.groupby("_NewIndex").sum()
        df.index.name = None
    elif data_col_name == "Stock Splits":
        df = df.groupby("_NewIndex").prod()
        df.index.name = None
    else:
        raise exception_cls(
            "New index contains duplicates but unsure how to aggregate for "
            f"'{data_col_name}'"
        )
    if "_NewIndex" in df.columns:
        df = df.drop("_NewIndex", axis=1)
    return df


def _discard_out_of_range_events(df_sub, indices, range_context):
    f_out_of_range = indices == -1
    if not f_out_of_range.any():
        return df_sub, indices

    if range_context.intraday or range_context.interval in ["1d", "1wk"]:
        raise range_context.merge_context.exception_cls(
            f"The following '{range_context.data_col}' events are out-of-range, "
            f"did not expect with interval {range_context.interval}: {df_sub.index[f_out_of_range]}"
        )

    range_context.merge_context.logger_getter().debug(
        "Discarding these %s events:\n%s",
        range_context.data_col,
        df_sub[f_out_of_range],
    )
    return df_sub[~f_out_of_range].copy(), indices[~f_out_of_range]


def safe_merge_dfs_impl(df_main, df_sub, interval, context):
    """Merge event data into a price dataframe while respecting interval boundaries."""
    if df_main.empty:
        return df_main

    data_cols = [column for column in df_sub.columns if column not in df_main]
    data_col = data_cols[0]

    df_main = df_main.sort_index()
    intraday = interval.endswith("m") or interval.endswith("s")
    td = context.interval_to_timedelta(interval)
    range_context = _OutOfRangeContext(
        interval=interval,
        intraday=intraday,
        td=td,
        data_col=data_col,
        merge_context=context,
    )
    df_main, df_sub, indices = _calculate_event_indices(df_main, df_sub, td, intraday)
    _mark_out_of_range_indices(df_main, df_sub, indices, intraday, td)
    df_main, df_sub, indices = _handle_out_of_range_events(df_main, df_sub, indices, range_context)
    df_sub, indices = _discard_out_of_range_events(df_sub, indices, range_context)

    new_index = df_main.index[indices]
    df_sub = _reindex_events(df_sub, new_index, data_col, context.exception_cls)

    if df_sub.empty:
        if data_col not in df_main.columns:
            df_main[data_col] = 0.0
        return df_main

    df = df_main.join(df_sub)
    f_na = df[data_col].isna()
    data_lost = sum(~f_na) < df_sub.shape[0]
    if data_lost:
        raise context.exception_cls("Data was lost in merge, investigate")

    return df


def _format_metadata_regular_times(md, tz):
    for key in ["firstTradeDate", "regularMarketTime"]:
        if key in md and md[key] is not None and isinstance(md[key], int):
            md[key] = _pd.to_datetime(md[key], unit="s", utc=True).tz_convert(tz)


def _format_metadata_current_trading_period(md, tz):
    if "currentTradingPeriod" not in md:
        return

    for period in ["regular", "pre", "post"]:
        period_data = md["currentTradingPeriod"].get(period)
        if not period_data or not isinstance(period_data.get("start"), int):
            continue
        for key in ["start", "end"]:
            period_data[key] = _pd.to_datetime(period_data[key], unit="s", utc=True).tz_convert(tz)
        del period_data["gmtoffset"]
        del period_data["timezone"]


def _format_trading_periods_from_list(trading_periods, tz):
    df = _pd.DataFrame.from_records(_np.hstack(trading_periods))
    df = df.drop(["timezone", "gmtoffset"], axis=1)
    df["start"] = _pd.to_datetime(df["start"], unit="s", utc=True).dt.tz_convert(tz)
    df["end"] = _pd.to_datetime(df["end"], unit="s", utc=True).dt.tz_convert(tz)
    return df


def _prepare_period_frame(period_records, rename_map=None):
    df = _pd.DataFrame.from_records(_np.hstack(period_records))
    if rename_map is not None:
        df = df.rename(columns=rename_map)
    return df.drop(["timezone", "gmtoffset"], axis=1)


def _format_trading_periods_from_dict(trading_periods, tz):
    pre_df = _prepare_period_frame(
        trading_periods["pre"], {"start": "pre_start", "end": "pre_end"}
    )
    post_df = _prepare_period_frame(
        trading_periods["post"], {"start": "post_start", "end": "post_end"}
    )
    regular_df = _prepare_period_frame(trading_periods["regular"])

    cols = ["pre_start", "pre_end", "start", "end", "post_start", "post_end"]
    df = regular_df.join(pre_df).join(post_df)
    for col in cols:
        df[col] = _pd.to_datetime(df[col], unit="s", utc=True).dt.tz_convert(tz)
    return df[cols]


def _format_trading_periods(trading_periods, tz):
    if isinstance(trading_periods, list):
        df = _format_trading_periods_from_list(trading_periods, tz)
    elif isinstance(trading_periods, dict):
        df = _format_trading_periods_from_dict(trading_periods, tz)
    else:
        return None

    start_dates = _pd.to_datetime(cast(_pd.Series, df["start"]).dt.date)
    df.index = _pd.DatetimeIndex(start_dates).tz_localize(tz)
    df.index.name = "Date"
    return df


def format_history_metadata_impl(md, trading_periods_only=True):
    """Normalize raw Yahoo history metadata into consistently typed structures."""
    if not isinstance(md, dict):
        return md
    if len(md) == 0:
        return md

    tz = md["exchangeTimezoneName"]

    if not trading_periods_only:
        _format_metadata_regular_times(md, tz)
        _format_metadata_current_trading_period(md, tz)

    if "tradingPeriods" in md:
        trading_periods = md["tradingPeriods"]
        if trading_periods == {"pre": [], "post": []}:
            return md

        df = _format_trading_periods(trading_periods, tz)
        if df is None:
            return md
        md["tradingPeriods"] = df

    return md
