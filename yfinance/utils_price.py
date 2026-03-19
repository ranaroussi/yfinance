"""Price-repair and metadata normalization helpers for yfinance."""

import datetime as _datetime
from typing import cast

import numpy as _np
import pandas as _pd


def _dts_in_same_interval(dt1, dt2, interval):
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


def fix_yahoo_returning_live_separate_impl(
    quotes,
    interval,
    tz_exchange,
    prepost,
    repair,
    currency,
    price_colnames,
):
    if interval[-1] not in ["m", "h"]:
        prepost = False

    dropped_row = None
    if len(quotes) > 1:
        dt1 = quotes.index[-1]
        dt2 = quotes.index[-2]
        if quotes.index.tz is None:
            dt1 = dt1.tz_localize("UTC")
            dt2 = dt2.tz_localize("UTC")
        dt1 = dt1.tz_convert(tz_exchange)
        dt2 = dt2.tz_convert(tz_exchange)
        if interval == "1d":
            if dt1.date() == dt2.date():
                dropped_row = quotes.iloc[-2]
                quotes = _pd.concat([quotes.iloc[:-2], quotes.iloc[-1:]])
        elif _dts_in_same_interval(dt1=dt2, dt2=dt1, interval=interval):
            idx1 = quotes.index[-1]
            idx2 = quotes.index[-2]
            if idx1 == idx2:
                return quotes, None

            if prepost and dt1.second == 0:
                return quotes, None

            stock_split = quotes["Stock Splits"].iloc[-2:].replace(0, 1).prod()
            if repair:
                currency_divide = 1000 if currency == "KWF" else 100
                if abs(stock_split / currency_divide - 1) > 0.25:
                    ratio = (
                        quotes.loc[idx1, price_colnames]
                        / quotes.loc[idx2, price_colnames]
                    )
                    if ((ratio / currency_divide - 1).abs() < 0.05).all():
                        for col in price_colnames:
                            quotes.loc[idx2, col] *= 100
                    elif ((ratio * currency_divide - 1).abs() < 0.05).all():
                        for col in price_colnames:
                            quotes.loc[idx2, col] *= 0.01

            if _np.isnan(quotes.loc[idx2, "Open"]):
                quotes.loc[idx2, "Open"] = quotes["Open"].iloc[-1]

            if not _np.isnan(quotes["High"].iloc[-1]):
                quotes.loc[idx2, "High"] = _np.nanmax(
                    [quotes["High"].iloc[-1], quotes["High"].iloc[-2]]
                )
                if "Adj High" in quotes.columns:
                    quotes.loc[idx2, "Adj High"] = _np.nanmax(
                        [quotes["Adj High"].iloc[-1], quotes["Adj High"].iloc[-2]]
                    )

            if not _np.isnan(quotes["Low"].iloc[-1]):
                quotes.loc[idx2, "Low"] = _np.nanmin(
                    [quotes["Low"].iloc[-1], quotes["Low"].iloc[-2]]
                )
                if "Adj Low" in quotes.columns:
                    quotes.loc[idx2, "Adj Low"] = _np.nanmin(
                        [quotes["Adj Low"].iloc[-1], quotes["Adj Low"].iloc[-2]]
                    )

            quotes.loc[idx2, "Close"] = quotes["Close"].iloc[-1]
            if "Adj Close" in quotes.columns:
                quotes.loc[idx2, "Adj Close"] = quotes["Adj Close"].iloc[-1]
            quotes.loc[idx2, "Volume"] += quotes["Volume"].iloc[-1]
            quotes.loc[idx2, "Dividends"] += quotes["Dividends"].iloc[-1]
            if stock_split != 1.0:
                quotes.loc[idx2, "Stock Splits"] = stock_split
            dropped_row = quotes.iloc[-1]
            quotes = quotes.drop(quotes.index[-1])

    return quotes, dropped_row


def safe_merge_dfs_impl(
    df_main,
    df_sub,
    interval,
    interval_to_timedelta,
    logger_getter,
    price_colnames,
    exception_cls,
):
    if df_main.empty:
        return df_main

    data_cols = [column for column in df_sub.columns if column not in df_main]
    data_col = data_cols[0]

    df_main = df_main.sort_index()
    intraday = interval.endswith("m") or interval.endswith("s")
    td = interval_to_timedelta(interval)

    if intraday:
        df_main["_date"] = df_main.index.date
        df_sub["_date"] = df_sub.index.date
        indices = _np.searchsorted(
            _np.append(df_main["_date"], [df_main["_date"].iloc[-1] + td]),
            df_sub["_date"],
            side="left",
        )
        df_main = df_main.drop("_date", axis=1)
        df_sub = df_sub.drop("_date", axis=1)
    else:
        indices = _np.searchsorted(
            _np.append(df_main.index, df_main.index[-1] + td),
            df_sub.index,
            side="right",
        )
        indices -= 1

    if intraday:
        for index, sub_index in enumerate(df_sub.index):
            dt = sub_index.date()
            if dt < df_main.index[0].date() or dt >= df_main.index[
                -1
            ].date() + _datetime.timedelta(days=1):
                indices[index] = -1
    else:
        for index, dt in enumerate(df_sub.index):
            if dt < df_main.index[0] or dt >= df_main.index[-1] + td:
                indices[index] = -1

    f_out_of_range = indices == -1
    if f_out_of_range.any():
        if intraday:
            df_sub = df_sub[~f_out_of_range]
            if df_sub.empty:
                df_main["Dividends"] = 0.0
                return df_main

            df_main["_date"] = df_main.index.date
            df_sub["_date"] = df_sub.index.date
            indices = _np.searchsorted(
                _np.append(df_main["_date"], [df_main["_date"].iloc[-1] + td]),
                df_sub["_date"],
                side="left",
            )
            df_main = df_main.drop("_date", axis=1)
            df_sub = df_sub.drop("_date", axis=1)
        else:
            empty_row_data = {
                **{col: [_np.nan] for col in price_colnames},
                "Volume": [0],
            }
            if interval == "1d":
                for index in _np.where(f_out_of_range)[0]:
                    dt = df_sub.index[index]
                    logger_getter().debug(
                        "Adding out-of-range %s @ %s in new prices row of NaNs",
                        data_col,
                        dt.date(),
                    )
                    empty_row = _pd.DataFrame(data=empty_row_data, index=[dt])
                    df_main = _pd.concat([df_main, empty_row], sort=True)
            else:
                last_dt = df_main.index[-1]
                next_interval_start_dt = last_dt + td
                next_interval_end_dt = next_interval_start_dt + td
                for index in _np.where(f_out_of_range)[0]:
                    dt = df_sub.index[index]
                    if next_interval_start_dt <= dt < next_interval_end_dt:
                        logger_getter().debug(
                            "Adding out-of-range %s @ %s in new prices row of NaNs",
                            data_col,
                            dt.date(),
                        )
                        empty_row = _pd.DataFrame(data=empty_row_data, index=[dt])
                        df_main = _pd.concat([df_main, empty_row], sort=True)
            df_main = df_main.sort_index()

            indices = _np.searchsorted(
                _np.append(df_main.index, df_main.index[-1] + td),
                df_sub.index,
                side="right",
            )
            indices -= 1
            for index, dt in enumerate(df_sub.index):
                if dt < df_main.index[0] or dt >= df_main.index[-1] + td:
                    indices[index] = -1

    f_out_of_range = indices == -1
    if f_out_of_range.any():
        if intraday or interval in ["1d", "1wk"]:
            raise exception_cls(
                f"The following '{data_col}' events are out-of-range, did not expect "
                f"with interval {interval}: {df_sub.index[f_out_of_range]}"
            )
        logger_getter().debug(
            "Discarding these %s events:\n%s",
            data_col,
            df_sub[f_out_of_range],
        )
        df_sub = df_sub[~f_out_of_range].copy()
        indices = indices[~f_out_of_range]

    def _reindex_events(df, new_index, data_col_name):
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

    new_index = df_main.index[indices]
    df_sub = _reindex_events(df_sub, new_index, data_col)

    df = df_main.join(df_sub)
    f_na = df[data_col].isna()
    data_lost = sum(~f_na) < df_sub.shape[0]
    if data_lost:
        raise exception_cls("Data was lost in merge, investigate")

    return df


def format_history_metadata_impl(md, trading_periods_only=True):
    if not isinstance(md, dict):
        return md
    if len(md) == 0:
        return md

    tz = md["exchangeTimezoneName"]

    if not trading_periods_only:
        for key in ["firstTradeDate", "regularMarketTime"]:
            if key in md and md[key] is not None and isinstance(md[key], int):
                md[key] = _pd.to_datetime(md[key], unit="s", utc=True).tz_convert(tz)

        if "currentTradingPeriod" in md:
            for period in ["regular", "pre", "post"]:
                if period in md["currentTradingPeriod"] and isinstance(
                    md["currentTradingPeriod"][period]["start"], int
                ):
                    for key in ["start", "end"]:
                        md["currentTradingPeriod"][period][key] = _pd.to_datetime(
                            md["currentTradingPeriod"][period][key], unit="s", utc=True
                        ).tz_convert(tz)
                    del md["currentTradingPeriod"][period]["gmtoffset"]
                    del md["currentTradingPeriod"][period]["timezone"]

    if "tradingPeriods" in md:
        trading_periods = md["tradingPeriods"]
        if trading_periods == {"pre": [], "post": []}:
            return md

        if isinstance(trading_periods, list):
            df = _pd.DataFrame.from_records(_np.hstack(trading_periods))
            df = df.drop(["timezone", "gmtoffset"], axis=1)
            df["start"] = _pd.to_datetime(
                df["start"], unit="s", utc=True
            ).dt.tz_convert(tz)
            df["end"] = _pd.to_datetime(df["end"], unit="s", utc=True).dt.tz_convert(tz)
        elif isinstance(trading_periods, dict):
            pre_df = _pd.DataFrame.from_records(_np.hstack(trading_periods["pre"]))
            post_df = _pd.DataFrame.from_records(_np.hstack(trading_periods["post"]))
            regular_df = _pd.DataFrame.from_records(
                _np.hstack(trading_periods["regular"])
            )

            pre_df = pre_df.rename(
                columns={"start": "pre_start", "end": "pre_end"}
            ).drop(["timezone", "gmtoffset"], axis=1)
            post_df = post_df.rename(
                columns={"start": "post_start", "end": "post_end"}
            ).drop(["timezone", "gmtoffset"], axis=1)
            regular_df = regular_df.drop(["timezone", "gmtoffset"], axis=1)

            cols = ["pre_start", "pre_end", "start", "end", "post_start", "post_end"]
            df = regular_df.join(pre_df).join(post_df)
            for col in cols:
                df[col] = _pd.to_datetime(df[col], unit="s", utc=True).dt.tz_convert(tz)
            df = df[cols]
        else:
            return md

        start_dates = _pd.to_datetime(cast(_pd.Series, df["start"]).dt.date)
        if isinstance(start_dates, _pd.Series):
            df.index = _pd.DatetimeIndex(start_dates)
        else:
            df.index = start_dates
        df.index = df.index.tz_localize(tz)
        df.index.name = "Date"
        md["tradingPeriods"] = df

    return md
