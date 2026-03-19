"""Chart fetch and normalization workflow extracted from the history package."""

from __future__ import annotations

import datetime as _datetime
import time as _time
from types import SimpleNamespace
import warnings
from typing import Any, Optional, cast

from curl_cffi import requests
import numpy as np
import pandas as pd

from yfinance.config import YF_CONFIG as YfConfig
from yfinance.const import _BASE_URL_, _PRICE_COLNAMES_
from yfinance.exceptions import (
    YFDataException,
    YFInvalidPeriodError,
    YFPricesMissingError,
    YFTzMissingError,
)

from yfinance.scrapers.history.helpers import (
    _HistoryRequest,
    _interval_to_supported_delta,
    _safe_timestamp,
)
from ... import shared, utils

_FetchState = SimpleNamespace


def _build_fetch_state(price_history, request: _HistoryRequest) -> _FetchState:
    logger = utils.get_yf_logger()
    return _FetchState(
        price_history=price_history,
        request=request,
        logger=logger,
        history_obj=cast(Any, price_history),
        period=request.period,
        interval=request.interval,
        start=request.start,
        end=request.end,
        prepost=request.prepost,
        actions=request.actions,
        auto_adjust=request.auto_adjust,
        back_adjust=request.back_adjust,
        repair=request.repair,
        keepna=request.keepna,
        rounding=request.rounding,
        timeout=request.timeout,
        raise_errors=request.raise_errors,
        interval_user=request.interval,
        period_user=request.period,
        start_user=request.start,
        end_user=request.end,
        start_dt=None,
        end_dt=None,
        tz=None,
        params={},
        intraday=False,
        price_data_debug="",
        valid_ranges=[],
        expect_capital_gains=False,
        tz_exchange="UTC",
        currency="",
        chart_result=None,
        chart_result0=None,
    )


def _warn_raise_errors(state: _FetchState) -> None:
    if state.raise_errors:
        warnings.warn(
            "'raise_errors' deprecated, do: yf.config.debug.hide_exceptions = False",
            DeprecationWarning,
            stacklevel=5,
        )


def _return_error_df(
    state: _FetchState,
    exception: Exception,
    clear_reconstruct: bool = False,
) -> pd.DataFrame:
    err_msg = str(exception)
    shared.set_df(state.price_history.ticker, utils.empty_df())
    shared.set_error(state.price_history.ticker, err_msg.split(": ", 1)[1])
    if state.raise_errors or (not YfConfig.debug.hide_exceptions):
        raise exception
    state.logger.error(err_msg)
    if clear_reconstruct:
        state.history_obj.clear_reconstruct_start_interval(state.interval)
    return utils.empty_df()


def _ensure_timezone(state: _FetchState) -> pd.DataFrame | None:
    if not (state.start or state.end or (state.period and state.period.lower() == "max")):
        return None
    state.tz = state.price_history.tz
    if state.tz is None:
        return _return_error_df(state, YFTzMissingError(state.price_history.ticker))
    return None


def _normalize_repair_request(state: _FetchState) -> pd.DataFrame | None:
    if not state.repair or state.interval not in ["5d", "1wk", "1mo", "3mo"]:
        return None
    if state.interval == "5d":
        raise ValueError("Yahoo's interval '5d' is nonsense, not supported with repair")
    if state.start is None and state.end is None and state.period is not None:
        state.tz = state.price_history.tz
        if state.tz is None:
            return _return_error_df(state, YFTzMissingError(state.price_history.ticker))
        if state.period == "ytd":
            state.start = _datetime.date(
                pd.Timestamp.now("UTC").tz_convert(state.tz).year,
                1,
                1,
            )
        else:
            state.start = pd.Timestamp.now("UTC").tz_convert(state.tz).date()
            state.start -= _interval_to_supported_delta(state.period)
            state.start -= _datetime.timedelta(days=4)
        state.period = None
    state.interval = "1d"
    return None


def _resolve_missing_period_bounds(state: _FetchState) -> None:
    if not (state.start or state.end):
        state.period = "1mo"
        return
    if not state.start:
        if state.end_dt is None:
            raise ValueError("Invalid end value for period calculation")
        state.start_dt = state.end_dt - _interval_to_supported_delta("1mo")
        state.start = int(state.start_dt.timestamp())
        return
    if state.end:
        return
    state.end_dt = (
        pd.Timestamp.now("UTC")
        if state.tz is None
        else pd.Timestamp.now("UTC").tz_convert(state.tz)
    )
    state.end = int(state.end_dt.timestamp())


def _resolve_max_period_request(state: _FetchState) -> None:
    if state.end is None:
        state.end = int(_time.time())
    if state.start is not None:
        return
    if state.interval == "1m":
        lookback = 691200
    elif state.interval in ("2m", "5m", "15m", "30m", "90m"):
        lookback = 5184000
    elif state.interval in ("1h", "60m"):
        lookback = 63072000
    else:
        lookback = 3122064000
    state.start = state.end - lookback + 5


def _resolve_period_request(state: _FetchState) -> None:
    period = cast(str, state.period)
    if period.lower() == "max":
        _resolve_max_period_request(state)
        return
    if state.start and state.end:
        raise ValueError(
            "Setting period, start and end is nonsense. Set maximum 2 of them."
        )
    if not (state.start or state.end):
        return
    period_td = _interval_to_supported_delta(period)
    if state.end is None:
        if state.start_dt is None:
            state.start_dt = _safe_timestamp(state.start)
        state.end_dt = state.start_dt + period_td
        state.end = int(state.end_dt.timestamp())
    if state.start is None:
        if state.end_dt is None:
            state.end_dt = _safe_timestamp(state.end)
        state.start_dt = state.end_dt - period_td
        state.start = int(state.start_dt.timestamp())
    state.period = None


def _resolve_period_and_dates(state: _FetchState) -> None:
    if state.start:
        state.start_dt = _safe_timestamp(utils.parse_user_dt(state.start, state.tz))
        state.start = int(state.start_dt.timestamp())
    if state.end:
        state.end_dt = _safe_timestamp(utils.parse_user_dt(state.end, state.tz))
        state.end = int(state.end_dt.timestamp())
    if state.period is None:
        _resolve_missing_period_bounds(state)
        return
    _resolve_period_request(state)


def _build_request_params(state: _FetchState) -> None:
    if state.start or state.end:
        state.params = {"period1": state.start, "period2": state.end}
    else:
        if state.period is None:
            raise ValueError("Period cannot be None when start/end are not set")
        state.period = state.period.lower()
        state.params = {"range": state.period}
    state.params["interval"] = state.interval.lower()
    state.params["includePrePost"] = state.prepost
    if state.params["interval"] == "30m":
        state.params["interval"] = "15m"
    state.params["events"] = "div,splits,capitalGains"


def _log_request_params(state: _FetchState) -> None:
    params_pretty = dict(state.params)
    tz_for_params = state.tz if state.tz is not None else "UTC"
    for key in ["period1", "period2"]:
        if key in params_pretty and params_pretty[key] is not None:
            params_pretty[key] = str(
                pd.Timestamp(cast(int, params_pretty[key]), unit="s")
                .tz_localize("UTC")
                .tz_convert(tz_for_params)
            )
    state.logger.debug(
        f"{state.price_history.ticker}: Yahoo GET parameters: {str(params_pretty)}"
    )


def _fetch_chart_data(state: _FetchState) -> Optional[dict[str, Any]]:
    url = f"{_BASE_URL_}/v8/finance/chart/{state.price_history.ticker}"
    data_client = state.history_obj.get_data_client()
    get_fn = data_client.get
    if state.end is not None:
        state.end_dt = _safe_timestamp(pd.Timestamp(state.end, unit="s").tz_localize("UTC"))
        if state.end_dt + _datetime.timedelta(minutes=30) <= pd.Timestamp.now("UTC"):
            get_fn = data_client.cache_get
    try:
        response = get_fn(url=url, params=state.params, timeout=state.timeout)
        if response is None or "Will be right back" in response.text:
            raise YFDataException("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***")
        data_json = response.json()
        if isinstance(data_json, dict):
            return cast(dict[str, Any], data_json)
    except (AttributeError, TypeError, ValueError, requests.exceptions.RequestException):
        if state.raise_errors or (not YfConfig.debug.hide_exceptions):
            raise
    return None


def _update_history_metadata(
    state: _FetchState,
    data: Optional[dict[str, Any]],
) -> None:
    chart = data.get("chart", {}) if isinstance(data, dict) else {}
    chart_result = chart.get("result") if isinstance(chart, dict) else None
    if not isinstance(chart_result, list) or len(chart_result) == 0 or chart_result[0] is None:
        state.history_obj.clear_history_metadata()
        state.chart_result = None
        return
    state.chart_result = chart_result
    chart_result0 = chart_result[0]
    metadata = chart_result0.get("meta", {}) if isinstance(chart_result0, dict) else {}
    state.history_obj.set_history_metadata(metadata)


def _format_debug_boundary(
    user_value,
    timestamp,
    intraday: bool,
    timezone,
    closing: bool = False,
) -> str:
    if user_value is not None:
        return f"{user_value}{')' if closing else ''}"
    if timestamp is None:
        return "?)" if closing else "?"
    dt = pd.Timestamp(timestamp, unit="s").tz_localize("UTC").tz_convert(timezone)
    rendered = str(dt if intraday else dt.date())
    return rendered + (")" if closing else "")


def _build_price_debug(state: _FetchState) -> None:
    state.intraday = state.params["interval"][-1] in ("m", "h")
    tz_for_debug = state.tz if state.tz is not None else "UTC"
    if state.start or state.period is None or cast(str, state.period).lower() == "max":
        state.price_data_debug = f" ({state.params['interval']} "
        state.price_data_debug += _format_debug_boundary(
            state.start_user,
            state.start,
            state.intraday,
            tz_for_debug,
        )
        state.price_data_debug += " -> "
        state.price_data_debug += _format_debug_boundary(
            state.end_user,
            state.end,
            state.intraday,
            tz_for_debug,
            closing=True,
        )
        return
    state.price_data_debug = f" (period={state.period})"


def _missing_quote_payload(data: dict[str, Any]) -> bool:
    return (
        "chart" not in data
        or data["chart"]["result"] is None
        or not data["chart"]["result"]
        or not data["chart"]["result"][0]["indicators"]["quote"][0]
    )


def _validate_chart_data(
    state: _FetchState,
    data: Optional[dict[str, Any]],
) -> pd.DataFrame | None:
    state.valid_ranges = cast(
        list[str],
        state.history_obj.get_history_metadata_value("validRanges", []),
    )
    base_exception = YFPricesMissingError(
        state.price_history.ticker,
        state.price_data_debug,
    )
    if data is None or not isinstance(data, dict):
        return _return_error_df(state, base_exception, clear_reconstruct=True)
    if "status_code" in data:
        state.price_data_debug += f"(Yahoo status_code = {data['status_code']})"
        exception = YFPricesMissingError(state.price_history.ticker, state.price_data_debug)
        return _return_error_df(state, exception, clear_reconstruct=True)
    if "chart" in data and data["chart"]["error"]:
        state.price_data_debug += (
            ' (Yahoo error = "' + data["chart"]["error"]["description"] + '")'
        )
        exception = YFPricesMissingError(state.price_history.ticker, state.price_data_debug)
        return _return_error_df(state, exception, clear_reconstruct=True)
    if _missing_quote_payload(data):
        return _return_error_df(state, base_exception, clear_reconstruct=True)
    if (
        state.period
        and state.period not in state.valid_ranges
        and not utils.is_valid_period_format(state.period)
    ):
        invalid = YFInvalidPeriodError(
            state.price_history.ticker,
            state.period,
            ", ".join(state.valid_ranges),
        )
        return _return_error_df(state, invalid, clear_reconstruct=True)
    if not state.chart_result:
        raise YFPricesMissingError(state.price_history.ticker, state.price_data_debug)
    state.chart_result0 = cast(dict[str, Any], state.chart_result[0])
    return None


def _load_metadata_values(state: _FetchState) -> None:
    quote_type = cast(
        str,
        state.history_obj.get_history_metadata_value("instrumentType", ""),
    )
    state.expect_capital_gains = quote_type in ("MUTUALFUND", "ETF")
    tz_for_debug = state.tz if state.tz is not None else "UTC"
    state.tz_exchange = cast(
        str,
        state.history_obj.get_history_metadata_value(
            "exchangeTimezoneName",
            tz_for_debug,
        ),
    )
    state.currency = cast(
        str,
        state.history_obj.get_history_metadata_value("currency", ""),
    )
    if state.period and state.period not in state.valid_ranges:
        state.end = int(_time.time())
        state.end_dt = _safe_timestamp(pd.Timestamp(state.end, unit="s").tz_localize("UTC"))
        state.start = _datetime.date.fromtimestamp(state.end)
        state.start -= _interval_to_supported_delta(state.period)
        state.start -= _datetime.timedelta(days=4)


def _log_ohlc_range(state: _FetchState, stage: str, df: pd.DataFrame) -> None:
    if df.empty:
        msg = f"{state.price_history.ticker}: {stage}: EMPTY"
    elif len(df) == 1:
        msg = f"{state.price_history.ticker}: {stage}: {df.index[0]} only"
    else:
        msg = f"{state.price_history.ticker}: {stage}: {df.index[0]} -> {df.index[-1]}"
    state.logger.debug(msg)


def _resample_30m_quotes(quotes: pd.DataFrame) -> pd.DataFrame:
    quotes2 = quotes.resample("30min")
    return pd.DataFrame(
        index=quotes2.last().index,
        data={
            "Open": quotes2["Open"].first(),
            "High": quotes2["High"].max(),
            "Low": quotes2["Low"].min(),
            "Close": quotes2["Close"].last(),
            "Adj Close": quotes2["Adj Close"].last(),
            "Volume": quotes2["Volume"].sum(),
        },
    )


def _drop_unrequested_prepost_rows(
    state: _FetchState,
    quotes: pd.DataFrame,
) -> pd.DataFrame:
    if "tradingPeriods" not in state.history_obj.get_history_metadata():
        return quotes
    trading_periods = state.history_obj.get_history_metadata_value("tradingPeriods")
    if not isinstance(trading_periods, pd.DataFrame):
        formatted_md = utils.format_history_metadata(
            state.history_obj.get_history_metadata(),
            tradingPeriodsOnly=True,
        )
        if isinstance(formatted_md, dict):
            state.history_obj.set_history_metadata(
                cast(dict[str, Any], formatted_md),
                formatted=True,
            )
            trading_periods = state.history_obj.get_history_metadata_value("tradingPeriods")
    if isinstance(trading_periods, pd.DataFrame):
        return cast(
            pd.DataFrame,
            utils.fix_yahoo_returning_prepost_unrequested(
                quotes,
                state.interval,
                trading_periods,
            ),
        )
    return quotes


def _extract_quotes(state: _FetchState) -> pd.DataFrame:
    quotes = cast(pd.DataFrame, utils.parse_quotes(cast(dict[str, Any], state.chart_result0)))
    if state.end and not quotes.empty:
        end_dt_cmp = (
            pd.Timestamp(state.end, unit="s")
            .tz_localize("UTC")
            .tz_convert("UTC")
            .tz_localize(None)
        )
        quotes_index = pd.DatetimeIndex(quotes.index)
        if cast(pd.Timestamp, quotes_index[-1]) >= end_dt_cmp:
            quotes = quotes.drop(quotes_index[-1])
    _log_ohlc_range(state, "yfinance received OHLC data", quotes)
    if state.interval.lower() == "30m":
        state.logger.debug(
            f"{state.price_history.ticker}: resampling 30m OHLC from 15m"
        )
        quotes = _resample_30m_quotes(quotes)
    quotes = cast(pd.DataFrame, utils.set_df_tz(quotes, state.interval, state.tz_exchange))
    quotes = cast(pd.DataFrame, utils.fix_yahoo_dst_issue(quotes, state.interval))
    if not state.prepost and state.intraday:
        quotes = _drop_unrequested_prepost_rows(state, quotes)
    _log_ohlc_range(state, "OHLC after cleaning", quotes)
    return quotes


def _normalize_dividend_currency(
    state: _FetchState,
    dividends: pd.DataFrame,
) -> pd.DataFrame:
    if "currency" not in dividends.columns:
        return dividends
    price_currency = cast(
        str,
        state.history_obj.get_history_metadata_value("currency", ""),
    )
    mismatch = cast(pd.Series, dividends["currency"] != price_currency)
    if mismatch.any():
        if not state.repair or price_currency == "":
            dividends["Dividends"] = (
                dividends["Dividends"].astype(str) + " " + dividends["currency"]
            )
        else:
            dividends = cast(
                pd.DataFrame,
                state.history_obj.convert_dividends_currency(
                    dividends,
                    price_currency,
                    state.repair,
                ),
            )
            if cast(pd.Series, dividends["currency"] != price_currency).any():
                dividends["Dividends"] = (
                    dividends["Dividends"].astype(str) + " " + dividends["currency"]
                )
    return dividends.drop("currency", axis=1)


def _extract_actions(
    state: _FetchState,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dividends_raw, splits_raw, capital_gains_raw = utils.parse_actions(
        cast(dict[str, Any], state.chart_result0)
    )
    dividends = cast(
        pd.DataFrame,
        utils.set_df_tz(cast(pd.DataFrame, dividends_raw), state.interval, state.tz_exchange),
    )
    splits = cast(
        pd.DataFrame,
        utils.set_df_tz(cast(pd.DataFrame, splits_raw), state.interval, state.tz_exchange),
    )
    capital_gains = cast(
        pd.DataFrame,
        utils.set_df_tz(
            cast(pd.DataFrame, capital_gains_raw),
            state.interval,
            state.tz_exchange,
        ),
    )
    if not state.expect_capital_gains:
        capital_gains = pd.DataFrame(
            columns=["Capital Gains"],
            index=pd.DatetimeIndex([]),
        )
    dividends = _normalize_dividend_currency(state, dividends)
    return dividends, splits, capital_gains


def _floor_quote_start(quotes: pd.DataFrame, tz_exchange: str) -> pd.Timestamp:
    first_quote_dt = cast(pd.Timestamp, pd.DatetimeIndex(quotes.index)[0])
    try:
        floored = first_quote_dt.floor("D")
        if pd.isna(floored):
            raise ValueError("Quote index floor returned NaT")
        return cast(pd.Timestamp, floored)
    except ValueError as exc:
        quotes_tz = pd.DatetimeIndex(quotes.index).tz
        if quotes_tz is None:
            quotes_tz = tz_exchange
        localized = pd.Timestamp(first_quote_dt.date()).tz_localize(
            quotes_tz,
            ambiguous=True,
            nonexistent="shift_forward",
        )
        if pd.isna(localized):
            raise ValueError("Quote index localization returned NaT") from exc
        return cast(pd.Timestamp, localized)


def _slice_actions_to_window(
    state: _FetchState,
    quotes: pd.DataFrame,
    dividends: pd.DataFrame,
    splits: pd.DataFrame,
    capital_gains: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if state.start is not None and not quotes.empty:
        start_d = _floor_quote_start(quotes, state.tz_exchange)
        dividends = cast(pd.DataFrame, dividends.loc[start_d:])
        capital_gains = cast(pd.DataFrame, capital_gains.loc[start_d:])
        splits = cast(pd.DataFrame, splits.loc[start_d:])
    if state.end is not None and state.end_dt is not None:
        end_dt_sub1 = state.end_dt - pd.Timedelta(1)
        dividends = cast(pd.DataFrame, dividends[:end_dt_sub1])
        capital_gains = cast(pd.DataFrame, capital_gains[:end_dt_sub1])
        splits = cast(pd.DataFrame, splits[:end_dt_sub1])
    return dividends, splits, capital_gains


def _localize_dates_to_exchange(df: pd.DataFrame, tz_exchange: str) -> pd.DataFrame:
    dates = [cast(pd.Timestamp, dt).date() for dt in pd.DatetimeIndex(df.index)]
    localized = df.copy()
    localized.index = pd.to_datetime(dates).tz_localize(
        tz_exchange,
        ambiguous=True,
        nonexistent="shift_forward",
    )
    return localized


def _localize_interday_actions(
    state: _FetchState,
    quotes: pd.DataFrame,
    dividends: pd.DataFrame,
    splits: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if state.intraday:
        return quotes, dividends, splits
    quotes = _localize_dates_to_exchange(quotes, state.tz_exchange)
    if not dividends.empty:
        dividends = _localize_dates_to_exchange(dividends, state.tz_exchange)
    if not splits.empty:
        splits = _localize_dates_to_exchange(splits, state.tz_exchange)
    return quotes, dividends, splits


def _merge_one_action(
    state: _FetchState,
    df: pd.DataFrame,
    action_df: pd.DataFrame,
    column: str,
) -> pd.DataFrame:
    if not action_df.empty:
        df = cast(pd.DataFrame, utils.safe_merge_dfs(df, action_df, state.interval))
    if column in df.columns:
        df.loc[df[column].isna(), column] = 0
    else:
        df[column] = 0.0
    return df


def _merge_action_frames(
    state: _FetchState,
    quotes: pd.DataFrame,
    dividends: pd.DataFrame,
    splits: pd.DataFrame,
    capital_gains: pd.DataFrame,
) -> pd.DataFrame:
    df = cast(pd.DataFrame, quotes.sort_index())
    df = _merge_one_action(state, df, dividends, "Dividends")
    df = _merge_one_action(state, df, splits, "Stock Splits")
    if state.expect_capital_gains:
        df = _merge_one_action(state, df, capital_gains, "Capital Gains")
    _log_ohlc_range(state, "OHLC after combining events", df)
    return df


def _repair_last_price_row(state: _FetchState, df: pd.DataFrame) -> pd.DataFrame:
    df_last = state.history_obj.repair_zero_price_rows(
        df.iloc[-1:],
        state.interval,
        state.tz_exchange,
        state.prepost,
    )
    if "Repaired?" not in df.columns:
        df["Repaired?"] = False
    return pd.concat([df.drop(df.index[-1]), df_last])


def _repair_prices_if_needed(state: _FetchState, df: pd.DataFrame) -> pd.DataFrame:
    if not state.repair:
        return df
    state.logger.debug(f"{state.price_history.ticker}: checking OHLC for repairs ...")
    df = df.sort_index()
    df, state.currency = state.history_obj.standardise_currency(df, state.currency)
    state.history_obj.set_history_metadata_value("currency", state.currency)
    df = state.history_obj.repair_bad_div_adjust(df, state.interval, state.currency)
    if not df.empty:
        df = _repair_last_price_row(state, df)
    df = state.history_obj.repair_unit_mixups(
        df,
        state.interval,
        state.tz_exchange,
        state.prepost,
    )
    df = state.history_obj.repair_bad_stock_splits(
        df,
        state.interval,
        state.tz_exchange,
    )
    df = state.history_obj.repair_zero_price_rows(
        df,
        state.interval,
        state.tz_exchange,
        state.prepost,
    )
    df = state.history_obj.repair_capital_gains(df)
    return df.sort_index()


def _apply_price_adjustment(state: _FetchState, df: pd.DataFrame) -> pd.DataFrame:
    try:
        if state.auto_adjust:
            return cast(pd.DataFrame, utils.auto_adjust(df))
        if state.back_adjust:
            return cast(pd.DataFrame, utils.back_adjust(df))
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        if state.raise_errors or (not YfConfig.debug.hide_exceptions):
            raise
        err_msg = (
            f"auto_adjust failed with {error}"
            if state.auto_adjust
            else f"back_adjust failed with {error}"
        )
        shared.set_df(state.price_history.ticker, utils.empty_df())
        shared.set_error(state.price_history.ticker, err_msg)
        state.logger.error("%s: %s", state.price_history.ticker, err_msg)
    return df


def _finalize_history_df(state: _FetchState, df: pd.DataFrame) -> pd.DataFrame:
    if state.rounding:
        price_hint = state.history_obj.get_history_metadata_value("priceHint")
        if isinstance(price_hint, int):
            df = df.round(price_hint)
    df = df.copy()
    df["Volume"] = cast(pd.Series, df["Volume"]).fillna(0).astype(np.int64)
    df.index.name = "Datetime" if state.intraday else "Date"
    if not state.actions:
        df = df.drop(
            columns=["Dividends", "Stock Splits", "Capital Gains"],
            errors="ignore",
        )
    if not state.keepna:
        data_cols = [
            column
            for column in (
                _PRICE_COLNAMES_
                + ["Volume", "Dividends", "Stock Splits", "Capital Gains"]
            )
            if column in df.columns
        ]
        mask_nan_or_zero = cast(
            pd.Series,
            (df[data_cols].isna() | (df[data_cols] == 0)).all(axis=1),
        )
        df = df.loc[~mask_nan_or_zero]
    if state.interval != state.interval_user:
        df = state.history_obj.resample_history(
            df,
            state.interval,
            state.interval_user,
            state.period_user,
        )
    _log_ohlc_range(state, "yfinance returning OHLC", df)
    state.history_obj.clear_reconstruct_start_interval(state.interval)
    return df


def fetch_history(price_history, request: _HistoryRequest) -> pd.DataFrame:
    """Fetch chart data from Yahoo and return a normalized history dataframe."""
    state = _build_fetch_state(price_history, request)
    _warn_raise_errors(state)
    failed_df = _normalize_repair_request(state)
    if failed_df is not None:
        return failed_df
    failed_df = _ensure_timezone(state)
    if failed_df is not None:
        return failed_df
    _resolve_period_and_dates(state)
    _build_request_params(state)
    _log_request_params(state)
    data = _fetch_chart_data(state)
    _update_history_metadata(state, data)
    _build_price_debug(state)
    failed_df = _validate_chart_data(state, data)
    if failed_df is not None:
        return failed_df
    _load_metadata_values(state)
    quotes = _extract_quotes(state)
    dividends, splits, capital_gains = _extract_actions(state)
    dividends, splits, capital_gains = _slice_actions_to_window(
        state,
        quotes,
        dividends,
        splits,
        capital_gains,
    )
    quotes, dividends, splits = _localize_interday_actions(
        state,
        quotes,
        dividends,
        splits,
    )
    df = _merge_action_frames(state, quotes, dividends, splits, capital_gains)
    df, last_trade = utils.fix_yahoo_returning_live_separate(
        df,
        state.params["interval"],
        state.tz_exchange,
        state.prepost,
        repair_context={"repair": state.repair, "currency": state.currency},
    )
    df = cast(pd.DataFrame, df)
    if isinstance(last_trade, pd.Series):
        state.history_obj.set_history_metadata_value(
            "lastTrade",
            {"Price": last_trade["Close"], "Time": last_trade.name},
        )
    df = cast(pd.DataFrame, df[~df.index.duplicated(keep="first")])
    df = _repair_prices_if_needed(state, df)
    df = _apply_price_adjustment(state, df)
    return _finalize_history_df(state, df)
