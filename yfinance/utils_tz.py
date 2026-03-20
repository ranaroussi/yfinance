"""Shared timezone lookup helpers for ticker history consumers."""

from __future__ import annotations

from typing import Any, Callable, Optional

from curl_cffi import requests

from . import cache, utils
from .config import YF_CONFIG as YfConfig
from .const import _BASE_URL_

_TZ_INFO_FETCH_CTR = {"count": 0}


def fetch_ticker_tz(data_client, ticker: str, timeout) -> Optional[str]:
    """Fetch exchange timezone directly from Yahoo chart metadata."""
    logger = utils.get_yf_logger()
    params = {"range": "1d", "interval": "1d"}
    url = f"{_BASE_URL_}/v8/finance/chart/{ticker}"

    try:
        response = data_client.cache_get(url=url, params=params, timeout=timeout)
        data = response.json()
    except (
        requests.exceptions.RequestException,
        AttributeError,
        TypeError,
        ValueError,
    ) as error:
        if YfConfig.debug.raise_on_error:
            raise
        logger.error("Failed to get ticker '%s' reason: %s", ticker, error)
        return None

    error = data.get("chart", {}).get("error")
    if error:
        logger.debug("Got error from yahoo api for ticker %s, Error: %s", ticker, error)
        return None

    try:
        return data["chart"]["result"][0]["meta"]["exchangeTimezoneName"]
    except (IndexError, KeyError, TypeError) as error:
        if YfConfig.debug.raise_on_error:
            raise
        logger.error(
            "Could not get exchangeTimezoneName for ticker '%s' reason: %s",
            ticker,
            error,
        )
        return None


def get_ticker_tz(
    data_client,
    ticker: str,
    timeout,
    *,
    info_provider: Optional[Callable[[], dict[str, Any]]] = None,
) -> Optional[str]:
    """Resolve and cache the ticker exchange timezone."""
    tz_cache = cache.get_tz_cache()
    tz = tz_cache.lookup(ticker)

    if tz is not None and (not isinstance(tz, str) or not utils.is_valid_timezone(tz)):
        tz_cache.store(ticker, None)
        tz = None

    if tz is None:
        tz = fetch_ticker_tz(data_client, ticker, timeout)
        if tz is None and info_provider is not None and _TZ_INFO_FETCH_CTR["count"] < 2:
            _TZ_INFO_FETCH_CTR["count"] += 1
            info = info_provider()
            for key in ("exchangeTimezoneName", "timeZoneFullName"):
                value = info.get(key)
                if isinstance(value, str):
                    tz = value
                    break
        if isinstance(tz, str) and utils.is_valid_timezone(tz):
            tz_cache.store(ticker, tz)
        else:
            tz = None

    return tz
