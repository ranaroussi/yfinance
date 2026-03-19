"""Internal scraper helpers shared by quote-summary consumers."""

from typing import Any, Optional

import curl_cffi

from yfinance.config import YF_CONFIG as YfConfig
from yfinance.const import _QUOTE_SUMMARY_URL_, quote_summary_valid_modules
from yfinance.data import YfData
from yfinance.exceptions import YFException
from .. import utils


def get_raw_json_or_none(
    data: YfData,
    url: str,
    params: dict[str, Any],
) -> Optional[dict[str, Any]]:
    """Fetch raw JSON and suppress recoverable Yahoo HTTP errors when configured."""
    try:
        return data.get_raw_json(url, params=params)
    except curl_cffi.requests.exceptions.HTTPError as err:
        if not YfConfig.debug.hide_exceptions:
            raise
        response_text = err.response.text if err.response is not None else ""
        utils.get_yf_logger().error("%s%s", err, response_text)
        return None


def fetch_quote_summary(
    data: YfData,
    symbol: str,
    modules: list[str],
) -> Optional[dict[str, Any]]:
    """Fetch selected quote-summary modules for one symbol."""
    if not isinstance(modules, list):
        raise YFException(
            "Should provide a list of modules, see available modules using "
            "`valid_modules`"
        )

    module_param = ",".join(
        module for module in modules if module in quote_summary_valid_modules
    )
    if len(module_param) == 0:
        raise YFException(
            "No valid modules provided, see available modules using "
            "`valid_modules`"
        )

    params_dict = {
        "modules": module_param,
        "corsDomain": "finance.yahoo.com",
        "formatted": "false",
        "symbol": symbol,
    }
    return get_raw_json_or_none(data, f"{_QUOTE_SUMMARY_URL_}/{symbol}", params_dict)
