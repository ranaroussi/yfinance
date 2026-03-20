"""Shared HTTP-layer helpers for Yahoo response handling."""

import json as _json
from typing import Any, Dict

from ..config import YF_CONFIG as YfConfig
from ..exceptions import YFDataException


def parse_json_response(
    response: Any, logger, error_message: str, *error_args: Any
) -> Dict[str, Any]:
    """Parse a Yahoo response body into JSON with common downtime handling."""
    if response is None or "Will be right back" in response.text:
        raise YFDataException("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***")

    try:
        return response.json()
    except _json.JSONDecodeError:
        if YfConfig.debug.raise_on_error:
            raise
        logger.error(error_message, *error_args)
        return {}


def log_response_payload(logger, data: Any) -> None:
    """Emit a debug dump of an unexpected Yahoo payload."""
    logger.debug("Got response: ")
    logger.debug("-------------")
    logger.debug(" %s", data)
    logger.debug("-------------")
