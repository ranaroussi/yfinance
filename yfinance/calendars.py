"""Calendar query utilities for earnings, IPO, economic events, and splits."""

from __future__ import annotations

from datetime import date, datetime, timedelta
import json
import warnings
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from requests import Response, Session, exceptions

from .const import _QUERY1_URL_
from .utils import log_indent_decorator, get_yf_logger, _parse_user_dt
from .screener import screen
from .data import YfData
from .exceptions import YFException


class CalendarQuery:
    """
    Simple CalendarQuery class for calendar queries, similar to yf.screener.query.QueryBase.

    Simple operand accepted by YF is of the form:
        `{ "operator": operator, "operands": [field, ...values] }`

    Nested operand accepted by YF:
        `{ "operator": operator, "operands": [ ...CalendarQuery ] }`

    ### Simple example:
    ```python
    op = CalendarQuery('eq', ['ticker', 'AAPL'])
    print(op.to_dict())
    ```
    """

    def __init__(self, operator: str, operand: Union[List[Any], List["CalendarQuery"]]):
        """
        :param operator: Operator string, e.g., 'eq', 'gte', 'and', 'or'.
            :param operand: List of operands: can be values (str, int), or nested
                CalendarQuery instances.
        """
        operator = operator.upper()
        self.operator = operator
        self.operands = operand

    def append(self, operand: Any) -> None:
        """
        Append an operand to the operands list.

        :param operand: CalendarQuery to append (can be value or CalendarQuery instance).
        """
        self.operands.append(operand)

    @property
    def is_empty(self) -> bool:
        """
        Check if the operands list is empty.

        :return: True if operands list is empty, False otherwise.
        """
        return len(self.operands) == 0

    def to_dict(self) -> dict:
        """
        Query-ready dict for YF.

        Simple operand accepted by YF is of the form:
            `{ "operator": operator, "operands": [field, ...values] }`

        Nested operand accepted by YF:
            `{ "operator": operator, "operands": [ ...CalendarQuery ] }`
        """
        op = self.operator
        ops = self.operands
        return {
            "operator": op,
            "operands": [o.to_dict() if isinstance(o, CalendarQuery) else o for o in ops],
        }


_CALENDAR_URL_ = f"{_QUERY1_URL_}/v1/finance/visualization"
DATE_STR_FORMAT = "%Y-%m-%d"

PREDEFINED_CALENDARS = {
    "sp_earnings": {
        "sortField": "intradaymarketcap",
        "includeFields": [
            "ticker",
            "companyshortname",
            "intradaymarketcap",
            "eventname",
            "startdatetime",
            "startdatetimetype",
            "epsestimate",
            "epsactual",
            "epssurprisepct",
        ],
        "nan_cols": ["Surprise (%)", "EPS Estimate", "Reported EPS"],
        "datetime_cols": ["Event Start Date"],
        "df_index": "Symbol",
        "renames": {
            "Surprise (%)": "Surprise(%)",
            "Company Name": "Company",
            "Market Cap (Intraday)": "Marketcap",
        },
    },
    "ipo_info": {
        "sortField": "startdatetime",
        "includeFields": [
            "ticker",
            "companyshortname",
            "exchange_short_name",
            "filingdate",
            "startdatetime",
            "amendeddate",
            "pricefrom",
            "priceto",
            "offerprice",
            "currencyname",
            "shares",
            "dealtype",
        ],
        "nan_cols": ["Price From", "Price To", "Price", "Shares"],
        "datetime_cols": ["Filing Date", "Date", "Amended Date"],
        "df_index": "Symbol",
        "renames": {
            "Exchange Short Name": "Exchange",
        },
    },
    "economic_event": {
        "sortField": "startdatetime",
        "includeFields": [
            "econ_release",
            "country_code",
            "startdatetime",
            "period",
            "after_release_actual",
            "consensus_estimate",
            "prior_release_actual",
            "originally_reported_actual",
        ],
        "nan_cols": ["Actual", "Market Expectation", "Prior to This", "Revised from"],
        "datetime_cols": ["Event Time"],
        "df_index": "Event",
        "renames": {
            "Country Code": "Region",
            "Market Expectation": "Expected",
            "Prior to This": "Last",
            "Revised from": "Revised",
        },
    },
    "splits": {
        "sortField": "startdatetime",
        "includeFields": [
            "ticker",
            "companyshortname",
            "startdatetime",
            "optionable",
            "old_share_worth",
            "share_worth",
        ],
        "nan_cols": [],
        "datetime_cols": ["Payable On"],
        "df_index": "Symbol",
        "renames": {
            "Optionable?": "Optionable",
        },
    },
}


class Calendars:
    """
    Get economic calendars, for example, Earnings, IPO, Economic Events, Splits

    ### Simple example default params:
    ```python
    import yfinance as yf
    calendars = yf.Calendars()
    earnings_calendar = calendars.get_earnings_calendar(limit=50)
    print(earnings_calendar)
    ```"""

    def __init__(
        self,
        start: Optional[Union[str, datetime, date]] = None,
        end: Optional[Union[str, datetime, date]] = None,
        session: Optional[Session] = None,
    ):
        """
        :param str | datetime | date start: start date (default today) \
            eg. start="2025-11-08"
        :param str | datetime | date end: end date (default `start + 7 days`) \
            eg. end="2025-11-08"
        :param session: requests.Session object, optional
        """

        self._logger = get_yf_logger()
        self._data: YfData = YfData(session=session)

        _start = self._parse_date_param(start)
        _end = self._parse_date_param(end)
        self._start = _start or datetime.now().strftime(DATE_STR_FORMAT)
        self._end = _end or (
            datetime.strptime(self._start, DATE_STR_FORMAT) + timedelta(days=7)
        ).strftime(DATE_STR_FORMAT)

        if not start and end:
            self._logger.debug(
                "Incomplete boundary: did not provide `start`, using start=%s end=%s",
                self._start,
                self._end,
            )
        elif start and not end:
            self._logger.debug(
                "Incomplete boundary: did not provide `end`, using start=%s end=%s "
                "(+7 days from start)",
                self._start,
                self._end,
            )

        self._most_active_qy: CalendarQuery = CalendarQuery("or", [])

        self._cache_request_body = {}
        self.calendars: Dict[str, pd.DataFrame] = {}

    @property
    def session(self) -> Session:
        """Return underlying HTTP session used by this calendars client."""
        return getattr(self._data, "_session")

    def _parse_date_param(self, _date: Optional[Union[str, datetime, date, int]]) -> str:
        if not _date:
            return ""
        return _parse_user_dt(_date).strftime(DATE_STR_FORMAT)

    def _parse_time_window(
        self, args: tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> tuple[Any, Any, Dict[str, Any]]:
        expected_keys = ("start", "end", "limit", "offset", "force")
        if len(args) > len(expected_keys):
            raise TypeError(
                f"Expected at most {len(expected_keys)} positional arguments, "
                f"received {len(args)}."
            )
        unknown = set(kwargs) - set(expected_keys)
        if unknown:
            unknown_fields = ", ".join(sorted(unknown))
            raise TypeError(f"Unexpected keyword argument(s): {unknown_fields}")

        merged: Dict[str, Any] = dict(zip(expected_keys, args))
        merged.update(kwargs)

        request = {
            "limit": merged.get("limit", 12),
            "offset": merged.get("offset", 0),
            "force": merged.get("force", False),
        }
        return merged.get("start"), merged.get("end"), request

    def _parse_earnings_request(
        self, args: tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> tuple[Optional[float], bool, Any, Any, Dict[str, Any]]:
        expected_keys = (
            "market_cap",
            "filter_most_active",
            "start",
            "end",
            "limit",
            "offset",
            "force",
        )
        if len(args) > len(expected_keys):
            raise TypeError(
                f"Expected at most {len(expected_keys)} positional arguments, "
                f"received {len(args)}."
            )
        unknown = set(kwargs) - set(expected_keys)
        if unknown:
            unknown_fields = ", ".join(sorted(unknown))
            raise TypeError(f"Unexpected keyword argument(s): {unknown_fields}")

        merged: Dict[str, Any] = dict(zip(expected_keys, args))
        merged.update(kwargs)

        request = {
            "limit": merged.get("limit", 12),
            "offset": merged.get("offset", 0),
            "force": merged.get("force", False),
        }
        market_cap = merged.get("market_cap")
        filter_most_active = merged.get("filter_most_active", True)
        return market_cap, filter_most_active, merged.get("start"), merged.get("end"), request

    @staticmethod
    def _warn_partial_bounds(start: Any, end: Any) -> None:
        if (start and not end) or (end and not start):
            warnings.warn(
                (
                    "When providing custom `start` and `end` parameters, you may "
                    "want to specify both to avoid unexpected behavior."
                ),
                UserWarning,
                stacklevel=3,
            )

    def _get_data(
        self,
        calendar_type: str,
        query: CalendarQuery,
        request: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        request = request or {}
        limit = request.get("limit", 12)
        offset = request.get("offset", 0)
        force = request.get("force", False)

        if calendar_type not in PREDEFINED_CALENDARS:
            raise YFException(f"Unknown calendar type: {calendar_type}")

        params = {"lang": "en-US", "region": "US"}
        body = {
            "sortType": "DESC",
            "entityIdType": calendar_type,
            "sortField": PREDEFINED_CALENDARS[calendar_type]["sortField"],
            "includeFields": PREDEFINED_CALENDARS[calendar_type]["includeFields"],
            "size": min(limit, 100),  # YF caps at 100, don't go higher
            "offset": offset,
            "query": query.to_dict(),
        }

        if self._cache_request_body.get(calendar_type, None) and not force:
            cache_body = self._cache_request_body[calendar_type]
            if cache_body == body and calendar_type in self.calendars:
                # Uses cache if force=False and new request has same body as previous
                self._logger.debug(
                    "Getting calendar_type=%s from local cache", calendar_type
                )
                return self.calendars[calendar_type]
        self._cache_request_body[calendar_type] = body

        self._logger.debug("Fetching calendar_type=%s with limit=%s", calendar_type, limit)
        response: Response = self._data.post(_CALENDAR_URL_, params=params, body=body)

        try:
            json_data = response.json()
        except json.JSONDecodeError:
            self._logger.error("%s: Failed to retrieve calendar.", calendar_type)
            json_data = {}

        # Error returned
        if json_data.get("finance", {}).get("error", {}):
            raise YFException(json_data.get("finance", {}).get("error", {}))

        self.calendars[calendar_type] = self._create_df(json_data)
        return self._cleanup_df(calendar_type)

    def _create_df(self, json_data: dict) -> pd.DataFrame:
        columns = []
        for col in json_data["finance"]["result"][0]["documents"][0]["columns"]:
            columns.append(col["label"])

            if col["label"] == "Event Start Date" and col["type"] == "STRING":
                # Rename duplicate columns Event Start Date
                columns[-1] = "Timing"

        rows = json_data["finance"]["result"][0]["documents"][0]["rows"]
        return pd.DataFrame(rows, columns=columns)

    def _cleanup_df(self, calendar_type: str) -> pd.DataFrame:
        predef_cal: dict = PREDEFINED_CALENDARS[calendar_type]
        df: pd.DataFrame = self.calendars[calendar_type]
        if df.empty:
            return df

        # Convert types
        nan_cols: list = predef_cal["nan_cols"]
        if nan_cols:
            df[nan_cols] = df[nan_cols].astype("float64").replace(0.0, np.nan)

        # Format the dataframe
        df.set_index(predef_cal["df_index"], inplace=True)
        for rename_from, rename_to in predef_cal["renames"].items():
            df.rename(columns={rename_from: rename_to}, inplace=True)

        for datetime_col in predef_cal["datetime_cols"]:
            df[datetime_col] = pd.to_datetime(df[datetime_col])

        return df

    @log_indent_decorator
    def _get_most_active_operands(
        self, _market_cap: Optional[float], force=False
    ) -> CalendarQuery:
        """
        Retrieve tickers from YF, converts them into operands accepted by YF.
        Saves the operands in self._most_active_qy.
        Will not re-query if already populated.

        Used for earnings calendar optional filter.

        :param force: if True, will re-query even if operands already exist
        :return: list of operands for active traded stocks
        """
        if not self._most_active_qy.is_empty and not force:
            return self._most_active_qy

        self._logger.debug("Fetching 200 most_active for earnings calendar")

        try:
            json_raw: dict = screen(query="MOST_ACTIVES", count=200)
        except exceptions.HTTPError:
            self._logger.error("Failed to retrieve most active stocks.")
            return self._most_active_qy

        raw = json_raw.get("quotes", [{}])

        self._most_active_qy = CalendarQuery("or", [])
        for stock in raw:
            if not isinstance(stock, dict):
                continue

            ticker = stock.get("symbol", "")
            t_market_cap = stock.get("marketCap", 0)
            # We filter market_cap here because we want to keep self._most_active_qy consistent
            if ticker and (_market_cap is None or t_market_cap >= _market_cap):
                self._most_active_qy.append(CalendarQuery("eq", ["ticker", ticker]))

        return self._most_active_qy

    def _get_startdatetime_operators(self, start=None, end=None) -> CalendarQuery:
        """
        Get startdatetime operands for start/end dates.
        If no dates passed, defaults to internal date set on initialization.
        """
        _start = self._parse_date_param(start)
        _end = self._parse_date_param(end)
        self._warn_partial_bounds(start, end)

        return CalendarQuery(
            "and",
            [
                CalendarQuery("gte", ["startdatetime", _start or self._start]),
                CalendarQuery("lte", ["startdatetime", _end or self._end]),
            ],
        )

    ### Manual getter functions:

    @log_indent_decorator
    def get_earnings_calendar(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """
        Retrieve earnings calendar from YF as a DataFrame.
        Will re-query every time it is called, overwriting previous data.

        :param args: optional positional args:
            ``market_cap, filter_most_active, start, end, limit, offset, force``.
        :param kwargs: optional legacy keyword args:
            ``market_cap``, ``filter_most_active``, ``start``, ``end``,
            ``limit``, ``offset``, ``force``.
        :return: DataFrame with earnings calendar
        """
        market_cap, filter_most_active, start, end, request = self._parse_earnings_request(
            args, kwargs
        )
        _start = self._parse_date_param(start)
        _end = self._parse_date_param(end)
        self._warn_partial_bounds(start, end)

        query = CalendarQuery(
            "and",
            [
                CalendarQuery("eq", ["region", "us"]),
                CalendarQuery(
                    "or",
                    [
                        CalendarQuery("eq", ["eventtype", "EAD"]),
                        CalendarQuery("eq", ["eventtype", "ERA"]),
                    ],
                ),
                CalendarQuery("gte", ["startdatetime", _start or self._start]),
                CalendarQuery("lte", ["startdatetime", _end or self._end]),
            ],
        )

        if market_cap is not None:
            if market_cap < 10_000_000:
                warnings.warn(
                    f"market_cap {market_cap} is very low, did you mean to set it higher?",
                    UserWarning,
                    stacklevel=2,
            )
            query.append(CalendarQuery("gte", ["intradaymarketcap", market_cap]))
        if filter_most_active and not request["offset"]:
            # YF does not like filter most active while offsetting
            query.append(self._get_most_active_operands(market_cap))

        return self._get_data(
            calendar_type="sp_earnings",
            query=query,
            request=request,
        )

    @log_indent_decorator
    def get_ipo_info_calendar(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """
        Retrieve IPOs calendar from YF as a Dataframe.

        :param args: optional legacy positional args:
            ``start, end, limit, offset, force``.
        :param kwargs: optional legacy keyword args:
            ``start``, ``end``, ``limit``, ``offset``, ``force``.
        :return: DataFrame with IPOs calendar
        """
        start, end, request = self._parse_time_window(args, kwargs)
        _start = self._parse_date_param(start)
        _end = self._parse_date_param(end)
        self._warn_partial_bounds(start, end)

        query = CalendarQuery(
            "or",
            [
                CalendarQuery(
                    "gtelt", ["startdatetime", _start or self._start, _end or self._end]
                ),
                CalendarQuery(
                    "gtelt", ["filingdate", _start or self._start, _end or self._end]
                ),
                CalendarQuery(
                    "gtelt", ["amendeddate", _start or self._start, _end or self._end]
                ),
            ],
        )

        return self._get_data(calendar_type="ipo_info", query=query, request=request)

    @log_indent_decorator
    def get_economic_events_calendar(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """
        Retrieve Economic Events calendar from YF as a DataFrame.

        :param args: optional legacy positional args:
            ``start, end, limit, offset, force``.
        :param kwargs: optional legacy keyword args:
            ``start``, ``end``, ``limit``, ``offset``, ``force``.
        :return: DataFrame with Economic Events calendar
        """
        start, end, request = self._parse_time_window(args, kwargs)
        query = self._get_startdatetime_operators(start, end)
        return self._get_data(calendar_type="economic_event", query=query, request=request)

    @log_indent_decorator
    def get_splits_calendar(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """
        Retrieve Splits calendar from YF as a DataFrame.

        :param args: optional legacy positional args:
            ``start, end, limit, offset, force``.
        :param kwargs: optional legacy keyword args:
            ``start``, ``end``, ``limit``, ``offset``, ``force``.
        :return: DataFrame with Splits calendar
        """
        start, end, request = self._parse_time_window(args, kwargs)
        query = self._get_startdatetime_operators(start, end)
        return self._get_data(calendar_type="splits", query=query, request=request)

    ### Easy / Default getter functions:

    @property
    def earnings_calendar(self) -> pd.DataFrame:
        """Earnings calendar with default settings."""
        if "sp_earnings" in self.calendars:
            return self.calendars["sp_earnings"]
        return self.get_earnings_calendar()

    @property
    def ipo_info_calendar(self) -> pd.DataFrame:
        """IPOs calendar with default settings."""
        if "ipo_info" in self.calendars:
            return self.calendars["ipo_info"]
        return self.get_ipo_info_calendar()

    @property
    def economic_events_calendar(self) -> pd.DataFrame:
        """Economic events calendar with default settings."""
        if "economic_event" in self.calendars:
            return self.calendars["economic_event"]
        return self.get_economic_events_calendar()

    @property
    def splits_calendar(self) -> pd.DataFrame:
        """Splits calendar with default settings."""
        if "splits" in self.calendars:
            return self.calendars["splits"]
        return self.get_splits_calendar()
