from __future__ import annotations # Just in case
import json
from typing import Any, Optional, List, Union, Dict
import warnings
import numpy as np
from requests import Session, Response, exceptions
import pandas as pd
from datetime import datetime, date, timedelta

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
        :param operand: List of operands: can be values (str, int), or other Operands instances (nested).
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
        self.session = session or Session()
        self._data: YfData = YfData(session=session)

        _start = self._parse_date_param(start)
        _end = self._parse_date_param(end)
        self._start = _start or datetime.now().strftime(DATE_STR_FORMAT)
        self._end = _end or (datetime.strptime(self._start, DATE_STR_FORMAT) + timedelta(days=7)).strftime(DATE_STR_FORMAT)

        if not start and end:
            self._logger.debug(f"Incomplete boundary: did not provide `start`, using today {self._start=} to {self._end=}")
        elif start and not end:
            self._logger.debug(f"Incomplete boundary: did not provide `end`, using {self._start=} to {self._end=}: +7 days from self._start")

        self._most_active_qy: CalendarQuery = CalendarQuery("or", [])

        self._cache_request_body = {}
        self.calendars: Dict[str, pd.DataFrame] = {}

    def _parse_date_param(self, _date: Optional[Union[str, datetime, date, int]]) -> str:
        if not _date:
            return ""
        else:
            return _parse_user_dt(_date).strftime(DATE_STR_FORMAT)

    def _get_data(
        self, calendar_type: str, query: CalendarQuery, limit=12, offset=0, force=False
    ) -> pd.DataFrame:
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
                self._logger.debug(f"Getting {calendar_type=} from local cache")
                return self.calendars[calendar_type]
        self._cache_request_body[calendar_type] = body

        self._logger.debug(f"Fetching {calendar_type=} with {limit=}")
        response: Response = self._data.post(_CALENDAR_URL_, params=params, body=body)

        try:
            json_data = response.json()
        except json.JSONDecodeError:
            self._logger.error(f"{calendar_type}: Failed to retrieve calendar.")
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
            if type(stock) is not dict:
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
        if (start and not end) or (end and not start):
            warnings.warn(
                "When prividing custom `start` and `end` parameters, you may want to specify both, to avoid unexpected behaviour.",
                UserWarning,
                stacklevel=2,
            )

        return CalendarQuery(
            "and",
            [
                CalendarQuery("gte", ["startdatetime", _start or self._start]),
                CalendarQuery("lte", ["startdatetime", _end or self._end]),
            ],
        )

    ### Manual getter functions:

    @log_indent_decorator
    def get_earnings_calendar(
        self,
        market_cap: Optional[float] = None,
        filter_most_active: bool = True,
        start=None,
        end=None,
        limit=12,
        offset=0,
        force=False,
    ) -> pd.DataFrame:
        """
        Retrieve earnings calendar from YF as a DataFrame.
        Will re-query every time it is called, overwriting previous data.

        :param market_cap: market cap cutoff in USD, default None
        :param filter_most_active: will filter for actively traded stocks (default True)
        :param str | datetime | date start: overwrite start date (default set by __init__) \
            eg. start="2025-11-08"
        :param str | datetime | date end: overwrite end date (default set by __init__) \
            eg. end="2025-11-08"
        :param limit: maximum number of results to return (YF caps at 100)
        :param offset: offsets the results for pagination. YF default 0
        :param force: if True, will re-query even if cache already exists
        :return: DataFrame with earnings calendar
        """
        _start = self._parse_date_param(start)
        _end = self._parse_date_param(end)
        if (start and not end) or (end and not start):
            warnings.warn(
                "When prividing custom `start` and `end` parameters, you may want to specify both, to avoid unexpected behaviour.",
                UserWarning,
                stacklevel=2,
            )

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
        if filter_most_active and not offset:
            # YF does not like filter most active while offsetting
            query.append(self._get_most_active_operands(market_cap))

        return self._get_data(
            calendar_type="sp_earnings",
            query=query,
            limit=limit,
            offset=offset,
            force=force,
        )

    @log_indent_decorator
    def get_ipo_info_calendar(
        self, start=None, end=None, limit=12, offset=0, force=False
    ) -> pd.DataFrame:
        """
        Retrieve IPOs calendar from YF as a Dataframe.

        :param str | datetime | date start: overwrite start date (default set by __init__) \
            eg. start="2025-11-08"
        :param str | datetime | date end: overwrite end date (default set by __init__) \
            eg. end="2025-11-08"
        :param limit: maximum number of results to return (YF caps at 100)
        :param offset: offsets the results for pagination. YF default 0
        :param force: if True, will re-query even if cache already exists
        :return: DataFrame with IPOs calendar
        """
        _start = self._parse_date_param(start)
        _end = self._parse_date_param(end)
        if (start and not end) or (end and not start):
            warnings.warn(
                "When prividing custom `start` and `end` parameters, you may want to specify both, to avoid unexpected behaviour.",
                UserWarning,
                stacklevel=2,
            )

        query = CalendarQuery(
            "or",
            [
                CalendarQuery("gtelt", ["startdatetime", _start or self._start, _end or self._end]),
                CalendarQuery("gtelt", ["filingdate", _start or self._start, _end or self._end]),
                CalendarQuery("gtelt", ["amendeddate", _start or self._start, _end or self._end]),
            ],
        )

        return self._get_data(
            calendar_type="ipo_info",
            query=query,
            limit=limit,
            offset=offset,
            force=force,
        )

    @log_indent_decorator
    def get_economic_events_calendar(
        self, start=None, end=None, limit=12, offset=0, force=False
    ) -> pd.DataFrame:
        """
        Retrieve Economic Events calendar from YF as a DataFrame.

        :param str | datetime | date start: overwrite start date (default set by __init__) \
            eg. start="2025-11-08"
        :param str | datetime | date end: overwrite end date (default set by __init__) \
            eg. end="2025-11-08"
        :param limit: maximum number of results to return (YF caps at 100)
        :param offset: offsets the results for pagination. YF default 0
        :param force: if True, will re-query even if cache already exists
        :return: DataFrame with Economic Events calendar
        """
        return self._get_data(
            calendar_type="economic_event",
            query=self._get_startdatetime_operators(start, end),
            limit=limit,
            offset=offset,
            force=force,
        )

    @log_indent_decorator
    def get_splits_calendar(
        self, start=None, end=None, limit=12, offset=0, force=False
    ) -> pd.DataFrame:
        """
        Retrieve Splits calendar from YF as a DataFrame.

        :param str | datetime | date start: overwrite start date (default set by __init__) \
            eg. start="2025-11-08"
        :param str | datetime | date end: overwrite end date (default set by __init__) \
            eg. end="2025-11-08"
        :param limit: maximum number of results to return (YF caps at 100)
        :param offset: offsets the results for pagination. YF default 0
        :param force: if True, will re-query even if cache already exists
        :return: DataFrame with Splits calendar
        """
        return self._get_data(
            calendar_type="splits",
            query=self._get_startdatetime_operators(start, end),
            limit=limit,
            offset=offset,
            force=force,
        )

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
