import json as _json

from .const import _QUERY1_URL_
from .data import YfData
from . import utils
import time
from datetime import (
    datetime as dt,
    timezone as tz,
    timedelta as delta
)
import typing as t

class Status:
    def __init__(self, region:'str', session=None, proxy=None, timeout=30, raise_errors=True):
        self.region = region
        self.session = session
        self.proxy = proxy
        self.timeout = timeout
        self.raise_errors = raise_errors

        self._data = YfData(session=self.session)
        self._logger = utils.get_yf_logger()
        self.next_req = 0

        self._parse_data(True)


    def _fetch_data(self):
        url = f"{_QUERY1_URL_}/v6/finance/markettime"
        params = {
            "formatted": True,
            "key": "finance",
            "lang": "en-GB",
            "region": self.region
        }
        data = self._data.cache_get(url=url, params=params, proxy=self.proxy, timeout=self.timeout)
        if data is None or "Will be right back" in data.text:
            raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                               "Our engineers are working quickly to resolve "
                               "the issue. Thank you for your patience.")
        try:
            data = data.json()
            self._raw_data = data["finance"]["marketTimes"][0]["marketTime"][0]
            type_change = self._raw_data["duration"][0]
            self.next_req = time.time() + int(type_change["hrs"])*3600 + int(type_change["mins"])*60
        except _json.JSONDecodeError:
            self._logger.error(f"{self.region}: Failed to retrieve market statusand recieved faulty data.")
            self._raw_data = {}

    def _parse_data(self, force:'bool'=False):
        if time.time() < self.next_req and not force:
            self._logger.info(f"Caching has blocked this request as the data shouldn't change for {time.time() - self.next_req} seconds. If you want to force a refresh of data set `force = True`")
            return
        self._fetch_data()

        if self._raw_data == {}:
            self._close = None
            self._open = None
            self._time = None
            self._tz = None
            self._is_open = None
            return
        
        self._close = dt.fromisoformat(self._raw_data["close"])
        self._open = dt.fromisoformat(self._raw_data["open"])
        self._time = dt.fromisoformat(self._raw_data["time"])
        self._tz = tz(delta(hours=int(self._raw_data["timezone"][0]["gmtoffset"])/6000), name=self._raw_data["timezone"][0]["short"])
        self._is_open = True if self._raw_data["status"] == "open" else False

    @property
    def raw_data(self):
        self._parse_data()
        return self._raw_data

    @property
    def id(self) -> 't.Optional[str]':
        return self.raw_data.get("id", None)
    
    @property
    def name(self) -> 'str':
        return self.raw_data.get("name", None)
    
    @property
    def status(self) -> 't.Literal["open", "closed", None]':
        return self.raw_data.get("status", None)
    
    @property
    def yf_market_id(self) -> 't.Optional[str]':
        return self.raw_data.get("yfit_market_id", None)
    
    @property
    def close(self) -> 't.Optional[dt]':
        return self._close
    
    @property
    def message(self) -> 't.Optional[str]':
        return self.raw_data.get("message", None)

    @property
    def open(self) -> 't.Optional[dt]':
        return self._open
    
    @property
    def yf_status(self) -> 't.Optional[str]': # Move research needed for specific string literals
        return self.raw_data.get("yfit_market_status", None)
    
    @property
    def request_time(self) -> 't.Optional[dt]':
        return self._time

    @property
    def timezone(self) -> 't.Optional[tz]':
        return self._tz

    @property
    def is_open(self) -> 't.Optional[bool]':
        return self._is_open