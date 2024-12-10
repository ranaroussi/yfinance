import json as _json

from .const import _QUERY1_URL_
from .data import YfData
from . import utils
import time
from datetime import (
    datetime as dt,
    timezone as tz,
    deltatime as delta
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
            self.raw_data = data["marketTimes"][0]["marketTime"][0]
            type_change = self.raw_data["duration"][0]
            self.next_req = time.time() + type_change["hrs"]*3600 + type_change["mins"]*60
        except _json.JSONDecodeError:
            self._logger.error(f"{self.query}: Failed to retrieve market statusand recieved faulty data.")
            self.raw_data = {}

    def _parse_data(self, force:'bool'=False):
        if time.time() < self.next_req and not force:
            self._logger.info(f"Caching has blocked this request as the data shouldn't change for {time.time() - self.next_req} seconds. If you want to force a refresh of data set `force = True`")
            return
        self._fetch_data()
        
        self._close = dt.fromisoformat(self.raw_data["close"])
        self._open = dt.fromisoformat(self.raw_data["open"])
        self._time = dt.fromisoformat(self.raw_data["time"])
        self._tz = tz(delta(int(self.raw_data["timezone"][0]["gmtoffset"])/6000), name=self.raw_data["short"])
        self._is_open = True if self.raw_data["status"] == "open" else False

    @property
    def id(self) -> 'str':
        return self.raw_data["id"]
    
    @property
    def name(self) -> 'str':
        return self.raw_data["name"]
    
    @property
    def status(self) -> 't.Literal["open", "closed"]':
        return self.raw_data["status"]
    
    @property
    def yf_market_id(self) -> 'str':
        return self.raw_data["yfit_market_id"]
    
    @property
    def close(self) -> 'dt':
        return self._close
    
    @property
    def message(self) -> 'str':
        return self.raw_data["message"]

    @property
    def open(self) -> 'dt':
        return self._open
    
    @property
    def yf_status(self) -> 'str': # Move research needed for specific string literals
        return self.raw_data["yfit_market_status"]
    
    @property
    def request_time(self) -> 'dt':
        return self._time

    @property
    def timezone(self) -> 'tz':
        return self._tz

    @property
    def is_open(self) -> 'bool':
        return self._is_open