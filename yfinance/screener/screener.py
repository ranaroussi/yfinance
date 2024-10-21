from typing import Dict

from yfinance import utils
from yfinance.data import YfData
from yfinance.const import _BASE_URL_, PREDEFINED_SCREENER_BODY_MAP
from .screener_query import Query

_SCREENER_URL_ = f"{_BASE_URL_}/v1/finance/screener"

class Screener:
    def __init__(self, session=None, proxy=None):
        self.proxy = proxy
        self.session = session

        self._data: YfData = YfData(session=session)
        self._body: Dict = {}
        self._response: Dict = {}
        self._body_updated = False
        self._accepted_body_keys = {"offset","size","sortField","sortType","quoteType","query","userId","userIdType"}
        self._predefined_bodies = PREDEFINED_SCREENER_BODY_MAP.keys()

    @property
    def body(self) -> Dict:
        return self._body
    
    @property
    def response(self) -> Dict:
        if self._body_updated or self._response is None:
            self._fetch_and_parse()
        
        self._body_updated = False
        return self._response
    
    @property
    def predefined_bodies(self) -> Dict:
        return self._predefined_bodies

    def set_default_body(self, query: Query, offset: int = 0, size: int = 100, sortField: str = "ticker", sortType: str = "desc", quoteType: str = "equity", userId: str = "", userIdType: str = "guid") -> None:
        self._body_updated = True

        self._body = {
            "offset": offset,
            "size": size,
            "sortField": sortField,
            "sortType": sortType,
            "quoteType": quoteType,
            "query": query.to_dict(),
            "userId": userId,
            "userIdType": userIdType
        }

    def set_predefined_body(self, k: str) -> None:
        body = PREDEFINED_SCREENER_BODY_MAP.get(k, None)
        if not body:
            raise ValueError(f'Invalid key {k} provided for predefined screener')
        
        self._body_updated = True
        self._body = body

    def set_body(self, body: Dict) -> None:
        missing_keys = [key for key in self._accepted_body_keys if key not in body]
        if missing_keys:
            raise ValueError(f"Missing required keys in body: {missing_keys}")

        extra_keys = [key for key in body if key not in self._accepted_body_keys]
        if extra_keys:
            raise ValueError(f"Body contains extra keys: {extra_keys}")

        self._body_updated = True
        self._body = body


    def patch_body(self, values: Dict) -> None:
        extra_keys = [key for key in values if key not in self._accepted_body_keys]
        if extra_keys:
            raise ValueError(f"Body contains extra keys: {extra_keys}")
        
        self._body_updated = True
        for k in values:
            self._body[k] = values[k]

    def _validate_body(self) -> None:
        if not all(k in self._body for k in self._accepted_body_keys):
            raise ValueError("Missing required keys in body")

    def _fetch(self) -> Dict:
        params_dict = {"corsDomain": "finance.yahoo.com", "formatted": "false", "lang": "en-US", "region": "US"}
        response = self._data.post(_SCREENER_URL_, body=self.body, user_agent_headers=self._data.user_agent_headers, params=params_dict, proxy=self.proxy)
        response.raise_for_status()
        return response.json()
    
    def _fetch_and_parse(self) -> None:
        response = None
        self._validate_body()
        
        try:
            response = self._fetch()
            self._response = response['finance']['result'][0]
        except Exception as e:
            logger = utils.get_yf_logger()
            logger.error(f"Failed to get screener data for '{self._body.get('query', 'query not set')}' reason: {e}")
            logger.debug("Got response: ")
            logger.debug("-------------")
            logger.debug(f" {response}")
            logger.debug("-------------")
