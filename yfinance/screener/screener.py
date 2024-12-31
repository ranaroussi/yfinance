from typing import TypedDict, Union

from yfinance import utils
from yfinance.data import YfData
from yfinance.const import _BASE_URL_, PREDEFINED_SCREENERS, PREDEFINED_SCREENERS_TYPE
from .screener_query import Query

_SCREENER_URL_ = f"{_BASE_URL_}/v1/finance/screener"
_PREDEFINED_URL_ = f"{_SCREENER_URL_}/predefined/saved"

_REQUIRED_BODY_TYPE_ = TypedDict("_REQUIRED_BODY_TYPE_", {'offset': int, 'size': int, 'sortField': str, 'sortType': str, 'quoteType': str, 'query': dict, 'userId': str, 'userIdType': str})
_BODY_TYPE_ = TypedDict("_BODY_TYPE_", {'offset': int, 'size': int, 'sortField': str, 'sortType': str, 'quoteType': str, 'query': dict, 'userId': str, 'userIdType': str}, total=False)
_EMPTY_DICT_ = TypedDict("_EMPTY_DICT_", {}, total=False)

class Screener:
    """
    The `Screener` class is used to execute the queries and return the filtered results.

    The Screener class provides methods to set and manipulate the body of a screener request,
    fetch and parse the screener results, and access predefined screener bodies.
    """
    PREDEFINED_SCREENERS = PREDEFINED_SCREENERS

    def __init__(self, session=None, proxy=None):
        """
        Args:
            session (requests.Session, optional): A requests session object to be used for making HTTP requests. Defaults to None.
            proxy (str, optional): A proxy URL to be used for making HTTP requests. Defaults to None.
        
        .. seealso::

            :attr:`Screener.predefined_bodies <yfinance.Screener.predefined_bodies>`
                supported predefined screens
        """
        self.proxy = proxy
        self.session = session

        self._data: YfData = YfData(session=session)
        self._body: 'Union[_REQUIRED_BODY_TYPE_, _EMPTY_DICT_]' = {}
        self._response: 'dict' = {}
        self._body_updated = False
        self._accepted_body_keys = {"offset","size","sortField","sortType","quoteType","query","userId","userIdType"}
        self.predefined = False

    @property
    def body(self) -> 'Union[_REQUIRED_BODY_TYPE_, _EMPTY_DICT_]':
        return self._body
    
    @property
    def response(self) -> 'dict':
        """
        Fetch screen result

        Example:

            .. code-block:: python

                result = screener.response
                symbols = [quote['symbol'] for quote in result['quotes']]
        """
        if self._body_updated or self._response is None:
            self._fetch_and_parse()
        
        self._body_updated = False
        return self._response
    
    def set_default_body(self, query: 'Query', offset: 'int' = 0, size: 'int' = 100, sortField: 'str' = "ticker", sortType: 'str' = "desc", quoteType: 'str' = "equity", userId: 'str' = "", userIdType: 'str' = "guid") -> 'Screener':
        """
        Set the default body using a custom query.

        Args:
            query (Query): The Query object to set as the body.
            offset (Optional[int]): The offset for the results. Defaults to 0.
            size (Optional[int]): The number of results to return. Defaults to 100. Maximum is 250 as set by Yahoo.
            sortField (Optional[str]): The field to sort the results by. Defaults to "ticker".
            sortType (Optional[str]): The type of sorting (e.g., "asc" or "desc"). Defaults to "desc".
            quoteType (Optional[str]): The type of quote (e.g., "equity"). Defaults to "equity".
            userId (Optional[str]): The user ID. Defaults to an empty string.
            userIdType (Optional[str]): The type of user ID (e.g., "guid"). Defaults to "guid".
        
        Returns:
            Screener: self

        Example:

            .. code-block:: python

                screener.set_default_body(qf)
        """
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
        return self
    @classmethod
    def set_predefined_body(cls, predefined_key: 'PREDEFINED_SCREENERS_TYPE') -> 'Screener':
        """
        Set a predefined body

        Args: 
            predefined_key (str): key to one of predefined screens 

        Returns:
            Screener: self

        Example:

            .. code-block:: python

                screener.set_predefined_body('day_gainers')
        
                
        .. seealso::

            :attr:`Screener.predefined_bodies <yfinance.Screener.predefined_bodies>`
                supported predefined screens
        """
        self = cls()
        self._body_updated = True
        self.predefined = predefined_key
        return self

    def set_body(self, body: '_REQUIRED_BODY_TYPE_') -> 'Screener':
        """
        Set the fully custom body using dictionary input

        Args: 
            body (dict): full query body

        Returns:
            Screener: self
        
        Example:

            .. code-block:: python

                screener.set_body({
                    "offset": 0,
                    "size": 100,
                    "sortField": "ticker",
                    "sortType": "desc",
                    "quoteType": "equity",
                    "query": qf.to_dict(),
                    "userId": "",
                    "userIdType": "guid"
                })
        """
        missing_keys = [key for key in self._accepted_body_keys if key not in body]
        if missing_keys:
            raise ValueError(f"Missing required keys in body: {missing_keys}")

        extra_keys = [key for key in body if key not in self._accepted_body_keys]
        if extra_keys:
            raise ValueError(f"Body contains extra keys: {extra_keys}")

        self._body_updated = True
        self._body = body
        self.predefined = False
        return self

    def patch_body(self, values: '_BODY_TYPE_') -> 'Screener':
        """
        Patch parts of the body using dictionary input

        Args: 
            body (Dict): partial query body

        Returns:
            Screener: self

        Example:

            .. code-block:: python

                screener.patch_body({"offset": 100})
        """
        extra_keys = [key for key in values if key not in self._accepted_body_keys]
        if extra_keys:
            raise ValueError(f"Body contains extra keys: {extra_keys}")
        
        self._body_updated = True
        for k in values:
            self._body[k] = values[k]
        self.predefined = False
        return self

    def _validate_body(self) -> None:
        if not all(k in self._body for k in self._accepted_body_keys):
            raise ValueError("Missing required keys in body")
        
        if self._body["size"] > 250:
            raise ValueError("Yahoo limits query size to 250. Please decrease the size of the query.")

    def _fetch(self) -> 'dict':
        params_dict = {"corsDomain": "finance.yahoo.com", "formatted": "false", "lang": "en-US", "region": "US"}
        response = self._data.post(_SCREENER_URL_, body=self.body, user_agent_headers=self._data.user_agent_headers, params=params_dict, proxy=self.proxy)
        response.raise_for_status()
        return response.json()
    
    def _fetch_predefined(self) -> 'dict':
        params_dict = {
            "count": 25,
            "formatted": True,
            "scrIds": self.predefined,
            "sortField": "",
            "sortType": None,
            "start": 0,
            "useRecordsResponse": False,
            "fields": ["ticker", "symbol", "longName", "sparkline", "shortName", "regularMarketPrice", "regularMarketChange", "regularMarketChangePercent", "regularMarketVolume", "averageDailyVolume3Month", "marketCap", "trailingPE", "regularMarketOpen"],
            "lang": "en-US", 
            "region": "US"
        }
        response = self._data.get(_PREDEFINED_URL_, user_agent_headers=self._data.user_agent_headers, params=params_dict, proxy=self.proxy)
        response.raise_for_status()
        return response.json()
    
    def _fetch_and_parse(self) -> None:
        response = None

        try:
            if self.predefined != False:
                response = self._fetch_predefined()
            else:
                self._validate_body()
                response = self._fetch()
            self._response = response['finance']['result'][0]
        except Exception as e:
            logger = utils.get_yf_logger()
            logger.error(f"Failed to get screener data for '{self._body.get('query', 'query not set')}' reason: {e}")
            logger.debug("Got response: ")
            logger.debug("-------------")
            logger.debug(f" {response}")
            logger.debug("-------------")

    @property
    def quotes(self) -> 'list[dict]':
        if self.predefined:
            return self.response.get("quotes", [])
        
        return self.response.get("records", [])
