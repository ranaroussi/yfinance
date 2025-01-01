import logging
from typing_extensions import TypedDict
from yfinance.const import _BASE_URL_, PREDEFINED_SCREENERS
from yfinance.data import YfData
from yfinance import utils
from .query import Query

from typing import Any, TypedDict

_SCREENER_URL_ = f"{_BASE_URL_}/v1/finance/screener"
_PREDEFINED_URL_ = f"{_SCREENER_URL_}/predefined/saved"

_REQUIRED_BODY_TYPE_ = TypedDict("_REQUIRED_BODY_TYPE_", {"offset": int, "size": int, "sortField": str, "sortType": str, "quoteType": str, "userId": str, "userIdType": str})
_BODY_TYPE_ = TypedDict("_BODY_TYPE_", {"offset": int, "size": int, "sortField": str, "sortType": str, "quoteType": str, "query": dict, "userId": str, "userIdType": str}, total=False)
_EMPTY_DICT_ = TypedDict("_EMPTY_DICT_", {}, total=False)

class PredefinedScreener:
    def __init__(self, key:'str', count:'int'=25, session=None, proxy=None, timeout=30):
        self.key = key
        self._data = YfData(session=session)
        self.proxy = proxy
        self.fields = ["ticker", "symbol", "longName", "sparkline", "shortName", "regularMarketPrice", "regularMarketChange", "regularMarketChangePercent", "regularMarketVolume", "averageDailyVolume3Month", "marketCap", "trailingPE", "regularMarketOpen"]
        self.timeout = timeout
        self._response = None
        self.count = count
        self.logger = logging.getLogger("yfinance")

    def _fetch(self):
        params_dict = {
            "count": self.count,
            "formatted": True,
            "scrIds": self.key,
            "sortField": "",
            "sortType": None,
            "start": 0,
            "useRecordsResponse": False,
            "fields": self.fields,
            "lang": "en-US", 
            "region": "US"
        }
        self._response = self._data.get(url=_PREDEFINED_URL_, params=params_dict, proxy=self.proxy)
        self._response.raise_for_status()
        self._response = self._response.json()["finance"]["result"][0]
        return self._response

    def set_fields(self, fields: 'list[str]'):
        """
        Set the fields to include in the screener.
        
        Args:
            fields: The fields to include.
        
        Returns:
            The Screener object.
        """
        if not isinstance(fields, list):
            raise TypeError("Fields must be a list of strings")
        elif len(fields) == 0:
            raise ValueError("Fields must be a list of strings")
        elif not isinstance(fields[0], str):
            raise TypeError("Fields must be a list of strings")
        
        self.fields = fields
        return self
    
    def set_config(self, proxy=None, session=None, timeout=None):
        """
        Set the proxy, session, and timeout for the screener.
        
        Args:
            proxy: The proxy to use.
            session: The session to use.
            timeout: The timeout to use.
        
        Returns:
            The Screener object.
        """
        if proxy is not None:
            self.proxy = proxy

        if session is not None:
            self.session = session
            self._data = YfData(session=self.session)
        
        if timeout is not None:
            self.timeout = timeout
            
        return self
    
    @property
    def quotes(self) -> 'list[dict]':
        """
        Get the quotes from the screener.
        
        Returns:
            The quotes from the screener.
        """
        return self.response.get("quotes", [])
    
    @property
    def response(self) -> 'dict':
        """
        Get the response from the screener.
        
        Returns:
            The response from the screener.
        """
        if self._response is None:
            self._fetch()
        return self._response
    



class Screener:
    """
    The `Screener` class allows you to screen stocks based on various criteria using either custom queries or predefined screens.

    The screener supports two main ways of filtering:
    1. Custom queries using the `EquityQuery` class to filter based on specific criteria
    2. Predefined screens for common stock screening strategies

    Args:
        query (Query): The query to use for screening. Must be an instance of `EquityQuery`.
        session (Optional[Session]): The requests Session object to use for making HTTP requests.
        proxy (Optional[str]): The proxy URL to use for requests.

    Example:
        Screen for technology stocks with price > $50 using custom query:

        .. code-block:: python

            import yfinance as yf
            
            # Create query for tech stocks over $50
            tech = yf.EquityQuery('eq', ['sector', 'Technology']) 
            price = yf.EquityQuery('gt', ['eodprice', 50])
            query = yf.EquityQuery('and', [tech, price])

            # Create and run screener
            screener = yf.Screener(query)
            results = screener.quotes

        Use a predefined screener:

        .. code-block:: python

            # Use predefined "day_gainers" screen
            screener = yf.Screener.predefined_body("day_gainers")
            gainers = screener.quotes
    """

    PREDEFINED_SCREENERS = PREDEFINED_SCREENERS
    ACCEPTED_BODY_KEYS = {"offset","size","sortField","sortType","quoteType","query","userId","userIdType"}


    def __init__(self, query:'Query'=None, sort_field:'str'="ticker", session=None, proxy=None):
        self._query = query
        self.session = session
        self.proxy = proxy
        self._data = YfData(session=self.session)
        self._logger = logging.getLogger("yfinance")
        self._response = None
        self.body = {"offset": 0, "size": 25, "sortField": sort_field, "sortType": "desc", "quoteType": "equity", "userId": "", "userIdType": "guid"}
  

    def set_query(self, data: 'dict', session=None, proxy=None) -> 'Screener':
        """
        Create the query from a dictionary representation.
        
        Args:
            data (dict): The dictionary representation of the query
            
        Returns:
            Screener: A new Screener instance with the specified parameters
            
        Raises:
            Exception: If the dictionary does not contain a valid query
        """
        self.query = Query.from_dict(data)
        return self
    
    @classmethod
    def from_dict(cls, data: 'dict', session=None, proxy=None) -> 'Screener':
        """
        Create the query from a dictionary representation.
        
        Args:
            data (dict): The dictionary representation of the query
            
        Returns:
            Screener: A new Screener instance with the specified parameters
            
        Raises:
            Exception: If the dictionary does not contain a valid query
        """
        self = cls(Query.from_dict(data), session=session, proxy=proxy)
        return self

    
    @property
    def query(self) -> 'Query':
        """
        Get the query from the screener.
        
        Returns:
            Query: The query from the screener.
        """
        return self._query
    
    @query.setter
    def query(self, value: 'Query'):
        """
        Set the query.
        
        Args:
            value (Query): The query to set.
            
        Returns:
            Screener: The screener instance for method chaining.
        """
        self._query = value
        return self
    
    def set_body(self, body: '_REQUIRED_BODY_TYPE_') -> 'Screener':
        """
        Set the fully custom body using dictionary input

        .. warning::
            Do not use this method to set query
            Query will be set automatically from `Screener.query`

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
                    "userId": "",
                    "userIdType": "guid"
                })
        """
        missing_keys = [key for key in Screener.ACCEPTED_BODY_KEYS if key not in body]
        if missing_keys:
            raise ValueError(f"Missing required keys in body: {missing_keys}")

        extra_keys = [key for key in body if key not in Screener.ACCEPTED_BODY_KEYS]
        if extra_keys:
            raise ValueError(f"Body contains extra keys: {extra_keys}")

        if "query" in body.keys():
            raise ValueError("Query must not be set here: set query")

        self._body_updated = True
        self._body = body
        return self

    
    def patch_body(self, offset: 'int' = 0, size: 'int' = 100, sortField: 'str' = "ticker", sortType: 'str' = "desc", quoteType: 'str' = "equity", userId: 'str' = "", userIdType: 'str' = "guid"):
        """
        Patch the body for the screener.
        
        Args:
            query: The query to use.
            offset: The offset to use.
            size: The size to use.
            sortField: The sortField to use.
            sortType: The sortType to use.
            quoteType: The quoteType to use.
            userId: The userId to use.
            userIdType: The userIdType to use.
            
        Returns:
            Screener: A new Screener instance with the specified parameters
            
        Raises:
            Exception: If the dictionary does not contain a valid query
        """
        return



    @staticmethod
    def predefined(query: 'str', count:'int'=25, session=None, proxy=None, timeout=30) -> 'PredefinedScreener':
        """
        Set the predefined body for the screener.
        
        Args:
            predefined_body: The predefined body to use.
        
        Returns:
            The Screener object.
        """
        if query not in Screener.PREDEFINED_SCREENERS:
            raise ValueError(f"Invalid predefined query: '{query}'. Valid queries are: {', '.join(Screener.PREDEFINED_SCREENERS)}")

        return PredefinedScreener(query, count, session=session, proxy=proxy, timeout=timeout)
    
    def _fetch(self):
        if self.body is None:
            raise ValueError("No body set for screener")
        
        if self.query is None:
            raise ValueError("No query set for screener")
        
        params_dict = {"corsDomain": "finance.yahoo.com", "formatted": "false", "lang": "en-US", "region": "US"}

        body = self.body.copy() # Copying the body so that query is not added to the original
        print("BODY", body)
        body["query"] = self.query.to_dict()

        response = self._data.post(_SCREENER_URL_, body=body, user_agent_headers=self._data.user_agent_headers, params=params_dict, proxy=self.proxy)
        print(body["query"])
        print(response.text)
        response.raise_for_status()

        self._response = response.json()

    @property
    def response(self) -> 'dict[str, Any]':
        """
        Get the results from the screener.
        
        Returns:
            The results from the screener.
        """
        if self._response == None:
            self._fetch()
        return self._response