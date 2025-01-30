from abc import ABC, abstractmethod
import numbers
from typing import Union, TypeVar, Generic, Literal, TypedDict

from .const import EQUITY_SCREENER_EQ_MAP, EQUITY_SCREENER_FIELDS, FUND_SCREENER_EQ_MAP, FUND_SCREENER_FIELDS, _PREDEFINED_URL_, _SCREENER_URL_
from yfinance.data import YfData
from yfinance.exceptions import YFNotImplementedError
from ..utils import dynamic_docstring, generate_list_table_from_dict_universal
import requests
from typing import Union, TypeVar, Generic, Literal, TypedDict

class RESULT(TypedDict, Generic['OPERATOR', 'OPERAND']):
    operator: 'OPERATOR'
    operands: 'OPERAND'

OPERATORS = Literal["EQ", "OR", "AND", "BTWN", "GT", "LT", "GTE", "LTE"]
OPERANDS = Union[list['QueryBase'],list[str],list[Union[str, float, int]],list[RESULT['OPERATOR', 'OPERANDS']]]

OPERATOR = TypeVar("OPERATOR", bound=OPERATORS)
OPERAND = TypeVar("OPERAND", bound=OPERANDS)


ISIN = 'QueryBase["OR", list[QueryBase["EQ", OPERANDS]]]'


class Query:
    """
    - `Query.screen` is equivalent to `yfinance.screen(query=query)` but it uses the settings of the `Query` object.
    - `Query.to_dict` returns a dictionary representation of the `Query` object.
    - `Query.head` returns the `QueryHead` object.
    - `Query.exchange` returns the exchange of the `Query` object from the `QueryHead` object.
    - `Query.region` returns the region of the `Query` object from the `QueryHead` object.
    - `Query.sector` returns the sector of the `Query` object from the `QueryHead` object.
    """
    def __init__(
        self,
        offset: 'int' = None, 
        size: 'int' = None,
        sortField: 'str' = None, 
        sortAsc: 'bool' = None,
        userId: 'str' = None, 
        userIdType: 'str' = None,
    ):
        """
        Args:
            offset (int, optional): The offset of the first record to return. Defaults to None.
            size (int, optional): The number of records to return. Defaults to None.
            sortField (str, optional): The field to sort by. Defaults to None.
            sortAsc (bool, optional): Whether to sort ascending or descending. Defaults to None.
            userId (str, optional): The user ID to filter by. Defaults to None.
            userIdType (str, optional): The type of user ID. Defaults to None.

        Defaults:
            offset = 0
            size = 25
            sortField = "ticker"
            sortAsc = False
            userId = ""
            userIdType = "guid"
        """
        self._query = None
        self._head = None
        self.offset = offset or 0
        self.size = size or 25
        self.sortField = sortField or "ticker"
        self.sortAsc = sortAsc or False
        self.userId = userId or ""
        self.userIdType = userIdType or "guid"

    @property
    def query(self) -> 'Union[QueryBase, None]':
        return self._query
    
    @query.setter
    def query(self, value: 'Union[QueryBase, None]'):
        self._query = value

    @property
    def head(self) -> 'Union[QueryHead, None]':
        return self._head
    
    @head.setter
    def head(self, value: 'Union[QueryHead, None]'):
        self._head = value

    @property
    def exchange(self) -> 'Union[str, QueryBase, None]':
        if self.head is not None:
            return self.head.exchange
        return None
    
    @exchange.setter
    def exchange(self, value: 'Union[str, QueryBase, None]'):
        if self.head is None:
            self.head = QueryHead()
        self.head.exchange = value

    @property
    def region(self) -> 'Union[str, QueryBase, None]':
        if self.head is not None:
            return self.head.region
        return None
    
    @region.setter
    def region(self, value: 'Union[str, QueryBase, None]'):
        if self.head is None:
            self.head = QueryHead()
        self.head.region = value

    @property
    def sector(self) -> 'Union[str, QueryBase, None]':
        if self.head is not None:
            return self.head.sector
        return None
    
    @sector.setter
    def sector(self, value: 'Union[str, QueryBase, None]'):
        if self.head is None:
            self.head = QueryHead()
        self.head.sector = value

    @property
    def fields(self) -> 'dict':
        return {
            "offset": self.offset,
            "size": self.size,
            "sortField": self.sortField,
            "sortType": "ASC" if self.sortAsc else "DESC",
            "userId": self.userId,
            "userIdType": self.userIdType,
            "quoteType": "EQUITY" if isinstance(self.query, EquityQuery) else "MUTUALFUND"
        }

    def to_dict(self):
        operands = []
        if self.query is not None:
            operands.append(self.query.to_dict())
        if self.head is not None:
            operands.append(self.head.to_dict())
        return {
            "operator": "AND",
            "operands": operands
        }

    def screen(self, session=None, proxy=None):
        return screen(
            self,
            offset=self.offset,
            size=self.size,
            sortField=self.sortField,
            sortAsc=self.sortAsc,
            userId=self.userId,
            userIdType=self.userIdType,
            session=session,
            proxy=proxy
        )

class QueryHead:
    region = None
    sector = None
    exchange = None

    def __init__(self, exchange: 'Union[str, QueryBase, None]' = None, region: 'Union[str, QueryBase, None]' = None, sector: 'Union[str, QueryBase, None]' = None):
        self.exchange = exchange
        self.region = region
        self.sector = sector

    def to_dict(self) -> 'RESULT[Literal["AND"], OPERANDS]':
        ret = {
            "operator": "AND",
            "operands": []
        }
        if self.exchange is not None:
            if isinstance(self.exchange, QueryBase):
                ret["operands"].append({
                    "operator": "EQ",
                    "operands": ["exchange", self.exchange.to_dict()]
                })
            else:
                ret["operands"].append({
                    "operator": "EQ",
                    "operands": ["exchange", self.exchange]
                })
        if self.region is not None:
            if isinstance(self.region, QueryBase):
                ret["operands"].append({
                    "operator": "EQ",
                    "operands": ["region", self.region.to_dict()]
                })
            else:
                ret["operands"].append({
                    "operator": "EQ",
                    "operands": ["region", self.region]
                })
        if self.sector is not None:
            if isinstance(self.sector, QueryBase):
                ret["operands"].append({
                    "operator": "EQ",
                    "operands": ["sector", self.sector.to_dict()]
                })
            else:
                ret["operands"].append({
                    "operator": "EQ",
                    "operands": ["sector", self.sector]
                })
        return ret





class QueryBase(ABC, Generic[OPERATOR, OPERAND]):
    def __init__(self, operator: 'OPERATOR', operand: 'OPERAND'):
        operator = operator.upper()

        if not isinstance(operand, list):
            raise TypeError('Invalid operand type')
        if len(operand) <= 0:
            raise ValueError('Invalid field for EquityQuery')
            
        if operator in {'OR','AND'}: 
            self._validate_or_and_operand(operand)
        elif operator == 'EQ': 
            self._validate_eq_operand(operand)
        elif operator == 'BTWN': 
            self._validate_btwn_operand(operand)
        elif operator in {'GT','LT','GTE','LTE'}: 
            self._validate_gt_lt(operand)
        else: 
            raise ValueError('Invalid Operator Value')

        self.operator = operator
        self.operands = operand

    @classmethod
    def is_in(cls, value:'str', is_in:'list[str]') -> 'QueryBase[Literal["OR"], list[QueryBase[Literal["EQ"], OPERAND]]]':
        # No validation needed as `OR` and `EQ` validation happens anyway
        return type(cls)(
            operator="OR",
            operand=[
                type(cls)(operator="EQ", operand=[value, v]) for v in is_in
            ]
        )

    @property
    @abstractmethod
    def valid_fields(self) -> 'list':
        raise YFNotImplementedError('valid_fields() needs to be implemented by child')

    @property
    @abstractmethod
    def valid_values(self) -> 'dict':
        raise YFNotImplementedError('valid_values() needs to be implemented by child')

    def _validate_or_and_operand(self, operand: 'list[QueryBase]') -> 'None':
        if len(operand) <= 1: 
            raise ValueError('Operand must be length longer than 1')
        if all(isinstance(e, QueryBase) for e in operand) is False: 
            raise TypeError(f'Operand must be type {type(self)} for OR/AND')

    def _validate_eq_operand(self, operand: 'list[Union[str, numbers.Real]]') -> 'None':
        if len(operand) != 2:
            raise ValueError('Operand must be length 2 for EQ')
        
        if  not any(operand[0] in fields_by_type for fields_by_type in self.valid_fields.values()):
            raise ValueError(f'Invalid field for {type(self)} "{operand[0]}"')
        if operand[0] in self.valid_values:
            vv = self.valid_values[operand[0]]
            if isinstance(vv, dict):
                # this data structure is slightly different to generate better docs, 
                # need to unpack here.
                vv = set().union(*[e for e in vv.values()])
            if operand[1] not in vv:
                raise ValueError(f'Invalid EQ value "{operand[1]}"')
    
    def _validate_btwn_operand(self, operand: 'list[Union[str, numbers.Real]]') -> 'None':
        if len(operand) != 3: 
            raise ValueError('Operand must be length 3 for BTWN')
        if  not any(operand[0] in fields_by_type for fields_by_type in self.valid_fields.values()):
            raise ValueError(f'Invalid field for {type(self)}')
        if isinstance(operand[1], numbers.Real) is False:
            raise TypeError('Invalid comparison type for BTWN')
        if isinstance(operand[2], numbers.Real) is False:
            raise TypeError('Invalid comparison type for BTWN')

    def _validate_gt_lt(self, operand: 'list[Union[str, numbers.Real]]') -> 'None':
        if len(operand) != 2:
            raise ValueError('Operand must be length 2 for GT/LT')
        if  not any(operand[0] in fields_by_type for fields_by_type in self.valid_fields.values()):
            raise ValueError(f'Invalid field for {type(self)} "{operand[0]}"')
        if isinstance(operand[1], numbers.Real) is False:
            raise TypeError('Invalid comparison type for GT/LT')

    def to_dict(self) -> 'RESULT[OPERATOR, OPERAND]':
        op = self.operator
        ops = self.operands
        return {
            "operator": op,
            "operands": [o.to_dict() if isinstance(o, (QueryBase, QueryHead)) else o for o in ops]
        }

    def __repr__(self, indent=0) -> 'str':
        indent_str = "  " * indent
        class_name = self.__class__.__name__

        if isinstance(self.operands, list):
            # For list operands, check if they contain any QueryBase objects
            if any(isinstance(op, QueryBase) for op in self.operands):
                # If there are nested queries, format them with newlines
                operands_str = ",\n".join(
                    f"{indent_str}  {op.__repr__(indent + 1) if isinstance(op, QueryBase) else repr(op)}"
                    for op in self.operands
                )
                return f"{class_name}({self.operator}, [\n{operands_str}\n{indent_str}])"
            else:
                # For lists of simple types, keep them on one line
                return f"{class_name}({self.operator}, {repr(self.operands)})"
        else:
            # Handle single operand
            return f"{class_name}({self.operator}, {repr(self.operands)})"

    def __str__(self) -> 'str':
        return self.__repr__()
    
    def screen(self, offset:'int'=None, size:'int'=None, sortField:'str'=None, sortAsc:'bool'=None, userId:'str'=None, userIdType:'str'=None, session=None, proxy=None):
        return screen(
            self,
            offset=offset,
            size=size,
            sortField=sortField,
            sortAsc=sortAsc,
            userId=userId,
            userIdType=userIdType,
            session=session,
            proxy=proxy
        )


class EquityQuery(QueryBase):
    """
    The `EquityQuery` class constructs filters for stocks based on specific criteria such as region, sector, exchange, and peer group.

    Start with value operations: `EQ` (equals), `BTWN` (between), `GT` (greater than), `LT` (less than), `GTE` (greater or equal), `LTE` (less or equal).

    Combine them with logical operations: `AND`, `OR`.

    Example:
        Predefined Yahoo query `aggressive_small_caps`:
        
        .. code-block:: python

            from yfinance import EquityQuery

            EquityQuery("and", [
                EquityQuery.is_in("exchange", ["NMS", "NYQ"]), 
                EquityQuery("lt", ["epsgrowth.lasttwelvemonths", 15])
            ])
    """

    @dynamic_docstring({"valid_operand_fields_table": generate_list_table_from_dict_universal(EQUITY_SCREENER_FIELDS)})
    @property
    def valid_fields(self) -> 'dict':
        """
        Valid operands, grouped by category.
        {valid_operand_fields_table}
        """
        return EQUITY_SCREENER_FIELDS
    
    @dynamic_docstring({"valid_values_table": generate_list_table_from_dict_universal(EQUITY_SCREENER_EQ_MAP, concat_keys=['exchange'])})
    @property
    def valid_values(self) -> 'dict':
        """
        Most operands take number values, but some have a restricted set of valid values.
        {valid_values_table}
        """
        return EQUITY_SCREENER_EQ_MAP


class FundQuery(QueryBase):
    """
    The `FundQuery` class constructs filters for mutual funds based on specific criteria such as region, sector, exchange, and peer group.

    Start with value operations: `EQ` (equals), `BTWN` (between), `GT` (greater than), `LT` (less than), `GTE` (greater or equal), `LTE` (less or equal).

    Combine them with logical operations: `AND`, `OR`.

    Example:
        Predefined Yahoo query `solid_large_growth_funds`:
        
        .. code-block:: python

            from yfinance import FundQuery
            
            FundQuery("and", [
                FundQuery("eq", ["categoryname", "Large Growth"]), 
                FundQuery.is_in("performanceratingoverall", [4, 5]), 
                FundQuery("lt", ["initialinvestment", 100001]), 
                FundQuery("lt", ["annualreturnnavy1categoryrank", 50]), 
                FundQuery("eq", ["exchange", "NAS"])
            ])
    """
    @dynamic_docstring({"valid_operand_fields_table": generate_list_table_from_dict_universal(FUND_SCREENER_FIELDS)})
    @property
    def valid_fields(self) -> 'dict':
        """
        Valid operands, grouped by category.
        {valid_operand_fields_table}
        """
        return FUND_SCREENER_FIELDS
    
    @dynamic_docstring({"valid_values_table": generate_list_table_from_dict_universal(FUND_SCREENER_EQ_MAP)})
    @property
    def valid_values(self) -> 'dict':
        """
        Most operands take number values, but some have a restricted set of valid values.
        {valid_values_table}
        """
        return FUND_SCREENER_EQ_MAP


@dynamic_docstring({"predefined_screeners": generate_list_table_from_dict_universal(PREDEFINED_SCREENER_QUERIES, bullets=True, title='Predefined queries (Dec-2024)')})
def screen(query: 'Union[str, EquityQuery, FundQuery, Query]',
            offset: 'int' = None, 
            size: 'int' = None, 
            sortField: 'str' = None, 
            sortAsc: 'bool' = None,
            userId: 'str' = None, 
            userIdType: 'str' = None, 
            session = None, proxy = None):
    """
    Run a screen: predefined query, or custom query.

    :Parameters:
        * Defaults only apply if query = EquityQuery or FundQuery
        query : str | Query:
            The query to execute, either name of predefined or custom query.
            For predefined list run yf.PREDEFINED_SCREENER_QUERIES.keys()
        offset : int
            The offset for the results. Default 0.
        size : int
            number of results to return. Default 100, maximum 250 (Yahoo)
        sortField : str
            field to sort by. Default "ticker"
        sortAsc : bool
            Sort ascending? Default False
        userId : str
            The user ID. Default empty.
        userIdType : str
            Type of user ID (e.g., "guid"). Default "guid".

    Example: predefined query
        .. code-block:: python

            import yfinance as yf
            response = yf.screen("aggressive_small_caps")

    Example: custom query
        .. code-block:: python

            import yfinance as yf
            from yfinance import EquityQuery
            q = EquityQuery('and', [
                   EquityQuery('gt', ['percentchange', 3]), 
                   EquityQuery('eq', ['region', 'us'])
            ])
            response = yf.screen(q, sortField = 'percentchange', sortAsc = True)

    To access predefineds query code
        .. code-block:: python

            import yfinance as yf
            query = yf.PREDEFINED_SCREENER_QUERIES['aggressive_small_caps']

    {predefined_screeners}
    """

    # Only use defaults when user NOT give a predefined, because
    # Yahoo's predefined endpoint auto-applies defaults. Also,
    # that endpoint might be ignoring these fields.
    defaults = {
        'offset': 0,
        'size': 25,
        'sortField': 'ticker',
        'sortAsc': False,
        'userId': "",
        'userIdType': "guid"
    }

    if size is not None and size > 250:
        raise ValueError("Yahoo limits query size to 250, reduce size.")

    fields = dict(locals())
    for k in ['query', 'session', 'proxy']:
        if k in fields:
            del fields[k]

    params_dict = {"corsDomain": "finance.yahoo.com", "formatted": "false", "lang": "en-US", "region": "US"}

    post_query = None
    if isinstance(query, str):
        # post_query = PREDEFINED_SCREENER_QUERIES[query]
        # Switch to Yahoo's predefined endpoint
        _data = YfData(session=session)
        params_dict['scrIds'] = query
        for k,v in fields.items():
            if v is not None:
                params_dict[k] = v
        resp = _data.get(url=_PREDEFINED_URL_, params=params_dict, proxy=proxy)
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            if query not in PREDEFINED_SCREENER_QUERIES:
                print(f"yfinance.screen: '{query}' is probably not a predefined query.")
            raise
        return resp.json()["finance"]["result"][0]

    elif isinstance(query, QueryBase):
        # Prepare other fields
        for k in defaults:
            if k not in fields or fields[k] is None:
                fields[k] = defaults[k]
        fields['sortType'] = 'ASC' if fields['sortAsc'] else 'DESC'
        del fields['sortAsc']

        post_query = fields
        post_query['query'] = query.to_dict()

        if isinstance(post_query['query'], EqyQy):
            post_query['quoteType'] = 'EQUITY'
        elif isinstance(post_query['query'], FndQy):
            post_query['quoteType'] = 'MUTUALFUND'

    elif isinstance(query, Query):
        post_query = query.fields.copy() # Copy to avoid modifying original
        post_query["query"] = query.to_dict()

    else:
        raise ValueError(f'Query must be type str or QueryBase, not "{type(query)}"')

    if query is None:
        raise ValueError('No query provided')

    # Fetch
    _data = YfData(session=session)
    response = _data.post(_SCREENER_URL_, 
                            body=post_query, 
                            user_agent_headers=_data.user_agent_headers, 
                            params=params_dict, 
                            proxy=proxy)
    response.raise_for_status()
    return response.json()['finance']['result'][0]

PREDEFINED_SCREENER_QUERIES = {
    "aggressive_small_caps": {"sortField":"eodvolume", "sortType":"desc",
                            "query": EqyQy("and", [EqyQy.is_in("exchange", ["NMS", "NYQ"]), EqyQy("lt", ["epsgrowth.lasttwelvemonths", 15])])},
    "day_gainers": {"sortField":"percentchange", "sortType":"DESC",
                    "query": EqyQy("and", [EqyQy("gt", ["percentchange", 3]), EqyQy("eq", ["region", "us"]), EqyQy("gte", ["intradaymarketcap", 2000000000]), EqyQy("gte", ["intradayprice", 5]), EqyQy("gt", ["dayvolume", 15000])])},
    "day_losers": {"sortField":"percentchange", "sortType":"ASC",
                    "query": EqyQy("and", [EqyQy("lt", ["percentchange", -2.5]), EqyQy("eq", ["region", "us"]), EqyQy("gte", ["intradaymarketcap", 2000000000]), EqyQy("gte", ["intradayprice", 5]), EqyQy("gt", ["dayvolume", 20000])])},
    "growth_technology_stocks": {"sortField":"eodvolume", "sortType":"desc",
                                "query": EqyQy("and", [EqyQy("gte", ["quarterlyrevenuegrowth.quarterly", 25]), EqyQy("gte", ["epsgrowth.lasttwelvemonths", 25]), EqyQy("eq", ["sector", "Technology"]), EqyQy.is_in("exchange", ["NMS", "NYQ"])])},
    "most_actives": {"sortField":"dayvolume", "sortType":"DESC",
                    "query": EqyQy("and", [EqyQy("eq", ["region", "us"]), EqyQy("gte", ["intradaymarketcap", 2000000000]), EqyQy("gt", ["dayvolume", 5000000])])},
    "most_shorted_stocks": {"size":25, "offset":0, "sortField":"short_percentage_of_shares_outstanding.value", "sortType":"DESC", 
                            "query": EqyQy("and", [EqyQy("eq", ["region", "us"]), EqyQy("gt", ["intradayprice", 1]), EqyQy("gt", ["avgdailyvol3m", 200000])])},
    "small_cap_gainers": {"sortField":"eodvolume", "sortType":"desc", 
                        "query": EqyQy("and", [EqyQy("lt", ["intradaymarketcap",2000000000]), EqyQy.is_in("exchange", ["NMS", "NYQ"])])},
    "undervalued_growth_stocks": {"sortType":"DESC", "sortField":"eodvolume", 
                                "query": EqyQy("and", [EqyQy("btwn", ["peratio.lasttwelvemonths", 0, 20]), EqyQy("lt", ["pegratio_5y", 1]), EqyQy("gte", ["epsgrowth.lasttwelvemonths", 25]), EqyQy.is_in("exchange", ["NMS", "NYQ"])])},
    "undervalued_large_caps": {"sortField":"eodvolume", "sortType":"desc", 
                            "query": EqyQy("and", [EqyQy("btwn", ["peratio.lasttwelvemonths", 0, 20]), EqyQy("lt", ["pegratio_5y", 1]), EqyQy("btwn", ["intradaymarketcap", 10000000000, 100000000000]), EqyQy.is_in("exchange", ["NMS", "NYQ"])])},
    "conservative_foreign_funds": {"sortType":"DESC", "sortField":"fundnetassets",
                                "query": FndQy("and", [FndQy.is_in("categoryname", ["Foreign Large Value", "Foreign Large Blend", "Foreign Large Growth", "Foreign Small/Mid Growth", "Foreign Small/Mid Blend", "Foreign Small/Mid Value"]), FndQy.is_in("performanceratingoverall", [4, 5]), FndQy("lt", ["initialinvestment", 100001]), FndQy("lt", ["annualreturnnavy1categoryrank", 50]), FndQy.is_in("riskratingoverall", [1, 2, 3]), FndQy("eq", ["exchange", "NAS"])])},
    "high_yield_bond": {"sortType":"DESC", "sortField":"fundnetassets",
                        "query": FndQy("and", [FndQy.is_in("performanceratingoverall", [4, 5]), FndQy("lt", ["initialinvestment", 100001]), FndQy("lt", ["annualreturnnavy1categoryrank", 50]), FndQy.is_in("riskratingoverall", [1, 2, 3]), FndQy("eq", ["categoryname", "High Yield Bond"]), FndQy("eq", ["exchange", "NAS"])])},
    "portfolio_anchors": {"sortType":"DESC", "sortField":"fundnetassets",
                        "query": FndQy("and", [FndQy("eq", ["categoryname", "Large Blend"]), FndQy.is_in("performanceratingoverall", [4, 5]), FndQy("lt", ["initialinvestment", 100001]), FndQy("lt", ["annualreturnnavy1categoryrank", 50]), FndQy("eq", ["exchange", "NAS"])])},
    "solid_large_growth_funds": {"sortType":"DESC", "sortField":"fundnetassets",
                                "query": FndQy("and", [FndQy("eq", ["categoryname", "Large Growth"]), FndQy.is_in("performanceratingoverall", [4, 5]), FndQy("lt", ["initialinvestment", 100001]), FndQy("lt", ["annualreturnnavy1categoryrank", 50]), FndQy("eq", ["exchange", "NAS"])])},
    "solid_midcap_growth_funds": {"sortType":"DESC", "sortField":"fundnetassets",
                                "query": FndQy("and", [FndQy("eq", ["categoryname", "Mid-Cap Growth"]), FndQy.is_in("performanceratingoverall", [4, 5]), FndQy("lt", ["initialinvestment", 100001]), FndQy("lt", ["annualreturnnavy1categoryrank", 50]), FndQy("eq", ["exchange", "NAS"])])},
    "top_mutual_funds": {"sortType":"DESC", "sortField":"percentchange",
                        "query": FndQy("and", [FndQy("gt", ["intradayprice", 15]), FndQy.is_in("performanceratingoverall", [4, 5]), FndQy("gt", ["initialinvestment", 1000]), FndQy("eq", ["exchange", "NAS"])])}
}
