from .query import EquityQuery as EqyQy
from .query import FundQuery as FndQy
from .query import QueryBase, EquityQuery, FundQuery, OP_DICT

from yfinance.const import _BASE_URL_
from yfinance.data import YfData

from ..utils import dynamic_docstring, generate_list_table_from_dict_universal

from typing import Union
import requests

_SCREENER_URL_ = f"{_BASE_URL_}/v1/finance/screener"
_PREDEFINED_URL_ = f"{_SCREENER_URL_}/predefined/saved"

PREDEFINED_SCREENER_BODY_DEFAULTS = {
    "offset":0, "size":25, "userId":"","userIdType":"guid"
}

PREDEFINED_SCREENER_QUERIES = {
    "aggressive_small_caps": {"sortField":"eodvolume", "sortType":"desc",
                            "query": EqyQy("AND", [EqyQy("IS-IN", ["exchange", "NMS", "NYQ"]), EqyQy("LT", ["epsgrowth.lasttwelvemonths", 15])])},
    "day_gainers": {"sortField":"percentchange", "sortType":"DESC",
                    "query": EqyQy("AND", [EqyQy("GT", ["percentchange", 3]), EqyQy("EQ", ["region", "us"]), EqyQy("GTE", ["intradaymarketcap", 2000000000]), EqyQy("GTE", ["intradayprice", 5]), EqyQy("GT", ["dayvolume", 15000])])},
    "day_losers": {"sortField":"percentchange", "sortType":"ASC",
                    "query": EqyQy("AND", [EqyQy("LT", ["percentchange", -2.5]), EqyQy("EQ", ["region", "us"]), EqyQy("GTE", ["intradaymarketcap", 2000000000]), EqyQy("GTE", ["intradayprice", 5]), EqyQy("GT", ["dayvolume", 20000])])},
    "growth_technology_stocks": {"sortField":"eodvolume", "sortType":"desc",
                                "query": EqyQy("AND", [EqyQy("GTE", ["quarterlyrevenuegrowth.quarterly", 25]), EqyQy("GTE", ["epsgrowth.lasttwelvemonths", 25]), EqyQy("EQ", ["sector", "Technology"]), EqyQy("IS-IN", ["exchange", "NMS", "NYQ"])])},
    "most_actives": {"sortField":"dayvolume", "sortType":"DESC",
                    "query": EqyQy("AND", [EqyQy("EQ", ["region", "us"]), EqyQy("GTE", ["intradaymarketcap", 2000000000]), EqyQy("GT", ["dayvolume", 5000000])])},
    "most_shorted_stocks": {"size":25, "offset":0, "sortField":"short_percentage_of_shares_outstanding.value", "sortType":"DESC", 
                            "query": EqyQy("AND", [EqyQy("EQ", ["region", "us"]), EqyQy("GT", ["intradayprice", 1]), EqyQy("GT", ["avgdailyvol3m", 200000])])},
    "small_cap_gainers": {"sortField":"eodvolume", "sortType":"desc", 
                        "query": EqyQy("AND", [EqyQy("LT", ["intradaymarketcap",2000000000]), EqyQy("IS-IN", ["exchange", "NMS", "NYQ"])])},
    "undervalued_growth_stocks": {"sortType":"DESC", "sortField":"eodvolume", 
                                "query": EqyQy("AND", [EqyQy("btwn", ["peratio.lasttwelvemonths", 0, 20]), EqyQy("LT", ["pegratio_5y", 1]), EqyQy("GTE", ["epsgrowth.lasttwelvemonths", 25]), EqyQy("IS-IN", ["exchange", "NMS", "NYQ"])])},
    "undervalued_large_caps": {"sortField":"eodvolume", "sortType":"desc", 
                            "query": EqyQy("AND", [EqyQy("btwn", ["peratio.lasttwelvemonths", 0, 20]), EqyQy("LT", ["pegratio_5y", 1]), EqyQy("btwn", ["intradaymarketcap", 10000000000, 100000000000]), EqyQy("IS-IN", ["exchange", "NMS", "NYQ"])])},
    "conservative_foreign_funds": {"sortType":"DESC", "sortField":"fundnetassets",
                                "query": FndQy("AND", [FndQy("IS-IN", ["categoryname", "Foreign Large Value", "Foreign Large Blend", "Foreign Large Growth", "Foreign Small/Mid Growth", "Foreign Small/Mid Blend", "Foreign Small/Mid Value"]), FndQy("IS-IN", ["performanceratingoverall", 4, 5]), FndQy("LT", ["initialinvestment", 100001]), FndQy("LT", ["annualreturnnavy1categoryrank", 50]), FndQy("IS-IN", ["riskratingoverall", 1, 2, 3]), FndQy("EQ", ["exchange", "NAS"])])},
    "high_yield_bond": {"sortType":"DESC", "sortField":"fundnetassets",
                        "query": FndQy("AND", [FndQy("IS-IN", ["performanceratingoverall", 4, 5]), FndQy("LT", ["initialinvestment", 100001]), FndQy("LT", ["annualreturnnavy1categoryrank", 50]), FndQy("IS-IN", ["riskratingoverall", 1, 2, 3]), FndQy("EQ", ["categoryname", "High Yield Bond"]), FndQy("EQ", ["exchange", "NAS"])])},
    "portfolio_anchors": {"sortType":"DESC", "sortField":"fundnetassets",
                        "query": FndQy("AND", [FndQy("EQ", ["categoryname", "Large Blend"]), FndQy("IS-IN", ["performanceratingoverall", 4, 5]), FndQy("LT", ["initialinvestment", 100001]), FndQy("LT", ["annualreturnnavy1categoryrank", 50]), FndQy("EQ", ["exchange", "NAS"])])},
    "solid_large_growth_funds": {"sortType":"DESC", "sortField":"fundnetassets",
                                "query": FndQy("AND", [FndQy("EQ", ["categoryname", "Large Growth"]), FndQy("IS-IN", ["performanceratingoverall", 4, 5]), FndQy("LT", ["initialinvestment", 100001]), FndQy("LT", ["annualreturnnavy1categoryrank", 50]), FndQy("EQ", ["exchange", "NAS"])])},
    "solid_midcap_growth_funds": {"sortType":"DESC", "sortField":"fundnetassets",
                                "query": FndQy("AND", [FndQy("EQ", ["categoryname", "Mid-Cap Growth"]), FndQy("IS-IN", ["performanceratingoverall", 4, 5]), FndQy("LT", ["initialinvestment", 100001]), FndQy("LT", ["annualreturnnavy1categoryrank", 50]), FndQy("EQ", ["exchange", "NAS"])])},
    "top_mutual_funds": {"sortType":"DESC", "sortField":"percentchange",
                        "query": FndQy("AND", [FndQy("GT", ["intradayprice", 15]), FndQy("IS-IN", ["performanceratingoverall", 4, 5]), FndQy("GT", ["initialinvestment", 1000]), FndQy("EQ", ["exchange", "NAS"])])}
}

@dynamic_docstring({"predefined_screeners": generate_list_table_from_dict_universal(PREDEFINED_SCREENER_QUERIES, bullets=True, title="Predefined queries (Dec-2024)")})
def screen(query: Union[str, EquityQuery, FundQuery, OP_DICT],
            offset: int = None,
            size: int = None,
            sortField: str = None,
            sortAsc: bool = None,
            userId: str = None,
            userIdType: str = None,
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
            q = EquityQuery("AND", [
                   EquityQuery("GT", ["percentchange", 3]), 
                   EquityQuery("EQ", ["region", "us"])
            ])
            response = yf.screen(q, sortField = "percentchange", sortAsc = True)

    To access predefineds query code
        .. code-block:: python

            import yfinance as yf
            query = yf.PREDEFINED_SCREENER_QUERIES["aggressive_small_caps"]

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

    elif isinstance(query, (QueryBase, dict)):
        # Prepare other fields
        for k in defaults:
            if k not in fields or fields[k] is None:
                fields[k] = defaults[k]
        fields['sortType'] = 'ASC' if fields['sortAsc'] else 'DESC'
        del fields['sortAsc']

        post_query = fields
        post_query['query'] = query

    else:
        raise ValueError(f'Query must be type str or QueryBase, not "{type(query)}"')

    if query is None:
        raise ValueError('No query provided')

    if isinstance(post_query['query'], EqyQy):
        post_query['quoteType'] = 'EQUITY'
    elif isinstance(post_query['query'], FndQy):
        post_query['quoteType'] = 'MUTUALFUND'

    if isinstance(post_query['query'], QueryBase):
        post_query['query'] = post_query['query'].to_dict()

    # Fetch
    _data = YfData(session=session)
    response = _data.post(_SCREENER_URL_, 
                            body=post_query, 
                            user_agent_headers=_data.user_agent_headers, 
                            params=params_dict, 
                            proxy=proxy)
    response.raise_for_status()
    return response.json()['finance']['result'][0]
