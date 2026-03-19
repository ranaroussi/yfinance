"""Yahoo screener helpers for predefined and custom query payloads."""

from dataclasses import dataclass
from json import dumps
from typing import Any, Dict, Optional, Tuple, Union
import warnings

import curl_cffi

from yfinance.const import _QUERY1_URL_
from yfinance.data import YfData

from ..utils import dynamic_docstring, generate_list_table_from_dict_universal
from .query import EquityQuery, FundQuery, QueryBase

_SCREENER_URL_ = f"{_QUERY1_URL_}/v1/finance/screener"
_PREDEFINED_URL_ = f"{_SCREENER_URL_}/predefined/saved"

PREDEFINED_SCREENER_BODY_DEFAULTS = {
    "offset": 0,
    "count": 25,
    "userId": "",
    "userIdType": "guid",
}


def _eq(operator: str, operands: list[Any]) -> EquityQuery:
    return EquityQuery(operator, operands)


def _fund(operator: str, operands: list[Any]) -> FundQuery:
    return FundQuery(operator, operands)


def _predefined_query(
    *,
    sort_field: str,
    sort_type: str,
    query: QueryBase,
    count: Optional[int] = None,
    offset: Optional[int] = None,
) -> Dict[str, Any]:
    item: Dict[str, Any] = {
        "sortField": sort_field,
        "sortType": sort_type,
        "query": query,
    }
    if count is not None:
        item["count"] = count
    if offset is not None:
        item["offset"] = offset
    return item


PREDEFINED_SCREENER_QUERIES = {
    "aggressive_small_caps": _predefined_query(
        sort_field="eodvolume",
        sort_type="desc",
        query=_eq(
            "and",
            [
                _eq("is-in", ["exchange", "NMS", "NYQ"]),
                _eq("lt", ["epsgrowth.lasttwelvemonths", 15]),
            ],
        ),
    ),
    "day_gainers": _predefined_query(
        sort_field="percentchange",
        sort_type="DESC",
        query=_eq(
            "and",
            [
                _eq("gt", ["percentchange", 3]),
                _eq("eq", ["region", "us"]),
                _eq("gte", ["intradaymarketcap", 2_000_000_000]),
                _eq("gte", ["intradayprice", 5]),
                _eq("gt", ["dayvolume", 15_000]),
            ],
        ),
    ),
    "day_losers": _predefined_query(
        sort_field="percentchange",
        sort_type="ASC",
        query=_eq(
            "and",
            [
                _eq("lt", ["percentchange", -2.5]),
                _eq("eq", ["region", "us"]),
                _eq("gte", ["intradaymarketcap", 2_000_000_000]),
                _eq("gte", ["intradayprice", 5]),
                _eq("gt", ["dayvolume", 20_000]),
            ],
        ),
    ),
    "growth_technology_stocks": _predefined_query(
        sort_field="eodvolume",
        sort_type="desc",
        query=_eq(
            "and",
            [
                _eq("gte", ["quarterlyrevenuegrowth.quarterly", 25]),
                _eq("gte", ["epsgrowth.lasttwelvemonths", 25]),
                _eq("eq", ["sector", "Technology"]),
                _eq("is-in", ["exchange", "NMS", "NYQ"]),
            ],
        ),
    ),
    "most_actives": _predefined_query(
        sort_field="dayvolume",
        sort_type="DESC",
        query=_eq(
            "and",
            [
                _eq("eq", ["region", "us"]),
                _eq("gte", ["intradaymarketcap", 2_000_000_000]),
                _eq("gt", ["dayvolume", 5_000_000]),
            ],
        ),
    ),
    "most_shorted_stocks": _predefined_query(
        sort_field="short_percentage_of_shares_outstanding.value",
        sort_type="DESC",
        query=_eq(
            "and",
            [
                _eq("eq", ["region", "us"]),
                _eq("gt", ["intradayprice", 1]),
                _eq("gt", ["avgdailyvol3m", 200_000]),
            ],
        ),
        count=25,
        offset=0,
    ),
    "small_cap_gainers": _predefined_query(
        sort_field="eodvolume",
        sort_type="desc",
        query=_eq(
            "and",
            [
                _eq("lt", ["intradaymarketcap", 2_000_000_000]),
                _eq("is-in", ["exchange", "NMS", "NYQ"]),
            ],
        ),
    ),
    "undervalued_growth_stocks": _predefined_query(
        sort_field="eodvolume",
        sort_type="DESC",
        query=_eq(
            "and",
            [
                _eq("btwn", ["peratio.lasttwelvemonths", 0, 20]),
                _eq("lt", ["pegratio_5y", 1]),
                _eq("gte", ["epsgrowth.lasttwelvemonths", 25]),
                _eq("is-in", ["exchange", "NMS", "NYQ"]),
            ],
        ),
    ),
    "undervalued_large_caps": _predefined_query(
        sort_field="eodvolume",
        sort_type="desc",
        query=_eq(
            "and",
            [
                _eq("btwn", ["peratio.lasttwelvemonths", 0, 20]),
                _eq("lt", ["pegratio_5y", 1]),
                _eq("btwn", ["intradaymarketcap", 10_000_000_000, 100_000_000_000]),
                _eq("is-in", ["exchange", "NMS", "NYQ"]),
            ],
        ),
    ),
    "conservative_foreign_funds": _predefined_query(
        sort_field="fundnetassets",
        sort_type="DESC",
        query=_fund(
            "and",
            [
                _fund(
                    "is-in",
                    [
                        "categoryname",
                        "Foreign Large Value",
                        "Foreign Large Blend",
                        "Foreign Large Growth",
                        "Foreign Small/Mid Growth",
                        "Foreign Small/Mid Blend",
                        "Foreign Small/Mid Value",
                    ],
                ),
                _fund("is-in", ["performanceratingoverall", 4, 5]),
                _fund("lt", ["initialinvestment", 100_001]),
                _fund("lt", ["annualreturnnavy1categoryrank", 50]),
                _fund("is-in", ["riskratingoverall", 1, 2, 3]),
                _fund("eq", ["exchange", "NAS"]),
            ],
        ),
    ),
    "high_yield_bond": _predefined_query(
        sort_field="fundnetassets",
        sort_type="DESC",
        query=_fund(
            "and",
            [
                _fund("is-in", ["performanceratingoverall", 4, 5]),
                _fund("lt", ["initialinvestment", 100_001]),
                _fund("lt", ["annualreturnnavy1categoryrank", 50]),
                _fund("is-in", ["riskratingoverall", 1, 2, 3]),
                _fund("eq", ["categoryname", "High Yield Bond"]),
                _fund("eq", ["exchange", "NAS"]),
            ],
        ),
    ),
    "portfolio_anchors": _predefined_query(
        sort_field="fundnetassets",
        sort_type="DESC",
        query=_fund(
            "and",
            [
                _fund("eq", ["categoryname", "Large Blend"]),
                _fund("is-in", ["performanceratingoverall", 4, 5]),
                _fund("lt", ["initialinvestment", 100_001]),
                _fund("lt", ["annualreturnnavy1categoryrank", 50]),
                _fund("eq", ["exchange", "NAS"]),
            ],
        ),
    ),
    "solid_large_growth_funds": _predefined_query(
        sort_field="fundnetassets",
        sort_type="DESC",
        query=_fund(
            "and",
            [
                _fund("eq", ["categoryname", "Large Growth"]),
                _fund("is-in", ["performanceratingoverall", 4, 5]),
                _fund("lt", ["initialinvestment", 100_001]),
                _fund("lt", ["annualreturnnavy1categoryrank", 50]),
                _fund("eq", ["exchange", "NAS"]),
            ],
        ),
    ),
    "solid_midcap_growth_funds": _predefined_query(
        sort_field="fundnetassets",
        sort_type="DESC",
        query=_fund(
            "and",
            [
                _fund("eq", ["categoryname", "Mid-Cap Growth"]),
                _fund("is-in", ["performanceratingoverall", 4, 5]),
                _fund("lt", ["initialinvestment", 100_001]),
                _fund("lt", ["annualreturnnavy1categoryrank", 50]),
                _fund("eq", ["exchange", "NAS"]),
            ],
        ),
    ),
    "top_mutual_funds": _predefined_query(
        sort_field="percentchange",
        sort_type="DESC",
        query=_fund(
            "and",
            [
                _fund("gt", ["intradayprice", 15]),
                _fund("is-in", ["performanceratingoverall", 4, 5]),
                _fund("gt", ["initialinvestment", 1000]),
                _fund("eq", ["exchange", "NAS"]),
            ],
        ),
    ),
}


@dataclass
class _ScreenRequest:
    offset: Optional[int] = None
    size: Optional[int] = None
    count: Optional[int] = None
    sort_field: Optional[str] = None
    sort_asc: Optional[bool] = None
    user_id: Optional[str] = None
    user_id_type: Optional[str] = None
    session: Any = None


def _base_params() -> Dict[str, str]:
    return {
        "corsDomain": "finance.yahoo.com",
        "formatted": "false",
        "lang": "en-US",
        "region": "US",
    }


def _normalize_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    legacy_map = {
        "sortField": "sort_field",
        "sortAsc": "sort_asc",
        "userId": "user_id",
        "userIdType": "user_id_type",
    }
    normalized = dict(kwargs)
    for legacy_key, normalized_key in legacy_map.items():
        if legacy_key in normalized:
            if normalized_key in normalized:
                raise TypeError(
                    f"Received both '{legacy_key}' and '{normalized_key}'. Use one form."
                )
            normalized[normalized_key] = normalized.pop(legacy_key)
    return normalized


def _parse_screen_request(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> _ScreenRequest:
    expected = (
        "offset",
        "size",
        "count",
        "sort_field",
        "sort_asc",
        "user_id",
        "user_id_type",
        "session",
    )
    if len(args) > len(expected):
        raise TypeError(
            f"Expected at most {len(expected)} positional arguments after query, "
            f"received {len(args)}."
        )

    normalized_kwargs = _normalize_kwargs(kwargs)
    unknown = set(normalized_kwargs) - set(expected)
    if unknown:
        unknown_fields = ", ".join(sorted(unknown))
        raise TypeError(f"Unexpected keyword argument(s): {unknown_fields}")

    data: Dict[str, Any] = dict(zip(expected, args))
    for key, value in normalized_kwargs.items():
        if key in data:
            raise TypeError(f"Received duplicated argument for '{key}'.")
        data[key] = value

    return _ScreenRequest(**data)


def _validate_size_and_count(request: _ScreenRequest) -> None:
    if request.count is not None and request.count > 250:
        raise ValueError("Yahoo limits query count to 250, reduce count.")
    if request.size is not None and request.size > 250:
        raise ValueError("Yahoo limits query size to 250, reduce size.")


def _coerce_to_custom_query(
    query: Union[str, EquityQuery, FundQuery],
    request: _ScreenRequest,
) -> Union[str, EquityQuery, FundQuery]:
    if not isinstance(query, str) or request.offset is None:
        return query

    post_query = PREDEFINED_SCREENER_QUERIES[query]
    query = post_query["query"]

    if request.sort_field is None:
        request.sort_field = post_query["sortField"]
    if request.sort_asc is None:
        request.sort_asc = post_query["sortType"].lower() == "asc"
    return query


def _predefined_fields(request: _ScreenRequest) -> Dict[str, Any]:
    fields = {
        "offset": request.offset,
        "count": request.count,
        "size": request.size,
        "sortField": request.sort_field,
        "sortAsc": request.sort_asc,
        "userId": request.user_id,
        "userIdType": request.user_id_type,
    }
    if fields["size"] is not None:
        warnings.warn(
            (
                "Screen 'size' argument is deprecated for predefined screens, "
                "set 'count' instead."
            ),
            DeprecationWarning,
            stacklevel=3,
        )
        fields["count"] = fields["size"]
        del fields["size"]

    return fields


def _run_predefined_query(query: str, request: _ScreenRequest, data: YfData) -> Dict[str, Any]:
    params_dict = _base_params()
    params_dict["scrIds"] = query

    for key, value in _predefined_fields(request).items():
        if value is not None:
            params_dict[key] = value

    response = data.get(url=_PREDEFINED_URL_, params=params_dict)
    try:
        response.raise_for_status()
    except curl_cffi.requests.exceptions.HTTPError:
        if query not in PREDEFINED_SCREENER_QUERIES:
            print(f"yfinance.screen: '{query}' is probably not a predefined query.")
        raise
    return response.json()["finance"]["result"][0]


def _custom_fields(request: _ScreenRequest) -> Dict[str, Any]:
    fields = {
        "offset": request.offset,
        "count": request.count,
        "size": request.size,
        "sortField": request.sort_field,
        "sortAsc": request.sort_asc,
        "userId": request.user_id,
        "userIdType": request.user_id_type,
    }
    defaults = {
        "offset": 0,
        "count": 25,
        "sortField": "ticker",
        "sortAsc": False,
        "userId": "",
        "userIdType": "guid",
    }
    for key, default in defaults.items():
        if fields.get(key) is None:
            fields[key] = default

    fields["sortType"] = "ASC" if fields["sortAsc"] else "DESC"
    del fields["sortAsc"]
    return fields


def _run_custom_query(query: QueryBase, request: _ScreenRequest, data: YfData) -> Dict[str, Any]:
    post_query = _custom_fields(request)
    post_query["query"] = query.to_dict()

    if isinstance(query, EquityQuery):
        post_query["quoteType"] = "EQUITY"
    elif isinstance(query, FundQuery):
        post_query["quoteType"] = "MUTUALFUND"

    payload = dumps(post_query, separators=(",", ":"), ensure_ascii=False)
    response = data.post(_SCREENER_URL_, data=payload, params=_base_params())
    response.raise_for_status()
    return response.json()["finance"]["result"][0]


@dynamic_docstring(
    {
        "predefined_screeners": generate_list_table_from_dict_universal(
            PREDEFINED_SCREENER_QUERIES,
            bullets=True,
            title="Predefined queries (Dec-2024)",
        )
    }
)
def screen(
    query: Union[str, EquityQuery, FundQuery],
    *args: Any,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Run a screen with either a predefined query name or a custom Query object.

    Positional args after ``query`` follow this legacy order:
    ``offset, size, count, sort_field, sort_asc, user_id, user_id_type, session``.

    Keyword args support both snake_case and legacy camelCase forms:
    ``sort_field`` / ``sortField``, ``sort_asc`` / ``sortAsc``,
    ``user_id`` / ``userId``, ``user_id_type`` / ``userIdType``.

    {predefined_screeners}
    """
    request = _parse_screen_request(args, kwargs)
    _validate_size_and_count(request)

    query = _coerce_to_custom_query(query, request)
    data = YfData(session=request.session)

    if isinstance(query, str):
        return _run_predefined_query(query, request, data)
    if isinstance(query, QueryBase):
        return _run_custom_query(query, request, data)
    raise ValueError(f'Query must be type str or QueryBase, not "{type(query)}"')
