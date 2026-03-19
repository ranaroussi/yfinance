"""Financial-statement formatting helpers used by yfinance."""

import re as _re
from typing import List, Optional, Sequence

import pandas as _pd


def build_template(data):
    """Build ordering metadata for Yahoo financial statement templates."""
    template_ttm_order = []
    template_annual_order = []
    template_order = []
    level_detail = []

    def traverse(node, level):
        if level > 5:
            return
        template_ttm_order.append(f"trailing{node['key']}")
        template_annual_order.append(f"annual{node['key']}")
        template_order.append(f"{node['key']}")
        level_detail.append(level)
        if "children" in node:
            for child in node["children"]:
                traverse(child, level + 1)

    for key in data["template"]:
        traverse(key, 0)

    return template_ttm_order, template_annual_order, template_order, level_detail


def retrieve_financial_details(data):
    """Extract trailing and annual financial time-series dictionaries."""
    ttm_dicts = []
    annual_dicts = []

    for key, timeseries in data.get("timeSeries", {}).items():
        try:
            if timeseries:
                time_series_dict = {"index": key}
                for each in timeseries:
                    if not each:
                        continue
                    time_series_dict[each.get("asOfDate")] = each.get("reportedValue")
                if "trailing" in key:
                    ttm_dicts.append(time_series_dict)
                elif "annual" in key:
                    annual_dicts.append(time_series_dict)
        except KeyError as err:
            print(f"An error occurred while processing the key: {err}")
    return ttm_dicts, annual_dicts


def format_annual_financial_statement(
    level_detail,
    annual_dicts,
    annual_order,
    ttm_dicts=None,
    ttm_order=None,
):
    """Format annual financial statement data into the expected dataframe shape."""
    annual = _pd.DataFrame.from_dict(annual_dicts).set_index("index")
    annual = annual.reindex(annual_order)
    annual.index = annual.index.str.replace(r"annual", "")

    if ttm_dicts and ttm_order:
        ttm = _pd.DataFrame.from_dict(ttm_dicts).set_index("index").reindex(ttm_order)
        ttm.columns = ["TTM " + str(col) for col in ttm.columns]
        ttm.index = ttm.index.str.replace(r"trailing", "")
        statement = annual.merge(ttm, left_index=True, right_index=True)
    else:
        statement = annual

    statement.index = camel2title(statement.T.index.tolist())
    statement["level_detail"] = level_detail
    statement = statement.set_index([statement.index, "level_detail"])
    statement = statement[sorted(statement.columns, reverse=True)]
    statement = statement.dropna(how="all")
    return statement


def format_quarterly_financial_statement(statement, level_detail, order):
    """Format quarterly financial statement data into the expected dataframe shape."""
    statement = statement.reindex(order)
    statement.index = camel2title(statement.T.columns.tolist())
    statement["level_detail"] = level_detail
    statement = statement.set_index([statement.index, "level_detail"])
    statement = statement[sorted(statement.columns, reverse=True)]
    statement = statement.dropna(how="all")
    statement.columns = _pd.to_datetime(statement.columns).date
    return statement


def camel2title(
    strings: Sequence[str],
    sep: str = " ",
    acronyms: Optional[Sequence[str]] = None,
) -> List[str]:
    """Convert camel-cased strings to title-cased display labels."""
    if isinstance(strings, str) or not hasattr(strings, "__iter__"):
        raise TypeError("camel2title() 'strings' argument must be iterable of strings")
    strings_list = list(strings)
    if len(strings_list) == 0:
        return strings_list
    if not isinstance(strings_list[0], str):
        raise TypeError("camel2title() 'strings' argument must be iterable of strings")
    if not isinstance(sep, str) or len(sep) != 1:
        raise ValueError(
            f"camel2title() 'sep' argument = '{sep}' must be single character"
        )
    if _re.match("[a-zA-Z0-9]", sep):
        raise ValueError(
            f"camel2title() 'sep' argument = '{sep}' cannot be alpha-numeric"
        )
    if _re.escape(sep) != sep and sep not in {" ", "-"}:
        raise ValueError(
            f"camel2title() 'sep' argument = '{sep}' cannot be special character"
        )

    if acronyms is None:
        pattern = "([a-z])([A-Z])"
        replacement = rf"\g<1>{sep}\g<2>"
        return [_re.sub(pattern, replacement, value).title() for value in strings_list]

    acronyms_list = list(acronyms)
    if (
        isinstance(acronyms, str)
        or not hasattr(acronyms, "__iter__")
        or (len(acronyms_list) > 0 and not isinstance(acronyms_list[0], str))
    ):
        raise TypeError("camel2title() 'acronyms' argument must be iterable of strings")
    for acronym in acronyms_list:
        if not _re.match("^[A-Z]+$", acronym):
            raise ValueError(
                "camel2title() 'acronyms' argument must only contain upper-case, "
                f"but '{acronym}' detected"
            )

    pattern = "([a-z])([A-Z])"
    replacement = rf"\g<1>{sep}\g<2>"
    strings_with_sep = [_re.sub(pattern, replacement, value) for value in strings_list]

    for acronym in acronyms_list:
        pattern = f"({acronym})([A-Z][a-z])"
        replacement = rf"\g<1>{sep}\g<2>"
        strings_with_sep = [
            _re.sub(pattern, replacement, value) for value in strings_with_sep
        ]

    split_strings = [value.split(sep) for value in strings_with_sep]
    titled_strings = [
        [word.title() if word not in acronyms_list else word for word in words]
        for words in split_strings
    ]
    return [sep.join(words) for words in titled_strings]


def snake_case_to_camel_case(value: str) -> str:
    """Convert a snake_case identifier to camelCase."""
    words = value.split("_")
    return words[0] + "".join(word.title() for word in words[1:])
