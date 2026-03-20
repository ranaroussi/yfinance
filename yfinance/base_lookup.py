"""TickerBase helpers for ISIN, news, and earnings-date lookups."""

from __future__ import print_function

import html as _html
from io import StringIO
import re as _re
from typing import Any, Optional
import unicodedata as _unicodedata
from urllib.parse import quote as urlencode

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd

from . import utils
from .const import _QUERY1_URL_, _ROOT_URL_
from .exceptions import YFEarningsDateMissing
from .http import parse_json_response

_BUSINESS_INSIDER_ROW_RE = _re.compile(
    r'new Array\("((?:[^"\\]|\\.)*)", "((?:[^"\\]|\\.)*)", '
    r'"((?:[^"\\]|\\.)*)", "((?:[^"\\]|\\.)*)", '
    r'"((?:[^"\\]|\\.)*)", "((?:[^"\\]|\\.)*)"\)'
)
_ISIN_NAME_STOPWORDS = {
    "company",
    "corp",
    "corporation",
    "inc",
    "incorporated",
    "limited",
    "ltd",
    "nv",
    "plc",
    "s",
    "sa",
    "se",
    "societe",
}
_ISIN_NOISE_TERMS = {
    "adr": 35,
    "depository": 45,
    "depositary": 45,
    "fidelite": 60,
    "hedged": 20,
    "prime": 25,
    "receipt": 30,
    "reg": 10,
    "unsponsored": 50,
}


def _decode_business_insider_field(value: str) -> str:
    """Decode simple escaped fields from the BusinessInsider suggest payload."""
    return _html.unescape(value.replace(r'\"', '"')).strip()


def _normalize_isin_name(value: str) -> str:
    """Normalize company-name text for fuzzy ISIN candidate matching."""
    normalized = _unicodedata.normalize("NFKD", value)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = _re.sub(r"[^a-zA-Z0-9]+", " ", normalized).lower()
    tokens = [token for token in normalized.split() if token not in _ISIN_NAME_STOPWORDS]
    return " ".join(tokens)


def _parse_isin_candidates(payload: str) -> list[dict[str, str]]:
    """Parse structured stock candidates from the BusinessInsider suggest payload."""
    candidates: list[dict[str, str]] = []
    for name, category, keywords, bias, extension, ids in _BUSINESS_INSIDER_ROW_RE.findall(payload):
        decoded_name = _decode_business_insider_field(name)
        decoded_category = _decode_business_insider_field(category)
        decoded_keywords = _decode_business_insider_field(keywords)
        if decoded_name == "Name" and decoded_category == "Category":
            continue
        if decoded_category != "Stocks":
            continue

        keyword_parts = decoded_keywords.split("|")
        symbol = keyword_parts[0].strip().upper() if keyword_parts else ""
        isin = keyword_parts[1].strip().upper() if len(keyword_parts) > 1 else ""
        if not utils.is_isin(isin):
            continue

        candidates.append(
            {
                "name": decoded_name,
                "category": decoded_category,
                "keywords": decoded_keywords,
                "bias": _decode_business_insider_field(bias),
                "extension": _decode_business_insider_field(extension),
                "ids": _decode_business_insider_field(ids),
                "symbol": symbol,
                "isin": isin,
                "normalized_name": _normalize_isin_name(decoded_name),
                "normalized_keywords": _normalize_isin_name(decoded_keywords),
            }
        )

    return candidates


def _score_isin_candidate(
    candidate: dict[str, str],
    ticker: str,
    normalized_targets: list[str],
) -> int:
    """Score a parsed BusinessInsider stock candidate for one Yahoo ticker."""
    score = 0
    candidate_symbol = candidate["symbol"]
    ticker_upper = ticker.upper()
    ticker_base = ticker_upper.split(".")[0]
    if candidate_symbol == ticker_upper:
        score += 220
    elif candidate_symbol == ticker_base:
        score += 160
    elif candidate_symbol:
        score += 10

    candidate_name = candidate["normalized_name"]
    candidate_tokens = set(candidate_name.split())
    for target in normalized_targets:
        if not target:
            continue
        target_tokens = set(target.split())
        if candidate_name == target:
            score += 150
            continue
        if candidate_name and (candidate_name in target or target in candidate_name):
            score += 120
            continue
        if candidate_tokens and target_tokens:
            overlap = len(candidate_tokens & target_tokens) / len(target_tokens)
            score += int(round(overlap * 100))

    searchable_tokens = set(
        f"{candidate['normalized_name']} {candidate['normalized_keywords']}".split()
    )
    for term, penalty in _ISIN_NOISE_TERMS.items():
        if term in searchable_tokens:
            score -= penalty

    return score


def _select_isin_candidate(
    candidates: list[dict[str, str]],
    ticker: str,
    quote_info: dict,
) -> Optional[dict[str, str]]:
    """Select the most likely stock candidate for a ticker from parsed results."""
    if not candidates:
        return None

    normalized_targets: list[str] = []
    for key in ("symbol", "shortName", "longName"):
        value = quote_info.get(key)
        if isinstance(value, str):
            normalized = _normalize_isin_name(value)
            if normalized and normalized not in normalized_targets:
                normalized_targets.append(normalized)

    scored = [
        (_score_isin_candidate(candidate, ticker, normalized_targets), candidate)
        for candidate in candidates
    ]
    best_score, best_candidate = max(scored, key=lambda item: item[0])
    if best_score <= 0:
        return None
    return best_candidate


def _select_exact_symbol_candidate(
    candidates: list[dict[str, str]],
    ticker: str,
) -> Optional[dict[str, str]]:
    """Return an exact symbol match from parsed BusinessInsider candidates."""
    ticker_upper = ticker.upper()
    for candidate in candidates:
        if candidate["symbol"] == ticker_upper:
            return candidate
    return None


class TickerBaseLookupMixin:
    """Mixin implementing external lookups used by ``TickerBase``."""

    ticker: str
    _isin: Optional[str]
    _quote: Any
    _data: Any
    _news: list
    _earnings_dates: dict[int, Optional[pd.DataFrame]]

    def _get_ticker_tz(self, timeout):
        raise NotImplementedError

    def get_isin(self) -> Optional[str]:
        """Return ISIN for the ticker when available."""
        if self._isin is not None:
            return self._isin

        ticker = self.ticker.upper()

        if "-" in ticker or "^" in ticker:
            self._isin = '-'
            return self._isin

        if self._quote.info is None:
            return None

        quote_info = self._quote.info
        query_values = [ticker]
        for key in ("shortName", "longName"):
            value = quote_info.get(key)
            if isinstance(value, str) and value != "" and value not in query_values:
                query_values.append(value)

        url = (
            "https://markets.businessinsider.com/ajax/SearchController_Suggest"
            f"?max_results=25&query={urlencode(query_values[0])}"
        )
        data = self._data.cache_get(url=url).text
        candidates = _parse_isin_candidates(data)
        candidate = _select_exact_symbol_candidate(candidates, ticker)

        if candidate is None:
            for query_value in query_values[1:]:
                url = (
                    "https://markets.businessinsider.com/ajax/SearchController_Suggest"
                    f"?max_results=25&query={urlencode(query_value)}"
                )
                data = self._data.cache_get(url=url).text
                candidate = _select_isin_candidate(
                    _parse_isin_candidates(data),
                    ticker,
                    quote_info,
                )
                if candidate is not None:
                    break

        if candidate is None:
            self._isin = '-'
            return self._isin

        self._isin = candidate["isin"]
        return self._isin

    def get_news(self, count=10, tab="news") -> list:
        """Allowed options for tab: "news", "all", "press releases"""
        if self._news:
            return self._news

        logger = utils.get_yf_logger()

        tab_queryrefs = {
            "all": "newsAll",
            "news": "latestNews",
            "press releases": "pressRelease",
        }

        query_ref = tab_queryrefs.get(tab.lower())
        if not query_ref:
            valid_tabs = ", ".join(tab_queryrefs.keys())
            raise ValueError(f"Invalid tab name '{tab}'. Choose from: {valid_tabs}")

        url = f"{_ROOT_URL_}/xhr/ncp?queryRef={query_ref}&serviceKey=ncp_fin"
        payload = {
            "serviceConfig": {
                "snippetCount": count,
                "s": [self.ticker],
            }
        }

        data = self._data.post(url, body=payload)
        data = parse_json_response(
            data,
            logger,
            "%s: Failed to retrieve the news and received faulty response instead.",
            self.ticker,
        )

        news = data.get("data", {}).get("tickerStream", {}).get("stream", [])

        self._news = [article for article in news if not article.get('ad', [])]
        return self._news

    def get_earnings_dates(self, limit=12, offset=0) -> Optional[pd.DataFrame]:
        """Return upcoming and historical earnings dates."""
        if limit > 100:
            raise ValueError("Yahoo caps limit at 100")

        if self._earnings_dates and limit in self._earnings_dates:
            return self._earnings_dates[limit]

        df = self._get_earnings_dates_using_scrape(limit, offset)
        self._earnings_dates[limit] = df
        return df

    @utils.log_indent_decorator
    def _get_earnings_dates_using_scrape(self, limit=12, offset=0) -> Optional[pd.DataFrame]:
        """
        Uses YfData.cache_get() to scrape earnings data from YahooFinance.
        (https://finance.yahoo.com/calendar/earnings?symbol=INTC)

        Args:
            limit (int): Number of rows to extract (max=100)
            offset (int): if 0, search from future EPS estimates.
                          if 1, search from the most recent EPS.
                          if x, search from x'th recent EPS.

        Returns:
            pd.DataFrame in the following format.

                       EPS Estimate Reported EPS Surprise(%)
            Date
            2025-10-30         2.97            -           -
            2025-07-22         1.73         1.54      -10.88
            2025-05-06         2.63          2.7        2.57
            2025-02-06         2.09         2.42       16.06
            2024-10-31         1.92         1.55      -19.36
            ...                 ...          ...         ...
            2014-07-31         0.61         0.65        7.38
            2014-05-01         0.55         0.68       22.92
            2014-02-13         0.55         0.58        6.36
            2013-10-31         0.51         0.54        6.86
            2013-08-01         0.46          0.5        7.86
        """
        if 0 < limit <= 25:
            size = 25
        elif 25 < limit <= 50:
            size = 50
        elif 50 < limit <= 100:
            size = 100
        else:
            raise ValueError("Please use limit <= 100")

        url = (
            f"https://finance.yahoo.com/calendar/earnings?symbol={self.ticker}"
            f"&offset={offset}&size={size}"
        )
        response = self._data.cache_get(url)

        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table")
        if not table:
            err_msg = "No earnings dates found, symbol may be delisted"
            logger = utils.get_yf_logger()
            logger.error("%s: %s", self.ticker, err_msg)
            return None

        table_html = str(table)
        html_stringio = StringIO(table_html)
        df = pd.read_html(html_stringio, na_values=['-'])[0]
        df = df.drop(["Symbol", "Company"], axis=1)
        df.rename(columns={'Surprise (%)': 'Surprise(%)'}, inplace=True)
        df = df.dropna(subset="Earnings Date")

        df['Earnings Date'] = df['Earnings Date'].str.replace('EDT', 'America/New_York')
        df['Earnings Date'] = df['Earnings Date'].str.replace('EST', 'America/New_York')
        date_parts = df['Earnings Date'].str.rsplit(' ', n=1, expand=True)
        df['Earnings Date'] = pd.to_datetime(date_parts[0], format='%B %d, %Y at %I %p')
        df['Earnings Date'] = pd.Series(
            [dt.tz_localize(tz_name) for dt, tz_name in zip(df['Earnings Date'], date_parts[1])]
        )
        return df.set_index("Earnings Date")

    @utils.log_indent_decorator
    def _get_earnings_dates_using_screener(self, limit=12) -> Optional[pd.DataFrame]:
        """
        Get earning dates (future and historic)

        In Summer 2025, Yahoo stopped updating the data at this endpoint.
        So reverting to scraping HTML.

        Args:
            limit (int): max amount of upcoming and recent earnings dates to return.
                Default value 12 should return next 4 quarters and last 8 quarters.
                Increase if more history is needed.
        Returns:
            pd.DataFrame
        """
        logger = utils.get_yf_logger()

        url = f"{_QUERY1_URL_}/v1/finance/visualization"
        params = {"lang": "en-US", "region": "US"}
        body = {
            "size": limit,
            "query": {"operator": "eq", "operands": ["ticker", self.ticker]},
            "sortField": "startdatetime",
            "sortType": "DESC",
            "entityIdType": "earnings",
            "includeFields": [
                "startdatetime",
                "timeZoneShortName",
                "epsestimate",
                "epsactual",
                "epssurprisepct",
                "eventtype",
            ],
        }
        response = self._data.post(url, params=params, body=body)
        json_data = response.json()

        columns = [
            row['label']
            for row in json_data['finance']['result'][0]['documents'][0]['columns']
        ]
        rows = json_data['finance']['result'][0]['documents'][0]['rows']
        df = pd.DataFrame(rows, columns=columns)

        if df.empty:
            _exception = YFEarningsDateMissing(self.ticker)
            err_msg = str(_exception)
            logger.error("%s: %s", self.ticker, err_msg)
            return None

        df['Event Type'] = df['Event Type'].replace('^1$', 'Call', regex=True)
        df['Event Type'] = df['Event Type'].replace('^2$', 'Earnings', regex=True)
        df['Event Type'] = df['Event Type'].replace('^11$', 'Meeting', regex=True)

        df['Earnings Date'] = pd.to_datetime(df['Event Start Date'])
        tz = self._get_ticker_tz(timeout=30)
        if df['Earnings Date'].dt.tz is None:
            df['Earnings Date'] = df['Earnings Date'].dt.tz_localize(tz)
        else:
            df['Earnings Date'] = df['Earnings Date'].dt.tz_convert(tz)

        columns_to_update = ['Surprise (%)', 'EPS Estimate', 'Reported EPS']
        df[columns_to_update] = df[columns_to_update].astype('float64').replace(0.0, np.nan)

        df.drop(['Event Start Date', 'Timezone short name'], axis=1, inplace=True)
        df.set_index('Earnings Date', inplace=True)
        df.rename(columns={'Surprise (%)': 'Surprise(%)'}, inplace=True)

        self._earnings_dates[limit] = df
        return df
