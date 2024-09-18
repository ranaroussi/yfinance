from ..ticker import Ticker
from ..const import _QUERY1_URL_
from ..data import YfData
from typing import Dict, List, Optional

import pandas as _pd

_QUERY_URL_ = f'{_QUERY1_URL_}/v1/finance'

class Domain:
    def __init__(self, key: str, session=None, proxy=None):
        self._key: str = key
        self.proxy = proxy
        self.session = session
        self._data: YfData = YfData(session=session)
        
        self._name: Optional[str] = None
        self._symbol: Optional[str] = None
        self._overview: Optional[Dict] = None
        self._top_companies: Optional[_pd.DataFrame] = None
        self._research_reports: Optional[List[Dict[str, str]]] = None
    
    @property
    def key(self) -> str:
        return self._key
    
    @property
    def name(self) -> str:
        self._ensure_fetched(self._name)
        return self._name
    
    @property
    def symbol(self) -> str:
        self._ensure_fetched(self._symbol)
        return self._symbol
    
    @property
    def ticker(self) -> Ticker:
        self._ensure_fetched(self._symbol)
        return Ticker(self._symbol)
    
    @property
    def overview(self) -> Dict:
        self._ensure_fetched(self._overview)
        return self._overview
    
    @property
    def top_companies(self) -> Optional[_pd.DataFrame]:
        self._ensure_fetched(self._top_companies)
        return self._top_companies 
    
    @property
    def research_reports(self) -> List[Dict[str, str]]:
        self._ensure_fetched(self._research_reports)
        return self._research_reports
    
    def _fetch(self, query_url, proxy) -> Dict:
        params_dict = {"formatted": "true", "withReturns": "true", "lang": "en-US", "region": "US"}
        result = self._data.get_raw_json(query_url, user_agent_headers=self._data.user_agent_headers, params=params_dict, proxy=proxy)
        return result
    
    def _parse_and_assign_common(self, data) -> None:
        self._name = data.get('name')
        self._symbol = data.get('symbol')
        self._overview = self._parse_overview(data.get('overview', {}))
        self._top_companies = self._parse_top_companies(data.get('topCompanies', {}))
        self._research_reports = data.get('researchReports')

    def _parse_overview(self, overview) -> Dict:
        return {
            "companies_count": overview.get('companiesCount', None),
            "market_cap": overview.get('marketCap', {}).get('raw', None),
            "message_board_id": overview.get('messageBoardId', None),
            "description": overview.get('description', None),
            "industries_count": overview.get('industriesCount', None),
            "market_weight": overview.get('marketWeight', {}).get('raw', None),
            "employee_count": overview.get('employeeCount', {}).get('raw', None)
        }

    def _parse_top_companies(self, top_companies) -> Optional[_pd.DataFrame]:
        top_companies_column = ['symbol', 'name', 'rating', 'market weight']
        top_companies_values = [(c.get('symbol'), 
                                c.get('name'), 
                                c.get('rating'), 
                                c.get('marketWeight',{}).get('raw',None)) for c in top_companies]

        if not top_companies_values: 
            return None
        
        return _pd.DataFrame(top_companies_values, columns = top_companies_column).set_index('symbol')

    def _fetch_and_parse(self) -> None:
        raise NotImplementedError("_fetch_and_parse() needs to be implemented by children classes")

    def _ensure_fetched(self, attribute) -> None:
        if attribute is None:
            self._fetch_and_parse()