from __future__ import print_function
from typing import Dict, Optional

import pandas as _pd

from .domain import Domain, _QUERY_URL_
from .. import utils

class Sector(Domain):
    def __init__(self, key, session=None, proxy=None):
        super(Sector, self).__init__(key, session, proxy)
        self._query_url: str = f'{_QUERY_URL_}/sectors/{self._key}'
        
        self._top_etfs: Optional[Dict] = None
        self._top_mutual_funds: Optional[Dict] = None
        self._industries: Optional[_pd.DataFrame] = None

    def __repr__(self):
        return f'yfinance.Sector object <{self._key}>'
    
    @property
    def top_etfs(self) -> Dict[str, str]:
        self._ensure_fetched(self._top_etfs)
        return self._top_etfs

    @property
    def top_mutual_funds(self) -> Dict[str, str]:
        self._ensure_fetched(self._top_mutual_funds)
        return self._top_mutual_funds

    @property
    def industries(self) -> _pd.DataFrame:
        self._ensure_fetched(self._industries)
        return self._industries
    
    def _parse_top_etfs(self, top_etfs: Dict) -> Dict[str, str]:
        return {e.get('symbol'): e.get('name') for e in top_etfs}

    def _parse_top_mutual_funds(self, top_mutual_funds: Dict) -> Dict[str, str]:
        return {e.get('symbol'): e.get('name') for e in top_mutual_funds}
    
    def _parse_industries(self, industries: Dict) -> _pd.DataFrame:
        industries_column = ['key','name','symbol','market weight']
        industries_values = [(i.get('key'),
                              i.get('name'),
                              i.get('symbol'),
                              i.get('marketWeight',{}).get('raw', None)
                              ) for i in industries if i.get('name') != 'All Industries']
        return _pd.DataFrame(industries_values, columns = industries_column).set_index('key')

    def _fetch_and_parse(self) -> None:
        result = None
        
        try:
            result = self._fetch(self._query_url, self.proxy)
            data = result['data']
            self._parse_and_assign_common(data)

            self._top_etfs = self._parse_top_etfs(data.get('topETFs', {}))
            self._top_mutual_funds = self._parse_top_mutual_funds(data.get('topMutualFunds', {}))
            self._industries = self._parse_industries(data.get('industries', {}))

        except Exception as e:
            logger = utils.get_yf_logger()
            logger.error(f"Failed to get sector data for '{self._key}' reason: {e}")
            logger.debug("Got response: ")
            logger.debug("-------------")
            logger.debug(f" {result}")
            logger.debug("-------------")