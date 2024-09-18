from __future__ import print_function
from typing import Dict, Optional

import pandas as _pd

from .domain import Domain, _QUERY_URL_
from .. import utils

class Industry(Domain):
    def __init__(self, key, session=None, proxy=None):
        super(Industry, self).__init__(key, session, proxy)
        self._query_url = f'{_QUERY_URL_}/industries/{self._key}'

        self._sector_key = None
        self._sector_name = None
        self._top_performing_companies = None
        self._top_growth_companies = None

    def __repr__(self):
        return f'yfinance.Industry object <{self._key}>'
    
    @property
    def sector_key(self) -> str:
        self._ensure_fetched(self._sector_key)
        return self._sector_key
    
    @property
    def sector_name(self) -> str:
        self._ensure_fetched(self._sector_name)
        return self._sector_name
    
    @property
    def top_performing_companies(self) -> Optional[_pd.DataFrame]:
        self._ensure_fetched(self._top_performing_companies)
        return self._top_performing_companies
    
    @property
    def top_growth_companies(self) -> Optional[_pd.DataFrame]:
        self._ensure_fetched(self._top_growth_companies)
        return self._top_growth_companies
    
    def _parse_top_performing_companies(self, top_performing_companies: Dict) -> Optional[_pd.DataFrame]:
        compnaies_column = ['symbol','name','ytd return',' last price','target price']
        compnaies_values = [(c.get('symbol', None),
                             c.get('name', None),
                             c.get('ytdReturn',{}).get('raw', None),
                             c.get('lastPrice',{}).get('raw', None),
                             c.get('targetPrice',{}).get('raw', None),) for c in top_performing_companies]
        
        if not compnaies_values: 
            return None

        return _pd.DataFrame(compnaies_values, columns = compnaies_column).set_index('symbol')
    
    def _parse_top_growth_companies(self, top_growth_companies: Dict) -> Optional[_pd.DataFrame]:
        compnaies_column = ['symbol','name','ytd return',' growth estimate']
        compnaies_values = [(c.get('symbol', None),
                             c.get('name', None),
                             c.get('ytdReturn',{}).get('raw', None),
                             c.get('growthEstimate',{}).get('raw', None),) for c in top_growth_companies]
        
        if not compnaies_values: 
            return None

        return _pd.DataFrame(compnaies_values, columns = compnaies_column).set_index('symbol')

    def _fetch_and_parse(self) -> None:
        result = None
        
        try:
            result = self._fetch(self._query_url, self.proxy)
            data = result['data']
            self._parse_and_assign_common(data)

            self._sector_key = data.get('sectorKey')
            self._sector_name = data.get('sectorName')
            self._top_performing_companies = self._parse_top_performing_companies(data.get('topPerformingCompanies'))
            self._top_growth_companies = self._parse_top_growth_companies(data.get('topGrowthCompanies'))

            return result
        except Exception as e:
            logger = utils.get_yf_logger()
            logger.error(f"Failed to get industry data for '{self._key}' reason: {e}")
            logger.debug("Got response: ")
            logger.debug("-------------")
            logger.debug(f" {result}")
            logger.debug("-------------")