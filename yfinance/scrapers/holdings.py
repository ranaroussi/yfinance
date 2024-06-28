from io import StringIO
import pandas as pd
from bs4 import BeautifulSoup

from yfinance.data import YfData

class Holdings:
    _SCRAPE_URL_ = 'https://finance.yahoo.com/quote'

    def __init__(self, data: YfData, symbol: str, proxy=None):
        self._data = data
        self._symbol = symbol
        self.proxy = proxy
        self._major = None

    @property
    def major(self) -> pd.DataFrame:
        if self._major is None:
            self._scrape(self.proxy)
        return self._major

    def _scrape(self, proxy):
        ticker_url = "{}/{}".format(self._SCRAPE_URL_, self._symbol)
        try:
            resp = self._data.cache_get(ticker_url + '/holdings', proxy)
            if "/holdings" not in resp.url:
                raise Exception(f'{self.ticker}: does not have holdings')

            # Manually parse, because while pandas.read_html gets tables, 
            # it doesn't get table names.
            soup = BeautifulSoup(resp.text, 'html.parser')
            h3 = None
            tables = {}
            for element in soup.descendants:
                if element.name == 'h3':
                    h3 = element.get_text(strip=True)
                elif element.name == 'table' and h3:
                    try:
                        df_list = pd.read_html(str(element))
                        if df_list:
                            tables[h3] = df_list[0]
                    except ValueError:
                        pass

            # Prettify tables, convert types
            for k in tables.keys():
                d = tables[k]

                d = d.set_index(d.columns[0])
                d.index.name = None
                d.columns = [k]

                d[k] = d[k].str.replace('--', '')
                f_pct = d[k].str.contains('%')
                if (f_pct|(d[k]=='')).all():
                    d[k] = d[k].str.replace('%', '')
                try:
                    d[k] = pd.to_numeric(d[k])
                except ValueError:
                    pass

                tables[k] = d

            # print("------------------------")
            # for k in tables.keys():
            #     print(k)
            #     t = tables[k]
            #     print(t)
            #     print(t[t.columns[0]].dtype)
            #     print("------------------------")

            self._major_holdings = tables

        except Exception:
            holdings = []
