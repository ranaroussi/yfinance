import datetime
import json

import pandas as pd
import numpy as np

from yfinance import utils
from yfinance.data import TickerData
from yfinance.exceptions import YFinanceDataException, YFinanceException

class Esg:
    def __init__(self, data: TickerData, proxy=None):
        self._data = data
        self.proxy = proxy
    def get_esg_scrape(self, proxy):
        self._sustainability_data = self._data.get_json_data_stores('sustainability', proxy)
        esg_data = self._sustainability_data['ESGStore']['peerScores']['esgPeerScoresDocuments']
        return esg_data