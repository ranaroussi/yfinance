'''
Module for mocking requests test
'''

import json

from pathlib import Path
from yfinance.utils import get_json

data_path = Path(__file__).parent/'data'

# Mock based on https://stackoverflow.com/a/28507806/3558475:
def get_mocked_get_json(url_map):
  '''
  Returns mocked get_json function
  '''
  def mocked_get_json(url, _=None):
    '''
    Mocks the get_json function
    '''
    if url not in url_map:
      return get_json(url)

    with open(data_path/url_map[url]) as json_file:
      data = json_file.read()

    return json.loads(data)
  return mocked_get_json
