import unittest

from .context import yfinance as yf

import datetime as _dt
import pandas as _pd
import pytz as _pytz

class TestUtils(unittest.TestCase):

    def test_parse_user_dt(self):
        """ Purpose of _parse_user_dt() is to take any date-like value,
            combine with specified timezone and
            return its localized timestamp.
        """
        tz_name = "America/New_York"
        tz = _pytz.timezone(tz_name)
        dt_answer = tz.localize(_dt.datetime(2023,1,1))

        # All possible versions of 'dt_answer'
        values = ["2023-01-01", _dt.date(2023,1,1), _dt.datetime(2023,1,1), _pd.Timestamp(_dt.date(2023,1,1))]
        # - now add localized versions
        values.append(tz.localize(_dt.datetime(2023,1,1)))
        values.append(_pd.Timestamp(_dt.date(2023,1,1)).tz_localize(tz_name))
        values.append(int(_pd.Timestamp(_dt.date(2023,1,1)).tz_localize(tz_name).timestamp()))

        for v in values:
            v2 = yf.utils._parse_user_dt(v, tz_name)
            self.assertEqual(v2, dt_answer.timestamp())

