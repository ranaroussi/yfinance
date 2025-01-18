import unittest
from unittest.mock import Mock

from yfinance.live import BaseWebSocket


class TestWebSocket(unittest.TestCase):
    def test_decode_message_valid(self):
        message = ("CgdCVEMtVVNEFYoMuUcYwLCVgIplIgNVU0QqA0NDQzApOAFFPWrEP0iAgOrxvANVx/25R12csrRHZYD8skR9/"
                   "7i0R7ABgIDq8bwD2AEE4AGAgOrxvAPoAYCA6vG8A/IBA0JUQ4ECAAAAwPrjckGJAgAA2P5ZT3tC")

        ws = BaseWebSocket(Mock())
        decoded = ws._decode_message(message)

        expected = {'id': 'BTC-USD', 'price': 94745.08, 'time': '1736509140000', 'currency': 'USD', 'exchange': 'CCC',
                    'quote_type': 41, 'market_hours': 1, 'change_percent': 1.5344921, 'day_volume': '59712028672',
                    'day_high': 95227.555, 'day_low': 92517.22, 'change': 1431.8906, 'open_price': 92529.99,
                    'last_size': '59712028672', 'price_hint': '2', 'vol_24hr': '59712028672',
                    'vol_all_currencies': '59712028672', 'from_currency': 'BTC', 'circulating_supply': 19808172.0,
                    'market_cap': 1876726640000.0}

        self.assertEqual(expected, decoded)

    def test_decode_message_invalid(self):
        websocket = BaseWebSocket(Mock())
        base64_message = "invalid_base64_string"
        decoded = websocket._decode_message(base64_message)
        assert "error" in decoded
        assert "raw_base64" in decoded
        self.assertEqual(base64_message, decoded["raw_base64"])
