import asyncio
import json
import unittest
import warnings
from unittest.mock import Mock

from websockets.asyncio.server import serve

from yfinance.live import AsyncWebSocket, BaseWebSocket


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


class _FakeAsyncConnection:
    def __init__(self, messages):
        self._messages = messages
        self.closed = False

    async def __aiter__(self):
        for message in self._messages:
            yield message
        raise asyncio.CancelledError

    async def close(self):
        self.closed = True


class TestAsyncWebSocketListen(unittest.IsolatedAsyncioTestCase):
    message = ("CgdCVEMtVVNEFYoMuUcYwLCVgIplIgNVU0QqA0NDQzApOAFFPWrEP0iAgOrxvANVx/25R12csrRHZYD8skR9/"
               "7i0R7ABgIDq8bwD2AEE4AGAgOrxvAPoAYCA6vG8A/IBA0JUQ4ECAAAAwPrjckGJAgAA2P5ZT3tC")

    async def _listen(self, handler):
        ws = AsyncWebSocket(verbose=False)
        ws._ws = _FakeAsyncConnection([json.dumps({"message": self.message})])
        with warnings.catch_warnings():
            # asyncio.iscoroutinefunction is deprecated since Python 3.14.
            warnings.simplefilter("error", DeprecationWarning)
            await ws.listen(handler)
        self.assertTrue(ws._ws.closed)

    async def test_listen_awaits_async_handler(self):
        received = []

        async def handler(message):
            received.append(message)

        await self._listen(handler)
        self.assertEqual(["BTC-USD"], [m["id"] for m in received])

    async def test_listen_calls_sync_handler(self):
        received = []

        def handler(message):
            received.append(message)

        await self._listen(handler)
        self.assertEqual(["BTC-USD"], [m["id"] for m in received])


class TestAsyncWebSocketReconnect(unittest.IsolatedAsyncioTestCase):
    async def test_listen_reconnects_and_resubscribes(self):
        subscribes = []

        async def server_handler(conn):
            subscribes.append(json.loads(await conn.recv()))
            if len(subscribes) == 1:
                # Drop the first connection
                await conn.close(1011)
            else:
                # Wait for the heartbeat to resume on the new connection
                subscribes.append(json.loads(await conn.recv()))
                await conn.send(json.dumps({"message": TestAsyncWebSocketListen.message}))
                await conn.wait_closed()

        received = []
        async with serve(server_handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            ws = AsyncWebSocket(url=f"ws://127.0.0.1:{port}", verbose=False)
            ws._subscription_interval = 0.1
            await ws.subscribe("BTC-USD")
            task = asyncio.create_task(ws.listen(received.append))
            for _ in range(100):
                if received:
                    break
                await asyncio.sleep(0.1)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        self.assertEqual([{"subscribe": ["BTC-USD"]}] * 3, subscribes)
        self.assertEqual(["BTC-USD"], [m["id"] for m in received])
