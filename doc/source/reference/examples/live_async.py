"""Async websocket example."""

import asyncio

import yfinance as yf


def message_handler(message):
    """Handle decoded websocket messages."""
    print("Received message:", message)


async def main():
    """Connect to the async websocket and start listening."""
    # =======================
    # With Context Manager
    # =======================
    async with yf.AsyncWebSocket() as ws:
        await ws.subscribe(["AAPL", "BTC-USD"])
        await ws.listen()

    # =======================
    # Without Context Manager
    # =======================
    ws = yf.AsyncWebSocket()
    await ws.subscribe(["AAPL", "BTC-USD"])
    await ws.listen()


asyncio.run(main())
