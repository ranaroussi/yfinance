"""Synchronous websocket example."""

import yfinance as yf


def message_handler(message):
    """Handle decoded websocket messages."""
    print("Received message:", message)

# =======================
# With Context Manager
# =======================
with yf.WebSocket() as ws:
    ws.subscribe(["AAPL", "BTC-USD"])
    ws.listen(message_handler)

# =======================
# Without Context Manager
# =======================
ws = yf.WebSocket()
ws.subscribe(["AAPL", "BTC-USD"])
ws.listen(message_handler)
