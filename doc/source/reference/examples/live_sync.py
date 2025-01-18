import yfinance as yf

# define your message callback
def message_handler(message):
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
