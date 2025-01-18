import yfinance as yf

# define your message callback
def message_handler(message):
    print("Received message:", message)

client = yf.WebSocket()
client.subscribe(["AAPL", "BTC-USD"])
client.listen(message_handler)
