import asyncio
import yfinance as yf

# define your message callback
def message_handler(message):
    print("Received message:", message)

async def main():
    client = yf.AsyncWebSocket()
    await client.subscribe(["AAPL", "BTC-USD"])
    await client.listen()

asyncio.run(main())
