"""WebSocket clients for Yahoo Finance live streaming quotes."""

import asyncio
import binascii
import base64
import json
from typing import Any, AsyncIterator, Callable, List, Optional, Protocol, Union, cast

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError, Message
from websockets.asyncio.client import connect as async_connect
from websockets.exceptions import WebSocketException
from websockets.sync.client import connect as sync_connect

from yfinance import utils
from yfinance.config import YF_CONFIG as YfConfig
from yfinance.pricing_pb2 import PricingData


class _AsyncWebSocketProtocol(Protocol):
    async def send(self, message: str) -> Any:
        """Send one message payload over the socket."""
        raise NotImplementedError

    async def close(self) -> Any:
        """Close the socket connection."""
        raise NotImplementedError

    def __aiter__(self) -> AsyncIterator[str]:
        """Yield inbound message payloads."""
        raise NotImplementedError


class _SyncWebSocketProtocol(Protocol):
    def send(self, message: str) -> Any:
        """Send one message payload over the socket."""
        raise NotImplementedError

    def recv(self) -> str:
        """Receive one message payload from the socket."""
        raise NotImplementedError

    def close(self) -> Any:
        """Close the socket connection."""
        raise NotImplementedError


class BaseWebSocket:
    """Shared functionality for sync and async Yahoo Finance websocket clients."""

    def __init__(self, url: str = "wss://streamer.finance.yahoo.com/?version=2", verbose=True):
        self.url = url
        self.verbose = verbose
        self.logger = utils.get_yf_logger()
        self._ws = None
        self._subscriptions = set()
        self._subscription_interval = 15  # seconds

    def subscriptions(self) -> List[str]:
        """Return the currently tracked subscriptions."""
        return list(self._subscriptions)

    def is_connected(self) -> bool:
        """Return whether a websocket instance has been established."""
        return self._ws is not None

    def _decode_message(self, base64_message: str) -> dict:
        try:
            decoded_bytes = base64.b64decode(base64_message)
            pricing_data = cast(Message, PricingData())
            pricing_data.ParseFromString(decoded_bytes)
            return cast(dict, MessageToDict(pricing_data, preserving_proto_field_name=True))
        except (binascii.Error, DecodeError, TypeError, ValueError) as error:
            if not YfConfig.debug.hide_exceptions:
                raise
            self.logger.error("Failed to decode message: %s", error, exc_info=True)
            if self.verbose:
                print(f"Failed to decode message: {error}")
            return {
                'error': str(error),
                'raw_base64': base64_message
            }

    def _decode_stream_payload(self, message: str) -> dict:
        """Decode one websocket payload into a pricing dictionary."""
        message_json = json.loads(message)
        encoded_data = message_json.get("message", "")
        return self._decode_message(encoded_data)


class AsyncWebSocket(BaseWebSocket):
    """
    Asynchronous WebSocket client for streaming real time pricing data.
    """

    def __init__(self, url: str = "wss://streamer.finance.yahoo.com/?version=2", verbose=True):
        """
        Initialize the AsyncWebSocket client.

        Args:
            url (str): The WebSocket server URL. Defaults to Yahoo Finance's WebSocket URL.
            verbose (bool): Flag to enable or disable print statements. Defaults to True.
        """
        super().__init__(url, verbose)
        self._ws: Optional[_AsyncWebSocketProtocol] = None
        self._message_handler = None  # Callable to handle messages
        self._heartbeat_task = None  # Task to send heartbeat subscribe

    async def _connect(self):
        try:
            if self._ws is None:
                self._ws = cast(_AsyncWebSocketProtocol, await async_connect(self.url))
                self.logger.info("Connected to WebSocket.")
                if self.verbose:
                    print("Connected to WebSocket.")
        except (OSError, WebSocketException) as error:
            if not YfConfig.debug.hide_exceptions:
                raise
            self.logger.error("Failed to connect to WebSocket: %s", error, exc_info=True)
            if self.verbose:
                print(f"Failed to connect to WebSocket: {error}")
            self._ws = None
            raise

    async def _periodic_subscribe(self):
        while True:
            try:
                await asyncio.sleep(self._subscription_interval)

                if self._subscriptions:
                    if self._ws is None:
                        raise RuntimeError("WebSocket is not connected.")
                    message = {"subscribe": list(self._subscriptions)}
                    await self._ws.send(json.dumps(message))

                    if self.verbose:
                        print(f"Heartbeat subscription sent for symbols: {self._subscriptions}")
            except (OSError, RuntimeError, TypeError, ValueError) as error:
                if not YfConfig.debug.hide_exceptions:
                    raise
                self.logger.error("Error in heartbeat subscription: %s", error, exc_info=True)
                if self.verbose:
                    print(f"Error in heartbeat subscription: {error}")
                break

    async def subscribe(self, symbols: Union[str, List[str]]):
        """
        Subscribe to a stock symbol or a list of stock symbols.

        Args:
            symbols (Union[str, List[str]]): Stock symbol(s) to subscribe to.
        """
        await self._connect()

        if isinstance(symbols, str):
            symbols = [symbols]

        self._subscriptions.update(symbols)

        if self._ws is None:
            raise RuntimeError("WebSocket is not connected.")
        message = {"subscribe": list(self._subscriptions)}
        await self._ws.send(json.dumps(message))

        # Start heartbeat subscription task
        if self._heartbeat_task is None:
            self._heartbeat_task = asyncio.create_task(self._periodic_subscribe())

        self.logger.info("Subscribed to symbols: %s", symbols)
        if self.verbose:
            print(f"Subscribed to symbols: {symbols}")

    async def unsubscribe(self, symbols: Union[str, List[str]]):
        """
        Unsubscribe from a stock symbol or a list of stock symbols.

        Args:
            symbols (Union[str, List[str]]): Stock symbol(s) to unsubscribe from.
        """
        await self._connect()

        if isinstance(symbols, str):
            symbols = [symbols]

        self._subscriptions.difference_update(symbols)

        if self._ws is None:
            raise RuntimeError("WebSocket is not connected.")
        message = {"unsubscribe": symbols}
        await self._ws.send(json.dumps(message))

        self.logger.info("Unsubscribed from symbols: %s", symbols)
        if self.verbose:
            print(f"Unsubscribed from symbols: {symbols}")

    async def _process_async_message(self, message: str):
        """Decode and dispatch one async stream message."""
        decoded_message = self._decode_stream_payload(message)

        if self._message_handler:
            try:
                if asyncio.iscoroutinefunction(self._message_handler):
                    await self._message_handler(decoded_message)
                else:
                    self._message_handler(decoded_message)
            except (RuntimeError, TypeError, ValueError) as handler_error:
                if not YfConfig.debug.hide_exceptions:
                    raise
                self.logger.error(
                    "Error in message handler: %s",
                    handler_error,
                    exc_info=True,
                )
                if self.verbose:
                    print("Error in message handler:", handler_error)
            return

        print(decoded_message)

    async def _recover_async_listener(self, error: Exception):
        """Log listener failures and reconnect after a backoff."""
        self.logger.error("Error while listening to messages: %s", error, exc_info=True)
        if self.verbose:
            print(f"Error while listening to messages: {error}")

        self.logger.info("Attempting to reconnect...")
        if self.verbose:
            print("Attempting to reconnect...")
        await asyncio.sleep(3)
        await self._connect()

    async def listen(self, message_handler=None):
        """
        Start listening to messages from the WebSocket server.

        Args:
            message_handler (Optional[Callable[[dict], None]]):
                Optional function to handle received messages.
        """
        await self._connect()
        self._message_handler = message_handler

        self.logger.info("Listening for messages...")
        if self.verbose:
            print("Listening for messages...")

        # Start heartbeat subscription task
        if self._heartbeat_task is None:
            self._heartbeat_task = asyncio.create_task(self._periodic_subscribe())

        if self._ws is None:
            raise RuntimeError("WebSocket is not connected.")
        while True:
            try:
                async for message in self._ws:
                    await self._process_async_message(message)

            except (KeyboardInterrupt, asyncio.CancelledError):
                self.logger.info("WebSocket listening interrupted. Closing connection...")
                if self.verbose:
                    print("WebSocket listening interrupted. Closing connection...")
                await self.close()
                break

            except (json.JSONDecodeError, OSError, RuntimeError, TypeError, ValueError) as error:
                if not YfConfig.debug.hide_exceptions:
                    raise
                await self._recover_async_listener(error)

    async def close(self):
        """Close the WebSocket connection."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

        if self._ws is not None:  # and not self._ws.closed:
            await self._ws.close()
            self.logger.info("WebSocket connection closed.")
            if self.verbose:
                print("WebSocket connection closed.")

    async def __aenter__(self):
        await self._connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()


class WebSocket(BaseWebSocket):
    """
    Synchronous WebSocket client for streaming real time pricing data.
    """

    def __init__(self, url: str = "wss://streamer.finance.yahoo.com/?version=2", verbose=True):
        """
        Initialize the WebSocket client.

        Args:
            url (str): The WebSocket server URL. Defaults to Yahoo Finance's WebSocket URL.
            verbose (bool): Flag to enable or disable print statements. Defaults to True.
        """
        super().__init__(url, verbose)
        self._ws: Optional[_SyncWebSocketProtocol] = None

    def _connect(self):
        try:
            if self._ws is None:
                self._ws = cast(_SyncWebSocketProtocol, sync_connect(self.url))
                self.logger.info("Connected to WebSocket.")
                if self.verbose:
                    print("Connected to WebSocket.")
        except (OSError, WebSocketException) as error:
            self.logger.error("Failed to connect to WebSocket: %s", error, exc_info=True)
            if self.verbose:
                print(f"Failed to connect to WebSocket: {error}")
            self._ws = None
            raise

    def subscribe(self, symbols: Union[str, List[str]]):
        """
        Subscribe to a stock symbol or a list of stock symbols.

        Args:
            symbols (Union[str, List[str]]): Stock symbol(s) to subscribe to.
        """
        self._connect()

        if isinstance(symbols, str):
            symbols = [symbols]

        self._subscriptions.update(symbols)

        if self._ws is None:
            raise RuntimeError("WebSocket is not connected.")
        message = {"subscribe": list(self._subscriptions)}
        self._ws.send(json.dumps(message))

        self.logger.info("Subscribed to symbols: %s", symbols)
        if self.verbose:
            print(f"Subscribed to symbols: {symbols}")

    def unsubscribe(self, symbols: Union[str, List[str]]):
        """
        Unsubscribe from a stock symbol or a list of stock symbols.

        Args:
            symbols (Union[str, List[str]]): Stock symbol(s) to unsubscribe from.
        """
        self._connect()

        if isinstance(symbols, str):
            symbols = [symbols]

        self._subscriptions.difference_update(symbols)

        if self._ws is None:
            raise RuntimeError("WebSocket is not connected.")
        message = {"unsubscribe": symbols}
        self._ws.send(json.dumps(message))

        self.logger.info("Unsubscribed from symbols: %s", symbols)
        if self.verbose:
            print(f"Unsubscribed from symbols: {symbols}")

    def _process_sync_message(
        self,
        message: str,
        message_handler: Optional[Callable[[dict], None]],
    ):
        """Decode and dispatch one sync stream message."""
        decoded_message = self._decode_stream_payload(message)

        if message_handler:
            try:
                message_handler(decoded_message)
            except (RuntimeError, TypeError, ValueError) as handler_error:
                if not YfConfig.debug.hide_exceptions:
                    raise
                self.logger.error(
                    "Error in message handler: %s",
                    handler_error,
                    exc_info=True,
                )
                if self.verbose:
                    print("Error in message handler:", handler_error)
            return

        print(decoded_message)

    def listen(self, message_handler: Optional[Callable[[dict], None]] = None):
        """
        Start listening to messages from the WebSocket server.

        Args:
            message_handler (Optional[Callable[[dict], None]]):
                Optional function to handle received messages.
        """
        self._connect()

        self.logger.info("Listening for messages...")
        if self.verbose:
            print("Listening for messages...")

        while True:
            try:
                if self._ws is None:
                    raise RuntimeError("WebSocket is not connected.")
                message = self._ws.recv()
                self._process_sync_message(message, message_handler)

            except KeyboardInterrupt:
                if self.verbose:
                    print("Received keyboard interrupt.")
                self.close()
                break

            except (json.JSONDecodeError, OSError, RuntimeError, TypeError, ValueError) as error:
                if not YfConfig.debug.hide_exceptions:
                    raise
                self.logger.error("Error while listening to messages: %s", error, exc_info=True)
                if self.verbose:
                    print(f"Error while listening to messages: {error}")
                break

    def close(self):
        """Close the WebSocket connection."""
        if self._ws is not None:
            self._ws.close()
            self.logger.info("WebSocket connection closed.")
            if self.verbose:
                print("WebSocket connection closed.")

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
