import logging
import asyncio
from typing import (
    List,
    Optional,
)
import hummingbot.connector.exchange.hoo.hoo_constants as CONSTANTS
from hummingbot.connector.exchange.hoo.hoo_auth import HooAuth
from hummingbot.connector.utils import build_api_factory
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.rest_assistant import RESTAssistant
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class HooUserStreamDataSource(UserStreamTrackerDataSource):

    _hoo_usds_logger = None

    def __init__(self,
                 auth: HooAuth,
                 trading_pairs: Optional[List[str]] = None,
                 domain="",
                 api_factory: Optional[WebAssistantsFactory] = None,
                 throttler: Optional[AsyncThrottler] = None):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._domain = domain
        self._throttler = throttler or self._get_throttler_instance()
        self._api_factory = api_factory or build_api_factory()
        self._rest_assistant: Optional[RESTAssistant] = None
        self._ws_assistant: Optional[WSAssistant] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._hoo_usds_logger is None:
            cls._hoo_usds_logger = logging.getLogger(__name__)
        return cls._hoo_usds_logger

    @property
    def last_recv_time(self) -> float:
        if self._ws_assistant:
            return self._ws_assistant.last_recv_time
        return -1

    async def listen_for_user_stream(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Connects to the user private channel in the exchange using a websocket connection. With the established
        connection listens to all balance events and order updates provided by the exchange, and stores them in the
        output queue
        """
        ws = None
        while True:
            try:
                ws: WSAssistant = await self._get_ws_assistant()
                ws_url = CONSTANTS.WSS_URL if self._domain == "" else CONSTANTS.WSS_INNOVATE_URL
                await ws.connect(ws_url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)

                # subscribe orders
                for trading_pair in self._trading_pairs:
                    await ws.send({'op': 'sub', 'topic': f'orders:{trading_pair}'})

                # subscribe balances
                await ws.send({'op': 'sub', 'topic': 'accounts'})

                async for ws_response in ws.iter_messages():
                    data = ws_response.data
                    if data.get('topic') and (CONSTANTS.ACCOUNTS_BALANCE_TYPE in data['topic'] or CONSTANTS.ORDER_CHANGE_TYPE in data['topic']):
                        output.put_nowait(data)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error while listening to user stream. Retrying after 5 seconds...")
            finally:
                # Make sure no background task is leaked.
                ws and await ws.disconnect()
                await self._sleep(5)

    @classmethod
    def _get_throttler_instance(cls) -> AsyncThrottler:
        return AsyncThrottler(CONSTANTS.RATE_LIMITS)

    async def _get_rest_assistant(self) -> RESTAssistant:
        if self._rest_assistant is None:
            self._rest_assistant = await self._api_factory.get_rest_assistant()
        return self._rest_assistant

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant
