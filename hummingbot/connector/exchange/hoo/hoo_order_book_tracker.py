import asyncio
import logging
from collections import deque, defaultdict
from typing import (
    Deque,
    Dict,
    List,
    Optional
)

from hummingbot.connector.exchange.hoo.hoo_order_book_data_source import HooOrderBookDataSource
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.logger.logger import HummingbotLogger


class HooOrderBookTracker(OrderBookTracker):
    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: Optional[List[str]] = None,
                 domain: str = "",
                 api_factory: Optional[WebAssistantsFactory] = None,
                 throttler: Optional[AsyncThrottler] = None):
        super().__init__(
            data_source=HooOrderBookDataSource(
                trading_pairs=trading_pairs,
                domain=domain,
                api_factory=api_factory,
                throttler=throttler),
            trading_pairs=trading_pairs,
            domain=domain
        )
        self._domain = domain
        self._order_book_stream_listener_task: Optional[asyncio.Task] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @property
    def exchange_name(self) -> str:
        if self._domain == "":
            return "hoo"
        else:
            return "hoo_innovate"

    def start(self):
        """
        Starts the background task that connects to the exchange and listens to order book updates and trade events.
        """
        self.stop()
        super().start()
        self._order_book_stream_listener_task = safe_ensure_future(
            self._data_source.listen_for_subscriptions()
        )

    def stop(self):
        """
        Stops the background task
        """
        if self._order_book_stream_listener_task is not None:
            self._order_book_stream_listener_task.cancel()
            self._order_book_stream_listener_task = None
        super().stop()
