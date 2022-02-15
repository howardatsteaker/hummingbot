import asyncio
import logging
import time

from collections import defaultdict
from typing import (
    Any,
    Dict,
    List,
    Optional,
)
import ujson

import hummingbot.connector.exchange.hoo.hoo_constants as CONSTANTS
from hummingbot.connector.exchange.hoo import hoo_utils
from hummingbot.connector.exchange.hoo.hoo_order_book import HooOrderBook
from hummingbot.connector.utils import build_api_factory
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.connections.data_types import (
    RESTMethod,
    RESTRequest,
    RESTResponse,
    WSRequest,
)
from hummingbot.core.web_assistant.rest_assistant import RESTAssistant
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class HooOrderBookDataSource(OrderBookTrackerDataSource):

    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 domain="",
                 api_factory: Optional[WebAssistantsFactory] = None,
                 throttler: Optional[AsyncThrottler] = None):
        super().__init__(trading_pairs)
        self._order_book_create_function = lambda: OrderBook()
        self._domain = domain
        self._throttler = throttler or self._get_throttler_instance()
        self._api_factory = api_factory or build_api_factory()
        self._rest_assistant: Optional[RESTAssistant] = None
        self._ws_assistant: Optional[WSAssistant] = None
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @staticmethod
    async def fetch_trading_pairs(
            domain="",
            api_factory: Optional[WebAssistantsFactory] = None,
            throttler: Optional[AsyncThrottler] = None) -> List[str]:
        """
        Returns a list of all known trading pairs enabled to operate with
        :param domain: the domain of the exchange being used (either "com" or "us"). Default value is "com"
        :return: list of trading pairs in client notation
        """
        local_api_factory = api_factory or build_api_factory()
        rest_assistant = await local_api_factory.get_rest_assistant()
        local_throttler = throttler or HooOrderBookDataSource._get_throttler_instance()
        url = hoo_utils.rest_url(CONSTANTS.TICKERS_MARKET_URL, domain=domain)
        request = RESTRequest(method=RESTMethod.GET, url=url)

        try:
            async with local_throttler.execute_task(limit_id=CONSTANTS.TICKERS_MARKET_URL):
                response: RESTResponse = await rest_assistant.call(request)
                if response.status == 200:
                    resp_text = await response.text()
                    resp_json = ujson.loads(resp_text)
                    data: list = resp_json['data']
                    trading_pairs = [item['symbol'] for item in data]
        except Exception as ex:
            HooOrderBookDataSource.logger().error(f"There was an error requesting exchange info ({str(ex)})")
        else:
            return trading_pairs

    @classmethod
    async def get_last_traded_prices(cls,
                                     trading_pairs: List[str],
                                     domain: str = "",
                                     api_factory: Optional[WebAssistantsFactory] = None,
                                     throttler: Optional[AsyncThrottler] = None) -> Dict[str, float]:
        """
        Return a dictionary the trading_pair as key and the current price as value for each trading pair passed as
        parameter
        :param trading_pairs: list of trading pairs to get the prices for
        :param domain: which Binance domain we are connecting to (the default value is 'com')
        :param api_factory: the instance of the web assistant factory to be used when doing requests to the server.
        If no instance is provided then a new one will be created.
        :param throttler: the instance of the throttler to use to limit request to the server. If it is not specified
        the function will create a new one.
        :return: Dictionary of associations between token pair and its latest price
        """
        local_api_factory = api_factory or build_api_factory()
        rest_assistant = await local_api_factory.get_rest_assistant()
        local_throttler = throttler or cls._get_throttler_instance()
        tasks = [cls._get_last_traded_price(t_pair, domain, rest_assistant, local_throttler) for t_pair in trading_pairs]
        results = await safe_gather(*tasks)
        return {t_pair: result for t_pair, result in zip(trading_pairs, results)}

    @classmethod
    async def _get_last_traded_price(cls,
                                     trading_pair: str,
                                     domain: str,
                                     rest_assistant: RESTAssistant,
                                     throttler: AsyncThrottler) -> float:
        url = hoo_utils.rest_url(CONSTANTS.TRADE_MARKET_URL, domain=domain)
        request = RESTRequest(method=RESTMethod.GET, url=f"{url}?symbol={trading_pair}")

        try:
            async with throttler.execute_task(limit_id=CONSTANTS.TRADE_MARKET_URL):
                resp: RESTResponse = await rest_assistant.call(request)
                if resp.status == 200:
                    resp_text = await resp.text()
                    resp_json = ujson.loads(resp_text)
                    data: list = resp_json['data']
                    price = float(max(data, key=lambda item: item['time'])['price'])
        except Exception as ex:
            HooOrderBookDataSource.logger().error(f"There was an error requesting last traded price ({str(ex)})")
        else:
            return price

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        """
        Creates a local instance of the exchange order book for a particular trading pair
        :param trading_pair: the trading pair for which the order book has to be retrieved
        :return: a local copy of the current order book in the exchange
        """
        snapshot: Dict[str, Any] = await self.get_snapshot(trading_pair, 1000)
        snapshot_timestamp: float = time.time() * 1e3
        snapshot_msg: OrderBookMessage = HooOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        order_book = self.order_book_create_function()
        order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
        return order_book

    async def get_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.
        :param trading_pair: the trading pair for which the order book will be retrieved
        :return: the response from the exchange (JSON dictionary)
        """
        rest_assitant = await self._get_rest_assistant()
        params = {'symbol': trading_pair}
        url = hoo_utils.rest_url(path_url=CONSTANTS.DEPTH_URL, domain=self._domain)
        request = RESTRequest(method=RESTMethod.GET, url=url, params=params)

        async with self._throttler.execute_task(limit_id=CONSTANTS.DEPTH_URL):
            response: RESTResponse = rest_assitant.call(request)
            if response.status != 200:
                raise IOError(f"Error fetching market snapshot for {trading_pair}. "
                              f"Response: {response}.")
            data = await response.json()

        return data['data']

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Reads the order diffs events queue. For each event creates a diff message instance and adds it to the
        output queue
        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created diff messages
        """
        message_queue = self._message_queue[CONSTANTS.DEPTH_EVENT_TYPE]
        while True:
            try:
                json_msg = await message_queue.get()
                if "op" in json_msg:
                    continue
                order_book_message: OrderBookMessage = HooOrderBook.snapshot_message_from_exchange(
                    json_msg,
                    float(json_msg['timestamp']),
                    {"trading_pair": json_msg['symbol']})
                output.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public order book updates from exchange")

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        This method runs continuously and request the full order book content from the exchange every hour.
        The method uses the REST API from the exchange because it does not provide an endpoint to get the full order
        book through websocket. With the information creates a snapshot messages that is added to the output queue
        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created snapshot messages
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot: Dict[str, Any] = await self.get_snapshot(trading_pair=trading_pair)
                        snapshot_timestamp: float = time.time() * 1e3
                        snapshot_msg: OrderBookMessage = HooOrderBook.snapshot_message_from_exchange(
                            snapshot,
                            snapshot_timestamp,
                            metadata={"trading_pair": trading_pair}
                        )
                        output.put_nowait(snapshot_msg)
                        self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.logger().error(f"Unexpected error fetching order book snapshot for {trading_pair}.",
                                            exc_info=True)
                        await self._sleep(5.0)
                await self._sleep(self.ONE_HOUR)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await self._sleep(5.0)

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Reads the trade events queue. For each event creates a trade message instance and adds it to the output queue
        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created trade messages
        """
        message_queue = self._message_queue[CONSTANTS.TRADE_EVENT_TYPE]
        while True:
            try:
                json_msg = await message_queue.get()

                if "op" in json_msg:
                    continue
                trade_msg: OrderBookMessage = HooOrderBook.trade_message_from_exchange(json_msg)
                output.put_nowait(trade_msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public trade updates from exchange")

    async def listen_for_subscriptions(self):
        """
        Connects to the trade events and order diffs websocket endpoints and listens to the messages sent by the
        exchange. Each message is stored in its own queue.
        """
        ws = None
        while True:
            try:
                ws: WSAssistant = await self._get_ws_assistant()
                ws_url = CONSTANTS.WSS_URL if self._domain == "" else CONSTANTS.WSS_INNOVATE_URL

                await ws.connect(ws_url=ws_url,
                                 ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
                await self._subscribe_channels(ws)

                async for ws_response in ws.iter_messages():
                    data = ws_response.data
                    if "op" in data:
                        continue
                    topic = data.get("topic")
                    if CONSTANTS.DEPTH_EVENT_TYPE in topic:
                        self._message_queue[CONSTANTS.DEPTH_EVENT_TYPE].put_nowait(data)
                    elif CONSTANTS.TRADE_EVENT_TYPE in topic:
                        self._message_queue[CONSTANTS.TRADE_EVENT_TYPE].put_nowait(data)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                    exc_info=True,
                )
                await self._sleep(5.0)
            finally:
                ws and await ws.disconnect()

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            for trading_pair in self._trading_pairs:
                payload = {
                    "op": "sub",
                    "topic": f"trade:{trading_pair}"
                }
                subscribe_trade_request: WSRequest = WSRequest(payload=payload)

                payload = {
                    "op": "sub",
                    "topic": f"depth:0:{trading_pair}"
                }
                subscribe_orderbook_request: WSRequest = WSRequest(payload=payload)

                await ws.send(subscribe_trade_request)
                await ws.send(subscribe_orderbook_request)

            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    @classmethod
    def _get_throttler_instance(cls) -> AsyncThrottler:
        throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        return throttler

    async def _get_rest_assistant(self) -> RESTAssistant:
        if self._rest_assistant is None:
            self._rest_assistant = await self._api_factory.get_rest_assistant()
        return self._rest_assistant

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant
