import asyncio
import logging
import math
import time

from decimal import Decimal
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional,
)

import hummingbot.connector.exchange.hoo.hoo_constants as CONSTANTS
from hummingbot.connector.exchange.hoo.hoo_in_flight_order import HooInFlightOrder, HooOrderUpdate
from hummingbot.connector.exchange.hoo.hoo_order_tracker import HooOrderTracker
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.exchange.hoo import hoo_utils
from hummingbot.connector.exchange.hoo.hoo_order_book_tracker import HooOrderBookTracker
from hummingbot.connector.exchange.hoo.hoo_auth import HooAuth
from hummingbot.connector.exchange.hoo.hoo_user_stream_tracker import HooUserStreamTracker
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, OrderState
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.rest_assistant import RESTAssistant
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import (
    OrderType,
    TradeType,
)
from hummingbot.logger import HummingbotLogger

s_logger = None
s_decimal_0 = Decimal(0)
s_decimal_NaN = Decimal("nan")


class HooExchange(ExchangeBase):
    SHORT_POLL_INTERVAL = 5.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    LONG_POLL_INTERVAL = 120.0

    MAX_ORDER_UPDATE_RETRIEVAL_RETRIES_WITH_FAILURES = 3


    def __init__(self,
                 hoo_api_key: str,
                 hoo_api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain=""
                 ):
        self._domain = domain
        super().__init__()
        self._trading_required = trading_required
        self._auth = HooAuth(
            api_key=hoo_api_key,
            secret_key=hoo_api_secret)
        self._api_factory = WebAssistantsFactory(auth=self._auth)
        self._rest_assistant = None
        self._throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        self._order_book_tracker = HooOrderBookTracker(
            trading_pairs,
            domain,
            self._api_factory,
            self._throttler)
        self._user_stream_tracker = HooUserStreamTracker(
            auth=self._auth,
            trading_pairs=trading_pairs,
            domain=domain,
            throttler=self._throttler)
        self._ev_loop = asyncio.get_event_loop()
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._order_not_found_records = {}  # Dict[client_order_id:str, count:int]
        self._trading_rules = {}  # Dict[trading_pair:str, TradingRule]
        self._trade_fees = {}  # Dict[trading_pair:str, (maker_fee_percent:Decimal, taken_fee_percent:Decimal)]
        self._last_update_trade_fees_timestamp = 0
        self._status_polling_task = None
        self._user_stream_event_listener_task = None
        self._trading_rules_polling_task = None
        self._last_poll_timestamp = 0
        self._last_trades_poll_binance_timestamp = 0
        self._order_tracker: HooOrderTracker = HooOrderTracker(connector=self)

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def trading_rules(self) -> Dict[str, TradingRule]:
        return self._trading_rules

    @property
    def in_flight_orders(self) -> Dict[str, InFlightOrder]:
        return self._order_tracker.active_orders

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self.in_flight_orders.values()
        ]

    @property
    def tracking_states(self) -> Dict[str, any]:
        """
        Returns a dictionary associating current active orders client id to their JSON representation
        """
        return {
            key: value.to_json()
            for key, value in self.in_flight_orders.items()
        }

    @property
    def order_book_tracker(self) -> HooOrderBookTracker:
        return self._order_book_tracker

    @property
    def user_stream_tracker(self) -> HooUserStreamTracker:
        return self._user_stream_tracker

    @property
    def status_dict(self) -> Dict[str, bool]:
        """
        Returns a dictionary with the values of all the conditions that determine if the connector is ready to operate.
        The key of each entry is the condition name, and the value is True if condition is ready, False otherwise.
        """
        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0,
        }

    @property
    def ready(self) -> bool:
        """
        Returns True if the connector is ready to operate (all connections established with the exchange). If it is
        not ready it returns False.
        """
        return all(self.status_dict.values())

    async def start_network(self):
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    async def stop_network(self):
        """
        This function is executed when the connector is stopped. It perform a general cleanup and stops all background
        tasks that require the connection with the exchange to work.
        """
        # Reset timestamps and _poll_notifier for status_polling_loop
        self._last_poll_timestamp = 0
        self._last_timestamp = 0
        self._poll_notifier = asyncio.Event()

        self._order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
        if self._trading_rules_polling_task is not None:
            self._trading_rules_polling_task.cancel()
        self._status_polling_task = self._user_stream_tracker_task = self._user_stream_event_listener_task = None

    async def check_network(self) -> NetworkStatus:
        """
        Checks connectivity with the exchange using the API
        """
        try:
            await self._api_request(
                method=RESTMethod.GET,
                path_url=CONSTANTS.SERVER_TIMESTAMP_URL,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        """
        Restore in-flight orders from saved tracking states, this is st the connector can pick up on where it left off
        when it disconnects.
        :param saved_states: The saved tracking_states.
        """
        self._order_tracker.restore_tracking_states(tracking_states=saved_states)

    def tick(self, timestamp: float):
        """
        Includes the logic that has to be processed every time a new tick happens in the bot. Particularly it enables
        the execution of the status update polling loop using an event.
        """
        now = time.time()
        poll_interval = (self.SHORT_POLL_INTERVAL
                         if now - self.user_stream_tracker.last_recv_time > 60.0
                         else self.LONG_POLL_INTERVAL)
        last_tick = int(self._last_timestamp / poll_interval)
        current_tick = int(timestamp / poll_interval)

        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    def get_order_book(self, trading_pair: str) -> OrderBook:
        """
        Returns the current order book for a particular market
        :param trading_pair: the pair of tokens for which the order book should be retrieved
        """
        if trading_pair not in self._order_book_tracker.order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return self._order_book_tracker.order_books[trading_pair]

    def start_tracking_order(self,
                             order_id: str,
                             exchange_order_id: Optional[str],
                             trade_no: Optional[str],
                             trading_pair: str,
                             trade_type: TradeType,
                             price: Decimal,
                             amount: Decimal,
                             order_type: OrderType):
        """
        Starts tracking an order by adding it to the order tracker.
        :param order_id: the order identifier
        :param exchange_order_id: the identifier for the order in the exchange
        :param trading_pair: the token pair for the operation
        :param trade_type: the type of order (buy or sell)
        :param price: the price for the order
        :param amount: the amount for the order
        :order type: type of execution for the order (MARKET, LIMIT, LIMIT_MAKER)
        """
        self._order_tracker.start_tracking_order(
            HooInFlightOrder(
                client_order_id=order_id,
                exchange_order_id=exchange_order_id,
                trade_no=trade_no,
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=trade_type,
                amount=amount,
                price=price,
            )
        )

    def stop_tracking_order(self, order_id: str):
        """
        Stops tracking an order
        :param order_id: The id of the order that will not be tracked any more
        """
        self._order_tracker.stop_tracking_order(client_order_id=order_id)

    def get_order_price_quantum(self, trading_pair: str, price: Decimal) -> Decimal:
        """
        Used by quantize_order_price() in _create_order()
        Returns a price step, a minimum price increment for a given trading pair.
        :param trading_pair: the trading pair to check for market conditions
        :param price: the starting point price
        """
        trading_rule = self._trading_rules[trading_pair]
        return trading_rule.min_price_increment

    def get_order_size_quantum(self, trading_pair: str, order_size: Decimal) -> Decimal:
        """
        Used by quantize_order_price() in _create_order()
        Returns an order amount step, a minimum amount increment for a given trading pair.
        :param trading_pair: the trading pair to check for market conditions
        :param order_size: the starting point order price
        """
        trading_rule = self._trading_rules[trading_pair]
        return trading_rule.min_base_amount_increment

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN,
                is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        """
        Calculates the estimated fee an order would pay based on the connector configuration
        :param base_currency: the order base currency
        :param quote_currency: the order quote currency
        :param order_type: the type of order (MARKET, LIMIT, LIMIT_MAKER)
        :param order_side: if the order is for buying or selling
        :param amount: the order amount
        :param price: the order price
        :return: the estimated fee for the order
        """

        """
        To get trading fee, this function is simplified by using fee override configuration. Most parameters to this
        function are ignore except order_type. Use OrderType.LIMIT_MAKER to specify you want trading fee for
        maker order.
        """
        is_maker = order_type is OrderType.LIMIT_MAKER
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.LIMIT,
            price: Decimal = s_decimal_NaN, **kwargs) -> str:
        new_order_id = hoo_utils.get_new_client_order_id(is_buy=True, trading_pair=trading_pair)
        safe_ensure_future(self._create_order(TradeType.BUY, new_order_id, trading_pair, amount, order_type, price))
        return new_order_id

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.LIMIT,
             price: Decimal = s_decimal_NaN, **kwargs) -> str:
        order_id = hoo_utils.get_new_client_order_id(is_buy=False, trading_pair=trading_pair)
        safe_ensure_future(self._create_order(TradeType.SELL, order_id, trading_pair, amount, order_type, price))
        return order_id

    def cancel(self, trading_pair: str, client_order_id: str):
        safe_ensure_future(self._execute_cancel(trading_pair, client_order_id))
        return client_order_id

    async def _create_order(self,
                            trade_type: TradeType,
                            order_id: str,
                            trading_pair: str,
                            amount: Decimal,
                            order_type: OrderType,
                            price: Optional[Decimal] = Decimal("NaN")):
        """
        Creates a an order in the exchange using the parameters to configure it
        :param trade_type: the side of the order (BUY of SELL)
        :param order_id: the id that should be assigned to the order (the client id)
        :param trading_pair: the token pair to operate with
        :param amount: the order amount
        :param order_type: the type of order to create (MARKET, LIMIT, LIMIT_MAKER)
        :param price: the order price
        """
        price = self.quantize_order_price(trading_pair, price)
        amount = self.quantize_order_amount(trading_pair, amount)

        self.start_tracking_order(
            order_id=order_id,
            exchange_order_id=None,
            trading_pair=trading_pair,
            trade_type=trade_type,
            price=price,
            amount=amount,
            order_type=order_type)

        order_result = None
        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        
        api_params = {"symbol": trading_pair,
                      "side": side_str,
                      "quantity": f"{amount:f}",
                      "price": f"{price:f}"}

        try:
            order_result = await self._api_request(
                method=RESTMethod.POST,
                path_url=CONSTANTS.ORDER_PLACE_URL,
                data=api_params,
                is_auth_required=True)

            order_result = order_result['data']

            exchange_order_id = str(order_result["order_id"])
            trade_no = str(order_result['trade_no'])

            order_update: HooOrderUpdate = HooOrderUpdate(
                client_order_id=order_id,
                exchange_order_id=exchange_order_id,
                trade_no=trade_no,
                trading_pair=trading_pair,
                update_timestamp=int(self.current_timestamp * 1e3),
                new_state=OrderState.OPEN,
            )
            self._order_tracker.process_order_update(order_update)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().network(
                f"Error submitting {side_str} limit order to Hoo for "
                f"{amount} {trading_pair} "
                f"{price}.",
                exc_info=True,
                app_warning_msg=str(e)
            )
            order_update: HooOrderUpdate = HooOrderUpdate(
                client_order_id=order_id,
                trading_pair=trading_pair,
                update_timestamp=int(self.current_timestamp * 1e3),
                new_state=OrderState.FAILED,
            )
            self._order_tracker.process_order_update(order_update)

    async def _execute_cancel(self, trading_pair: str, order_id: str):
        """
        Requests the exchange to cancel an active order
        :param trading_pair: the trading pair the order to cancel operates with
        :param order_id: the client id of the order to cancel
        """
        tracked_order = self._order_tracker.fetch_tracked_order(order_id)
        if tracked_order is not None:
            try:
                api_params = {
                    "symbol": tracked_order.trading_pair,
                    "order_id": order_id,
                    "trade_no": tracked_order.trade_no
                }
                cancel_result = await self._api_request(
                    method=RESTMethod.POST,
                    path_url=CONSTANTS.ORDER_CANCEL_URL,
                    params=api_params,
                    is_auth_required=True)

                if cancel_result.get("msg") == "ok":
                    order_update: OrderUpdate = OrderUpdate(
                        client_order_id=order_id,
                        trading_pair=tracked_order.trading_pair,
                        update_timestamp=int(self.current_timestamp * 1e3),
                        new_state=OrderState.CANCELLED,
                    )
                    self._order_tracker.process_order_update(order_update)
                return cancel_result

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(f"There was a an error when requesting cancellation of order {order_id}")
                raise

    async def _trading_rules_polling_loop(self):
        """
        Updates the trading rules by requesting the latest definitions from the exchange.
        Executes regularly every 30 minutes
        """
        while True:
            try:
                await safe_gather(
                    self._update_trading_rules(),
                )
                await asyncio.sleep(30 * 60)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching trading rules.", exc_info=True,
                                      app_warning_msg="Could not fetch new trading rules from Binance. "
                                                      "Check network connection.")
                await asyncio.sleep(0.5)

    async def _update_trading_rules(self):
        exchange_info = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKERS_MARKET_URL)
        trading_rules_list = await self._format_trading_rules(exchange_info)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Example:
        {  
            'code': 0, 
            'data': [
                    {'amount': '1.586',
                    'change': '-0.235462',
                    'high': '3.05',
                    'low': '3.05',
                    'price': '0',
                    'symbol': 'EOS-USDT',
                    'amt_num': 4,
                    'qty_num': 2,
                    'volume': '0.52'
                    }, 
                ]
        }
        """
        trading_pair_rules = exchange_info_dict.get("data", [])
        retval = []
        for rule in trading_pair_rules:
            try:
                trading_pair = rule['symbol']
                price_decimals = Decimal(str(rule['amt_num']))
                size_decimals = Decimal(str(rule['qty_num']))
                price_step = Decimal('1') / Decimal(str(math.pow(10, price_decimals)))
                size_step = Decimal('1') / Decimal(str(math.pow(10, size_decimals)))

                retval.append(
                    TradingRule(trading_pair,
                                min_price_increment=Decimal(price_step),
                                min_base_amount_increment=Decimal(size_step)))

            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")
        return retval
    
    async def _status_polling_loop(self):
        """
        Performs all required operation to keep the connector updated and synchronized with the exchange.
        It contains the backup logic to update status using API requests in case the main update source (the user stream
        data source websocket) fails.
        Executes when the _poll_notifier event is enabled by the `tick` function.
        """
        while True:
            try:
                await self._poll_notifier.wait()
                await self._update_balances()
                await self._update_order_status()
                self._last_poll_timestamp = self.current_timestamp
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching account updates.", exc_info=True,
                                      app_warning_msg="Could not fetch account updates from Hoo. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(0.5)
            finally:
                self._poll_notifier = asyncio.Event()

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                topic: str = event_message.get("topic")
                if topic == CONSTANTS.ACCOUNTS_BALANCE_TYPE:
                    asset_name = event_message['symbol']
                    free_balance = Decimal(event_message['available'])
                    total_balance = Decimal(event_message['total'])
                    self._account_available_balances[asset_name] = free_balance
                    self._account_balances[asset_name] = total_balance
                elif topic.startswith(CONSTANTS.ORDER_CHANGE_TYPE):
                    exchange_order_id = event_message['order_id']
                    tracked_order = self._order_tracker.fetch_order(exchange_order_id=exchange_order_id)
                    if tracked_order is not None:
                        order_update = OrderUpdate(
                            trading_pair=event_message['symbol'],
                            update_timestamp=event_message['timestamp'],
                            new_state=CONSTANTS.OrderState[event_message['status']],
                            client_order_id=tracked_order.client_order_id,
                            exchange_order_id=exchange_order_id,
                            fill_price=Decimal(event_message['match_price']) if event_message['match_price'] != "0" else None,
                            executed_amount_base=Decimal(event_message['match_qty']),
                        )
                        self._order_tracker.process_order_update(order_update=order_update)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await asyncio.sleep(5.0)

    async def _update_order_status(self):
        # This is intended to be a backup measure to close straggler orders, in case Hoo's user stream events
        # are not working.
        # The minimum poll interval for order status is 10 seconds.
        last_tick = self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
        current_tick = self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL

        tracked_orders: List[InFlightOrder] = list(self.in_flight_orders.values())
        if current_tick > last_tick and len(tracked_orders) > 0:

            tasks = [self._api_request(
                     method=RESTMethod.GET,
                     path_url=CONSTANTS.ORDERS_DETAIL_URL,
                     params={
                         "symbol": o.trading_pair,
                         "order_id": o.exchange_order_id},
                     is_auth_required=True) for o in tracked_orders]
            self.logger().debug(f"Polling for order status updates of {len(tasks)} orders.")
            results = await safe_gather(*tasks, return_exceptions=True)
            for order_update, tracked_order in zip(results, tracked_orders):
                client_order_id = tracked_order.client_order_id

                # If the order has already been cancelled or has failed do nothing
                if client_order_id not in self.in_flight_orders:
                    continue

                if isinstance(order_update, Exception):
                    self.logger().network(
                        f"Error fetching status update for the order {client_order_id}: {order_update}.",
                        app_warning_msg=f"Failed to fetch status update for the order {client_order_id}."
                    )
                    self._order_not_found_records[client_order_id] = (
                        self._order_not_found_records.get(client_order_id, 0) + 1)
                    if (self._order_not_found_records[client_order_id] >=
                            self.MAX_ORDER_UPDATE_RETRIEVAL_RETRIES_WITH_FAILURES):
                        # Wait until the order not found error have repeated a few times before actually treating
                        # it as failed. See: https://github.com/CoinAlpha/hummingbot/issues/601

                        order_update: OrderUpdate = OrderUpdate(
                            client_order_id=client_order_id,
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=int(self.current_timestamp * 1e3),
                            new_state=OrderState.FAILED,
                        )
                        self._order_tracker.process_order_update(order_update)

                else:
                    # Update order execution status
                    order_data = order_update['data']
                    new_state = CONSTANTS.ORDER_STATE[order_data["status"]]

                    update = OrderUpdate(
                        client_order_id=client_order_id,
                        exchange_order_id=str(order_data["order_id"]),
                        trading_pair=tracked_order.trading_pair,
                        update_timestamp=int(self.current_timestamp * 1e3),
                        new_state=new_state,
                    )
                    self._order_tracker.process_order_update(update)

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from Binance. Check API key and network connection."
                )
                await asyncio.sleep(1.0)

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        try:
            account_info = await self._api_request(
                method=RESTMethod.GET,
                path_url=CONSTANTS.BALANCE_URL,
                is_auth_required=True)

            balances = account_info.get('data', [])
            for balance_entry in balances:
                asset_name = balance_entry["symbol"]
                free_balance = Decimal(balance_entry["amount"])
                total_balance = Decimal(balance_entry["amount"]) + Decimal(balance_entry["freeze"])
                self._account_available_balances[asset_name] = free_balance
                self._account_balances[asset_name] = total_balance
                remote_asset_names.add(asset_name)

            asset_names_to_remove = local_asset_names.difference(remote_asset_names)
            for asset_name in asset_names_to_remove:
                del self._account_available_balances[asset_name]
                del self._account_balances[asset_name]
        except IOError:
            self.logger().exception("Error getting account balances from server")

    async def _api_request(self,
                           method: RESTMethod,
                           path_url: str,
                           params: Optional[Dict[str, Any]] = None,
                           data: Optional[Dict[str, Any]] = None,
                           is_auth_required: bool = False) -> Dict[str, Any]:
        client = await self._get_rest_assistant()
        url = hoo_utils.rest_url(path_url, self._domain)

        request = RESTRequest(method=method,
                              url=url,
                              data=data,
                              params=params,
                              is_auth_required=is_auth_required)

        async with self._throttler.execute_task(limit_id=path_url):
            response = await client.call(request)

            if response.status != 200:
                data = await response.text()
                raise IOError(f"Error fetching data from {url}. HTTP status is {response.status} ({data}).")
            try:
                parsed_response = await response.json()
            except Exception:
                raise IOError(f"Error parsing data from {response}.")

            if "code" in parsed_response and "msg" in parsed_response and parsed_response['code'] != 0:
                raise IOError(f"The request to Hoo failed. Error: {parsed_response}. Request: {request}")

        return parsed_response

    async def _get_rest_assistant(self) -> RESTAssistant:
        if self._rest_assistant is None:
            self._rest_assistant = await self._api_factory.get_rest_assistant()
        return self._rest_assistant