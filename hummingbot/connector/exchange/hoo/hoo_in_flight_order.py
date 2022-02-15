import copy
from decimal import Decimal
from typing import (
    Any,
    NamedTuple,
    Tuple,
    Optional,
)
from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate

s_decimal_0 = Decimal("0")


class HooOrderUpdate(NamedTuple):
    trading_pair: str
    update_timestamp: int  # milliseconds
    new_state: OrderState
    client_order_id: Optional[str] = None
    exchange_order_id: Optional[str] = None
    trade_no: Optional[str] = None
    trade_id: Optional[str] = None
    fill_price: Optional[Decimal] = None  # If None, defaults to order price
    executed_amount_base: Optional[Decimal] = None
    executed_amount_quote: Optional[Decimal] = None
    fee_asset: Optional[str] = None
    cumulative_fee_paid: Optional[Decimal] = None
    trade_fee_percent: Optional[Decimal] = None


class HooInFlightOrder(InFlightOrder):
    
    def __init__(self,
                 client_order_id: str,
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 amount: Decimal,
                 price: Optional[Decimal] = None,
                 exchange_order_id: Optional[str] = None,
                 trade_no: Optional[str] = None,
                 initial_state: OrderState = OrderState.PENDING_CREATE):
        super().__init__(client_order_id,
                         trading_pair,
                         order_type,
                         trade_type,
                         amount,
                         price,
                         exchange_order_id,
                         initial_state)
        self.trade_no = trade_no

    def update_trade_no(self, trade_no: str):
        self.trade_no = trade_no

    def update_with_order_update(self, order_update: HooOrderUpdate) -> bool:
        """
        Updates the in flight order with an order update (from REST API or WS API)
        return: True if the order gets updated otherwise False
        """
        if order_update.client_order_id != self.client_order_id and order_update.exchange_order_id != self.exchange_order_id:
            return False

        if self.exchange_order_id is None:
            self.update_exchange_order_id(order_update.exchange_order_id)

        self.update_trade_no(order_update.trade_no)

        updated = False
        prev_order_state: Tuple[Any] = self.attributes
        prev_executed_amount_base = copy.deepcopy(self.executed_amount_base)
        prev_cumulative_fee_paid = copy.deepcopy(self.cumulative_fee_paid)

        self.current_state = order_update.new_state
        if order_update.executed_amount_base:
            self.executed_amount_base = order_update.executed_amount_base
        if order_update.executed_amount_quote:
            self.executed_amount_quote = order_update.executed_amount_quote
        if order_update.cumulative_fee_paid:
            self.cumulative_fee_paid = order_update.cumulative_fee_paid
        if not self.fee_asset and order_update.fee_asset:
            self.fee_asset = order_update.fee_asset

        updated: bool = prev_order_state != self.attributes

        if updated:
            self.last_update_timestamp = order_update.update_timestamp
            if order_update.new_state in {OrderState.OPEN, OrderState.CANCELLED, OrderState.FAILED}:
                return True

            if self.executed_amount_base > prev_executed_amount_base:
                self.last_filled_price = order_update.fill_price or self.price
                self.last_filled_amount = (
                    order_update.executed_amount_base
                    if prev_executed_amount_base == s_decimal_0
                    else order_update.executed_amount_base - prev_executed_amount_base
                )
                self.last_fee_paid = (
                    order_update.cumulative_fee_paid
                    if prev_cumulative_fee_paid == s_decimal_0
                    else order_update.cumulative_fee_paid - prev_cumulative_fee_paid
                )
                # trade_id defaults to update timestamp if not provided
                trade_id: str = order_update.trade_id or order_update.update_timestamp
                self.last_trade_id = trade_id
                self.order_fills[trade_id] = TradeUpdate(
                    trade_id=trade_id,
                    client_order_id=order_update.client_order_id,
                    exchange_order_id=order_update.exchange_order_id,
                    trading_pair=order_update.trading_pair,
                    fee_asset=order_update.fee_asset,
                    fee_paid=self.last_fee_paid,
                    fill_base_amount=self.last_filled_amount,
                    fill_quote_amount=self.last_filled_amount * (order_update.fill_price or self.price),
                    fill_price=(order_update.fill_price or self.price),
                    fill_timestamp=order_update.update_timestamp,
                )

            if self.is_filled:
                self.current_state = OrderState.FILLED

        return updated
