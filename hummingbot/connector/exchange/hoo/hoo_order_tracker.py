from typing import Dict, Optional
from decimal import Decimal

from hummingbot.connector.exchange.hoo.hoo_in_flight_order import HooInFlightOrder, HooOrderUpdate
from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.in_flight_order import OrderState


class HooOrderTracker(ClientOrderTracker):

    def __init__(self, connector: ConnectorBase) -> None:
        super().__init__(connector)
        self._in_flight_orders: Dict[str, HooInFlightOrder] = {}

    def fetch_tracked_order(self, client_order_id: str) -> Optional[HooInFlightOrder]:
        return self._in_flight_orders.get(client_order_id, None)

    def process_order_update(self, order_update: HooOrderUpdate):
        if not order_update.client_order_id and not order_update.exchange_order_id:
            self.logger().error("OrderUpdate does not contain any client_order_id or exchange_order_id", exc_info=True)
            return

        tracked_order: Optional[HooInFlightOrder] = self.fetch_order(
            order_update.client_order_id, order_update.exchange_order_id
        )

        if tracked_order:
            previous_state: OrderState = tracked_order.current_state
            previous_executed_amount_base: Decimal = tracked_order.executed_amount_base

            updated: bool = tracked_order.update_with_order_update(order_update)
            if updated:
                self._trigger_order_creation(tracked_order, previous_state, order_update.new_state)
                self._trigger_order_fills(tracked_order, previous_executed_amount_base)
                self._trigger_order_completion(tracked_order, order_update)

        else:
            self.logger().debug(f"Order is not/no longer being tracked ({order_update})")
