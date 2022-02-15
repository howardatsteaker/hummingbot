from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

# Base URL
REST_URL = "https://api.hoolgd.com/open{}"
WSS_URL = "wss://api.hoolgd.com/ws"
WSS_INNOVATE_URL = "wss://api.hoolgd.com/wsi"

# Public API endpoints
SERVER_TIMESTAMP_URL = "/v1/timestamp"
TICKERS_MARKET_URL = "/v1/tickers/market"
TRADE_MARKET_URL = "/v1/trade/market"
DEPTH_URL = "/v1/depth/market"
BALANCE_URL = "/v1/balance"
ORDERS_DETAIL_URL = "/v1/orders/detail"
ORDER_PLACE_URL = "/v1/orders/place"
ORDER_CANCEL_URL = "/v1/orders/cancel"
FEE_RATE_URL = "v1/fee-rate"

WS_HEARTBEAT_TIME_INTERVAL = 15

# Hoo params
SIDE_BUY = 1
SIDE_SELL = -1

# Rate Limit time intervals
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

# Order States
ORDER_STATE = {
    2: OrderState.OPEN,
    3: OrderState.PARTIALLY_FILLED,
    4: OrderState.FILLED,
    5: OrderState.CANCELLED,
    6: OrderState.CANCELLED,
}

# Websocket event types
DEPTH_EVENT_TYPE = "depth"
TRADE_EVENT_TYPE = "trade"
ACCOUNTS_BALANCE_TYPE = "accounts"
ORDER_CHANGE_TYPE = "orders"

RATE_LIMITS = [
    RateLimit(limit_id=TICKERS_MARKET_URL, limit=1, time_interval=ONE_SECOND),
    RateLimit(limit_id=TRADE_MARKET_URL, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=DEPTH_URL, limit=5, time_interval=ONE_SECOND),
]
