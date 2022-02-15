import os
import socket

import hummingbot.connector.exchange.hoo.hoo_constants as CONSTANTS

from hummingbot.client.config.config_methods import using_exchange
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce

CENTRALIZED = True
DEFAULT_FEES = [0.1, 0.1]


def get_new_client_order_id(is_buy: bool, trading_pair: str) -> str:
    """
    Creates a client order id for a new order
    :param is_buy: True if the order is a buy order, False otherwise
    :param trading_pair: the trading pair the order will be operating with
    :return: an identifier for the new order to be used in the client
    """
    side = "B" if is_buy else "S"
    symbols = trading_pair.split("-")
    base = symbols[0].upper()
    quote = symbols[1].upper()
    base_str = f"{base[0]}{base[-1]}"
    quote_str = f"{quote[0]}{quote[-1]}"
    client_instance_id = hex(abs(hash(f"{socket.gethostname()}{os.getpid()}")))[2:6]
    return f"HBOT-HOO-{side}{base_str}{quote_str}{client_instance_id}{get_tracking_nonce()}"


def rest_url(path_url: str, domain: str = "") -> str:
    return CONSTANTS.REST_URL.format(domain) + path_url


KEYS = {
    "hoo_api_key":
        ConfigVar(key="hoo_api_key",
                  prompt="Enter your Hoo API key >>> ",
                  required_if=using_exchange("hoo"),
                  is_secure=True,
                  is_connect_key=True),
    "hoo_api_secret":
        ConfigVar(key="hoo_api_secret",
                  prompt="Enter your Hoo API secret >>> ",
                  required_if=using_exchange("hoo"),
                  is_secure=True,
                  is_connect_key=True),
}

OTHER_DOMAINS = ["hoo_innovate"]
OTHER_DOMAINS_PARAMETER = {"hoo_innovate": "/innovate"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"hoo_innovate": "LOOKS-USDT"}
OTHER_DOMAINS_DEFAULT_FEES = {"hoo_innovate": [0.1, 0.1]}
OTHER_DOMAINS_KEYS = {"hoo_innovate": {
    "hoo_innovate_api_key":
        ConfigVar(key="hoo_innovate_api_key",
                  prompt="Enter your Hoo innovate API key >>> ",
                  required_if=using_exchange("hoo_innovate"),
                  is_secure=True,
                  is_connect_key=True),
    "hoo_innovate_api_secret":
        ConfigVar(key="hoo_innovate_api_secret",
                  prompt="Enter your Hoo innovate API secret >>> ",
                  required_if=using_exchange("hoo_innovate"),
                  is_secure=True,
                  is_connect_key=True),
}}