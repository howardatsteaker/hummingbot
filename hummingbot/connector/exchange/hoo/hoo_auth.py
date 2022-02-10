import time
import hashlib
import hmac
from collections import OrderedDict

from typing import (
    Any,
    Dict
)

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, RESTMethod, WSRequest
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce


class HooAuth(AuthBase):

    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if request.method == RESTMethod.POST:
            request.data = self.add_auth_to_params(request.data)
        else:
            request.params = self.add_auth_to_params(request.params)

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    def add_auth_to_params(self, params: Dict[str, Any]):
        sign_obj = self._generate_signature()
        request_params = OrderedDict(params or {})
        request_params.update(sign_obj)
        return request_params

    def _generate_signature(self):
        ts = int(time.time())
        nonce = get_tracking_nonce()
        obj = {"ts": ts, "nonce": nonce, "sign": "", "client_id": self.api_key}
        s = "client_id=%s&nonce=%s&ts=%s" % (self.api_key, nonce, ts) 
        v = hmac.new(self.secret_key.encode(), s.encode(), digestmod=hashlib.sha256)
        obj["sign"] = v.hexdigest()
        return obj

    def _generate_websocket_subscription(self):
        ts = int(time.time())
        nonce = get_tracking_nonce()
        obj = {"ts": ts, "nonce": nonce, "sign": "", "client_id": self.api_key, "op": "apilogin"}
        s = "client_id=%s&nonce=%s&ts=%s" % (self.api_key, nonce, ts)
        v = hmac.new(self.secret_key.encode(), s.encode(), digestmod=hashlib.sha256)
        obj["sign"] = v.hexdigest()
        return obj
