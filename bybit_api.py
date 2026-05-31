#!/usr/bin/env python3
"""
bybit_api.py — Bybit v5 trading layer.

Two transports:
  1. REST  (fallback / queries)               — POST /v5/order/create
  2. WS Trade (PRIMARY — the latency win)      — op "order.create" on wss://.../v5/trade
     Persistent authed socket → no per-order HTTP round-trip.

Order placement uses ATTACHED SL/TP (takeProfit/stopLoss on the order itself) —
atomic, single request, server-side. Cleaner than Delta's separate bracket call.

Auth (v5):
  REST sign = HMAC_SHA256(secret, timestamp + api_key + recv_window + body)
  WS  auth  = HMAC_SHA256(secret, "GET/realtime" + expires)
"""
import hashlib
import hmac
import json
import logging
import os
import threading
import time
import uuid
from typing import Optional

import requests
import websockets

API_KEY    = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
SYMBOL     = os.getenv("SYMBOL", "BTCUSDT")
CATEGORY   = "linear"
RECV_WINDOW = "5000"

# Testnet toggle — set BYBIT_TESTNET=true to trade fake money (test WS orders safely)
_TESTNET   = os.getenv("BYBIT_TESTNET", "false").lower() == "true"
REST_URL   = "https://api-testnet.bybit.com" if _TESTNET else "https://api.bybit.com"
WS_TRADE   = ("wss://stream-testnet.bybit.com/v5/trade"   if _TESTNET else "wss://stream.bybit.com/v5/trade")
WS_PRIVATE = ("wss://stream-testnet.bybit.com/v5/private" if _TESTNET else "wss://stream.bybit.com/v5/private")

log = logging.getLogger("bybit_api")

# ════════════════════════════════════════════════════════════════════════
# REST (fallback + queries)
# ════════════════════════════════════════════════════════════════════════
_http = requests.Session()
_http.mount("https://", requests.adapters.HTTPAdapter(pool_connections=2, pool_maxsize=4, max_retries=0))


def _rest_headers(body: str) -> dict:
    ts = str(int(time.time() * 1000))
    sign = hmac.new(API_SECRET.encode(),
                    (ts + API_KEY + RECV_WINDOW + body).encode(),
                    hashlib.sha256).hexdigest()
    return {"X-BAPI-API-KEY": API_KEY, "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": RECV_WINDOW, "X-BAPI-SIGN": sign,
            "Content-Type": "application/json"}


def rest_post(path: str, params: dict) -> Optional[dict]:
    body = json.dumps(params)
    try:
        r = _http.post(REST_URL + path, headers=_rest_headers(body), data=body, timeout=10)
        return r.json()
    except Exception as e:
        log.error(f"REST POST {path} error: {e}")
        return None


def rest_get(path: str, params: dict) -> Optional[dict]:
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    try:
        r = _http.get(REST_URL + path + "?" + qs, headers=_rest_headers(qs), timeout=10)
        return r.json()
    except Exception as e:
        log.error(f"REST GET {path} error: {e}")
        return None


def get_position() -> Optional[dict]:
    """Return open position dict for SYMBOL, or None if flat."""
    r = rest_get("/v5/position/list", {"category": CATEGORY, "symbol": SYMBOL})
    if r and r.get("retCode") == 0:
        for p in r["result"]["list"]:
            if float(p.get("size", 0)) != 0:
                return p
    return None


def place_market_order_rest(side: str, qty: str, sl: float = 0, tp: float = 0) -> Optional[dict]:
    """REST market order with attached SL/TP. side = 'Buy'|'Sell'. Fallback path."""
    params = {"category": CATEGORY, "symbol": SYMBOL, "side": side,
              "orderType": "Market", "qty": qty, "timeInForce": "IOC",
              "orderLinkId": uuid.uuid4().hex}
    if sl: params["stopLoss"] = str(sl)
    if tp: params["takeProfit"] = str(tp)
    return rest_post("/v5/order/create", params)


# ════════════════════════════════════════════════════════════════════════
# WS TRADE — persistent authed socket, order.create (PRIMARY, low-latency)
# ════════════════════════════════════════════════════════════════════════
class BybitTradeWS:
    """
    Persistent authenticated WS for placing orders without HTTP round-trips.
    Usage:
        tws = BybitTradeWS(); await tws.connect()
        resp = await tws.place_market(side="Buy", qty="0.001", sl=..., tp=...)
    """
    def __init__(self, logger=None):
        self.ws = None
        self.authed = False
        self.log = logger or log
        self._pending = {}   # reqId -> asyncio.Future

    def _ws_auth_args(self):
        expires = int((time.time() + 10) * 1000)
        sign = hmac.new(API_SECRET.encode(), f"GET/realtime{expires}".encode(),
                        hashlib.sha256).hexdigest()
        return [API_KEY, expires, sign]

    async def connect(self):
        import asyncio
        self.ws = await websockets.connect(WS_TRADE, ping_interval=20, open_timeout=15)
        await self.ws.send(json.dumps({"op": "auth", "args": self._ws_auth_args()}))
        # read auth ack
        ack = json.loads(await self.ws.recv())
        self.authed = bool(ack.get("success"))
        self.log.info(f"[WS_TRADE] auth {'OK' if self.authed else 'FAILED: '+str(ack)}")
        asyncio.create_task(self._reader())
        return self.authed

    async def _reader(self):
        try:
            async for frame in self.ws:
                msg = json.loads(frame)
                rid = msg.get("reqId") or msg.get("req_id")
                if rid in self._pending:
                    fut = self._pending.pop(rid)
                    if not fut.done():
                        fut.set_result(msg)
        except Exception as e:
            self.log.warning(f"[WS_TRADE] reader stopped: {e}")
            self.authed = False

    async def place_market(self, side: str, qty: str, sl: float = 0, tp: float = 0, timeout=5):
        """Place market order over WS with attached SL/TP. Returns response dict."""
        import asyncio
        rid = uuid.uuid4().hex
        order = {"category": CATEGORY, "symbol": SYMBOL, "side": side,
                 "orderType": "Market", "qty": qty, "timeInForce": "IOC",
                 "orderLinkId": rid}
        if sl: order["stopLoss"] = str(sl)
        if tp: order["takeProfit"] = str(tp)
        ts = str(int(time.time() * 1000))
        req = {"reqId": rid,
               "header": {"X-BAPI-TIMESTAMP": ts, "X-BAPI-RECV-WINDOW": RECV_WINDOW},
               "op": "order.create", "args": [order]}
        fut = asyncio.get_event_loop().create_future()
        self._pending[rid] = fut
        await self.ws.send(json.dumps(req))
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending.pop(rid, None)
            self.log.error("[WS_TRADE] order.create timeout — falling back to REST")
            return None


# ── Connectivity check (public — no keys needed) ───────────────────────────
def server_time() -> Optional[float]:
    try:
        r = _http.get(REST_URL + "/v5/market/time", timeout=10).json()
        return float(r["result"]["timeSecond"])
    except Exception as e:
        log.error(f"server_time error: {e}")
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
    print("Bybit server time:", server_time())
    if API_KEY and API_SECRET:
        print("Keys present — testing authed REST position query...")
        print("Position:", get_position())
    else:
        print("No API keys set (BYBIT_API_KEY / BYBIT_API_SECRET). WS-trade + order tests need keys.")
