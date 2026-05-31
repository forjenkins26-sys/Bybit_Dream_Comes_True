#!/usr/bin/env python3
"""
bybit_feed.py — Bybit v5 public WebSocket kline feed for Vol Surge.
Emits CLOSED candles via callback. Uses Bybit's `confirm` flag (true = bar closed)
— no forced-close watchdog needed (cleaner than Delta).

Endpoint : wss://stream.bybit.com/v5/public/linear   (USDT perp)
Topic    : kline.{interval}.{symbol}   e.g. kline.1.BTCUSDT  (interval in minutes)
"""
import asyncio
import json
import logging
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, Optional

import requests
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

_TESTNET  = os.getenv("BYBIT_TESTNET", "false").lower() == "true"
WS_PUBLIC = ("wss://stream-testnet.bybit.com/v5/public/linear" if _TESTNET else "wss://stream.bybit.com/v5/public/linear")
REST_URL  = "https://api-testnet.bybit.com" if _TESTNET else "https://api.bybit.com"
SYMBOL    = os.getenv("SYMBOL", "BTCUSDT")
INTERVAL  = os.getenv("BYBIT_INTERVAL", "1")    # minutes: "1","5","15"

_BACKOFF_INIT, _BACKOFF_MAX, _BACKOFF_MULT = 1.0, 60.0, 2.0
_HEARTBEAT = 20.0


@dataclass
class Candle:
    ts:     int    # candle start (Unix seconds, UTC)
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float

    def __repr__(self):
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(self.ts, tz=timezone.utc)
        return (f"Candle({dt.strftime('%Y-%m-%d %H:%M UTC')} "
                f"O={self.open:.1f} H={self.high:.1f} L={self.low:.1f} C={self.close:.1f})")


class BybitFeed:
    def __init__(self, symbol=SYMBOL, interval=INTERVAL, buffer_size=300,
                 on_candle_close: Optional[Callable] = None, logger=None):
        self.symbol   = symbol
        self.interval = interval
        self.buffer:  deque = deque(maxlen=buffer_size)
        self.mark_price: Optional[float] = None
        self.mark_price_updated_at: Optional[float] = None
        self.mark_price_event = threading.Event()
        self.last_closed: Optional[Candle] = None
        self.connected = False
        self.last_frame_at: Optional[float] = None
        self.reconnect_count = 0
        self._on_close = on_candle_close
        self._warmed = False
        self._running = False
        self.log = logger or logging.getLogger("bybit_feed")

    @property
    def is_ready(self) -> bool:
        return self._warmed and len(self.buffer) >= 250

    # ── REST backfill (warmup) ──────────────────────────────────────────────
    async def _backfill(self, count=300):
        self.log.info(f"[FEED] REST backfill {count} {self.interval}m candles...")
        try:
            raw = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: requests.get(
                    f"{REST_URL}/v5/market/kline",
                    params={"category": "linear", "symbol": self.symbol,
                            "interval": self.interval, "limit": min(count, 1000)},
                    timeout=20,
                ).json()
            )
            rows = raw.get("result", {}).get("list", [])
            # Bybit returns newest-first; each row: [start_ms, open, high, low, close, volume, turnover]
            now = int(time.time())
            per = int(self.interval) * 60
            loaded = 0
            for r in reversed(rows):   # oldest -> newest
                ts = int(r[0]) // 1000
                if ts + per <= now:    # only fully-closed bars
                    c = Candle(ts=ts, open=float(r[1]), high=float(r[2]),
                               low=float(r[3]), close=float(r[4]), volume=float(r[5]))
                    self.buffer.append(c); self.last_closed = c; loaded += 1
            self._warmed = True
            self.log.info(f"[FEED] Backfill done — {loaded} candles | buffer={len(self.buffer)} "
                          f"| newest={self.buffer[-1] if self.buffer else 'n/a'}")
        except Exception as e:
            self.log.error(f"[FEED] Backfill error: {e}")
            self._warmed = True

    async def start(self):
        self._running = True
        self.log.info(f"[FEED] Starting Bybit {self.symbol} {self.interval}m")
        await self._backfill(300)
        backoff = _BACKOFF_INIT
        while self._running:
            try:
                await self._ws_loop()
                backoff = _BACKOFF_INIT
            except (ConnectionClosed, WebSocketException, OSError) as e:
                self.connected = False
                self.log.warning(f"[FEED] WS disconnect: {e!r} — retry {backoff:.0f}s")
            except Exception as e:
                self.connected = False
                self.log.error(f"[FEED] WS error: {e!r} — retry {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * _BACKOFF_MULT, _BACKOFF_MAX)
            self.reconnect_count += 1
            if self.last_closed:
                await self._backfill_gap()

    async def _backfill_gap(self):
        # Re-pull recent bars to fill any disconnect gap (dedup on emit)
        await self._backfill(50)

    def stop(self):
        self._running = False

    async def _ws_loop(self):
        async with websockets.connect(WS_PUBLIC, ping_interval=None, open_timeout=15, max_size=2**20) as ws:
            self.connected = True
            self.log.info("[FEED] WS connected")
            await ws.send(json.dumps({"op": "subscribe",
                                      "args": [f"kline.{self.interval}.{self.symbol}",
                                               f"tickers.{self.symbol}"]}))
            hb = asyncio.create_task(self._heartbeat(ws))
            try:
                async for frame in ws:
                    self.last_frame_at = time.time()
                    msg = json.loads(frame)
                    topic = msg.get("topic", "")
                    if topic.startswith("kline."):
                        self._handle_kline(msg)
                    elif topic.startswith("tickers."):
                        self._handle_ticker(msg)
            finally:
                hb.cancel()
                self.connected = False

    async def _heartbeat(self, ws):
        while True:
            await asyncio.sleep(_HEARTBEAT)
            try:
                await ws.send(json.dumps({"op": "ping"}))
            except Exception:
                break

    def _handle_kline(self, msg):
        for d in msg.get("data", []):
            if not d.get("confirm"):     # bar still forming
                # update mark price proxy from forming close
                try:
                    self.mark_price = float(d["close"])
                    self.mark_price_updated_at = time.time()
                    self.mark_price_event.set()
                except Exception:
                    pass
                continue
            # confirm == true -> bar closed
            ts = int(d["start"]) // 1000
            c = Candle(ts=ts, open=float(d["open"]), high=float(d["high"]),
                       low=float(d["low"]), close=float(d["close"]), volume=float(d["volume"]))
            self._emit_closed(c)

    def _handle_ticker(self, msg):
        d = msg.get("data", {})
        p = d.get("markPrice") or d.get("lastPrice")
        if p:
            try:
                self.mark_price = float(p)
                self.mark_price_updated_at = time.time()
                self.mark_price_event.set()
            except Exception:
                pass

    def _emit_closed(self, candle: Candle):
        if self.last_closed and candle.ts <= self.last_closed.ts:
            return  # dedup
        self.buffer.append(candle)
        self.last_closed = candle
        self.log.info(f"[FEED] CLOSED {candle}")
        if self._on_close:
            try:
                self._on_close(candle, self.buffer)
            except Exception as e:
                self.log.error(f"[FEED] callback raised: {e}")


# ── Standalone smoke test ──────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
    feed = BybitFeed(interval=os.getenv("BYBIT_INTERVAL", "1"))

    def on_close(c, buf):
        print(f">>> CLOSED bar emitted: {c}  (buffer={len(buf)})")

    feed._on_close = on_close
    try:
        asyncio.run(feed.start())
    except KeyboardInterrupt:
        feed.stop()
