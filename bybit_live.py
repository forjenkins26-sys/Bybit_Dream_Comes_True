#!/usr/bin/env python3
"""
bybit_live.py — Vol Surge live bot on Bybit (USDT perp, BTCUSDT).

Flow:
  feed (closed candle) -> engine -> signal -> WS market order WITH attached SL/TP
  Bybit manages SL/TP exit SERVER-SIDE (survives bot crash — no software monitor needed).
  Private WS / position poll detects close -> log -> ready for next.

Order transport: WS-trade (order.create) PRIMARY, REST fallback.
SL/TP: attached to the entry order (atomic, server-side).

SAFETY:
  python bybit_live.py --test-order   # places ONE tiny order + closes it (verify live path)
  python bybit_live.py                 # run the bot

Env:
  BYBIT_API_KEY, BYBIT_API_SECRET   (required for live)
  BYBIT_TESTNET=true|false
  SYMBOL=BTCUSDT
  LOT_SIZE=0.001                     (BTC)
  MIN_BODY_PTS, VS_BURST_MULT, FIXED_SL_PTS, FIXED_TP_PTS
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
"""
import asyncio, os, sys, json, time, logging, threading
from collections import deque
from datetime import datetime, timezone, timedelta

import csv
from pathlib import Path
import requests
import bybit_api as B
from bybit_feed import BybitFeed, Candle
from signal_engine import SignalConfig, SignalEngine

# ── Slippage / trade log ─────────────────────────────────────────────────
DATA_DIR  = Path(os.getenv("DATA_DIR", str(Path(__file__).parent)))
SLIP_FILE = DATA_DIR / "bybit_slippage.csv"
SLIP_HDRS = ["timestamp_ist", "side", "signal_ref", "fill_avg", "entry_slip_pts",
             "sl", "tp", "order_lat_ms", "config"]

def _log_slip(row: dict):
    new = not SLIP_FILE.exists()
    try:
        with open(SLIP_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=SLIP_HDRS)
            if new: w.writeheader()
            w.writerow(row)
    except Exception as e:
        logging.getLogger("bybit_live").warning(f"slip log fail: {e}")

# ── Config ──────────────────────────────────────────────────────────────
SYMBOL      = os.getenv("SYMBOL", "BTCUSDT")
LOT_SIZE    = os.getenv("LOT_SIZE", "0.001")          # BTC, string (Bybit qty)
INTERVAL    = os.getenv("BYBIT_INTERVAL", "1")
MIN_BODY    = float(os.getenv("MIN_BODY_PTS", "150"))
BURST_MULT  = float(os.getenv("VS_BURST_MULT", "2.0"))
FIXED_SL    = float(os.getenv("FIXED_SL_PTS", "50"))
FIXED_TP    = float(os.getenv("FIXED_TP_PTS", "100"))
TG_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT     = os.getenv("TELEGRAM_CHAT_ID", "")
CONFIG_TAG  = f"MB{MIN_BODY:.0f} · {INTERVAL}m · SL{FIXED_SL:.0f}/TP{FIXED_TP:.0f} · burst{BURST_MULT} · Bybit"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("bybit_live")

_state_lock   = threading.Lock()
open_trade    = None
_entry_busy   = False
trade_ws      = None   # BybitTradeWS

# Incremental Heikin-Ashi state (Pine-exact) — engine runs on HA candles, not raw.
# MUST match the backtest (which feeds HA candles). use_ha=False on the engine.
_ha_op = None
_ha_cp = None
_ha_buf: deque = deque(maxlen=300)


def tg(msg: str):
    if not (TG_TOKEN and TG_CHAT):
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}, timeout=15)
    except Exception as e:
        log.warning(f"TG failed: {e}")


async def place_entry(side: str, fill_ref: float):
    """Place WS market order with attached SL/TP (REST fallback). side='Buy'|'Sell'."""
    if side == "Buy":
        sl = round(fill_ref - FIXED_SL, 1); tp = round(fill_ref + FIXED_TP, 1)
    else:
        sl = round(fill_ref + FIXED_SL, 1); tp = round(fill_ref - FIXED_TP, 1)
    t0 = time.time()
    resp = None
    if trade_ws and trade_ws.authed:
        resp = await trade_ws.place_market(side, LOT_SIZE, sl=sl, tp=tp)
    if not resp:   # fallback to REST
        log.warning("[ENTRY] WS unavailable/failed -> REST fallback")
        resp = B.place_market_order_rest(side, LOT_SIZE, sl=sl, tp=tp)
    lat = (time.time() - t0) * 1000
    ok = bool(resp and resp.get("retCode", resp.get("data", {}).get("retCode", -1)) == 0) if resp else False
    return resp, ok, lat, sl, tp


def _seed_ha_from_backfill(buffer: deque):
    """Seed the HA buffer from the feed's backfilled raw candles — once, on first call.
    Eliminates the ~10-bar warmup gap after (re)start so the engine is signal-ready."""
    global _ha_op, _ha_cp
    for rc in list(buffer)[:-1]:   # all backfilled bars except the live one about to process
        ha_c = (rc.open + rc.high + rc.low + rc.close) / 4.0
        ha_o = (rc.open + rc.close) / 2.0 if _ha_op is None else (_ha_op + _ha_cp) / 2.0
        ha_h = max(rc.high, ha_o, ha_c); ha_l = min(rc.low, ha_o, ha_c)
        _ha_buf.append(Candle(ts=rc.ts, open=round(ha_o,2), high=round(ha_h,2),
                              low=round(ha_l,2), close=round(ha_c,2), volume=rc.volume))
        _ha_op, _ha_cp = ha_o, ha_c
    log.info(f"[HA] seeded {len(_ha_buf)} HA candles from backfill — engine signal-ready")


def on_candle_close(raw: Candle, buffer: deque):
    """Engine callback — runs in feed thread. Computes HA from raw, feeds HA to engine.
    Matches the backtest exactly (HA-exact signal). Schedules entry on the asyncio loop."""
    global open_trade, _entry_busy, _ha_op, _ha_cp
    # One-time seed: build HA history from backfilled candles (no 10-bar warmup gap)
    if not _ha_buf and len(buffer) > 10:
        _seed_ha_from_backfill(buffer)
    # ── Incremental Heikin-Ashi (Pine-exact) ─────────────────────────────
    ha_c = (raw.open + raw.high + raw.low + raw.close) / 4.0
    ha_o = (raw.open + raw.close) / 2.0 if _ha_op is None else (_ha_op + _ha_cp) / 2.0
    ha_h = max(raw.high, ha_o, ha_c)
    ha_l = min(raw.low,  ha_o, ha_c)
    hac = Candle(ts=raw.ts, open=round(ha_o, 2), high=round(ha_h, 2),
                 low=round(ha_l, 2), close=round(ha_c, 2), volume=raw.volume)
    _ha_buf.append(hac)
    _ha_op, _ha_cp = ha_o, ha_c
    if len(_ha_buf) < 10:
        return

    st = engine.on_candle_close(hac, _ha_buf, in_trade=(open_trade is not None))
    if not st or st.signal not in ("BUY", "SELL"):
        return
    with _state_lock:
        if open_trade or _entry_busy:
            log.info(f"[SIGNAL] {st.signal} ignored — in trade / busy")
            return
        # confirm flat on exchange (Bybit manages SL/TP server-side; position may have closed)
        pos = B.get_position()
        if pos:
            log.info("[SIGNAL] ignored — exchange still shows open position")
            return
        _entry_busy = True
    side = "Buy" if st.signal == "BUY" else "Sell"
    # Anchor SL/TP off the REAL market price (raw close / live mark), NOT the HA close.
    fill_ref = feed.mark_price or raw.close
    log.info(f"[SIGNAL] {st.signal} | raw_close={raw.close:.1f} ha_close={st.close:.1f} "
             f"ref={fill_ref:.1f} body={st.candle_body:.1f} chop={st.chop_avg_tr:.1f}")
    # run the async order on the loop
    asyncio.run_coroutine_threadsafe(_do_entry(side, fill_ref), LOOP)


async def _do_entry(side, fill_ref):
    global open_trade, _entry_busy
    try:
        resp, ok, lat, sl, tp = await place_entry(side, fill_ref)
        if not ok:
            log.error(f"[ENTRY] FAILED: {resp}")
            tg(f"❌ <b>ENTRY FAILED</b> [{side}]\nResp: <code>{str(resp)[:200]}</code>")
            return
        # ── Slippage: real fill avg vs signal reference ──────────────────
        await asyncio.sleep(0.4)   # let position settle
        fill_avg = fill_ref
        try:
            pos = await asyncio.get_event_loop().run_in_executor(None, B.get_position)
            if pos and pos.get("avgPrice"):
                fill_avg = float(pos["avgPrice"])
        except Exception as e:
            log.warning(f"[SLIP] avg fetch fail: {e}")
        slip = (fill_avg - fill_ref) if side == "Buy" else (fill_ref - fill_avg)  # +ve = worse fill
        open_trade = {"side": side, "ref": fill_ref, "fill": fill_avg, "sl": sl, "tp": tp,
                      "ts": time.time(), "lat_ms": lat, "slip": slip}
        ist = (datetime.now(timezone.utc) + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S")
        _log_slip({"timestamp_ist": ist, "side": side, "signal_ref": round(fill_ref, 1),
                   "fill_avg": round(fill_avg, 1), "entry_slip_pts": round(slip, 1),
                   "sl": sl, "tp": tp, "order_lat_ms": round(lat), "config": CONFIG_TAG})
        log.info(f"[ENTRY] {side} | ref {fill_ref:.1f} fill {fill_avg:.1f} slip {slip:+.1f}pts | "
                 f"SL {sl} TP {tp} | order-lat {lat:.0f}ms")
        tg(f"🟢 <b>{side} ENTERED</b> [Bybit {INTERVAL}m WS]\n"
           f"Signal: {fill_ref:,.1f} → Fill: {fill_avg:,.1f} | Slip: <b>{slip:+.1f}pts</b>\n"
           f"SL: {sl:,.1f} | TP: {tp:,.1f}\n"
           f"Order latency: {lat:.0f}ms (server-side SL/TP)\n"
           f"⚙️ {CONFIG_TAG}")
    finally:
        _entry_busy = False


async def _position_watch():
    """Poll position; when a tracked trade's position goes flat -> exit closed."""
    global open_trade
    while True:
        await asyncio.sleep(3)
        if not open_trade:
            continue
        try:
            pos = await asyncio.get_event_loop().run_in_executor(None, B.get_position)
            if pos is None:   # flat -> SL or TP hit server-side
                ot = open_trade
                open_trade = None
                dur = round(time.time() - ot["ts"], 1)
                tg(f"⚪ <b>{ot['side']} CLOSED</b> [Bybit]\n"
                   f"Server-side SL/TP hit | Duration: {dur}s\n"
                   f"⚙️ {CONFIG_TAG}")
                log.info(f"[EXIT] position flat — trade closed after {dur}s")
        except Exception as e:
            log.warning(f"[WATCH] {e}")


# ── Health HTTP server (keeps Fly machine alive + status check) ───────────
def _start_health_server():
    from http.server import BaseHTTPRequestHandler, HTTPServer
    port = int(os.getenv("PORT", "8080"))
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a): pass
        def do_GET(self):
            ot = open_trade
            body = {
                "status": "alive",
                "config": CONFIG_TAG,
                "feed_ready": feed.is_ready,
                "in_trade": ot is not None,
                "trade": {k: ot[k] for k in ("side", "ref", "fill", "sl", "tp", "slip", "lat_ms")} if ot else None,
                "ws_trade": bool(trade_ws and trade_ws.authed),
            }
            self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers()
            self.wfile.write(json.dumps(body).encode())
    threading.Thread(target=lambda: HTTPServer(("0.0.0.0", port), H).serve_forever(),
                     daemon=True, name="health").start()
    log.info(f"[HEALTH] server on :{port}")


# ── Engine ──────────────────────────────────────────────────────────────
cfg = SignalConfig(lookback=5, burst_mult=BURST_MULT, sl_mult=1.8, tp2_r=FIXED_TP/FIXED_SL,
                   cooldown=3, use_ha=False, use_ema_filter=False, use_session=False,
                   safety_factor=1.0, use_min_body=(MIN_BODY > 0), min_body_pts=MIN_BODY,
                   use_breakout_ctx=True, breakout_ctx_bars=5)
engine = SignalEngine(config=cfg)
feed   = BybitFeed(symbol=SYMBOL, interval=INTERVAL, on_candle_close=on_candle_close, logger=log)
LOOP   = None


async def main():
    global trade_ws, LOOP
    LOOP = asyncio.get_event_loop()
    _start_health_server()
    # preflight
    if not (B.API_KEY and B.API_SECRET):
        log.error("No API keys. Set BYBIT_API_KEY / BYBIT_API_SECRET."); return
    log.info(f"[PREFLIGHT] testnet={B._TESTNET} symbol={SYMBOL} {CONFIG_TAG}")
    pos = B.get_position()
    log.info(f"[PREFLIGHT] position query OK: {'FLAT' if pos is None else pos.get('size')}")
    # connect WS-trade (low-latency orders)
    trade_ws = B.BybitTradeWS(logger=log)
    if not await trade_ws.connect():
        log.warning("[PREFLIGHT] WS-trade auth failed — will use REST fallback for orders")
    tg(f"🟢 <b>Bybit bot LIVE</b>\n{CONFIG_TAG}\nWS-trade: {'✓' if trade_ws.authed else 'REST fallback'}")
    asyncio.create_task(_position_watch())
    # feed runs its own loop in this thread
    await feed.start()


async def test_order():
    """Place ONE tiny market order + close it — verify live order path."""
    if not (B.API_KEY and B.API_SECRET):
        print("No keys."); return
    print(f"TEST ORDER on {'TESTNET' if B._TESTNET else 'LIVE'} | {SYMBOL} qty={LOT_SIZE}")
    pos = B.get_position()
    print("Position before:", "FLAT" if pos is None else pos.get("size"))
    tws = B.BybitTradeWS(); ok = await tws.connect()
    print("WS auth:", ok)
    t0 = time.time()
    r = await tws.place_market("Buy", LOT_SIZE) if ok else B.place_market_order_rest("Buy", LOT_SIZE)
    print(f"BUY resp ({(time.time()-t0)*1000:.0f}ms):", json.dumps(r)[:300])
    await asyncio.sleep(1)
    rc = B.place_market_order_rest("Sell", LOT_SIZE)   # close reduce
    print("CLOSE resp:", json.dumps(rc)[:200])
    print("Position after:", "FLAT" if B.get_position() is None else "STILL OPEN — check manually")


if __name__ == "__main__":
    if "--test-order" in sys.argv:
        asyncio.run(test_order())
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            feed.stop()
