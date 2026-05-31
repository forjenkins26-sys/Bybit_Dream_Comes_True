#!/usr/bin/env python3
"""
bybit_live.py — Vol Surge live bot on Bybit (USDT perp, BTCUSDT).

Flow:
  feed (closed candle) -> engine -> signal -> WS market order WITH attached SL/TP
  Bybit manages SL/TP exit SERVER-SIDE (survives bot crash — no software monitor needed).
  Private WS / position poll detects close -> log -> ready for next.

Order transport: WS-trade (order.create) PRIMARY, REST fallback.
SL/TP: attached to the entry order (atomic, server-side).

Env:
  BYBIT_API_KEY, BYBIT_API_SECRET   (required for live)
  BYBIT_TESTNET=true|false
  SYMBOL=BTCUSDT
  LOT_SIZE=0.001                     (BTC)
  MIN_BODY_PTS, VS_BURST_MULT, FIXED_SL_PTS, FIXED_TP_PTS
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
"""
import asyncio, os, sys, json, time, logging, threading, uuid
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Optional

import csv
from pathlib import Path
import requests
import bybit_api as B
from bybit_feed import BybitFeed, Candle
from signal_engine import SignalConfig, SignalEngine

# ── FastAPI dashboard ────────────────────────────────────────────────────
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
import uvicorn

app = FastAPI()

# ── Config ──────────────────────────────────────────────────────────────
SYMBOL      = os.getenv("SYMBOL", "BTCUSDT")
LOT_SIZE    = float(os.getenv("LOT_SIZE", "0.001"))
LOT_SIZE_STR = str(LOT_SIZE)
INTERVAL    = os.getenv("BYBIT_INTERVAL", "1")
MIN_BODY    = float(os.getenv("MIN_BODY_PTS", "150"))
BURST_MULT  = float(os.getenv("VS_BURST_MULT", "2.0"))
FIXED_SL    = float(os.getenv("FIXED_SL_PTS", "50"))
FIXED_TP    = float(os.getenv("FIXED_TP_PTS", "100"))
TP_R        = round(FIXED_TP / FIXED_SL, 2) if FIXED_SL > 0 else 2.0
TG_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT     = os.getenv("TELEGRAM_CHAT_ID", "")
CONFIG_TAG  = f"MB{MIN_BODY:.0f} · {INTERVAL}m · SL{FIXED_SL:.0f}/TP{FIXED_TP:.0f} · burst{BURST_MULT} · Bybit"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("bybit_live")

# ── Data files ──────────────────────────────────────────────────────────
DATA_DIR       = Path(os.getenv("DATA_DIR", str(Path(__file__).parent)))
_DATA_PERSISTENT = (DATA_DIR / ".persistent_marker").exists() or DATA_DIR != Path(__file__).parent
TRADE_FILE     = DATA_DIR / "bybit_trades.csv"
LIFECYCLE_FILE = DATA_DIR / "bybit_lifecycle.csv"
SLIP_FILE      = DATA_DIR / "bybit_slippage.csv"

TRADE_HDRS = [
    "trade_id", "direction", "entry_time_ist",
    "fill_price", "signal_ref", "entry_slippage_pts", "entry_slippage_pct",
    "sl_price", "tp_price",
    "signal_recv_time", "entry_fill_time",
    "signal_latency_ms", "entry_latency_ms",
    "exit_price", "exit_time_ist", "exit_type",
    "pts", "pnl_usdt",
    "python_actual_outcome",
    "slippage_ratio", "structure_grade",
    "trade_duration_sec",
    "entry_order_id",
    "chop_avg_tr", "burst_threshold", "candle_body",
    "config",
]
LIFECYCLE_HDRS = [
    "trade_id", "timestamp_ist", "unix_ts",
    "event", "order_id", "side", "qty", "price",
    "latency_from_prev_ms", "notes",
]
SLIP_HDRS = ["timestamp_ist", "side", "signal_ref", "fill_avg", "entry_slip_pts",
             "sl", "tp", "order_lat_ms", "config"]

def _init_csvs():
    for fpath, headers in [
        (TRADE_FILE,     TRADE_HDRS),
        (LIFECYCLE_FILE, LIFECYCLE_HDRS),
        (SLIP_FILE,      SLIP_HDRS),
    ]:
        if not fpath.exists():
            with open(fpath, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=headers).writeheader()

def _append_csv(fpath, headers, row):
    try:
        with open(fpath, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=headers, extrasaction="ignore").writerow(row)
    except Exception as e:
        log.warning(f"CSV write error ({fpath.name}): {e}")

_lifecycle_last_ts: dict = {}
def _log_lifecycle(trade_id, event, order_id="", side="", qty=0, price=0, notes=""):
    now_unix = time.time()
    now_ist  = (datetime.utcnow() + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S.%f")[:-3]
    prev_ts  = _lifecycle_last_ts.get(trade_id, now_unix)
    lat_ms   = round((now_unix - prev_ts) * 1000, 1) if trade_id in _lifecycle_last_ts else 0
    _lifecycle_last_ts[trade_id] = now_unix
    _append_csv(LIFECYCLE_FILE, LIFECYCLE_HDRS, {
        "trade_id": trade_id, "timestamp_ist": now_ist, "unix_ts": round(now_unix, 3),
        "event": event, "order_id": order_id or "", "side": side,
        "qty": qty or "", "price": price or "",
        "latency_from_prev_ms": lat_ms, "notes": notes,
    })

# ── Structure grade ──────────────────────────────────────────────────────
def _structure_grade(slippage_ratio: float) -> str:
    if slippage_ratio < 0.25: return "INTACT"
    if slippage_ratio < 0.5:  return "MILD"
    if slippage_ratio < 1.0:  return "DEGRADED"
    if slippage_ratio < 1.5:  return "BROKEN"
    return "CRITICAL"

# ── State ────────────────────────────────────────────────────────────────
_state_lock   = threading.Lock()
open_trade    = None
_entry_busy   = False
trade_ws      = None

_ha_op = None
_ha_cp = None
_ha_buf: deque = deque(maxlen=300)

# ── Telegram ─────────────────────────────────────────────────────────────
def tg(msg: str):
    if not (TG_TOKEN and TG_CHAT):
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}, timeout=15)
    except Exception as e:
        log.warning(f"TG failed: {e}")

# ── Entry ─────────────────────────────────────────────────────────────────
async def place_entry(side: str, fill_ref: float):
    if side == "Buy":
        sl = round(fill_ref - FIXED_SL, 1); tp = round(fill_ref + FIXED_TP, 1)
    else:
        sl = round(fill_ref + FIXED_SL, 1); tp = round(fill_ref - FIXED_TP, 1)
    t0 = time.time()
    resp = None
    if trade_ws and trade_ws.authed:
        resp = await trade_ws.place_market(side, LOT_SIZE_STR, sl=sl, tp=tp)
    if not resp:
        log.warning("[ENTRY] WS unavailable -> REST fallback")
        resp = B.place_market_order_rest(side, LOT_SIZE_STR, sl=sl, tp=tp)
    lat = (time.time() - t0) * 1000
    ok = bool(resp and (resp.get("retCode", -1) == 0 or
              resp.get("data", {}).get("retCode", -1) == 0)) if resp else False
    return resp, ok, lat, sl, tp


def _seed_ha_from_backfill(buffer: deque):
    global _ha_op, _ha_cp
    for rc in list(buffer)[:-1]:
        ha_c = (rc.open + rc.high + rc.low + rc.close) / 4.0
        ha_o = (rc.open + rc.close) / 2.0 if _ha_op is None else (_ha_op + _ha_cp) / 2.0
        ha_h = max(rc.high, ha_o, ha_c); ha_l = min(rc.low, ha_o, ha_c)
        _ha_buf.append(Candle(ts=rc.ts, open=round(ha_o,2), high=round(ha_h,2),
                              low=round(ha_l,2), close=round(ha_c,2), volume=rc.volume))
        _ha_op, _ha_cp = ha_o, ha_c
    log.info(f"[HA] seeded {len(_ha_buf)} HA candles from backfill")


def on_candle_close(raw: Candle, buffer: deque):
    global open_trade, _entry_busy, _ha_op, _ha_cp
    if not _ha_buf and len(buffer) > 10:
        _seed_ha_from_backfill(buffer)
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
        pos = B.get_position()
        if pos:
            log.info("[SIGNAL] ignored — exchange still shows open position")
            return
        _entry_busy = True
    side = "Buy" if st.signal == "BUY" else "Sell"
    fill_ref = feed.mark_price or raw.close
    signal_recv_time = time.time()
    log.info(f"[SIGNAL] {st.signal} | raw_close={raw.close:.1f} ha_close={st.close:.1f} "
             f"ref={fill_ref:.1f} body={st.candle_body:.1f} chop={st.chop_avg_tr:.1f}")
    asyncio.run_coroutine_threadsafe(
        _do_entry(side, fill_ref, signal_recv_time, st), LOOP)


async def _do_entry(side, fill_ref, signal_recv_time, st=None,
                    trade_id=None, is_test=False,
                    chop_avg_tr=0.0, burst_threshold=0.0, candle_body=0.0):
    global open_trade, _entry_busy
    try:
        if trade_id is None:
            trade_id = f"T{int(time.time()*1000)}"
        entry_submit_time = time.time()
        resp, ok, lat, sl, tp = await place_entry(side, fill_ref)
        entry_fill_time = time.time()

        if not ok:
            log.error(f"[ENTRY] FAILED: {resp}")
            tg(f"❌ <b>ENTRY FAILED</b> [{side}]\nResp: <code>{str(resp)[:200]}</code>")
            return

        await asyncio.sleep(0.4)
        fill_avg = fill_ref
        try:
            pos = await asyncio.get_event_loop().run_in_executor(None, B.get_position)
            if pos and pos.get("avgPrice"):
                fill_avg = float(pos["avgPrice"])
        except Exception as e:
            log.warning(f"[SLIP] avg fetch fail: {e}")

        slip = (fill_avg - fill_ref) if side == "Buy" else (fill_ref - fill_avg)
        sl_dist = FIXED_SL
        _ratio = round(abs(slip) / sl_dist, 3) if sl_dist > 0 else 0.0
        _grade = "TEST" if is_test else _structure_grade(_ratio)
        _slip_pct = round(abs(slip) / fill_avg * 100, 4) if fill_avg else 0.0
        # signal_recv_time = when candle close detected; entry_submit_time = when order sent
        signal_latency_ms = round((entry_submit_time - signal_recv_time) * 1000, 1)
        entry_latency_ms  = round(lat, 1)

        ist = (datetime.now(timezone.utc) + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S")

        with _state_lock:
            open_trade = {
                "trade_id":           trade_id,
                "direction":          "BUY" if side == "Buy" else "SELL",
                "fill_price":         fill_avg,
                "signal_ref":         fill_ref,
                "sl_price":           sl,
                "tp_price":           tp,
                "entry_slippage_pts": round(slip, 2),
                "entry_slippage_pct": _slip_pct,
                "slippage_ratio":     _ratio,
                "structure_grade":    _grade,
                "signal_latency_ms":  signal_latency_ms,
                "entry_latency_ms":   entry_latency_ms,
                "signal_recv_time":   signal_recv_time,
                "entry_fill_time":    entry_fill_time,
                "entry_time_ist":     ist,
                "ts":                 entry_fill_time,
                "lat_ms":             lat,
                "chop_avg_tr":        chop_avg_tr or (st.chop_avg_tr if st else 0),
                "burst_threshold":    burst_threshold or (st.burst_threshold if st else 0),
                "candle_body":        candle_body or (st.candle_body if st else 0),
                "is_test":            is_test,
            }

        _log_lifecycle(trade_id, "ENTRY_ACKED", side=side.lower(), qty=LOT_SIZE,
                       price=fill_avg, notes=f"slip={slip:+.2f}pts grade={_grade}")

        # Log slippage CSV
        _append_csv(SLIP_FILE, SLIP_HDRS, {
            "timestamp_ist": ist, "side": side, "signal_ref": round(fill_ref, 1),
            "fill_avg": round(fill_avg, 1), "entry_slip_pts": round(slip, 1),
            "sl": sl, "tp": tp, "order_lat_ms": round(lat), "config": CONFIG_TAG,
        })

        dir_label  = "BUY" if side == "Buy" else "SELL"
        fill_ist   = (datetime.now(timezone.utc) + timedelta(seconds=19800)).strftime("%d/%m %H:%M:%S IST")
        _chop      = chop_avg_tr or (st.chop_avg_tr if st else 0)
        _burst     = burst_threshold or (st.burst_threshold if st else 0)

        # Server-side SL/TP — equivalent of bracket order
        tg(f"🔒 <b>SERVER-SIDE SL/TP SET</b> [{dir_label}]\n"
           f"SL: {sl:,.1f} (exchange stop-market — server-side)\n"
           f"TP: {tp:,.1f} (exchange limit — server-side)\n"
           f"Exits are safe during disconnects ✓")

        tg(f"{'🧪 TEST' if is_test else '🟢 LIVE'} <b>{dir_label} ENTERED</b> [Bybit {INTERVAL}m WS]\n"
           f"Fill: <b>{fill_avg:,.1f}</b> | Slip: <b>{slip:+.2f}pts</b>\n"
           f"SL: {sl:,.1f} [FIXED {FIXED_SL:.0f}pts] | TP: {tp:,.1f} [FIXED {FIXED_TP:.0f}pts]\n"
           f"Fill: {fill_ist}\n"
           f"Signal lat: {signal_latency_ms:.0f}ms | Entry lat: {entry_latency_ms:.0f}ms\n"
           f"Structure: <b>{_grade}</b> | Chop: {_chop:.1f} Burst: {_burst:.1f}\n"
           f"⚙️ {CONFIG_TAG}")
        log.info(f"[ENTRY] {side} | ref {fill_ref:.1f} fill {fill_avg:.1f} slip {slip:+.2f}pts | "
                 f"SL {sl} TP {tp} | lat {lat:.0f}ms | grade={_grade}")
    finally:
        with _state_lock:
            _entry_busy = False


async def _position_watch():
    global open_trade
    while True:
        await asyncio.sleep(3)
        with _state_lock:
            has_trade = open_trade is not None
        if not has_trade:
            continue
        try:
            pos = await asyncio.get_event_loop().run_in_executor(None, B.get_position)
            if pos is None:
                with _state_lock:
                    ot = open_trade
                    if not ot:
                        continue
                    open_trade = None

                dur = round(time.time() - ot["ts"], 1)
                side     = ot.get("direction", "?")
                fill     = ot.get("fill_price", 0)
                sl_px    = ot.get("sl_price", 0)
                tp_px    = ot.get("tp_price", 0)
                slip     = ot.get("entry_slippage_pts", 0)
                lat      = ot.get("lat_ms", 0)
                trade_id = ot.get("trade_id", "?")
                is_test  = ot.get("is_test", False)
                grade    = ot.get("structure_grade", "?")

                exit_px = None
                outcome = "CLOSED"
                pnl_usdt = 0.0
                exit_type = "SERVER_SIDE"
                try:
                    r = B.rest_get("/v5/position/closed-pnl",
                                   {"category": "linear", "symbol": SYMBOL, "limit": "1"})
                    if r and r.get("retCode") == 0 and r["result"]["list"]:
                        last = r["result"]["list"][0]
                        exit_px  = float(last.get("avgExitPrice", 0) or 0)
                        pnl_usdt = float(last.get("closedPnl", 0) or 0)
                        if side == "BUY":
                            outcome   = "TP" if exit_px >= tp_px - 5 else "SL"
                            exit_type = "TP_LIVE" if outcome == "TP" else "SL_LIVE"
                        else:
                            outcome   = "TP" if exit_px <= tp_px + 5 else "SL"
                            exit_type = "TP_LIVE" if outcome == "TP" else "SL_LIVE"
                except Exception as e:
                    log.warning(f"[EXIT] closed pnl fetch fail: {e}")

                pts = round((exit_px - fill) if side=="BUY" else (fill - exit_px), 2) if exit_px else 0
                ist = (datetime.now(timezone.utc) + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S")

                _log_lifecycle(trade_id, f"EXIT_{outcome}", price=exit_px or 0, notes=exit_type)

                _append_csv(TRADE_FILE, TRADE_HDRS, {
                    "trade_id":              trade_id,
                    "direction":             side,
                    "entry_time_ist":        ot.get("entry_time_ist", ist),
                    "fill_price":            round(fill, 1),
                    "signal_ref":            round(ot.get("signal_ref", fill), 1),
                    "entry_slippage_pts":    round(slip, 2),
                    "entry_slippage_pct":    ot.get("entry_slippage_pct", ""),
                    "sl_price":              sl_px,
                    "tp_price":              tp_px,
                    "signal_recv_time":      ot.get("signal_recv_time", ""),
                    "entry_fill_time":       ot.get("entry_fill_time", ""),
                    "signal_latency_ms":     ot.get("signal_latency_ms", ""),
                    "entry_latency_ms":      ot.get("entry_latency_ms", ""),
                    "exit_price":            round(exit_px, 1) if exit_px else "",
                    "exit_time_ist":         ist,
                    "exit_type":             exit_type,
                    "pts":                   pts,
                    "pnl_usdt":              round(pnl_usdt, 4),
                    "python_actual_outcome": outcome,
                    "slippage_ratio":        ot.get("slippage_ratio", ""),
                    "structure_grade":       grade,
                    "trade_duration_sec":    dur,
                    "entry_order_id":        "",
                    "chop_avg_tr":           ot.get("chop_avg_tr", ""),
                    "burst_threshold":       ot.get("burst_threshold", ""),
                    "candle_body":           ot.get("candle_body", ""),
                    "config":                CONFIG_TAG,
                })

                outcome_emoji = "✅" if outcome == "TP" else "🔴"
                outcome_label = "TP HIT" if outcome == "TP" else "SL HIT"
                dur_fmt = f"{int(dur)//60}m {int(dur)%60}s" if dur >= 60 else f"{int(dur)}s"
                tg(f"{outcome_emoji} <b>{outcome_label} [{side}]</b>\n"
                   f"Entry: {fill:,.1f} → Exit: {exit_px:,.1f}\n"
                   f"PnL: <b>{pts:+.2f}pts</b> | {pnl_usdt:+.4f} USDT | Outcome: {outcome}\n"
                   f"Structure: {grade} | Duration: {dur_fmt}\n"
                   f"⚙️ {CONFIG_TAG}")
                log.info(f"[EXIT] {outcome} | pts={pts:+.2f} pnl={pnl_usdt:+.4f} USDT | dur={dur}s")
        except Exception as e:
            log.warning(f"[WATCH] {e}")


# ── FastAPI endpoints ─────────────────────────────────────────────────────
def _fetch_price() -> Optional[float]:
    return feed.mark_price if hasattr(feed, "mark_price") else None

@app.get("/")
@app.get("/health")
async def health():
    with _state_lock:
        ot = dict(open_trade) if open_trade else None
    return JSONResponse({"status": "alive", "config": CONFIG_TAG,
                         "in_trade": ot is not None,
                         "ws_trade": bool(trade_ws and trade_ws.authed),
                         "feed_ready": getattr(feed, "is_ready", False)})

@app.get("/api/live")
async def api_live():
    with _state_lock:
        ot = dict(open_trade) if open_trade else None
    px = _fetch_price()
    unr = None
    if ot and px:
        unr = round((px - ot["fill_price"]) if ot["direction"] == "BUY" else (ot["fill_price"] - px), 1)
    return JSONResponse({"price": px, "unreal": unr,
                         "sl": ot.get("sl_price") if ot else None,
                         "tp": ot.get("tp_price") if ot else None,
                         "has_trade": bool(ot)})

@app.get("/api/stream")
async def api_stream():
    async def event_gen():
        import json as _json
        last_px = None
        while True:
            try:
                px = _fetch_price()
                if px and px != last_px:
                    last_px = px
                    with _state_lock:
                        ot = dict(open_trade) if open_trade else None
                    unr = None
                    if ot and px:
                        unr = round((px - ot["fill_price"]) if ot["direction"]=="BUY" else (ot["fill_price"] - px), 1)
                    data = _json.dumps({"price": round(px, 1), "unreal": unr,
                                        "sl": ot.get("sl_price") if ot else None,
                                        "tp": ot.get("tp_price") if ot else None,
                                        "has_trade": bool(ot)})
                    yield f"data: {data}\n\n"
                await asyncio.sleep(0.5)
            except Exception:
                await asyncio.sleep(1)
    return StreamingResponse(event_gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.get("/test/fire/{side}")
async def test_fire(side: str, confirm: str = ""):
    side = side.upper()
    if side not in ("BUY", "SELL"):
        raise HTTPException(400, "Use /test/fire/buy or /test/fire/sell")
    if confirm.lower() != "yes":
        return JSONResponse({
            "warning": f"LIVE order — add ?confirm=yes to place REAL {side} on Bybit",
            "retry_url": f"/test/fire/{side.lower()}?confirm=yes"
        }, status_code=403)
    global _entry_busy
    with _state_lock:
        if open_trade or _entry_busy:
            return JSONResponse({"error": "Already in trade or entry busy — close first with /test/close"}, status_code=400)
        _entry_busy = True   # set inside lock before any async work
    price = _fetch_price() or 77000.0
    trade_id = f"TEST_{side[0]}{int(time.time()*1000)}"
    bybit_side = "Buy" if side == "BUY" else "Sell"
    asyncio.create_task(_do_entry(bybit_side, price, time.time(),
                                  trade_id=trade_id, is_test=True,
                                  chop_avg_tr=50.0, burst_threshold=100.0, candle_body=120.0))
    return JSONResponse({"test": "fired", "side": side, "price": price, "trade_id": trade_id})

@app.get("/test/close")
async def test_close(confirm: str = ""):
    global open_trade
    if confirm.lower() != "yes":
        return JSONResponse({
            "warning": "LIVE close — add ?confirm=yes to cancel SL/TP and close position on Bybit",
            "retry_url": "/test/close?confirm=yes"
        }, status_code=403)
    with _state_lock:
        if not open_trade:
            return JSONResponse({"error": "No open trade"}, status_code=400)
        ot = dict(open_trade)

    price = _fetch_price() or float(ot.get("fill_price", 0))
    bybit_side = "Sell" if ot["direction"] == "BUY" else "Buy"
    r = B.place_market_order_rest(bybit_side, LOT_SIZE_STR)
    if not r or r.get("retCode", -1) != 0:
        return JSONResponse({"error": "Close order failed", "resp": str(r)}, status_code=500)

    with _state_lock:
        open_trade = None

    tg(f"🔴 <b>MANUAL CLOSE</b> [Bybit]\n{ot['direction']} closed @ {price:,.1f}\n⚙️ {CONFIG_TAG}")
    return JSONResponse({"test": "closed", "exit_price": price})

@app.get("/test/telegram")
async def test_telegram():
    tg(f"✅ Bybit Vol Surge Telegram test — connection OK\n⚙️ {CONFIG_TAG}")
    return JSONResponse({"status": "sent"})

@app.get("/admin/clear-all-trades")
async def clear_all_trades():
    try:
        count = 0
        try:
            with open(TRADE_FILE, "r", encoding="utf-8") as f:
                count = max(0, sum(1 for _ in f) - 1)
        except Exception:
            pass
        with open(TRADE_FILE, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=TRADE_HDRS).writeheader()
        return JSONResponse({"status": "ok", "wiped": count})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/admin/purge-test-trades")
async def purge_test_trades():
    try:
        with open(TRADE_FILE, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        genuine = [r for r in rows if not str(r.get("trade_id","")).startswith("TEST_")]
        removed = len(rows) - len(genuine)
        if removed > 0:
            with open(TRADE_FILE, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=TRADE_HDRS, extrasaction="ignore")
                w.writeheader(); w.writerows(genuine)
        return JSONResponse({"status": "ok", "purged": removed, "remaining": len(genuine)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── Dashboard ─────────────────────────────────────────────────────────────
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    trades = []
    try:
        with open(TRADE_FILE, "r", encoding="utf-8") as f:
            trades = list(csv.DictReader(f))
    except Exception:
        pass

    lifecycle_rows = []
    try:
        with open(LIFECYCLE_FILE, "r", encoding="utf-8") as f:
            lifecycle_rows = list(csv.DictReader(f))[-50:]
    except Exception:
        pass

    with _state_lock:
        ot = dict(open_trade) if open_trade else None

    now_ist = (datetime.utcnow() + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S IST")

    def _f(v, dec=1):
        try: return f"{float(v):,.{dec}f}"
        except: return "—"
    def _pts(v):
        try:
            f = float(v); s = "+" if f >= 0 else ""
            return f"{s}{f:.2f}"
        except: return "—"
    def _pc(v):
        try:
            f = float(v); c = "#4ade80" if f > 0 else "#f87171" if f < 0 else "#9ca3af"
            s = "+" if f >= 0 else ""
            return f'<span style="color:{c};font-weight:700;">{s}{f:.2f}</span>'
        except: return '<span style="color:#6b7280;">—</span>'
    def _dir(v):
        if v == "BUY":  return '<span style="color:#4ade80;font-weight:700;">▲ BUY</span>'
        if v == "SELL": return '<span style="color:#f87171;font-weight:700;">▼ SELL</span>'
        return "—"
    def _dir_arrow(v):
        if v == "BUY":  return '<span style="color:#4ade80;font-size:16px;font-weight:700;">▲</span>'
        if v == "SELL": return '<span style="color:#f87171;font-size:16px;font-weight:700;">▼</span>'
        return "—"
    def _outcome(v):
        if v == "TP":   return '<span style="background:#14532d;color:#4ade80;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;">TP ✓</span>'
        if v == "SL":   return '<span style="background:#450a0a;color:#f87171;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;">SL ✗</span>'
        if v == "TEST": return '<span style="background:#1e3a5f;color:#60a5fa;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;">TEST</span>'
        return f'<span style="color:#6b7280;">{v or "—"}</span>'
    def _grade(v):
        c = {"INTACT":"#4ade80","MILD":"#facc15","DEGRADED":"#fb923c","BROKEN":"#f87171","CRITICAL":"#dc2626","TEST":"#60a5fa"}.get(v,"#6b7280")
        return f'<span style="color:{c};font-weight:600;font-size:11px;">{v or "—"}</span>'
    def _exit_type(v):
        if v == "TP_LIVE":  return '<span style="color:#4ade80;font-size:11px;">TP_LIVE ✓</span>'
        if v == "SL_LIVE":  return '<span style="color:#f87171;font-size:11px;">SL_LIVE ✗</span>'
        return f'<span style="color:#6b7280;font-size:11px;">{v or "—"}</span>'
    def _ms(v):
        try: return f"{float(v):.0f}ms"
        except: return "—"

    # stats
    total   = len(trades)
    tp_cnt  = sum(1 for t in trades if t.get("python_actual_outcome") == "TP")
    sl_cnt  = sum(1 for t in trades if t.get("python_actual_outcome") == "SL")
    pts_list = []
    for t in trades:
        try: pts_list.append(float(t["pts"]))
        except: pass
    tot_pts = round(sum(pts_list), 2)
    avg_pts = round(sum(pts_list)/len(pts_list), 2) if pts_list else 0
    win_rt  = f"{round(tp_cnt/total*100)}%" if total else "—"
    slip_list = []
    for t in trades:
        try: slip_list.append(float(t["entry_slippage_pts"]))
        except: pass
    avg_slip = round(sum(slip_list)/len(slip_list), 2) if slip_list else 0
    lat_list = []
    for t in trades:
        try: lat_list.append(float(t["entry_latency_ms"]))
        except: pass
    avg_lat = round(sum(lat_list)/len(lat_list), 1) if lat_list else 0
    sig_lat_list = []
    for t in trades:
        try: sig_lat_list.append(float(t.get("signal_latency_ms", 0) or 0))
        except: pass
    avg_sig_lat = round(sum(sig_lat_list)/len(sig_lat_list), 1) if sig_lat_list else 0

    real_trades  = [t for t in trades if not str(t.get("trade_id","")).startswith("TEST_")]
    buy_trades   = [t for t in real_trades if t.get("direction") == "BUY"]
    sell_trades  = [t for t in real_trades if t.get("direction") == "SELL"]
    buy_wr  = f"{round(sum(1 for t in buy_trades  if t.get('python_actual_outcome')=='TP')/len(buy_trades)*100)}%"  if buy_trades  else "—"
    sell_wr = f"{round(sum(1 for t in sell_trades if t.get('python_actual_outcome')=='TP')/len(sell_trades)*100)}%" if sell_trades else "—"
    tp_pts_l = [float(t["pts"]) for t in real_trades if t.get("python_actual_outcome")=="TP" and t.get("pts")]
    sl_pts_l = [float(t["pts"]) for t in real_trades if t.get("python_actual_outcome")=="SL" and t.get("pts")]
    avg_tp_pts = round(sum(tp_pts_l)/len(tp_pts_l), 1) if tp_pts_l else 0
    avg_sl_pts = round(sum(sl_pts_l)/len(sl_pts_l), 1) if sl_pts_l else 0
    pnl_list = []
    for t in trades:
        try: pnl_list.append(float(t["pnl_usdt"]))
        except: pass
    tot_pnl = round(sum(pnl_list), 4)

    grade_order  = ["INTACT","MILD","DEGRADED","BROKEN","CRITICAL","TEST"]
    grade_counts = {}
    grade_wr     = {}
    for t in real_trades:
        g = t.get("structure_grade","")
        if g: grade_counts[g] = grade_counts.get(g, 0) + 1
    for g in grade_order:
        g_trades = [t for t in real_trades if t.get("structure_grade") == g]
        g_tp = sum(1 for t in g_trades if t.get("python_actual_outcome") == "TP")
        if g_trades:
            grade_wr[g] = (len(g_trades), g_tp, round(g_tp/len(g_trades)*100))

    # open trade panel
    open_panel = ""
    if ot:
        d       = ot.get("direction","?")
        fill_px = ot.get("fill_price", 0)
        sl_px   = ot.get("sl_price", 0)
        tp_px   = ot.get("tp_price", 0)
        slip    = ot.get("entry_slippage_pts", 0)
        sig_lat = ot.get("signal_latency_ms", 0)
        en_l    = ot.get("entry_latency_ms", 0)
        grade_v = ot.get("structure_grade","?")
        dir_col   = "#4ade80" if d == "BUY" else "#f87171"
        grade_col = {"INTACT":"#4ade80","MILD":"#facc15","DEGRADED":"#fb923c","BROKEN":"#f87171","CRITICAL":"#dc2626","TEST":"#60a5fa"}.get(grade_v,"#9ca3af")
        elapsed   = round(time.time() - ot.get("ts", time.time()))
        elapsed_s = f"{elapsed//60}m {elapsed%60}s" if elapsed >= 60 else f"{elapsed}s"
        px_now    = _fetch_price() or 0
        unreal    = round((px_now - fill_px) if d == "BUY" else (fill_px - px_now), 1) if px_now else 0
        unreal_col = "#4ade80" if unreal >= 0 else "#f87171"
        open_panel = f"""
<div style="background:#0a1f0a;border:2px solid #166534;border-radius:10px;margin:0 24px 20px;padding:16px 20px;">
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;">
    <span style="background:#166534;color:#4ade80;padding:3px 10px;border-radius:4px;font-size:12px;font-weight:700;">🔴 LIVE TRADE OPEN</span>
    <span style="color:{dir_col};font-size:18px;font-weight:700;">{'▲' if d=='BUY' else '▼'} {d}</span>
    <span style="color:#6b7280;font-size:12px;">in trade for {elapsed_s}</span>
    {"<span style='background:#1e3a5f;color:#60a5fa;padding:2px 8px;border-radius:4px;font-size:11px;'>🧪 TEST</span>" if ot.get("is_test") else ""}
  </div>
  <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px;">
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Fill Price</div><div style="color:#f9fafb;font-size:16px;font-weight:700;">{_f(fill_px)}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Live Price</div><div id="ot-live-px" style="color:#60a5fa;font-size:16px;font-weight:700;">{_f(px_now)}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Unrealized</div><div id="ot-unreal" style="color:{unreal_col};font-size:16px;font-weight:700;">{'+' if unreal>=0 else ''}{unreal:.1f} pts</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">SL Level</div><div style="color:#f87171;font-size:16px;">{_f(sl_px)}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">TP Level</div><div style="color:#4ade80;font-size:16px;">{_f(tp_px)}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Dist to SL</div><div id="ot-dist-sl" style="color:#f87171;font-size:14px;">{round(abs(px_now-sl_px),1) if px_now else '—'} pts</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Dist to TP</div><div id="ot-dist-tp" style="color:#4ade80;font-size:14px;">{round(abs(tp_px-px_now),1) if px_now else '—'} pts</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Entry Slippage</div><div style="color:#facc15;font-size:16px;">{'+' if float(slip or 0)>=0 else ''}{float(slip or 0):.2f} pts</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Structure Grade</div><div style="color:{grade_col};font-size:16px;font-weight:700;">{grade_v}</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Signal Latency</div><div style="color:#60a5fa;font-size:14px;">{float(sig_lat or 0):.0f}ms</div></div>
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Entry Latency</div><div style="color:#60a5fa;font-size:14px;">{float(en_l or 0):.0f}ms</div></div>
  </div>
</div>"""

    # grade rows
    grade_rows = ""
    for g in grade_order:
        cnt = grade_counts.get(g, 0)
        pct = round(cnt/total*100) if total else 0
        wr_data = grade_wr.get(g)
        wr_str  = f"{wr_data[2]}%" if wr_data else "—"
        col = {"INTACT":"#4ade80","MILD":"#facc15","DEGRADED":"#fb923c","BROKEN":"#f87171","CRITICAL":"#dc2626","TEST":"#60a5fa"}.get(g,"#6b7280")
        bar = "█" * min(pct, 30)
        grade_rows += f"""<tr>
          <td style="color:{col};font-weight:600;">{g}</td>
          <td style="color:#e2e8f0;text-align:right;">{cnt}</td>
          <td style="color:#9ca3af;text-align:right;">{pct}%</td>
          <td style="color:#e2e8f0;text-align:right;">{wr_str}</td>
          <td style="color:{col};font-size:10px;letter-spacing:1px;">{bar}</td>
        </tr>"""

    # lifecycle rows
    lc_rows = ""
    for e in reversed(lifecycle_rows[-20:]):
        ev = e.get("event","")
        ev_col = "#4ade80" if "TP" in ev or "ACKED" in ev else "#f87171" if "SL" in ev else "#60a5fa"
        lc_rows += f"""<tr style="border-bottom:1px solid #1f2937;">
          <td style="color:#6b7280;font-size:11px;">{e.get('timestamp_ist','')}</td>
          <td style="color:{ev_col};font-weight:600;font-size:11px;">{ev}</td>
          <td style="color:#9ca3af;font-size:11px;font-family:monospace;">{e.get('trade_id','')[:16]}</td>
          <td style="color:#e2e8f0;text-align:right;">{_f(e.get('price',''))}</td>
          <td style="color:#60a5fa;text-align:right;">{e.get('latency_from_prev_ms','')}</td>
          <td style="color:#6b7280;font-size:11px;">{e.get('notes','')}</td>
        </tr>"""
    lc_empty = "" if lc_rows else '<tr><td colspan="6" style="color:#4b5563;text-align:center;padding:16px;">No lifecycle events yet</td></tr>'

    # journal rows (detailed)
    journal_rows = ""
    for i, t in enumerate(reversed(trades), 1):
        outcome  = t.get("python_actual_outcome","")
        is_test_t = str(t.get("trade_id","")).startswith("TEST_")
        rbg = "#0a1a0a" if outcome=="TP" else "#1a0a0a" if outcome=="SL" else "#0d1117"
        journal_rows += f"""
        <tr class="trade-row" data-tradetype="{'test' if is_test_t else 'live'}" style="background:{rbg};border-bottom:1px solid #1f2937;">
          <td style="color:#6b7280;text-align:center;">{total-i+1}</td>
          <td>{_dir(t.get('direction',''))}</td>
          <td style="color:#d1d5db;font-size:11px;">{t.get('entry_time_ist','—')}</td>
          <td style="color:#e5e7eb;text-align:right;">{_f(t.get('fill_price',''))}</td>
          <td style="color:#34d399;text-align:right;">{_f(t.get('tp_price',''))}</td>
          <td style="color:#f87171;text-align:right;">{_f(t.get('sl_price',''))}</td>
          <td style="color:#e5e7eb;text-align:right;">{_f(t.get('exit_price',''))}</td>
          <td style="text-align:right;">{_pc(t.get('pts',''))}</td>
          <td style="color:#9ca3af;text-align:right;">{_pc(t.get('pnl_usdt',''))}</td>
          <td style="text-align:center;">{_outcome(outcome)}</td>
          <td style="text-align:center;">{_exit_type(t.get('exit_type',''))}</td>
          <td style="text-align:right;">{_pc(t.get('entry_slippage_pts',''))}</td>
          <td style="color:#60a5fa;text-align:right;">{_ms(t.get('signal_latency_ms',''))}</td>
          <td style="color:#9ca3af;text-align:right;">{_ms(t.get('entry_latency_ms',''))}</td>
          <td style="text-align:center;">{_grade(t.get('structure_grade',''))}</td>
        </tr>"""

    journal_empty = "" if trades else '<tr><td colspan="15" style="text-align:center;color:#6b7280;padding:40px;">No trades yet — waiting for first signal</td></tr>'

    # TV-style rows
    tv_rows = ""
    for i, t in enumerate(reversed(trades), 1):
        outcome  = t.get("python_actual_outcome","")
        is_test_tv = str(t.get("trade_id","")).startswith("TEST_")
        rbg = "#0a1a0a" if outcome=="TP" else "#1a0a0a" if outcome=="SL" else "#0d1117"
        pts_val = 0.0
        try: pts_val = float(t.get("pts", 0) or 0)
        except: pass
        pnl_val = 0.0
        try: pnl_val = float(t.get("pnl_usdt", 0) or 0)
        except: pass
        status_str = ("TP ✓" if outcome=="TP" else "SL ✗" if outcome=="SL" else "TEST" if outcome=="TEST" else "—")
        status_col = "#4ade80" if outcome=="TP" else "#f87171" if outcome=="SL" else "#60a5fa"
        pts_col  = "#4ade80" if pts_val >= 0 else "#f87171"
        pnl_col  = "#4ade80" if pnl_val >= 0 else "#f87171"
        tv_rows += f"""
        <tr class="trade-row" data-tradetype="{'test' if is_test_tv else 'live'}" style="background:{rbg};border-bottom:1px solid #1f2937;">
          <td>{_dir_arrow(t.get('direction',''))}</td>
          <td style="color:#9ca3af;font-size:11px;">{t.get('entry_time_ist','—')[:5]}</td>
          <td style="color:#d1d5db;font-size:11px;">{t.get('entry_time_ist','—')[6:11] if len(t.get('entry_time_ist',''))>6 else '—'}</td>
          <td style="color:#d1d5db;font-size:11px;">{t.get('exit_time_ist','—')[6:11] if len(t.get('exit_time_ist',''))>6 else '—'}</td>
          <td style="color:#e5e7eb;text-align:right;">{_f(t.get('fill_price',''))}</td>
          <td style="color:#e5e7eb;text-align:right;">{_f(t.get('exit_price',''))}</td>
          <td style="color:#f87171;text-align:right;">{_f(t.get('sl_price',''))}</td>
          <td style="color:{pts_col};text-align:right;font-weight:700;">{'+' if pts_val>=0 else ''}{pts_val:.1f}</td>
          <td style="color:#9ca3af;text-align:right;">{LOT_SIZE_STR}</td>
          <td style="color:{pnl_col};text-align:right;font-weight:700;">{'+' if pnl_val>=0 else ''}{pnl_val:.4f}</td>
          <td style="color:{status_col};text-align:center;font-size:11px;font-weight:700;">{status_str}</td>
        </tr>"""
    tv_empty = "" if trades else '<tr><td colspan="11" style="text-align:center;color:#6b7280;padding:40px;">No trades yet — waiting for first signal</td></tr>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bybit Bot — Live Dashboard ({INTERVAL}m · MB{MIN_BODY:.0f})</title>
<style>
  *{{margin:0;padding:0;box-sizing:border-box}}
  body{{background:#080c10;color:#e2e8f0;font-family:'Segoe UI',system-ui,monospace;font-size:13px}}
  .hdr{{background:#0d1117;border-bottom:2px solid #1d4ed8;padding:16px 28px;display:flex;align-items:center;justify-content:space-between}}
  .hdr h1{{font-size:18px;font-weight:700;color:#f9fafb}}
  .toolbar{{padding:10px 28px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;border-bottom:1px solid #1f2937}}
  .sec{{font-size:10px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:1.2px;padding:20px 28px 10px;display:flex;align-items:center;justify-content:space-between}}
  .stats{{display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:10px;padding:0 28px 20px}}
  .stat{{background:#0d1117;border:1px solid #1e293b;border-radius:8px;padding:14px 16px}}
  .sv{{font-size:20px;font-weight:700;line-height:1.2}}
  .sl{{font-size:10px;color:#6b7280;margin-top:4px;text-transform:uppercase;letter-spacing:0.6px}}
  .panel{{background:#0d1117;border:1px solid #1e293b;border-radius:10px;margin:0 28px 16px;padding:20px 24px}}
  .panel h3{{font-size:11px;color:#9ca3af;font-weight:700;margin-bottom:14px;text-transform:uppercase;letter-spacing:0.8px;border-bottom:1px solid #1f2937;padding-bottom:8px}}
  .grid2{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin:0 28px 16px}}
  .grid3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;margin:0 28px 16px}}
  .grid2 .panel,.grid3 .panel{{margin:0}}
  .tw{{padding:0 28px 24px;overflow-x:auto}}
  table{{width:100%;border-collapse:collapse;font-size:11px}}
  th{{background:#0d1117;color:#6b7280;font-weight:600;text-transform:uppercase;font-size:10px;padding:9px 12px;border-bottom:2px solid #1f2937;white-space:nowrap}}
  td{{padding:8px 12px;white-space:nowrap;vertical-align:middle}}
  tr:hover td{{background:#161f2e!important}}
  .kv{{display:flex;justify-content:space-between;align-items:center;padding:7px 0;border-bottom:1px solid #1a2030;font-size:12px}}
  .kv:last-child{{border-bottom:none}}
  .kl{{color:#6b7280}}
  .kv2{{color:#e2e8f0;font-weight:600}}
  .insight{{background:#0f1f0f;border-left:3px solid #4ade80;padding:9px 14px;border-radius:4px;font-size:12px;color:#86efac;margin:5px 0;line-height:1.5}}
  .insight.warn{{background:#1f0f0f;border-color:#f87171;color:#fca5a5}}
  .insight.info{{background:#0f1525;border-color:#60a5fa;color:#93c5fd}}
  .footer{{text-align:center;padding:18px;color:#374151;font-size:10px;border-top:1px solid #1f2937;margin-top:4px}}
  .toggle-btn{{background:#1e293b;color:#e2e8f0;border:1px solid #334155;border-radius:6px;padding:5px 14px;font-size:11px;cursor:pointer;font-family:inherit;transition:background 0.15s}}
  .toggle-btn.active{{background:#1d4ed8;color:#fff;border-color:#2563eb}}
  .toggle-group{{display:flex;gap:6px}}
  .hidden{{display:none}}
</style>
<script>
  setTimeout(()=>location.reload(),30000);
  setInterval(()=>{{document.getElementById('clk').textContent=new Date().toLocaleTimeString('en-IN',{{timeZone:'Asia/Kolkata'}})}},1000);
  window.onload=()=>document.getElementById('clk').textContent=new Date().toLocaleTimeString('en-IN',{{timeZone:'Asia/Kolkata'}});

  const CURRENT_IN_TRADE  = {'true' if ot else 'false'};
  const CURRENT_DIRECTION = '{ot.get("direction","") if ot else ""}';
  const CURRENT_FILL      = '{ot.get("fill_price","") if ot else ""}';
  const CURRENT_SL        = '{ot.get("sl_price","") if ot else ""}';
  const CURRENT_TP        = '{ot.get("tp_price","") if ot else ""}';

  function playSignalSound() {{
    try {{
      const ctx = new (window.AudioContext || window.webkitAudioContext)();
      [[880,0],[1100,0.25],[880,0.5],[1100,0.75],[1320,1.0]].forEach(([freq,t]) => {{
        const o = ctx.createOscillator(), g = ctx.createGain();
        o.connect(g); g.connect(ctx.destination);
        o.type = 'sine'; o.frequency.value = freq;
        g.gain.setValueAtTime(0.4, ctx.currentTime + t);
        g.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + t + 0.2);
        o.start(ctx.currentTime + t); o.stop(ctx.currentTime + t + 0.25);
      }});
    }} catch(e) {{}}
  }}
  function playExitSound(isTP) {{
    try {{
      const ctx = new (window.AudioContext || window.webkitAudioContext)();
      const freqs = isTP ? [[1320,0],[1100,0.2],[880,0.4]] : [[440,0],[330,0.2],[220,0.4]];
      freqs.forEach(([freq,t]) => {{
        const o = ctx.createOscillator(), g = ctx.createGain();
        o.connect(g); g.connect(ctx.destination);
        o.type = 'sine'; o.frequency.value = freq;
        g.gain.setValueAtTime(0.4, ctx.currentTime + t);
        g.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + t + 0.25);
        o.start(ctx.currentTime + t); o.stop(ctx.currentTime + t + 0.3);
      }});
    }} catch(e) {{}}
  }}
  function sendNotification(title, body) {{
    if (!('Notification' in window)) return;
    if (Notification.permission === 'granted')
      new Notification(title, {{ body: body }});
  }}
  function enableAlerts() {{
    const btn = document.getElementById('alert-btn');
    if (localStorage.getItem('vs_alerts') === '1' && Notification.permission === 'granted') {{
      localStorage.setItem('vs_alerts', '0');
      btn.textContent = '🔕 Enable Alerts'; btn.style.background=''; btn.style.color=''; btn.style.borderColor=''; return;
    }}
    Notification.requestPermission().then(p => {{
      if (p === 'granted') {{
        btn.textContent='🔔 Alerts ON'; btn.style.background='#14532d'; btn.style.color='#4ade80'; btn.style.borderColor='#166534';
        localStorage.setItem('vs_alerts','1'); playSignalSound();
        sendNotification('✅ Bybit Bot Alerts ON','You will be notified on signals');
      }} else {{ btn.textContent='🔕 Enable Alerts'; localStorage.setItem('vs_alerts','0'); }}
    }});
  }}
  window.addEventListener('load',()=>{{
    const btn=document.getElementById('alert-btn');
    if(localStorage.getItem('vs_alerts')==='1'&&Notification.permission==='granted'){{
      btn.textContent='🔔 Alerts ON';btn.style.background='#14532d';btn.style.color='#4ade80';btn.style.borderColor='#166534';
    }}
    const alertsEnabled=localStorage.getItem('vs_alerts')==='1'&&Notification.permission==='granted';
    const prevInTrade=localStorage.getItem('vs_in_trade')==='true';
    if(alertsEnabled&&CURRENT_IN_TRADE&&!prevInTrade){{
      playSignalSound();
      sendNotification('🔥 Bybit '+CURRENT_DIRECTION+' SIGNAL','Entry: '+CURRENT_FILL+' SL: '+CURRENT_SL+' TP: '+CURRENT_TP);
      document.body.style.boxShadow='inset 0 0 60px rgba(74,222,128,0.3)';
      setTimeout(()=>document.body.style.boxShadow='',3000);
    }}
    if(alertsEnabled&&!CURRENT_IN_TRADE&&prevInTrade){{
      playExitSound(false);
      sendNotification('📊 Bybit Trade Closed','Previous '+prevInTrade+' trade exited');
    }}
    localStorage.setItem('vs_in_trade',CURRENT_IN_TRADE);
    localStorage.setItem('vs_trade_dir',CURRENT_DIRECTION);
  }});

  let _page=1; const _PER_PAGE=10; let _filter='all';
  function _visibleRows(){{
    const scope=document.getElementById('view-detailed');
    return Array.from(scope?scope.querySelectorAll('.trade-row'):[]).filter(r=>{{
      if(_filter==='live') return r.dataset.tradetype!=='test';
      if(_filter==='test') return r.dataset.tradetype==='test';
      return true;
    }});
  }}
  function _applyPage(){{
    const rows=_visibleRows(); const total=rows.length;
    const pages=Math.max(1,Math.ceil(total/_PER_PAGE));
    if(_page>pages)_page=pages;
    const allRows=Array.from(document.querySelectorAll('.trade-row'));
    const detRows=allRows.filter(r=>r.closest('#view-detailed'));
    const tvRows=allRows.filter(r=>r.closest('#view-tv'));
    detRows.forEach(r=>r.style.display='none'); tvRows.forEach(r=>r.style.display='none');
    const visIdx=new Set(rows.map(r=>detRows.indexOf(r)));
    detRows.forEach((r,i)=>{{if(visIdx.has(i)&&i>=(_page-1)*_PER_PAGE&&i<_page*_PER_PAGE)r.style.display='';}});
    tvRows.forEach((r,i)=>{{if(visIdx.has(i)&&i>=(_page-1)*_PER_PAGE&&i<_page*_PER_PAGE)r.style.display='';}});
    const info=document.getElementById('page-info');
    const btnP=document.getElementById('btn-prev-page');
    const btnN=document.getElementById('btn-next-page');
    if(info)info.textContent=total===0?'No trades':'Page '+_page+' / '+pages+' · '+total+' trades';
    if(btnP)btnP.disabled=_page<=1; if(btnN)btnN.disabled=_page>=pages;
  }}
  function filterTrades(mode){{
    _filter=mode;_page=1;
    ['btn-filter-all','btn-filter-live','btn-filter-test'].forEach(id=>document.getElementById(id).classList.remove('active'));
    document.getElementById('btn-filter-'+mode).classList.add('active'); _applyPage();
  }}
  function prevPage(){{if(_page>1){{_page--;_applyPage();}}}}
  function nextPage(){{const pages=Math.ceil(_visibleRows().length/_PER_PAGE);if(_page<pages){{_page++;_applyPage();}}}}
  function switchView(mode){{
    const det=document.getElementById('view-detailed'),tv=document.getElementById('view-tv');
    const thDet=document.getElementById('th-detailed'),thTv=document.getElementById('th-tv');
    const btnDet=document.getElementById('btn-detailed'),btnTv=document.getElementById('btn-tv');
    if(mode==='detailed'){{det.classList.remove('hidden');tv.classList.add('hidden');thDet.classList.remove('hidden');thTv.classList.add('hidden');btnDet.classList.add('active');btnTv.classList.remove('active');}}
    else{{tv.classList.remove('hidden');det.classList.add('hidden');thTv.classList.remove('hidden');thDet.classList.add('hidden');btnTv.classList.add('active');btnDet.classList.remove('active');}}
  }}
  window.addEventListener('DOMContentLoaded',()=>_applyPage());

  (function liveSSE(){{
    const elPx=document.getElementById('ot-live-px'),elUnr=document.getElementById('ot-unreal'),
          elSL=document.getElementById('ot-dist-sl'),elTP=document.getElementById('ot-dist-tp');
    const src=new EventSource('/api/stream');
    src.onmessage=function(e){{
      try{{
        const d=JSON.parse(e.data);
        if(!d.price)return;
        if(elPx)elPx.textContent=d.price.toLocaleString('en-US',{{minimumFractionDigits:1,maximumFractionDigits:1}});
        if(elUnr&&d.unreal!==null&&d.unreal!==undefined){{
          elUnr.textContent=(d.unreal>=0?'+':'')+d.unreal.toFixed(1)+' pts';
          elUnr.style.color=d.unreal>=0?'#4ade80':'#f87171';
        }}
        if(elSL&&d.sl)elSL.textContent=Math.abs(d.price-d.sl).toFixed(1)+' pts';
        if(elTP&&d.tp)elTP.textContent=Math.abs(d.tp-d.price).toFixed(1)+' pts';
      }}catch(err){{}}
    }};
    src.onerror=function(){{src.close();}};
  }})();

  let _modalCb=null;
  function _showModal(title,msg,yesLabel,yesColor,cb){{
    document.getElementById('modal-title').textContent=title;
    document.getElementById('modal-body').innerHTML=msg;
    document.getElementById('modal-yes').textContent=yesLabel;
    document.getElementById('modal-yes').style.background=yesColor;
    document.getElementById('modal-overlay').style.display='flex';
    _modalCb=cb;
  }}
  function _modalYes(){{document.getElementById('modal-overlay').style.display='none';if(_modalCb)_modalCb(true);}}
  function _modalCancel(){{document.getElementById('modal-overlay').style.display='none';if(_modalCb)_modalCb(false);}}

  async function testFire(side){{
    const isBuy=side==='BUY';
    _showModal(
      '⚠️ LIVE ORDER — '+side,
      '<b style="color:#f87171">REAL order will be placed on Bybit!</b><br><br>Symbol: BTCUSDT · Lot: {LOT_SIZE_STR} BTC<br>SL & TP placed automatically.<br><br>Are you sure?',
      '✓ Yes, '+side, isBuy?'#166534':'#7f1d1d', async(ok)=>{{
      if(!ok)return;
      const r=await fetch('/test/fire/'+side.toLowerCase()+'?confirm=yes');
      const j=await r.json();
      if(j.error){{_showModal('❌ Error',j.error,'OK','#374151',()=>{{}});return;}}
      _showModal('✅ Trade Fired',side+' @ <b>'+j.price+'</b><br>SL/TP set by Bybit server-side.','OK','#1d4ed8',()=>location.reload());
    }});
  }}
  async function testClose(){{
    _showModal('⚠️ LIVE CLOSE','<b style="color:#f87171">This will market-close the open position on Bybit.</b><br>Continue?',
      '✓ Yes, Close','#7f1d1d',async(ok)=>{{
      if(!ok)return;
      const r=await fetch('/test/close?confirm=yes');
      const j=await r.json();
      if(j.error){{_showModal('❌ Error',j.error,'OK','#374151',()=>{{}});return;}}
      _showModal('✅ Trade Closed','Closed @ <b>'+j.exit_price+'</b>','OK','#1d4ed8',()=>location.reload());
    }});
  }}
  async function testTelegram(){{
    const r=await fetch('/test/telegram');
    const j=await r.json();
    alert(j.status==='sent'?'✅ Telegram message sent!':'Error: '+JSON.stringify(j));
  }}
</script>
</head>
<body>

<!-- HEADER -->
<div class="hdr">
  <div>
    <h1>⚡ Bybit Bot — Live Dashboard ({INTERVAL}m · MB{MIN_BODY:.0f})</h1>
    <div style="color:#6b7280;font-size:11px;margin-top:3px;">BTCUSDT · Bybit · <span style="color:#60a5fa;font-weight:600;">{INTERVAL}m HA candles</span> · WS-native · page reload 30s · {now_ist}</div>
  </div>
  <div style="text-align:right;display:flex;flex-direction:column;align-items:flex-end;gap:6px;">
    <span style="background:#0a2a1f;color:#4ade80;padding:4px 14px;border-radius:20px;font-size:12px;font-weight:700;">🟢 LIVE</span>
    <span style="color:#6b7280;font-size:11px;">🕐 IST <b id="clk"></b></span>
    <button id="alert-btn" class="toggle-btn" onclick="enableAlerts()" style="font-size:11px;padding:4px 12px;">🔕 Enable Alerts</button>
    <span style="color:#{'4ade80' if ot else '6b7280'};font-size:11px;">{'🔴 POSITION OPEN' if ot else '⚪ IDLE'}</span>
    <span style="color:#{'4ade80' if _DATA_PERSISTENT else 'f59e0b'};font-size:10px;">{'💾 Data Persistent' if _DATA_PERSISTENT else '⚠️ Data Ephemeral'}</span>
  </div>
</div>

<!-- TOOLBAR -->
<div class="toolbar">
  <div style="font-size:11px;color:#6b7280;">
    📊 Closed trades: <b style="color:#e2e8f0">{total}</b>
    &nbsp;|&nbsp; <span style="color:#4ade80">⚡ WS-native orders</span>
    &nbsp;|&nbsp; SL{FIXED_SL:.0f}/TP{FIXED_TP:.0f} · burst{BURST_MULT} · MB{MIN_BODY:.0f}
  </div>
  <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
    <span style="color:#f59e0b;font-size:11px;font-weight:700;">⚠️ LIVE (real orders):</span>
    <button onclick="testFire('BUY')"  style="background:#14532d;color:#4ade80;border:1px solid #166534;border-radius:5px;padding:4px 12px;font-size:11px;cursor:pointer;">▲ Test BUY</button>
    <button onclick="testFire('SELL')" style="background:#450a0a;color:#f87171;border:1px solid #7f1d1d;border-radius:5px;padding:4px 12px;font-size:11px;cursor:pointer;">▼ Test SELL</button>
    <button onclick="testClose()"      style="background:#1e293b;color:#9ca3af;border:1px solid #334155;border-radius:5px;padding:4px 12px;font-size:11px;cursor:pointer;">✖ Close Trade</button>
    <button onclick="testTelegram()"   style="background:#1e293b;color:#60a5fa;border:1px solid #1e40af;border-radius:5px;padding:4px 12px;font-size:11px;cursor:pointer;">📨 Test TG</button>
  </div>
</div>

<!-- OPEN TRADE -->
{open_panel}

<!-- TRADE JOURNAL -->
<div class="sec">
  <span>Trade Journal — All Trades (newest first)</span>
  <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
    <div class="toggle-group" style="border-right:1px solid #1f2937;padding-right:10px;margin-right:2px;">
      <button class="toggle-btn active" id="btn-filter-all"  onclick="filterTrades('all')">All</button>
      <button class="toggle-btn"        id="btn-filter-live" onclick="filterTrades('live')" style="color:#4ade80;">🟢 Live</button>
      <button class="toggle-btn"        id="btn-filter-test" onclick="filterTrades('test')" style="color:#60a5fa;">🧪 Test</button>
    </div>
    <div class="toggle-group" style="border-right:1px solid #1f2937;padding-right:10px;margin-right:2px;">
      <button class="toggle-btn active" id="btn-detailed" onclick="switchView('detailed')">📋 Detailed</button>
      <button class="toggle-btn"        id="btn-tv"       onclick="switchView('tv')">📊 TV Style</button>
    </div>
    <div style="display:flex;align-items:center;gap:8px;">
      <button class="toggle-btn" id="btn-prev-page" onclick="prevPage()" style="padding:3px 10px;">‹</button>
      <span id="page-info" style="color:#9ca3af;font-size:11px;min-width:140px;text-align:center;">Page 1 / 1</span>
      <button class="toggle-btn" id="btn-next-page" onclick="nextPage()" style="padding:3px 10px;">›</button>
    </div>
  </div>
</div>

<div class="tw" style="height:420px;overflow-y:auto;overflow-x:auto;margin-bottom:20px;padding:0 28px 0;">
<table style="min-width:900px;">
  <thead id="th-detailed" style="position:sticky;top:0;z-index:2;background:#0d1117;">
    <tr>
      <th>#</th><th>Dir</th><th>Time (IST)</th>
      <th style="text-align:right">Fill $</th><th style="text-align:right">TP $</th><th style="text-align:right">SL $</th>
      <th style="text-align:right">Exit $</th><th style="text-align:right">Pts</th><th style="text-align:right">P&L USDT</th>
      <th>Result</th><th>Exit Type</th>
      <th style="text-align:right">Slip pts</th><th style="text-align:right">Sig lat</th>
      <th style="text-align:right">En lat</th><th>Grade</th>
    </tr>
  </thead>
  <thead id="th-tv" class="hidden" style="position:sticky;top:0;z-index:2;background:#0d1117;">
    <tr>
      <th>Dir</th><th>Date</th><th>In</th><th>Out</th>
      <th style="text-align:right">Entry $</th><th style="text-align:right">Exit $</th><th style="text-align:right">SL $</th>
      <th style="text-align:right">Pts</th><th style="text-align:right">Lots</th>
      <th style="text-align:right">P&L USDT</th><th style="text-align:center">Status</th>
    </tr>
  </thead>
  <tbody id="view-detailed">
    {journal_rows}
    {journal_empty}
  </tbody>
  <tbody id="view-tv" class="hidden">
    {tv_rows}
    {tv_empty}
  </tbody>
</table>
</div>

<!-- PERFORMANCE -->
<div class="sec">Performance Summary</div>
<div class="stats">
  <div class="stat"><div class="sv" style="color:#f9fafb">{total}</div><div class="sl">Total Trades</div></div>
  <div class="stat"><div class="sv" style="color:{'#4ade80' if tp_cnt>=sl_cnt else '#f87171'}">{win_rt}</div><div class="sl">Win Rate</div></div>
  <div class="stat"><div class="sv" style="color:#4ade80">{tp_cnt}</div><div class="sl">TP Hits</div></div>
  <div class="stat"><div class="sv" style="color:#f87171">{sl_cnt}</div><div class="sl">SL Hits</div></div>
  <div class="stat"><div class="sv" style="color:{'#4ade80' if tot_pts>=0 else '#f87171'}">{'+' if tot_pts>0 else ''}{tot_pts}</div><div class="sl">Total Pts</div></div>
  <div class="stat"><div class="sv" style="color:{'#4ade80' if avg_pts>=0 else '#f87171'}">{'+' if avg_pts>0 else ''}{avg_pts}</div><div class="sl">Avg Pts/Trade</div></div>
  <div class="stat"><div class="sv" style="color:#4ade80">{avg_tp_pts:+.1f}</div><div class="sl">Avg TP Pts</div></div>
  <div class="stat"><div class="sv" style="color:#f87171">{avg_sl_pts:+.1f}</div><div class="sl">Avg SL Pts</div></div>
  <div class="stat"><div class="sv" style="color:#facc15">{avg_slip:+.2f}</div><div class="sl">Avg Entry Slip</div></div>
  <div class="stat"><div class="sv" style="color:#60a5fa">{avg_sig_lat:.0f}ms</div><div class="sl">Avg Signal Lat</div></div>
  <div class="stat"><div class="sv" style="color:#818cf8">{avg_lat:.0f}ms</div><div class="sl">Avg Entry Lat</div></div>
  <div class="stat"><div class="sv" style="color:#4ade80">{buy_wr}</div><div class="sl">BUY Win Rate</div></div>
  <div class="stat"><div class="sv" style="color:#f87171">{sell_wr}</div><div class="sl">SELL Win Rate</div></div>
  <div class="stat"><div class="sv" style="color:{'#4ade80' if tot_pnl>=0 else '#f87171'}">{'+' if tot_pnl>0 else ''}{tot_pnl:.4f}</div><div class="sl">Total P&L USDT</div></div>
</div>

<!-- ANALYSIS -->
<div class="grid2">
  <div class="panel">
    <h3>🎯 Fill Quality vs Signal</h3>
    <div class="kv"><span class="kl">Avg entry slippage</span><span class="kv2">{avg_slip:+.2f} pts</span></div>
    <div class="kv"><span class="kl">BUY fill slippage avg</span><span class="kv2">{'—' if not buy_trades else f"{round(sum(float(t.get('entry_slippage_pts',0)) for t in buy_trades)/len(buy_trades),2):+.2f} pts"}</span></div>
    <div class="kv"><span class="kl">SELL fill slippage avg</span><span class="kv2">{'—' if not sell_trades else f"{round(sum(float(t.get('entry_slippage_pts',0)) for t in sell_trades)/len(sell_trades),2):+.2f} pts"}</span></div>
    <div class="kv" style="margin-top:8px"><span class="kl">Grade distribution</span><span></span></div>
    <table style="margin-top:6px">
      <thead><tr><th>Grade</th><th style="text-align:right">Count</th><th style="text-align:right">%</th><th style="text-align:right">Win Rate</th><th>Bar</th></tr></thead>
      <tbody>{grade_rows or '<tr><td colspan="5" style="color:#4b5563;padding:8px">No data yet</td></tr>'}</tbody>
    </table>
  </div>
  <div class="panel">
    <h3>💡 Profitability Insights</h3>
    <div class="kv"><span class="kl">Overall win rate</span><span class="kv2">{win_rt} ({total} trades)</span></div>
    <div class="kv"><span class="kl">BUY win rate</span><span class="kv2">{buy_wr} ({len(buy_trades)} trades)</span></div>
    <div class="kv"><span class="kl">SELL win rate</span><span class="kv2">{sell_wr} ({len(sell_trades)} trades)</span></div>
    <div class="kv"><span class="kl">Avg pts on TP</span><span class="kv2" style="color:#4ade80">{avg_tp_pts:+.1f} pts</span></div>
    <div class="kv"><span class="kl">Avg pts on SL</span><span class="kv2" style="color:#f87171">{avg_sl_pts:+.1f} pts</span></div>
    <div class="kv"><span class="kl">Total P&L</span><span class="kv2" style="color:{'#4ade80' if tot_pnl>=0 else '#f87171'}">{tot_pnl:+.4f} USDT</span></div>
    <div class="kv"><span class="kl">Required WR break-even</span><span class="kv2">{f"{round(abs(avg_sl_pts)/(abs(avg_sl_pts)+avg_tp_pts)*100)}%" if avg_tp_pts>0 and avg_sl_pts<0 else "—"}</span></div>
    <div style="margin-top:10px">
      {"<div class='insight'>✅ INTACT entries performing well</div>" if grade_wr.get("INTACT",("","",0))[2]>60 else ""}
      {"<div class='insight warn'>⚠️ High slippage — consider skipping DEGRADED+ entries</div>" if grade_counts.get("DEGRADED",0)+grade_counts.get("BROKEN",0)+grade_counts.get("CRITICAL",0)>2 else ""}
      {"<div class='insight info'>ℹ️ Need 20+ trades for meaningful insights</div>" if total<20 else ""}
      {"<div class='insight'>✅ Sufficient data for analysis</div>" if total>=20 else ""}
    </div>
  </div>
</div>

<!-- LATENCY -->
<div class="panel" style="margin:0 24px 16px;">
  <h3>⚡ WS Latency</h3>
  <div class="grid3">
    <div>
      <div class="kv"><span class="kl">Avg signal latency</span><span class="kv2" style="color:#4ade80">{avg_sig_lat:.0f} ms</span></div>
      <div class="kv"><span class="kl">Best signal lat</span><span class="kv2" style="color:#4ade80">{"—" if not sig_lat_list else f"{min(sig_lat_list):.0f} ms"}</span></div>
      <div class="kv"><span class="kl">Worst signal lat</span><span class="kv2" style="color:#facc15">{"—" if not sig_lat_list else f"{max(sig_lat_list):.0f} ms"}</span></div>
    </div>
    <div>
      <div class="kv"><span class="kl">Avg entry latency</span><span class="kv2" style="color:#818cf8">{avg_lat:.0f} ms</span></div>
      <div class="kv"><span class="kl">Best entry</span><span class="kv2" style="color:#4ade80">{"—" if not lat_list else f"{min(lat_list):.0f} ms"}</span></div>
      <div class="kv"><span class="kl">Worst entry</span><span class="kv2" style="color:#f87171">{"—" if not lat_list else f"{max(lat_list):.0f} ms"}</span></div>
    </div>
    <div>
      <div class="kv"><span class="kl">Total avg end-to-end</span><span class="kv2" style="color:#facc15">{round(avg_sig_lat+avg_lat):.0f} ms</span></div>
      <div class="kv"><span class="kl">Order transport</span><span class="kv2" style="color:#4ade80">WS-trade (primary)</span></div>
      <div class="kv"><span class="kl">Target</span><span class="kv2" style="color:#4ade80">Signal &lt;500ms · Entry &lt;500ms</span></div>
    </div>
  </div>
</div>

<!-- ORDER LIFECYCLE -->
<div class="sec">Order Lifecycle — Last 20 Events</div>
<div class="tw">
<table>
  <thead><tr>
    <th>IST Time</th><th>Event</th><th>Trade ID</th>
    <th style="text-align:right">Price</th><th style="text-align:right">+ms</th><th>Notes</th>
  </tr></thead>
  <tbody>{lc_rows or lc_empty}</tbody>
</table>
</div>

<div class="footer">
  Bybit Vol Surge {INTERVAL}m · MB{MIN_BODY:.0f} · SL{FIXED_SL:.0f}/TP{FIXED_TP:.0f} · burst{BURST_MULT} · LOT={LOT_SIZE_STR} BTC · LIVE · Singapore
</div>

<!-- MODAL -->
<div id="modal-overlay" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,0.75);z-index:9999;align-items:center;justify-content:center;">
  <div style="background:#0d1117;border:1px solid #334155;border-radius:12px;padding:28px 32px;min-width:340px;max-width:480px;box-shadow:0 20px 60px rgba(0,0,0,0.8);">
    <div id="modal-title" style="font-size:15px;font-weight:700;color:#f9fafb;margin-bottom:14px;"></div>
    <div id="modal-body"  style="font-size:13px;color:#9ca3af;line-height:1.7;margin-bottom:22px;"></div>
    <div style="display:flex;gap:10px;justify-content:flex-end;">
      <button onclick="_modalCancel()" style="background:#1e293b;color:#9ca3af;border:1px solid #334155;border-radius:6px;padding:8px 20px;font-size:13px;cursor:pointer;font-family:inherit;">✕ Cancel</button>
      <button id="modal-yes" onclick="_modalYes()" style="color:#fff;border:none;border-radius:6px;padding:8px 20px;font-size:13px;cursor:pointer;font-family:inherit;font-weight:600;"></button>
    </div>
  </div>
</div>

</body>
</html>"""
    return HTMLResponse(content=html)


def _start_health_server():
    port = int(os.getenv("PORT", "8080"))
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")
    server = uvicorn.Server(config)
    threading.Thread(target=server.run, daemon=True, name="health").start()
    log.info(f"[HEALTH] FastAPI server on :{port}")


# ── Engine ──────────────────────────────────────────────────────────────
cfg = SignalConfig(lookback=5, burst_mult=BURST_MULT, sl_mult=1.8, tp2_r=TP_R,
                   cooldown=3, use_ha=False, use_ema_filter=False, use_session=False,
                   safety_factor=1.0, use_min_body=(MIN_BODY > 0), min_body_pts=MIN_BODY,
                   use_breakout_ctx=True, breakout_ctx_bars=5)
engine = SignalEngine(config=cfg)
feed   = BybitFeed(symbol=SYMBOL, interval=INTERVAL, on_candle_close=on_candle_close, logger=log)
LOOP   = None


def run_preflight() -> dict:
    """Pre-flight validation — mirrors Anand/Mummy bot pattern."""
    results = {}
    passed  = True

    # 1. Credentials present
    ok = bool(B.API_KEY and B.API_SECRET)
    results["credentials"] = {"ok": ok}
    if not ok:
        passed = False
        log.error("PRE-FLIGHT FAIL: API credentials missing")

    # 2. API auth — query server time (authed endpoint not needed; use position query)
    try:
        st = B.server_time()
        ok = st is not None
        results["api_auth"] = {"ok": ok, "server_time": st}
    except Exception as e:
        ok = False
        results["api_auth"] = {"ok": False, "detail": str(e)}
    if not ok:
        passed = False
        log.error("PRE-FLIGHT FAIL: Bybit API unreachable")

    # 3. Price feed
    try:
        r = B.rest_get("/v5/market/tickers", {"category": "linear", "symbol": SYMBOL})
        price = float(r["result"]["list"][0]["lastPrice"]) if r and r.get("retCode") == 0 else None
        ok = price is not None
        results["price_feed"] = {"ok": ok, "price": price}
    except Exception as e:
        ok = False
        results["price_feed"] = {"ok": False, "detail": str(e)}
    if not ok:
        passed = False
        log.error("PRE-FLIGHT FAIL: price feed unavailable")

    # 4. No open position
    try:
        pos = B.get_position()
        ok  = pos is None
        results["no_open_position"] = {
            "ok": ok,
            "detail": f"size={pos.get('size')} avgPrice={pos.get('avgPrice')}" if pos else "flat",
        }
        if not ok:
            passed = False
            log.error("PRE-FLIGHT FAIL: stale open position — close manually before trading")
    except Exception as e:
        results["no_open_position"] = {"ok": False, "detail": str(e)}
        passed = False

    # 5. Balance fetch
    try:
        r = B.rest_get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        ok = r is not None and r.get("retCode") == 0
        bal = None
        if ok:
            coins = r["result"]["list"][0].get("coin", [])
            usdt = next((c for c in coins if c["coin"] == "USDT"), None)
            bal = float(usdt["availableToWithdraw"]) if usdt else None
        results["balance_fetch"] = {"ok": ok, "usdt_available": bal}
    except Exception as e:
        ok = False
        results["balance_fetch"] = {"ok": False, "detail": str(e)}
    if not ok:
        passed = False
        log.error("PRE-FLIGHT FAIL: balance fetch failed")

    # 6. Lot size sanity
    ok = LOT_SIZE >= 0.001
    results["lot_size"] = {"ok": ok, "lot_btc": LOT_SIZE, "min_btc": 0.001}
    if not ok:
        passed = False
        log.error(f"PRE-FLIGHT FAIL: LOT_SIZE={LOT_SIZE} < minimum=0.001")

    results["all_passed"] = passed
    results["mode"]       = "LIVE"
    results["timestamp"]  = datetime.now().isoformat()

    status = "✅ ALL PASSED" if passed else "❌ FAILED"
    log.info(f"[PRE-FLIGHT] {status}")
    summary = {k: (v.get("ok") if isinstance(v, dict) else v) for k, v in results.items()}
    tg(f"{'✅' if passed else '❌'} Pre-flight {'PASSED' if passed else 'FAILED'}\n"
       f"Mode: LIVE | {json.dumps(summary, indent=2)}")
    return results


async def main():
    global trade_ws, LOOP
    LOOP = asyncio.get_event_loop()
    _init_csvs()
    _start_health_server()
    if not (B.API_KEY and B.API_SECRET):
        log.error("No API keys."); return

    # Pre-flight (same as Anand/Mummy bot)
    pf = run_preflight()
    if not pf.get("all_passed"):
        log.error("[PREFLIGHT] FAILED — fix issues above before trading")
        # don't exit; still start feed for monitoring but warn

    trade_ws = B.BybitTradeWS(logger=log)
    if not await trade_ws.connect():
        log.warning("[PREFLIGHT] WS-trade auth failed — will use REST fallback")

    tg(f"🟢 <b>LIVE Vol Surge Bybit started</b>\n"
       f"Signal: WS-native\n"
       f"Candles: Heikin-Ashi ✓\n"
       f"MB{MIN_BODY:.0f} · SL{FIXED_SL:.0f}/TP{FIXED_TP:.0f} · burst{BURST_MULT} · {INTERVAL}m\n"
       f"WS-trade: {'✓' if trade_ws.authed else 'REST fallback'}\n"
       f"Dashboard: https://bybit-volsurge-bot.fly.dev/dashboard")

    asyncio.create_task(_position_watch())
    await feed.start()


async def test_order():
    if not (B.API_KEY and B.API_SECRET):
        print("No keys."); return
    print(f"TEST ORDER on {'TESTNET' if B._TESTNET else 'LIVE'} | {SYMBOL} qty={LOT_SIZE_STR}")
    pos = B.get_position()
    print("Position before:", "FLAT" if pos is None else pos.get("size"))
    tws = B.BybitTradeWS(); ok = await tws.connect()
    print("WS auth:", ok)
    t0 = time.time()
    r = await tws.place_market("Buy", LOT_SIZE_STR) if ok else B.place_market_order_rest("Buy", LOT_SIZE_STR)
    print(f"BUY resp ({(time.time()-t0)*1000:.0f}ms):", json.dumps(r)[:300])
    await asyncio.sleep(1)
    rc = B.place_market_order_rest("Sell", LOT_SIZE_STR)
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
