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

# ── FastAPI dashboard ────────────────────────────────────────────────────
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
import uvicorn

app = FastAPI()

# ── Data / trade log ─────────────────────────────────────────────────────
DATA_DIR   = Path(os.getenv("DATA_DIR", str(Path(__file__).parent)))
SLIP_FILE  = DATA_DIR / "bybit_slippage.csv"
TRADE_FILE = DATA_DIR / "bybit_trades.csv"
SLIP_HDRS  = ["timestamp_ist", "side", "signal_ref", "fill_avg", "entry_slip_pts",
              "sl", "tp", "order_lat_ms", "config"]
TRADE_HDRS = ["trade_id", "direction", "entry_time_ist", "fill_price", "sl_price", "tp_price",
              "exit_price", "pts", "pnl_usdt", "outcome", "entry_slip_pts", "order_lat_ms",
              "trade_duration_sec", "config"]

def _log_slip(row: dict):
    new = not SLIP_FILE.exists()
    try:
        with open(SLIP_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=SLIP_HDRS)
            if new: w.writeheader()
            w.writerow(row)
    except Exception as e:
        logging.getLogger("bybit_live").warning(f"slip log fail: {e}")

def _log_trade(row: dict):
    new = not TRADE_FILE.exists()
    try:
        with open(TRADE_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=TRADE_HDRS, extrasaction="ignore")
            if new: w.writeheader()
            w.writerow(row)
    except Exception as e:
        logging.getLogger("bybit_live").warning(f"trade log fail: {e}")

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
                side   = ot["side"]
                fill   = ot["fill"]
                sl_px  = ot["sl"]
                tp_px  = ot["tp"]
                slip   = ot.get("slip", 0)
                lat    = ot.get("lat_ms", 0)
                # Determine outcome by checking which level was closer at close
                # (server closed it; we infer TP vs SL from price proximity)
                # Re-query last closed PnL from Bybit for accuracy
                exit_px = None
                outcome = "CLOSED"
                try:
                    r = B.rest_get("/v5/position/closed-pnl", {"category": "linear", "symbol": SYMBOL, "limit": "1"})
                    if r and r.get("retCode") == 0 and r["result"]["list"]:
                        last = r["result"]["list"][0]
                        exit_px = float(last.get("avgExitPrice", 0) or 0)
                        pnl_v   = float(last.get("closedPnl", 0) or 0)
                        if side == "Buy":
                            outcome = "TP" if exit_px >= tp_px - 5 else "SL"
                        else:
                            outcome = "TP" if exit_px <= tp_px + 5 else "SL"
                except Exception as e:
                    log.warning(f"[EXIT] closed pnl fetch fail: {e}")

                pts = round((exit_px - fill) if side=="Buy" else (fill - exit_px), 1) if exit_px else 0
                pnl_usdt = round(pts * float(LOT_SIZE), 4) if exit_px else 0
                ist = (datetime.now(timezone.utc) + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S")
                _log_trade({
                    "trade_id": f"T{int(ot['ts'])}",
                    "direction": side,
                    "entry_time_ist": ist,
                    "fill_price": round(fill, 1),
                    "sl_price": sl_px,
                    "tp_price": tp_px,
                    "exit_price": round(exit_px, 1) if exit_px else "",
                    "pts": pts,
                    "pnl_usdt": pnl_usdt,
                    "outcome": outcome,
                    "entry_slip_pts": round(slip, 1),
                    "order_lat_ms": round(lat),
                    "trade_duration_sec": dur,
                    "config": CONFIG_TAG,
                })
                outcome_emoji = "✅" if outcome == "TP" else "❌"
                tg(f"{outcome_emoji} <b>{side} {outcome}</b> [Bybit]\n"
                   f"Fill: {fill:,.1f} → Exit: {exit_px:,.1f} | Pts: {pts:+.1f}\n"
                   f"P&L: {pnl_usdt:+.4f} USDT | Duration: {dur}s\n"
                   f"⚙️ {CONFIG_TAG}")
                log.info(f"[EXIT] {outcome} | pts={pts:+.1f} pnl={pnl_usdt:+.4f} USDT | dur={dur}s")
        except Exception as e:
            log.warning(f"[WATCH] {e}")


# ── FastAPI routes ────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return JSONResponse({"status": "alive", "config": CONFIG_TAG,
                         "in_trade": open_trade is not None,
                         "ws_trade": bool(trade_ws and trade_ws.authed)})

@app.get("/api/live")
async def api_live():
    with _state_lock:
        ot = dict(open_trade) if open_trade else None
    px = feed.mark_price if hasattr(feed, "mark_price") else None
    unr = None
    if ot and px:
        unr = round((px - ot["fill"]) if ot["side"] == "Buy" else (ot["fill"] - px), 1)
    return JSONResponse({"price": px, "unreal": unr,
                         "sl": ot.get("sl") if ot else None,
                         "tp": ot.get("tp") if ot else None,
                         "has_trade": bool(ot)})

@app.get("/api/stream")
async def api_stream():
    async def event_gen():
        import json as _json
        last_px = None
        while True:
            try:
                px = feed.mark_price if hasattr(feed, "mark_price") else None
                if px and px != last_px:
                    last_px = px
                    with _state_lock:
                        ot = dict(open_trade) if open_trade else None
                    unr = None
                    if ot and px:
                        unr = round((px - ot["fill"]) if ot["side"] == "Buy" else (ot["fill"] - px), 1)
                    data = _json.dumps({"price": round(px, 1), "unreal": unr,
                                        "sl": ot.get("sl") if ot else None,
                                        "tp": ot.get("tp") if ot else None,
                                        "has_trade": bool(ot)})
                    yield f"data: {data}\n\n"
                await asyncio.sleep(0.5)
            except Exception:
                await asyncio.sleep(1)
    return StreamingResponse(event_gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    # load trades
    trades = []
    try:
        with open(TRADE_FILE, "r", encoding="utf-8") as f:
            trades = list(csv.DictReader(f))
    except Exception:
        pass

    with _state_lock:
        ot = dict(open_trade) if open_trade else None

    now_ist = (datetime.utcnow() + timedelta(seconds=19800)).strftime("%d/%m/%Y %H:%M:%S IST")

    def _f(v, dec=1):
        try: return f"{float(v):,.{dec}f}"
        except: return "—"
    def _pc(v):
        try:
            f = float(v); c = "#4ade80" if f > 0 else "#f87171" if f < 0 else "#9ca3af"
            s = "+" if f >= 0 else ""
            return f'<span style="color:{c};font-weight:700;">{s}{f:.2f}</span>'
        except: return '<span style="color:#6b7280;">—</span>'
    def _dir(v):
        if v in ("Buy","BUY"):  return '<span style="color:#4ade80;font-weight:700;">▲ BUY</span>'
        if v in ("Sell","SELL"): return '<span style="color:#f87171;font-weight:700;">▼ SELL</span>'
        return "—"
    def _outcome(v):
        if v == "TP": return '<span style="background:#14532d;color:#4ade80;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;">TP ✓</span>'
        if v == "SL": return '<span style="background:#450a0a;color:#f87171;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;">SL ✗</span>'
        return f'<span style="color:#6b7280;">{v or "—"}</span>'

    # stats
    total   = len(trades)
    tp_cnt  = sum(1 for t in trades if t.get("outcome") == "TP")
    sl_cnt  = sum(1 for t in trades if t.get("outcome") == "SL")
    pts_list = []
    for t in trades:
        try: pts_list.append(float(t["pts"]))
        except: pass
    tot_pts = round(sum(pts_list), 2)
    avg_pts = round(sum(pts_list)/len(pts_list), 2) if pts_list else 0
    win_rt  = f"{round(tp_cnt/total*100)}%" if total else "—"
    slip_list = []
    for t in trades:
        try: slip_list.append(float(t["entry_slip_pts"]))
        except: pass
    avg_slip = round(sum(slip_list)/len(slip_list), 2) if slip_list else 0
    lat_list = []
    for t in trades:
        try: lat_list.append(float(t["order_lat_ms"]))
        except: pass
    avg_lat = round(sum(lat_list)/len(lat_list), 1) if lat_list else 0
    buy_trades  = [t for t in trades if t.get("direction") in ("Buy","BUY")]
    sell_trades = [t for t in trades if t.get("direction") in ("Sell","SELL")]
    buy_wr  = f"{round(sum(1 for t in buy_trades  if t.get('outcome')=='TP')/len(buy_trades)*100)}%"  if buy_trades  else "—"
    sell_wr = f"{round(sum(1 for t in sell_trades if t.get('outcome')=='TP')/len(sell_trades)*100)}%" if sell_trades else "—"
    tp_pts_l = [float(t["pts"]) for t in trades if t.get("outcome")=="TP" and t.get("pts")]
    sl_pts_l = [float(t["pts"]) for t in trades if t.get("outcome")=="SL" and t.get("pts")]
    avg_tp_pts = round(sum(tp_pts_l)/len(tp_pts_l), 1) if tp_pts_l else 0
    avg_sl_pts = round(sum(sl_pts_l)/len(sl_pts_l), 1) if sl_pts_l else 0
    pnl_list = []
    for t in trades:
        try: pnl_list.append(float(t["pnl_usdt"]))
        except: pass
    tot_pnl = round(sum(pnl_list), 4)

    # open trade panel
    open_panel = ""
    if ot:
        d = ot.get("side", "?")
        fill_px = ot.get("fill", 0)
        sl_px   = ot.get("sl", 0)
        tp_px   = ot.get("tp", 0)
        slip    = ot.get("slip", 0)
        lat     = ot.get("lat_ms", 0)
        dir_col = "#4ade80" if d == "Buy" else "#f87171"
        elapsed = round(time.time() - ot.get("ts", time.time()))
        elapsed_s = f"{elapsed//60}m {elapsed%60}s" if elapsed >= 60 else f"{elapsed}s"
        px_now  = feed.mark_price if hasattr(feed, "mark_price") else 0
        unreal  = round((px_now - fill_px) if d == "Buy" else (fill_px - px_now), 1) if px_now else 0
        unreal_col = "#4ade80" if unreal >= 0 else "#f87171"
        open_panel = f"""
<div style="background:#0a1f0a;border:2px solid #166534;border-radius:10px;margin:0 24px 20px;padding:16px 20px;">
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;">
    <span style="background:#166534;color:#4ade80;padding:3px 10px;border-radius:4px;font-size:12px;font-weight:700;">🔴 LIVE TRADE OPEN</span>
    <span style="color:{dir_col};font-size:18px;font-weight:700;">{'▲' if d=='Buy' else '▼'} {'BUY' if d=='Buy' else 'SELL'}</span>
    <span style="color:#6b7280;font-size:12px;">in trade for {elapsed_s}</span>
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
    <div><div style="color:#6b7280;font-size:10px;text-transform:uppercase;">Order Latency</div><div style="color:#60a5fa;font-size:14px;">{float(lat or 0):.0f}ms</div></div>
  </div>
</div>"""

    # journal rows
    journal_rows = ""
    for i, t in enumerate(reversed(trades), 1):
        outcome = t.get("outcome","")
        rbg = "#0a1a0a" if outcome=="TP" else "#1a0a0a" if outcome=="SL" else "#0d1117"
        pts_v = 0.0
        try: pts_v = float(t.get("pts",0) or 0)
        except: pass
        journal_rows += f"""
        <tr style="background:{rbg};border-bottom:1px solid #1f2937;">
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
          <td style="text-align:right;">{_pc(t.get('entry_slip_pts',''))}</td>
          <td style="color:#60a5fa;text-align:right;">{t.get('order_lat_ms','—')}ms</td>
          <td style="color:#9ca3af;text-align:right;">{t.get('trade_duration_sec','—')}s</td>
        </tr>"""
    journal_empty = "" if trades else '<tr><td colspan="13" style="text-align:center;color:#6b7280;padding:40px;">No trades yet — waiting for first signal</td></tr>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bybit Bot — Live Dashboard</title>
<style>
  *{{margin:0;padding:0;box-sizing:border-box}}
  body{{background:#080c10;color:#e2e8f0;font-family:'Segoe UI',system-ui,monospace;font-size:13px}}
  .hdr{{background:#0d1117;border-bottom:2px solid #1d4ed8;padding:16px 28px;display:flex;align-items:center;justify-content:space-between}}
  .hdr h1{{font-size:18px;font-weight:700;color:#f9fafb}}
  .sec{{font-size:10px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:1.2px;padding:20px 28px 10px}}
  .stats{{display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:10px;padding:0 28px 20px}}
  .stat{{background:#0d1117;border:1px solid #1e293b;border-radius:8px;padding:14px 16px}}
  .sv{{font-size:20px;font-weight:700;line-height:1.2}}
  .sl{{font-size:10px;color:#6b7280;margin-top:4px;text-transform:uppercase;letter-spacing:0.6px}}
  .panel{{background:#0d1117;border:1px solid #1e293b;border-radius:10px;margin:0 28px 16px;padding:20px 24px}}
  .panel h3{{font-size:11px;color:#9ca3af;font-weight:700;margin-bottom:14px;text-transform:uppercase;letter-spacing:0.8px;border-bottom:1px solid #1f2937;padding-bottom:8px}}
  .grid2{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin:0 28px 16px}}
  .grid2 .panel{{margin:0}}
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
  .insight.info{{background:#0f1525;border-color:#60a5fa;color:#93c5fd}}
  .footer{{text-align:center;padding:18px;color:#374151;font-size:10px;border-top:1px solid #1f2937;margin-top:4px}}
</style>
<script>
  setTimeout(()=>location.reload(),30000);
  setInterval(()=>{{document.getElementById('clk').textContent=new Date().toLocaleTimeString('en-IN',{{timeZone:'Asia/Kolkata'}})}},1000);
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
  }})();
</script>
</head>
<body>
<div class="hdr">
  <div>
    <h1>⚡ Bybit Bot — Live Dashboard ({INTERVAL}m · MB{MIN_BODY:.0f})</h1>
    <div style="color:#6b7280;font-size:11px;margin-top:3px;">BTCUSDT · Bybit · <span style="color:#60a5fa;font-weight:600;">{INTERVAL}m HA candles</span> · WS-native · page reload 30s · {now_ist}</div>
  </div>
  <div style="text-align:right;display:flex;flex-direction:column;align-items:flex-end;gap:6px;">
    <span style="background:#0a2a1f;color:#4ade80;padding:4px 14px;border-radius:20px;font-size:12px;font-weight:700;">🟢 LIVE</span>
    <span style="color:#6b7280;font-size:11px;">🕐 IST <b id="clk"></b></span>
    <span style="color:#{'4ade80' if ot else '6b7280'};font-size:11px;">{'🔴 POSITION OPEN' if ot else '⚪ IDLE'}</span>
  </div>
</div>

{open_panel}

<div class="sec">Trade Journal — All Trades (newest first)</div>
<div class="tw" style="height:400px;overflow-y:auto;">
<table style="min-width:900px;">
  <thead style="position:sticky;top:0;z-index:2;background:#0d1117;">
    <tr>
      <th>#</th><th>Dir</th><th>Time (IST)</th>
      <th style="text-align:right">Fill $</th>
      <th style="text-align:right">TP $</th>
      <th style="text-align:right">SL $</th>
      <th style="text-align:right">Exit $</th>
      <th style="text-align:right">Pts</th>
      <th style="text-align:right">P&L USDT</th>
      <th>Result</th>
      <th style="text-align:right">Slip pts</th>
      <th style="text-align:right">Order lat</th>
      <th style="text-align:right">Duration</th>
    </tr>
  </thead>
  <tbody>
    {journal_rows}
    {journal_empty}
  </tbody>
</table>
</div>

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
  <div class="stat"><div class="sv" style="color:#facc15">{avg_slip:+.2f}</div><div class="sl">Avg Slip Pts</div></div>
  <div class="stat"><div class="sv" style="color:#60a5fa">{avg_lat:.0f}ms</div><div class="sl">Avg Order Lat</div></div>
  <div class="stat"><div class="sv" style="color:#4ade80">{buy_wr}</div><div class="sl">BUY Win Rate</div></div>
  <div class="stat"><div class="sv" style="color:#f87171">{sell_wr}</div><div class="sl">SELL Win Rate</div></div>
  <div class="stat"><div class="sv" style="color:{'#4ade80' if tot_pnl>=0 else '#f87171'}">{'+' if tot_pnl>0 else ''}{tot_pnl:.4f}</div><div class="sl">Total P&L USDT</div></div>
</div>

<div class="grid2">
  <div class="panel">
    <h3>💡 Profitability Insights</h3>
    <div class="kv"><span class="kl">Win rate</span><span class="kv2">{win_rt} ({total} trades)</span></div>
    <div class="kv"><span class="kl">BUY win rate</span><span class="kv2">{buy_wr} ({len(buy_trades)} trades)</span></div>
    <div class="kv"><span class="kl">SELL win rate</span><span class="kv2">{sell_wr} ({len(sell_trades)} trades)</span></div>
    <div class="kv"><span class="kl">Avg TP pts</span><span class="kv2" style="color:#4ade80">{avg_tp_pts:+.1f} pts</span></div>
    <div class="kv"><span class="kl">Avg SL pts</span><span class="kv2" style="color:#f87171">{avg_sl_pts:+.1f} pts</span></div>
    <div class="kv"><span class="kl">Total P&L</span><span class="kv2" style="color:{'#4ade80' if tot_pnl>=0 else '#f87171'}">{tot_pnl:+.4f} USDT</span></div>
    <div style="margin-top:10px">
      {"<div class='insight info'>ℹ️ Need 10+ trades for meaningful insights</div>" if total<10 else ""}
      {"<div class='insight'>✅ Sufficient data for analysis</div>" if total>=10 else ""}
    </div>
  </div>
  <div class="panel">
    <h3>⚡ Slippage & Latency</h3>
    <div class="kv"><span class="kl">Avg entry slippage</span><span class="kv2">{avg_slip:+.2f} pts</span></div>
    <div class="kv"><span class="kl">BUY avg slippage</span><span class="kv2">{"—" if not buy_trades else f"{round(sum(float(t.get('entry_slip_pts',0)) for t in buy_trades)/len(buy_trades),2):+.2f} pts"}</span></div>
    <div class="kv"><span class="kl">SELL avg slippage</span><span class="kv2">{"—" if not sell_trades else f"{round(sum(float(t.get('entry_slip_pts',0)) for t in sell_trades)/len(sell_trades),2):+.2f} pts"}</span></div>
    <div class="kv"><span class="kl">Avg order latency</span><span class="kv2" style="color:#60a5fa">{avg_lat:.0f}ms</span></div>
    <div class="kv"><span class="kl">Best order lat</span><span class="kv2" style="color:#4ade80">{"—" if not lat_list else f"{min(lat_list):.0f}ms"}</span></div>
    <div class="kv"><span class="kl">Worst order lat</span><span class="kv2" style="color:#f87171">{"—" if not lat_list else f"{max(lat_list):.0f}ms"}</span></div>
    <div class="kv"><span class="kl">Order transport</span><span class="kv2" style="color:#4ade80">WS-trade (primary)</span></div>
  </div>
</div>

<div class="footer">
  Bybit Vol Surge {INTERVAL}m · MB{MIN_BODY:.0f} · SL{FIXED_SL:.0f}/TP{FIXED_TP:.0f} · burst{BURST_MULT} · LOT={LOT_SIZE} BTC · LIVE
</div>
</body>
</html>"""
    return HTMLResponse(content=html)


def _start_health_server():
    port = int(os.getenv("PORT", "8080"))
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")
    server = uvicorn.Server(config)
    threading.Thread(target=server.run, daemon=True, name="health").start()
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
