#!/usr/bin/env python3
"""
bybit_paper.py — DRY run: real Bybit feed -> HA -> engine -> log signals.
NO keys, NO orders. Validates the live signal pipeline end-to-end.
Logs every closed bar's engine state + flags BUY/SELL signals.
"""
import asyncio, os, logging
from collections import deque
from bybit_feed import BybitFeed, Candle
from signal_engine import SignalConfig, SignalEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("paper")

MIN_BODY   = float(os.getenv("MIN_BODY_PTS", "150"))
BURST_MULT = float(os.getenv("VS_BURST_MULT", "2.0"))
FIXED_SL   = float(os.getenv("FIXED_SL_PTS", "50"))
FIXED_TP   = float(os.getenv("FIXED_TP_PTS", "100"))
INTERVAL   = os.getenv("BYBIT_INTERVAL", "1")

cfg = SignalConfig(lookback=5, burst_mult=BURST_MULT, sl_mult=1.8, tp2_r=FIXED_TP/FIXED_SL,
                   cooldown=3, use_ha=False, use_ema_filter=False, use_session=False,
                   safety_factor=1.0, use_min_body=(MIN_BODY>0), min_body_pts=MIN_BODY,
                   use_breakout_ctx=True, breakout_ctx_bars=5)
engine = SignalEngine(config=cfg)
_ha_op = _ha_cp = None
_ha_buf = deque(maxlen=300)
_n = 0

def on_close(raw, buffer):
    global _ha_op, _ha_cp, _n
    if not _ha_buf and len(buffer) > 10:   # seed HA from backfill once (no warmup gap)
        for rc in list(buffer)[:-1]:
            _hc=(rc.open+rc.high+rc.low+rc.close)/4.0
            _ho=(rc.open+rc.close)/2.0 if _ha_op is None else (_ha_op+_ha_cp)/2.0
            _hh=max(rc.high,_ho,_hc);_hl=min(rc.low,_ho,_hc)
            _ha_buf.append(Candle(ts=rc.ts,open=round(_ho,2),high=round(_hh,2),low=round(_hl,2),close=round(_hc,2),volume=rc.volume))
            _ha_op,_ha_cp=_ho,_hc
        log.info(f"[HA] seeded {len(_ha_buf)} candles from backfill")
    ha_c = (raw.open+raw.high+raw.low+raw.close)/4.0
    ha_o = (raw.open+raw.close)/2.0 if _ha_op is None else (_ha_op+_ha_cp)/2.0
    ha_h = max(raw.high, ha_o, ha_c); ha_l = min(raw.low, ha_o, ha_c)
    hac = Candle(ts=raw.ts, open=round(ha_o,2), high=round(ha_h,2), low=round(ha_l,2), close=round(ha_c,2), volume=raw.volume)
    _ha_buf.append(hac); _ha_op, _ha_cp = ha_o, ha_c
    if len(_ha_buf) < 10: return
    st = engine.on_candle_close(hac, _ha_buf, in_trade=False)
    _n += 1
    if st:
        flag = f"  <<< {st.signal} SIGNAL >>>" if st.signal in ("BUY","SELL") else ""
        log.info(f"[BAR {_n}] raw_c={raw.close:.1f} ha_c={st.close:.1f} body={st.candle_body:.1f} "
                 f"chop={st.chop_avg_tr:.1f} burst_thresh={st.burst_threshold:.1f} "
                 f"{'BULL' if st.is_burst_bull else 'BEAR' if st.is_burst_bear else 'none'}{flag}")
        if st.signal in ("BUY","SELL"):
            ep = st.close
            sl = ep-FIXED_SL if st.signal=="BUY" else ep+FIXED_SL
            tp = ep+FIXED_TP if st.signal=="BUY" else ep-FIXED_TP
            log.info(f"   -> WOULD PLACE {st.signal} | entry~{ep:.1f} SL {sl:.1f} TP {tp:.1f} (PAPER, no order)")

if __name__ == "__main__":
    log.info(f"PAPER MODE — MB{MIN_BODY:.0f} burst{BURST_MULT} SL{FIXED_SL:.0f}/TP{FIXED_TP:.0f} {INTERVAL}m | feed+HA+engine, NO orders")
    feed = BybitFeed(interval=INTERVAL, on_candle_close=on_close, logger=log)
    try:
        asyncio.run(feed.start())
    except KeyboardInterrupt:
        feed.stop()
