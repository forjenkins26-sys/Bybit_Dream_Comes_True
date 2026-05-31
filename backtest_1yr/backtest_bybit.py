#!/usr/bin/env python3
"""
backtest_bybit.py — run Vol Surge winning configs on 1yr Bybit BTCUSDT data.
HA-exact signal + real-OHLC exits. No slippage (matches Testing Strategy method).
Prints WR/PF/pts + monthly, and compares to Delta's BTCUSD results.
"""
import json, sys
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta
HERE = __import__("pathlib").Path(__file__).parent
sys.path.insert(0, str(HERE))
from signal_engine import SignalConfig, SignalEngine

# minimal Candle (avoid importing feed)
from dataclasses import dataclass
@dataclass
class Candle:
    ts:int; open:float; high:float; low:float; close:float; volume:float

IST = timezone(timedelta(hours=5,minutes=30))
LOT, INRU = 0.1, 84.0
raw = json.load(open(HERE/"bybit_cache_1yr.json"))
c = [Candle(ts=x["ts"],open=x["open"],high=x["high"],low=x["low"],close=x["close"],volume=x["volume"]) for x in raw]
print(f"Bybit cache: {len(c):,} candles  "
      f"({datetime.fromtimestamp(c[0].ts,IST):%d-%b-%Y} -> {datetime.fromtimestamp(c[-1].ts,IST):%d-%b-%Y})")

def bt(mb, burst, SL, TP, monthly=False):
    cfg=SignalConfig(lookback=5,burst_mult=burst,sl_mult=1.8,tp2_r=TP/SL,cooldown=3,use_ha=False,
                     use_ema_filter=False,use_session=False,safety_factor=1.0,
                     use_min_body=(mb>0),min_body_pts=float(mb),use_breakout_ctx=True,breakout_ctx_bars=5)
    e=SignalEngine(config=cfg);ha_op=ha_cp=None;it=None;buf=deque(maxlen=50)
    w=l=0;pts=0.0;gw=0.0;gl=0.0;mo=defaultdict(lambda:[0,0,0.0])
    for rc in c:
        hc=(rc.open+rc.high+rc.low+rc.close)/4.0
        ho=(rc.open+rc.close)/2.0 if ha_op is None else (ha_op+ha_cp)/2.0
        hh=max(rc.high,ho,hc);hl=min(rc.low,ho,hc)
        hac=Candle(ts=rc.ts,open=round(ho,2),high=round(hh,2),low=round(hl,2),close=round(hc,2),volume=rc.volume)
        buf.append(hac);ha_op,ha_cp=ho,hc
        if len(buf)<10: continue
        if it:
            d=it["dir"];htp=rc.high>=it["tp"] if d=="BUY" else rc.low<=it["tp"];hsl=rc.low<=it["sl"] if d=="BUY" else rc.high>=it["sl"]
            if htp or hsl:
                if htp and hsl: rb=rc.close>rc.open;res=("SL" if rb else "TP") if d=="BUY" else ("TP" if rb else "SL")
                else: res="SL" if hsl else "TP"
                p=TP if res=="TP" else -SL
                if res=="TP": w+=1;gw+=TP
                else: l+=1;gl+=SL
                pts+=p; mo[datetime.fromtimestamp(it["ts"],IST).strftime("%Y-%m")][0 if res=="TP" else 1]+=1
                mo[datetime.fromtimestamp(it["ts"],IST).strftime("%Y-%m")][2]+=p; it=None
            else: e.on_candle_close(hac,buf,in_trade=True);continue
        st=e.on_candle_close(hac,buf,in_trade=False)
        if st and st.signal in("BUY","SELL"):
            ep=st.close
            if st.signal=="BUY": sl_p=ep-SL;tp_p=ep+TP
            else: sl_p=ep+SL;tp_p=ep-TP
            it=dict(dir=st.signal,tp=tp_p,sl=sl_p,ts=rc.ts)
    n=w+l
    if n==0: return None
    r=dict(mb=mb,burst=burst,SL=SL,TP=TP,n=n,w=w,l=l,wr=w/n*100,pf=(gw/gl if gl else 999),
           pts=pts,inr=pts*LOT*INRU,lose_mo=sum(1 for v in mo.values() if v[2]<0),nmo=len(mo),mo=mo)
    return r

# Winning configs (Delta numbers in comment for comparison)
CONFIGS = [
    ("MB150 SL50/TP100 1:2  (Delta: 82.3% PF9.28)", 150,2.0,50,100),
    ("MB150 SL50/TP150 1:3  (Delta: 70.2% PF7.07)", 150,2.0,50,150),
    ("MB50  SL75/TP225 1:3  (Delta: 44.8% PF2.44 Rs18.5L)",50,2.0,75,225),
    ("MB50  SL75/TP150 1:2  (Delta: 57.6% PF2.72 Rs17.6L)",50,2.0,75,150),
    ("MB250 SL50/TP100 1:2  (Delta: 91.0% PF20.2)",250,2.0,50,100),
]
print(f"\n{'='*100}\n  BYBIT BTCUSDT — Vol Surge winners (no slippage)  |  vs Delta BTCUSD\n{'='*100}")
print(f"  {'Config':<48}{'Trades':>7}{'WR':>8}{'PF':>7}{'Pts':>9}{'INR/yr':>12}{'LoseMo':>8}")
print("  "+"-"*98)
results={}
for label,mb,burst,SL,TP in CONFIGS:
    r=bt(mb,burst,SL,TP)
    results[label]=r
    if r:
        print(f"  {label:<48}{r['n']:>7}{r['wr']:>7.1f}%{r['pf']:>7.2f}{r['pts']:>+9.0f}{r['inr']:>+12,.0f}{str(r['lose_mo'])+'/'+str(r['nmo']):>8}")

# Monthly for the live pick (MB150 SL50/TP100)
live=results[CONFIGS[0][0]]
if live:
    print(f"\n{'='*70}\n  MONTHLY — LIVE PICK: MB150 SL50/TP100 (R:R 1:2) on Bybit\n{'='*70}")
    print(f"  {'Month':<9}{'Trades':>7}{'W':>5}{'L':>5}{'Pts':>9}{'WR%':>8}")
    for mk in sorted(live['mo']):
        w,l,p=live['mo'][mk];n=w+l
        print(f"  {mk:<9}{n:>7}{w:>5}{l:>5}{p:>+9.0f}{(w/n*100 if n else 0):>7.1f}%")
