#!/usr/bin/env python3
"""
dry_run_compare.py — compare 1m candle closes: Bybit BTCUSDT vs Delta BTCUSD (live WS).
Verifies raw close + Heikin-Ashi close alignment between the two exchanges.
Runs until N matched closed bars captured, then prints comparison.

NOTE: Bybit=BTCUSDT (USDT perp), Delta=BTCUSD — different contracts, expect small diff.
"""
import asyncio, json, time, os
import websockets

N = int(os.getenv("CMP_BARS", "4"))
BYBIT = "wss://stream.bybit.com/v5/public/linear"
DELTA = "wss://socket.india.delta.exchange"

bybit_bars = {}   # start_ts(sec) -> dict(o,h,l,c)
delta_bars = {}

async def bybit_task():
    async with websockets.connect(BYBIT, ping_interval=None, open_timeout=15) as ws:
        await ws.send(json.dumps({"op":"subscribe","args":["kline.1.BTCUSDT"]}))
        async for f in ws:
            m=json.loads(f)
            if not m.get("topic","").startswith("kline."): continue
            for d in m.get("data",[]):
                if d.get("confirm"):
                    ts=int(d["start"])//1000
                    bybit_bars[ts]={"o":float(d["open"]),"h":float(d["high"]),"l":float(d["low"]),"c":float(d["close"])}
                    print(f"  [BYBIT] closed {ts} C={d['close']}")

async def delta_task():
    async with websockets.connect(DELTA, ping_interval=None, open_timeout=15) as ws:
        await ws.send(json.dumps({"type":"subscribe","payload":{"channels":[{"name":"candlestick_1m","symbols":["BTCUSD"]}]}}))
        forming={"ts":None}
        async for f in ws:
            m=json.loads(f)
            mt=str(m.get("type",m.get("channel","")))
            if "candlestick" not in mt: continue
            data=m.get("data",m)
            if isinstance(data,list): data=data[0] if data else {}
            raw_ts=int(data.get("candle_start_time",data.get("start",0)) or 0)
            if raw_ts>1_000_000_000_000_000: raw_ts//=1_000_000
            elif raw_ts>1_000_000_000_000: raw_ts//=1000
            if raw_ts==0: continue
            o,h,l,c=float(data.get("open",0)),float(data.get("high",0)),float(data.get("low",0)),float(data.get("close",0))
            if forming["ts"] is None:
                forming.update(ts=raw_ts,o=o,h=h,l=l,c=c); continue
            if raw_ts!=forming["ts"]:
                delta_bars[forming["ts"]]={"o":forming["o"],"h":forming["h"],"l":forming["l"],"c":forming["c"]}
                print(f"  [DELTA] closed {forming['ts']} C={forming['c']}")
                forming.update(ts=raw_ts,o=o,h=h,l=l,c=c)
            else:
                forming.update(h=max(forming["h"],h),l=min(forming["l"],l),c=c)

async def main():
    print(f"Comparing 1m closes — Bybit BTCUSDT vs Delta BTCUSD | need {N} matched bars\n")
    t=[asyncio.create_task(bybit_task()),asyncio.create_task(delta_task())]
    # wait until N matched timestamps
    while True:
        matched=sorted(set(bybit_bars)&set(delta_bars))
        if len(matched)>=N: break
        await asyncio.sleep(2)
    for x in t: x.cancel()
    print(f"\n{'='*78}\n  1m CLOSE COMPARISON (HA = (O+H+L+C)/4)\n{'='*78}")
    print(f"  {'BarTime(UTC)':<18}{'Bybit C':>10}{'Delta C':>10}{'dC':>8}{'Bybit HA':>10}{'Delta HA':>10}{'dHA':>8}")
    print("  "+"-"*74)
    from datetime import datetime,timezone
    dcs=[]; dhas=[]
    for ts in matched[-N:]:
        b=bybit_bars[ts]; d=delta_bars[ts]
        bha=(b["o"]+b["h"]+b["l"]+b["c"])/4; dha=(d["o"]+d["h"]+d["l"]+d["c"])/4
        dc=b["c"]-d["c"]; dh=bha-dha; dcs.append(abs(dc)); dhas.append(abs(dh))
        tt=datetime.fromtimestamp(ts,timezone.utc).strftime("%H:%M")
        print(f"  {tt:<18}{b['c']:>10.1f}{d['c']:>10.1f}{dc:>+8.1f}{bha:>10.1f}{dha:>10.1f}{dh:>+8.1f}")
    print("  "+"-"*74)
    print(f"  avg |dClose|={sum(dcs)/len(dcs):.1f}pts   avg |dHA|={sum(dhas)/len(dhas):.1f}pts")

if __name__=="__main__":
    asyncio.run(main())
