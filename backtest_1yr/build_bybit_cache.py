#!/usr/bin/env python3
"""
build_bybit_cache.py — pull ~1 year of 1m BTCUSDT klines from Bybit REST.
Endpoint: /v5/market/kline (category=linear, interval=1, limit=1000, newest-first).
Paginates backwards via `end` cursor. Saves bybit_cache_1yr.json (oldest->newest).
"""
import json, time, sys
from pathlib import Path
import requests

REST = "https://api.bybit.com/v5/market/kline"
SYMBOL = "BTCUSDT"
INTERVAL = "1"          # minutes
DAYS = int(sys.argv[sys.argv.index("--days")+1]) if "--days" in sys.argv else 366
OUT = Path(__file__).parent / "bybit_cache_1yr.json"

now_ms   = int(time.time()*1000)
start_ms = now_ms - DAYS*86400*1000
PER_MS   = 60*1000

bars = {}   # start_ms -> [o,h,l,c,v]
end = now_ms
sess = requests.Session()
calls = 0
print(f"Pulling {DAYS}d of 1m {SYMBOL} from Bybit...")
while end > start_ms:
    try:
        r = sess.get(REST, params={"category":"linear","symbol":SYMBOL,"interval":INTERVAL,
                                   "end":end,"limit":1000}, timeout=20).json()
    except Exception as e:
        print(f"  [retry] {e}"); time.sleep(1); continue
    lst = r.get("result",{}).get("list",[])
    if not lst:
        print(f"  empty response, stopping. retMsg={r.get('retMsg')}"); break
    for row in lst:   # [start_ms, o,h,l,c,v,turnover]
        ts = int(row[0])
        bars[ts] = [float(row[1]),float(row[2]),float(row[3]),float(row[4]),float(row[5])]
    oldest = min(int(x[0]) for x in lst)
    end = oldest - 1
    calls += 1
    if calls % 20 == 0:
        from datetime import datetime, timezone
        print(f"  calls={calls} bars={len(bars):,} oldest={datetime.fromtimestamp(oldest/1000,timezone.utc).strftime('%d-%b-%Y %H:%M')}", flush=True)
    time.sleep(0.12)   # rate-limit courtesy

# Save oldest->newest as [{ts(sec),open,high,low,close,volume}]
rows = []
for ts in sorted(bars):
    o,h,l,c,v = bars[ts]
    rows.append({"ts":ts//1000,"open":o,"high":h,"low":l,"close":c,"volume":v})
OUT.write_text(json.dumps(rows, separators=(",",":")), encoding="utf-8")
from datetime import datetime, timezone
f = datetime.fromtimestamp(rows[0]["ts"],timezone.utc).strftime("%d-%b-%Y")
t = datetime.fromtimestamp(rows[-1]["ts"],timezone.utc).strftime("%d-%b-%Y")
print(f"\n[OK] {len(rows):,} candles saved: {OUT}")
print(f"     Range: {f} -> {t}  | calls={calls}")
