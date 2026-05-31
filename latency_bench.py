#!/usr/bin/env python3
"""
latency_bench.py — prove the Bybit WS-order latency win vs REST.
Requires API keys. Run on TESTNET first (BYBIT_TESTNET=true) — places real (fake) orders.

Measures round-trip for:
  A) REST  POST /v5/order/create
  B) WS    op order.create (persistent authed socket)

Places small market orders + immediately closes (reduce-only). TESTNET ONLY by default.
"""
import asyncio, os, time, statistics, logging
import bybit_api as B

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
QTY = os.getenv("BENCH_QTY", "0.001")
N   = int(os.getenv("BENCH_N", "5"))


async def main():
    if not (B.API_KEY and B.API_SECRET):
        print("Set BYBIT_API_KEY / BYBIT_API_SECRET first."); return
    if not B._TESTNET:
        print("REFUSING: set BYBIT_TESTNET=true — this places real orders otherwise."); return

    print(f"Bench on TESTNET | symbol={B.SYMBOL} qty={QTY} N={N}\n")

    # ── A) REST ──
    rest_ms = []
    for i in range(N):
        s = time.time()
        r = B.place_market_order_rest("Buy", QTY)
        rest_ms.append((time.time() - s) * 1000)
        B.place_market_order_rest("Sell", QTY)   # close
        await asyncio.sleep(0.5)
    print(f"REST  order round-trip ms: {[round(x) for x in rest_ms]}  median={statistics.median(rest_ms):.0f}")

    # ── B) WS ──
    tws = B.BybitTradeWS()
    ok = await tws.connect()
    if not ok:
        print("WS auth failed — check keys/permissions."); return
    ws_ms = []
    for i in range(N):
        s = time.time()
        r = await tws.place_market("Buy", QTY)
        ws_ms.append((time.time() - s) * 1000)
        await tws.place_market("Sell", QTY)   # close
        await asyncio.sleep(0.5)
    print(f"WS    order round-trip ms: {[round(x) for x in ws_ms]}  median={statistics.median(ws_ms):.0f}")

    print(f"\n>>> WS saves ~{statistics.median(rest_ms) - statistics.median(ws_ms):.0f}ms per order vs REST")


if __name__ == "__main__":
    asyncio.run(main())
