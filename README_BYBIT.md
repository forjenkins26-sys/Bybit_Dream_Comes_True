# Bybit Bot — Build Status

Port of the Vol Surge strategy to Bybit, using **WebSocket order placement** (the latency
upgrade Delta can't give). USDT-margined perp (BTCUSDT).

## Why Bybit
- **WS order placement** (`order.create`) — no per-order HTTP round-trip → lower latency, less slippage.
- **Attached SL/TP** on the order itself (`stopLoss`/`takeProfit`) — atomic, server-side, simpler + safer than Delta's separate bracket call.
- Kline has a **`confirm` flag** (true = bar closed) → no forced-close watchdog needed.
- FIU-IND registered (legal for India, post-KYC).

## Files

| File | Status | Purpose |
|------|--------|---------|
| `signal_engine.py` | ✅ reused (unchanged from Anand) | Vol Surge signal — TF/strategy agnostic |
| `bybit_feed.py` | ✅ built + live-tested | Public WS kline feed, emits closed candles via `confirm` |
| `bybit_api.py` | ✅ built (public tested; auth needs keys) | REST + **WS-trade order placement** + auth |
| `latency_bench.py` | ✅ built (needs keys + testnet) | Proves WS-order vs REST latency win |
| `bybit_live.py` | ⏳ TODO | Main bot loop: feed → engine → WS order. Port of volsurge_v5_live |

## Verified so far (no keys needed)
- Public WS kline: **working** — backfill 299 candles + live closed bars with `confirm` flag ✅
- REST connectivity: **working** (server time) ✅

## What's left (needs YOUR keys)
1. Set env: `BYBIT_API_KEY`, `BYBIT_API_SECRET`, `BYBIT_TESTNET=true`
2. Run `python latency_bench.py` → measures real WS-order vs REST latency on **testnet** (fake money)
3. Build `bybit_live.py` — wire feed → engine → WS order with the chosen strategy config
4. Validate on testnet, then flip `BYBIT_TESTNET=false` for live

## Strategy config (from Testing Strategy winners — no slippage backtest)
- **Live-ready pick:** 1m, MIN_BODY=150, burst=2.0, **R:R 1:2 (SL50/TP100)** — 82% WR, PF 9.28
- Or max-money: 1m MB50 SL75/TP225 (1:3) — but small SL = slippage-sensitive
- Engine config maps 1:1 (same `SignalConfig` params as Delta bots)

## Symbol / margin differences vs Delta
- Symbol: `BTCUSDT` (was `BTCUSD`)
- USDT-margined (was INR/USD) — qty in BTC, P&L in USDT
- `qty` min step + tick size: check `/v5/market/instruments-info`

## Endpoints
| | Live | Testnet |
|--|------|---------|
| REST | api.bybit.com | api-testnet.bybit.com |
| Public WS | stream.bybit.com/v5/public/linear | stream-testnet... |
| Trade WS | stream.bybit.com/v5/trade | stream-testnet... |
| Private WS | stream.bybit.com/v5/private | stream-testnet... |

## Next step
**Get testnet API keys** (testnet.bybit.com → API Management) → run `latency_bench.py` to confirm
the WS latency win is real, then I'll build `bybit_live.py`.
