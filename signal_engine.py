#!/usr/bin/env python3
"""
signal_engine.py — Pine Vol Surge v5 logic ported to Python
============================================================
Implements EXACT Pine parity for:
  - Heikin-Ashi candle conversion (when use_ha=True — matches TradingView HA chart)
  - True Range  (ta.tr(true))
  - ATR5        (ta.atr(5) — Wilder's RMA, alpha=1/5)
  - EMA200      (ta.ema(close, 200) — alpha=2/201)
  - Chop avg TR (average of TR[1..lookback] — the 5 bars BEFORE current)
  - Burst detection (body >= chopAvgTR × burst_mult)
  - SL distance (atr5[1] × sl_mult — previous bar's ATR5)
  - Cooldown counter
  - Session filter (IST = UTC+5:30)

IMPORTANT — Heikin-Ashi:
  TradingView chart set to Heikin-Ashi produces 78% WR vs 49% WR on regular candles
  (46 trades vs 173 trades — HA is far more selective).
  use_ha=True (default) converts regular OHLC from Delta WebSocket to HA before
  running all indicators — this is REQUIRED for Pine parity when Pine chart is HA.

Does NOT:
  - Place orders
  - Manage trade state
  - Connect to any exchange

Pine references (Volume surge 5 latest.txt):
  Line 46  : ema200  = ta.ema(close, emaLen)
  Line 47  : atr5    = ta.atr(5)
  Lines 53-57 : chopAvgTR = avg of ta.tr(true)[1..lookback]
  Line 58  : burstThreshold = chopAvgTR * vsBurstMult
  Lines 59-60 : isBurstBull / isBurstBear
  Line 62  : vsSLDist = atr5[1] * vsSLMult        (previous bar ATR5!)
  Lines 167-168: signal gated by barstate.isconfirmed
"""

import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import List, Optional

# ── Config ────────────────────────────────────────────────────────────────────

@dataclass
class SignalConfig:
    """Mirrors Pine input defaults from Volume surge 5 latest.txt."""
    lookback:     int   = 5      # vsLookback — chop window (bars before current)
    burst_mult:   float = 2.0    # vsBurstMult
    sl_mult:      float = 1.8    # vsSLMult  — Pine default 1.8
    tp1_r:        float = 1.0    # vsTP1R (kept for completeness)
    tp2_r:        float = 1.3    # vsTP2R — Pine default 1.3 (matches pine_volsurge_v5.pine vsTP2R)
    cooldown:     int   = 3      # vsCooldown bars after signal

    ema_length:   int   = 200    # emaLen
    use_ema_filter: bool = False  # useEmaFilt (default OFF in Pine)
    use_1h_gate:  bool  = False  # use1hGate  (default OFF in Pine)

    use_session:  bool  = False  # useSession (default OFF in Pine)

    safety_factor: float = 1.0   # SIGNAL_SAFETY_FACTOR — 1.0 = exact Pine parity (no buffer)
                                  # body must exceed burst_threshold * safety_factor to fire
                                  # increase (e.g. 1.15) to add a +15% guard vs Pine

    # ── Heikin-Ashi ────────────────────────────────────────────────────────────
    # use_ha=True  → convert regular OHLC to HA before running all indicators.
    #                REQUIRED when TradingView chart is set to Heikin-Ashi.
    #                Produces 78% WR (46 trades) vs 49% WR (173 trades) on regular.
    # use_ha=False → run indicators on raw OHLC (only if TV chart is regular candles).
    use_ha: bool = True

    # ── Momentum Quality Filter A: Min Absolute Body ───────────────────────────
    # Blocks signal when candle_body < min_body_pts regardless of burst threshold.
    # Prevents entries when chop is tiny and dynamic threshold drops too low.
    # Pine equivalent: useMinBody / minBodyPts (default ON, 250 pts for BTC 5m)
    use_min_body: bool  = True
    min_body_pts: float = 250.0

    # ── Breakout Context Filter ────────────────────────────────────────────────
    # Require burst candle to actually break the prior N-bar range.
    # BUY:  close > ta.highest(high[1], breakout_ctx_bars)
    # SELL: close < ta.lowest(low[1],   breakout_ctx_bars)
    # Pine: useBreakoutCtx=false (default OFF), breakoutCtxBars=8
    # Filters out big bodies inside consolidation that don't break any level.
    use_breakout_ctx:  bool  = False
    breakout_ctx_bars: int   = 8    # Pine default: breakoutCtxBars=8

    # IST = UTC+5:30 session windows
    london_open:  int   = 11
    london_close: int   = 17
    ny_open:      int   = 18
    ny_close:     int   = 23
    use_asian:    bool  = True
    asian_open:   int   = 6
    asian_close:  int   = 10


# ── Result types ──────────────────────────────────────────────────────────────

@dataclass
class IndicatorState:
    """All computed values for one just-closed candle. Used for parity logging."""
    ts:              int
    close:           float
    candle_body:     float
    tr:              float
    chop_avg_tr:     float
    burst_threshold: float
    is_burst_bull:   bool
    is_burst_bear:   bool
    atr5:            float     # ATR5 at current bar
    atr5_prev:       float     # ATR5 at previous bar (= atr5[1] in Pine)
    sl_dist:         float     # = atr5_prev * sl_mult
    ema200:          float
    above_ema:       bool
    below_ema:       bool
    session_ok:      bool
    cooldown_ok:     bool
    cooldown_left:   int
    signal:          str       # "BUY", "SELL", or ""
    bars_in_buffer:  int
    ema_bars_used:   int
    warmup_warning:  str


@dataclass
class SignalResult:
    """Emitted only when a signal fires."""
    signal:      str    # "BUY" or "SELL"
    ts:          int
    entry_price: float
    sl_dist:     float
    sl:          float
    tp1:         float  # entry ± sl_dist × tp1_r
    tp2:         float  # entry ± sl_dist × tp2_r  (= single TP in v5)
    state:       IndicatorState


# ── Heikin-Ashi conversion ───────────────────────────────────────────────────

def compute_ha_candles(candles: list) -> list:
    """
    Convert a list of regular OHLC Candle objects to Heikin-Ashi candles.

    Matches TradingView's Heikin-Ashi calculation exactly:
      HA_Close[i] = (Open[i] + High[i] + Low[i] + Close[i]) / 4
      HA_Open[i]  = (HA_Open[i-1] + HA_Close[i-1]) / 2
      HA_High[i]  = max(High[i], HA_Open[i], HA_Close[i])
      HA_Low[i]   = min(Low[i],  HA_Open[i], HA_Close[i])

    Seed (bar 0):
      HA_Open[0]  = (Open[0] + Close[0]) / 2
      HA_Close[0] = (Open[0] + High[0] + Low[0] + Close[0]) / 4

    Returns a new list of Candle objects with HA values.
    Timestamps and volume are preserved unchanged from the original candles.

    WHY THIS MATTERS:
      HA smooths noise — candle bodies are larger in trending bars and smaller
      in choppy bars. This makes burst detection far more selective:
        Regular candles → 173 trades, 49% WR
        HA candles      →  46 trades, 78% WR  (same Pine strategy)
    """
    from candle_feed import Candle  # local import to avoid circular dependency
    if not candles:
        return []

    ha = []
    # Seed bar 0
    ha_open_prev  = (candles[0].open + candles[0].close) / 2
    ha_close_prev = (candles[0].open + candles[0].high + candles[0].low + candles[0].close) / 4
    ha_high       = max(candles[0].high, ha_open_prev, ha_close_prev)
    ha_low        = min(candles[0].low,  ha_open_prev, ha_close_prev)
    ha.append(Candle(
        ts=candles[0].ts, open=round(ha_open_prev, 2), high=round(ha_high, 2),
        low=round(ha_low, 2),   close=round(ha_close_prev, 2), volume=candles[0].volume,
    ))

    for i in range(1, len(candles)):
        c = candles[i]
        ha_close = (c.open + c.high + c.low + c.close) / 4
        ha_open  = (ha_open_prev + ha_close_prev) / 2
        ha_high  = max(c.high, ha_open, ha_close)
        ha_low   = min(c.low,  ha_open, ha_close)
        ha.append(Candle(
            ts=c.ts, open=round(ha_open, 2), high=round(ha_high, 2),
            low=round(ha_low, 2), close=round(ha_close, 2), volume=c.volume,
        ))
        ha_open_prev  = ha_open
        ha_close_prev = ha_close

    return ha


# ── Low-level math — exact Pine parity ───────────────────────────────────────

def compute_tr(high: float, low: float, prev_close: float) -> float:
    """
    Pine: ta.tr(true) — True Range with gap fill.
    max(high-low, |high-prev_close|, |low-prev_close|)
    """
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def compute_atr_rma(trs: List[float], period: int) -> List[float]:
    """
    Pine: ta.atr(period) — Wilder's RMA.
      alpha = 1/period
      seed  = SMA of first `period` values
      rma[i] = alpha * tr[i] + (1 - alpha) * rma[i-1]

    Returns a list the same length as `trs`.
    Values before index (period-1) are 0.0.
    """
    n = len(trs)
    if n == 0:
        return []
    result = [0.0] * n
    if n < period:
        return result

    alpha = 1.0 / period
    # Seed from SMA of first `period` values
    seed = sum(trs[:period]) / period
    result[period - 1] = seed

    for i in range(period, n):
        result[i] = alpha * trs[i] + (1 - alpha) * result[i - 1]

    return result


def compute_ema_series(prices: List[float], length: int) -> List[float]:
    """
    Pine: ta.ema(source, length).
      alpha = 2 / (length + 1)
      seed  = first price (Pine also seeds from bar 0)
      ema[i] = alpha * price[i] + (1 - alpha) * ema[i-1]

    Note: Pine's EMA seeds from bar 0 of the full chart history.
    Our seed is from bar 0 of the backfill window (~300 bars).
    Convergence error at bar 300: (1 - 2/201)^300 ≈ 5% — acceptable.
    """
    n = len(prices)
    if n == 0:
        return []
    alpha  = 2.0 / (length + 1)
    result = [0.0] * n
    result[0] = prices[0]
    for i in range(1, n):
        result[i] = alpha * prices[i] + (1 - alpha) * result[i - 1]
    return result


def _ist_hour(ts_unix: int) -> int:
    """Return IST hour (UTC+5:30) for a Unix timestamp."""
    IST = timezone(timedelta(hours=5, minutes=30))
    return datetime.fromtimestamp(ts_unix, tz=IST).hour


def _check_session(ts_unix: int, cfg: SignalConfig) -> bool:
    """Pine: sessionOK — checks IST hour against active windows."""
    if not cfg.use_session:
        return True
    h = _ist_hour(ts_unix)
    in_london = cfg.london_open <= h < cfg.london_close
    in_ny     = cfg.ny_open     <= h < cfg.ny_close
    in_asian  = cfg.use_asian and cfg.asian_open <= h < cfg.asian_close
    return in_london or in_ny or in_asian


def _session_label(ts_unix: int, cfg: SignalConfig) -> str:
    if not cfg.use_session:
        return "OFF"
    h = _ist_hour(ts_unix)
    if cfg.london_open <= h < cfg.london_close: return "LON"
    if cfg.ny_open     <= h < cfg.ny_close:     return "NY"
    if cfg.use_asian and cfg.asian_open <= h < cfg.asian_close: return "ASN"
    return "OFF"


# ── Core indicator computation ────────────────────────────────────────────────

def compute_indicators(
    buffer:       deque,
    cfg:          SignalConfig,
    cooldown_left: int,
    in_trade:     bool = False,
) -> Optional[IndicatorState]:
    """
    Compute all Pine indicators for the most recent closed candle in `buffer`.

    When cfg.use_ha=True (default), regular OHLC candles are first converted to
    Heikin-Ashi before all indicator calculations. This matches TradingView when
    the chart is set to Heikin-Ashi candle type (78% WR vs 49% WR on regular).

    Indexing mirrors Pine's bar-relative notation:
      candle_list[-1]     = current bar (just closed) = bar[0] in Pine
      candle_list[-2]     = previous bar              = bar[1] in Pine
      trs[-2]             = ta.tr(true)[1] in Pine
      atr5_series[-2]     = atr5[1]        in Pine

    Parameters
    ----------
    buffer        : deque of regular OHLC Candle objects, newest last
    cfg           : SignalConfig matching Pine inputs
    cooldown_left : bars of cooldown remaining BEFORE this bar's decrement
    in_trade      : True if bot already has an open position
    """
    raw_candles = list(buffer)
    n = len(raw_candles)

    # ── Heikin-Ashi conversion (before any indicator computation) ─────────────
    # If use_ha=True, all indicators (ATR, EMA, body, chop) run on HA values.
    # This is critical for Pine parity when the TradingView chart is HA.
    if cfg.use_ha and n >= 2:
        candles = compute_ha_candles(raw_candles)
    else:
        candles = raw_candles

    # Re-measure after potential HA conversion (length stays the same, just safety)
    n = len(candles)

    # Need: lookback+1 bars before current + current = lookback+2 minimum
    # Also need 1 bar before that for ATR prev_close = lookback+3 minimum
    min_bars = max(cfg.ema_length + 10, cfg.lookback + 8)

    warmup_warning = ""
    if n < min_bars:
        warmup_warning = f"WARMUP: only {n} bars (need {min_bars} for full EMA{cfg.ema_length})"

    if n < cfg.lookback + 4:
        return None   # not enough bars for even basic computation

    curr = candles[-1]
    prev = candles[-2]

    # ── True Range series ─────────────────────────────────────────────────────
    # trs[0]: H-L of oldest bar (no prev close available — approximation)
    # trs[i]: true TR of candles[i] using candles[i-1].close for i >= 1
    trs: List[float] = [candles[0].high - candles[0].low]
    for i in range(1, n):
        trs.append(compute_tr(candles[i].high, candles[i].low, candles[i - 1].close))

    # ── ATR5 (Wilder's RMA) ───────────────────────────────────────────────────
    # Feed trs[1:] to skip the approximated first TR.
    # atr5_list[j] corresponds to candles[j+1].
    # atr5_list[-1] → ATR5 at current bar   = atr5     in Pine
    # atr5_list[-2] → ATR5 at previous bar  = atr5[1]  in Pine
    atr5_list = compute_atr_rma(trs[1:], period=5)
    atr5_curr = atr5_list[-1] if len(atr5_list) >= 5  else 0.0
    atr5_prev = atr5_list[-2] if len(atr5_list) >= 6  else 0.0

    # ── SL distance (Pine line 62) ────────────────────────────────────────────
    # vsSLDist = atr5[1] * vsSLMult  →  uses PREVIOUS bar's ATR5
    sl_dist = round(atr5_prev * cfg.sl_mult, 1)

    # ── Chop avg TR (Pine lines 53-57) ────────────────────────────────────────
    # chopAvgTR = average of ta.tr(true)[1..lookback]
    # ta.tr(true)[1] = trs[-2], ta.tr(true)[2] = trs[-3], ...
    # ta.tr(true)[lookback] = trs[-(lookback+1)]
    chop_window = [trs[-(i + 2)] for i in range(cfg.lookback)]
    chop_avg_tr = sum(chop_window) / cfg.lookback if len(chop_window) == cfg.lookback else 0.0

    # ── Burst (Pine lines 58-60) ──────────────────────────────────────────────
    burst_threshold       = chop_avg_tr * cfg.burst_mult
    effective_burst       = burst_threshold * cfg.safety_factor   # safety buffer
    candle_body           = abs(curr.close - curr.open)
    is_burst_bull         = candle_body >= effective_burst and curr.close > curr.open
    is_burst_bear         = candle_body >= effective_burst and curr.close < curr.open

    # ── Momentum Quality Filter A: Min Absolute Body ──────────────────────────
    # Pine: minBodyOK = not useMinBody or candleBody >= minBodyPts
    min_body_ok = not cfg.use_min_body or candle_body >= cfg.min_body_pts

    # ── Breakout Context Filter ────────────────────────────────────────────────
    # Pine: recentHigh = ta.highest(high[1], breakoutCtxBars)  (8 bars default)
    #       recentLow  = ta.lowest(low[1],   breakoutCtxBars)
    # high[1] = candles[-2], high[N] = candles[-(N+1)]
    # Window = candles[-(N+1):-1] = N bars before current bar, excl. current.
    N = cfg.breakout_ctx_bars
    if cfg.use_breakout_ctx and len(candles) >= N + 2:
        prior = candles[-(N + 1):-1]  # high[1]..high[N] in Pine notation
        prior_range_high  = max(c.high for c in prior)
        prior_range_low   = min(c.low  for c in prior)
        breakout_ctx_bull = curr.close > prior_range_high
        breakout_ctx_bear = curr.close < prior_range_low
    else:
        breakout_ctx_bull = True   # filter OFF or insufficient bars → always pass
        breakout_ctx_bear = True

    # ── EMA200 (Pine line 46) ─────────────────────────────────────────────────
    closes      = [c.close for c in candles]
    ema_series  = compute_ema_series(closes, cfg.ema_length)
    ema200      = ema_series[-1] if ema_series else 0.0
    above_ema   = curr.close > ema200
    below_ema   = curr.close < ema200
    ema_ok_long  = not cfg.use_ema_filter or above_ema
    ema_ok_short = not cfg.use_ema_filter or below_ema

    # ── 1H Gate (disabled by default — placeholder, always passes) ────────────
    gate_ok_long  = True   # use1hGate = false in Pine defaults
    gate_ok_short = True

    # ── Session (Pine lines 77-81) ────────────────────────────────────────────
    session_ok = _check_session(curr.ts, cfg)

    # ── Cooldown (Pine lines 84-87) ───────────────────────────────────────────
    # Pine: decrement at start of each bar, THEN check
    effective_cooldown = max(0, cooldown_left - 1)
    cooldown_ok        = effective_cooldown == 0

    # ── Signal (Pine lines 167-168) ───────────────────────────────────────────
    signal = ""
    if not in_trade and cooldown_ok and session_ok:
        if is_burst_bull and ema_ok_long and gate_ok_long and min_body_ok and breakout_ctx_bull:
            signal = "BUY"
        elif is_burst_bear and ema_ok_short and gate_ok_short and min_body_ok and breakout_ctx_bear:
            signal = "SELL"

    return IndicatorState(
        ts              = curr.ts,
        close           = curr.close,
        candle_body     = candle_body,
        tr              = trs[-1],
        chop_avg_tr     = chop_avg_tr,
        burst_threshold = burst_threshold,
        is_burst_bull   = is_burst_bull,
        is_burst_bear   = is_burst_bear,
        atr5            = atr5_curr,
        atr5_prev       = atr5_prev,
        sl_dist         = sl_dist,
        ema200          = ema200,
        above_ema       = above_ema,
        below_ema       = below_ema,
        session_ok      = session_ok,
        cooldown_ok     = cooldown_ok,
        cooldown_left   = effective_cooldown,
        signal          = signal,
        bars_in_buffer  = n,
        ema_bars_used   = n,
        warmup_warning  = warmup_warning,
    )


# ── SignalEngine class ────────────────────────────────────────────────────────

class SignalEngine:
    """
    Stateful wrapper around compute_indicators().
    Tracks the cooldown counter across bar closes.

    Usage:
        engine = SignalEngine()
        # On each candle close:
        result = engine.on_candle_close(candle, feed.buffer, in_trade=False)
        if result and result.signal:
            # signal fired
    """

    def __init__(self, config: SignalConfig = None, logger: logging.Logger = None):
        self.cfg          = config or SignalConfig()
        self._cooldown    = 0    # bars of cooldown remaining
        self.log          = logger or logging.getLogger("signal_engine")
        self._bar_count   = 0

    def reset_cooldown(self):
        self._cooldown = 0

    def on_candle_close(
        self,
        candle,         # Candle from candle_feed
        buffer: deque,
        in_trade: bool = False,
    ) -> Optional[IndicatorState]:
        """
        Process a closed candle. Returns IndicatorState with .signal set to
        "BUY", "SELL", or "" (no signal).

        Call this from the CandleFeed on_candle_close callback.
        """
        self._bar_count += 1

        state = compute_indicators(buffer, self.cfg, self._cooldown, in_trade)
        if state is None:
            self.log.debug(f"[ENGINE] Skipping bar #{self._bar_count} — insufficient buffer")
            return None

        # Update cooldown AFTER signal generation (matches Pine order)
        if state.signal in ("BUY", "SELL"):
            self._cooldown = self.cfg.cooldown
        elif self._cooldown > 0:
            self._cooldown -= 1

        self._log_bar(state)

        return state

    def build_signal_result(self, state: IndicatorState) -> Optional[SignalResult]:
        """Convert IndicatorState → SignalResult when state.signal is set."""
        if not state.signal:
            return None

        d = state.signal
        entry = state.close
        sl    = round(entry - state.sl_dist, 1) if d == "BUY" else round(entry + state.sl_dist, 1)
        tp1   = round(entry + state.sl_dist * self.cfg.tp1_r, 1) if d == "BUY" else round(entry - state.sl_dist * self.cfg.tp1_r, 1)
        tp2   = round(entry + state.sl_dist * self.cfg.tp2_r, 1) if d == "BUY" else round(entry - state.sl_dist * self.cfg.tp2_r, 1)

        return SignalResult(
            signal      = d,
            ts          = state.ts,
            entry_price = entry,
            sl_dist     = state.sl_dist,
            sl          = sl,
            tp1         = tp1,
            tp2         = tp2,
            state       = state,
        )

    # ── Logging ───────────────────────────────────────────────────────────────

    def _log_bar(self, s: IndicatorState):
        """
        Detailed per-bar log for manual parity comparison against TradingView.
        Copy these values and compare line by line with Pine's status table.
        """
        from datetime import datetime, timezone
        dt_utc = datetime.fromtimestamp(s.ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        IST    = timezone(timedelta(hours=5, minutes=30))
        dt_ist = datetime.fromtimestamp(s.ts, tz=IST).strftime("%H:%M IST")

        burst_str = ""
        if s.is_burst_bull: burst_str = "BULL ✓"
        elif s.is_burst_bear: burst_str = "BEAR ✓"
        else: burst_str = f"none  (body={s.candle_body:.1f} < thresh={s.burst_threshold:.1f})"

        signal_str = s.signal if s.signal else "—"
        cooldown_str = f"{s.cooldown_left} bars" if s.cooldown_left > 0 else "READY"

        warn = f"\n  ⚠️  {s.warmup_warning}" if s.warmup_warning else ""

        ema_dir  = "ABOVE ▲" if s.above_ema else "BELOW ▼"
        ema_gate = "filter ON" if self.cfg.use_ema_filter else "filter OFF"
        sess_dir = "OK ✓" if s.session_ok else "OFF ✗"
        sess_gate = "filter ON" if self.cfg.use_session else "filter OFF"

        ha_label = "HA" if self.cfg.use_ha else "OHLC"
        self.log.info(
            f"\n[ENGINE] ── Bar #{self._bar_count} · {dt_utc} ({dt_ist}) [{ha_label}] ──────────────────\n"
            f"  close          : {s.close:>12,.1f}\n"
            f"  candle body    : {s.candle_body:>12,.1f} pts\n"
            f"  chop_avg_tr    : {s.chop_avg_tr:>12,.1f} pts  (avg TR of {self.cfg.lookback} bars before)\n"
            f"  burst_threshold: {s.burst_threshold:>12,.1f} pts  (chop x {self.cfg.burst_mult})\n"
            f"  burst          : {burst_str}\n"
            f"  tr (current)   : {s.tr:>12,.1f} pts\n"
            f"  atr5           : {s.atr5:>12,.2f} pts  (current bar)\n"
            f"  atr5[1]        : {s.atr5_prev:>12,.2f} pts  (prev bar -- Pine atr5[1])\n"
            f"  sl_dist        : {s.sl_dist:>12,.1f} pts  (= atr5[1] x {self.cfg.sl_mult})\n"
            f"  ema200         : {s.ema200:>12,.1f}  ({ema_dir})  ({ema_gate})\n"
            f"  session        : {sess_dir}  ({sess_gate})\n"
            f"  cooldown       : {cooldown_str}\n"
            f"  bars in buffer : {s.bars_in_buffer}\n"
            f"  ─────────────────────────────────────────────────────\n"
            f"  SIGNAL         : {signal_str}"
            f"{warn}\n"
        )

        if s.signal:
            sr = self.build_signal_result(s)
            if sr:
                self.log.info(
                    f"[ENGINE] 🔥 SIGNAL {s.signal}\n"
                    f"  entry : {sr.entry_price:,.1f}\n"
                    f"  sl    : {sr.sl:,.1f}  (−{sr.sl_dist:.1f} pts)\n"
                    f"  tp1   : {sr.tp1:,.1f}  (+{round(abs(sr.tp1 - sr.entry_price), 1)} pts)\n"
                    f"  tp2   : {sr.tp2:,.1f}  (+{round(abs(sr.tp2 - sr.entry_price), 1)} pts)  ← v5 uses this\n"
                )
