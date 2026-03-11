"""
Up/Down Momentum Trader — Standalone Polymarket Module

Trades BTC/ETH/SOL/XRP "Up or Down" 15-minute markets on Polymarket.
Runs for a specified number of hours then auto-shuts off.

Based on: github.com/undertow-tez/fathom-builds/skills/crypto-updown-trader

Usage:
    python3 updown_trader.py --hours 1 --budget 20 --assets btc,eth
    python3 updown_trader.py --hours 2 --bet-size 3 --assets btc
    python3 updown_trader.py --dry-run --hours 1

Architecture:
    - Fires at :08, :23, :38, :53 of each hour (4 windows per hour per asset)
    - Momentum scoring: MA alignment, RSI, candle direction, volatility, volume
    - Score >= 4 triggers bet, v3.0 filters: blackout, DOWN qual, hourly trend, score cap, drawdown, cooldown
    - Ties resolve UP (structural edge)
    - Slug-based market discovery via Gamma API
    - Bankr API for bet execution
    - Auto-shutdown after --hours expires
"""

import os
import sys
import json
import time
import math
import argparse
import requests
from datetime import datetime, timedelta, timezone

from company_clock import now_et, is_weekend, in_hours, current_hour_et, current_day_name, status as clock_status
from hedge_fund_config import (
    ENABLE_UPDOWN_MODULE, UPDOWN_BET_SIZE, UPDOWN_MIN_SCORE,
    UPDOWN_MAX_DAILY, UPDOWN_DRAWDOWN_LIMIT, UPDOWN_COOLDOWN_MINUTES,
    UPDOWN_COOLDOWN_AFTER_LOSSES, UPDOWN_UP_ONLY, UPDOWN_MAX_PRICE,
    UPDOWN_MIN_PRICE, UPDOWN_ASSETS, UPDOWN_MAX_CONCURRENT
)

# ============================================================
# CONFIG
# ============================================================
BINANCE_US_URL = "https://api.binance.us/api/v3/klines"
GAMMA_API_URL = "https://gamma-api.polymarket.com/markets"

ASSET_SYMBOLS = {
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
    "xrp": "XRPUSDT",
}

ASSET_SLUGS = {
    "btc": "btc-updown",
    "eth": "eth-updown",
    "sol": "sol-updown",
    "xrp": "xrp-updown",
}

DEFAULT_BUDGET = 20.0
DEFAULT_BET_SIZE = UPDOWN_BET_SIZE  # From config (base size, actual scales $5-$10)
DEFAULT_HOURS = 1
DEFAULT_ASSETS = ["btc"]
MIN_SCORE_DEFAULT = UPDOWN_MIN_SCORE  # From config (6.0 = HIGH only)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hedge_fund_performance.db')
LOCK_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.updown_locks')

# ============================================================
# v3.0 FILTERS (from fathom-builds reference)
# ============================================================
BLACKOUT_HOURS_ET = [11, 12, 13]  # 11 AM - 2 PM ET (12.5% WR historically)
BLACKOUT_WEEKENDS = True          # Sat+Sun blackout (18% WR historically)
MAX_SCORE = 6.5                   # Scores >5 = momentum trap (33% WR)
DRAWDOWN_LIMIT = UPDOWN_DRAWDOWN_LIMIT  # From config ($10)
COOLDOWN_MINUTES = UPDOWN_COOLDOWN_MINUTES  # From config (45 min)
DOWN_MIN_SCORE = -4.0             # DOWN needs score <= -4
DOWN_MIN_VOL = 0.05               # DOWN needs volatility > 0.05%
DOWN_RSI_MIN = 30                 # DOWN needs RSI 30-45
DOWN_RSI_MAX = 45


# ============================================================
# DATA: Fetch 1-min candles from Binance US
# ============================================================

def fetch_candles(asset, limit=60):
    """Fetch 1-minute candles from Binance US."""
    symbol = ASSET_SYMBOLS.get(asset)
    if not symbol:
        print(f"  [!] Unknown asset: {asset}")
        return None

    try:
        resp = requests.get(BINANCE_US_URL, params={
            "symbol": symbol,
            "interval": "1m",
            "limit": limit,
        }, timeout=15)

        if resp.status_code != 200:
            print(f"  [!] Binance US HTTP {resp.status_code} for {symbol}")
            return None

        data = resp.json()
        candles = []
        for c in data:
            candles.append({
                "time": c[0],
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": float(c[5]),
            })
        return candles

    except Exception as e:
        print(f"  [!] Binance US error for {asset}: {e}")
        return None


# ============================================================
# INDICATORS
# ============================================================

def calc_ma(prices, period):
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period


def calc_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0  # neutral
    gains = 0.0
    losses = 0.0
    for i in range(len(prices) - period, len(prices)):
        diff = prices[i] - prices[i - 1]
        if diff >= 0:
            gains += diff
        else:
            losses -= diff
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    return 100.0 - (100.0 / (1.0 + rs))


def calc_volatility(candles, period=10):
    if len(candles) < period:
        return 0.0
    returns = []
    for c in candles[-period:]:
        if c["open"] != 0:
            returns.append((c["close"] - c["open"]) / c["open"])
    if not returns:
        return 0.0
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / len(returns)
    return math.sqrt(variance)


def calc_hourly_trend(candles):
    """Calculate the hourly price change % from 60 1-min candles."""
    if not candles or len(candles) < 30:
        return 0.0
    open_price = candles[0]["open"]
    close_price = candles[-1]["close"]
    if open_price == 0:
        return 0.0
    return ((close_price - open_price) / open_price) * 100


def consecutive_direction(candles, lookback=5):
    recent = candles[-lookback:]
    ups = sum(1 for c in recent if c["close"] >= c["open"])
    downs = lookback - ups
    streak = "UP" if ups > downs else "DOWN"
    strength = max(ups, downs) / lookback
    return {"ups": ups, "downs": downs, "streak": streak, "strength": strength}


# ============================================================
# STRATEGY: Momentum scoring (-5 to +5)
# ============================================================

def analyze(candles, min_score=MIN_SCORE_DEFAULT):
    """Score momentum. Positive = UP, negative = DOWN."""
    closes = [c["close"] for c in candles]
    current_price = closes[-1]

    ma5 = calc_ma(closes, 5)
    ma10 = calc_ma(closes, 10)
    ma20 = calc_ma(closes, 20)
    rsi = calc_rsi(closes, 14)
    vol = calc_volatility(candles, 10)
    low_vol = vol < 0.0005
    dir5 = consecutive_direction(candles, 5)
    dir10 = consecutive_direction(candles, 10)

    recent_vol = sum(c["volume"] for c in candles[-5:])
    prior_vol = sum(c["volume"] for c in candles[-10:-5])
    vol_rising = recent_vol > prior_vol * 1.2 if prior_vol > 0 else False

    score = 0.0
    reasons = []

    # 1. MA alignment (strongest signal, +/-2)
    if ma5 and ma10 and ma20:
        if ma5 > ma10 > ma20:
            score += 2
            reasons.append("MA5>MA10>MA20 (strong uptrend)")
        elif ma5 < ma10 < ma20:
            score -= 2
            reasons.append("MA5<MA10<MA20 (strong downtrend)")
        elif ma5 > ma10:
            score += 1
            reasons.append("MA5>MA10 (short-term bullish)")
        elif ma5 < ma10:
            score -= 1
            reasons.append("MA5<MA10 (short-term bearish)")

    # 2. Candle direction (+/-1.5)
    if dir5["strength"] >= 0.8:
        pts = 1.5 if dir5["streak"] == "UP" else -1.5
        score += pts
        reasons.append(f"{dir5['ups']}/5 candles UP ({dir5['streak']} momentum)")
    elif dir5["strength"] >= 0.6:
        pts = 0.5 if dir5["streak"] == "UP" else -0.5
        score += pts
        reasons.append(f"{dir5['ups']}/5 candles UP (weak {dir5['streak']})")

    # 3. RSI (+/-1)
    if rsi > 70:
        score -= 1
        reasons.append(f"RSI {rsi:.1f} (overbought)")
    elif rsi < 30:
        score += 1
        reasons.append(f"RSI {rsi:.1f} (oversold bounce)")
    elif rsi > 55:
        score += 0.5
        reasons.append(f"RSI {rsi:.1f} (bullish zone)")
    elif rsi < 45:
        score -= 0.5
        reasons.append(f"RSI {rsi:.1f} (bearish zone)")

    # 4. Low volatility = UP edge (ties resolve UP)
    if low_vol:
        score += 1
        reasons.append("Low volatility (tie edge -> UP)")

    # 5. Volume confirmation (1.2x multiplier)
    if vol_rising and abs(score) > 0:
        score *= 1.2
        reasons.append("Rising volume (confirms direction)")

    # Decision
    abs_score = abs(score)
    if abs_score >= 3:
        decision = "BET_UP" if score > 0 else "BET_DOWN"
        confidence = "HIGH"
    elif abs_score >= min_score:
        decision = "BET_UP" if score > 0 else "BET_DOWN"
        confidence = "MEDIUM"
    else:
        decision = "NO_BET"
        confidence = "LOW"

    return {
        "decision": decision,
        "confidence": confidence,
        "score": round(score, 2),
        "price": current_price,
        "rsi": round(rsi, 1),
        "ma5": round(ma5, 2) if ma5 else None,
        "ma10": round(ma10, 2) if ma10 else None,
        "ma20": round(ma20, 2) if ma20 else None,
        "volatility": round(vol * 100, 4),
        "dir5": f"{dir5['ups']}/5 UP",
        "dir10": f"{dir10['ups']}/10 UP",
        "vol_rising": vol_rising,
        "reasons": reasons,
    }


# ============================================================
# MARKET DISCOVERY: Slug-based Gamma API lookup
# ============================================================

def get_current_window_start():
    """Get the start of the current 15-min window in ET, return as UTC epoch."""
    et_now = now_et()

    # Round down to nearest 15-min boundary
    minute = et_now.minute
    window_minute = (minute // 15) * 15
    window_start_et = et_now.replace(minute=window_minute, second=0, microsecond=0)

    # Convert to UTC epoch
    return int(window_start_et.astimezone(timezone.utc).timestamp())


def find_market(asset, timeframe="15m"):
    """Find the active Up/Down market via slug-based lookup."""
    window_epoch = get_current_window_start()
    slug_base = ASSET_SLUGS.get(asset, f"{asset}-updown")
    slug = f"{slug_base}-{timeframe}-{window_epoch}"

    try:
        resp = requests.get(GAMMA_API_URL, params={"slug": slug}, timeout=10)
        if resp.status_code == 200:
            markets = resp.json()
            if markets and len(markets) > 0:
                mkt = markets[0]
                if not mkt.get("closed", True):
                    return {
                        "slug": slug,
                        "title": mkt.get("question", mkt.get("title", "")),
                        "condition_id": mkt.get("conditionId", ""),
                        "end_date": mkt.get("endDate", ""),
                        "tokens": mkt.get("tokens", []),
                        "outcomes": mkt.get("outcomes", []),
                        "outcomePrices": mkt.get("outcomePrices", []),
                    }
    except Exception as e:
        print(f"  [!] Gamma API error for {slug}: {e}")

    # Try previous window (markets may linger)
    prev_epoch = window_epoch - 900
    slug_prev = f"{slug_base}-{timeframe}-{prev_epoch}"
    try:
        resp = requests.get(GAMMA_API_URL, params={"slug": slug_prev}, timeout=10)
        if resp.status_code == 200:
            markets = resp.json()
            if markets and len(markets) > 0:
                mkt = markets[0]
                if not mkt.get("closed", True):
                    # Check it hasn't ended yet
                    end = mkt.get("endDate", "")
                    if end:
                        try:
                            end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
                            if end_dt > datetime.now(timezone.utc) + timedelta(minutes=2):
                                return {
                                    "slug": slug_prev,
                                    "title": mkt.get("question", mkt.get("title", "")),
                                    "condition_id": mkt.get("conditionId", ""),
                                    "end_date": end,
                                    "tokens": mkt.get("tokens", []),
                                    "outcomes": mkt.get("outcomes", []),
                                    "outcomePrices": mkt.get("outcomePrices", []),
                                }
                        except Exception:
                            pass
    except Exception:
        pass

    return None


def get_token_prices(market):
    """Extract Up/Down prices from Gamma API outcomePrices."""
    import json as _json
    oc_names = market.get("outcomes", [])
    oc_prices = market.get("outcomePrices", [])
    # Gamma API returns these as JSON strings, not lists
    if isinstance(oc_names, str):
        try:
            oc_names = _json.loads(oc_names)
        except (ValueError, TypeError):
            oc_names = []
    if isinstance(oc_prices, str):
        try:
            oc_prices = _json.loads(oc_prices)
        except (ValueError, TypeError):
            oc_prices = []
    if oc_names and oc_prices and len(oc_names) == len(oc_prices):
        result = {}
        for name, price_str in zip(oc_names, oc_prices):
            try:
                result[name] = float(price_str)
            except (ValueError, TypeError):
                pass
        if "Up" in result and "Down" in result:
            return result
    return None


# ============================================================
# LOCK FILES: Prevent duplicate bets
# ============================================================

def check_lock(asset, window_epoch):
    """Check if we already bet on this window."""
    os.makedirs(LOCK_DIR, exist_ok=True)
    lock_file = os.path.join(LOCK_DIR, f".lock_{asset}_15m_{window_epoch}")
    return os.path.exists(lock_file)


def set_lock(asset, window_epoch):
    """Mark this window as bet on."""
    os.makedirs(LOCK_DIR, exist_ok=True)
    lock_file = os.path.join(LOCK_DIR, f".lock_{asset}_15m_{window_epoch}")
    with open(lock_file, "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())


def cleanup_locks(max_age_hours=2):
    """Remove lock files older than max_age_hours."""
    if not os.path.exists(LOCK_DIR):
        return
    cutoff = time.time() - (max_age_hours * 3600)
    for fn in os.listdir(LOCK_DIR):
        fp = os.path.join(LOCK_DIR, fn)
        if os.path.getmtime(fp) < cutoff:
            os.remove(fp)


# ============================================================
# BANKR EXECUTION
# ============================================================

def place_bet_via_bankr(direction, bet_size, market_title, bankr):
    """Place a bet on Polymarket via Bankr API (uses bankr.place_bet)."""
    side = "Up" if direction == "BET_UP" else "Down"

    print(f"  [BANKER] Placing ${bet_size:.2f} on {side}...")
    try:
        result = bankr.place_bet(
            market_title=market_title,
            side=side,
            amount=bet_size,
        )
        if result.get("success"):
            trade_id = result.get("trade_id", "")
            print(f"  [BANKER] Submitted. Trade ID: {trade_id or 'pending'}")
            # Verify bet execution
            try:
                verify_result = bankr.verify_bet_execution(market_title, side)
                if verify_result.get("verified"):
                    print(f"  [VERIFIED] Scalper bet confirmed in Bankr positions")
                else:
                    print(f"  [WARN] Scalper bet unverified: {verify_result.get('reason', 'unknown')} -- logging anyway")
            except Exception as ve:
                print(f"  [WARN] Scalper verification failed: {ve} -- logging anyway")
            return {"success": True, "trade_id": trade_id, "response": result.get("response", "")}
        else:
            print(f"  [BANKER] Error: {result.get('error', 'unknown')}")
            return {"success": False, "error": result.get("error", "unknown")}
    except Exception as e:
        print(f"  [BANKER] Exception: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# DB LOGGING
# ============================================================

def log_updown_bet(db_path, asset, direction, score, bet_size, market_title, trade_id=None, balance_before=None, real_odds=None,
                     signal_data=None):
    """Log an up/down bet via The Archivist with full decision snapshot."""
    try:
        from data_intake import DataIntake
        from db_writer import DBWriter
        _intake = DataIntake(db_path)
        archivist = DBWriter(db_path)

        side = "yes" if direction == "BET_UP" else "no"
        confidence = abs(int(score * 20))
        edge = abs(score) / 10
        market_id = f"updown-{asset}-15m-{int(time.time())}"

        # Build decision snapshot from signal data
        sig = signal_data or {}
        decision_snapshot = {
            "raw_data": {
                "asset": asset,
                "current_price": sig.get("price"),
                "rsi": sig.get("rsi"),
                "volatility": sig.get("volatility"),
                "candle_direction_ratio": sig.get("candle_ratio"),
                "volume_trend": sig.get("volume_trend"),
                "hourly_trend": sig.get("hourly_trend"),
                "market_odds": real_odds,
            },
            "modifiers": {
                "ma_alignment": sig.get("ma_alignment"),
                "momentum_indicators": sig.get("indicators", []),
            },
            "decision": {
                "score": score,
                "confidence": confidence,
                "direction": direction,
                "edge": edge,
                "side": side,
            },
            "strategy": {
                "bet_type": "UPDOWN",
                "asset": asset.upper(),
                "window_15m": True,
            },
        }

        bet_id = _intake.validate_and_write_bet(
            market_id=market_id,
            market_title=market_title,
            category="crypto",
            side=side,
            amount=bet_size,
            odds=real_odds if real_odds else 0.50,
            confidence_score=confidence,
            edge=edge,
            reasoning=f"UpDown {asset.upper()} score={score:.1f} ({direction})",
            balance_before=balance_before,
            cycle_type="updown",
            bet_type="UPDOWN",
            format_type="updown",
            decision_snapshot=decision_snapshot,
        )

        if bet_id and trade_id:
            archivist.set_trade_id(bet_id, trade_id)

        return bet_id
    except Exception as e:
        print(f"  [SCALPER] Log error: {e}")
        return None


# ============================================================
# SESSION TRACKER
# ============================================================

class SessionTracker:
    """Track up/down trader sessions with live heartbeat.

    Creates updown_sessions table on first run. Queries bets table
    each cycle to detect resolved bets and compute effective budget
    (budget - wagered + winnings returned). Zero API cost.
    """

    def __init__(self, db_path):
        self.db_path = db_path
        self.session_id = None
        self.started_at = None
        self.budget = 0.0
        self.total_wagered = 0.0
        self.total_returned = 0.0
        self._ensure_table()

    def _ensure_table(self):
        from db_writer import DBWriter
        _arch = DBWriter(self.db_path)
        _arch.execute("""
            CREATE TABLE IF NOT EXISTS updown_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT DEFAULT 'running',
                runtime_hours REAL,
                assets TEXT,
                budget REAL,
                bet_size REAL,
                min_score REAL,
                dry_run INTEGER DEFAULT 0,
                cycles_run INTEGER DEFAULT 0,
                bets_placed INTEGER DEFAULT 0,
                bets_won INTEGER DEFAULT 0,
                bets_lost INTEGER DEFAULT 0,
                bets_pending INTEGER DEFAULT 0,
                total_wagered REAL DEFAULT 0.0,
                total_returned REAL DEFAULT 0.0,
                net_pnl REAL DEFAULT 0.0,
                roi REAL DEFAULT 0.0
            )
        """, commit=True)

    def start(self, hours, assets, budget, bet_size, min_score, dry_run):
        """Start a new session. Returns session_id."""
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.budget = budget or 0.0
        self.total_wagered = 0.0
        self.total_returned = 0.0
        from db_writer import DBWriter
        _arch = DBWriter(self.db_path)
        _arch.execute("""
            INSERT INTO updown_sessions (started_at, status, runtime_hours, assets,
                budget, bet_size, min_score, dry_run)
            VALUES (?, 'running', ?, ?, ?, ?, ?, ?)
        """, (self.started_at, hours, assets, budget, bet_size, min_score,
              1 if dry_run else 0), commit=True)
        row = _arch.fetchone("SELECT last_insert_rowid()")
        self.session_id = row[0] if row else None
        print(f"[SCALPER] Started session #{self.session_id}")
        return self.session_id

    def log_cycle(self, bets_placed, amount_spent):
        """Update session after a cycle completes."""
        self.total_wagered += amount_spent
        from db_writer import DBWriter
        _arch = DBWriter(self.db_path)
        _arch.execute("""
            UPDATE updown_sessions
            SET cycles_run = cycles_run + 1,
                bets_placed = bets_placed + ?,
                total_wagered = ?
            WHERE id = ?
        """, (bets_placed, self.total_wagered, self.session_id), commit=True)

    def heartbeat(self):
        """Check resolved bets, return effective budget remaining.

        Queries bets table for updown bets placed during this session.
        Calculates real P&L from resolved bets and returns the effective
        budget: original budget - wagered + winnings returned.
        """
        if not self.started_at or not self.budget:
            return None

        from db_writer import DBWriter
        _arch = DBWriter(self.db_path)

        # Check resolved bets from this session
        resolved = _arch.fetchall("""
            SELECT won, profit, amount FROM bets
            WHERE cycle_type = 'updown' AND status = 'resolved'
            AND timestamp >= ?
        """, (self.started_at,))

        # Check pending count
        _prow = _arch.fetchone("""
            SELECT COUNT(*) FROM bets
            WHERE cycle_type = 'updown' AND status = 'pending'
            AND timestamp >= ?
        """, (self.started_at,))
        pending = _prow[0] if _prow else 0

        wins = sum(1 for r in resolved if r[0])
        losses = sum(1 for r in resolved if not r[0])
        self.total_returned = sum(
            (r[2] + (r[1] or 0)) for r in resolved if r[0]
        )

        effective = self.budget - self.total_wagered + self.total_returned

        if resolved:
            print(f"  [HEARTBEAT] {wins}W/{losses}L/{pending}P | "
                  f"Wagered: ${self.total_wagered:.2f} | "
                  f"Returned: ${self.total_returned:.2f} | "
                  f"Effective budget: ${effective:.2f}")
        elif pending > 0:
            print(f"  [HEARTBEAT] {pending} pending | "
                  f"Wagered: ${self.total_wagered:.2f} | "
                  f"Effective budget: ${effective:.2f}")

        return effective

    def close(self, status='completed'):
        """Close session, update DB, print summary card."""
        if not self.session_id:
            return

        from db_writer import DBWriter
        _arch = DBWriter(self.db_path)
        rows = _arch.fetchall("""
            SELECT won, profit, amount, status FROM bets
            WHERE cycle_type = 'updown' AND timestamp >= ?
        """, (self.started_at,))

        # Also get cycles_run from session record
        session_row = _arch.fetchone(
            "SELECT cycles_run, assets, runtime_hours FROM updown_sessions WHERE id = ?",
            (self.session_id,)
        )

        wins = sum(1 for r in rows if r[3] == 'resolved' and r[0])
        losses = sum(1 for r in rows if r[3] == 'resolved' and not r[0])
        pending = sum(1 for r in rows if r[3] == 'pending')
        self.total_returned = sum(
            (r[2] + (r[1] or 0)) for r in rows
            if r[3] == 'resolved' and r[0]
        )
        net_pnl = self.total_returned - self.total_wagered
        roi = (net_pnl / self.total_wagered * 100) if self.total_wagered > 0 else 0.0

        ended_at = datetime.now(timezone.utc).isoformat()
        _arch.execute("""
            UPDATE updown_sessions
            SET ended_at = ?, status = ?, bets_won = ?, bets_lost = ?,
                bets_pending = ?, total_returned = ?, net_pnl = ?, roi = ?
            WHERE id = ?
        """, (ended_at, status, wins, losses, pending,
              self.total_returned, net_pnl, roi, self.session_id), commit=True)

        # Summary card
        total_bets = wins + losses + pending
        cycles = session_row[0] if session_row else '?'
        assets_str = (session_row[1] or '?').upper() if session_row else '?'
        hours = session_row[2] if session_row else '?'

        print(f"\n{'='*60}")
        print(f"SESSION #{self.session_id} COMPLETE")
        print(f"{'='*60}")
        print(f"Runtime: {hours}h | Cycles: {cycles} | Assets: {assets_str}")
        if self.budget:
            print(f"Budget: ${self.budget:.2f} | Wagered: ${self.total_wagered:.2f} | "
                  f"Status: {status}")
        print(f"\nResults: {total_bets} bets | {wins}W / {losses}L / {pending} pending")
        print(f"Wagered: ${self.total_wagered:.2f} | Returned: ${self.total_returned:.2f} | "
              f"Net P&L: ${net_pnl:+.2f}")
        print(f"ROI: {roi:+.1f}%")
        print(f"{'='*60}")

    @staticmethod
    def print_history(db_path, limit=10):
        """Print past session history from DB."""
        try:
            from db_writer import DBWriter
            _arch = DBWriter(db_path)
            rows = _arch.fetchall("""
                SELECT id, started_at, ended_at, status, runtime_hours, assets,
                       budget, bet_size, min_score, dry_run, cycles_run,
                       bets_placed, bets_won, bets_lost, bets_pending,
                       total_wagered, total_returned, net_pnl, roi
                FROM updown_sessions ORDER BY id DESC LIMIT ?
            """, (limit,))
        except Exception:
            print("No session history found (table may not exist yet).")
            return

        if not rows:
            print("No sessions found.")
            return

        print(f"\nUP/DOWN TRADER -- SESSION HISTORY")
        print(f"{'='*95}")
        print(f"{'#':>3} | {'Date':10} | {'Assets':7} | {'Budget':>7} | "
              f"{'Bets':>4} | {'W/L/P':7} | {'Net P&L':>8} | {'ROI':>7} | Status")
        print(f"{'-'*95}")
        for r in reversed(rows):
            date = r[1][:10] if r[1] else '?'
            assets = (r[5] or '?').upper()[:7]
            budget = f"${r[6]:.2f}" if r[6] else '  n/a'
            bets = r[11] or 0
            w, l, p = r[12] or 0, r[13] or 0, r[14] or 0
            wlp = f"{w}/{l}/{p}"
            pnl = f"${r[17]:+.2f}" if r[17] else ' $0.00'
            roi = f"{r[18]:+.1f}%" if r[18] else '  0.0%'
            status = r[3] or '?'
            print(f"{r[0]:>3} | {date:10} | {assets:7} | {budget:>7} | "
                  f"{bets:>4} | {wlp:7} | {pnl:>8} | {roi:>7} | {status}")
        print(f"{'='*95}")


# ============================================================
# MAIN CYCLE
# ============================================================

def is_blackout_hour():
    """Check if current ET hour is in blackout window or weekend."""
    if BLACKOUT_WEEKENDS and is_weekend():
        return True, f"weekend ({current_day_name()})"
    h = current_hour_et()
    return h in BLACKOUT_HOURS_ET, h


def check_drawdown(db_path):
    """Check today's net P&L. Returns (is_over_limit, daily_pnl)."""
    try:
        from db_writer import DBWriter
        _arch = DBWriter(db_path)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        rows = _arch.fetchall("""
            SELECT won, profit, amount FROM bets
            WHERE cycle_type = 'updown' AND status = 'resolved'
            AND timestamp LIKE ?
        """, (f"{today}%",))
        if not rows:
            return False, 0.0
        pnl = sum(r[1] or 0 for r in rows)
        return pnl <= -DRAWDOWN_LIMIT, pnl
    except Exception:
        return False, 0.0


def check_cooldown(db_path):
    """Check if last 2 updown bets were losses. Returns (in_cooldown, minutes_left)."""
    try:
        from db_writer import DBWriter
        _arch = DBWriter(db_path)
        rows = _arch.fetchall("""
            SELECT won, resolved_at FROM bets
            WHERE cycle_type = 'updown' AND status = 'resolved'
            ORDER BY id DESC LIMIT 2
        """)
        if len(rows) < 2:
            return False, 0
        # Both losses?
        if rows[0][0] == 0 and rows[1][0] == 0:
            # Check if cooldown has expired
            last_resolved = rows[0][1]
            if last_resolved:
                try:
                    resolved_dt = datetime.fromisoformat(last_resolved.replace("Z", "+00:00"))
                    elapsed = (datetime.now(timezone.utc) - resolved_dt).total_seconds() / 60
                    if elapsed < COOLDOWN_MINUTES:
                        return True, int(COOLDOWN_MINUTES - elapsed)
                except Exception:
                    pass
        return False, 0
    except Exception:
        return False, 0


def qualify_down_bet(signal, candles):
    """Strict DOWN qualification. Returns (qualified, reason)."""
    # 1. Score must be <= DOWN_MIN_SCORE
    if signal["score"] > DOWN_MIN_SCORE:
        return False, f"DOWN score {signal['score']:.1f} > {DOWN_MIN_SCORE} (need stronger bearish)"
    # 2. Hourly trend must be down > 0.5%
    hourly = calc_hourly_trend(candles)
    if hourly > -0.5:
        return False, f"Hourly trend {hourly:+.2f}% (need < -0.5% for DOWN)"
    # 3. Volatility must be > threshold (ties unlikely)
    if signal["volatility"] < DOWN_MIN_VOL:
        return False, f"Vol {signal['volatility']:.4f}% < {DOWN_MIN_VOL}% (low vol = ties = UP wins)"
    # 4. RSI must be 30-45 (bearish but not oversold bounce)
    if signal["rsi"] < DOWN_RSI_MIN or signal["rsi"] > DOWN_RSI_MAX:
        return False, f"RSI {signal['rsi']:.1f} outside {DOWN_RSI_MIN}-{DOWN_RSI_MAX} range for DOWN"
    return True, "DOWN qualified (all 4 checks passed)"


def apply_hourly_trend_filter(signal, candles):
    """Kill UP signals into falling market, boost UP into rising market.
    Returns (modified_signal, filter_reason or None)."""
    hourly = calc_hourly_trend(candles)
    score = signal["score"]
    # Hourly DOWN > 0.5% but score says UP → kill
    if hourly < -0.5 and score > 0:
        signal["decision"] = "NO_BET"
        return signal, f"Hourly trend {hourly:+.2f}% kills UP signal (falling market)"
    # Hourly UP > 0.5% and score says UP → boost
    if hourly > 0.5 and score > 0:
        signal["score"] = round(score + 0.5, 2)
        return signal, f"Hourly trend {hourly:+.2f}% confirms UP (+0.5 boost)"
    return signal, None


def run_cycle(assets, bet_size, bankr, dry_run=False, min_score=MIN_SCORE_DEFAULT,
              budget_remaining=None):
    """Run one analysis cycle across all assets.
    Returns (bets_placed, amount_spent)."""
    bets_placed = 0
    amount_spent = 0.0
    window_epoch = get_current_window_start()

    for asset in assets:
        # Budget check — use remaining budget as bet size if less than default
        actual_bet = bet_size
        if budget_remaining is not None:
            left = budget_remaining - amount_spent
            if left < 1.0:
                print(f"\n  [{asset.upper()}] SKIP - budget exhausted (${left:.2f} left)")
                continue
            if left < bet_size:
                actual_bet = round(left, 2)
                print(f"\n  [{asset.upper()}] Budget low - reducing bet ${bet_size:.2f} -> ${actual_bet:.2f}")

        print(f"\n  [{asset.upper()}] Analyzing...")

        # Lock check
        if check_lock(asset, window_epoch):
            print(f"  [{asset.upper()}] Already bet this window, skipping")
            continue

        # Fetch candles
        candles = fetch_candles(asset, limit=60)
        if not candles or len(candles) < 20:
            print(f"  [{asset.upper()}] Insufficient candle data")
            continue

        # Analyze
        signal = analyze(candles, min_score=min_score)
        print(f"  [{asset.upper()}] ${signal['price']:.2f} | Score: {signal['score']:+.1f} | "
              f"RSI: {signal['rsi']} | {signal['dir5']} | Vol: {signal['volatility']}%")
        for r in signal["reasons"]:
            print(f"    - {r}")

        if signal["decision"] == "NO_BET":
            print(f"  [{asset.upper()}] NO_BET (score {signal['score']:.1f}, need >={min_score})")
            continue

        # --- v3.0 FILTERS ---

        # Filter 1: Score cap (momentum trap protection)
        if abs(signal["score"]) > MAX_SCORE:
            print(f"  [{asset.upper()}] SKIP - score {signal['score']:.1f} > max {MAX_SCORE} (momentum trap)")
            continue

        # Filter 2: Hourly trend filter
        signal, trend_reason = apply_hourly_trend_filter(signal, candles)
        if trend_reason:
            print(f"    - {trend_reason}")
        if signal["decision"] == "NO_BET":
            continue

        # Direction decided by score alone: positive = UP, negative = DOWN
        # Score threshold (>=5.0) applies equally to both directions
        if signal["decision"] == "BET_DOWN":
            print(f"  [{asset.upper()}] DOWN signal (score {signal['score']:.1f})")

        # Tiered bet sizing: $2/$3/$5 by score
        score_abs = abs(signal['score'])
        if score_abs >= 6.0:
            actual_bet = 5.0
        elif score_abs >= 5.5:
            actual_bet = 3.0
        else:
            actual_bet = 2.0

        # Cap to budget remaining
        if budget_remaining is not None:
            left = budget_remaining - amount_spent
            if left < 1.0:
                print(f"  [{asset.upper()}] SKIP - budget exhausted (${left:.2f} left)")
                continue
            actual_bet = min(actual_bet, left)

        print(f"  [{asset.upper()}] Bet: ${actual_bet:.2f} (score {abs(signal['score']):.1f})")

        # Find market
        market = find_market(asset)
        if not market:
            print(f"  [{asset.upper()}] No active market found on Gamma API")
            continue

        direction = signal["decision"]
        side = "UP" if direction == "BET_UP" else "DOWN"
        side_label = "Up" if direction == "BET_UP" else "Down"

        # Read real token prices from market data
        token_prices = get_token_prices(market)
        our_price = 0.50  # fallback
        if token_prices:
            our_price = token_prices.get(side_label, 0.50)
            other_price = token_prices.get("Down" if side_label == "Up" else "Up", 0.50)
            print(f"  [{asset.upper()}] Prices: Up={token_prices.get('Up', '?')} | Down={token_prices.get('Down', '?')} | Our side ({side_label}): {our_price:.2f}")
        else:
            print(f"  [{asset.upper()}] Could not read token prices, using 0.50 default")

        # Price filter: only bet when our side is cheap (mispriced)
        # 35-45c = good value, >45c = too expensive, <35c = too risky
        # KEY: If our signal side is too expensive, the OTHER side is cheap.
        # Flip to the cheap side — the market is giving us value on the contrarian bet.
        if our_price > UPDOWN_MAX_PRICE:
            # Our signal side is expensive — check if opposite side is in value zone
            opp_label = "Down" if side_label == "Up" else "Up"
            opp_price = token_prices.get(opp_label, 0.50) if token_prices else 0.50
            if UPDOWN_MIN_PRICE <= opp_price <= UPDOWN_MAX_PRICE:
                print(f"  [{asset.upper()}] FLIP - {side_label} too expensive ({our_price:.2f}), {opp_label} is cheap ({opp_price:.2f})")
                # Flip direction
                side_label = opp_label
                side = side_label.upper()
                direction = "BET_UP" if side == "UP" else "BET_DOWN"
                our_price = opp_price
            else:
                print(f"  [{asset.upper()}] SKIP - {side_label} at {our_price:.2f}c too expensive, {opp_label} at {opp_price:.2f}c not in range")
                continue
        if our_price < UPDOWN_MIN_PRICE:
            print(f"  [{asset.upper()}] SKIP - our side ({side_label}) at {our_price:.2f}c, too cheap/risky (need >={UPDOWN_MIN_PRICE})")
            continue

        print(f"  [{asset.upper()}] {side} signal ({signal['confidence']}) @ {our_price:.2f}c | Market: {market['title'][:60]}")

        if dry_run:
            print(f"  [{asset.upper()}] [DRY RUN] Would bet ${actual_bet:.2f} on {side} @ {our_price:.2f}")
            set_lock(asset, window_epoch)
            amount_spent += actual_bet
            continue

        # V3.1: Risk Manager assessment
        if risk_manager:
            bet_dict = {
                'category': 'updown',
                'side': direction,
                'amount': actual_bet,
                'market_id': market.get('id', market.get('condition_id', '')),
                'market_title': market.get('title', ''),
            }
            risk_ok, risk_level, risk_warnings = risk_manager.assess(bet_dict)
            if risk_warnings:
                for w in risk_warnings:
                    print(f"  [RISK MANAGER] {w}")
            if not risk_ok:
                print(f"  [RISK MANAGER] BLOCKED: {risk_warnings}")
                continue

        # V3.1: Compliance pre-flight
        if compliance:
            comp_dict = {
                'category': 'updown',
                'market_id': market.get('id', market.get('condition_id', '')),
                'market_title': market.get('title', ''),
                'amount': actual_bet,
            }
            approved, reason, comp_warnings = compliance.pre_flight(comp_dict)
            if comp_warnings:
                for w in comp_warnings:
                    print(f"  [COMPLIANCE] {w}")
            if not approved:
                print(f"  [COMPLIANCE] REJECTED: {reason}")
                continue

        # Place bet via Bankr
        result = place_bet_via_bankr(direction, actual_bet, market["title"], bankr)
        if result.get("success"):
            set_lock(asset, window_epoch)
            # Get last known balance for tracking
            _bal = None
            try:
                from db_writer import DBWriter
                _arch = DBWriter(DB_PATH)
                _row = _arch.fetchone(
                    "SELECT available_balance FROM portfolio_snapshots ORDER BY id DESC LIMIT 1"
                )
                if _row:
                    _bal = _row[0]
            except Exception:
                pass
            log_updown_bet(DB_PATH, asset, direction, signal["score"],
                          actual_bet, market["title"], result.get("trade_id"),
                          balance_before=_bal, real_odds=our_price,
                          signal_data={
                              "price": signal.get("price"),
                              "rsi": signal.get("rsi"),
                              "volatility": signal.get("volatility"),
                              "candle_ratio": signal.get("candle_ratio"),
                              "volume_trend": signal.get("volume_trend"),
                              "hourly_trend": signal.get("hourly_trend"),
                              "ma_alignment": signal.get("ma_alignment"),
                              "indicators": signal.get("indicators", []),
                              "confidence": signal.get("confidence"),
                          })
            bets_placed += 1
            amount_spent += actual_bet
            print(f"  [{asset.upper()}] BET PLACED: ${actual_bet:.2f} {side} @ {our_price:.2f}")
        else:
            print(f"  [{asset.upper()}] Bet failed: {result.get('error')}")

    return bets_placed, amount_spent



# ============================================================
# MANAGER INTEGRATION — called from hedge_fund_active.py
# ============================================================

_scalper_state = {
    "daily_bets": 0,
    "daily_pnl": 0.0,
    "consecutive_losses": 0,
    "last_loss_time": None,
    "last_reset_date": None,
    "bankr_instance": None,
}

def run_scalper_cycle(bankr=None, wallet=None, dry_run=False, risk_manager=None, compliance=None, intel_package=None):
    """Run one Scalper cycle — called by Manager every 2 min.
    Checks if we're in a 15-min window and conditions are met."""
    import pytz
    from datetime import datetime, timezone

    state = _scalper_state

    # Daily reset
    today = datetime.now(timezone.utc).date()
    if state["last_reset_date"] != today:
        state["daily_bets"] = 0
        state["daily_pnl"] = 0.0
        state["consecutive_losses"] = 0
        state["last_loss_time"] = None
        state["last_reset_date"] = today
        print(f"[SCALPER] Daily reset")

    if not ENABLE_UPDOWN_MODULE:
        return

    # Max daily bets check
    # DB-based daily bet count (survives restarts)
    try:
        from db_writer import DBWriter
        _arch = DBWriter(DB_PATH)
        _today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        _row = _arch.fetchone("SELECT COUNT(*) FROM bets WHERE cycle_type='updown' AND timestamp LIKE ?", (f"{_today}%",))
        _count = _row[0] if _row else 0
    except Exception:
        _count = 0
    if _count >= UPDOWN_MAX_DAILY:
        return

    # Concurrent position check
    try:
        _open = _arch.fetchone("SELECT COUNT(*) FROM bets WHERE cycle_type='updown' AND status != 'resolved'")
        _open_count = _open[0] if _open else 0
        if _open_count >= UPDOWN_MAX_CONCURRENT:
            print(f"[SCALPER] Max concurrent positions reached ({_open_count}/{UPDOWN_MAX_CONCURRENT})")
            return
    except Exception:
        pass

    # Drawdown check
    over_limit, daily_pnl = check_drawdown(DB_PATH)
    if over_limit:
        print(f"[SCALPER] Daily drawdown limit hit (${daily_pnl:+.2f})")
        return

    # Cooldown check (1 loss = 45 min pause)
    if state["consecutive_losses"] >= UPDOWN_COOLDOWN_AFTER_LOSSES and state["last_loss_time"]:
        elapsed = (datetime.now(timezone.utc) - state["last_loss_time"]).total_seconds() / 60
        if elapsed < UPDOWN_COOLDOWN_MINUTES:
            return  # Silent — don't spam logs
        else:
            state["consecutive_losses"] = 0
            state["last_loss_time"] = None

    # Blackout check (hours + weekends)
    in_blackout, reason = is_blackout_hour()
    if in_blackout:
        return

    # Check if we're near a 15-min window (:08, :23, :38, :53)
    now = datetime.now(timezone.utc)
    minute = now.minute
    # Only fire within 2 minutes of window start
    window_minutes = [8, 23, 38, 53]
    in_window = any(abs(minute - wm) <= 2 or abs(minute - wm - 60) <= 2 for wm in window_minutes)
    if not in_window:
        return

    print(f"\n[SCALPER] === Up/Down Cycle ({now.strftime('%H:%M')} UTC) ===")
    print(f"[SCALPER] Bets today: {state['daily_bets']}/{UPDOWN_MAX_DAILY} | P&L: ${state['daily_pnl']:+.2f}")

    # Store bankr for reuse
    if bankr:
        state["bankr_instance"] = bankr
    _bankr = state["bankr_instance"]
    if not _bankr:
        print("[SCALPER] No Bankr instance — skipping")
        return

    # Run the cycle
    assets = UPDOWN_ASSETS
    bets, spent = run_cycle(
        assets=assets,
        bet_size=UPDOWN_BET_SIZE,
        bankr=_bankr,
        dry_run=dry_run,
        min_score=UPDOWN_MIN_SCORE,
        budget_remaining=UPDOWN_BET_SIZE * (UPDOWN_MAX_DAILY - state["daily_bets"])
    )

    state["daily_bets"] += bets
    if bets > 0:
        print(f"[SCALPER] Placed {bets} bet(s), ${spent:.2f} spent")


def update_scalper_result(won, profit):
    """Called by resolver when an updown bet resolves."""
    state = _scalper_state
    state["daily_pnl"] += profit
    if not won:
        state["consecutive_losses"] += 1
        state["last_loss_time"] = datetime.now(timezone.utc)
    else:
        state["consecutive_losses"] = 0
        state["last_loss_time"] = None


def wait_for_next_window():
    """Sleep until the next :08, :23, :38, or :53 mark."""
    now = datetime.now(timezone.utc)
    minute = now.minute
    targets = [8, 23, 38, 53]

    # Find next target minute
    next_target = None
    for t in targets:
        if minute < t:
            next_target = t
            break
    if next_target is None:
        # Next hour's :08
        next_target = targets[0]
        next_time = now.replace(minute=next_target, second=0, microsecond=0) + timedelta(hours=1)
    else:
        next_time = now.replace(minute=next_target, second=0, microsecond=0)

    wait_seconds = (next_time - now).total_seconds()
    if wait_seconds < 0:
        wait_seconds = 0
    return wait_seconds, next_time


# ============================================================
# ENTRY POINT
# ============================================================

def main():
    if not ENABLE_UPDOWN_MODULE:
        print("[SCALPER] Module disabled via ENABLE_UPDOWN_MODULE config")
        return

    parser = argparse.ArgumentParser(description="Up/Down Momentum Trader for Polymarket")
    parser.add_argument("--hours", type=float, default=DEFAULT_HOURS,
                       help=f"How many hours to run (default: {DEFAULT_HOURS})")
    parser.add_argument("--budget", type=float, default=None,
                       help="Total budget (auto-calculates bet size)")
    parser.add_argument("--bet-size", type=float, default=DEFAULT_BET_SIZE,
                       help=f"Fixed bet size in USD (default: ${DEFAULT_BET_SIZE})")
    parser.add_argument("--assets", type=str, default="btc",
                       help="Comma-separated assets: btc,eth,sol,xrp (default: btc)")
    parser.add_argument("--min-score", type=float, default=MIN_SCORE_DEFAULT,
                       help=f"Minimum score to trigger bet (default: {MIN_SCORE_DEFAULT})")
    parser.add_argument("--dry-run", action="store_true",
                       help="Analyze only, don't place bets")
    parser.add_argument("--history", action="store_true",
                       help="Show past session history and exit")
    args = parser.parse_args()

    # --history: print and exit
    if args.history:
        SessionTracker.print_history(DB_PATH)
        return

    assets = [a.strip().lower() for a in args.assets.split(",")]
    hours = args.hours
    dry_run = args.dry_run
    min_score = args.min_score

    # Budget is hard spending cap -- max possible loss
    budget = args.budget
    if budget:
        windows_per_hour = 4
        total_windows = windows_per_hour * hours * len(assets)
        selectivity = 0.3  # ~30% of windows trigger bets
        expected_bets = max(1, total_windows * selectivity)
        bet_size = budget / expected_bets
        bet_size = max(1.0, min(bet_size, 10.0))  # Clamp $1-$10
    else:
        bet_size = args.bet_size

    # Load .env for BANKR_API_KEY
    try:
        from dotenv import load_dotenv
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
        load_dotenv(env_path)
    except ImportError:
        pass  # dotenv not installed, rely on environment

    # Init Bankr
    bankr = None
    if not dry_run:
        try:
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from bankr import BankrExecutor
            bankr = BankrExecutor(dry_run=False)
            print("[BANKER] Connected")
        except Exception as e:
            print(f"[!] Could not init Bankr: {e}")
            print("[!] Falling back to dry-run mode")
            dry_run = True

    # Bet resolver (resolves updown bets via Bankr every 10 min)
    resolver = None
    if bankr:
        try:
            from bankr import BetResolver
            resolver = BetResolver(db_path=DB_PATH, bankr=bankr)
            print("[SETTLEMENT CLERK] Bet resolver initialized")
        except Exception as e:
            print(f"[!] Could not init BetResolver: {e}")

    # Session tracker
    tracker = SessionTracker(DB_PATH)
    assets_str = ",".join(assets)
    tracker.start(hours, assets_str, budget, bet_size, min_score, dry_run)

    # Banner
    end_time = datetime.now(timezone.utc) + timedelta(hours=hours)
    print(f"\n{'='*60}")
    print(f"UP/DOWN MOMENTUM TRADER v3.0 (fathom filters)")
    print(f"{'='*60}")
    print(f"Assets: {', '.join(a.upper() for a in assets)}")
    print(f"Runtime: {hours}h (auto-shutdown at {end_time.strftime('%H:%M UTC')})")
    print(f"Bet size: ${bet_size:.2f}")
    if budget:
        print(f"Budget: ${budget:.2f} (hard cap -- shuts off when spent)")
    print(f"Min score: {args.min_score}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"Cycle: :08, :23, :38, :53 each hour")
    print(f"Blackout: {BLACKOUT_HOURS_ET} ET | Max score: {MAX_SCORE} | Drawdown: ${DRAWDOWN_LIMIT}")
    print(f"DOWN qual: score<={DOWN_MIN_SCORE}, vol>{DOWN_MIN_VOL}%, RSI {DOWN_RSI_MIN}-{DOWN_RSI_MAX}")
    print(f"{'='*60}\n")

    total_bets = 0
    total_spent = 0.0
    cycle_count = 0
    budget_left = budget if budget else None

    shutdown_status = 'completed'
    try:
        while datetime.now(timezone.utc) < end_time:
            # Budget exhaustion check (uses effective budget from heartbeat)
            if budget and budget_left is not None and budget_left < 1.0:
                print(f"\n[SCALPER] Budget exhausted (${budget_left:.2f} effective) -- shutting down")
                shutdown_status = 'budget_exhausted'
                break

            # v3.0: Blackout hour check
            in_blackout, et_hour = is_blackout_hour()
            if in_blackout:
                print(f"\n[SCALPER] Blackout: {et_hour} — skipping cycle")
                time.sleep(60)
                continue

            # v3.0: Drawdown protection (skip cycle, don't kill timer)
            over_limit, daily_pnl = check_drawdown(DB_PATH)
            if over_limit:
                print(f"\n[SCALPER] Daily P&L ${daily_pnl:+.2f} exceeds -${DRAWDOWN_LIMIT:.0f} limit — skipping cycle")
                time.sleep(60)
                continue

            # v3.0: Cooldown after consecutive losses
            in_cooldown, mins_left = check_cooldown(DB_PATH)
            if in_cooldown:
                print(f"\n[SCALPER] 2 consecutive losses — pausing {mins_left}min (of {COOLDOWN_MINUTES}min)")
                time.sleep(min(mins_left * 60, 300))
                continue

            # Wait for next window
            wait_secs, next_time = wait_for_next_window()

            # Check if next window is past our end time
            if datetime.now(timezone.utc) + timedelta(seconds=wait_secs) > end_time:
                print(f"\n[SCALPER] Next window at {next_time.strftime('%H:%M')} is past shutdown time")
                break

            if wait_secs > 10:
                budget_str = f" | Budget: ${budget_left:.2f} left" if budget_left is not None else ""
                print(f"[WAIT] Next window at {next_time.strftime('%H:%M UTC')} "
                      f"({int(wait_secs)}s) | Shutdown at {end_time.strftime('%H:%M UTC')}{budget_str}")
                time.sleep(wait_secs)

            # Run cycle
            cycle_count += 1
            now = datetime.now(timezone.utc)
            remaining = (end_time - now).total_seconds() / 60
            print(f"\n{'='*60}")
            print(f"CYCLE #{cycle_count} - {now.strftime('%H:%M:%S UTC')} "
                  f"({remaining:.0f}min remaining)")
            if budget_left is not None:
                print(f"Budget: ${total_spent:.2f}/{budget:.2f} spent | ${budget_left:.2f} remaining")
            print(f"{'='*60}")

            cleanup_locks()
            bets, spent = run_cycle(assets, bet_size, bankr, dry_run,
                                    min_score=min_score, budget_remaining=budget_left)
            total_bets += bets
            total_spent += spent

            # Log cycle + resolve bets + heartbeat
            tracker.log_cycle(bets, spent)
            if resolver:
                resolver.run()
            effective = tracker.heartbeat()

            # Use effective budget (accounts for winnings returned)
            if effective is not None and budget:
                budget_left = effective
            else:
                budget_left = budget - total_spent if budget else None

            if bets > 0:
                print(f"\n[SCALPER] {bets} bet(s) placed (${spent:.2f}) | "
                      f"Total: {total_bets} bets, ${total_spent:.2f} spent")

            # Small sleep to avoid rapid re-fire
            time.sleep(30)

    except KeyboardInterrupt:
        print(f"\n[SCALPER] Manual shutdown")
        shutdown_status = 'stopped'

    # Close session with summary card
    tracker.close(shutdown_status)


if __name__ == "__main__":
    main()
