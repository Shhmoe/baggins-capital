"""
Active Hedge Fund Agent - Polymarket Crypto
Prediction market trader focused on crypto markets on Polymarket
Momentum-based edge evaluation using CoinGecko price data

DATA SOURCES:
- Polymarket Gamma API for crypto prediction markets
- CoinGecko for real-time crypto prices (free, no key)
- Bankr API for trade execution
"""

import os
import time
import random
import json
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False
    print("[!] anthropic SDK not installed - AI heartbeat disabled")

from updown_trader import run_scalper_cycle
from sports_analyst import SportsAnalyst
from hedge_fund_config import *
from performance_tracker import PerformanceTracker
from bet_notifier import BetNotifier
from bankr import BankrExecutor
from polymarket_crypto import CryptoMarketScanner
from wallet_coordinator import WalletCoordinator
from bankr import BetResolver

try:
    from weather_agent import WeatherAgent, PatternAnalyzer
    HAS_WEATHER = True
except ImportError:
    HAS_WEATHER = False
    print("[!] weather_agent not found - weather module disabled")

try:
    from avantis_signals import AvantisSignals
    from avantis_executor import AvantisExecutor
    HAS_AVANTIS = True
except ImportError:
    HAS_AVANTIS = False
    print("[!] avantis modules not found - Avantis disabled")

load_dotenv()


class ActiveHedgeFundAgent:
    """Crypto prediction market hedge fund agent on Polymarket."""

    def __init__(self, dry_run=True, starting_balance=100.0):
        self.dry_run = dry_run

        # Components
        self.tracker = PerformanceTracker()
        self.notifier = BetNotifier()

        # Data source -- crypto
        self.crypto_scanner = CryptoMarketScanner()

        # Trade executor
        self.bankr = BankrExecutor(dry_run=dry_run)

        # Wallet Coordinator -- single source of truth for balance + positions
        self.wallet = WalletCoordinator(self.bankr, self.tracker, starting_balance, dry_run)

        # Weather module
        self.weather_agent = None
        self.sports_analyst = None
        if HAS_WEATHER and getattr(__import__('hedge_fund_config'), 'ENABLE_WEATHER_MODULE', False):
            try:
                # Will be fully initialized after AI client is set up
                self._weather_pending = True
            except Exception as e:
                print(f"[!] Weather module init failed: {e}")
                self._weather_pending = False
        else:
            self._weather_pending = False

        # AI heartbeat client (Bankr LLM Gateway)
        self._ai_client = None
        if HAS_ANTHROPIC:
            bankr_key = os.getenv("BANKR_API_KEY", "").strip()
            anthropic_key = os.getenv("API_KEY", "").strip()
            if bankr_key:
                self._ai_client = anthropic.Anthropic(
                    api_key=bankr_key,
                    base_url="https://llm.bankr.bot"
                )
                self._ai_fallback_key = anthropic_key
                print("[MANAGER] Heartbeat using Bankr LLM Gateway")
            elif anthropic_key:
                self._ai_client = anthropic.Anthropic(api_key=anthropic_key)
                self._ai_fallback_key = None
                print("[MANAGER] Heartbeat using direct Anthropic")
        self.last_heartbeat = None
        self.last_pattern_analysis = None
        self.pattern_analyzer = PatternAnalyzer(
            db_path=self.tracker.db_path,
            notifier=self.notifier if hasattr(self, 'notifier') else None
        )

        # Initialize weather agent with AI client (after AI setup)
        if self._weather_pending:
            try:
                self.weather_agent = WeatherAgent(
                    tracker=self.tracker,
                    notifier=self.notifier,
                    bankr=self.bankr,
                    ai_client=self._ai_client,
                    ai_fallback_key=getattr(self, '_ai_fallback_key', None),
                )
                print("[WEATHER ANALYST] Weather module initialized")
            except Exception as e:
                print(f"[!] Weather agent init failed: {e}")
                self.weather_agent = None

        # Sports Analyst (Baggins' Buddy)
        try:
            from hedge_fund_config import ENABLE_SPORTS_MODULE
            if ENABLE_SPORTS_MODULE:
                self.sports_analyst = SportsAnalyst()
                print("[BUDDY] Sports analyst initialized")
        except Exception as e:
            print(f"[BUDDY] Init error: {e}")

        # State
        self.active_positions = []
        self._positions_locked = False
        self.daily_bet_count = 0
        self.crypto_daily_bet_count = 0
        self.last_improvement_time = datetime.now()
        self.last_results_sms = datetime.now()
        self.last_crypto_scan = datetime.now() - timedelta(seconds=CRYPTO_SCAN_INTERVAL)
        self.last_balance_sync = None
        self.last_claim_check = None
        self._last_weather_resolve = None
        self.consecutive_losses = 0
        self.paused_polymarket = PAUSED_POLYMARKET  # Branch-specific pause
        self.paused_avantis = PAUSED_AVANTIS        # Branch-specific pause
        self.avantis_signals = None  # Set properly after resolver init
        self.avantis_executor = None
        self.avantis_daily_bet_count = 0

        # Load active positions from DB via wallet coordinator
        crypto_positions = self.wallet.load_positions_from_db()
        self.active_positions = crypto_positions

        # Check position lock
        if len(self.active_positions) >= CRYPTO_MAX_CONCURRENT:
            self._positions_locked = True

        # Count today's bets
        try:
            conn = sqlite3.connect(self.tracker.db_path)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM bets WHERE DATE(timestamp) = DATE('now')")
            self.daily_bet_count = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM bets WHERE DATE(timestamp) = DATE('now') AND category = 'crypto'")
            self.crypto_daily_bet_count = c.fetchone()[0]
            conn.close()
        except Exception:
            pass

        # In live mode, sync balance with actual wallet
        if not self.dry_run:
            self.wallet.sync_with_wallet()
            self.last_balance_sync = datetime.now()

        deployed = self.wallet.total_deployed()

        print("\n" + "="*60)
        print("CRYPTO HEDGE FUND AGENT")
        print("="*60)
        print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
        print(f"[CFO] {self.wallet.status_summary()}")
        print(f"Active Crypto Positions: {len(self.active_positions)}")
        for pos in self.active_positions:
            print(f"  - {pos['market_title'][:55]} (${pos['amount']:.2f})")
        print(f"Today's Bets: {self.crypto_daily_bet_count}/{CRYPTO_MAX_DAILY_BETS} crypto")
        if self.weather_agent:
            print(f"Weather Positions: {len(self.weather_agent.active_weather_bets)}/{WEATHER_MAX_CONCURRENT}")
        if self.avantis_signals:
            avantis_open = self.tracker.get_open_avantis_positions()
            print(f"Avantis Positions: {len(avantis_open)}/{getattr(__import__('hedge_fund_config'), 'AVANTIS_MAX_CONCURRENT', 3)}")
        print(f"Scan Interval: {CRYPTO_SCAN_INTERVAL//60} minutes")
        print(f"Bet Size: ${CRYPTO_BET_MIN}-${CRYPTO_BET_MAX}")
        print(f"Max Concurrent: {CRYPTO_MAX_CONCURRENT}")
        print("="*60)

        self.notifier.notify_alert(
            f"Crypto hedge fund started\n"
            f"Mode: {'DRY RUN' if dry_run else 'LIVE'}\n"
            f"{self.wallet.status_summary()}\n"
            f"Positions: {len(self.active_positions)}\n"
            f"Today's bets: {self.daily_bet_count}/{CRYPTO_MAX_DAILY_BETS}"
        )

        # Heartbeat: startup snapshot
        self.tracker.save_portfolio_snapshot(
            available=self.wallet.available,
            deployed=deployed,
            positions=self.wallet.total_position_count(),
            pending=len(self.active_positions),
            daily_roi=0.0,
            consec_losses=self.consecutive_losses
        )

        # Unified bet resolver (Bankr claim + weather data)
        self.resolver = BetResolver(
            db_path=self.tracker.db_path,
            bankr=self.bankr,
            wallet=self.wallet,
            weather_agent=self.weather_agent,
            tracker=self.tracker,
            notifier=self.notifier
        )

        # Avantis leverage trading module (Base chain)
        self.last_avantis_scan = datetime.now() - timedelta(seconds=AVANTIS_SCAN_INTERVAL if hasattr(__import__('hedge_fund_config'), 'AVANTIS_SCAN_INTERVAL') else 900)
        if HAS_AVANTIS and getattr(__import__('hedge_fund_config'), 'ENABLE_AVANTIS', False):
            pause_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.pause_avantis')
            if not os.path.exists(pause_file) and not self.paused_avantis:
                try:
                    self.avantis_signals = AvantisSignals()
                    self.avantis_executor = AvantisExecutor(dry_run=dry_run)
                    # Count today's avantis bets
                    try:
                        conn = sqlite3.connect(self.tracker.db_path, timeout=30)
                        c = conn.cursor()
                        c.execute("SELECT COUNT(*) FROM avantis_positions WHERE DATE(timestamp) = DATE('now')")
                        self.avantis_daily_bet_count = c.fetchone()[0]
                        conn.close()
                    except Exception:
                        pass
                    print(f"[LEVERAGE SCOUT] Module initialized - {self.avantis_daily_bet_count} trades today")
                except Exception as e:
                    print(f"[!] Avantis init failed: {e}")
                    self.avantis_signals = None
                    self.avantis_executor = None
            else:
                print("[LEVERAGE SCOUT] Module paused via file or config")

    # Backward compat property
    @property
    def balance(self):
        return self.wallet.available

    @balance.setter
    def balance(self, value):
        self.wallet.available = value

    @property
    def starting_daily_balance(self):
        return self.wallet.starting_daily

    @starting_daily_balance.setter
    def starting_daily_balance(self, value):
        self.wallet.starting_daily = value

    def _is_already_bet(self, market_id, market_title=None):
        """Check if we already have a bet on this market.

        STRICT: If DB check fails, assume we already bet (safe default).
        Checks both market_id AND market_title to prevent duplicates.
        """
        market_id_str = str(market_id)
        # Check in-memory active positions first (fast path)
        for pos in self.active_positions:
            if str(pos.get('market_id')) == market_id_str:
                return True
            if market_title and pos.get('market_title', '').lower() == market_title.lower():
                return True
        # Check DB for any bet on same market (unresolved OR resolved within 72h)
        try:
            conn = sqlite3.connect(self.tracker.db_path, timeout=30)
            c = conn.cursor()
            # Check 1: Any unresolved bet on same market_id
            c.execute("""SELECT COUNT(*) FROM bets
                         WHERE market_id = ? AND status != 'resolved'""", (market_id_str,))
            if c.fetchone()[0] > 0:
                conn.close()
                return True
            # Check 2: Any bet (resolved or not) on same market_id in last 72h
            c.execute("""SELECT COUNT(*) FROM bets
                         WHERE market_id = ? AND timestamp > datetime('now', '-72 hours')""", (market_id_str,))
            if c.fetchone()[0] > 0:
                conn.close()
                return True
            # Check 3: Title-based fallback (catches cases where market_id differs)
            if market_title:
                c.execute("""SELECT COUNT(*) FROM bets
                             WHERE market_title = ? AND timestamp > datetime('now', '-72 hours')""",
                          (market_title,))
                if c.fetchone()[0] > 0:
                    conn.close()
                    print(f"  [CRYPTO TRADER] Title match caught duplicate: {market_title[:50]}")
                    return True
            conn.close()
        except Exception as e:
            print(f"  [CRYPTO TRADER] DB check error (defaulting to SKIP): {e}")
            return True  # SAFE DEFAULT: assume duplicate on error
        return False
    # ------------------------------------------------------------------
    # Crypto betting
    # ------------------------------------------------------------------

    def run_crypto_cycle(self):
        """Run crypto cycle -- markets resolving within 72 hours."""
        time_since = (datetime.now() - self.last_crypto_scan).total_seconds()
        if time_since < CRYPTO_SCAN_INTERVAL:
            return

        self.last_crypto_scan = datetime.now()

        if self.paused_polymarket:
            return

        if self.crypto_daily_bet_count >= CRYPTO_MAX_DAILY_BETS:
            return

        if self.balance < CRYPTO_BET_MIN + 5.0:
            return

        crypto_positions = self.wallet.module_positions("crypto")
        if len(crypto_positions) >= CRYPTO_MAX_CONCURRENT:
            return

        print(f"\n{'='*60}")
        print(f"CRYPTO CYCLE - {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*60}")
        print(f"[CFO] {self.wallet.status_summary()}")
        print(f"Crypto Bets Today: {self.crypto_daily_bet_count}/{CRYPTO_MAX_DAILY_BETS}")
        print(f"Crypto Positions: {len(crypto_positions)}/{CRYPTO_MAX_CONCURRENT}")

        daily_roi = (self.balance - self.starting_daily_balance) / self.starting_daily_balance if self.starting_daily_balance > 0 else 0.0
        print(f"Daily ROI: {daily_roi:+.1%}")

        markets = self.crypto_scanner.scan_crypto_markets()
        if not markets:
            print("[!] No crypto markets found")
            return

        # Filter: must resolve within 72 hours
        crypto_markets = [m for m in markets if 0 < m.get("days_until", 99) <= CRYPTO_MAX_HOURS / 24.0]
        if not crypto_markets:
            print(f"  No markets resolving within {CRYPTO_MAX_HOURS}h")
            return

        print(f"[CRYPTO TRADER] {len(crypto_markets)} markets within {CRYPTO_MAX_HOURS}h window")

        recommendations = self.crypto_scanner.evaluate_markets(crypto_markets)
        print(f"\n[CRYPTO TRADER] {len(recommendations)} recommendations")

        if not recommendations:
            return

        slots_open = CRYPTO_MAX_CONCURRENT - len(crypto_positions)
        bets_remaining = CRYPTO_MAX_DAILY_BETS - self.crypto_daily_bet_count
        max_bets = min(slots_open, bets_remaining, 3)

        bets_executed = 0
        existing_positions = self.wallet.module_positions("crypto")
        for rec in recommendations[:max_bets]:
            if self.balance < CRYPTO_BET_MIN + 5.0:
                print("[!] Balance too low for more bets")
                break

            if self._should_skip_bet(rec, existing_positions, bets_executed):
                continue
            if self._execute_crypto_bet(rec):
                bets_executed += 1

        print(f"\n[CRYPTO TRADER] Executed {bets_executed} bets")

    def _should_skip_bet(self, rec, existing_positions, bets_executed):
        """Check if a bet should be skipped."""
        if self.paused_polymarket:
            print(f"  [PAUSED] Polymarket crypto branch paused - stopping execution")
            return True
        if self._is_already_bet(rec['market_id'], rec.get('market_title')):
            print(f"  [SKIP] Already have position on {rec['market_title'][:50]}")
            return True

        # Use wallet coordinator for risk check
        can, reason = self.wallet.can_bet('crypto', rec['bet_amount'])
        if not can:
            print(f"  [CFO] {reason}")
            return True
        return False
    def _execute_crypto_bet(self, rec: dict) -> bool:
        """Execute a single crypto bet — flat $3 sizing."""
        bet_amount = 3.0

        can, reason = self.wallet.can_bet('crypto', bet_amount)
        if not can:
            print(f"  [CFO] {reason}")
            return False

        print(f"\n[CRYPTO TRADER] Executing...")
        print(f"  Market: {rec['market_title'][:60]}")
        print(f"  Side: {rec['bet_side'].upper()}")
        print(f"  Amount: ${bet_amount:.2f}")
        print(f"  Odds: {rec['bet_odds']:.1%}")
        print(f"  Edge: {rec['edge']:+.1%}")
        print(f"  Confidence: {rec['confidence']}")

        bankr_result = self.bankr.place_bet(
            market_title=rec['market_title'],
            side=rec['bet_side'],
            amount=bet_amount,
            odds=rec['bet_odds']
        )

        if not bankr_result['success']:
            print(f"  [ERROR] Bankr API failed: {bankr_result.get('error')}")
            return False

        print(f"  [BANKER] Trade ID: {bankr_result['trade_id']}")

        balance_before = self.balance

        bet_id = self.tracker.log_bet(
            market_id=rec['market_id'],
            market_title=rec['market_title'],
            category='crypto',
            side=rec['bet_side'],
            amount=bet_amount,
            odds=rec['bet_odds'],
            score=rec['confidence'],
            edge=rec['edge'],
            reasoning=rec['reasoning'],
            balance_before=balance_before
        )

        # Update cycle_type in DB
        try:
            conn = sqlite3.connect(self.tracker.db_path)
            c = conn.cursor()
            c.execute("UPDATE bets SET cycle_type = ? WHERE id = ?", ('crypto', bet_id))
            conn.commit()
            conn.close()
        except Exception:
            pass

        term = rec.get('_term', 'crypto')

        position_data = {
            'bet_id': bet_id,
            'market_id': rec['market_id'],
            'market_title': rec['market_title'],
            'amount': bet_amount,
            'side': rec['bet_side'],
            'odds': rec['bet_odds'],
            'category': 'crypto',
            'term': term,
            'cycle_type': 'crypto',
            'days_until': rec.get('days_until'),
            'coin_id': rec.get('coin_id'),
            'target_price': rec.get('target_price'),
            'direction': rec.get('direction'),
            'entry_price': rec.get('current_price'),
            'placed_at': datetime.now(),
            'score': rec['confidence'],
        }

        self.wallet.reserve_funds('crypto', bet_amount, position_data)
        self.active_positions.append(position_data)

        self.daily_bet_count += 1
        self.crypto_daily_bet_count += 1

        daily_roi = (self.balance - self.starting_daily_balance) / self.starting_daily_balance if self.starting_daily_balance > 0 else 0.0

        print(f"  [SUCCESS] Crypto bet placed (ID: {bet_id})")
        print(f"  Balance: ${balance_before:.2f} -> ${self.balance:.2f}")

        self.notifier.notify_alert(
            f"CRYPTO BET PLACED\n\n"
            f"Market: {rec['market_title'][:80]}\n"
            f"Side: {rec['bet_side'].upper()} | ${bet_amount:.2f}\n"
            f"Edge: {rec['edge']:+.1%} | Confidence: {rec['confidence']}\n"
            f"{rec.get('coin_id', '').upper()}: ${rec.get('current_price', 0):,.2f}\n"
            f"Balance: ${self.balance:.2f} | ROI: {daily_roi:+.1%}"
        )

        if self.dry_run:
            self._simulate_crypto_resolution(bet_id, rec)

        return True

    def _simulate_crypto_resolution(self, bet_id, rec):
        """Simulate crypto bet resolution for dry run testing."""
        win_prob = rec['our_estimate'] + random.uniform(-0.15, 0.15)
        win_prob = max(0.1, min(0.9, win_prob))
        won = random.random() < win_prob

        bet = next(p for p in self.active_positions if p['bet_id'] == bet_id)
        amount = bet['amount']

        if won:
            profit = (amount / rec['bet_odds']) - amount
            returned = amount + profit
            self.wallet.release_funds('crypto', bet_id, returned)
            self.consecutive_losses = 0
        else:
            profit = -amount
            self.wallet.release_funds('crypto', bet_id, 0)
            self.consecutive_losses += 1

        self.active_positions = [p for p in self.active_positions if p['bet_id'] != bet_id]
        self.tracker.resolve_bet(bet_id, won, profit, self.balance)

        daily_roi = (self.balance - self.starting_daily_balance) / self.starting_daily_balance if self.starting_daily_balance > 0 else 0.0
        print(f"  [RESOLVED] {'WON' if won else 'LOST'} - Profit: ${profit:+.2f}")

        if SMS_ON_BET_RESOLVE:
            self.notifier.notify_bet_resolved(
                market_title=rec.get('market_title', 'Unknown'),
                side=rec.get('bet_side', '?'),
                amount=amount,
                won=won,
                profit=profit,
                balance_before=self.balance - profit if won else self.balance + amount,
                balance_after=self.balance,
                daily_roi=daily_roi
            )

        if STOP_LOSS_DAILY is not None and daily_roi < STOP_LOSS_DAILY:
            self.paused_polymarket = True
            self.notifier.notify_alert(
                f"STOP LOSS TRIGGERED\n"
                f"Daily ROI: {daily_roi:+.1%}\n"
                f"Polymarket branch paused for rest of day"
            )

        if PAUSE_AFTER_LOSSES is not None and self.consecutive_losses >= PAUSE_AFTER_LOSSES:
            self.paused_polymarket = True
            self.notifier.notify_alert(
                f"Polymarket PAUSED after {self.consecutive_losses} consecutive losses\n"
                f"Taking a break to reassess"
            )

    # ------------------------------------------------------------------
    # Avantis leverage trading (Base chain)
    # ------------------------------------------------------------------

    def run_avantis_cycle(self):
        """Scan for Avantis trading signals and execute trades."""
        if not self.avantis_signals or not self.avantis_executor:
            return
        if self.paused_avantis:
            return

        # Check pause file
        pause_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.pause_avantis')
        if os.path.exists(pause_file):
            return

        scan_interval = getattr(__import__('hedge_fund_config'), 'AVANTIS_SCAN_INTERVAL', 900)
        time_since = (datetime.now() - self.last_avantis_scan).total_seconds()
        if time_since < scan_interval:
            return

        self.last_avantis_scan = datetime.now()

        max_daily = getattr(__import__('hedge_fund_config'), 'AVANTIS_MAX_DAILY_TRADES', 5)
        max_concurrent = getattr(__import__('hedge_fund_config'), 'AVANTIS_MAX_CONCURRENT', 3)

        if self.avantis_daily_bet_count >= max_daily:
            return

        # Check open positions
        open_positions = self.tracker.get_open_avantis_positions()
        if len(open_positions) >= max_concurrent:
            return

        print(f"\n{'='*60}")
        print(f"AVANTIS CYCLE - {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*60}")
        print(f"Trades Today: {self.avantis_daily_bet_count}/{max_daily}")
        print(f"Open Positions: {len(open_positions)}/{max_concurrent}")

        try:
            signals = self.avantis_signals.scan_opportunities()
        except Exception as e:
            print(f"[LEVERAGE SCOUT] Signal scan error: {e}")
            return

        if not signals:
            print("[LEVERAGE SCOUT] No signals found")
            return

        min_confidence = getattr(__import__('hedge_fund_config'), 'AVANTIS_MIN_CONFIDENCE', 0.65)
        good_signals = [s for s in signals if s['confidence'] >= min_confidence]

        if not good_signals:
            print(f"[LEVERAGE SCOUT] {len(signals)} signals, none above {min_confidence:.0%} confidence")
            return

        print(f"[LEVERAGE SCOUT] {len(good_signals)} signals above {min_confidence:.0%} confidence")

        slots = max_concurrent - len(open_positions)
        remaining = max_daily - self.avantis_daily_bet_count
        max_trades = min(slots, remaining)

        trades_executed = 0
        for signal in good_signals[:max_trades]:
            # Skip if already have open position on same pair+side
            already_open = any(
                p['pair'] == signal['pair'] and p['side'] == signal['side']
                for p in open_positions
            )
            if already_open:
                print(f"  [SKIP] Already have {signal['side']} on {signal['pair']}")
                continue

            if self._execute_avantis_trade(signal):
                trades_executed += 1
                self.avantis_daily_bet_count += 1

        print(f"\n[LEVERAGE SCOUT] Executed {trades_executed} trades")

    def _execute_avantis_trade(self, signal):
        """Execute a single Avantis leverage trade."""
        collateral = getattr(__import__('hedge_fund_config'), 'AVANTIS_COLLATERAL', 5.0)
        max_leverage = getattr(__import__('hedge_fund_config'), 'AVANTIS_MAX_LEVERAGE', 75)
        default_sl = getattr(__import__('hedge_fund_config'), 'AVANTIS_DEFAULT_SL', 3.0)
        default_tp = getattr(__import__('hedge_fund_config'), 'AVANTIS_DEFAULT_TP', 5.0)

        pair = signal['pair']
        side = signal['side']
        leverage = min(signal.get('leverage', 25), max_leverage)
        entry_price = signal.get('entry_price', 0)
        confidence = signal.get('confidence', 0)
        sl = signal.get('stop_loss_pct', default_sl)
        tp = signal.get('take_profit_pct', default_tp)
        reasoning = signal.get('reasoning', '')
        signal_type = signal.get('action', 'UNKNOWN')

        print(f"\n[LEVERAGE SCOUT] Executing...")
        print(f"  Pair: {pair}")
        print(f"  Side: {side.upper()} | {leverage}x")
        print(f"  Collateral: ${collateral:.2f}")
        print(f"  Entry: ${entry_price:,.2f}")
        print(f"  SL: {sl:.1f}% | TP: {tp:.1f}%")
        print(f"  Confidence: {confidence:.0%}")

        try:
            result = self.avantis_executor.open_position(
                market=pair,
                side=side,
                collateral_usd=collateral,
                leverage=leverage,
                stop_loss_pct=sl,
                take_profit_pct=tp
            )
        except Exception as e:
            print(f"  [ERROR] Executor failed: {e}")
            return False

        if not result.get('success'):
            print(f"  [ERROR] Trade failed: {result.get('error', 'unknown')}")
            return False

        trade_id = result.get('trade_id', result.get('response', '')[:50])

        # Log to DB
        try:
            pos_id = self.tracker.log_avantis_position(
                pair=pair,
                side=side,
                leverage=leverage,
                collateral=collateral,
                entry_price=entry_price,
                stop_loss_pct=sl,
                take_profit_pct=tp,
                confidence=confidence,
                signal_type=signal_type,
                reasoning=reasoning[:500],
                trade_id=str(trade_id)
            )
        except Exception as e:
            print(f"  [WARNING] DB log failed: {e}")
            pos_id = None

        # Reserve funds in wallet coordinator
        position_data = {
            'avantis_id': pos_id,
            'pair': pair,
            'side': side,
            'leverage': leverage,
            'collateral': collateral,
            'entry_price': entry_price,
            'placed_at': datetime.now(),
        }
        self.wallet.reserve_funds('avantis', collateral, position_data)

        print(f"  [SUCCESS] Position opened (DB #{pos_id})")

        self.notifier.notify_alert(
            f"AVANTIS TRADE\n\n"
            f"{leverage}x {side.upper()} {pair}\n"
            f"Entry: ${entry_price:,.2f}\n"
            f"Collateral: ${collateral:.2f}\n"
            f"SL: {sl:.1f}% | TP: {tp:.1f}%\n"
            f"Confidence: {confidence:.0%}\n"
            f"Signal: {signal_type}"
        )

        return True

    def _check_avantis_positions(self):
        """Check if any Avantis positions have been closed (SL/TP hit)."""
        if not self.avantis_executor:
            return

        open_positions = self.tracker.get_open_avantis_positions()
        if not open_positions:
            return

        try:
            bankr_positions = self.avantis_executor.get_open_positions()
        except Exception as e:
            print(f"[LEVERAGE SCOUT] Position check error: {e}")
            return

        # Check for ambiguous response (couldn't parse)
        if bankr_positions and any(p.get('ambiguous') for p in bankr_positions):
            print("  [LEVERAGE SCOUT] Position response ambiguous — skipping auto-close")
            return

        if not bankr_positions:
            # Bankr confirms no open positions — check trade history for real P&L
            for pos in open_positions:
                try:
                    opened_at = datetime.fromisoformat(pos['timestamp'])
                    if (datetime.now() - opened_at).total_seconds() > 300:
                        print(f"  [LEVERAGE SCOUT] Position #{pos['id']} ({pos['pair']}) appears closed")
                        # Query Bankr for actual trade history / P&L
                        pnl = None
                        collateral = pos.get('collateral', 0)
                        exit_reason = 'position_not_found'
                        try:
                            history = self.avantis_executor.get_trade_history(pos['pair'])
                            if history.get('success') and history.get('pnl') is not None:
                                pnl = history['pnl']
                                exit_reason = 'closed_with_pnl'
                                print(f"  [LEVERAGE SCOUT] Real P&L for {pos['pair']}: ${pnl:+.2f}")
                        except Exception as he:
                            print(f"  [LEVERAGE SCOUT] Trade history query failed: {he}")

                        if pnl is not None:
                            # Got real P&L
                            released = max(0, collateral + pnl)
                            self.tracker.close_avantis_position(
                                position_id=pos['id'],
                                exit_price=0,
                                pnl=pnl,
                                pnl_pct=(pnl / collateral * 100) if collateral else 0,
                                exit_reason=exit_reason
                            )
                            self.wallet.release_funds('avantis', pos['id'], released)
                            self.notifier.notify_alert(
                                f"AVANTIS POSITION CLOSED\n\n"
                                f"{pos['pair']} {pos.get('side', '?').upper()} {pos.get('leverage', '?')}x\n"
                                f"P&L: ${pnl:+.2f} | Collateral: ${collateral:.2f}\n"
                                f"Exit: {exit_reason}"
                            )
                        else:
                            # Can't determine P&L — mark needs_review, don't auto-close
                            print(f"  [LEVERAGE SCOUT] WARNING: Unknown P&L for #{pos['id']} — marking needs_review")
                            self.tracker.close_avantis_position(
                                position_id=pos['id'],
                                exit_price=0,
                                pnl=0,
                                pnl_pct=0,
                                exit_reason='needs_review'
                            )
                            # Still release collateral (assume worst case: lost)
                            self.wallet.release_funds('avantis', pos['id'], 0)
                            self.notifier.notify_alert(
                                f"AVANTIS NEEDS REVIEW\n\n"
                                f"{pos['pair']} {pos.get('side', '?').upper()} {pos.get('leverage', '?')}x\n"
                                f"Collateral: ${collateral:.2f}\n"
                                f"Could not determine P&L — manual review needed"
                            )
                except Exception as e:
                    print(f"  [LEVERAGE SCOUT] Error closing #{pos['id']}: {e}")
            return

        # Check each DB position against Bankr's open positions
        bankr_pairs = set()
        for bp in bankr_positions:
            pair_key = bp.get('pair', '') or bp.get('market', '')
            bankr_pairs.add(pair_key.upper())

        for pos in open_positions:
            pair_upper = pos['pair'].upper().replace('/', '')
            # Check if this position is still in Bankr's list
            still_open = False
            for bp_pair in bankr_pairs:
                if pair_upper in bp_pair.upper().replace('/', '') or bp_pair.upper().replace('/', '') in pair_upper:
                    still_open = True
                    break

            if not still_open:
                try:
                    opened_at = datetime.fromisoformat(pos['timestamp'])
                    if (datetime.now() - opened_at).total_seconds() > 300:
                        # Position closed (SL/TP or liquidation)
                        # Try to determine P&L from collateral
                        collateral = pos['collateral']
                        print(f"  [LEVERAGE SCOUT] Position #{pos['id']} ({pos['pair']}) CLOSED by exchange")
                        self.tracker.close_avantis_position(
                            position_id=pos['id'],
                            exit_price=0,
                            pnl=-collateral,
                            pnl_pct=-100.0,
                            exit_reason='sl_tp_or_liquidation'
                        )
                        self.wallet.release_funds('avantis', pos['id'], 0)

                        self.notifier.notify_alert(
                            f"AVANTIS POSITION CLOSED\n\n"
                            f"{pos['pair']} {pos['side'].upper()} {pos['leverage']}x\n"
                            f"Status: Closed by exchange (SL/TP/Liq)\n"
                            f"Collateral: ${collateral:.2f}"
                        )
                except Exception as e:
                    print(f"  [LEVERAGE SCOUT] Error processing #{pos['id']}: {e}")

    # ------------------------------------------------------------------
    # Position management & claiming
    # ------------------------------------------------------------------

    def _maybe_check_positions(self):
        """Check if any positions have resolved using AI reasoning."""
        if not self.active_positions or self.dry_run:
            return
        if self.last_claim_check is not None:
            hours_since = (datetime.now() - self.last_claim_check).total_seconds() / 3600
            if hours_since < 24:
                return

        print(f"\n[MANAGER] 24h position check with AI reasoning...")
        self.last_claim_check = datetime.now()

        try:
            result = self.bankr._run_prompt(
                "List ALL my Polymarket positions with details. For each show: "
                "market title, shares held, current P&L, whether market is OPEN or RESOLVED. "
                "Do NOT redeem or sell anything."
            )
            if not result.get('success'):
                print(f"  [!] Could not fetch positions: {result.get('error')}")
                return

            bankr_raw = result.get('response', '')
            print(f"  [BANKER] {bankr_raw[:400]}")

            wallet_result = self.bankr._run_prompt(
                "What is my available USDC balance on Polymarket only?"
            )
            wallet_raw = wallet_result.get('response', '') if wallet_result.get('success') else 'unknown'

            if self._ai_client:
                analysis = self._ai_analyze_positions(bankr_raw, wallet_raw)
                if analysis:
                    self._apply_heartbeat_analysis(analysis, bankr_raw)
                    return
                print("  [!] AI analysis failed, falling back to basic check")

            print("  [BASIC] No AI available - logging positions only")
            for pos in self.active_positions:
                print(f"  [ACCOUNTANT] {pos.get('market_title', '?')[:55]} - ${pos.get('amount', 0):.2f}")

        except Exception as e:
            print(f"  [!] Position check error: {e}")

    def _ai_analyze_positions(self, bankr_positions, wallet_info):
        """Use Claude to analyze position data and wallet balance with reasoning."""
        if not self._ai_client:
            return None

        our_positions = []
        for pos in self.active_positions:
            our_positions.append({
                'bet_id': pos.get('bet_id'),
                'market': pos.get('market_title', '?'),
                'side': pos.get('side', '?'),
                'amount_bet': pos.get('amount', 0),
                'odds_at_entry': pos.get('odds', 0),
            })

        deployed = self.wallet.total_deployed()

        prompt = f"""You are the AI heartbeat for a Polymarket crypto hedge fund.

Analyze the current state and provide structured reasoning.

## Bankr Position Data (raw):
{bankr_positions}

## Wallet Info (raw):
{wallet_info}

## Our Tracked Bets (from DB):
{json.dumps(our_positions, indent=2)}

## Current State:
- Available balance (tracked): ${self.balance:.2f}
- Deployed in bets: ${deployed:.2f}
- Total tracked: ${self.balance + deployed:.2f}
- Consecutive losses: {self.consecutive_losses}

## Instructions:
1. For each position, determine: is the market OPEN or RESOLVED?
2. If resolved, did we WIN or LOSE?
3. Extract the actual USDC balance from wallet info
4. Provide overall assessment and any concerns

IMPORTANT: Only mark a position as RESOLVED if Bankr explicitly says the market has ended/resolved/settled. If the market is still open (even if we're losing), keep it as OPEN.

Respond in JSON format:
{{
    "wallet_usdc": <number or null if unknown>,
    "positions": [
        {{
            "bet_id": <int>,
            "market": "<title>",
            "status": "OPEN" or "RESOLVED",
            "won": true/false/null,
            "current_pnl": "<description of P&L from Bankr data>",
            "shares": <number or null>
        }}
    ],
    "reasoning": "<2-3 sentences explaining your analysis>",
    "concerns": "<any red flags or things to watch>",
    "recommended_actions": ["<action1>", "<action2>"]
}}"""

        try:
            response = self._ai_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2500,
                messages=[{"role": "user", "content": prompt}]
            )
            raw_text = response.content[0].text.strip()

            if '```json' in raw_text:
                raw_text = raw_text.split('```json')[1].split('```')[0].strip()
            elif '```' in raw_text:
                raw_text = raw_text.split('```')[1].split('```')[0].strip()

            analysis = json.loads(raw_text)
            print(f"  [MANAGER] Reasoning: {analysis.get('reasoning', 'none')}")
            if analysis.get('concerns'):
                print(f"  [MANAGER] Concerns: {analysis.get('concerns')}")
            return analysis

        except anthropic.APIStatusError as e:
            if self._ai_fallback_key and e.status_code in (403, 502, 503):
                print(f"  [MANAGER] Bankr gateway error ({e.status_code}), falling back to Anthropic...")
                try:
                    fallback_client = anthropic.Anthropic(api_key=self._ai_fallback_key)
                    response = fallback_client.messages.create(
                        model="claude-haiku-4-5-20251001",
                        max_tokens=2500,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    raw_text = response.content[0].text.strip()
                    if '```json' in raw_text:
                        raw_text = raw_text.split('```json')[1].split('```')[0].strip()
                    elif '```' in raw_text:
                        raw_text = raw_text.split('```')[1].split('```')[0].strip()
                    analysis = json.loads(raw_text)
                    print(f"  [AI/FALLBACK] Reasoning: {analysis.get('reasoning', 'none')}")
                    return analysis
                except Exception as e2:
                    print(f"  [AI/FALLBACK] Error: {e2}")
                    return None
            print(f"  [MANAGER] API error: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"  [MANAGER] JSON parse error: {e}")
            print(f"  [MANAGER] Raw response: {raw_text[:300]}")
            return None
        except Exception as e:
            print(f"  [MANAGER] Analysis error: {e}")
            return None

    def _apply_heartbeat_analysis(self, analysis, bankr_raw):
        """Apply the AI analysis results: update positions, log heartbeat."""
        import re
        positions_data = analysis.get('positions', [])

        wallet_usdc = analysis.get('wallet_usdc')
        if wallet_usdc is not None and wallet_usdc > 0:
            old_balance = self.balance
            self.wallet.available = float(wallet_usdc)
            if abs(old_balance - self.balance) > 1.0:
                print(f"  [CFO] Balance updated: ${old_balance:.2f} -> ${self.balance:.2f}")

        for pos_info in positions_data:
            bet_id = pos_info.get('bet_id')
            if not bet_id:
                continue

            matching = [p for p in self.active_positions if p.get('bet_id') == bet_id]
            if not matching:
                continue

            pos = matching[0]
            status = pos_info.get('status', 'OPEN').upper()
            pnl = pos_info.get('current_pnl', '')

            if status == 'RESOLVED':
                won = bool(pos_info.get('won', False))
                print(f"  [RESOLVED] {pos.get('market_title', '?')[:50]} - {'WON' if won else 'LOST'}")

                profit = pos['amount'] if won else -pos['amount']
                if won:
                    self.wallet.release_funds('crypto', bet_id, pos['amount'] * 2)
                    self.consecutive_losses = 0
                else:
                    self.wallet.release_funds('crypto', bet_id, 0)
                    self.consecutive_losses += 1

                self.tracker.resolve_bet(pos['bet_id'], won, profit, self.balance)
                self.active_positions = [p for p in self.active_positions if p['bet_id'] != bet_id]

                if SMS_ON_BET_RESOLVE:
                    daily_roi = (self.balance - self.starting_daily_balance) / self.starting_daily_balance if self.starting_daily_balance > 0 else 0.0
                    self.notifier.notify_bet_resolved(
                        market_title=pos.get('market_title', '?'),
                        side=pos.get('side', '?'),
                        amount=pos['amount'],
                        won=won,
                        profit=profit,
                        balance_before=self.balance,
                        balance_after=self.balance,
                        daily_roi=daily_roi
                    )
            else:
                print(f"  [CFO] {pos.get('market_title', '?')[:50]} - {pnl}")

        deployed = self.wallet.total_deployed()
        self.tracker.log_heartbeat(
            heartbeat_type='position_check',
            bankr_raw=bankr_raw[:2000],
            ai_reasoning=analysis.get('reasoning'),
            ai_actions=analysis.get('recommended_actions'),
            positions_data=positions_data,
            wallet_balance=self.balance,
            total_value=self.balance + deployed
        )

        if len(self.active_positions) < CRYPTO_MAX_CONCURRENT:
            self.wallet.sync_with_wallet(update_starting=False)

    # claim_resolved_positions() removed -- replaced by BetResolver

    # ------------------------------------------------------------------
    # Summaries & resets
    # ------------------------------------------------------------------

    def send_results_summary(self):
        """Send hourly summary via Telegram."""
        now = datetime.now()
        time_since_last = (now - self.last_results_sms).total_seconds()

        if time_since_last < SMS_SUMMARY_INTERVAL:
            return

        daily_roi = (self.balance - self.starting_daily_balance) / self.starting_daily_balance if self.starting_daily_balance > 0 else 0.0
        daily_profit = self.balance - self.starting_daily_balance

        conn = sqlite3.connect(self.tracker.db_path)
        c = conn.cursor()
        c.execute("""
            SELECT COUNT(*) as total, SUM(won) as wins
            FROM bets
            WHERE DATE(timestamp) = DATE('now')
            AND status = 'resolved'
        """)
        result = c.fetchone()
        conn.close()

        total_resolved = result[0] if result[0] else 0
        wins = result[1] if result[1] else 0
        losses = total_resolved - wins
        win_rate = (wins / total_resolved * 100) if total_resolved > 0 else 0

        pos_lines = []
        for pos in self.active_positions[:8]:
            title = pos.get('market_title', '?')[:40]
            amt = pos.get('amount', 0)
            side = pos.get('side', '?').upper()
            pos_lines.append(f"  {side}: {title} (${amt:.2f})")

        pos_text = '\n'.join(pos_lines) if pos_lines else '  None'

        message = f"""HOURLY CRYPTO FUND

{self.wallet.status_summary()}
P&L: ${daily_profit:+.2f} ({daily_roi:+.1%})

Bets: {self.crypto_daily_bet_count} placed
Resolved: {total_resolved} ({wins}W-{losses}L)
Win Rate: {win_rate:.0f}%

Active Positions ({len(self.active_positions)}):
{pos_text}

Status: {'PAUSED (Polymarket)' if self.paused_polymarket else 'ACTIVE (Polymarket)'}
Weather: {len(self.weather_agent.active_weather_bets) if self.weather_agent else 0} positions
Avantis: {len(self.tracker.get_open_avantis_positions()) if self.avantis_signals else 0} positions
{now.strftime('%I:%M %p')}"""

        self.notifier.notify_alert(message)
        self.last_results_sms = now
        print(f"\n[MESSENGER] Hourly summary sent")

        deployed = self.wallet.total_deployed()
        pending = len([p for p in self.active_positions if p.get('category') == 'crypto'])
        self.tracker.save_portfolio_snapshot(
            available=self.balance,
            deployed=deployed,
            positions=self.wallet.total_position_count(),
            pending=pending,
            daily_roi=daily_roi,
            consec_losses=self.consecutive_losses
        )

    def check_daily_reset(self):
        """Reset daily counters at 10 PM for next day's markets."""
        now = datetime.now()

        if now.hour == DAILY_RESET_HOUR and (now - self.last_improvement_time).total_seconds() > 3600:
            print(f"\n{'='*60}")
            print(f"DAILY RESET - 10 PM")
            print(f"{'='*60}")

            improvements = self.tracker.run_daily_improvement()
            today_roi = self.tracker.get_daily_roi()

            if today_roi['total_bets'] > 0:
                print(f"\n[MANAGER]")
                print(f"  Profit: ${today_roi['profit']:+.2f}")
                print(f"  ROI: {today_roi['roi']:+.1%}")
                print(f"  Target Met: {'YES' if today_roi['met_target'] else 'NO'}")

            self.notifier.notify_alert(
                f"DAILY RESET 10PM\n\n"
                f"Crypto bets today: {self.crypto_daily_bet_count}/{CRYPTO_MAX_DAILY_BETS}\n"
                f"{self.wallet.status_summary()}\n"
                f"Active: {self.wallet.total_position_count()} positions\n"
                f"Counter reset - ready for tomorrow"
            )

            deployed = self.wallet.total_deployed()
            total_value = self.balance + deployed
            self.tracker.save_daily_performance(
                starting_balance=self.starting_daily_balance,
                ending_balance=total_value
            )

            self.last_improvement_time = now
            self.daily_bet_count = 0
            self.crypto_daily_bet_count = 0
            self.avantis_daily_bet_count = 0
            if self.weather_agent:
                self.weather_agent.reset_daily_counts()
            self.paused_polymarket = False
            self.consecutive_losses = 0

            self.wallet.daily_reset()
            if self.dry_run:
                self.wallet.starting_daily = self.balance

            print(f"\n[MANAGER] Bet counter reset to 0/{CRYPTO_MAX_DAILY_BETS}")
            print(f"Starting Balance: ${self.balance:.2f}")

    def periodic_balance_check(self):
        """Re-sync wallet balance periodically."""
        self.wallet.periodic_sync()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _run_pattern_analysis(self):
        """Run pattern analyzer every 30 hours to find strategy improvements."""
        PATTERN_INTERVAL_HOURS = 30

        if self.last_pattern_analysis is not None:
            hours_since = (datetime.now() - self.last_pattern_analysis).total_seconds() / 3600
            if hours_since < PATTERN_INTERVAL_HOURS:
                return

        self.last_pattern_analysis = datetime.now()
        try:
            results = self.pattern_analyzer.run_analysis()
            if results:
                actionable_count = sum(1 for s in results if s.get("actionable"))
                if actionable_count > 0:
                    print(f"  [DETECTIVE] {actionable_count} actionable findings - check Telegram")
                else:
                    print(f"  [DETECTIVE] No action needed - strategy looks correct")
        except Exception as e:
            print(f"  [DETECTIVE] Analysis error: {e}")

    def _run_periodic_heartbeat(self):
        """Run AI heartbeat every 6 hours for full portfolio analysis."""
        if not self._ai_client or self.dry_run:
            return
        if self.last_heartbeat is not None:
            hours_since = (datetime.now() - self.last_heartbeat).total_seconds() / 3600
            if hours_since < 6:
                return

        print(f"\n[MANAGER] 6h AI analysis...")
        self.last_heartbeat = datetime.now()

        try:
            wallet_result = self.bankr._run_prompt(
                "Show my full Polymarket portfolio: available USDC balance and all positions with P&L."
            )
            if not wallet_result.get('success'):
                print(f"  [!] Bankr query failed")
                return

            bankr_raw = wallet_result.get('response', '')
            deployed = self.wallet.total_deployed()

            our_positions = []
            for pos in self.active_positions:
                our_positions.append({
                    'bet_id': pos.get('bet_id'),
                    'market': pos.get('market_title', '?'),
                    'side': pos.get('side', '?'),
                    'amount_bet': pos.get('amount', 0),
                    'term': pos.get('term', '?'),
                })

            prompt = f"""You are the AI heartbeat for a crypto hedge fund on Polymarket.
Do a full portfolio health check.

## Bankr Portfolio Data:
{bankr_raw}

## Our Tracked Bets:
{json.dumps(our_positions, indent=2)}

## Fund State:
- Available: ${self.balance:.2f}
- Deployed: ${deployed:.2f}
- Total: ${self.balance + deployed:.2f}
- Consecutive losses: {self.consecutive_losses}
- Daily bets: {self.crypto_daily_bet_count}/{CRYPTO_MAX_DAILY_BETS}
- Polymarket paused: {self.paused_polymarket}
- Avantis paused: {self.paused_avantis}

## Analyze:
1. Portfolio health (are we diversified? too concentrated?)
2. Position quality (which bets look good/bad based on current P&L?)
3. Risk level (are we overexposed? any positions we should watch closely?)
4. Wallet accuracy (does Bankr balance match our tracked balance?)

Respond in JSON:
{{
    "wallet_usdc": <extracted USDC balance>,
    "portfolio_health": "GOOD" / "WARNING" / "CRITICAL",
    "reasoning": "<3-4 sentences>",
    "position_assessments": [
        {{"market": "<title>", "assessment": "<1 sentence>", "risk": "low/medium/high"}}
    ],
    "concerns": "<any issues>",
    "recommended_actions": ["<action>"]
}}"""

            response = self._ai_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2500,
                messages=[{"role": "user", "content": prompt}]
            )
            raw_text = response.content[0].text.strip()

            if '```json' in raw_text:
                raw_text = raw_text.split('```json')[1].split('```')[0].strip()
            elif '```' in raw_text:
                raw_text = raw_text.split('```')[1].split('```')[0].strip()

            analysis = json.loads(raw_text)

            health = analysis.get('portfolio_health', '?')
            reasoning = analysis.get('reasoning', '')
            print(f"  [MANAGER] Health: {health}")
            print(f"  [MANAGER] {reasoning}")

            if analysis.get('concerns'):
                print(f"  [MANAGER] Concerns: {analysis['concerns']}")

            for pa in analysis.get('position_assessments', []):
                print(f"  [{pa.get('risk', '?').upper()}] {pa.get('market', '?')[:50]} - {pa.get('assessment', '')}")

            ai_wallet = analysis.get('wallet_usdc')
            if ai_wallet is not None and ai_wallet > 0:
                if abs(self.balance - float(ai_wallet)) > 2.0:
                    print(f"  [CFO] Correcting: ${self.balance:.2f} -> ${ai_wallet:.2f}")
                    self.wallet.available = float(ai_wallet)

            self.tracker.log_heartbeat(
                heartbeat_type='periodic_analysis',
                bankr_raw=bankr_raw[:2000],
                ai_reasoning=reasoning,
                ai_actions=analysis.get('recommended_actions'),
                positions_data=analysis.get('position_assessments'),
                wallet_balance=self.balance,
                total_value=self.balance + deployed
            )

            if health == 'CRITICAL':
                self.notifier.notify_alert(
                    f"HEARTBEAT ALERT\n\n"
                    f"Health: {health}\n"
                    f"{reasoning}\n\n"
                    f"Concerns: {analysis.get('concerns', 'none')}"
                )

        except Exception as e:
            print(f"  [MANAGER] Error: {e}")


    def _write_shared_status(self):
        """Write shared status file for Baggins monitoring."""
        try:
            deployed = self.wallet.total_deployed()
            daily_roi = (self.balance - self.starting_daily_balance) / self.starting_daily_balance if self.starting_daily_balance > 0 else 0.0

            crypto_positions = []
            for pos in self.active_positions:
                if pos.get('category') == 'crypto':
                    crypto_positions.append({
                        'market': pos.get('market_title', '?')[:60],
                        'side': pos.get('side', '?'),
                        'amount': pos.get('amount', 0),
                        'term': pos.get('term', '?'),
                    })

            weather_positions = []
            if self.weather_agent:
                for wb in self.weather_agent.active_weather_bets:
                    weather_positions.append({
                        'city': wb.get('city', '?'),
                        'range': wb.get('temp_range', '?'),
                        'side': wb.get('side', '?'),
                        'amount': wb.get('amount', 0),
                    })

            avantis_positions = []
            avantis_open = self.tracker.get_open_avantis_positions() if self.avantis_signals else []
            for ap in avantis_open:
                avantis_positions.append({
                    'pair': ap.get('pair', '?'),
                    'side': ap.get('side', '?'),
                    'leverage': ap.get('leverage', 0),
                    'collateral': ap.get('collateral', 0),
                })

            status = {
                'timestamp': datetime.now().isoformat(),
                'balance': round(self.balance, 2),
                'deployed': round(deployed, 2),
                'total_value': round(self.balance + deployed, 2),
                'daily_roi': round(daily_roi, 4),
                'crypto_positions': crypto_positions,
                'weather_positions': weather_positions,
                'health': 'ACTIVE',
                'consecutive_losses': self.consecutive_losses,
                'crypto_paused': self.paused_polymarket,
                'weather_paused': not bool(self.weather_agent),
                'crypto_daily_bets': self.crypto_daily_bet_count,
                'weather_daily_bets': self.weather_agent.weather_daily_bet_count if self.weather_agent else 0,
                'avantis_positions': avantis_positions,
                'avantis_paused': self.paused_avantis,
                'avantis_daily_trades': self.avantis_daily_bet_count,
            }

            os.makedirs('/home/ubuntu/shared', exist_ok=True)
            with open('/home/ubuntu/shared/hedge_fund_status.json', 'w') as f:
                json.dump(status, f, indent=2)

        except Exception as e:
            pass  # Never crash the agent for status writes

    # _check_weather_resolutions() removed -- replaced by BetResolver

    def run(self):
        """Main run loop."""
        print(f"\n{'='*60}")
        print("STARTING CRYPTO HEDGE FUND AGENT")
        print(f"{'='*60}")

        cycle_count = 0

        try:
            while True:
                cycle_count += 1
                # Weather cycle FIRST -- weather gets seniority on funds
                if self.weather_agent:
                    pause_weather = os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.pause_weather'))
                    if not pause_weather:
                        self.weather_agent.run_weather_cycle(
                            available_balance=self.wallet.available,
                            wallet=self.wallet
                        )
                    elif cycle_count == 1:
                        print("[PAUSED] Weather module paused via .pause_weather file")

                # Scalper cycle (Up/Down 15-min markets)
                if getattr(__import__('hedge_fund_config'), 'ENABLE_UPDOWN_MODULE', False):
                    try:
                        run_scalper_cycle(
                            bankr=self.bankr,
                            wallet=self.wallet,
                            dry_run=self.dry_run
                        )
                    except Exception as e:
                        print(f"[SCALPER] Cycle error: {e}")

                # Crypto cycle SECOND -- weather gets first pick
                if getattr(__import__('hedge_fund_config'), 'ENABLE_CRYPTO_MODULE', True):
                    pause_crypto = os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.pause_crypto'))
                    if not pause_crypto:
                        self.run_crypto_cycle()
                    elif cycle_count == 1:
                        print("[PAUSED] Crypto module paused via .pause_crypto file")

                # Avantis leverage trading THIRD
                if self.avantis_signals and self.avantis_executor:
                    try:
                        self.run_avantis_cycle()
                        self._check_avantis_positions()
                    except Exception as e:
                        print(f"[LEVERAGE SCOUT] Cycle error: {e}")

                # Sports Analyst (Baggins' Buddy)
                if self.sports_analyst:
                    try:
                        self.sports_analyst.run_sports_cycle(
                            bankr=self.bankr,
                            wallet=self.wallet,
                            dry_run=self.dry_run
                        )
                    except Exception as e:
                        print(f"[BUDDY] Cycle error: {e}")

                # Unified resolver: Bankr claim (10 min) + weather data (1h)
                self.resolver.run()

                self._maybe_check_positions()   # 24h AI safety net
                self._run_periodic_heartbeat()
                self._run_pattern_analysis()
                self.periodic_balance_check()
                self.send_results_summary()
                self.check_daily_reset()

                # Write shared status for Baggins monitoring
                self._write_shared_status()

                # Sleep for the shortest cycle interval (rapid = 5 min)
                print(f"\n[MANAGER] Next cycle in {CRYPTO_SCAN_INTERVAL//60} minutes...")
                time.sleep(CRYPTO_SCAN_INTERVAL)

        except KeyboardInterrupt:
            print(f"\n\n{'='*60}")
            print("AGENT STOPPED")
            print(f"{'='*60}")
            print(f"Total Cycles: {cycle_count}")
            print(f"Final Balance: ${self.balance:.2f}")

            summary = self.tracker.get_summary()
            print(f"\nTotal Bets: {summary['total_bets']}")
            print(f"Win Rate: {summary['win_rate']*100:.1f}%")
            print(f"Total Profit: ${summary['total_profit']:+.2f}")


def test_agent():
    """Test the agent in dry run mode."""
    agent = ActiveHedgeFundAgent(
        dry_run=True,
        starting_balance=100.0
    )

    for i in range(3):
        print(f"\n{'#'*60}")
        print(f"TEST CYCLE {i+1}/3")
        print(f"{'#'*60}")

        agent.last_crypto_scan = datetime.now() - timedelta(seconds=CRYPTO_SCAN_INTERVAL + 1)
        agent.run_crypto_cycle()
        if i < 2:
            print("\n[MANAGER] 10 seconds until next cycle...")
            time.sleep(10)

    print(f"\n{'='*60}")
    print("TEST COMPLETE")
    print(f"{'='*60}")
    summary = agent.tracker.get_summary()
    print(f"Total Bets: {summary['total_bets']}")
    print(f"Win Rate: {summary['win_rate']*100:.1f}%")
    print(f"Final Balance: ${agent.balance:.2f}")
    print(f"Profit: ${agent.balance - 100:+.2f}")
    print(f"ROI: {((agent.balance - 100) / 100)*100:+.1f}%")


if __name__ == "__main__":
    if DRY_RUN:
        test_agent()
    else:
        agent = ActiveHedgeFundAgent(
            dry_run=False,
            starting_balance=STARTING_BALANCE
        )
        agent.run()
