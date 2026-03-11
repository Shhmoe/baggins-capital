"""
The Manager — Executive Department
Baggins Capital V3

Orchestrates every department. Decides when each employee runs.
Triggers daily reset at 22:00 UTC. Only authority to approve
and apply Detective findings.

V3 Workflow (10-step Manager cycle):
  1. Query CFO: available capital, deployment %, per-bet max, position count
  2. Query Risk Manager: active circuit breakers, HIGH severity warnings
  3. Check Compliance blocklist for new entries since last cycle
  4. Check Market Pulse for real-time alerts (CB proximity, cap exhaustion, exposure)
  5. Determine eligible departments (time windows, daily caps, active warnings)
  6. Circuit-breaker-active departments: log suppression, skip
  7. Signal eligible Liaisons to pre-pull intelligence packages
  8. Run eligible departments: Liaison → Risk → CFO → Dept Intelligence → Compliance → Banker
  9. Review detective_findings for pending findings awaiting approval
  10. At 22:00 UTC daily reset: archive pulse, confirm Historian, reset counters

Department: Executive
Reports to: — (Top of org)
"""

import os
import time
import random
import json
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
from company_clock import now_utc, now_et, status as clock_status, get_context
from hedge_fund_config import *
from archivist import Archivist
from compliance import ComplianceOfficer
from risk_manager import RiskManager
from historian import Historian
from bet_notifier import BetNotifier
from bankr import BankrExecutor
from polymarket_crypto import CryptoMarketScanner
from wallet_coordinator import WalletCoordinator
from bankr import BetResolver

# V3 Modules
from company_clock import CompanyClock
from db_writer import DBWriter
from db_reader import DBReader
from db_steward import DBSteward
from data_intake import DataIntake
from intel_crypto import CryptoIntel
from intel_scalper import ScalperIntel
from intel_weather import WeatherIntel
from intel_sports import SportsIntel
from market_pulse import MarketPulse
from detective import Detective
from signals_library import SignalsLibrary
from market_scout import MarketScout

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
    """The Manager — V3 orchestrator for all 24 employees."""

    def __init__(self, dry_run=True, starting_balance=100.0):
        self.dry_run = dry_run

        # ══════════════════════════════════════════════════════════════
        # CORE EMPLOYEES (existing)
        # ══════════════════════════════════════════════════════════════
        self.tracker = Archivist()
        self.notifier = BetNotifier()
        self.crypto_scanner = CryptoMarketScanner()
        self.bankr = BankrExecutor(dry_run=dry_run)
        self.wallet = WalletCoordinator(self.bankr, self.tracker, starting_balance, dry_run)

        # ══════════════════════════════════════════════════════════════
        # V3 EMPLOYEES — Data & Analytics
        # ══════════════════════════════════════════════════════════════
        self.clock = CompanyClock()
        self.db_writer = DBWriter(self.tracker.db_path)
        self.db_reader = DBReader(self.tracker.db_path)
        self.db_steward = DBSteward(self.tracker.db_path)
        self.data_intake = DataIntake(self.tracker.db_path)
        self.pulse = MarketPulse(self.tracker.db_path)
        self.scout = MarketScout(self.tracker.db_path)
        self._scout_queues = {}
        self.detective = Detective(self.tracker.db_path)
        self.signals = SignalsLibrary(self.tracker.db_path)

        # ══════════════════════════════════════════════════════════════
        # V3 EMPLOYEES — Intelligence Liaisons
        # ══════════════════════════════════════════════════════════════
        self.intel_crypto = CryptoIntel(self.tracker.db_path)
        self.intel_scalper = ScalperIntel(self.tracker.db_path)
        self.intel_weather = WeatherIntel(self.tracker.db_path)
        self.intel_sports = SportsIntel(self.tracker.db_path)

        # ══════════════════════════════════════════════════════════════
        # V3 EMPLOYEES — Operations (upgraded)
        # ══════════════════════════════════════════════════════════════
        self.compliance = ComplianceOfficer(self.tracker.db_path)
        self.risk_manager = RiskManager(self.tracker.db_path)
        self.historian = Historian(self.tracker.db_path)

        print("[MANAGER] V3 employees initialized:")
        print("  [DB STEWARD] Database infrastructure")
        print("  [PULSE] Market Pulse Analyst")
        print("  [DETECTIVE] Forensic investigator")
        print("  [SIGNALS] Signals Librarian")
        print("  [LIAISON] Crypto, Scalper, Weather, Sports intelligence")
        print("  [COMPLIANCE] Adaptive pre-flight validation")
        print("  [RISK] Adaptive risk assessment")
        print("  [HISTORIAN] Deep daily analysis")

        # Initialize V3 tables (DB Steward's job)
        try:
            self.db_steward.initialize_v3_tables()
            print("  [DB STEWARD] V3 tables verified")
        except Exception as e:
            print(f"  [DB STEWARD] Table init warning: {e}")

        # Register V3 hooks
        self._register_v3_hooks()

        # ══════════════════════════════════════════════════════════════
        # WEATHER MODULE
        # ══════════════════════════════════════════════════════════════
        self.weather_agent = None
        self.sports_analyst = None
        if HAS_WEATHER and getattr(__import__('hedge_fund_config'), 'ENABLE_WEATHER_MODULE', False):
            try:
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

        # ══════════════════════════════════════════════════════════════
        # STATE
        # ══════════════════════════════════════════════════════════════
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
        self.paused_polymarket = PAUSED_POLYMARKET
        self.paused_avantis = PAUSED_AVANTIS
        self.avantis_signals = None
        self.avantis_executor = None
        self.avantis_daily_bet_count = 0

        # V3 cycle state
        self._last_pulse_update = None
        self._last_detective_check = None
        self._last_monday_check = None
        self._v3_cycle_count = 0

        # Load active positions from DB via wallet coordinator
        crypto_positions = self.wallet.load_positions_from_db()
        self.active_positions = crypto_positions

        if len(self.active_positions) >= CRYPTO_MAX_CONCURRENT:
            self._positions_locked = True

        # Count today's bets
        try:
            self.daily_bet_count = self.db_reader.get_daily_bet_count()
            self.crypto_daily_bet_count = self.db_reader.get_daily_bet_count(category='crypto', cycle_type='crypto')
        except Exception:
            pass

        # In live mode, sync balance with actual wallet
        if not self.dry_run:
            self.wallet.sync_with_wallet()
            self.last_balance_sync = datetime.now()

        deployed = self.wallet.total_deployed()

        print("\n" + "="*60)
        print("BAGGINS CAPITAL V3 — THE MANAGER")
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
        print(f"V3 Employees: 24 active")
        print("="*60)

        self.notifier.notify_alert(
            f"Baggins Capital V3 Started\n"
            f"Mode: {'DRY RUN' if dry_run else 'LIVE'}\n"
            f"{self.wallet.status_summary()}\n"
            f"Positions: {len(self.active_positions)}\n"
            f"Today's bets: {self.daily_bet_count}/{CRYPTO_MAX_DAILY_BETS}\n"
            f"V3: 24 employees active"
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
                    try:
                        row = self.db_reader.fetchone("SELECT COUNT(*) FROM avantis_positions WHERE DATE(timestamp) = DATE('now')")
                        self.avantis_daily_bet_count = row[0] if row else 0
                    except Exception:
                        pass
                    print(f"[LEVERAGE SCOUT] Module initialized - {self.avantis_daily_bet_count} trades today")
                except Exception as e:
                    print(f"[!] Avantis init failed: {e}")
                    self.avantis_signals = None
                    self.avantis_executor = None
            else:
                print("[LEVERAGE SCOUT] Module paused via file or config")

        # Initial pulse update
        try:
            self.pulse.update()
            self._last_pulse_update = datetime.now()
            print("[PULSE] Initial pulse update complete")
        except Exception as e:
            print(f"[PULSE] Initial update error: {e}")

    # ══════════════════════════════════════════════════════════════
    # V3 HOOK REGISTRATION
    # ══════════════════════════════════════════════════════════════

    def _register_v3_hooks(self):
        """Register all V3 employee hook handlers with Company Clock."""
        # DAILY_RESET_HOOK (22:00 UTC)
        self.clock.register_hook('DAILY_RESET_HOOK', self.pulse.on_daily_reset)
        self.clock.register_hook('DAILY_RESET_HOOK', self.compliance.on_daily_reset)

        # PRE_RESET_HOOK (21:45 UTC — archive before reset)
        self.clock.register_hook('PRE_RESET_HOOK', self.pulse.on_pre_reset)

        # MONDAY_OPEN_HOOK (weekly adaptive recalculation)
        self.clock.register_hook('MONDAY_OPEN_HOOK', self._on_monday_open)

        # CIRCUIT_BREAKER_HOOK — notify Messenger on circuit breaker
        self.clock.register_hook('CIRCUIT_BREAKER_HOOK', self._on_circuit_breaker)
        self.clock.register_hook('CIRCUIT_BREAKER_CLEAR_HOOK', self._on_circuit_breaker_clear)

        # DAILY_CAP_HIT_HOOK — log when department hits cap
        self.clock.register_hook('DAILY_CAP_HIT_HOOK', self._on_cap_hit)

        # DAILY_CAP_WARNING_HOOK — CB proximity warning
        self.clock.register_hook('DAILY_CAP_WARNING_HOOK', self._on_cap_warning)

        print("  [CLOCK] V3 hooks registered: DAILY_RESET, PRE_RESET, MONDAY_OPEN, "
              "CIRCUIT_BREAKER, CIRCUIT_BREAKER_CLEAR, DAILY_CAP_HIT, DAILY_CAP_WARNING")

    def _on_circuit_breaker(self, payload=None):
        """Hook handler: circuit breaker triggered."""
        dept = payload.get('department', 'unknown') if payload else 'unknown'
        self.notifier.notify_alert(
            f"CIRCUIT BREAKER TRIGGERED\n\n"
            f"Department: {dept}\n"
            f"All new bets halted for {dept}\n"
            f"Manager must manually clear")

    def _on_circuit_breaker_clear(self, payload=None):
        """Hook handler: circuit breaker cleared."""
        dept = payload.get('department', 'unknown') if payload else 'unknown'
        print(f"  [HOOK] Circuit breaker cleared: {dept} — entering recovery mode")
        self.notifier.notify_alert(
            f"CIRCUIT BREAKER CLEARED\n\n"
            f"Department: {dept}\n"
            f"Recovery mode: 50% sizing for 2h")

    def _on_cap_hit(self, payload=None):
        """Hook handler: daily cap reached for a department."""
        dept = payload.get('department', 'unknown') if payload else 'unknown'
        print(f"  [HOOK] Daily cap hit: {dept}")

    def _on_cap_warning(self, payload=None):
        """Hook handler: department approaching circuit breaker or cap exhaustion."""
        dept = payload.get('department', 'unknown') if payload else 'unknown'
        warn_type = payload.get('type', '') if payload else ''
        print(f"  [HOOK] Cap warning: {dept} ({warn_type})")

    def _on_monday_open(self, payload=None):
        """Monday open: recalculate all adaptive limits from Historian data."""
        print("\n[MANAGER] MONDAY_OPEN — weekly adaptive recalculation")

        # Gather 7d department win rates from Historian
        dept_win_rates = {}
        dept_bet_counts = {}
        volatility_scores = {}
        category_avg_adverse = {}

        for dept in ['crypto', 'weather', 'sports', 'updown']:
            try:
                stats = self.db_reader.get_department_stats(dept, days=7)
                if stats:
                    dept_win_rates[dept] = stats.get('win_rate', 0.50)
                    dept_bet_counts[dept] = stats.get('total_bets', 0)
                    # Volatility = std dev of daily P&L
                    volatility_scores[dept] = stats.get('volatility', 0.15)
                    category_avg_adverse[dept] = stats.get('avg_loss', 0.10)
            except Exception as e:
                print(f"  [MANAGER] Stats error for {dept}: {e}")

        recalc_payload = {
            'dept_win_rates': dept_win_rates,
            'dept_bet_counts': dept_bet_counts,
            'volatility_scores': volatility_scores,
            'category_avg_adverse': category_avg_adverse,
        }

        # CFO: recalculate deployment cap, reserve floor, position limits
        try:
            blended_wr = self.db_reader.get_blended_win_rate(days=7)
            self.wallet.on_monday_open({'win_rate_7d': blended_wr})
        except Exception as e:
            print(f"  [CFO] Recalc error: {e}")

        # Risk Manager: update thresholds from Historian data
        try:
            self.risk_manager.on_monday_open(recalc_payload)
        except Exception as e:
            print(f"  [RISK] Recalc error: {e}")

        # Compliance: rescale daily caps by department WR
        try:
            self.compliance.on_monday_open(recalc_payload)
        except Exception as e:
            print(f"  [COMPLIANCE] Recalc error: {e}")

        print("[MANAGER] Weekly recalculation complete")

    # ══════════════════════════════════════════════════════════════
    # V3 CYCLE CHECKS (Steps 1-7 of Manager Cycle)
    # ══════════════════════════════════════════════════════════════

    def _run_v3_pre_trading_checks(self):
        """V3 Manager cycle steps 1-7. Runs before any trading department."""
        self._v3_cycle_count += 1
        ctx = get_context()

        # Step 1: Query CFO
        try:
            cfo_state = self.wallet.get_state()
            if self._v3_cycle_count % 10 == 1:  # Log every 10th cycle
                print(f"  [CFO] Available: ${cfo_state.get('available_capital', 0):.2f} | "
                      f"Deploy: {cfo_state.get('deployment_cap_pct', 0.70):.0%} | "
                      f"Positions: {cfo_state.get('open_positions', 0)}/{cfo_state.get('position_limit', 25)}")
        except Exception as e:
            print(f"  [CFO] CRITICAL — cannot query state: {e}")
            print(f"  [MANAGER] HALTING all trading until CFO restored")
            return False

        # Step 2: Query Risk Manager for circuit breakers
        try:
            risk_summary = self.risk_manager.get_risk_summary()
            active_cbs = risk_summary.get('circuit_breakers_active', [])
            if active_cbs:
                for dept in active_cbs:
                    print(f"  [RISK] Circuit breaker ACTIVE: {dept} — department halted")
                    # Fire condition hook
                    self.clock.fire_hook('CIRCUIT_BREAKER_HOOK', {
                        'department': dept, 'action': 'active'})
        except Exception as e:
            print(f"  [RISK] Warning — assessment unavailable: {e}")

        # Step 3: Check Compliance blocklist for new entries since last cycle
        try:
            blocklist_summary = self.compliance.get_daily_summary()
            rejected = blocklist_summary.get('rejected', 0)
            if rejected > 0 and self._v3_cycle_count % 10 == 1:
                print(f"  [COMPLIANCE] {rejected} rejections today | "
                      f"Adaptive caps: {blocklist_summary.get('adaptive_caps', {})}")
        except Exception as e:
            print(f"  [COMPLIANCE] Blocklist check error: {e}")

        # Step 4: Update Market Pulse + surface alerts
        try:
            pulse_needed = (self._last_pulse_update is None or
                           (datetime.now() - self._last_pulse_update).total_seconds() > 1800)
            if pulse_needed:
                self.pulse.update()
                self._last_pulse_update = datetime.now()

            # Surface pulse alerts to Manager
            for dept in ['crypto', 'weather', 'sports', 'updown']:
                snapshot = self.pulse.get_department_snapshot(dept)
                if not snapshot:
                    continue
                # CB proximity alert (4+ consecutive losses)
                cb_prox = snapshot.get('circuit_breaker_proximity', {})
                if cb_prox.get('details', {}).get('warning'):
                    consec = cb_prox['details'].get('consecutive_losses', 0)
                    print(f"  [PULSE] WARNING: {dept} at {consec} consecutive losses — CB proximity")
                    if consec >= 4:
                        self.clock.fire_hook('DAILY_CAP_WARNING_HOOK', {
                            'department': dept, 'type': 'cb_proximity',
                            'consecutive_losses': consec})
                # Cap exhaustion (80%+ used)
                cap_prog = snapshot.get('daily_cap_progress', {})
                if cap_prog.get('value', 0) >= 0.80:
                    remaining = cap_prog.get('details', {}).get('remaining', 0)
                    print(f"  [PULSE] {dept} cap at {cap_prog['value']:.0%} — {remaining} bets remaining")
                    if cap_prog['value'] >= 1.0:
                        self.clock.fire_hook('DAILY_CAP_HIT_HOOK', {
                            'department': dept})
        except Exception as e:
            print(f"  [PULSE] Update error: {e}")

        # Step 5: Check time-based hooks (Company Clock)
        try:
            self.clock.check_and_fire_hooks()
        except Exception as e:
            print(f"  [CLOCK] Hook check error: {e}")

        # Check Monday open (weekly recalibration)
        try:
            now = datetime.now()
            if now.weekday() == 0 and now.hour >= 9:  # Monday after 9 AM
                if (self._last_monday_check is None or
                    (now - self._last_monday_check).days >= 1):
                    self._on_monday_open()
                    self._last_monday_check = now
        except Exception:
            pass

        return True

    def _get_liaison_package(self, department):
        """Step 7: Get intelligence package from department's Liaison."""
        try:
            if department == 'crypto':
                return self.intel_crypto.get_package()
            elif department == 'updown':
                return self.intel_scalper.get_package()
            elif department == 'weather':
                return self.intel_weather.get_package()
            elif department == 'sports':
                return self.intel_sports.get_package()
        except Exception as e:
            print(f"  [LIAISON] {department} package error: {e}")
        return None

    def _is_department_eligible(self, department):
        """Step 5-6: Check if department can trade this cycle."""
        # Circuit breaker check
        try:
            risk_summary = self.risk_manager.get_risk_summary()
            if department in risk_summary.get('circuit_breakers_active', []):
                return False, "circuit breaker active"
        except Exception:
            pass

        # Time window check for weather
        if department == 'weather':
            ctx = get_context()
            hour_et = ctx.hour
            in_window = (8 <= hour_et < 10) or (14 <= hour_et < 16) or (21 <= hour_et < 23)
            if not in_window:
                return False, "outside weather trading window"

        return True, "eligible"

    # ══════════════════════════════════════════════════════════════
    # V3 DETECTIVE REVIEW (Step 9)
    # ══════════════════════════════════════════════════════════════

    def _review_detective_findings(self):
        """Step 9: Check if Detective should investigate, review pending findings."""
        try:
            ran_investigation = False
            if self.detective.should_investigate():
                print("\n[DETECTIVE] Starting forensic investigation...")
                result = self.detective.investigate()
                if result.get('ran'):
                    ran_investigation = True
                    count = result.get('findings', 0)
                    print(f"  [DETECTIVE] Investigation complete: {count} new findings")
                else:
                    print("  [DETECTIVE] No actionable findings this session")

            # Catalog any pending findings from detective_findings table into Signals Library
            pending = self.db_reader.fetchall(
                "SELECT id, root_cause, confidence, recommended_action, affected_employee "
                "FROM detective_findings WHERE status = 'pending'"
            )
            if pending:
                cataloged = 0
                for row in pending:
                    finding_id, root_cause, confidence, recommendation, category = row
                    result = self.signals.catalog_finding(
                        finding_id=finding_id,
                        category=category or 'unknown',
                        root_cause=root_cause or '',
                        recommendation=recommendation or '',
                        confidence=confidence or 0.5,
                    )
                    if result and result.get('cataloged'):
                        cataloged += 1
                if cataloged > 0:
                    print(f"  [DETECTIVE] {cataloged}/{len(pending)} pending findings cataloged to Signals Library")
                if ran_investigation and cataloged > 0:
                    self.notifier.notify_alert(
                        f"DETECTIVE FINDINGS\n\n"
                        f"{cataloged} new findings cataloged to Signals Library\n"
                        f"Check detective_findings table"
                    )
            elif ran_investigation:
                print("  [DETECTIVE] No pending findings to catalog")
        except Exception as e:
            print(f"  [DETECTIVE] Investigation error: {e}")

    # ══════════════════════════════════════════════════════════════
    # BACKWARD COMPAT PROPERTIES
    # ══════════════════════════════════════════════════════════════

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
        for pos in self.active_positions:
            if str(pos.get('market_id')) == market_id_str:
                return True
            if market_title and pos.get('market_title', '').lower() == market_title.lower():
                return True
        try:
            row = self.db_reader.fetchone(
                "SELECT COUNT(*) FROM bets WHERE market_id = ? AND status != 'resolved'",
                (market_id_str,))
            if row[0] > 0:
                return True
            row = self.db_reader.fetchone(
                "SELECT COUNT(*) FROM bets WHERE market_id = ? AND timestamp > datetime('now', '-72 hours')",
                (market_id_str,))
            if row[0] > 0:
                return True
            if market_title:
                row = self.db_reader.fetchone(
                    "SELECT COUNT(*) FROM bets WHERE market_title = ? AND timestamp > datetime('now', '-72 hours')",
                    (market_title,))
                if row[0] > 0:
                    print(f"  [CRYPTO TRADER] Title match caught duplicate: {market_title[:50]}")
                    return True
        except Exception as e:
            print(f"  [CRYPTO TRADER] DB check error (defaulting to SKIP): {e}")
            return True
        return False

    # ------------------------------------------------------------------
    # Crypto betting
    # ------------------------------------------------------------------

    def run_crypto_cycle(self, scout_queue=None):
        """Run crypto cycle — markets resolving within 72 hours."""
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

        # V3 Step 5-6: Check department eligibility
        eligible, reason = self._is_department_eligible('crypto')
        if not eligible:
            print(f"  [MANAGER] Crypto department ineligible: {reason}")
            return

        print(f"\n{'='*60}")
        print(f"CRYPTO CYCLE - {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*60}")
        print(f"[CFO] {self.wallet.status_summary()}")
        print(f"Crypto Bets Today: {self.crypto_daily_bet_count}/{CRYPTO_MAX_DAILY_BETS}")
        print(f"Crypto Positions: {len(crypto_positions)}/{CRYPTO_MAX_CONCURRENT}")

        daily_roi = (self.balance - self.starting_daily_balance) / self.starting_daily_balance if self.starting_daily_balance > 0 else 0.0
        print(f"Daily ROI: {daily_roi:+.1%}")

        # V3 Step 7: Get Liaison intelligence package
        intel_package = self._get_liaison_package('crypto')
        if intel_package:
            # Check for modifier drift flags
            drift_flag = intel_package.get('use_flat_modifiers', False)
            if drift_flag:
                print("  [LIAISON] Modifier drift detected — using flat modifiers this cycle")
            # Log package summary
            pulse = intel_package.get('pulse', {})
            wr = pulse.get('rolling_win_rate', {}).get('value')
            if wr is not None:
                print(f"  [LIAISON] Crypto WR: {wr:.1%} | Streak: {pulse.get('current_streak', {}).get('value', 0)}")

        # Use Scout queue if available, otherwise fall back to direct scan
        if scout_queue:
            crypto_markets = scout_queue
            print(f"[CRYPTO TRADER] {len(crypto_markets)} markets from Scout queue")
        else:
            markets = self.crypto_scanner.scan_crypto_markets()
            if not markets:
                print("[!] No crypto markets found")
                return
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

            if self._should_skip_bet(rec, existing_positions, bets_executed, intel_package):
                continue
            if self._execute_crypto_bet(rec, intel_package):
                bets_executed += 1

        print(f"\n[CRYPTO TRADER] Executed {bets_executed} bets")

    def _should_skip_bet(self, rec, existing_positions, bets_executed, intel_package=None):
        """V3 6-Step Bet Protocol: Liaison → Risk → CFO → Dept Intel → Compliance → Banker.

        Returns True if bet should be skipped (rejected at any gate).
        """
        if self.paused_polymarket:
            print(f"  [PAUSED] Polymarket crypto branch paused - stopping execution")
            return True

        # In-memory position check (fast path)
        for pos in existing_positions:
            if str(pos.get('market_id')) == str(rec['market_id']):
                print(f"  [SKIP] Already have position on {rec['market_title'][:50]}")
                return True

        bet_dict = {
            'market_id': rec['market_id'],
            'market_title': rec.get('market_title', ''),
            'category': 'crypto',
            'side': rec.get('bet_side', ''),
            'amount': rec.get('bet_amount', 3.0),
            'odds': rec.get('bet_odds', 0),
            'edge': rec.get('edge', 0),
            'confidence': rec.get('confidence', 0),
        }

        # ── STEP 1: Intelligence Liaison (already fetched, applied via intel_package) ──
        # Liaison package was retrieved in run_crypto_cycle and passed here

        # ── STEP 2: Risk Manager (advisory — only blocks on circuit breaker) ──
        risk_ok, risk_level, risk_warnings = self.risk_manager.assess(bet_dict)
        if not risk_ok:
            print(f"  [RISK] Blocked: {risk_warnings[0] if risk_warnings else 'circuit breaker'}")
            return True

        # Apply risk advisory: raise confidence floor on HIGH warnings
        if risk_level == 'high':
            original_conf = bet_dict['confidence']
            # HIGH warnings: need 10% higher confidence
            min_conf = original_conf * 1.10
            if rec.get('confidence', 0) < min_conf:
                print(f"  [RISK] HIGH warning — confidence {rec['confidence']} below raised floor {min_conf:.0f}")
                # Advisory only — don't skip, just log the warning

        # Check recovery mode sizing
        recovery_mult = self.risk_manager.get_recovery_sizing_multiplier('crypto')
        if recovery_mult < 1.0:
            bet_dict['amount'] = round(bet_dict['amount'] * recovery_mult, 2)
            print(f"  [RISK] Recovery mode — bet sized at {recovery_mult:.0%}: ${bet_dict['amount']:.2f}")

        # ── STEP 3: CFO verification ──
        can, reason = self.wallet.can_bet('crypto', bet_dict['amount'])
        if not can:
            print(f"  [CFO] {reason}")
            return True

        # ── STEP 4: Department Intelligence (from Liaison package) ──
        if intel_package:
            # Apply modifier drift flag
            if intel_package.get('use_flat_modifiers', False):
                # Flag is advisory — trader should use flat modifiers
                pass  # Trader's evaluate_markets already ran, but we log the warning

        # ── STEP 5: Compliance pre-flight (HARD GATE) ──
        approved, reason, warnings = self.compliance.pre_flight(bet_dict)
        if not approved:
            print(f"  [COMPLIANCE] Rejected: {reason}")
            return True

        # All gates passed — proceed to Step 6 (Banker execution) in _execute_crypto_bet
        return False

    def _execute_crypto_bet(self, rec: dict, intel_package=None) -> bool:
        """Execute a single crypto bet — V3 Step 6: Banker + Write Archivist."""
        bet_amount = 3.0

        # Apply recovery mode sizing if active
        recovery_mult = self.risk_manager.get_recovery_sizing_multiplier('crypto')
        if recovery_mult < 1.0:
            bet_amount = round(bet_amount * recovery_mult, 2)

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
        print(f"  Type: {rec.get('bet_type', 'UNKNOWN')}")
        print(f"  Confidence: {rec['confidence']}")

        # Step 6a: Execute via Banker
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

        # Verify bet execution
        try:
            verify_result = self.bankr.verify_bet_execution(rec['market_title'], rec['bet_side'])
            if verify_result.get("verified"):
                print(f"  [VERIFIED] Crypto bet confirmed in Bankr positions")
            else:
                print(f"  [WARN] Crypto bet unverified: {verify_result.get('reason', 'unknown')} -- logging anyway")
        except Exception as ve:
            print(f"  [WARN] Crypto verification failed: {ve} -- logging anyway")

        balance_before = self.balance

        # Step 6b: Log full decision snapshot via Write Archivist
        decision_snapshot = {
            'raw_data': {
                'market_odds': rec.get('bet_odds'),
                'our_estimate': rec.get('our_estimate'),
                'coin_id': rec.get('coin_id'),
                'current_price': rec.get('current_price'),
                'change_24h': rec.get('change_24h'),
                'change_7d': rec.get('change_7d'),
                'target_price': rec.get('target_price'),
            },
            'modifiers': {
                'hour_mod': rec.get('hour_mod'),
                'asset_mod': rec.get('asset_mod'),
            },
            'decision': {
                'confidence': rec.get('confidence'),
                'edge': rec.get('edge'),
                'format_type': rec.get('format_type', 'unknown'),
                'direction': rec.get('direction'),
                'same_day': rec.get('same_day'),
                'days_until': rec.get('days_until'),
            },
            'strategy': {
                'bet_type': rec.get('bet_type', 'UNKNOWN'),
                'term': rec.get('_term', 'crypto'),
                'composite_score': rec.get('composite_score'),
            },
            'v3_context': {
                'liaison_package': bool(intel_package),
                'risk_level': 'assessed',
                'recovery_mode': recovery_mult < 1.0,
                'cfo_state': 'verified',
                'compliance': 'passed',
            },
        }

        bet_id = self.data_intake.validate_and_write_bet(
            market_id=rec['market_id'],
            market_title=rec['market_title'],
            category='crypto',
            side=rec['bet_side'],
            amount=bet_amount,
            odds=rec['bet_odds'],
            confidence_score=rec['confidence'],
            edge=rec['edge'],
            reasoning=rec['reasoning'],
            balance_before=balance_before,
            cycle_type='crypto',
            bet_type=rec.get('bet_type', 'UNKNOWN'),
            format_type=rec.get('format_type', 'unknown'),
            decision_snapshot=decision_snapshot,
        )

        term = rec.get('_term', 'crypto')

        position_data = {
            'bet_id': bet_id,
            'market_id': rec['market_id'],
            'market_title': rec['market_title'],
            'amount': bet_amount,
            'side': rec['bet_side'],
            'odds': rec['bet_odds'],
            'category': 'crypto',
            'bet_type': rec.get('bet_type', 'UNKNOWN'),
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
        self.data_intake.validate_and_write_resolution(bet_id, won, profit, self.balance)

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

        if bankr_positions and any(p.get('ambiguous') for p in bankr_positions):
            print("  [LEVERAGE SCOUT] Position response ambiguous — skipping auto-close")
            return

        if not bankr_positions:
            for pos in open_positions:
                try:
                    opened_at = datetime.fromisoformat(pos['timestamp'])
                    if (datetime.now() - opened_at).total_seconds() > 300:
                        print(f"  [LEVERAGE SCOUT] Position #{pos['id']} ({pos['pair']}) appears closed")
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
                            print(f"  [LEVERAGE SCOUT] WARNING: Unknown P&L for #{pos['id']} — marking needs_review")
                            self.tracker.close_avantis_position(
                                position_id=pos['id'],
                                exit_price=0,
                                pnl=0,
                                pnl_pct=0,
                                exit_reason='needs_review'
                            )
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

        bankr_pairs = set()
        for bp in bankr_positions:
            pair_key = bp.get('pair', '') or bp.get('market', '')
            bankr_pairs.add(pair_key.upper())

        for pos in open_positions:
            pair_upper = pos['pair'].upper().replace('/', '')
            still_open = False
            for bp_pair in bankr_pairs:
                if pair_upper in bp_pair.upper().replace('/', '') or bp_pair.upper().replace('/', '') in pair_upper:
                    still_open = True
                    break

            if not still_open:
                try:
                    opened_at = datetime.fromisoformat(pos['timestamp'])
                    if (datetime.now() - opened_at).total_seconds() > 300:
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
                model="deepseek-v3.2",
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
                        model="deepseek-v3.2",
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

                self.data_intake.validate_and_write_resolution(pos['bet_id'], won, profit, self.balance)
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

        result = self.db_reader.fetchone("""
            SELECT COUNT(*) as total, SUM(won) as wins
            FROM bets
            WHERE DATE(timestamp) = DATE('now')
            AND status = 'resolved'
        """)

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
        """V3 Daily Reset at 22:00 UTC — fires hooks, runs Historian, resets counters."""
        now = datetime.now()

        if now.hour == DAILY_RESET_HOUR and (now - self.last_improvement_time).total_seconds() > 3600:
            print(f"\n{'='*60}")
            print(f"V3 DAILY RESET - 22:00 UTC")
            print(f"{'='*60}")

            # Step 10a: Fire PRE_RESET hook (archive pulse data)
            try:
                self.clock.fire_hook('PRE_RESET_HOOK')
                print("  [CLOCK] PRE_RESET hook fired")
            except Exception as e:
                print(f"  [CLOCK] PRE_RESET error: {e}")

            # Step 10b: Run Historian daily analysis
            try:
                self.historian.run_daily_analysis()
                print("  [HISTORIAN] Daily analysis complete")
            except Exception as e:
                print(f"  [HISTORIAN] Daily analysis error: {e}")

            # Step 10c: Fire DAILY_RESET hook (pulse reset, compliance cleanup)
            try:
                self.clock.fire_hook('DAILY_RESET_HOOK')
                print("  [CLOCK] DAILY_RESET hook fired")
            except Exception as e:
                print(f"  [CLOCK] DAILY_RESET error: {e}")

            # Log daily improvement metrics
            improvements = self.tracker.run_daily_improvement()
            today_roi = self.tracker.get_daily_roi()

            if today_roi['total_bets'] > 0:
                print(f"\n[MANAGER] Daily Performance:")
                print(f"  Profit: ${today_roi['profit']:+.2f}")
                print(f"  ROI: {today_roi['roi']:+.1%}")
                print(f"  Target Met: {'YES' if today_roi['met_target'] else 'NO'}")

            # V3 compliance + risk summary
            try:
                compliance_summary = self.compliance.get_daily_summary()
                risk_summary = self.risk_manager.get_risk_summary()
                print(f"  [COMPLIANCE] Today: {compliance_summary.get('approved', 0)} approved, "
                      f"{compliance_summary.get('rejected', 0)} rejected")
                print(f"  [COMPLIANCE] Adaptive caps: {compliance_summary.get('adaptive_caps', {})}")
                print(f"  [RISK] Circuit breakers: {risk_summary.get('circuit_breakers_active', [])}")
                print(f"  [RISK] Recovery mode: {risk_summary.get('recovery_mode', [])}")
            except Exception as e:
                print(f"  [MANAGER] Summary error: {e}")

            # Signals Library summary
            try:
                signals_summary = self.signals.get_summary()
                if signals_summary.get('total_signals', 0) > 0:
                    print(f"  [SIGNALS] Library: {signals_summary['total_signals']} total, "
                          f"{signals_summary.get('positive_outcomes', 0)} positive outcomes")
            except Exception:
                pass

            # Notification
            self.notifier.notify_alert(
                f"V3 DAILY RESET 22:00 UTC\n\n"
                f"Crypto bets today: {self.crypto_daily_bet_count}/{CRYPTO_MAX_DAILY_BETS}\n"
                f"{self.wallet.status_summary()}\n"
                f"Active: {self.wallet.total_position_count()} positions\n"
                f"Historian: daily analysis complete\n"
                f"Counter reset — ready for tomorrow"
            )

            deployed = self.wallet.total_deployed()
            total_value = self.balance + deployed
            self.tracker.save_daily_performance(
                starting_balance=self.starting_daily_balance,
                ending_balance=total_value
            )

            # Reset counters
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

            print(f"\n[MANAGER] Counters reset. Starting balance: ${self.balance:.2f}")
        print(f"Company Clock: {clock_status()}")

    def periodic_balance_check(self):
        """Re-sync wallet balance periodically."""
        self.wallet.periodic_sync()

    # ------------------------------------------------------------------
    # Main loop — V3 Workflow
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
                model="deepseek-v3.2",
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

            # V3: include risk + compliance state
            try:
                risk_summary = self.risk_manager.get_risk_summary()
                compliance_summary = self.compliance.get_daily_summary()
            except Exception:
                risk_summary = {}
                compliance_summary = {}

            status = {
                'timestamp': datetime.now().isoformat(),
                'version': 'v3',
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
                'v3_cycle_count': self._v3_cycle_count,
                'circuit_breakers': risk_summary.get('circuit_breakers_active', []),
                'adaptive_caps': compliance_summary.get('adaptive_caps', {}),
            }

            shared_dir = os.getenv('SHARED_DIR', os.path.expanduser('~/shared'))
            os.makedirs(shared_dir, exist_ok=True)
            with open(os.path.join(shared_dir, 'hedge_fund_status.json'), 'w') as f:
                json.dump(status, f, indent=2)

        except Exception as e:
            pass  # Never crash the agent for status writes

    def run(self):
        """V3 Main Run Loop — 10-step Manager cycle.

        Firing order per V3 Playbook:
        1. V3 pre-trading checks (CFO, Risk, Pulse, Hooks, Liaisons)
        2. Weather cycle (seniority on funds)
        3. Scalper cycle (Up/Down 15-min)
        4. Crypto cycle (6-step bet protocol)
        5. Avantis cycle (leverage trading)
        6. Sports cycle
        7. Settlement Clerk (resolver)
        8. Position checks + heartbeat
        9. Detective review (30h interval)
        10. Summaries, daily reset, shared status
        """
        print(f"\n{'='*60}")
        print("STARTING BAGGINS CAPITAL V3")
        print(f"{'='*60}")

        cycle_count = 0

        try:
            while True:
                cycle_count += 1

                # ══════════════════════════════════════════════════════
                # STEP 0: Market Scout — pre-screen all markets
                # ══════════════════════════════════════════════════════
                try:
                    self._scout_queues = self.scout.scan()
                    # V3.1: Log Scout metadata for Historian
                    try:
                        depths = self.scout.get_queue_depths()
                        drops = self.scout.get_drop_summary()
                        if drops:
                            top_drops = sorted(drops.items(), key=lambda x: x[1], reverse=True)[:3]
                            drop_str = ", ".join(f"{k}: {v}" for k, v in top_drops)
                            print(f"  [SCOUT] Top drop reasons: {drop_str}")
                    except Exception:
                        pass
                except Exception as e:
                    print(f"  [SCOUT] Scan error (falling back to direct fetch): {e}")
                    self._scout_queues = {}

                # ══════════════════════════════════════════════════════
                # V3 STEPS 1-7: Pre-trading checks
                # ══════════════════════════════════════════════════════
                trading_allowed = self._run_v3_pre_trading_checks()
                if not trading_allowed:
                    print("[MANAGER] Trading halted — CFO unavailable")
                    time.sleep(CRYPTO_SCAN_INTERVAL)
                    continue

                # ══════════════════════════════════════════════════════
                # STEP 8: Run eligible departments
                # ══════════════════════════════════════════════════════

                # Weather cycle FIRST — weather gets seniority on funds
                if self.weather_agent:
                    pause_weather = os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.pause_weather'))
                    if not pause_weather:
                        eligible, reason = self._is_department_eligible('weather')
                        if eligible:
                            self.weather_agent.run_weather_cycle(
                                available_balance=self.wallet.available,
                                wallet=self.wallet,
                                scout_weather_events=self.scout.get_weather_events() if self._scout_queues else None,
                                risk_manager=self.risk_manager,
                                compliance=self.compliance,
                                intel_package=self._get_liaison_package('weather'),
                            )
                    elif cycle_count == 1:
                        print("[PAUSED] Weather module paused via .pause_weather file")

                # Scalper cycle (Up/Down 15-min markets)
                if getattr(__import__('hedge_fund_config'), 'ENABLE_UPDOWN_MODULE', False):
                    try:
                        eligible, reason = self._is_department_eligible('updown')
                        if eligible:
                            run_scalper_cycle(
                                bankr=self.bankr,
                                wallet=self.wallet,
                                dry_run=self.dry_run,
                                risk_manager=self.risk_manager,
                                compliance=self.compliance,
                                intel_package=self._get_liaison_package('updown'),
                            )
                    except Exception as e:
                        print(f"[SCALPER] Cycle error: {e}")

                # Crypto cycle — full 6-step bet protocol
                if getattr(__import__('hedge_fund_config'), 'ENABLE_CRYPTO_MODULE', True):
                    pause_crypto = os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.pause_crypto'))
                    if not pause_crypto:
                        self.run_crypto_cycle(scout_queue=self._scout_queues.get('crypto'))
                    elif cycle_count == 1:
                        print("[PAUSED] Crypto module paused via .pause_crypto file")

                # Avantis leverage trading
                if self.avantis_signals and self.avantis_executor:
                    try:
                        self.run_avantis_cycle()
                        self._check_avantis_positions()
                    except Exception as e:
                        print(f"[LEVERAGE SCOUT] Cycle error: {e}")

                # Sports Analyst (Baggins' Buddy)
                if self.sports_analyst:
                    try:
                        eligible, reason = self._is_department_eligible('sports')
                        if eligible:
                            self.sports_analyst.run_sports_cycle(
                                bankr=self.bankr,
                                wallet=self.wallet,
                                dry_run=self.dry_run,
                                scout_queue=self.scout.get_sports_events() if self._scout_queues else None,
                                risk_manager=self.risk_manager,
                                compliance=self.compliance,
                                intel_package=self._get_liaison_package('sports'),
                            )
                    except Exception as e:
                        print(f"[BUDDY] Cycle error: {e}")

                # ══════════════════════════════════════════════════════
                # SETTLEMENT + MONITORING
                # ══════════════════════════════════════════════════════

                # Settlement Clerk: Bankr claim (10 min) + weather data (1h)
                self.resolver.run()

                # Position checks + AI heartbeat
                self._maybe_check_positions()
                self._run_periodic_heartbeat()

                # ══════════════════════════════════════════════════════
                # STEP 9: Detective review (30h interval)
                # ══════════════════════════════════════════════════════
                self._review_detective_findings()
                self._run_pattern_analysis()

                # ══════════════════════════════════════════════════════
                # STEP 10: Summaries, balance, daily reset
                # ══════════════════════════════════════════════════════
                self.periodic_balance_check()
                self.send_results_summary()
                self.check_daily_reset()
                self._write_shared_status()

                # Sleep for the shortest cycle interval
                print(f"\n[MANAGER] V3 cycle #{self._v3_cycle_count} complete. Next in {CRYPTO_SCAN_INTERVAL//60} min...")
                time.sleep(CRYPTO_SCAN_INTERVAL)

        except KeyboardInterrupt:
            print(f"\n\n{'='*60}")
            print("BAGGINS CAPITAL V3 STOPPED")
            print(f"{'='*60}")
            print(f"Total Cycles: {cycle_count}")
            print(f"V3 Cycles: {self._v3_cycle_count}")
            print(f"Final Balance: ${self.balance:.2f}")

            summary = self.db_reader.get_summary()
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
