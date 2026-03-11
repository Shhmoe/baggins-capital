"""
The CFO — Executive Department
Baggins Capital V3

Single source of truth for ALL capital math. Adaptive limits that respond
to live performance data. No other employee calculates balance math.

V3 Upgrades:
  - Deployment cap: 60-80% tiered by 7d blended win rate (weekly at MONDAY_OPEN_HOOK)
  - Reserve floor: max($5, balance × 8%)
  - Bet sizing ceiling: 15% + confidence multiplier
  - Position limit: CLAMP(balance÷8, 15, 40)
  - Full CFOState object on every query
  - Recalculates on 10%+ balance change

Department: Executive
Reports to: The Manager
"""

import re
import math
from datetime import datetime, timedelta


class WalletCoordinator:
    """Adaptive CFO. Single source of capital truth."""

    # ── Hard constants (never change) ──
    ABSOLUTE_MIN_RESERVE = 5.0         # $5 floor — non-negotiable
    WALLET_SYNC_HOURS = 4

    # ── Adaptive defaults (recalculated weekly) ──
    _deployment_cap_pct = 0.70         # Current tier
    _position_limit = 25               # Current limit
    _bet_ceiling_pct = 0.15            # 15% of remaining deployable

    # Deployment cap tiers by 7d blended win rate
    DEPLOYMENT_TIERS = [
        (0.00, 0.45, 0.60, 'CONSERVATIVE'),  # WR < 45% → 60%
        (0.45, 0.55, 0.70, 'NEUTRAL'),        # 45-55% → 70%
        (0.55, 0.65, 0.75, 'GROWTH'),         # 55-65% → 75%
        (0.65, 1.01, 0.80, 'STRONG'),         # 65%+ → 80%
    ]

    def __init__(self, bankr, tracker, starting_balance, dry_run=False):
        self.bankr = bankr
        self.tracker = tracker
        self.dry_run = dry_run

        self._available = float(starting_balance)
        self._starting_daily = float(starting_balance)
        self._positions = {'crypto': [], 'weather': [], 'avantis': []}
        self._last_sync = None
        self._withdrawals_today = 0.0

        # Adaptive state
        self._win_rate_7d = 0.50
        self._deployment_cap_band = 'NEUTRAL'
        self._last_cap_update = None
        self._last_balance_for_recalc = float(starting_balance)

    # ══════════════════════════════════════════════════════════════
    # CFO STATE OBJECT — Full response on every query
    # ══════════════════════════════════════════════════════════════

    def get_state(self):
        """Return full CFOState dict. All four adaptive values included."""
        total = self._available + self.total_deployed()
        reserve = self._calculate_reserve_floor(total)
        available_after_reserve = max(0, self._available - reserve)
        max_deployable = total * self._deployment_cap_pct
        remaining_deployable = max(0, max_deployable - self.total_deployed())
        per_bet_ceiling = remaining_deployable * self._bet_ceiling_pct

        return {
            'total_balance': round(total, 2),
            'reserve_floor': round(reserve, 2),
            'available_capital': round(available_after_reserve, 2),
            'deployment_cap_pct': self._deployment_cap_pct,
            'max_deployable': round(max_deployable, 2),
            'currently_deployed': round(self.total_deployed(), 2),
            'remaining_deployable': round(remaining_deployable, 2),
            'per_bet_ceiling': round(per_bet_ceiling, 2),
            'position_limit': self._position_limit,
            'open_positions': self.total_position_count(),
            'positions_remaining': max(0, self._position_limit - self.total_position_count()),
            'win_rate_7d': self._win_rate_7d,
            'deployment_cap_band': self._deployment_cap_band,
            'last_cap_update': self._last_cap_update,
            'next_cap_review': 'MONDAY_OPEN_HOOK',
        }

    # ══════════════════════════════════════════════════════════════
    # ADAPTIVE RECALCULATION
    # ══════════════════════════════════════════════════════════════

    def recalculate_adaptive_limits(self, win_rate_7d=None):
        """Recalculate all adaptive parameters. Called at MONDAY_OPEN_HOOK
        and on 10%+ balance change."""
        total = self._available + self.total_deployed()

        # Update win rate if provided (from Historian/DBReader)
        if win_rate_7d is not None:
            self._win_rate_7d = win_rate_7d

        # ── Deployment cap tier ──
        for wr_low, wr_high, cap, band in self.DEPLOYMENT_TIERS:
            if wr_low <= self._win_rate_7d < wr_high:
                old_cap = self._deployment_cap_pct
                self._deployment_cap_pct = cap
                self._deployment_cap_band = band
                if old_cap != cap:
                    print(f"[CFO] Deployment cap: {old_cap:.0%} → {cap:.0%} ({band}, WR={self._win_rate_7d:.1%})")
                break

        # ── Position limit: CLAMP(balance÷8, 15, 40) ──
        old_limit = self._position_limit
        self._position_limit = max(15, min(40, int(total / 8)))
        if old_limit != self._position_limit:
            print(f"[CFO] Position limit: {old_limit} → {self._position_limit}")

        self._last_cap_update = datetime.now().isoformat()
        self._last_balance_for_recalc = total

    def _check_balance_triggered_recalc(self):
        """Check if balance changed 10%+ since last recalc — trigger if so."""
        total = self._available + self.total_deployed()
        if self._last_balance_for_recalc > 0:
            change = abs(total - self._last_balance_for_recalc) / self._last_balance_for_recalc
            if change >= 0.10:
                print(f"[CFO] Balance changed {change:.0%} — recalculating adaptive limits")
                self.recalculate_adaptive_limits()

    def on_monday_open(self, payload=None):
        """Hook handler: weekly adaptive recalculation."""
        print("[CFO] MONDAY_OPEN_HOOK — recalculating adaptive limits")
        # Caller should pass win_rate_7d from Historian/DBReader
        wr = payload.get('win_rate_7d', self._win_rate_7d) if payload else self._win_rate_7d
        self.recalculate_adaptive_limits(win_rate_7d=wr)

    # ══════════════════════════════════════════════════════════════
    # RESERVE FLOOR
    # ══════════════════════════════════════════════════════════════

    def _calculate_reserve_floor(self, total_balance):
        """Reserve floor: max($5, balance × 8%)."""
        return max(self.ABSOLUTE_MIN_RESERVE, total_balance * 0.08)

    # ══════════════════════════════════════════════════════════════
    # BET SIZING — 15% ceiling + confidence multiplier
    # ══════════════════════════════════════════════════════════════

    def get_bet_size(self, flat_amount, confidence=None):
        """Calculate actual bet size with confidence multiplier.

        - Ceiling: 15% of remaining deployable
        - Confidence multiplier scales high-conviction bets up to ceiling
        - Low confidence = flat amount unchanged
        - confidence 80+ gets proportional boost
        """
        state = self.get_state()
        ceiling = state['per_bet_ceiling']

        if confidence is None or confidence < 80:
            # Standard flat sizing, capped at ceiling
            return min(flat_amount, ceiling)

        # Confidence multiplier: 80→1.0x, 90→1.25x, 100→1.5x
        multiplier = 1.0 + (confidence - 80) * 0.025
        sized = flat_amount * multiplier
        return min(sized, ceiling)

    # ══════════════════════════════════════════════════════════════
    # RISK CHECKS (V3 — uses adaptive limits)
    # ══════════════════════════════════════════════════════════════

    def can_bet(self, module, amount, confidence=None):
        """Check if a bet is allowed. Returns (bool, reason).
        Uses adaptive deployment cap, reserve floor, position limit."""
        amount = float(amount)
        total = self._available + self.total_deployed()

        # Check for balance-triggered recalc
        self._check_balance_triggered_recalc()

        # Reserve check (adaptive: max($5, 8%))
        reserve = self._calculate_reserve_floor(total)
        if self._available - amount < reserve:
            return False, f"Would leave balance below ${reserve:.2f} reserve"

        # Deployment cap check (adaptive: 60-80%)
        max_deployable = total * self._deployment_cap_pct
        if (self.total_deployed() + amount) > max_deployable:
            return False, (
                f"Would deploy ${self.total_deployed() + amount:.2f} "
                f"(>{max_deployable:.2f} = {self._deployment_cap_pct:.0%} of total)"
            )

        # Bet sizing ceiling (15% of remaining deployable)
        remaining_deployable = max(0, max_deployable - self.total_deployed())
        ceiling = remaining_deployable * self._bet_ceiling_pct
        if amount > ceiling:
            return False, f"Bet ${amount:.2f} > ceiling ${ceiling:.2f} (15% of remaining deployable)"

        # Position limit (adaptive: CLAMP(balance÷8, 15, 40))
        if self.total_position_count() >= self._position_limit:
            return False, f"At max {self._position_limit} concurrent positions"

        return True, "OK"

    # ══════════════════════════════════════════════════════════════
    # FUND MANAGEMENT (unchanged from V2)
    # ══════════════════════════════════════════════════════════════

    def reserve_funds(self, module, amount, position_data):
        """Deduct funds and register a position."""
        amount = float(amount)
        self._available -= amount
        pos = dict(position_data)
        pos['_amount'] = amount
        pos['_module'] = module
        pos['_reserved_at'] = datetime.now().isoformat()
        self._positions[module].append(pos)
        print(f"[CFO] Reserved ${amount:.2f} for {module} | Available: ${self._available:.2f} | Deployed: ${self.total_deployed():.2f}")

    def release_funds(self, module, bet_id, returned_amount):
        """Return funds and deregister a position."""
        returned_amount = float(returned_amount)
        self._available += returned_amount

        before = len(self._positions[module])
        self._positions[module] = [
            p for p in self._positions[module]
            if p.get('bet_id') != bet_id
        ]
        removed = before - len(self._positions[module])
        print(f"[CFO] Released ${returned_amount:.2f} from {module} (removed {removed} pos) | Available: ${self._available:.2f}")

    # ══════════════════════════════════════════════════════════════
    # POSITION QUERIES (unchanged)
    # ══════════════════════════════════════════════════════════════

    @property
    def available(self):
        return self._available

    @available.setter
    def available(self, value):
        self._available = float(value)

    @property
    def starting_daily(self):
        return self._starting_daily

    @starting_daily.setter
    def starting_daily(self, value):
        self._starting_daily = float(value)

    def total_deployed(self):
        total = 0.0
        for module_positions in self._positions.values():
            for p in module_positions:
                total += p.get('_amount', p.get('amount', 0))
        return total

    def module_deployed(self, module):
        return sum(p.get('_amount', p.get('amount', 0)) for p in self._positions.get(module, []))

    def module_positions(self, module):
        return list(self._positions.get(module, []))

    def total_position_count(self):
        return sum(len(positions) for positions in self._positions.values())

    # ══════════════════════════════════════════════════════════════
    # STARTUP RELOAD (unchanged)
    # ══════════════════════════════════════════════════════════════

    def load_positions_from_db(self):
        """Load pending bets from DB on startup. Returns crypto positions list."""
        try:
            rows = self.tracker._fetchall("""
                SELECT id, market_id, market_title, amount, side, odds, category, reasoning, cycle_type
                FROM bets WHERE status = 'pending'
            """)

            self._positions = {'crypto': [], 'weather': [], 'avantis': []}
            total_deducted = 0.0

            for r in rows:
                category = r[6] if len(r) > 6 else 'crypto'
                reasoning = r[7] if len(r) > 7 else ''
                cycle_type = r[8] if len(r) > 8 else 'short'

                if cycle_type in ('rapid', 'short', 'long'):
                    term = cycle_type
                else:
                    term = 'short' if 'short term' in (reasoning or '').lower() else 'long'

                pos = {
                    'bet_id': r[0],
                    'market_id': str(r[1]),
                    'market_title': r[2],
                    'amount': r[3],
                    '_amount': r[3],
                    'side': r[4],
                    'odds': r[5],
                    'category': category,
                    'term': term,
                    'cycle_type': cycle_type or 'short',
                    'placed_at': datetime.now(),
                    '_module': category,
                }

                module = 'weather' if category == 'weather' else 'crypto'
                self._positions[module].append(pos)
                self._available -= r[3]
                total_deducted += r[3]

            total_loaded = self.total_position_count()
            if total_loaded:
                print(f"[CFO] Loaded {total_loaded} positions from DB "
                      f"(crypto: {len(self._positions['crypto'])}, weather: {len(self._positions['weather'])}, avantis: {len(self._positions['avantis'])})")
                print(f"[CFO] Deducted ${total_deducted:.2f} from balance -> ${self._available:.2f}")

            # Initial adaptive calc
            self.recalculate_adaptive_limits()

            return list(self._positions['crypto'])

        except Exception as e:
            print(f"[CFO] Failed to load positions: {e}")
            return []

    # ══════════════════════════════════════════════════════════════
    # WALLET SYNC (unchanged)
    # ══════════════════════════════════════════════════════════════

    def sync_with_wallet(self, update_starting=True):
        """Sync available balance with actual Polygon USDC via Bankr."""
        if self.dry_run:
            return self._available

        deployed = self.total_deployed()
        try:
            result = self.bankr.check_polygon_balance()
            if result.get('success'):
                response = result.get('response', '')
                print(f"[CFO] Bankr: {response[:400]}")

                total_usdc = self._parse_usdc_from_response(response)

                self._available = total_usdc
                if update_starting:
                    self._starting_daily = total_usdc + deployed

                self._last_sync = datetime.now()

                print(f"[CFO] Available USDC: ${total_usdc:.2f}")
                print(f"[CFO] Deployed in bets: ${deployed:.2f}")
                print(f"[CFO] Total tracked: ${total_usdc + deployed:.2f}")

                # Check if balance change triggers adaptive recalc
                self._check_balance_triggered_recalc()

                return total_usdc
            else:
                print(f"[CFO] Check failed: {result.get('error')}")
                print(f"[CFO] Using tracked balance: ${self._available:.2f}")
        except Exception as e:
            print(f"[CFO] Error: {e}")
            print(f"[CFO] Using tracked balance: ${self._available:.2f}")
        return self._available

    def get_polymarket_balance(self):
        """Get Polygon USDC balance for weather/crypto betting."""
        if self.dry_run:
            return self._available
        try:
            result = self.bankr.check_polygon_balance()
            if result.get('success'):
                response = result.get('response', '')
                return self._parse_usdc_from_response(response)
            else:
                print(f"[CFO] Polygon check failed: {result.get('error')}")
                return self._available
        except Exception as e:
            print(f"[CFO] Error checking Polygon balance: {e}")
            return self._available

    def get_avantis_balance(self):
        """Get Base USDC + ETH balance for Avantis leverage trading."""
        if self.dry_run:
            return {'usdc': self._available, 'eth': 0.0, 'total_for_trading': self._available}
        try:
            result = self.bankr.check_base_balance()
            if result.get('success'):
                response = result.get('response', '')
                usdc = self._parse_usdc_from_response(response)
                eth = self._parse_eth_from_response(response)
                if usdc == 0 and hasattr(self, '_last_base_usdc') and self._last_base_usdc > 0:
                    usdc = self._last_base_usdc
                elif usdc > 0:
                    self._last_base_usdc = usdc
                return {'usdc': usdc, 'eth': eth, 'total_for_trading': usdc}
            else:
                print(f"[CFO] Base check failed: {result.get('error')}")
                if hasattr(self, '_last_base_usdc') and self._last_base_usdc > 0:
                    return {'usdc': self._last_base_usdc, 'eth': 0.0, 'total_for_trading': self._last_base_usdc}
                return {'usdc': 0.0, 'eth': 0.0, 'total_for_trading': 0.0}
        except Exception as e:
            print(f"[CFO] Error checking Base balance: {e}")
            return {'usdc': 0.0, 'eth': 0.0, 'total_for_trading': 0.0}

    def periodic_sync(self):
        if self.dry_run:
            return
        if self._last_sync is None:
            return
        hours_since = (datetime.now() - self._last_sync).total_seconds() / 3600
        if hours_since >= self.WALLET_SYNC_HOURS:
            print(f"\n[CFO SYNC] {hours_since:.1f}h since last balance check - refreshing...")
            self.sync_with_wallet(update_starting=False)

    def daily_reset(self):
        """Reset starting balance for new day's ROI calculation."""
        self._withdrawals_today = 0.0
        if not self.dry_run:
            self.sync_with_wallet(update_starting=True)
        else:
            self._starting_daily = self._available

    # ══════════════════════════════════════════════════════════════
    # STATUS
    # ══════════════════════════════════════════════════════════════

    def status_summary(self):
        """Human-readable status line for logs."""
        crypto_count = len(self._positions['crypto'])
        weather_count = len(self._positions['weather'])
        avantis_count = len(self._positions.get('avantis', []))
        deployed = self.total_deployed()
        total = self._available + deployed
        pct = (deployed / total * 100) if total > 0 else 0
        return (
            f"Available: ${self._available:.2f} | "
            f"Deployed: ${deployed:.2f} ({pct:.0f}%) | "
            f"Positions: {crypto_count}C + {weather_count}W + {avantis_count}A = {crypto_count + weather_count + avantis_count}/{self._position_limit} | "
            f"Cap: {self._deployment_cap_pct:.0%} ({self._deployment_cap_band}) | "
            f"Total: ${total:.2f}"
        )

    def record_withdrawal(self, amount, purpose="fund_transfer"):
        self._withdrawals_today += amount
        self._starting_daily -= amount
        self._available -= amount
        print(f"[CFO] Withdrawal recorded: ${amount:.2f} ({purpose})")
        print(f"[CFO] Starting daily adjusted: ${self._starting_daily:.2f}")
        print(f"[CFO] Available adjusted: ${self._available:.2f}")

    # ══════════════════════════════════════════════════════════════
    # HELPERS (unchanged)
    # ══════════════════════════════════════════════════════════════

    @staticmethod
    def _parse_usdc_from_response(response):
        total_usdc = 0
        m = re.search(r'USD\s+Coin\s*\([^)]+\)\s*[-:]\s*(\d+(?:\.\d+)?)', response, re.IGNORECASE)
        if m:
            total_usdc = float(m.group(1))
        if total_usdc == 0:
            m = re.search(r'USDC(?:\.e)?\s*[-:]\s*(\d+(?:\.\d+)?)', response, re.IGNORECASE)
            if m:
                total_usdc = float(m.group(1))
        if total_usdc == 0:
            amounts = re.findall(r'\$\s*(\d+(?:\.\d+)?)', response)
            if amounts:
                total_usdc = max(float(a) for a in amounts)
        if total_usdc == 0:
            amounts = re.findall(r'(\d+(?:\.\d+)?)\s*USDC', response, re.IGNORECASE)
            if amounts:
                total_usdc = sum(float(a) for a in amounts)
        if total_usdc == 0:
            m = re.search(r'total\s+balance[^$]*?\$(\d+(?:\.\d+)?)', response, re.IGNORECASE)
            if m:
                total_usdc = float(m.group(1))
        return total_usdc

    @staticmethod
    def _parse_eth_from_response(response):
        total_eth = 0
        m = re.search(r'(?:ETH|Ethereum)\s*[-:]\s*(\d+(?:\.\d+)?)', response, re.IGNORECASE)
        if m:
            total_eth = float(m.group(1))
        if total_eth == 0:
            amounts = re.findall(r'(\d+(?:\.\d+)?)\s*ETH', response, re.IGNORECASE)
            if amounts:
                total_eth = sum(float(a) for a in amounts)
        return total_eth
