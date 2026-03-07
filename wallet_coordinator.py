"""
Wallet Coordinator - Central balance & risk manager for Hedge Fund Agent
Single source of truth for both crypto and weather modules.
Replaces per-module balance tracking with unified position registry.
"""

import re
import sqlite3
from datetime import datetime, timedelta


class WalletCoordinator:
    """Central balance + risk manager. Single source of truth for both modules."""

    # Risk config
    MAX_TOTAL_DEPLOYMENT_PCT = 0.70   # 70% cap across all modules
    MIN_RESERVE = 5.0                  # Always keep $5
    MAX_SINGLE_BET_PCT = 0.30          # No bet > 30% of available
    WEATHER_BUDGET_FLOOR_PCT = 0.50    # Reserve 50% of deployable funds for weather
    WALLET_SYNC_HOURS = 4              # Sync every 4h
    MAX_TOTAL_CONCURRENT = 25          # Hard cap all positions (weather-primary)

    def __init__(self, bankr, tracker, starting_balance, dry_run=False):
        self.bankr = bankr
        self.tracker = tracker
        self.dry_run = dry_run

        self._available = float(starting_balance)
        self._starting_daily = float(starting_balance)
        self._positions = {'crypto': [], 'weather': [], 'avantis': []}
        self._last_sync = None
        self._withdrawals_today = 0.0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Risk checks
    # ------------------------------------------------------------------

    def can_bet(self, module, amount):
        """Check if a bet is allowed. Returns (bool, reason)."""
        amount = float(amount)

        # Reserve check
        if self._available - amount < self.MIN_RESERVE:
            return False, f"Would leave balance below ${self.MIN_RESERVE:.2f} reserve"

        # Weather budget floor — crypto/avantis can't starve weather
        if module != 'weather':
            weather_deployed = sum(p.get('_amount', 0) for p in self._positions.get('weather', []))
            total_funds = self._available + self.total_deployed()
            max_deploy = total_funds * self.MAX_TOTAL_DEPLOYMENT_PCT
            weather_floor = max_deploy * self.WEATHER_BUDGET_FLOOR_PCT
            non_weather_ceiling = max_deploy - weather_floor
            non_weather_deployed = self.total_deployed() - weather_deployed
            if non_weather_deployed + amount > non_weather_ceiling:
                return False, f"Non-weather budget ceiling reached (reserving {self.WEATHER_BUDGET_FLOOR_PCT:.0%} for weather)"

        # Single bet size check
        if amount > self._available * self.MAX_SINGLE_BET_PCT:
            return False, f"Bet ${amount:.2f} > {self.MAX_SINGLE_BET_PCT:.0%} of available ${self._available:.2f}"

        # Total deployment check
        total_funds = self._available + self.total_deployed()
        if (self.total_deployed() + amount) > total_funds * self.MAX_TOTAL_DEPLOYMENT_PCT:
            return False, (
                f"Would deploy ${self.total_deployed() + amount:.2f} "
                f"(>{total_funds * self.MAX_TOTAL_DEPLOYMENT_PCT:.2f} = "
                f"{self.MAX_TOTAL_DEPLOYMENT_PCT:.0%} of total)"
            )

        # Concurrent position check
        if self.total_position_count() >= self.MAX_TOTAL_CONCURRENT:
            return False, f"At max {self.MAX_TOTAL_CONCURRENT} total concurrent positions"

        return True, "OK"

    # ------------------------------------------------------------------
    # Fund management
    # ------------------------------------------------------------------

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

        # Remove from position registry
        before = len(self._positions[module])
        self._positions[module] = [
            p for p in self._positions[module]
            if p.get('bet_id') != bet_id
        ]
        removed = before - len(self._positions[module])
        print(f"[CFO] Released ${returned_amount:.2f} from {module} (removed {removed} pos) | Available: ${self._available:.2f}")

    # ------------------------------------------------------------------
    # Position queries
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Startup reload
    # ------------------------------------------------------------------

    def load_positions_from_db(self):
        """Load pending bets from DB on startup. Returns crypto positions list."""
        try:
            conn = sqlite3.connect(self.tracker.db_path)
            c = conn.cursor()
            c.execute("""
                SELECT id, market_id, market_title, amount, side, odds, category, reasoning, cycle_type
                FROM bets WHERE status = 'pending'
            """)
            rows = c.fetchall()
            conn.close()

            self._positions = {'crypto': [], 'weather': [], 'avantis': []}
            total_deducted = 0.0

            for r in rows:
                category = r[6] if len(r) > 6 else 'crypto'
                reasoning = r[7] if len(r) > 7 else ''
                cycle_type = r[8] if len(r) > 8 else 'short'

                # Determine term from cycle_type or reasoning
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

            return list(self._positions['crypto'])

        except Exception as e:
            print(f"[CFO] Failed to load positions: {e}")
            return []

    # ------------------------------------------------------------------
    # Wallet sync
    # ------------------------------------------------------------------

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
                return total_usdc
            else:
                print(f"[CFO] Check failed: {result.get('error')}")
                print(f"[CFO] Using tracked balance: ${self._available:.2f}")
        except Exception as e:
            print(f"[CFO] Error: {e}")
            print(f"[CFO] Using tracked balance: ${self._available:.2f}")
        return self._available

    def get_polymarket_balance(self):
        """Get Polygon USDC balance for weather/crypto betting. Returns float."""
        if self.dry_run:
            return self._available
        
        try:
            result = self.bankr.check_polygon_balance()
            if result.get('success'):
                response = result.get('response', '')
                total_usdc = self._parse_usdc_from_response(response)
                return total_usdc
            else:
                print(f"[CFO] Polygon check failed: {result.get('error')}")
                return self._available
        except Exception as e:
            print(f"[CFO] Error checking Polygon balance: {e}")
            return self._available

    def get_avantis_balance(self):
        """Get Base USDC + ETH balance for Avantis leverage trading. Returns dict."""
        if self.dry_run:
            return {'usdc': self._available, 'eth': 0.0, 'total_for_trading': self._available}
        
        try:
            result = self.bankr.check_base_balance()
            if result.get('success'):
                response = result.get('response', '')
                print(f"[CFO] Base raw response: {response[:200]}")
                usdc = self._parse_usdc_from_response(response)
                eth = self._parse_eth_from_response(response)
                # If parse returned 0 but we know there's funds, use last known
                if usdc == 0 and hasattr(self, '_last_base_usdc') and self._last_base_usdc > 0:
                    print(f"[CFO] Base parse returned $0, using last known: ${self._last_base_usdc:.2f}")
                    usdc = self._last_base_usdc
                elif usdc > 0:
                    self._last_base_usdc = usdc
                return {
                    'usdc': usdc,
                    'eth': eth,
                    'total_for_trading': usdc
                }
            else:
                print(f"[CFO] Base check failed: {result.get('error')}")
                # Fallback to last known
                if hasattr(self, '_last_base_usdc') and self._last_base_usdc > 0:
                    print(f"[CFO] Using last known Base USDC: ${self._last_base_usdc:.2f}")
                    return {'usdc': self._last_base_usdc, 'eth': 0.0, 'total_for_trading': self._last_base_usdc}
                return {'usdc': 0.0, 'eth': 0.0, 'total_for_trading': 0.0}
        except Exception as e:
            print(f"[CFO] Error checking Base balance: {e}")
            return {'usdc': 0.0, 'eth': 0.0, 'total_for_trading': 0.0}

    def periodic_sync(self):
        """Sync if enough time has elapsed."""
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

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

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
            f"Positions: {crypto_count}C + {weather_count}W + {avantis_count}A = {crypto_count + weather_count + avantis_count} | "
            f"Total: ${total:.2f}"
        )

    def record_withdrawal(self, amount, purpose="fund_transfer"):
        """Record a withdrawal so balance sync doesn't treat it as a loss."""
        self._withdrawals_today += amount
        self._starting_daily -= amount
        self._available -= amount
        print(f"[CFO] Withdrawal recorded: ${amount:.2f} ({purpose})")
        print(f"[CFO] Starting daily adjusted: ${self._starting_daily:.2f}")
        print(f"[CFO] Available adjusted: ${self._available:.2f}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_usdc_from_response(response):
        """Extract USDC amount from Bankr response text."""
        total_usdc = 0

        # Pattern 1: "USD Coin (PoS) - 100.709062" or "USDC - 31.669" or "USDC.e - 41.67"
        m = re.search(r'USD\s+Coin\s*\([^)]+\)\s*[-:]\s*(\d+(?:\.\d+)?)', response, re.IGNORECASE)
        if m:
            total_usdc = float(m.group(1))
        
        # Pattern 1b: "USDC - 31.669" or "USDC.e - 41.67"
        if total_usdc == 0:
            m = re.search(r'USDC(?:\.e)?\s*[-:]\s*(\d+(?:\.\d+)?)', response, re.IGNORECASE)
            if m:
                total_usdc = float(m.group(1))

        # Pattern 2: "$31.67" or "$ 31.67"
        if total_usdc == 0:
            amounts = re.findall(r'\$\s*(\d+(?:\.\d+)?)', response)
            if amounts:
                total_usdc = max(float(a) for a in amounts)

        # Pattern 3: "31.67 USDC"
        if total_usdc == 0:
            amounts = re.findall(r'(\d+(?:\.\d+)?)\s*USDC', response, re.IGNORECASE)
            if amounts:
                total_usdc = sum(float(a) for a in amounts)

        # Pattern 4: "total balance ... $31.67"
        if total_usdc == 0:
            m = re.search(r'total\s+balance[^$]*?\$(\d+(?:\.\d+)?)', response, re.IGNORECASE)
            if m:
                total_usdc = float(m.group(1))

        return total_usdc

    @staticmethod
    def _parse_eth_from_response(response):
        """Extract ETH amount from Bankr response text."""
        total_eth = 0

        # Pattern 1: "ETH - 0.5" or "Ethereum - 0.5"
        m = re.search(r'(?:ETH|Ethereum)\s*[-:]\s*(\d+(?:\.\d+)?)', response, re.IGNORECASE)
        if m:
            total_eth = float(m.group(1))
        
        # Pattern 2: "0.5 ETH"
        if total_eth == 0:
            amounts = re.findall(r'(\d+(?:\.\d+)?)\s*ETH', response, re.IGNORECASE)
            if amounts:
                total_eth = sum(float(a) for a in amounts)

        return total_eth
