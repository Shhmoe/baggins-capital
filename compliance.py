"""
The Compliance Officer — Operations Department
Baggins Capital V3

Hard gate for ALL bet validation. pre_flight() called before every placement.
Binary pass or reject — not advisory. No bet reaches the Banker without passing.

V3 Upgrades:
  - Performance-scaled daily caps (0.5x-1.5x multiplier by dept 7d WR)
  - Resolution-aware dedup window (30min to same-day based on market speed)
  - Blocklist auto-expiry by reason category
  - Hard floor caps that can never go below minimum

Department: Operations
Reports to: The Manager
"""

import json
from datetime import datetime
from archivist import Archivist
from signals_library import SignalsLibrary


class ComplianceOfficer:
    """Adaptive pre-flight validation. Hard gate, smart intelligence."""

    # ── Hard floor caps — can NEVER go below these ──
    CAP_FLOORS = {
        'crypto': 15,
        'weather': 5,
        'updown': 4,
        'sports': 2,
    }

    # ── Base caps (from config, scaled by performance) ──
    BASE_CAPS = {
        'crypto': 25,
        'weather': 30,
        'updown': 8,
        'sports': 3,
    }

    # Cap multiplier tiers by 7d department win rate
    CAP_TIERS = [
        (0.00, 0.35, 0.50),   # WR < 35% → 50% of base cap
        (0.35, 0.45, 0.75),   # 35-45% → 75%
        (0.45, 0.55, 1.00),   # 45-55% → 100% (neutral)
        (0.55, 0.65, 1.25),   # 55-65% → 125%
        (0.65, 1.01, 1.50),   # 65%+ → 150%
    ]

    # Blocklist reason categories and their default expiry behavior
    EXPIRY_RULES = {
        'PERMANENT': None,          # Never auto-expires
        'RISK': None,               # Manager must clear
        'TECHNICAL': 24,            # 24h auto-expiry
        'LIQUIDITY': 48,            # 48h auto-expiry
        'CALIBRATION': 168,         # 7 days auto-expiry
    }

    def __init__(self, db_path='hedge_fund_performance.db'):
        self.db_path = db_path
        self._signals = SignalsLibrary(db_path)

        # Adaptive state (recalculated weekly at MONDAY_OPEN_HOOK)
        self._dept_win_rates = {}     # {dept: 7d_wr}
        self._dept_bet_count = {}     # {dept: 7d_bet_count}
        self._adaptive_caps = dict(self.BASE_CAPS)  # Current active caps

    # ══════════════════════════════════════════════════════════════
    # PRE-FLIGHT — the hard gate
    # ══════════════════════════════════════════════════════════════

    def pre_flight(self, bet_dict):
        """Run all pre-flight checks. Returns (approved, reason, warnings).

        Three checks in order:
        1. Deduplication (resolution-aware)
        2. Daily cap (performance-scaled)
        3. Blocklist (auto-expiry aware)

        On REJECT: log and do not retry same market same cycle.
        """
        warnings = []
        category = bet_dict.get('category', 'unknown')
        market_id = str(bet_dict.get('market_id', ''))
        market_title = bet_dict.get('market_title', '')
        amount = float(bet_dict.get('amount', 0))

        # ── CHECK 1: Resolution-aware deduplication ──
        is_dup, dup_reason = self._check_duplicate_adaptive(market_id, market_title, category)
        if is_dup:
            self._log_decision(bet_dict, False, dup_reason)
            return False, dup_reason, []

        # ── CHECK 2: Performance-scaled daily cap ──
        over_cap, cap_reason = self._check_daily_cap_adaptive(category)
        if over_cap:
            self._log_decision(bet_dict, False, cap_reason)
            return False, cap_reason, []

        # ── CHECK 3: Auto-expiry blocklist ──
        blocked, block_reason = self._check_blocklist_adaptive(market_id, market_title, category)
        if blocked:
            self._log_decision(bet_dict, False, block_reason)
            return False, block_reason, []

        # ── ADVISORY warnings ──
        odds = bet_dict.get('odds', 0)
        if odds and odds < 0.03:
            warnings.append(f"Very low odds ({odds:.1%}) — thin market, slippage risk")

        edge = bet_dict.get('edge', 0)
        if edge > 0.50:
            warnings.append(f"Edge {edge:.0%} is very high — verify market isn't stale")

        # ── CHECK 4: Signals Library — DISABLED until manual review ──
        # Signals Library check completely disabled. Do not re-enable
        # without explicit user approval.
        # try:
        # category = bet_dict.get("category", "unknown")
        # market_title = bet_dict.get("market_title", "")
        # signal_hit = self._signals.get_category_warnings(category)
        # if signal_hit and signal_hit.get("active"):
        # confidence = signal_hit.get("confidence", 0)
        # if confidence > 0.9:
        # reason = f"Signals Library: {signal_hit.get("root_cause", "known bad pattern")} (conf={confidence:.0%})"
        # self._log_decision(bet_dict, False, reason)
        # return False, reason, warnings
        # elif confidence > 0.5:
        # warnings.append(f"Signal advisory: {signal_hit.get("root_cause", "?")} (conf={confidence:.0%})")
        # except Exception:
        # pass  # Signals check is advisory, never block on error

        self._log_decision(bet_dict, True, "approved", warnings)

        if warnings:
            for w in warnings:
                print(f"  [COMPLIANCE] Warning: {w}")

        return True, "approved", warnings

    # ══════════════════════════════════════════════════════════════
    # CHECK 1: Resolution-aware dedup (V3)
    # ══════════════════════════════════════════════════════════════

    def _check_duplicate_adaptive(self, market_id, market_title, category):
        """Resolution-aware dedup window.

        - Open position on same market = ALWAYS blocked
        - Resolved position: dedup window scales by category speed
          - updown/crypto (fast): 30min after resolution
          - weather: 6h after resolution
          - sports: same-day (24h)
        """
        try:
            _arch = Archivist(self.db_path)

            # Unresolved bet on same market_id — ALWAYS blocked
            row = _arch._fetchone(
                "SELECT COUNT(*) FROM bets WHERE market_id = ? AND status != 'resolved'",
                (market_id,))
            if row and row[0] > 0:
                return True, f"Duplicate: unresolved bet on market {market_id}"

            # Resolution-aware window by category speed
            dedup_windows = {
                'crypto': '-30 minutes',
                'updown': '-30 minutes',
                'weather': '-6 hours',
                'sports': '-24 hours',
            }
            window = dedup_windows.get(category, '-72 hours')

            row = _arch._fetchone("""
                SELECT COUNT(*) FROM bets
                WHERE market_id = ?
                AND status = 'resolved'
                AND resolved_at > datetime('now', ?)
            """, (market_id, window))
            if row and row[0] > 0:
                return True, f"Duplicate: resolved bet on market {market_id} within dedup window"

            # Unresolved same-day bet on same market (any status)
            row = _arch._fetchone("""
                SELECT COUNT(*) FROM bets
                WHERE market_id = ?
                AND timestamp > datetime('now', '-24 hours')
                AND status != 'resolved'
            """, (market_id,))
            if row and row[0] > 0:
                return True, f"Duplicate: pending bet on market {market_id} today"

        except Exception as e:
            print(f"  [COMPLIANCE] Dedup check error: {e}")
        return False, ""

    # ══════════════════════════════════════════════════════════════
    # CHECK 2: Performance-scaled daily caps (V3)
    # ══════════════════════════════════════════════════════════════

    def _check_daily_cap_adaptive(self, category):
        """Daily cap scaled by 7d department win rate."""
        cap = self._adaptive_caps.get(category)
        if cap is None:
            return False, ""

        try:
            _arch = Archivist(self.db_path)
            row = _arch._fetchone("""
                SELECT COUNT(*) FROM bets
                WHERE category = ?
                AND timestamp > datetime('now', '-24 hours')
            """, (category,))
            count = row[0] if row else 0

            if count >= cap:
                return True, f"Daily cap reached: {count}/{cap} {category} bets today"

        except Exception as e:
            print(f"  [COMPLIANCE] Daily cap check error: {e}")
        return False, ""

    # ══════════════════════════════════════════════════════════════
    # CHECK 3: Auto-expiry blocklist (V3)
    # ══════════════════════════════════════════════════════════════

    def _check_blocklist_adaptive(self, market_id, market_title, category):
        """Blocklist with auto-expiry by reason category."""
        try:
            _arch = Archivist(self.db_path)

            # Check market_id — only active and non-expired entries
            row = _arch._fetchone("""
                SELECT reason, reason_category, expires_at FROM compliance_blocklist
                WHERE block_type = 'market_id' AND block_value = ? AND active = 1
            """, (market_id,))
            if row:
                if self._is_entry_expired(row):
                    # Auto-expire it
                    self._expire_blocklist_entry(market_id, 'market_id')
                else:
                    return True, f"Blocklisted market: {row[0]}"

            # Check category
            row = _arch._fetchone("""
                SELECT reason FROM compliance_blocklist
                WHERE block_type = 'category' AND block_value = ? AND active = 1
            """, (category,))
            if row:
                return True, f"Blocklisted category: {row[0]}"

            # Check title keywords
            if market_title:
                rows = _arch._fetchall(
                    "SELECT block_value, reason FROM compliance_blocklist WHERE block_type = 'keyword' AND active = 1")
                for kw_row in rows:
                    if kw_row[0].lower() in market_title.lower():
                        return True, f"Blocklisted keyword '{kw_row[0]}': {kw_row[1]}"

        except Exception as e:
            print(f"  [COMPLIANCE] Blocklist check error: {e}")
        return False, ""

    def _is_entry_expired(self, row):
        """Check if a blocklist entry has expired based on its reason_category."""
        reason_category = row[1] if len(row) > 1 else 'PERMANENT'
        expires_at = row[2] if len(row) > 2 else None

        if reason_category in ('PERMANENT', 'RISK'):
            return False  # Never auto-expires

        if expires_at:
            try:
                exp_dt = datetime.fromisoformat(expires_at)
                return datetime.now() > exp_dt
            except (ValueError, TypeError):
                pass

        return False

    def _expire_blocklist_entry(self, block_value, block_type):
        """Auto-expire a blocklist entry."""
        try:
            _arch = Archivist(self.db_path)
            _arch._execute("""
                UPDATE compliance_blocklist SET active = 0
                WHERE block_type = ? AND block_value = ?
            """, (block_type, block_value), commit=True)
            print(f"  [COMPLIANCE] Auto-expired blocklist: {block_type}={block_value}")
        except Exception as e:
            print(f"  [COMPLIANCE] Expire error: {e}")

    # ══════════════════════════════════════════════════════════════
    # ADAPTIVE RECALCULATION (weekly at MONDAY_OPEN_HOOK)
    # ══════════════════════════════════════════════════════════════

    def recalculate_adaptive_caps(self, dept_win_rates=None, dept_bet_counts=None):
        """Recalculate daily caps based on 7d department performance.
        Called at MONDAY_OPEN_HOOK by Manager."""

        if dept_win_rates:
            self._dept_win_rates = dept_win_rates
        if dept_bet_counts:
            self._dept_bet_count = dept_bet_counts

        for dept, base_cap in self.BASE_CAPS.items():
            wr = self._dept_win_rates.get(dept, 0.50)
            bet_count = self._dept_bet_count.get(dept, 0)

            # Need minimum 5 bets in 7d to adjust
            if bet_count < 5:
                self._adaptive_caps[dept] = base_cap
                continue

            # Find multiplier tier
            multiplier = 1.0
            for wr_low, wr_high, mult in self.CAP_TIERS:
                if wr_low <= wr < wr_high:
                    multiplier = mult
                    break

            new_cap = max(self.CAP_FLOORS[dept], int(base_cap * multiplier))
            old_cap = self._adaptive_caps.get(dept, base_cap)

            if new_cap != old_cap:
                print(f"  [COMPLIANCE] {dept} daily cap: {old_cap} → {new_cap} (WR={wr:.0%}, mult={multiplier}x)")

            self._adaptive_caps[dept] = new_cap

    def on_monday_open(self, payload=None):
        """Hook handler: weekly cap recalculation."""
        print("[COMPLIANCE] MONDAY_OPEN_HOOK — recalculating adaptive caps")
        if payload:
            self.recalculate_adaptive_caps(
                dept_win_rates=payload.get('dept_win_rates'),
                dept_bet_counts=payload.get('dept_bet_counts'),
            )

    def cleanup_expired_blocklist(self):
        """Clean up expired blocklist entries. Called at DAILY_RESET_HOOK."""
        try:
            _arch = Archivist(self.db_path)

            # Find auto-expirable entries
            rows = _arch._fetchall("""
                SELECT id, block_type, block_value, reason_category, expires_at
                FROM compliance_blocklist
                WHERE active = 1 AND reason_category IN ('TECHNICAL', 'LIQUIDITY', 'CALIBRATION')
            """)

            expired = 0
            for row in rows:
                expires_at = row[4]
                if expires_at:
                    try:
                        if datetime.now() > datetime.fromisoformat(expires_at):
                            _arch._execute(
                                "UPDATE compliance_blocklist SET active = 0 WHERE id = ?",
                                (row[0],), commit=True)
                            expired += 1
                    except (ValueError, TypeError):
                        pass

            if expired:
                print(f"  [COMPLIANCE] Expired {expired} blocklist entries at daily reset")

        except Exception as e:
            print(f"  [COMPLIANCE] Cleanup error: {e}")

    def on_daily_reset(self, payload=None):
        """Hook handler: daily cap reset + blocklist cleanup."""
        self.cleanup_expired_blocklist()

    # ══════════════════════════════════════════════════════════════
    # BLOCKLIST MANAGEMENT
    # ══════════════════════════════════════════════════════════════

    def add_to_blocklist(self, block_type, block_value, reason,
                          added_by='system', reason_category='PERMANENT',
                          expires_at=None):
        """Add item to blocklist with reason category and optional expiry."""
        try:
            _arch = Archivist(self.db_path)

            # Auto-calculate expiry if not provided
            if expires_at is None and reason_category in self.EXPIRY_RULES:
                hours = self.EXPIRY_RULES[reason_category]
                if hours:
                    expires_at = (datetime.now() + __import__('datetime').timedelta(hours=hours)).isoformat()

            _arch._execute("""
                INSERT OR REPLACE INTO compliance_blocklist
                    (block_type, block_value, reason, added_by, reason_category, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (block_type, block_value, reason, added_by,
                  reason_category, expires_at), commit=True)
            print(f"  [COMPLIANCE] Blocklisted {block_type}={block_value}: {reason} ({reason_category})")
        except Exception as e:
            print(f"  [COMPLIANCE] Blocklist add error: {e}")

    # ══════════════════════════════════════════════════════════════
    # LOGGING & SUMMARY
    # ══════════════════════════════════════════════════════════════

    def _log_decision(self, bet_dict, approved, reason, warnings=None):
        try:
            _arch = Archivist(self.db_path)
            _arch._execute("""
                INSERT INTO compliance_log (market_id, category, amount, approved, reason, warnings)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                str(bet_dict.get('market_id', '')),
                bet_dict.get('category', ''),
                float(bet_dict.get('amount', 0)),
                1 if approved else 0,
                reason,
                json.dumps(warnings) if warnings else None,
            ), commit=True)
        except Exception as e:
            print(f"  [COMPLIANCE] Log error: {e}")

    def get_daily_summary(self):
        try:
            _arch = Archivist(self.db_path)
            total = _arch._fetchone(
                "SELECT COUNT(*) FROM compliance_log WHERE timestamp > datetime('now', '-24 hours')")
            approved = _arch._fetchone(
                "SELECT COUNT(*) FROM compliance_log WHERE timestamp > datetime('now', '-24 hours') AND approved = 1")
            rejected = _arch._fetchone(
                "SELECT COUNT(*) FROM compliance_log WHERE timestamp > datetime('now', '-24 hours') AND approved = 0")
            return {
                "total_checks": total[0] if total else 0,
                "approved": approved[0] if approved else 0,
                "rejected": rejected[0] if rejected else 0,
                "adaptive_caps": dict(self._adaptive_caps),
            }
        except Exception:
            return {"total_checks": 0, "approved": 0, "rejected": 0}
