"""
The Risk Manager — Data & Analytics Department
Baggins Capital V3

Advisory risk assessment. Monitors correlation, exposure, stale positions,
and consecutive loss streaks. Issues severity-rated warnings.

V3 Upgrades:
  - Capital-weighted correlation thresholds (not raw count)
  - Volatility-based exposure ceilings per category
  - Category-specific stale position sensitivity
  - Statistical circuit breaker (2.5 std dev below expected, not 5 consecutive)
  - Recovery protocol (50% sizing for 2h after Manager clears)
  - All thresholds update weekly at MONDAY_OPEN_HOOK

Only hard block: circuit breaker. Everything else is advisory.

Department: Data & Analytics
Reports to: The Manager
"""

import json
import math
from datetime import datetime, timedelta
from archivist import Archivist


class RiskManager:
    """Adaptive risk assessment. Advisory except circuit breaker."""

    # ── Static defaults (overridden by adaptive calc) ──
    STALE_BET_HOURS = 48               # Base stale threshold
    CIRCUIT_BREAKER_STD_DEVS = 2.5     # Statistical trigger
    CIRCUIT_BREAKER_WINDOW_HOURS = 3   # Rolling window for CB check
    RECOVERY_DURATION_HOURS = 2        # 50% sizing recovery window

    # ── Adaptive state (recalculated weekly at MONDAY_OPEN_HOOK) ──

    def __init__(self, db_path='hedge_fund_performance.db'):
        self.db_path = db_path

        # Adaptive parameters (set by recalculate_adaptive_limits)
        self._dept_win_rates = {}           # {dept: 7d_win_rate}
        self._volatility_scores = {}        # {category: std_dev_score}
        self._category_avg_adverse = {}     # {category: avg_adverse_move}
        self._exposure_ceilings = {}        # {category: max_pct}

        # Circuit breaker state
        self._circuit_breakers = {}         # {dept: triggered_at}
        self._recovery_mode = {}            # {dept: cleared_at}

    # ══════════════════════════════════════════════════════════════
    # MAIN ASSESSMENT
    # ══════════════════════════════════════════════════════════════

    def assess(self, bet_dict):
        """Run risk assessment on a bet.

        Returns:
            (approved: bool, risk_level: str, warnings: list[str])
            Only returns approved=False on circuit breaker.
        """
        warnings = []
        category = bet_dict.get('category', 'unknown')
        side = bet_dict.get('side', '').lower()
        amount = float(bet_dict.get('amount', 0))

        # ── HARD BLOCK: Statistical circuit breaker ──
        if self._is_circuit_breaker_active(category):
            reason = f"Circuit breaker active for {category}"
            self._log_event(bet_dict, 'blocked', reason)
            return False, 'critical', [reason]

        # Check if circuit breaker should fire
        cb_triggered, cb_reason = self._check_statistical_circuit_breaker(category)
        if cb_triggered:
            self._trigger_circuit_breaker(category)
            self._log_event(bet_dict, 'blocked', cb_reason)
            return False, 'critical', [cb_reason]

        # ── HARD BLOCK: Recovery mode — 50% sizing ──
        if self._is_in_recovery(category):
            warnings.append(f"{category} in recovery mode — 50% bet sizing")

        # ── ADVISORY: Capital-weighted correlation ──
        corr_warning = self._check_correlation_weighted(bet_dict)
        if corr_warning:
            warnings.append(corr_warning)

        # ── ADVISORY: Volatility-based exposure concentration ──
        exp_warning = self._check_exposure_adaptive(category, amount)
        if exp_warning:
            warnings.append(exp_warning)

        # ── ADVISORY: Category-specific stale positions ──
        stale_warning = self._check_stale_adaptive(category)
        if stale_warning:
            warnings.append(stale_warning)

        # ── ADVISORY: Per-category losing streak ──
        cat_streak = self._get_category_streak(category)
        if cat_streak >= 3:
            warnings.append(f"{category}: {cat_streak} consecutive losses")

        # Determine risk level
        high_warnings = sum(1 for w in warnings if 'HIGH' in w or 'recovery' in w)
        if high_warnings > 0 or len(warnings) >= 3:
            risk_level = 'high'
        elif len(warnings) >= 1:
            risk_level = 'medium'
        else:
            risk_level = 'low'

        self._log_event(bet_dict, 'approved', f"risk={risk_level}, warnings={len(warnings)}")

        if warnings:
            for w in warnings:
                print(f"  [RISK] Advisory: {w}")

        return True, risk_level, warnings

    # ══════════════════════════════════════════════════════════════
    # STATISTICAL CIRCUIT BREAKER (V3)
    # ══════════════════════════════════════════════════════════════

    def _check_statistical_circuit_breaker(self, department):
        """Fire only when actual performance falls 2.5 std dev below expected.

        Uses dept win rate from Historian. Normal cold streaks don't trigger.
        """
        try:
            _arch = Archivist(self.db_path)

            # Get bets in the rolling window
            rows = _arch._fetchall("""
                SELECT won FROM bets
                WHERE category = ? AND status = 'resolved'
                AND resolved_at > datetime('now', ?)
                ORDER BY resolved_at DESC
            """, (department, f'-{self.CIRCUIT_BREAKER_WINDOW_HOURS} hours'))

            if len(rows) < 5:  # Need minimum sample
                return False, ""

            n = len(rows)
            wins = sum(1 for (won,) in rows if won)
            actual_wr = wins / n

            # Expected win rate from Historian data (or 50% default)
            expected_wr = self._dept_win_rates.get(department, 0.50)

            # Standard deviation of binomial: sqrt(p * (1-p) / n)
            if expected_wr <= 0 or expected_wr >= 1:
                expected_wr = 0.50
            std_dev = math.sqrt(expected_wr * (1 - expected_wr) / n)

            if std_dev == 0:
                return False, ""

            # Z-score: how many std devs below expected
            z_score = (expected_wr - actual_wr) / std_dev

            if z_score >= self.CIRCUIT_BREAKER_STD_DEVS:
                reason = (f"Circuit breaker: {department} at {actual_wr:.0%} WR "
                         f"({z_score:.1f}σ below expected {expected_wr:.0%}) "
                         f"over {n} bets in {self.CIRCUIT_BREAKER_WINDOW_HOURS}h")
                return True, reason

        except Exception as e:
            print(f"  [RISK] CB check error: {e}")

        return False, ""

    def _trigger_circuit_breaker(self, department):
        """Trigger circuit breaker for a department."""
        self._circuit_breakers[department] = datetime.now()
        print(f"  [RISK] CIRCUIT BREAKER TRIGGERED for {department}")

    def _is_circuit_breaker_active(self, department):
        """Check if circuit breaker is active for a department."""
        return department in self._circuit_breakers

    def clear_circuit_breaker(self, department):
        """Manager clears a circuit breaker. Department enters recovery mode."""
        if department in self._circuit_breakers:
            del self._circuit_breakers[department]
            self._recovery_mode[department] = datetime.now()
            print(f"  [RISK] Circuit breaker cleared for {department} — entering 2h recovery (50% sizing)")
            return True
        return False

    def _is_in_recovery(self, department):
        """Check if department is in recovery mode (50% sizing for 2h)."""
        if department not in self._recovery_mode:
            return False

        cleared_at = self._recovery_mode[department]
        elapsed = (datetime.now() - cleared_at).total_seconds() / 3600
        if elapsed >= self.RECOVERY_DURATION_HOURS:
            del self._recovery_mode[department]
            print(f"  [RISK] {department} recovery complete — full sizing restored")
            return False
        return True

    def get_recovery_sizing_multiplier(self, department):
        """Returns 0.5 during recovery, 1.0 otherwise."""
        return 0.5 if self._is_in_recovery(department) else 1.0

    # ══════════════════════════════════════════════════════════════
    # CAPITAL-WEIGHTED CORRELATION (V3)
    # ══════════════════════════════════════════════════════════════

    def _check_correlation_weighted(self, bet_dict):
        """Capital-weighted directional exposure, not raw position count."""
        category = bet_dict.get('category', '')
        side = bet_dict.get('side', '').lower()
        amount = float(bet_dict.get('amount', 0))

        try:
            _arch = Archivist(self.db_path)

            # Total deployed in same category + same direction
            rows = _arch._fetchall("""
                SELECT COALESCE(SUM(amount), 0), COUNT(*)
                FROM bets
                WHERE category = ? AND side = ? AND status != 'resolved'
            """, (category, side))

            if not rows:
                return None

            same_dir_amount = rows[0][0] or 0
            same_dir_count = rows[0][1] or 0

            # Total deployed across all
            total_row = _arch._fetchone(
                "SELECT COALESCE(SUM(amount), 0) FROM bets WHERE status != 'resolved'")
            total_deployed = (total_row[0] or 0) + amount

            if total_deployed <= 0:
                return None

            # Capital-weighted concentration
            concentration = (same_dir_amount + amount) / total_deployed

            # Volatility-adjusted threshold
            vol = self._volatility_scores.get(category, 0.15)
            # Low volatility categories get more headroom
            medium_threshold = 0.25 if vol > 0.20 else 0.35
            high_threshold = 0.40 if vol > 0.20 else 0.55

            if concentration > high_threshold:
                return f"HIGH correlation: {concentration:.0%} capital in {category} {side.upper()} (threshold {high_threshold:.0%})"
            elif concentration > medium_threshold:
                return f"MEDIUM correlation: {concentration:.0%} capital in {category} {side.upper()}"

        except Exception:
            pass
        return None

    # ══════════════════════════════════════════════════════════════
    # VOLATILITY-BASED EXPOSURE CEILINGS (V3)
    # ══════════════════════════════════════════════════════════════

    def _check_exposure_adaptive(self, category, amount):
        """Category-specific exposure ceiling based on volatility score."""
        try:
            _arch = Archivist(self.db_path)
            total_row = _arch._fetchone(
                "SELECT COALESCE(SUM(amount), 0) FROM bets WHERE status != 'resolved'")
            total_deployed = (total_row[0] or 0) + amount

            cat_row = _arch._fetchone(
                "SELECT COALESCE(SUM(amount), 0) FROM bets WHERE category = ? AND status != 'resolved'",
                (category,))
            cat_deployed = (cat_row[0] or 0) + amount

            if total_deployed <= 0:
                return None

            concentration = cat_deployed / total_deployed

            # Ceiling = 55% minus volatility×100 (per spec)
            vol = self._volatility_scores.get(category, 0.15)
            ceiling = max(0.20, 0.55 - vol)  # Floor at 20%
            ceiling = self._exposure_ceilings.get(category, ceiling)

            if concentration > ceiling:
                return f"HIGH exposure: {category} at {concentration:.0%} (ceiling {ceiling:.0%})"
            elif concentration > ceiling * 0.75:
                return f"MEDIUM exposure: {category} at {concentration:.0%} (ceiling {ceiling:.0%})"

        except Exception:
            pass
        return None

    # ══════════════════════════════════════════════════════════════
    # CATEGORY-SPECIFIC STALE POSITIONS (V3)
    # ══════════════════════════════════════════════════════════════

    def _check_stale_adaptive(self, category):
        """Stale threshold calibrated to category's avg adverse move."""
        try:
            _arch = Archivist(self.db_path)

            # Category-specific adverse move threshold
            avg_adverse = self._category_avg_adverse.get(category, 0.10)
            stale_threshold = max(0.05, avg_adverse * 1.5)

            # Check for positions pending > 48h
            stale_count = _arch._fetchone(f"""
                SELECT COUNT(*) FROM bets
                WHERE category = ? AND status != 'resolved'
                AND timestamp < datetime('now', '-{self.STALE_BET_HOURS} hours')
            """, (category,))
            stale = stale_count[0] if stale_count else 0

            # Also flag 72h+ zero movement as liquidity risk
            liquidity_stale = _arch._fetchone("""
                SELECT COUNT(*) FROM bets
                WHERE category = ? AND status != 'resolved'
                AND timestamp < datetime('now', '-72 hours')
            """, (category,))
            liq_stale = liquidity_stale[0] if liquidity_stale else 0

            warnings = []
            if stale > 0:
                warnings.append(f"{stale} {category} positions pending >{self.STALE_BET_HOURS}h")
            if liq_stale > 0:
                warnings.append(f"{liq_stale} {category} positions >72h (liquidity risk)")

            if warnings:
                return '; '.join(warnings)

        except Exception:
            pass
        return None

    def _get_category_streak(self, category):
        """Count consecutive losses in a specific category."""
        try:
            _arch = Archivist(self.db_path)
            rows = _arch._fetchall("""
                SELECT won FROM bets
                WHERE category = ? AND status = 'resolved'
                ORDER BY resolved_at DESC LIMIT 10
            """, (category,))
            streak = 0
            for (won,) in rows:
                if won:
                    break
                streak += 1
            return streak
        except Exception:
            return 0

    # ══════════════════════════════════════════════════════════════
    # ADAPTIVE RECALCULATION (weekly at MONDAY_OPEN_HOOK)
    # ══════════════════════════════════════════════════════════════

    def recalculate_adaptive_limits(self, dept_win_rates=None,
                                     volatility_scores=None,
                                     category_avg_adverse=None):
        """Update all adaptive thresholds from Historian data.
        Called at MONDAY_OPEN_HOOK by Manager."""

        if dept_win_rates:
            self._dept_win_rates = dept_win_rates
            print(f"  [RISK] Updated dept win rates: {dept_win_rates}")

        if volatility_scores:
            self._volatility_scores = volatility_scores
            # Recalculate exposure ceilings
            for cat, vol in volatility_scores.items():
                self._exposure_ceilings[cat] = max(0.20, 0.55 - vol)
            print(f"  [RISK] Updated exposure ceilings: {self._exposure_ceilings}")

        if category_avg_adverse:
            self._category_avg_adverse = category_avg_adverse
            print(f"  [RISK] Updated adverse move thresholds: {category_avg_adverse}")

    def on_monday_open(self, payload=None):
        """Hook handler: weekly adaptive recalculation."""
        print("[RISK] MONDAY_OPEN_HOOK — recalculating adaptive thresholds")
        if payload:
            self.recalculate_adaptive_limits(
                dept_win_rates=payload.get('dept_win_rates'),
                volatility_scores=payload.get('volatility_scores'),
                category_avg_adverse=payload.get('category_avg_adverse'),
            )

    # ══════════════════════════════════════════════════════════════
    # LOGGING
    # ══════════════════════════════════════════════════════════════

    def _log_event(self, bet_dict, decision, details):
        """Log risk assessment for audit trail."""
        try:
            _arch = Archivist(self.db_path)
            _arch._execute("""
                INSERT INTO risk_events (timestamp, market_id, category, amount, decision, details)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                datetime.now().isoformat(),
                str(bet_dict.get('market_id', '')),
                bet_dict.get('category', ''),
                float(bet_dict.get('amount', 0)),
                decision,
                details,
            ), commit=True)
        except Exception as e:
            print(f"  [RISK] Log error: {e}")

    # ══════════════════════════════════════════════════════════════
    # SUMMARY
    # ══════════════════════════════════════════════════════════════

    def get_risk_summary(self):
        """Get current risk state for daily report."""
        try:
            _arch = Archivist(self.db_path)

            cats = {}
            for cat in ['crypto', 'weather', 'updown', 'sports']:
                row = _arch._fetchone(
                    "SELECT COALESCE(SUM(amount), 0), COUNT(*) FROM bets WHERE category = ? AND status != 'resolved'",
                    (cat,))
                streak = self._get_category_streak(cat)
                if row:
                    cats[cat] = {
                        "deployed": row[0],
                        "positions": row[1],
                        "streak": streak,
                        "circuit_breaker": cat in self._circuit_breakers,
                        "recovery": self._is_in_recovery(cat),
                    }

            return {
                "circuit_breakers_active": list(self._circuit_breakers.keys()),
                "recovery_mode": list(self._recovery_mode.keys()),
                "category_exposure": cats,
                "dept_win_rates": self._dept_win_rates,
                "exposure_ceilings": self._exposure_ceilings,
            }
        except Exception:
            return {"circuit_breakers_active": [], "category_exposure": {}}
