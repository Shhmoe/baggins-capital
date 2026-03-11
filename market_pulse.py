"""
The Market Pulse Analyst — Data & Analytics Department
Baggins Capital V3

Maintains real-time intelligence feed. Updates every cycle (30min max staleness).
Tracks live metrics: win rates, streaks, cap progress, circuit breaker proximity,
modifier drift, exposure concentration. Writes to pulse_insights table.

No deep analysis. No calibration curves. No source rankings.
That's the Historian's job. Pulse = fast-moving operational signals only.

Department: Data & Analytics
Reports to: The Historian
"""

from datetime import datetime
from db_reader import DBReader
from db_writer import DBWriter
from company_clock import get_context
import hedge_fund_config as config


class MarketPulse:
    """Real-time operational metrics. Updated every cycle."""

    def __init__(self, db_path='hedge_fund_performance.db'):
        self._reader = DBReader(db_path)
        self._writer = DBWriter(db_path)
        self._last_update = None

    def update(self):
        """Run full pulse update. Called every Manager cycle."""
        ctx = get_context()
        now = ctx.timestamp_et

        self._update_department_win_rates()
        self._update_streaks()
        self._update_cap_progress()
        self._update_circuit_breaker_proximity()
        self._update_exposure_concentration()
        self._update_modifier_drift_flags()

        self._last_update = now
        return True

    # ══════════════════════════════════════════════════════════════
    # ROLLING WIN RATES (last 50 bets per department)
    # ══════════════════════════════════════════════════════════════

    def _update_department_win_rates(self):
        """Rolling win rate per department over last 50 resolved bets."""
        departments = ['crypto', 'weather', 'sports', 'updown']

        for dept in departments:
            try:
                rows = self._reader.fetchall("""
                    SELECT won FROM bets
                    WHERE category = ? AND status = 'resolved'
                    ORDER BY resolved_at DESC LIMIT 50
                """, (dept,))

                if not rows:
                    continue

                total = len(rows)
                wins = sum(1 for r in rows if r[0])
                wr = round(wins / total, 3) if total else 0

                self._writer.write_pulse_insight(
                    department=dept,
                    metric_type='rolling_win_rate',
                    value=wr,
                    details={'wins': wins, 'total': total, 'window': 'last_50'}
                )
            except Exception as e:
                print(f"  [PULSE] Win rate error ({dept}): {e}")

        # Blended across all departments
        try:
            blended = self._reader.get_blended_win_rate(days=7)
            self._writer.write_pulse_insight(
                department='all',
                metric_type='blended_win_rate',
                value=blended,
                details={'window': '7d'}
            )
        except Exception as e:
            print(f"  [PULSE] Blended WR error: {e}")

    # ══════════════════════════════════════════════════════════════
    # CURRENT STREAKS
    # ══════════════════════════════════════════════════════════════

    def _update_streaks(self):
        """Current win/loss streak per department."""
        departments = ['crypto', 'weather', 'sports', 'updown']

        for dept in departments:
            try:
                rows = self._reader.fetchall("""
                    SELECT won FROM bets
                    WHERE category = ? AND status = 'resolved'
                    ORDER BY resolved_at DESC LIMIT 20
                """, (dept,))

                if not rows:
                    continue

                # Count streak from most recent
                streak_type = 'win' if rows[0][0] else 'loss'
                streak_count = 0
                for (won,) in rows:
                    if (won and streak_type == 'win') or (not won and streak_type == 'loss'):
                        streak_count += 1
                    else:
                        break

                self._writer.write_pulse_insight(
                    department=dept,
                    metric_type='current_streak',
                    value=streak_count if streak_type == 'win' else -streak_count,
                    details={'type': streak_type, 'count': streak_count}
                )
            except Exception as e:
                print(f"  [PULSE] Streak error ({dept}): {e}")

    # ══════════════════════════════════════════════════════════════
    # DAILY CAP PROGRESS
    # ══════════════════════════════════════════════════════════════

    def _update_cap_progress(self):
        """Bets placed today vs daily cap per department."""
        caps = {
            'crypto': getattr(config, 'CRYPTO_MAX_DAILY_BETS', 20),
            'weather': getattr(config, 'WEATHER_MAX_DAILY_BETS', 30),
            'updown': getattr(config, 'UPDOWN_MAX_DAILY', 8),
            'sports': getattr(config, 'SPORTS_MAX_DAILY', 3),
        }

        for dept, cap in caps.items():
            try:
                placed = self._reader.get_daily_bet_count(category=dept)
                remaining = max(0, cap - placed)
                progress = round(placed / cap, 2) if cap > 0 else 1.0

                self._writer.write_pulse_insight(
                    department=dept,
                    metric_type='daily_cap_progress',
                    value=progress,
                    details={
                        'placed': placed,
                        'cap': cap,
                        'remaining': remaining,
                    }
                )
            except Exception as e:
                print(f"  [PULSE] Cap progress error ({dept}): {e}")

    # ══════════════════════════════════════════════════════════════
    # CIRCUIT BREAKER PROXIMITY
    # ══════════════════════════════════════════════════════════════

    def _update_circuit_breaker_proximity(self):
        """Flag departments approaching circuit breaker (5 consecutive losses)."""
        departments = ['crypto', 'weather', 'sports', 'updown']
        cb_threshold = getattr(config, 'CIRCUIT_BREAKER_THRESHOLD', 5)

        for dept in departments:
            try:
                consec_losses = self._reader.get_consecutive_losses(department=dept)
                proximity = consec_losses / cb_threshold if cb_threshold else 0

                # Flag at 3+ losses (one or two away from trigger)
                is_warning = consec_losses >= (cb_threshold - 2)
                is_triggered = consec_losses >= cb_threshold

                self._writer.write_pulse_insight(
                    department=dept,
                    metric_type='circuit_breaker_proximity',
                    value=proximity,
                    details={
                        'consecutive_losses': consec_losses,
                        'threshold': cb_threshold,
                        'warning': is_warning,
                        'triggered': is_triggered,
                    }
                )
            except Exception as e:
                print(f"  [PULSE] CB proximity error ({dept}): {e}")

    # ══════════════════════════════════════════════════════════════
    # EXPOSURE CONCENTRATION
    # ══════════════════════════════════════════════════════════════

    def _update_exposure_concentration(self):
        """Current capital exposure per department as % of pending bets."""
        departments = ['crypto', 'weather', 'sports', 'updown']

        try:
            total_pending = 0
            dept_exposure = {}

            for dept in departments:
                pending = self._reader.get_pending_bets(category=dept)
                exposure = sum(b.get('amount', 0) for b in pending) if pending else 0
                dept_exposure[dept] = exposure
                total_pending += exposure

            for dept in departments:
                exposure = dept_exposure[dept]
                pct = round(exposure / total_pending, 3) if total_pending > 0 else 0

                self._writer.write_pulse_insight(
                    department=dept,
                    metric_type='exposure_concentration',
                    value=pct,
                    details={
                        'exposure_usd': round(exposure, 2),
                        'total_pending_usd': round(total_pending, 2),
                    }
                )
        except Exception as e:
            print(f"  [PULSE] Exposure error: {e}")

    # ══════════════════════════════════════════════════════════════
    # MODIFIER DRIFT FLAGS
    # ══════════════════════════════════════════════════════════════

    def _update_modifier_drift_flags(self):
        """Flag when live performance diverges >0.1 from Historian calibration.

        Compares rolling WR against Historian's last calibrated values.
        If drift > 0.1, flag for Liaison to tell trader to use flat modifiers.
        """
        departments = ['crypto', 'updown']  # Only depts with modifiers

        for dept in departments:
            try:
                # Get Historian's last calibrated expected WR
                rec = self._reader.get_latest_historian_recommendation(
                    dept, 'modifier_drift')

                if not rec:
                    continue

                expected_wr = rec.get('confidence', 0.50)  # Historian stores expected WR as confidence

                # Get live rolling WR
                rows = self._reader.fetchall("""
                    SELECT won FROM bets
                    WHERE category = ? AND status = 'resolved'
                    ORDER BY resolved_at DESC LIMIT 30
                """, (dept,))

                if len(rows) < 10:
                    continue

                live_wr = sum(1 for r in rows if r[0]) / len(rows)
                drift = abs(live_wr - expected_wr)

                self._writer.write_pulse_insight(
                    department=dept,
                    metric_type='modifier_drift',
                    value=drift,
                    details={
                        'live_wr': round(live_wr, 3),
                        'expected_wr': round(expected_wr, 3),
                        'drift': round(drift, 3),
                        'flagged': drift > 0.1,
                    }
                )
            except Exception as e:
                print(f"  [PULSE] Drift flag error ({dept}): {e}")

    # ══════════════════════════════════════════════════════════════
    # RESET (called at DAILY_RESET_HOOK)
    # ══════════════════════════════════════════════════════════════

    def on_daily_reset(self, payload=None):
        """Hook handler: reset daily rolling counters."""
        print("  [PULSE] Daily reset — archiving today's final snapshot.")
        self.update()  # One final update before reset

    def on_pre_reset(self, payload=None):
        """Hook handler: archive current day's pulse data."""
        print("  [PULSE] Pre-reset snapshot archived.")
        self.update()

    # ══════════════════════════════════════════════════════════════
    # QUICK ACCESS (for liaisons that want latest without DB query)
    # ══════════════════════════════════════════════════════════════

    def get_department_snapshot(self, department):
        """Get latest pulse data for a department. Convenience method."""
        pulse_data = self._reader.get_pulse_insights(department=department)

        snapshot = {}
        for entry in pulse_data:
            snapshot[entry['metric_type']] = {
                'value': entry['value'],
                'details': entry['details'],
                'timestamp': entry['timestamp'],
            }
        return snapshot
