"""
Crypto Intelligence Liaison — Trading Desk
Baggins Capital V3

Pre-pulls data from pulse_insights + historian_insights and packages it
for the Crypto Trader. Runs before every Crypto evaluation cycle.
Never makes trading decisions. Never advises. Just translates and delivers.

Department: Trading Desk
Reports to: Crypto Trader
"""

from db_reader import DBReader


class CryptoIntel:
    """Pre-packages intelligence for Crypto Trader. Nothing else."""

    DEPARTMENT = 'crypto'

    # Fallback defaults if data unavailable
    FALLBACK = {
        'confidence_floor_boost': 15,
        'use_flat_modifiers': True,
        'hour_mod': 1.0,
        'asset_mod': 1.0,
    }

    def __init__(self, db_path='hedge_fund_performance.db'):
        self._reader = DBReader(db_path)

    def get_package(self):
        """Build complete intelligence package for Crypto Trader.

        Returns dict with:
            - win_rates: per-strategy WR (FADE/SNAP/MOMENTUM/HOLD)
            - hour_mod: current hour modifier from Historian
            - asset_mods: per-asset modifiers from Historian
            - modifier_drift: True if drift detected >0.1
            - format_performance: WR by format type
            - streak: current win/loss streak
            - cap_progress: daily cap usage
            - circuit_breaker_proximity: how close to circuit breaker
            - exposure: current crypto exposure
            - available: True if data was fetched successfully
        """
        package = {
            'department': self.DEPARTMENT,
            'available': False,
        }

        try:
            # ── Real-time from Pulse ──
            pulse = self._reader.get_pulse_insights(department=self.DEPARTMENT)
            pulse_map = {p['metric_type']: p for p in pulse}

            package['rolling_win_rate'] = self._extract_value(pulse_map, 'rolling_win_rate')
            package['streak'] = self._extract_pulse(pulse_map, 'current_streak')
            package['cap_progress'] = self._extract_pulse(pulse_map, 'daily_cap_progress')
            package['circuit_breaker'] = self._extract_pulse(pulse_map, 'circuit_breaker_proximity')
            package['exposure'] = self._extract_pulse(pulse_map, 'exposure_concentration')

            # ── Deep analysis from Historian ──
            # Strategy win rates (FADE/SNAP/MOMENTUM/HOLD)
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'strategy_performance')
            if rec:
                package['strategy_performance'] = rec
            else:
                package['strategy_performance'] = None

            # Hour modifier calibration
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'hour_modifier')
            if rec and rec.get('recommendation'):
                package['hour_mod_status'] = rec
            else:
                package['hour_mod_status'] = None

            # Asset modifier calibration
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'asset_modifier')
            if rec and rec.get('recommendation'):
                package['asset_mod_status'] = rec
            else:
                package['asset_mod_status'] = None

            # Format performance (touch/settlement/range/threshold_below)
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'format_performance')
            if rec:
                package['format_performance'] = rec
            else:
                package['format_performance'] = None

            # Modifier drift detection
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'modifier_drift')
            package['modifier_drift'] = bool(rec and rec.get('confidence', 0) > 0.5)

            # If drift detected, flag to use flat modifiers
            package['use_flat_modifiers'] = package['modifier_drift']

            package['available'] = True

        except Exception as e:
            print(f"  [CRYPTO INTEL] Package build failed: {e}")
            package['available'] = False
            package['error'] = str(e)

        return package

    def _extract_value(self, pulse_map, metric_type):
        """Extract just the value from a pulse entry."""
        entry = pulse_map.get(metric_type)
        return entry['value'] if entry else None

    def _extract_pulse(self, pulse_map, metric_type):
        """Extract value + details from a pulse entry."""
        entry = pulse_map.get(metric_type)
        if entry:
            return {'value': entry['value'], 'details': entry['details']}
        return None
