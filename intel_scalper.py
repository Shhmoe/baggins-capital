"""
Scalper Intelligence Liaison — Trading Desk
Baggins Capital V3

Pre-pulls data from pulse_insights + historian_insights and packages it
for the Scalper. Runs before every Scalper evaluation cycle.
Never evaluates signals. Never advises. Just translates and delivers.

Department: Trading Desk
Reports to: The Scalper
"""

from db_reader import DBReader


class ScalperIntel:
    """Pre-packages intelligence for Scalper. Nothing else."""

    DEPARTMENT = 'updown'

    FALLBACK = {
        'confidence_floor_boost': 15,
        'zero_out_signals': [],
    }

    def __init__(self, db_path='hedge_fund_performance.db'):
        self._reader = DBReader(db_path)

    def get_package(self):
        """Build complete intelligence package for Scalper.

        Returns dict with:
            - signal_calibration: weights per signal type from Historian
            - false_positive_rates: per signal type
            - window_performance: WR by firing window (:08/:23/:38/:53)
            - zero_out_signals: signal types with >60% false positive rate
            - streak: current win/loss streak
            - cap_progress: daily cap usage
            - circuit_breaker_proximity: closeness to circuit breaker
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
            # Signal calibration weights (RSI/MA/candle)
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'signal_calibration')
            package['signal_calibration'] = rec

            # False positive rates per signal type
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'false_positive_rates')
            package['false_positive_rates'] = rec

            # Build zero-out list: signal types with >60% false positive rate
            zero_out = []
            if rec and rec.get('finding'):
                try:
                    import json
                    rates = json.loads(rec['finding']) if isinstance(rec['finding'], str) else {}
                    for signal_type, rate in rates.items():
                        if isinstance(rate, (int, float)) and rate > 0.60:
                            zero_out.append(signal_type)
                except (json.JSONDecodeError, TypeError):
                    pass
            package['zero_out_signals'] = zero_out

            # Win rates by firing window
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'window_performance')
            package['window_performance'] = rec

            package['available'] = True

        except Exception as e:
            print(f"  [SCALPER INTEL] Package build failed: {e}")
            package['available'] = False
            package['error'] = str(e)

        return package

    def _extract_value(self, pulse_map, metric_type):
        entry = pulse_map.get(metric_type)
        return entry['value'] if entry else None

    def _extract_pulse(self, pulse_map, metric_type):
        entry = pulse_map.get(metric_type)
        if entry:
            return {'value': entry['value'], 'details': entry['details']}
        return None
