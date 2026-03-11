"""
Sports Intelligence Liaison — Trading Desk
Baggins Capital V3

Pre-pulls data from pulse_insights + historian_insights and packages it
for the Sports Buddy. Runs before every Sports evaluation cycle.
Never calculates edge. Never evaluates markets. Just translates and delivers.

Department: Trading Desk
Reports to: Sports Buddy
"""

from db_reader import DBReader


class SportsIntel:
    """Pre-packages intelligence for Sports Buddy. Nothing else."""

    DEPARTMENT = 'sports'

    FALLBACK = {
        'confidence_floor_boost': 15,
        'edge_threshold': 0.10,
        'non_core_penalty': 10,
    }

    def __init__(self, db_path='hedge_fund_performance.db'):
        self._reader = DBReader(db_path)

    def get_package(self):
        """Build complete intelligence package for Sports Buddy.

        Returns dict with:
            - edge_thresholds: calibrated edge thresholds by sport/event type
            - sport_win_rates: WR by sport (UFC/boxing/playoffs/other)
            - bookmaker_calibration: Polymarket vs bookmaker calibration data
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
            # Edge thresholds by sport/event type
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'edge_thresholds')
            package['edge_thresholds'] = rec

            # Win rates by sport type (UFC/boxing/playoffs)
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'sport_win_rates')
            package['sport_win_rates'] = rec

            # Bookmaker vs Polymarket calibration
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'bookmaker_calibration')
            package['bookmaker_calibration'] = rec

            package['available'] = True

        except Exception as e:
            print(f"  [SPORTS INTEL] Package build failed: {e}")
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
