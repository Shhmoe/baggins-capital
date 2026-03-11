"""
Weather Intelligence Liaison — Trading Desk
Baggins Capital V3

Pre-pulls data from pulse_insights + historian_insights and packages it
for the Weather Analyst. Runs before every Weather evaluation cycle.
Never queries weather APIs. Never evaluates markets. Just translates and delivers.

Department: Trading Desk
Reports to: Weather Analyst
"""

from db_reader import DBReader


class WeatherIntel:
    """Pre-packages intelligence for Weather Analyst. Nothing else."""

    DEPARTMENT = 'weather'

    FALLBACK = {
        'confidence_floor_boost': 15,
        'source_priority': [],
        'exclude_sources': [],
    }

    def __init__(self, db_path='hedge_fund_performance.db'):
        self._reader = DBReader(db_path)

    def get_package(self):
        """Build complete intelligence package for Weather Analyst.

        Returns dict with:
            - source_rankings: season-aware source priority list from Historian
            - credibility_scores: per-source credibility from Historian
            - accuracy_by_range: rolling accuracy by temperature range
            - excluded_sources: zero-weighted sources to skip
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
            # Source rankings (season-aware)
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'source_rankings')
            package['source_rankings'] = rec

            # Source credibility scores
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'source_credibility')
            package['source_credibility'] = rec

            # Accuracy by temperature range
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'accuracy_by_range')
            package['accuracy_by_range'] = rec

            # Zero-weighted sources to exclude
            excluded = []
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'excluded_sources')
            if rec and rec.get('finding'):
                try:
                    import json
                    excluded = json.loads(rec['finding']) if isinstance(rec['finding'], str) else []
                except (json.JSONDecodeError, TypeError):
                    pass
            package['excluded_sources'] = excluded

            # City-specific performance
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'city_performance')
            package['city_performance'] = rec

            # Calibration self-correction
            rec = self._reader.get_latest_historian_recommendation(
                self.DEPARTMENT, 'calibration_correction')
            package['calibration_correction'] = rec

            package['available'] = True

        except Exception as e:
            print(f"  [WEATHER INTEL] Package build failed: {e}")
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
