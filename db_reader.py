"""
The DB Reader (Read Archivist) — Data & Analytics Department
Baggins Capital V3

Handles ALL outgoing reads from the database. Never writes. Never analyzes.
Maintains in-memory cache of latest historian_insights and pulse_insights.

All employees route reads through here:
  Historian daily pulls, Pulse live metrics, Liaison intel packages,
  Risk Manager position queries, Manager findings review, Compliance checks.

Department: Data & Analytics
Reports to: The Manager
"""

import json
import time
from datetime import datetime
from archivist import Archivist


class DBReader:
    """All database reads + cache management. Nothing else."""

    def __init__(self, db_path='hedge_fund_performance.db'):
        self.db_path = db_path
        self._arch = Archivist(db_path)

        # Cache management
        self._historian_cache = None
        self._historian_cache_time = 0
        self._pulse_cache = None
        self._pulse_cache_time = 0
        self._cache_ttl_historian = 3600  # 1 hour (invalidated on Historian run)
        self._cache_ttl_pulse = 120       # 2 min (pulse updates frequently)

    # ══════════════════════════════════════════════════════════════
    # CACHE MANAGEMENT
    # ══════════════════════════════════════════════════════════════

    def invalidate_historian_cache(self):
        """Called when Historian completes daily run."""
        self._historian_cache = None
        self._historian_cache_time = 0

    def invalidate_pulse_cache(self):
        """Called when Market Pulse Analyst updates."""
        self._pulse_cache = None
        self._pulse_cache_time = 0

    # ══════════════════════════════════════════════════════════════
    # HISTORIAN INSIGHTS (cached)
    # ══════════════════════════════════════════════════════════════

    def get_historian_insights(self, department=None, days=7):
        """Get recent Historian insights. Cached."""
        now = time.time()
        cache_key = f"{department}_{days}"

        if (self._historian_cache and
            self._historian_cache.get('key') == cache_key and
            now - self._historian_cache_time < self._cache_ttl_historian):
            return self._historian_cache['data']

        try:
            sql = """
                SELECT date, department, insight_type, finding, recommendation,
                       confidence, data_points, created_at
                FROM historian_insights
                WHERE date >= date('now', ?)
            """
            params = [f'-{days} days']
            if department:
                sql += " AND department = ?"
                params.append(department)
            sql += " ORDER BY date DESC, confidence DESC"

            rows = self._arch._fetchall(sql, tuple(params))
            data = [{
                'date': r[0], 'department': r[1], 'insight_type': r[2],
                'finding': r[3], 'recommendation': r[4],
                'confidence': r[5], 'data_points': r[6], 'created_at': r[7],
            } for r in rows]

            self._historian_cache = {'key': cache_key, 'data': data}
            self._historian_cache_time = now
            return data
        except Exception as e:
            print(f"  [DB READER] Historian read error: {e}")
            return []

    def get_latest_historian_recommendation(self, department, insight_type):
        """Get the most recent recommendation for a specific type."""
        try:
            row = self._arch._fetchone("""
                SELECT finding, recommendation, confidence, data_points
                FROM historian_insights
                WHERE department = ? AND insight_type = ?
                ORDER BY date DESC, id DESC LIMIT 1
            """, (department, insight_type))
            if row:
                return {
                    'finding': row[0], 'recommendation': row[1],
                    'confidence': row[2], 'data_points': row[3],
                }
        except Exception:
            pass
        return None

    # ══════════════════════════════════════════════════════════════
    # PULSE INSIGHTS (cached, short TTL)
    # ══════════════════════════════════════════════════════════════

    def get_pulse_insights(self, department=None):
        """Get latest Market Pulse metrics. Short-lived cache."""
        now = time.time()
        cache_key = department or 'all'

        if (self._pulse_cache and
            self._pulse_cache.get('key') == cache_key and
            now - self._pulse_cache_time < self._cache_ttl_pulse):
            return self._pulse_cache['data']

        try:
            sql = """
                SELECT department, metric_type, value, details, timestamp
                FROM pulse_insights
                WHERE timestamp > datetime('now', '-1 hour')
            """
            params = []
            if department:
                sql += " AND department = ?"
                params.append(department)
            sql += " ORDER BY timestamp DESC"

            rows = self._arch._fetchall(sql, tuple(params))

            # Deduplicate: keep latest per (department, metric_type)
            seen = {}
            data = []
            for r in rows:
                key = (r[0], r[1])
                if key not in seen:
                    seen[key] = True
                    data.append({
                        'department': r[0], 'metric_type': r[1],
                        'value': r[2],
                        'details': json.loads(r[3]) if r[3] else None,
                        'timestamp': r[4],
                    })

            self._pulse_cache = {'key': cache_key, 'data': data}
            self._pulse_cache_time = now
            return data
        except Exception as e:
            print(f"  [DB READER] Pulse read error: {e}")
            return []

    # ══════════════════════════════════════════════════════════════
    # BET QUERIES
    # ══════════════════════════════════════════════════════════════

    def get_bet(self, bet_id):
        """Get a single bet by ID."""
        return self._arch.get_bet(bet_id)

    def get_pending_bets(self, category=None, max_age_hours=None):
        """Get all pending bets."""
        return self._arch.get_pending_bets(category, max_age_hours)

    def get_pending_count(self, category=None):
        """Get count of pending bets."""
        return self._arch.get_pending_count(category)

    def get_daily_bet_count(self, category=None, cycle_type=None):
        """Get bets placed today."""
        return self._arch.get_daily_bet_count(category, cycle_type)

    def bet_exists(self, market_id):
        """Check if a bet exists for this market."""
        return self._arch.bet_exists(market_id)

    # ══════════════════════════════════════════════════════════════
    # DECISION SNAPSHOTS
    # ══════════════════════════════════════════════════════════════

    def get_decision(self, bet_id):
        """Get full decision snapshot for a bet."""
        return self._arch.get_decision(bet_id)

    def query_decisions(self, **kwargs):
        """Query decision snapshots with filters."""
        return self._arch.query_decisions(**kwargs)

    # ══════════════════════════════════════════════════════════════
    # DETECTIVE FINDINGS
    # ══════════════════════════════════════════════════════════════

    def get_pending_findings(self):
        """Get Detective findings awaiting Manager review."""
        try:
            rows = self._arch._fetchall("""
                SELECT id, anomaly_id, root_cause, confidence, recommended_action,
                       affected_employee, affected_parameter, status, created_at
                FROM detective_findings
                WHERE status = 'pending'
                ORDER BY created_at DESC
            """)
            return [{
                'id': r[0], 'anomaly_id': r[1], 'root_cause': r[2],
                'confidence': r[3], 'recommended_action': r[4],
                'affected_employee': r[5], 'affected_parameter': r[6],
                'status': r[7], 'created_at': r[8],
            } for r in rows]
        except Exception:
            return []

    def get_anomaly_flags(self):
        """Get Historian's pending_detective_review flags."""
        try:
            rows = self._arch._fetchall("""
                SELECT id, department, insight_type, finding, recommendation
                FROM historian_insights
                WHERE insight_type = 'anomaly_flag'
                AND date >= date('now', '-7 days')
                ORDER BY date DESC
            """)
            return [{
                'id': r[0], 'department': r[1], 'insight_type': r[2],
                'finding': r[3], 'recommendation': r[4],
            } for r in rows]
        except Exception:
            return []

    # ══════════════════════════════════════════════════════════════
    # SIGNALS LIBRARY
    # ══════════════════════════════════════════════════════════════

    def get_signals(self, category=None, applied=None, limit=50):
        """Get signals from the institutional memory."""
        try:
            sql = "SELECT * FROM signals_library WHERE 1=1"
            params = []
            if category:
                sql += " AND category = ?"
                params.append(category)
            if applied is not None:
                sql += " AND applied = ?"
                params.append(1 if applied else 0)
            sql += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)
            return self._arch._fetchall(sql, tuple(params))
        except Exception:
            return []

    def find_similar_pattern(self, pattern_hash):
        """Check if a pattern has been seen before."""
        try:
            row = self._arch._fetchone(
                "SELECT * FROM signals_library WHERE pattern_hash = ? ORDER BY created_at DESC LIMIT 1",
                (pattern_hash,))
            return row
        except Exception:
            return None

    # ══════════════════════════════════════════════════════════════
    # WEATHER DATA
    # ══════════════════════════════════════════════════════════════

    def get_source_stats(self, city, source_name):
        return self._arch.get_source_stats(city, source_name)

    def get_source_prediction_log(self, city, source_name, limit=30):
        return self._arch.get_source_prediction_log(city, source_name, limit)

    def get_source_weight(self, city, source_name):
        return self._arch.get_source_weight(city, source_name)

    def get_city_patterns(self, city):
        return self._arch.get_city_patterns(city)

    def get_latest_forecasts(self, city, target_date):
        return self._arch.get_latest_forecasts(city, target_date)

    # ══════════════════════════════════════════════════════════════
    # PERFORMANCE QUERIES
    # ══════════════════════════════════════════════════════════════

    def get_department_stats(self, department, days=7):
        """Get win rate, P&L, bet count for a department over N days."""
        try:
            rows = self._arch._fetchall("""
                SELECT won, profit, amount, confidence_score, odds
                FROM bets
                WHERE category = ? AND status = 'resolved'
                AND resolved_at > datetime('now', ?)
            """, (department, f'-{days} days'))

            if not rows:
                return {'bets': 0, 'wins': 0, 'win_rate': 0, 'pnl': 0, 'avg_confidence': 0}

            total = len(rows)
            wins = sum(1 for r in rows if r[0])
            pnl = sum(r[1] or 0 for r in rows)
            avg_conf = sum(r[3] or 0 for r in rows) / total if total else 0

            return {
                'bets': total, 'wins': wins,
                'win_rate': wins / total if total else 0,
                'pnl': round(pnl, 2),
                'avg_confidence': round(avg_conf, 1),
            }
        except Exception:
            return {'bets': 0, 'wins': 0, 'win_rate': 0, 'pnl': 0, 'avg_confidence': 0}

    def get_blended_win_rate(self, days=7):
        """Get blended win rate across all departments."""
        try:
            row = self._arch._fetchone("""
                SELECT COUNT(*), SUM(CASE WHEN won THEN 1 ELSE 0 END)
                FROM bets
                WHERE status = 'resolved'
                AND resolved_at > datetime('now', ?)
            """, (f'-{days} days',))
            if row and row[0] > 0:
                return round(row[1] / row[0], 3)
            return 0.5
        except Exception:
            return 0.5

    def get_consecutive_losses(self, department=None):
        """Get current consecutive loss streak."""
        try:
            sql = "SELECT won FROM bets WHERE status = 'resolved'"
            params = []
            if department:
                sql += " AND category = ?"
                params.append(department)
            sql += " ORDER BY resolved_at DESC LIMIT 20"

            rows = self._arch._fetchall(sql, tuple(params))
            streak = 0
            for (won,) in rows:
                if won:
                    break
                streak += 1
            return streak
        except Exception:
            return 0

    # ══════════════════════════════════════════════════════════════
    # AGENT STATE
    # ══════════════════════════════════════════════════════════════

    def get_state(self, key, default=None):
        """Get a persistent state value."""
        try:
            row = self._arch._fetchone(
                "SELECT value FROM agent_state WHERE key = ?", (key,))
            if row:
                try:
                    return json.loads(row[0])
                except (json.JSONDecodeError, TypeError):
                    return row[0]
        except Exception:
            pass
        return default


    def get_summary(self):
        """Get overall performance summary. Delegates to Archivist."""
        return self._arch.get_summary()

    # ══════════════════════════════════════════════════════════════
    # GENERIC READ
    # ══════════════════════════════════════════════════════════════

    def fetchone(self, sql, params=()):
        """Execute raw SQL read and fetch one. Use sparingly."""
        return self._arch._fetchone(sql, params)

    def fetchall(self, sql, params=()):
        """Execute raw SQL read and fetch all. Use sparingly."""
        return self._arch._fetchall(sql, params)
