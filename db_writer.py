"""
The DB Writer (Write Archivist) — Data & Analytics Department
Baggins Capital V3

Handles ALL incoming writes to the database. Never reads. Never analyzes.
Thin wrapper around the existing Archivist engine for write operations.

All employees route writes through here:
  bet placements, decision snapshots, resolution records, Risk warnings,
  Compliance rejections, Historian insights, Pulse insights, Detective findings,
  Signals Library updates, blocklist entries, Manager approval records.

Department: Data & Analytics
Reports to: The Manager
"""

import json
from datetime import datetime
from archivist import Archivist


class DBWriter:
    """All database writes. Nothing else."""

    def __init__(self, db_path='hedge_fund_performance.db'):
        self.db_path = db_path
        self._arch = Archivist(db_path)

    # ── Bet Recording ──

    def record_bet(self, **kwargs):
        """Record a new bet. Delegates to Archivist.record_bet()."""
        return self._arch.record_bet(**kwargs)

    def resolve_bet(self, bet_id, won, profit, balance_after, resolved_by=None):
        """Mark a bet as resolved."""
        self._arch.resolve_bet(bet_id, won, profit, balance_after, resolved_by)

    def record_resolution_detail(self, bet_id, resolved_by, won, profit,
                                  redeemed_amount=None, actual_data=None):
        """Record detailed resolution audit trail."""
        self._arch.record_resolution_detail(bet_id, resolved_by, won, profit,
                                             redeemed_amount, actual_data)

    def set_trade_id(self, bet_id, trade_id):
        """Set the Bankr trade_id for a bet."""
        self._arch.set_trade_id(bet_id, trade_id)

    # ── Decision Snapshots ──

    def record_decision(self, bet_id, category, snapshot):
        """Store full decision context for a bet."""
        self._arch._record_decision(bet_id, category, snapshot)
        self._arch._commit()

    # ── Weather Data ──

    def record_weather_bet(self, bet_id, city, temp_range, forecast_temps, weighted_mean):
        self._arch.record_weather_bet(bet_id, city, temp_range, forecast_temps, weighted_mean)

    def record_weather_prediction(self, bet_id, city, market_date, forecasts,
                                   weighted_mean, our_probability, edge):
        self._arch.record_weather_prediction(bet_id, city, market_date, forecasts,
                                              weighted_mean, our_probability, edge)

    def record_weather_resolution(self, bet_id, predicted_high, actual_high, error):
        self._arch.record_weather_resolution(bet_id, predicted_high, actual_high, error)

    def record_forecast_snapshot(self, city, target_date, source, high_temp,
                                  low_temp=None, unit='F', collection_run=0):
        self._arch.record_forecast_snapshot(city, target_date, source, high_temp,
                                             low_temp, unit, collection_run)

    def log_forecast_collection(self, cities_scanned, total_readings, sources_failed=None):
        self._arch.log_forecast_collection(cities_scanned, total_readings, sources_failed)

    def update_source_credibility(self, city, source_name, **kwargs):
        self._arch.update_source_credibility(city, source_name, **kwargs)

    # ── Portfolio & Settlement ──

    def record_portfolio_check(self, portfolio_raw, redeem_response, pending_count, resolved_count=0):
        self._arch.record_portfolio_check(portfolio_raw, redeem_response, pending_count, resolved_count)

    # ── Historian Insights ──

    def write_historian_insight(self, date, department, insight_type, finding,
                                recommendation=None, confidence=0.5, data_points=0):
        """Store a Historian insight."""
        self._arch._execute("""
            INSERT INTO historian_insights
                (date, department, insight_type, finding, recommendation, confidence, data_points)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (date, department, insight_type, finding, recommendation,
              confidence, data_points), commit=True)

    # ── Pulse Insights ──

    def write_pulse_insight(self, department, metric_type, value, details=None):
        """Store a Market Pulse Analyst metric."""
        self._arch._execute("""
            INSERT INTO pulse_insights
                (timestamp, department, metric_type, value, details)
            VALUES (?, ?, ?, ?, ?)
        """, (datetime.now().isoformat(), department, metric_type,
              value, json.dumps(details) if details else None), commit=True)

    # ── Detective Findings ──

    def write_detective_finding(self, anomaly_id, root_cause, confidence_in_finding,
                                 recommended_action, affected_employee,
                                 affected_parameter, status='pending'):
        """Store a Detective finding for Manager review."""
        self._arch._execute("""
            INSERT INTO detective_findings
                (anomaly_id, root_cause, confidence, recommended_action,
                 affected_employee, affected_parameter, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (anomaly_id, root_cause, confidence_in_finding, recommended_action,
              affected_employee, affected_parameter, status,
              datetime.now().isoformat()), commit=True)

    # ── Signals Library ──

    def write_signal(self, finding_id, pattern_hash, category, description,
                      recommendation, outcome=None, applied=False, confidence=0.5):
        """Store a Signals Library entry."""
        self._arch._execute("""
            INSERT INTO signals_library
                (finding_id, pattern_hash, category, description,
                 recommendation, outcome, applied, confidence, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (finding_id, pattern_hash, category, description,
              recommendation, outcome, 1 if applied else 0, confidence,
              datetime.now().isoformat()), commit=True)

    def update_signal_outcome(self, signal_id, outcome, performance_delta):
        """Update a signal entry with its outcome after monitoring period."""
        self._arch._execute("""
            UPDATE signals_library
            SET outcome = ?, performance_delta = ?, updated_at = ?
            WHERE id = ?
        """, (outcome, performance_delta, datetime.now().isoformat(),
              signal_id), commit=True)

    # ── Risk Manager ──

    def write_risk_warning(self, department, severity, category, details, recommended_action):
        """Store a Risk Manager warning."""
        self._arch._execute("""
            INSERT INTO risk_events
                (timestamp, market_id, category, amount, decision, details)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (datetime.now().isoformat(), department, category, 0,
              severity, json.dumps({
                  'details': details,
                  'recommended_action': recommended_action,
              })), commit=True)

    # ── Compliance ──

    def write_compliance_log(self, market_id, category, amount, approved, reason, warnings=None):
        """Store a Compliance decision."""
        self._arch._execute("""
            INSERT INTO compliance_log (market_id, category, amount, approved, reason, warnings)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (market_id, category, amount, 1 if approved else 0,
              reason, json.dumps(warnings) if warnings else None), commit=True)

    # ── Agent State ──

    def set_state(self, key, value):
        """Set a persistent key-value state."""
        self._arch._execute("""
            INSERT OR REPLACE INTO agent_state (key, value, updated_at)
            VALUES (?, ?, ?)
        """, (key, json.dumps(value) if not isinstance(value, str) else value,
              datetime.now().isoformat()), commit=True)

    # ── Generic Write ──

    def fetchone(self, sql, params=()):
        """Execute a read query and return one row."""
        return self._arch._fetchone(sql, params)

    def fetchall(self, sql, params=()):
        """Execute a read query and return all rows."""
        return self._arch._fetchall(sql, params)

    def execute(self, sql, params=(), commit=True):
        """Execute raw SQL write. Use sparingly — prefer typed methods."""
        self._arch._execute(sql, params, commit=commit)
