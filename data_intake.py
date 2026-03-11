"""
The Data Intake Coordinator — Operations Department
Baggins Capital V3

Validates and structures ALL incoming resolution data before it hits the DB.
Sits between Settlement Clerk and Write Archivist. Never resolves bets.
Never queries external APIs. Just validates schema, checks for duplicates,
and ensures data integrity before writing.

Department: Operations
Reports to: Settlement Clerk
"""

import json
from datetime import datetime
from db_reader import DBReader
from db_writer import DBWriter


class DataIntake:
    """Validates resolution data before DB writes. Nothing else."""

    # Required fields per resolution type
    REQUIRED_FIELDS = {
        'bet_resolution': ['bet_id', 'won', 'profit', 'balance_after'],
        'weather_resolution': ['bet_id', 'predicted_high', 'actual_high', 'error'],
        'forecast_snapshot': ['city', 'target_date', 'source', 'high_temp'],
    }

    def __init__(self, db_path='hedge_fund_performance.db'):
        self._reader = DBReader(db_path)
        self._writer = DBWriter(db_path)

    def validate_and_write_resolution(self, bet_id, won, profit, balance_after,
                                       resolved_by=None, redeemed_amount=None,
                                       actual_data=None):
        """Validate a bet resolution and write if clean."""
        errors = []

        # ── Type checks ──
        if not isinstance(bet_id, int):
            errors.append(f"bet_id must be int, got {type(bet_id).__name__}")
        if not isinstance(won, bool):
            errors.append(f"won must be bool, got {type(won).__name__}")
        if not isinstance(profit, (int, float)):
            errors.append(f"profit must be numeric, got {type(profit).__name__}")
        if not isinstance(balance_after, (int, float)):
            errors.append(f"balance_after must be numeric, got {type(balance_after).__name__}")

        if errors:
            return {'accepted': False, 'errors': errors}

        # ── Bet exists? ──
        bet = self._reader.get_bet(bet_id)
        if not bet:
            return {'accepted': False, 'errors': [f"bet_id {bet_id} not found"]}

        # ── Already resolved? ──
        if bet.get('status') == 'resolved':
            return {'accepted': False, 'errors': [f"bet_id {bet_id} already resolved"]}

        # ── Sanity: profit matches won/loss ──
        if won and profit < 0:
            errors.append(f"Won=True but profit={profit} is negative — suspicious")
        if not won and profit > 0:
            errors.append(f"Won=False but profit={profit} is positive — suspicious")

        # Warnings don't block — just flag
        warnings = errors.copy()
        errors = []  # Allow through with warnings

        # ── Write ──
        try:
            self._writer.resolve_bet(bet_id, won, profit, balance_after, resolved_by)

            if redeemed_amount is not None or actual_data is not None:
                self._writer.record_resolution_detail(
                    bet_id, resolved_by or 'system', won, profit,
                    redeemed_amount, actual_data
                )

            return {'accepted': True, 'warnings': warnings, 'bet_id': bet_id}
        except Exception as e:
            return {'accepted': False, 'errors': [f"Write failed: {e}"]}

    def validate_and_write_weather_resolution(self, bet_id, predicted_high,
                                                actual_high, error):
        """Validate weather resolution data and write if clean."""
        errors = []

        if not isinstance(bet_id, int):
            errors.append(f"bet_id must be int, got {type(bet_id).__name__}")
        if not isinstance(predicted_high, (int, float)):
            errors.append(f"predicted_high must be numeric")
        if not isinstance(actual_high, (int, float)):
            errors.append(f"actual_high must be numeric")

        # Temperature sanity (-60F to 140F)
        for label, val in [('predicted_high', predicted_high), ('actual_high', actual_high)]:
            if isinstance(val, (int, float)) and (val < -60 or val > 140):
                errors.append(f"{label}={val} outside plausible range [-60, 140]")

        if errors:
            return {'accepted': False, 'errors': errors}

        try:
            self._writer.record_weather_resolution(bet_id, predicted_high, actual_high, error)
            return {'accepted': True, 'bet_id': bet_id}
        except Exception as e:
            return {'accepted': False, 'errors': [f"Write failed: {e}"]}

    def validate_and_write_forecast(self, city, target_date, source, high_temp,
                                     low_temp=None, unit='F', collection_run=0):
        """Validate forecast snapshot and write if clean."""
        errors = []

        if not city or not isinstance(city, str):
            errors.append("city must be non-empty string")
        if not target_date or not isinstance(target_date, str):
            errors.append("target_date must be non-empty string")
        if not source or not isinstance(source, str):
            errors.append("source must be non-empty string")
        if not isinstance(high_temp, (int, float)):
            errors.append(f"high_temp must be numeric, got {type(high_temp).__name__}")

        # Temperature sanity
        if isinstance(high_temp, (int, float)) and (high_temp < -60 or high_temp > 140):
            errors.append(f"high_temp={high_temp} outside plausible range")

        if errors:
            return {'accepted': False, 'errors': errors}

        try:
            self._writer.record_forecast_snapshot(
                city, target_date, source, high_temp,
                low_temp, unit, collection_run
            )
            return {'accepted': True}
        except Exception as e:
            return {'accepted': False, 'errors': [f"Write failed: {e}"]}

    def validate_decision_snapshot(self, bet_id, category, snapshot):
        """Validate and write a decision snapshot."""
        errors = []

        if not isinstance(bet_id, int):
            errors.append("bet_id must be int")
        if not category or not isinstance(category, str):
            errors.append("category must be non-empty string")
        if not isinstance(snapshot, dict):
            errors.append("snapshot must be a dict")

        if errors:
            return {'accepted': False, 'errors': errors}

        try:
            self._writer.record_decision(bet_id, category, snapshot)
            return {'accepted': True, 'bet_id': bet_id}
        except Exception as e:
            return {'accepted': False, 'errors': [f"Write failed: {e}"]}

    def validate_and_write_bet(self, **kwargs):
        """Validate bet data and write if clean. Returns bet_id or None."""
        errors = []

        # Required fields
        required = ['market_id', 'market_title', 'category', 'side', 'amount']
        for field in required:
            if field not in kwargs or kwargs[field] is None:
                errors.append(f"Missing required field: {field}")

        # Amount sanity
        amount = kwargs.get('amount', 0)
        if isinstance(amount, (int, float)):
            if amount <= 0:
                errors.append(f"amount must be positive, got {amount}")
            if amount > 100:
                errors.append(f"amount {amount} exceeds $100 safety limit")
        else:
            errors.append(f"amount must be numeric, got {type(amount).__name__}")

        # Category validation
        category = kwargs.get('category', '')
        valid_categories = ('crypto', 'weather', 'sports', 'updown')
        if category not in valid_categories:
            errors.append(f"Invalid category: {category}")

        # Side validation
        side = kwargs.get('side', '')
        if not side or not isinstance(side, str):
            errors.append(f"side must be non-empty string")

        if errors:
            for e in errors:
                print(f"  [DATA INTAKE] REJECTED: {e}")
            return None

        # Write through DB Writer
        try:
            bet_id = self._writer.record_bet(**kwargs)
            return bet_id
        except Exception as e:
            print(f"  [DATA INTAKE] Write failed: {e}")
            return None
