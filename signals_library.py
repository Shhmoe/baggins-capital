"""
The Signals Librarian — Data & Analytics Department
Baggins Capital V3

Institutional memory for Detective findings. Tracks what was found,
what action was taken, and whether it helped. Prevents repeating
the same mistakes by recognizing recurring patterns.

Department: Data & Analytics
Reports to: The Manager
"""

import json
import hashlib
from datetime import datetime
from db_reader import DBReader
from db_writer import DBWriter


class SignalsLibrary:
    """Institutional memory for Detective findings. Nothing else."""

    def __init__(self, db_path='hedge_fund_performance.db'):
        self._reader = DBReader(db_path)
        self._writer = DBWriter(db_path)

    def catalog_finding(self, finding_id, category, root_cause,
                         recommendation, applied=False, confidence=0.5):
        """Add a Detective finding to the library.

        Called by Manager after reviewing a finding (approve or reject).
        """
        pattern_hash = self._generate_hash(category, root_cause)

        # Check for existing similar pattern
        existing = self._reader.find_similar_pattern(pattern_hash)
        if existing:
            print(f"  [SIGNALS] Pattern already cataloged (hash={pattern_hash})")
            return {'cataloged': False, 'reason': 'duplicate', 'hash': pattern_hash}

        self._writer.write_signal(
            finding_id=finding_id,
            pattern_hash=pattern_hash,
            category=category,
            description=root_cause,
            recommendation=recommendation,
            applied=applied,
            confidence=confidence,
        )

        print(f"  [SIGNALS] Cataloged finding #{finding_id} as {category} (hash={pattern_hash})")
        return {'cataloged': True, 'hash': pattern_hash}

    def record_outcome(self, signal_id, outcome, performance_delta):
        """Record the outcome after a finding was applied.

        Called after monitoring period (e.g., 7 days after applying a change).
        """
        self._writer.update_signal_outcome(signal_id, outcome, performance_delta)
        print(f"  [SIGNALS] Recorded outcome for signal #{signal_id}: {outcome} (delta={performance_delta:+.2f})")

    def check_pattern(self, category, root_cause):
        """Check if this pattern has been seen before.

        Returns the previous signal entry if found, None otherwise.
        Used by Detective before submitting a finding — if the same
        pattern was seen before and the fix didn't work, Detective
        can adjust its recommendation.
        """
        pattern_hash = self._generate_hash(category, root_cause)
        existing = self._reader.find_similar_pattern(pattern_hash)

        if existing:
            return {
                'seen_before': True,
                'hash': pattern_hash,
                'signal': existing,
            }
        return {'seen_before': False, 'hash': pattern_hash}

    def get_category_warnings(self, category):
        """Get active signals/findings for a category. Used by Compliance."""
        try:
            rows = self._reader.fetchall(
                "SELECT id, category, description, recommendation, confidence "
                "FROM signals_library WHERE category = ? ORDER BY created_at DESC LIMIT 5",
                (category,)
            )
            if not rows:
                return None
            # Return the highest confidence signal for this category
            best = max(rows, key=lambda r: r[4] if r[4] else 0)
            return {
                'active': True,
                'confidence': best[4] or 0.5,
                'root_cause': best[2] or 'unknown pattern',
                'recommendation': best[3] or '',
                'signal_count': len(rows),
            }
        except Exception:
            return None

    def get_effective_signals(self, category=None, min_delta=0.0):
        """Get signals that had positive outcomes when applied.

        Useful for the Manager to see what fixes actually worked.
        """
        signals = self._reader.get_signals(category=category, applied=True)
        effective = []

        for s in signals:
            # signals_library row: id, finding_id, pattern_hash, category,
            # description, recommendation, outcome, applied, performance_delta,
            # created_at, updated_at
            if len(s) >= 9 and s[8] is not None and s[8] > min_delta:
                effective.append({
                    'id': s[0],
                    'category': s[3],
                    'description': s[4],
                    'recommendation': s[5],
                    'outcome': s[6],
                    'performance_delta': s[8],
                })

        return effective

    def get_failed_signals(self, category=None):
        """Get signals that were applied but didn't help (negative delta).

        Helps avoid repeating ineffective fixes.
        """
        signals = self._reader.get_signals(category=category, applied=True)
        failed = []

        for s in signals:
            if len(s) >= 9 and s[8] is not None and s[8] < 0:
                failed.append({
                    'id': s[0],
                    'category': s[3],
                    'description': s[4],
                    'recommendation': s[5],
                    'outcome': s[6],
                    'performance_delta': s[8],
                })

        return failed

    def get_summary(self):
        """Get library summary stats."""
        try:
            total = self._reader.fetchone(
                "SELECT COUNT(*) FROM signals_library")
            applied = self._reader.fetchone(
                "SELECT COUNT(*) FROM signals_library WHERE applied = 1")
            with_outcome = self._reader.fetchone(
                "SELECT COUNT(*) FROM signals_library WHERE outcome IS NOT NULL")
            positive = self._reader.fetchone(
                "SELECT COUNT(*) FROM signals_library WHERE performance_delta > 0")

            return {
                'total_signals': total[0] if total else 0,
                'applied': applied[0] if applied else 0,
                'with_outcome': with_outcome[0] if with_outcome else 0,
                'positive_outcomes': positive[0] if positive else 0,
            }
        except Exception:
            return {'total_signals': 0, 'applied': 0, 'with_outcome': 0, 'positive_outcomes': 0}

    def _generate_hash(self, category, description):
        """Generate pattern hash for deduplication."""
        content = f"{category}:{description}".lower().strip()
        return hashlib.md5(content.encode()).hexdigest()[:12]
