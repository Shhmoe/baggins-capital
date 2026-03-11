"""
The Detective — Data & Analytics Department
Baggins Capital V3

Deep forensic analysis every 30 hours. Investigates anomalies flagged by
the Historian's anomaly_flag insights. Root-causes failures, recommends
parameter changes. Findings go to detective_findings table for Manager review.

NEVER auto-applies changes. Manager sign-off required for all recommendations.

Department: Data & Analytics
Reports to: The Manager
"""

import json
import hashlib
from datetime import datetime
from db_reader import DBReader
from db_writer import DBWriter
from company_clock import get_context


class Detective:
    """30h deep forensic analysis. Investigates anomalies, recommends fixes."""

    # Minimum hours between full investigations
    INVESTIGATION_INTERVAL_HOURS = 30

    def __init__(self, db_path='hedge_fund_performance.db'):
        self._reader = DBReader(db_path)
        self._writer = DBWriter(db_path)

    def should_investigate(self):
        """Check if enough time has passed since last investigation."""
        last_run = self._reader.get_state('detective_last_run')
        if not last_run:
            return True

        try:
            last_dt = datetime.fromisoformat(last_run)
            now = datetime.now()
            hours_since = (now - last_dt).total_seconds() / 3600
            return hours_since >= self.INVESTIGATION_INTERVAL_HOURS
        except (ValueError, TypeError):
            return True

    def investigate(self):
        """Run full forensic investigation. Called by Manager."""
        if not self.should_investigate():
            return {'ran': False, 'reason': 'too_soon'}

        print("[DETECTIVE] Starting forensic investigation...")
        findings_count = 0

        # ── 1. Investigate Historian anomaly flags ──
        findings_count += self._investigate_anomaly_flags()

        # ── 2. Department performance deep dive ──
        findings_count += self._investigate_department_failures()

        # ── 3. Systematic pattern detection ──
        findings_count += self._investigate_systematic_patterns()

        # ── 4. Stale position analysis ──
        findings_count += self._investigate_stale_positions()

        # Mark investigation complete
        self._writer.set_state('detective_last_run', datetime.now().isoformat())

        print(f"[DETECTIVE] Investigation complete. {findings_count} findings submitted.")
        return {'ran': True, 'findings': findings_count}

    # ══════════════════════════════════════════════════════════════
    # ANOMALY FLAG INVESTIGATION
    # ══════════════════════════════════════════════════════════════

    def _investigate_anomaly_flags(self):
        """Investigate anomalies flagged by the Historian."""
        flags = self._reader.get_anomaly_flags()
        findings = 0

        for flag in flags:
            try:
                # Generate anomaly ID for dedup
                anomaly_id = f"historian_flag_{flag['id']}"

                # Check if already investigated
                existing = self._reader.fetchone(
                    "SELECT id FROM detective_findings WHERE anomaly_id = ?",
                    (anomaly_id,))
                if existing:
                    continue

                # Analyze the flag
                analysis = self._analyze_anomaly(flag)
                if not analysis:
                    continue

                self._writer.write_detective_finding(
                    anomaly_id=anomaly_id,
                    root_cause=analysis['root_cause'],
                    confidence_in_finding=analysis['confidence'],
                    recommended_action=analysis['recommendation'],
                    affected_employee=analysis.get('affected_employee', flag.get('department', 'unknown')),
                    affected_parameter=analysis.get('affected_parameter', 'unknown'),
                )
                findings += 1

            except Exception as e:
                print(f"  [DETECTIVE] Anomaly investigation error: {e}")

        return findings

    def _analyze_anomaly(self, flag):
        """Root-cause a Historian anomaly flag."""
        finding = flag.get('finding', '')
        dept = flag.get('department', '')

        # ── Win rate collapse ──
        if 'win_rate' in finding.lower() or 'performance' in finding.lower():
            stats = self._reader.get_department_stats(dept, days=7)
            if stats['bets'] >= 5 and stats['win_rate'] < 0.30:
                return {
                    'root_cause': f"{dept} win rate collapsed to {stats['win_rate']:.0%} over {stats['bets']} bets (7d)",
                    'confidence': 0.8,
                    'recommendation': f"Review {dept} strategy parameters. Consider raising confidence floor.",
                    'affected_employee': dept,
                    'affected_parameter': 'confidence_floor',
                }

        # ── Loss streak ──
        if 'streak' in finding.lower() or 'consecutive' in finding.lower():
            streak = self._reader.get_consecutive_losses(department=dept)
            if streak >= 3:
                return {
                    'root_cause': f"{dept} has {streak} consecutive losses",
                    'confidence': 0.7,
                    'recommendation': f"Investigate recent {dept} bet decisions for systematic error.",
                    'affected_employee': dept,
                    'affected_parameter': 'strategy',
                }

        # ── Modifier drift ──
        if 'drift' in finding.lower() or 'modifier' in finding.lower():
            return {
                'root_cause': f"Modifier drift detected in {dept}: {finding}",
                'confidence': 0.6,
                'recommendation': f"Reset {dept} modifiers to neutral (1.0) until recalibration.",
                'affected_employee': dept,
                'affected_parameter': 'modifiers',
            }

        # Generic flag
        return {
            'root_cause': f"Anomaly in {dept}: {finding}",
            'confidence': 0.4,
            'recommendation': flag.get('recommendation', 'Manual review needed.'),
            'affected_employee': dept,
            'affected_parameter': 'unknown',
        }

    # ══════════════════════════════════════════════════════════════
    # DEPARTMENT FAILURE ANALYSIS
    # ══════════════════════════════════════════════════════════════

    def _investigate_department_failures(self):
        """Deep dive into departments with poor 7-day performance."""
        departments = ['crypto', 'weather', 'sports', 'updown']
        findings = 0

        for dept in departments:
            try:
                stats = self._reader.get_department_stats(dept, days=7)
                if stats['bets'] < 5:
                    continue

                # Significant underperformance: WR < 35% with enough sample
                if stats['win_rate'] < 0.35:
                    anomaly_id = f"dept_failure_{dept}_{datetime.now().strftime('%Y%m%d')}"

                    existing = self._reader.fetchone(
                        "SELECT id FROM detective_findings WHERE anomaly_id = ?",
                        (anomaly_id,))
                    if existing:
                        continue

                    # Dig deeper: check recent decisions
                    decisions = self._reader.query_decisions(
                        category=dept, limit=10, days=7)

                    root_cause = self._diagnose_failure(dept, stats, decisions)

                    self._writer.write_detective_finding(
                        anomaly_id=anomaly_id,
                        root_cause=root_cause['explanation'],
                        confidence_in_finding=root_cause['confidence'],
                        recommended_action=root_cause['recommendation'],
                        affected_employee=dept,
                        affected_parameter=root_cause.get('parameter', 'strategy'),
                    )
                    findings += 1

            except Exception as e:
                print(f"  [DETECTIVE] Dept failure analysis error ({dept}): {e}")

        return findings

    def _diagnose_failure(self, dept, stats, decisions):
        """Diagnose root cause of department underperformance."""
        wr = stats['win_rate']
        pnl = stats['pnl']
        bets = stats['bets']

        # Check if low confidence bets are being placed
        if stats['avg_confidence'] < 60:
            return {
                'explanation': f"{dept}: avg confidence {stats['avg_confidence']:.0f} is below 60 — accepting low-quality bets",
                'confidence': 0.75,
                'recommendation': f"Raise {dept} minimum confidence threshold by 10 points",
                'parameter': 'min_confidence',
            }

        # Losing money despite reasonable confidence
        if wr < 0.35 and stats['avg_confidence'] > 70:
            return {
                'explanation': f"{dept}: {wr:.0%} WR despite {stats['avg_confidence']:.0f} avg confidence — calibration broken",
                'confidence': 0.8,
                'recommendation': f"Run calibration analysis on {dept}. Confidence scores don't match outcomes.",
                'parameter': 'calibration',
            }

        return {
            'explanation': f"{dept}: {wr:.0%} WR, ${pnl:.2f} P&L over {bets} bets — underperforming",
            'confidence': 0.5,
            'recommendation': f"Review {dept} recent decisions for pattern errors.",
            'parameter': 'strategy',
        }

    # ══════════════════════════════════════════════════════════════
    # SYSTEMATIC PATTERN DETECTION
    # ══════════════════════════════════════════════════════════════

    def _investigate_systematic_patterns(self):
        """Look for cross-department systematic issues."""
        findings = 0

        try:
            # Check if ALL departments are losing simultaneously
            all_stats = {}
            losing_depts = 0
            for dept in ['crypto', 'weather', 'sports', 'updown']:
                stats = self._reader.get_department_stats(dept, days=3)
                all_stats[dept] = stats
                if stats['bets'] >= 3 and stats['win_rate'] < 0.40:
                    losing_depts += 1

            if losing_depts >= 3:
                anomaly_id = f"systematic_loss_{datetime.now().strftime('%Y%m%d')}"
                existing = self._reader.fetchone(
                    "SELECT id FROM detective_findings WHERE anomaly_id = ?",
                    (anomaly_id,))
                if not existing:
                    self._writer.write_detective_finding(
                        anomaly_id=anomaly_id,
                        root_cause=f"Systematic underperformance: {losing_depts}/4 departments below 40% WR in 3 days",
                        confidence_in_finding=0.85,
                        recommended_action="Consider market-wide cooldown. All departments struggling simultaneously may indicate external factor.",
                        affected_employee='all',
                        affected_parameter='system_health',
                    )
                    findings += 1

        except Exception as e:
            print(f"  [DETECTIVE] Systematic pattern error: {e}")

        return findings

    # ══════════════════════════════════════════════════════════════
    # STALE POSITION ANALYSIS
    # ══════════════════════════════════════════════════════════════

    def _investigate_stale_positions(self):
        """Flag positions that have been pending too long."""
        findings = 0

        try:
            # Get bets pending > 72 hours
            stale = self._reader.get_pending_bets(max_age_hours=72)
            if not stale:
                return 0

            stale_count = len(stale) if isinstance(stale, list) else 0
            if stale_count >= 3:
                anomaly_id = f"stale_positions_{datetime.now().strftime('%Y%m%d')}"
                existing = self._reader.fetchone(
                    "SELECT id FROM detective_findings WHERE anomaly_id = ?",
                    (anomaly_id,))
                if not existing:
                    self._writer.write_detective_finding(
                        anomaly_id=anomaly_id,
                        root_cause=f"{stale_count} bets pending > 72 hours. Possible settlement pipeline issue.",
                        confidence_in_finding=0.9,
                        recommended_action="Check Settlement Clerk and Bankr connectivity. Force-resolve stuck bets if markets have ended.",
                        affected_employee='settlement_clerk',
                        affected_parameter='resolution_pipeline',
                    )
                    findings += 1

        except Exception as e:
            print(f"  [DETECTIVE] Stale position error: {e}")

        return findings

    # ══════════════════════════════════════════════════════════════
    # PATTERN HASH (for Signals Library dedup)
    # ══════════════════════════════════════════════════════════════

    def generate_pattern_hash(self, category, root_cause):
        """Generate a hash for pattern deduplication in Signals Library."""
        content = f"{category}:{root_cause}".lower().strip()
        return hashlib.md5(content.encode()).hexdigest()[:12]
