"""
The Historian — Data & Analytics Department
Baggins Capital

Daily automated insights from the Archivist's data.
The Archivist LOGS everything. The Historian ANALYZES it and produces
SHORT, ACTIONABLE findings that other employees can query.

Writes findings to `historian_insights` table so employees can ask:
"What did the Historian say about my department today?"

Runs on daily reset (22:00 UTC).
"""

import json
from datetime import datetime, timezone, timedelta
from archivist import Archivist


class Historian:
    """Daily insights from bet_decisions data. Employees query, Historian answers."""

    def __init__(self, db_path='hedge_fund_performance.db'):
        self.db_path = db_path
        self._ensure_tables()

    def _ensure_tables(self):
        """Create historian_insights table if needed."""
        try:
            _arch = Archivist(self.db_path)
            _arch._execute("""
                CREATE TABLE IF NOT EXISTS historian_insights (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    department TEXT NOT NULL,
                    insight_type TEXT NOT NULL,
                    finding TEXT NOT NULL,
                    recommendation TEXT,
                    confidence REAL DEFAULT 0.5,
                    data_points INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """, commit=True)
            _arch._execute("""
                CREATE INDEX IF NOT EXISTS idx_historian_dept_date
                ON historian_insights(department, date)
            """, commit=True)
        except Exception as e:
            print(f"[HISTORIAN] Table setup error: {e}")

    def run_daily_analysis(self):
        """Run all daily analyses. Called at daily reset."""
        print(f"\n[HISTORIAN] === DAILY ANALYSIS ===")
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        insights = []

        # Department P&L reports
        insights.extend(self._analyze_department_performance(today))

        # Modifier drift detection
        insights.extend(self._analyze_modifier_drift(today))

        # Streak detection
        insights.extend(self._analyze_streaks(today))

        # Source performance (weather-specific)
        insights.extend(self._analyze_source_performance(today))

        # Format type analysis (crypto-specific)
        insights.extend(self._analyze_format_performance(today))

        # Store insights
        stored = 0
        for insight in insights:
            self._store_insight(insight)
            stored += 1

        print(f"[HISTORIAN] Stored {stored} insights for {today}")

        # Print summary
        self._print_summary(insights)

        return insights

    def _analyze_department_performance(self, today):
        """Today's bets: count, win rate, P&L by department."""
        insights = []
        try:
            _arch = Archivist(self.db_path)
            for dept in ['crypto', 'weather', 'updown', 'sports']:
                rows = _arch._fetchall("""
                    SELECT won, profit FROM bets
                    WHERE category = ? AND status = 'resolved'
                    AND resolved_at > datetime('now', '-24 hours')
                """, (dept,))

                if not rows:
                    continue

                total = len(rows)
                wins = sum(1 for r in rows if r[0])
                wr = wins / total if total > 0 else 0
                pnl = sum(r[1] or 0 for r in rows)

                finding = f"{dept}: {wins}W/{total-wins}L ({wr:.0%} WR), P&L: ${pnl:+.2f}"
                rec = None
                if wr < 0.30 and total >= 3:
                    rec = f"{dept} underperforming today — review strategy"
                elif wr > 0.65 and total >= 3:
                    rec = f"{dept} running hot — maintain current approach"

                insights.append({
                    'date': today,
                    'department': dept,
                    'insight_type': 'daily_performance',
                    'finding': finding,
                    'recommendation': rec,
                    'confidence': min(0.9, 0.5 + total * 0.05),
                    'data_points': total,
                })
                print(f"  [HISTORIAN] {finding}")

        except Exception as e:
            print(f"  [HISTORIAN] Dept analysis error: {e}")
        return insights

    def _analyze_modifier_drift(self, today):
        """Check if modifiers (hour_mod, asset_mod) are still accurate."""
        insights = []
        try:
            _arch = Archivist(self.db_path)

            # Crypto: hour_mod effectiveness
            rows = _arch._fetchall("""
                SELECT
                    json_extract(bd.modifiers, '$.hour_mod') as hour_mod,
                    b.won
                FROM bet_decisions bd
                JOIN bets b ON b.id = bd.bet_id
                WHERE bd.category = 'crypto' AND b.status = 'resolved'
                AND b.resolved_at > datetime('now', '-7 days')
                AND json_extract(bd.modifiers, '$.hour_mod') IS NOT NULL
            """)

            if len(rows) >= 5:
                boosted = [(r[0], r[1]) for r in rows if r[0] and r[0] > 1.05]
                penalized = [(r[0], r[1]) for r in rows if r[0] and r[0] < 0.95]

                if len(boosted) >= 3:
                    boosted_wr = sum(1 for _, w in boosted if w) / len(boosted)
                    if boosted_wr < 0.40:
                        finding = f"hour_mod boosted bets ({len(boosted)}) winning only {boosted_wr:.0%} — modifier may be stale"
                        insights.append({
                            'date': today, 'department': 'crypto',
                            'insight_type': 'modifier_drift',
                            'finding': finding,
                            'recommendation': 'Recalibrate hour modifier or increase min_sample',
                            'confidence': 0.7, 'data_points': len(boosted),
                        })
                        print(f"  [HISTORIAN] {finding}")

            # Crypto: asset_mod effectiveness
            rows = _arch._fetchall("""
                SELECT
                    json_extract(bd.modifiers, '$.asset_mod') as asset_mod,
                    json_extract(bd.raw_data, '$.coin_id') as coin,
                    b.won
                FROM bet_decisions bd
                JOIN bets b ON b.id = bd.bet_id
                WHERE bd.category = 'crypto' AND b.status = 'resolved'
                AND b.resolved_at > datetime('now', '-14 days')
                AND json_extract(bd.modifiers, '$.asset_mod') IS NOT NULL
            """)

            if len(rows) >= 5:
                # Group by coin
                coin_data = {}
                for mod, coin, won in rows:
                    if coin not in coin_data:
                        coin_data[coin] = []
                    coin_data[coin].append((mod, won))

                for coin, data in coin_data.items():
                    if len(data) >= 3:
                        wr = sum(1 for _, w in data if w) / len(data)
                        avg_mod = sum(m for m, _ in data) / len(data)
                        if avg_mod > 1.05 and wr < 0.40:
                            finding = f"{coin} asset_mod avg {avg_mod:.2f} but WR only {wr:.0%} over {len(data)} bets"
                            insights.append({
                                'date': today, 'department': 'crypto',
                                'insight_type': 'modifier_drift',
                                'finding': finding,
                                'recommendation': f'Asset modifier for {coin} is overconfident',
                                'confidence': 0.65, 'data_points': len(data),
                            })

        except Exception as e:
            print(f"  [HISTORIAN] Modifier drift error: {e}")
        return insights

    def _analyze_streaks(self, today):
        """Detect winning/losing streaks per department."""
        insights = []
        try:
            _arch = Archivist(self.db_path)

            for dept in ['crypto', 'weather', 'updown', 'sports']:
                rows = _arch._fetchall("""
                    SELECT won FROM bets
                    WHERE category = ? AND status = 'resolved'
                    ORDER BY resolved_at DESC LIMIT 15
                """, (dept,))

                if len(rows) < 3:
                    continue

                # Count current streak
                streak = 0
                streak_type = None
                for (won,) in rows:
                    if streak == 0:
                        streak_type = 'win' if won else 'loss'
                        streak = 1
                    elif (won and streak_type == 'win') or (not won and streak_type == 'loss'):
                        streak += 1
                    else:
                        break

                if streak >= 3:
                    finding = f"{dept}: {streak}-bet {streak_type} streak"
                    rec = None
                    if streak_type == 'loss' and streak >= 4:
                        rec = f"Consider reviewing {dept} strategy — extended losing streak"
                    elif streak_type == 'win' and streak >= 5:
                        rec = f"{dept} on a roll — conditions may be favorable"

                    insights.append({
                        'date': today, 'department': dept,
                        'insight_type': 'streak',
                        'finding': finding,
                        'recommendation': rec,
                        'confidence': 0.8, 'data_points': streak,
                    })
                    print(f"  [HISTORIAN] {finding}")

        except Exception as e:
            print(f"  [HISTORIAN] Streak analysis error: {e}")
        return insights

    def _analyze_source_performance(self, today):
        """Weather-specific: which sources are most/least accurate right now?"""
        insights = []
        try:
            _arch = Archivist(self.db_path)

            # Per-source accuracy over last 7 days
            rows = _arch._fetchall("""
                SELECT source_name, COUNT(*) as cnt,
                       ROUND(AVG(error), 2) as avg_err,
                       ROUND(AVG(bias), 2) as avg_bias
                FROM source_prediction_log
                WHERE logged_at > datetime('now', '-7 days')
                GROUP BY source_name
                HAVING cnt >= 3
                ORDER BY avg_err ASC
            """)

            if rows:
                best = rows[0]
                worst = rows[-1]
                finding = f"Best source (7d): {best[0]} (avg error {best[2]}F, bias {best[3]:+.1f}F, n={best[1]}). Worst: {worst[0]} ({worst[2]}F, n={worst[1]})"
                insights.append({
                    'date': today, 'department': 'weather',
                    'insight_type': 'source_ranking',
                    'finding': finding,
                    'recommendation': f"Trust {best[0]} more, watch {worst[0]}" if best[2] < worst[2] - 1.0 else None,
                    'confidence': 0.75, 'data_points': sum(r[1] for r in rows),
                })
                print(f"  [HISTORIAN] {finding}")

            # Per-city source ranking
            city_rows = _arch._fetchall("""
                SELECT city, source_name,
                       COUNT(*) as cnt, ROUND(AVG(error), 2) as avg_err
                FROM source_prediction_log
                WHERE logged_at > datetime('now', '-14 days')
                GROUP BY city, source_name
                HAVING cnt >= 3
                ORDER BY city, avg_err ASC
            """)

            # Group by city and find best/worst
            city_sources = {}
            for city, source, cnt, err in city_rows:
                if city not in city_sources:
                    city_sources[city] = []
                city_sources[city].append((source, cnt, err))

            for city, sources in city_sources.items():
                if len(sources) >= 3:
                    best_s = sources[0]
                    worst_s = sources[-1]
                    if worst_s[2] > best_s[2] + 1.5:
                        finding = f"{city}: {best_s[0]} ({best_s[2]}F err) >> {worst_s[0]} ({worst_s[2]}F err)"
                        insights.append({
                            'date': today, 'department': 'weather',
                            'insight_type': 'city_source_ranking',
                            'finding': finding,
                            'recommendation': f"Weight {best_s[0]} higher for {city}",
                            'confidence': 0.7, 'data_points': best_s[1] + worst_s[1],
                        })

        except Exception as e:
            print(f"  [HISTORIAN] Source analysis error: {e}")
        return insights

    def _analyze_format_performance(self, today):
        """Crypto-specific: which bet formats (touch/settlement/range) perform best?"""
        insights = []
        try:
            _arch = Archivist(self.db_path)
            rows = _arch._fetchall("""
                SELECT format_type, COUNT(*) as cnt,
                       SUM(CASE WHEN won THEN 1 ELSE 0 END) as wins,
                       ROUND(SUM(COALESCE(profit, 0)), 2) as pnl
                FROM bets
                WHERE category = 'crypto' AND status = 'resolved'
                AND format_type IS NOT NULL AND format_type != 'unknown'
                GROUP BY format_type
                HAVING cnt >= 3
            """)

            for fmt, cnt, wins, pnl in rows:
                wr = wins / cnt if cnt > 0 else 0
                finding = f"Format '{fmt}': {wins}W/{cnt-wins}L ({wr:.0%} WR), P&L ${pnl:+.2f}"
                rec = None
                if wr < 0.30 and cnt >= 5:
                    rec = f"'{fmt}' format underperforming — consider reducing exposure"
                elif wr > 0.60 and cnt >= 5:
                    rec = f"'{fmt}' format strong — lean into it"

                insights.append({
                    'date': today, 'department': 'crypto',
                    'insight_type': 'format_performance',
                    'finding': finding,
                    'recommendation': rec,
                    'confidence': min(0.85, 0.5 + cnt * 0.03),
                    'data_points': cnt,
                })

        except Exception as e:
            print(f"  [HISTORIAN] Format analysis error: {e}")
        return insights

    def _store_insight(self, insight):
        """Store a single insight in the DB."""
        try:
            _arch = Archivist(self.db_path)
            _arch._execute("""
                INSERT INTO historian_insights
                    (date, department, insight_type, finding, recommendation, confidence, data_points)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                insight['date'],
                insight['department'],
                insight['insight_type'],
                insight['finding'],
                insight.get('recommendation'),
                insight.get('confidence', 0.5),
                insight.get('data_points', 0),
            ), commit=True)
        except Exception as e:
            print(f"  [HISTORIAN] Store error: {e}")

    def _print_summary(self, insights):
        """Print daily summary for Manager."""
        if not insights:
            print("[HISTORIAN] No insights today (not enough data yet)")
            return

        print(f"\n[HISTORIAN] === DAILY BRIEFING ({len(insights)} findings) ===")
        for i in insights:
            emoji = "+" if i.get('recommendation') and 'strong' in (i.get('recommendation') or '') else "-" if i.get('recommendation') and ('under' in (i.get('recommendation') or '') or 'review' in (i.get('recommendation') or '')) else "~"
            print(f"  [{emoji}] {i['finding']}")
            if i.get('recommendation'):
                print(f"      Rec: {i['recommendation']}")

    def get_insights_for(self, department, days=7):
        """Query recent insights for a department. Other employees call this."""
        try:
            _arch = Archivist(self.db_path)
            rows = _arch._fetchall("""
                SELECT date, insight_type, finding, recommendation, confidence, data_points
                FROM historian_insights
                WHERE department = ?
                AND date >= date('now', ?)
                ORDER BY date DESC, confidence DESC
            """, (department, f'-{days} days'))

            return [
                {
                    'date': r[0], 'type': r[1], 'finding': r[2],
                    'recommendation': r[3], 'confidence': r[4], 'data_points': r[5],
                }
                for r in rows
            ]
        except Exception:
            return []

    def get_latest_recommendation(self, department, insight_type):
        """Get the most recent recommendation for a specific type.
        Employees call this to check: 'What did the Historian say about my modifiers?'"""
        try:
            _arch = Archivist(self.db_path)
            row = _arch._fetchone("""
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
