"""
Performance Tracker - Daily ROI and Self-Improvement
Tracks all bets, calculates daily ROI, runs daily improvements
"""

import sqlite3
import json
from datetime import datetime, timedelta
from collections import defaultdict


class PerformanceTracker:
    """Track performance and run daily improvements."""

    def __init__(self, db_path='hedge_fund_performance.db'):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Create database schema."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        # Bets table
        c.execute("""
            CREATE TABLE IF NOT EXISTS bets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                market_id TEXT NOT NULL,
                market_title TEXT NOT NULL,
                category TEXT NOT NULL,
                side TEXT NOT NULL,
                amount REAL NOT NULL,
                odds REAL NOT NULL,
                confidence_score INTEGER NOT NULL,
                edge REAL NOT NULL,
                reasoning TEXT,
                status TEXT DEFAULT 'pending',
                resolved_at DATETIME,
                won INTEGER,
                profit REAL,
                balance_before REAL,
                balance_after REAL
            )
        """)

        # Daily performance table
        c.execute("""
            CREATE TABLE IF NOT EXISTS daily_performance (
                date DATE PRIMARY KEY,
                starting_balance REAL NOT NULL,
                ending_balance REAL NOT NULL,
                total_bets INTEGER NOT NULL,
                winning_bets INTEGER NOT NULL,
                win_rate REAL NOT NULL,
                profit_loss REAL NOT NULL,
                roi REAL NOT NULL,
                target_roi REAL NOT NULL,
                met_target INTEGER NOT NULL
            )
        """)

        # Category performance table
        c.execute("""
            CREATE TABLE IF NOT EXISTS category_performance (
                category TEXT NOT NULL,
                total_bets INTEGER NOT NULL,
                winning_bets INTEGER NOT NULL,
                win_rate REAL NOT NULL,
                total_profit REAL NOT NULL,
                avg_score REAL NOT NULL,
                last_updated DATETIME NOT NULL,
                PRIMARY KEY (category)
            )
        """)

        # Score range performance
        c.execute("""
            CREATE TABLE IF NOT EXISTS score_performance (
                score_range TEXT NOT NULL,
                total_bets INTEGER NOT NULL,
                winning_bets INTEGER NOT NULL,
                win_rate REAL NOT NULL,
                avg_profit REAL NOT NULL,
                last_updated DATETIME NOT NULL,
                PRIMARY KEY (score_range)
            )
        """)

        # Improvement log
        c.execute("""
            CREATE TABLE IF NOT EXISTS improvements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                improvement_type TEXT NOT NULL,
                old_value REAL,
                new_value REAL,
                reason TEXT,
                data JSON
            )
        """)

        # Portfolio snapshots - hourly balance/position state
        c.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                available_balance REAL NOT NULL,
                deployed_balance REAL NOT NULL,
                total_value REAL NOT NULL,
                active_positions INTEGER NOT NULL,
                pending_bets INTEGER NOT NULL,
                daily_roi REAL,
                consecutive_losses INTEGER DEFAULT 0
            )
        """)

        # Market scans - what was evaluated each cycle
        c.execute("""
            CREATE TABLE IF NOT EXISTS market_scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                markets_found INTEGER NOT NULL,
                markets_above_threshold INTEGER NOT NULL,
                bets_placed INTEGER NOT NULL,
                short_slots_open INTEGER,
                long_slots_open INTEGER,
                skipped_reasons TEXT
            )
        """)

        # Agent state - persistent state across restarts
        c.execute("""
            CREATE TABLE IF NOT EXISTS agent_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at DATETIME NOT NULL
            )
        """)

        # Heartbeat logs - AI reasoning about positions and wallet
        c.execute("""
            CREATE TABLE IF NOT EXISTS heartbeat_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                heartbeat_type TEXT NOT NULL,
                bankr_raw TEXT,
                ai_reasoning TEXT,
                ai_actions TEXT,
                positions_data TEXT,
                wallet_balance REAL,
                total_value REAL
            )
        """)

        # Weather module tables -- credibility learning pipeline
        c.execute("""
            CREATE TABLE IF NOT EXISTS weather_bets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bet_id INTEGER NOT NULL,
                city TEXT NOT NULL,
                temp_range TEXT,
                forecast_temps TEXT,
                weighted_mean REAL DEFAULT 0.0
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS weather_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bet_id INTEGER NOT NULL,
                city TEXT NOT NULL,
                market_date TEXT NOT NULL,
                open_meteo_high REAL,
                noaa_high REAL,
                weatherapi_high REAL,
                weighted_mean REAL NOT NULL,
                our_probability REAL NOT NULL,
                edge REAL NOT NULL,
                timestamp DATETIME,
                forecast_data TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS weather_resolutions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bet_id INTEGER NOT NULL,
                predicted_high REAL NOT NULL,
                actual_high REAL NOT NULL,
                error REAL NOT NULL,
                won INTEGER,
                profit REAL,
                timestamp DATETIME
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS source_prediction_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bet_id INTEGER NOT NULL,
                city TEXT,
                source_name TEXT NOT NULL,
                predicted_high REAL NOT NULL,
                actual_high REAL NOT NULL,
                error REAL NOT NULL,
                bias REAL,
                market_date TEXT,
                logged_at DATETIME
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS weather_sources (
                city TEXT NOT NULL,
                source_name TEXT NOT NULL,
                credibility_weight REAL DEFAULT 1.0,
                total_predictions INTEGER DEFAULT 0,
                accurate_predictions INTEGER DEFAULT 0,
                avg_error REAL DEFAULT 0.0,
                last_updated DATETIME,
                last_error_direction TEXT,
                consecutive_same_direction INTEGER DEFAULT 0,
                avg_bias REAL DEFAULT 0.0,
                bias_direction TEXT,
                bias_consistency REAL DEFAULT 0.0,
                PRIMARY KEY (city, source_name)
            )
        """)

        # Drop dead weather-era tables if they exist
        for dead_table in ['source_predictions', 'source_rankings', 'location_confidence']:
            c.execute(f"DROP TABLE IF EXISTS {dead_table}")

        # Phase 3: Add bias tracking columns to weather_sources (idempotent)
        for col_sql in [
            "ALTER TABLE weather_sources ADD COLUMN bias_direction TEXT",
            "ALTER TABLE weather_sources ADD COLUMN avg_bias REAL DEFAULT 0.0",
        ]:
            try:
                c.execute(col_sql)
            except Exception:
                pass  # Column already exists

        # Phase 4: Add resolved_by column to bets table (idempotent)
        try:
            c.execute("ALTER TABLE bets ADD COLUMN resolved_by TEXT DEFAULT NULL")
        except Exception:
            pass  # Column already exists

        # Phase 5: Withdrawal + take profit + Avantis position tracking
        c.execute("""
            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                amount REAL NOT NULL,
                from_chain TEXT NOT NULL,
                to_chain TEXT NOT NULL,
                purpose TEXT NOT NULL,
                tx_hash TEXT,
                applied INTEGER DEFAULT 1
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS take_profits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                amount REAL NOT NULL,
                source TEXT NOT NULL,
                destination TEXT NOT NULL,
                reason TEXT NOT NULL,
                balance_before REAL,
                balance_after REAL,
                tx_hash TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS avantis_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                pair TEXT NOT NULL,
                side TEXT NOT NULL,
                leverage INTEGER NOT NULL,
                collateral REAL NOT NULL,
                entry_price REAL,
                stop_loss_pct REAL,
                take_profit_pct REAL,
                confidence REAL,
                signal_type TEXT,
                reasoning TEXT,
                status TEXT DEFAULT 'open',
                closed_at DATETIME,
                exit_price REAL,
                pnl REAL,
                pnl_pct REAL,
                exit_reason TEXT,
                trade_id TEXT,
                bet_id INTEGER
            )
        """)

        conn.commit()
        conn.close()

    def log_bet(self, market_id, market_title, category, side, amount, odds,
                score, edge, reasoning, balance_before):
        """Log a new bet. Prevents duplicates on same market_id+side."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        c = conn.cursor()

        # Dedup check: max 1 bet per market_id (any side, any status)
        c.execute("""
            SELECT COUNT(*) FROM bets
            WHERE market_id = ?
        """, (str(market_id),))
        bet_count = c.fetchone()[0]
        if bet_count >= 1:
            print(f"[ACCOUNTANT] Already bet on market {market_id} ({bet_count} existing)")
            conn.close()
            return None

        c.execute("""
            INSERT INTO bets
            (timestamp, market_id, market_title, category, side, amount,
             odds, confidence_score, edge, reasoning, balance_before)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now(),
            market_id,
            market_title,
            category,
            side,
            amount,
            odds,
            score,
            edge,
            reasoning,
            balance_before
        ))

        bet_id = c.lastrowid
        conn.commit()
        conn.close()

        return bet_id

    def resolve_bet(self, bet_id, won, profit, balance_after):
        """Mark bet as resolved."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute("""
            UPDATE bets
            SET status = 'resolved',
                resolved_at = ?,
                won = ?,
                profit = ?,
                balance_after = ?
            WHERE id = ?
        """, (datetime.now(), int(won), profit, balance_after, bet_id))

        conn.commit()
        conn.close()

        # Update performance stats
        self._update_performance_stats()

    def log_weather_resolution(self, bet_id, predicted_high, actual_high, error):
        """Log weather resolution for credibility tracking."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        c = conn.cursor()

        # Get won/profit from bets table
        c.execute("SELECT won, profit FROM bets WHERE id = ?", (bet_id,))
        row = c.fetchone()
        won = row[0] if row else 0
        profit = row[1] if row else 0.0

        c.execute("""
            INSERT INTO weather_resolutions
                (bet_id, predicted_high, actual_high, error, won, profit, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (bet_id, predicted_high, actual_high, error, won, profit,
              datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        conn.commit()
        conn.close()

    def log_weather_prediction(self, bet_id, city, market_date, forecasts,
                                weighted_mean, our_probability, edge):
        """Store per-source forecast data when a weather bet is placed.
        This is critical for the credibility learning loop — without it,
        resolutions can't compare predicted vs actual per source.
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            c = conn.cursor()

            # Build forecast_data JSON: {source_name: {high_temp: X, low_temp: Y}}
            import json
            forecast_data = {}
            for f in forecasts:
                src = f.get("source", "unknown")
                forecast_data[src] = {
                    "high_temp": f.get("high_temp", 0),
                    "low_temp": f.get("low_temp", 0),
                }

            # Also populate legacy columns if available
            open_meteo = next((f["high_temp"] for f in forecasts if f.get("source") == "open_meteo"), None)
            noaa = next((f["high_temp"] for f in forecasts if f.get("source") == "noaa"), None)
            weatherapi = next((f["high_temp"] for f in forecasts if f.get("source") == "weatherapi"), None)

            c.execute("""
                INSERT INTO weather_predictions
                    (bet_id, city, market_date, open_meteo_high, noaa_high, weatherapi_high,
                     weighted_mean, our_probability, edge, timestamp, forecast_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (bet_id, city, market_date, open_meteo, noaa, weatherapi,
                  weighted_mean, our_probability, edge,
                  datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                  json.dumps(forecast_data)))

            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[ACCOUNTANT] log_weather_prediction error: {e}")

    def log_weather_bet(self, bet_id, city, temp_range, forecast_temps, weighted_mean):
        """Store weather bet metadata (city, range, forecasts).
        Used by the resolution pipeline to match bets to actual temperatures.
        """
        try:
            import json
            conn = sqlite3.connect(self.db_path, timeout=30)
            c = conn.cursor()

            c.execute("""
                INSERT INTO weather_bets (bet_id, city, temp_range, forecast_temps, weighted_mean)
                VALUES (?, ?, ?, ?, ?)
            """, (bet_id, city, temp_range,
                  json.dumps(forecast_temps) if isinstance(forecast_temps, dict) else str(forecast_temps),
                  weighted_mean))

            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[ACCOUNTANT] log_weather_bet error: {e}")

    def get_daily_roi(self, date=None):
        """Calculate ROI for specific day (default: today)."""
        if date is None:
            date = datetime.now().date()

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        # Get all resolved bets for this day
        c.execute("""
            SELECT balance_before, balance_after, profit
            FROM bets
            WHERE DATE(timestamp) = ?
            AND status = 'resolved'
            ORDER BY timestamp ASC
        """, (date,))

        bets = c.fetchall()
        conn.close()

        if not bets:
            return {
                'date': date,
                'starting_balance': None,
                'ending_balance': None,
                'profit': 0,
                'roi': 0,
                'total_bets': 0
            }

        starting_balance = bets[0][0]
        ending_balance = bets[-1][1]
        total_profit = sum(b[2] for b in bets)
        roi = (total_profit / starting_balance) if starting_balance > 0 else 0

        return {
            'date': date,
            'starting_balance': starting_balance,
            'ending_balance': ending_balance,
            'profit': total_profit,
            'roi': roi,
            'total_bets': len(bets),
            'met_target': roi >= 0.40
        }

    def get_category_performance(self):
        """Get performance by category."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute("""
            SELECT
                category,
                COUNT(*) as total,
                SUM(won) as wins,
                CAST(SUM(won) AS REAL) / COUNT(*) as win_rate,
                SUM(profit) as total_profit,
                AVG(confidence_score) as avg_score
            FROM bets
            WHERE status = 'resolved'
            GROUP BY category
        """)

        results = {}
        for row in c.fetchall():
            results[row[0]] = {
                'total_bets': row[1],
                'wins': row[2],
                'win_rate': row[3],
                'total_profit': row[4],
                'avg_score': row[5]
            }

        conn.close()
        return results

    def get_score_performance(self):
        """Get performance by score range."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        score_ranges = [
            ('60-69', 60, 69),
            ('70-79', 70, 79),
            ('80-89', 80, 89),
            ('90-100', 90, 100)
        ]

        results = {}
        for range_name, min_score, max_score in score_ranges:
            c.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(won) as wins,
                    CAST(SUM(won) AS REAL) / COUNT(*) as win_rate,
                    AVG(profit) as avg_profit
                FROM bets
                WHERE status = 'resolved'
                AND confidence_score >= ?
                AND confidence_score <= ?
            """, (min_score, max_score))

            row = c.fetchone()
            if row[0] > 0:
                results[range_name] = {
                    'total_bets': row[0],
                    'wins': row[1],
                    'win_rate': row[2],
                    'avg_profit': row[3]
                }

        conn.close()
        return results

    def _update_performance_stats(self):
        """Update category and score performance tables."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        # Update category performance
        c.execute("DELETE FROM category_performance")
        cat_perf = self.get_category_performance()
        for cat, stats in cat_perf.items():
            c.execute("""
                INSERT INTO category_performance
                (category, total_bets, winning_bets, win_rate, total_profit,
                 avg_score, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                cat,
                stats['total_bets'],
                stats['wins'],
                stats['win_rate'],
                stats['total_profit'],
                stats['avg_score'],
                datetime.now()
            ))

        # Update score performance
        c.execute("DELETE FROM score_performance")
        score_perf = self.get_score_performance()
        for range_name, stats in score_perf.items():
            c.execute("""
                INSERT INTO score_performance
                (score_range, total_bets, winning_bets, win_rate, avg_profit,
                 last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                range_name,
                stats['total_bets'],
                stats['wins'],
                stats['win_rate'],
                stats['avg_profit'],
                datetime.now()
            ))

        conn.commit()
        conn.close()

    def save_daily_performance(self, starting_balance, ending_balance):
        """Save today's performance to daily_performance table."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        today = datetime.now().date()

        # Get today's resolved bets
        c.execute("""
            SELECT COUNT(*), SUM(won), SUM(profit)
            FROM bets
            WHERE DATE(timestamp) = ? AND status = 'resolved'
        """, (today,))
        row = c.fetchone()
        total_bets = row[0] or 0
        winning_bets = row[1] or 0
        total_profit = row[2] or 0.0
        win_rate = (winning_bets / total_bets) if total_bets > 0 else 0.0
        roi = (total_profit / starting_balance) if starting_balance > 0 else 0.0
        target_roi = 0.05  # 5% daily target (realistic)
        met_target = 1 if roi >= target_roi else 0

        c.execute("""
            INSERT INTO daily_performance
            (date, starting_balance, ending_balance, total_bets, winning_bets,
             win_rate, profit_loss, roi, target_roi, met_target)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                ending_balance = ?, total_bets = ?, winning_bets = ?,
                win_rate = ?, profit_loss = ?, roi = ?, met_target = ?
        """, (
            today, starting_balance, ending_balance, total_bets, winning_bets,
            win_rate, total_profit, roi, target_roi, met_target,
            ending_balance, total_bets, winning_bets,
            win_rate, total_profit, roi, met_target
        ))

        conn.commit()
        conn.close()
        print(f"[ACCOUNTANT] Daily performance saved: {total_bets} bets, ${total_profit:+.2f}, ROI {roi:+.1%}")

    def save_portfolio_snapshot(self, available, deployed, positions, pending, daily_roi, consec_losses):
        """Save a point-in-time portfolio snapshot."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            INSERT INTO portfolio_snapshots
            (timestamp, available_balance, deployed_balance, total_value,
             active_positions, pending_bets, daily_roi, consecutive_losses)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now(), available, deployed, available + deployed,
            positions, pending, daily_roi, consec_losses
        ))
        conn.commit()
        conn.close()

    def log_market_scan(self, markets_found, above_threshold, bets_placed,
                        short_open, long_open, skipped_reasons=None):
        """Log a market scan cycle result."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            INSERT INTO market_scans
            (timestamp, markets_found, markets_above_threshold, bets_placed,
             short_slots_open, long_slots_open, skipped_reasons)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now(), markets_found, above_threshold, bets_placed,
            short_open, long_open,
            json.dumps(skipped_reasons) if skipped_reasons else None
        ))
        conn.commit()
        conn.close()

    def get_state(self, key, default=None):
        """Get a persistent agent state value."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("SELECT value FROM agent_state WHERE key = ?", (key,))
        row = c.fetchone()
        conn.close()
        return row[0] if row else default

    def set_state(self, key, value):
        """Set a persistent agent state value."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            INSERT INTO agent_state (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = ?, updated_at = ?
        """, (key, str(value), datetime.now(), str(value), datetime.now()))
        conn.commit()
        conn.close()

    # ------------------------------------------------------------------
    # Withdrawal / Take Profit tracking
    # ------------------------------------------------------------------

    def log_withdrawal(self, amount, from_chain, to_chain, purpose, tx_hash=None):
        """Log a fund withdrawal/bridge so P&L calculations exclude it."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        c = conn.cursor()
        c.execute("""
            INSERT INTO withdrawals (timestamp, amount, from_chain, to_chain, purpose, tx_hash)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (datetime.now(), amount, from_chain, to_chain, purpose, tx_hash))
        conn.commit()
        conn.close()
        print(f"[ACCOUNTANT] Logged: ${amount:.2f} {from_chain} -> {to_chain} ({purpose})")

    def get_today_withdrawals(self):
        """Get total withdrawn today (for ROI adjustment)."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        c = conn.cursor()
        c.execute("""
            SELECT COALESCE(SUM(amount), 0) FROM withdrawals
            WHERE DATE(timestamp) = DATE('now')
        """)
        total = c.fetchone()[0]
        conn.close()
        return total

    def log_take_profit(self, amount, source, destination, reason, balance_before=None, balance_after=None, tx_hash=None):
        """Record a profit-taking event (bridge, withdrawal, fund transfer)."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        c = conn.cursor()
        c.execute("""
            INSERT INTO take_profits (timestamp, amount, source, destination, reason, balance_before, balance_after, tx_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (datetime.now(), amount, source, destination, reason, balance_before, balance_after, tx_hash))
        conn.commit()
        conn.close()
        print(f"[ACCOUNTANT] ${amount:.2f} from {source} -> {destination} ({reason})")

    # ------------------------------------------------------------------
    # Avantis position tracking
    # ------------------------------------------------------------------

    def log_avantis_position(self, pair, side, leverage, collateral, entry_price,
                              stop_loss_pct, take_profit_pct, confidence, signal_type,
                              reasoning, trade_id=None):
        """Open a new Avantis position record."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        c = conn.cursor()
        c.execute("""
            INSERT INTO avantis_positions
            (timestamp, pair, side, leverage, collateral, entry_price,
             stop_loss_pct, take_profit_pct, confidence, signal_type, reasoning, trade_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (datetime.now(), pair, side, leverage, collateral, entry_price,
              stop_loss_pct, take_profit_pct, confidence, signal_type, reasoning, trade_id))
        pos_id = c.lastrowid
        conn.commit()
        conn.close()
        print(f"[ACCOUNTANT] Position #{pos_id}: {leverage}x {side.upper()} {pair} @ ${entry_price:,.2f}")
        return pos_id

    def close_avantis_position(self, position_id, exit_price, pnl, pnl_pct, exit_reason):
        """Close an Avantis position with P&L."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        c = conn.cursor()
        c.execute("""
            UPDATE avantis_positions
            SET status = 'closed', closed_at = ?, exit_price = ?, pnl = ?, pnl_pct = ?, exit_reason = ?
            WHERE id = ?
        """, (datetime.now(), exit_price, pnl, pnl_pct, exit_reason, position_id))
        conn.commit()
        conn.close()
        won = "WIN" if pnl > 0 else "LOSS"
        print(f"[ACCOUNTANT] Position #{position_id} CLOSED ({won}): ${pnl:+.2f} ({pnl_pct:+.1f}%) - {exit_reason}")

    def get_open_avantis_positions(self):
        """Get all open Avantis positions."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        c = conn.cursor()
        c.execute("""
            SELECT id, timestamp, pair, side, leverage, collateral, entry_price,
                   stop_loss_pct, take_profit_pct, confidence, signal_type, trade_id
            FROM avantis_positions WHERE status = 'open'
            ORDER BY timestamp DESC
        """)
        rows = c.fetchall()
        conn.close()
        positions = []
        for r in rows:
            positions.append({
                'id': r[0], 'timestamp': r[1], 'pair': r[2], 'side': r[3],
                'leverage': r[4], 'collateral': r[5], 'entry_price': r[6],
                'stop_loss_pct': r[7], 'take_profit_pct': r[8],
                'confidence': r[9], 'signal_type': r[10], 'trade_id': r[11]
            })
        return positions

    def get_avantis_stats(self):
        """Get Avantis W/L/P&L summary."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM avantis_positions WHERE status = 'open'")
        open_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM avantis_positions WHERE status = 'closed'")
        closed = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM avantis_positions WHERE status = 'closed' AND pnl > 0")
        wins = c.fetchone()[0]
        c.execute("SELECT COALESCE(SUM(pnl), 0) FROM avantis_positions WHERE status = 'closed'")
        total_pnl = c.fetchone()[0]
        c.execute("SELECT COALESCE(SUM(collateral), 0) FROM avantis_positions WHERE status = 'open'")
        deployed = c.fetchone()[0]
        conn.close()
        losses = closed - wins
        win_rate = (wins / closed * 100) if closed > 0 else 0
        return {
            'open': open_count, 'closed': closed, 'wins': wins, 'losses': losses,
            'win_rate': win_rate, 'total_pnl': total_pnl, 'deployed': deployed
        }

    def log_heartbeat(self, heartbeat_type, bankr_raw=None, ai_reasoning=None,
                      ai_actions=None, positions_data=None, wallet_balance=None,
                      total_value=None):
        """Log a heartbeat event with AI reasoning."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            INSERT INTO heartbeat_logs
            (timestamp, heartbeat_type, bankr_raw, ai_reasoning, ai_actions,
             positions_data, wallet_balance, total_value)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now(), heartbeat_type, bankr_raw, ai_reasoning,
            json.dumps(ai_actions) if ai_actions else None,
            json.dumps(positions_data) if positions_data else None,
            wallet_balance, total_value
        ))
        conn.commit()
        conn.close()

    def run_daily_improvement(self):
        """Run daily self-improvement cycle."""
        improvements = []

        # Get today's performance
        today_roi = self.get_daily_roi()

        # Get category performance
        cat_perf = self.get_category_performance()

        # Get score performance
        score_perf = self.get_score_performance()

        print("\n" + "="*60)
        print("DAILY IMPROVEMENT CYCLE")
        print("="*60)

        # Analyze daily ROI
        print(f"\nToday's ROI: {today_roi['roi']*100:.1f}%")
        print(f"Target: 40%")
        print(f"Met target: {'✓ YES' if today_roi['met_target'] else '✗ NO'}")

        if today_roi['roi'] >= 0.40:
            print("🎯 CRUSHING IT! Maintaining strategy.")
        elif today_roi['roi'] >= 0.20:
            print("📈 Good progress. Minor optimizations.")
        else:
            print("⚠️ Below target. Need adjustments.")
            improvements.append({
                'type': 'low_roi',
                'action': 'increase_activity',
                'reason': f"ROI {today_roi['roi']*100:.1f}% below 40% target"
            })

        # Analyze category performance
        print("\n" + "="*60)
        print("CATEGORY PERFORMANCE")
        print("="*60)

        for cat, stats in sorted(cat_perf.items(),
                                 key=lambda x: x[1]['win_rate'],
                                 reverse=True):
            print(f"\n{cat.upper()}:")
            print(f"  Bets: {stats['total_bets']}")
            print(f"  Win Rate: {stats['win_rate']*100:.1f}%")
            print(f"  Profit: ${stats['total_profit']:.2f}")
            print(f"  Avg Score: {stats['avg_score']:.1f}")

            # Recommend adjustments
            if stats['win_rate'] >= 0.75:
                print(f"  💰 EXCELLENT - Increase bet size on {cat}")
                improvements.append({
                    'type': 'category_adjustment',
                    'category': cat,
                    'action': 'lower_threshold',
                    'old_value': 60,
                    'new_value': 55,
                    'reason': f"High win rate {stats['win_rate']*100:.1f}%"
                })
            elif stats['win_rate'] < 0.55:
                print(f"  ⚠️ WEAK - Raise standards for {cat}")
                improvements.append({
                    'type': 'category_adjustment',
                    'category': cat,
                    'action': 'raise_threshold',
                    'old_value': 60,
                    'new_value': 65,
                    'reason': f"Low win rate {stats['win_rate']*100:.1f}%"
                })

        # Analyze score performance
        print("\n" + "="*60)
        print("SCORE RANGE PERFORMANCE")
        print("="*60)

        for range_name, stats in sorted(score_perf.items()):
            print(f"\n{range_name}:")
            print(f"  Bets: {stats['total_bets']}")
            print(f"  Win Rate: {stats['win_rate']*100:.1f}%")
            print(f"  Avg Profit: ${stats['avg_profit']:.2f}")

            if range_name == '80-100' and stats['win_rate'] >= 0.80:
                print("  🔥 HIGH CONVICTION WINS - Trust these more")
                improvements.append({
                    'type': 'score_adjustment',
                    'score_range': range_name,
                    'action': 'increase_bet_size',
                    'reason': f"Strong performance {stats['win_rate']*100:.1f}%"
                })

        # Log improvements
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        for imp in improvements:
            c.execute("""
                INSERT INTO improvements
                (timestamp, improvement_type, old_value, new_value, reason, data)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                datetime.now(),
                imp['type'],
                imp.get('old_value'),
                imp.get('new_value'),
                imp['reason'],
                json.dumps(imp)
            ))

        conn.commit()
        conn.close()

        print("\n" + "="*60)
        print(f"IMPROVEMENTS LOGGED: {len(improvements)}")
        print("="*60)

        return improvements

    def get_summary(self):
        """Get overall performance summary."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        # Overall stats
        c.execute("""
            SELECT
                COUNT(*) as total_bets,
                SUM(won) as total_wins,
                SUM(profit) as total_profit,
                AVG(confidence_score) as avg_score
            FROM bets
            WHERE status = 'resolved'
        """)

        overall = c.fetchone()

        # Recent performance (last 7 days)
        c.execute("""
            SELECT
                DATE(timestamp) as date,
                SUM(profit) as daily_profit
            FROM bets
            WHERE status = 'resolved'
            AND timestamp >= datetime('now', '-7 days')
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
        """)

        recent = c.fetchall()

        conn.close()

        return {
            'total_bets': overall[0] or 0,
            'total_wins': overall[1] or 0,
            'win_rate': (overall[1] / overall[0]) if overall[0] > 0 else 0,
            'total_profit': overall[2] or 0,
            'avg_score': overall[3] or 0,
            'recent_days': [
                {'date': r[0], 'profit': r[1]} for r in recent
            ]
        }
