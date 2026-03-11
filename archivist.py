#!/usr/bin/env python3
"""
The Archivist — Baggins Capital's single database authority.

Every employee goes through The Archivist for ALL database operations.
No other module should import sqlite3 or connect to the DB directly.

Department: Data & Analytics
Reports to: The Manager
"""

import sqlite3
import json
import threading
from datetime import datetime, timedelta
from collections import defaultdict

# Schema version — bump this when adding migrations
SCHEMA_VERSION = 1


class Archivist:
    """The company's single database authority.

    Owns all schema, enforces data integrity, logs decision context,
    and provides a typed API for every DB operation.
    """

    def __init__(self, db_path='hedge_fund_performance.db'):
        self.db_path = db_path
        self._local = threading.local()
        self.initialize()

    # ══════════════════════════════════════════════════════════════
    # CONNECTION MANAGEMENT
    # ══════════════════════════════════════════════════════════════

    def _conn(self):
        """Get a thread-local database connection."""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path, timeout=30)
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA busy_timeout=30000")
        return self._local.conn

    def _execute(self, sql, params=(), commit=False):
        """Execute a single SQL statement. Returns cursor."""
        conn = self._conn()
        c = conn.cursor()
        c.execute(sql, params)
        if commit:
            conn.commit()
        return c

    def _executemany(self, sql, param_list, commit=True):
        """Execute SQL with multiple parameter sets."""
        conn = self._conn()
        c = conn.cursor()
        c.executemany(sql, param_list)
        if commit:
            conn.commit()
        return c

    def _fetchone(self, sql, params=()):
        """Execute and fetch one row."""
        return self._execute(sql, params).fetchone()

    def _fetchall(self, sql, params=()):
        """Execute and fetch all rows."""
        return self._execute(sql, params).fetchall()

    def _commit(self):
        """Commit the current transaction."""
        self._conn().commit()

    def close(self):
        """Close the thread-local connection."""
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

    # ══════════════════════════════════════════════════════════════
    # SCHEMA INITIALIZATION
    # ══════════════════════════════════════════════════════════════

    def initialize(self):
        """Create all tables and run migrations. Safe to call multiple times."""
        self._create_core_tables()
        self._create_weather_tables()
        self._create_sports_tables()
        self._create_trading_tables()
        self._create_financial_tables()
        self._create_analytics_tables()
        self._run_migrations()
        self._commit()

    def _create_core_tables(self):
        """Core bet tracking and agent state tables."""

        # ── Central bet ledger ──
        self._execute("""
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

        # ── Decision snapshots — full context for every bet (NEW) ──
        self._execute("""
            CREATE TABLE IF NOT EXISTS bet_decisions (
                bet_id INTEGER PRIMARY KEY,
                created_at DATETIME NOT NULL,
                category TEXT NOT NULL,
                raw_data TEXT,
                modifiers TEXT,
                decision TEXT,
                strategy TEXT,
                FOREIGN KEY (bet_id) REFERENCES bets(id)
            )
        """)

        # ── Persistent key-value state ──
        self._execute("""
            CREATE TABLE IF NOT EXISTS agent_state (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ── Schema version tracking ──
        self._execute("""
            CREATE TABLE IF NOT EXISTS schema_versions (
                version INTEGER PRIMARY KEY,
                applied_at DATETIME NOT NULL,
                description TEXT
            )
        """)

        # ── Heartbeat logs ──
        self._execute("""
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

        # ── Market scan logs ──
        self._execute("""
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

        # ── Improvement log ──
        self._execute("""
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

        # ── Strategy change log ──
        self._execute("""
            CREATE TABLE IF NOT EXISTS strategy_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                version TEXT,
                change_type TEXT,
                description TEXT,
                rationale TEXT,
                expected_impact TEXT
            )
        """)

        # ── Resolution audit trail ──
        self._execute("""
            CREATE TABLE IF NOT EXISTS bet_resolutions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bet_id INTEGER NOT NULL,
                resolved_at TEXT NOT NULL,
                resolved_by TEXT NOT NULL,
                won INTEGER,
                profit REAL,
                redeemed_amount REAL,
                actual_data TEXT,
                FOREIGN KEY (bet_id) REFERENCES bets(id)
            )
        """)

        # ── Portfolio check log ──
        self._execute("""
            CREATE TABLE IF NOT EXISTS portfolio_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                checked_at TEXT NOT NULL,
                portfolio_raw TEXT,
                redeem_response TEXT,
                pending_count INTEGER,
                resolved_count INTEGER DEFAULT 0
            )
        """)

    def _create_weather_tables(self):
        """Weather department tables."""

        self._execute("""
            CREATE TABLE IF NOT EXISTS weather_bets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bet_id INTEGER NOT NULL,
                city TEXT NOT NULL,
                temp_range TEXT,
                forecast_temps TEXT,
                weighted_mean REAL DEFAULT 0.0,
                FOREIGN KEY (bet_id) REFERENCES bets(id)
            )
        """)

        self._execute("""
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
                forecast_data TEXT,
                FOREIGN KEY (bet_id) REFERENCES bets(id)
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS weather_resolutions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bet_id INTEGER NOT NULL,
                predicted_high REAL NOT NULL,
                actual_high REAL NOT NULL,
                error REAL NOT NULL,
                won INTEGER,
                profit REAL,
                timestamp DATETIME,
                FOREIGN KEY (bet_id) REFERENCES bets(id)
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS weather_sources (
                city TEXT NOT NULL,
                source_name TEXT NOT NULL,
                credibility_weight REAL DEFAULT 1.0,
                total_predictions INTEGER DEFAULT 0,
                accurate_predictions INTEGER DEFAULT 0,
                avg_error REAL DEFAULT 0.0,
                last_updated TEXT,
                last_error_direction TEXT,
                consecutive_same_direction INTEGER DEFAULT 0,
                avg_bias REAL DEFAULT 0.0,
                bias_direction TEXT,
                bias_consistency REAL DEFAULT 0.0,
                PRIMARY KEY (city, source_name)
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS weather_city_patterns (
                city TEXT PRIMARY KEY,
                total_bets INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                win_rate REAL DEFAULT 0.0,
                total_profit REAL DEFAULT 0.0,
                last_updated TEXT
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS weather_side_patterns (
                city TEXT,
                side TEXT,
                total_bets INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                win_rate REAL DEFAULT 0.0,
                total_profit REAL DEFAULT 0.0,
                last_updated TEXT,
                PRIMARY KEY (city, side)
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS weather_calibration (
                confidence_bucket INTEGER PRIMARY KEY,
                total_bets INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                actual_win_rate REAL DEFAULT 0.0,
                last_updated TEXT
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS weather_heartbeat_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                health TEXT,
                reasoning TEXT,
                source_adjustments TEXT,
                city_recommendations TEXT,
                calibration_assessment TEXT,
                patterns_detected TEXT,
                concerns TEXT
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS historical_temperatures (
                city TEXT NOT NULL,
                month INTEGER NOT NULL,
                mean_high REAL NOT NULL,
                std_high REAL NOT NULL,
                mean_low REAL NOT NULL,
                std_low REAL NOT NULL,
                data_points INTEGER NOT NULL,
                PRIMARY KEY (city, month)
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS source_prediction_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bet_id INTEGER NOT NULL,
                city TEXT NOT NULL,
                source_name TEXT NOT NULL,
                predicted_high REAL NOT NULL,
                actual_high REAL,
                error REAL,
                bias REAL,
                market_date TEXT,
                logged_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (bet_id) REFERENCES bets(id)
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS forecast_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT NOT NULL,
                target_date TEXT NOT NULL,
                source TEXT NOT NULL,
                high_temp REAL,
                low_temp REAL,
                unit TEXT DEFAULT 'F',
                fetched_at TEXT NOT NULL,
                collection_run INTEGER DEFAULT 0
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS forecast_collection_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at TEXT NOT NULL,
                cities_scanned INTEGER DEFAULT 0,
                total_readings INTEGER DEFAULT 0,
                sources_failed TEXT
            )
        """)

        # Indexes for weather queries
        self._execute("CREATE INDEX IF NOT EXISTS idx_snapshots_city_date ON forecast_snapshots(city, target_date)")
        self._execute("CREATE INDEX IF NOT EXISTS idx_spl_city ON source_prediction_log(city)")
        self._execute("CREATE INDEX IF NOT EXISTS idx_spl_source ON source_prediction_log(source_name)")
        self._execute("CREATE INDEX IF NOT EXISTS idx_spl_bet ON source_prediction_log(bet_id)")

    def _create_sports_tables(self):
        """Sports department tables."""

        self._execute("""
            CREATE TABLE IF NOT EXISTS sports_markets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT,
                event_title TEXT,
                market_title TEXT,
                sport TEXT,
                polymarket_yes_price REAL,
                polymarket_no_price REAL,
                bookmaker_implied_prob REAL,
                edge REAL,
                scanned_at TEXT,
                bet_placed INTEGER DEFAULT 0,
                event_slug TEXT DEFAULT '',
                market_type TEXT DEFAULT '',
                confidence REAL DEFAULT 0,
                signals TEXT DEFAULT ''
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS sports_odds_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_title TEXT,
                bookmaker TEXT,
                outcome TEXT,
                odds REAL,
                implied_prob REAL,
                fetched_at TEXT
            )
        """)

    def _create_trading_tables(self):
        """Trading desk tables (Scalper, Leverage)."""

        self._execute("""
            CREATE TABLE IF NOT EXISTS updown_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT DEFAULT 'running',
                runtime_hours REAL,
                assets TEXT,
                budget REAL,
                bet_size REAL,
                min_score REAL,
                dry_run INTEGER DEFAULT 0,
                cycles_run INTEGER DEFAULT 0,
                bets_placed INTEGER DEFAULT 0,
                bets_won INTEGER DEFAULT 0,
                bets_lost INTEGER DEFAULT 0,
                bets_pending INTEGER DEFAULT 0,
                total_wagered REAL DEFAULT 0.0,
                total_returned REAL DEFAULT 0.0,
                net_pnl REAL DEFAULT 0.0,
                roi REAL DEFAULT 0.0
            )
        """)

        self._execute("""
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

    def _create_financial_tables(self):
        """Financial tracking tables (CFO domain)."""

        self._execute("""
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

        self._execute("""
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

    def _create_analytics_tables(self):
        """Performance analytics tables (Accountant domain)."""

        self._execute("""
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

        self._execute("""
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

        self._execute("""
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

        self._execute("""
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

    # ══════════════════════════════════════════════════════════════
    # SCHEMA MIGRATIONS
    # ══════════════════════════════════════════════════════════════

    def _run_migrations(self):
        """Run any pending schema migrations."""
        current = self._get_schema_version()

        if current < 1:
            self._migrate_v1()

    def _get_schema_version(self):
        """Get current schema version."""
        try:
            row = self._fetchone("SELECT MAX(version) FROM schema_versions")
            return row[0] if row and row[0] else 0
        except Exception:
            return 0

    def _migrate_v1(self):
        """v1: Add columns that were previously added via ad-hoc ALTER TABLE."""
        alterations = [
            "ALTER TABLE bets ADD COLUMN cycle_type TEXT DEFAULT 'short'",
            "ALTER TABLE bets ADD COLUMN trade_id TEXT",
            "ALTER TABLE bets ADD COLUMN resolved_by TEXT DEFAULT NULL",
            "ALTER TABLE bets ADD COLUMN bet_type TEXT DEFAULT NULL",
            "ALTER TABLE bets ADD COLUMN format_type TEXT DEFAULT 'unknown'",
            "ALTER TABLE weather_sources ADD COLUMN bias_direction TEXT",
            "ALTER TABLE weather_sources ADD COLUMN avg_bias REAL DEFAULT 0.0",
        ]
        for sql in alterations:
            try:
                self._execute(sql)
            except Exception:
                pass  # Column already exists

        # Index for bet lookups
        self._execute("CREATE INDEX IF NOT EXISTS idx_bets_market_side_pending ON bets(market_id, side, status)")
        self._execute("CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status)")
        self._execute("CREATE INDEX IF NOT EXISTS idx_bets_category ON bets(category)")
        self._execute("CREATE INDEX IF NOT EXISTS idx_bets_timestamp ON bets(timestamp)")
        self._execute("CREATE INDEX IF NOT EXISTS idx_bet_decisions_category ON bet_decisions(category)")

        self._execute(
            "INSERT INTO schema_versions (version, applied_at, description) VALUES (?, ?, ?)",
            (1, datetime.now().isoformat(), "Initial Archivist schema + bet_decisions table")
        )
        print("[ARCHIVIST] Migration v1 applied: core schema + bet_decisions + indexes")

    # ══════════════════════════════════════════════════════════════
    # BET RECORDING
    # ══════════════════════════════════════════════════════════════

    def record_bet(self, market_id, market_title, category, side, amount, odds,
                   confidence_score, edge, reasoning, balance_before,
                   cycle_type='crypto', bet_type=None, format_type='unknown',
                   decision_snapshot=None):
        """Record a new bet atomically — single insert, no two-phase hack.

        Args:
            market_id: Polymarket market ID
            market_title: Human-readable market title
            category: 'crypto', 'weather', 'updown', 'sports'
            side: 'YES', 'NO', 'UP', 'DOWN'
            amount: Bet size in USD
            odds: Market odds (0.0-1.0)
            confidence_score: 0-100 integer
            edge: Decimal edge (e.g. 0.15 for 15%)
            reasoning: Text reasoning string
            balance_before: Wallet balance before bet
            cycle_type: 'crypto', 'weather', 'updown', 'sports'
            bet_type: 'HOLD', 'FADE', 'SNAP', 'LOTTO', etc.
            format_type: 'touch', 'settlement', 'range', 'threshold_below', 'unknown'
            decision_snapshot: Dict with full decision context (raw_data, modifiers, decision, strategy)

        Returns:
            bet_id (int) or None if duplicate
        """
        # Dedup check
        row = self._fetchone(
            "SELECT COUNT(*) FROM bets WHERE market_id = ?",
            (str(market_id),)
        )
        if row[0] >= 1:
            print(f"[ARCHIVIST] Already bet on market {market_id} ({row[0]} existing)")
            return None

        # Single atomic insert with ALL fields
        self._execute("""
            INSERT INTO bets
            (timestamp, market_id, market_title, category, side, amount,
             odds, confidence_score, edge, reasoning, balance_before,
             cycle_type, bet_type, format_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now(), market_id, market_title, category, side, amount,
            odds, confidence_score, edge, reasoning, balance_before,
            cycle_type, bet_type or 'UNKNOWN', format_type
        ))

        bet_id = self._execute("SELECT last_insert_rowid()").fetchone()[0]

        # Store decision snapshot if provided
        if decision_snapshot and bet_id:
            self._record_decision(bet_id, category, decision_snapshot)

        self._commit()
        print(f"[ARCHIVIST] Bet #{bet_id} recorded: {category}/{cycle_type} {side} ${amount} on {market_title[:60]}")
        return bet_id

    def _record_decision(self, bet_id, category, snapshot):
        """Store the full decision context for a bet."""
        self._execute("""
            INSERT INTO bet_decisions (bet_id, created_at, category, raw_data, modifiers, decision, strategy)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            bet_id,
            datetime.now().isoformat(),
            category,
            json.dumps(snapshot.get('raw_data', {})),
            json.dumps(snapshot.get('modifiers', {})),
            json.dumps(snapshot.get('decision', {})),
            json.dumps(snapshot.get('strategy', {}))
        ))

    def bet_exists(self, market_id):
        """Check if a bet exists for this market."""
        row = self._fetchone(
            "SELECT COUNT(*) FROM bets WHERE market_id = ?",
            (str(market_id),)
        )
        return row[0] > 0

    def get_bet(self, bet_id):
        """Get a single bet by ID."""
        row = self._fetchone("SELECT * FROM bets WHERE id = ?", (bet_id,))
        if not row:
            return None
        cols = ['id', 'timestamp', 'market_id', 'market_title', 'category',
                'side', 'amount', 'odds', 'confidence_score', 'edge',
                'reasoning', 'status', 'resolved_at', 'won', 'profit',
                'balance_before', 'balance_after', 'cycle_type', 'trade_id',
                'resolved_by', 'bet_type', 'format_type']
        return dict(zip(cols, row))

    def get_pending_bets(self, category=None, max_age_hours=None):
        """Get all pending bets, optionally filtered."""
        sql = "SELECT * FROM bets WHERE status = 'pending'"
        params = []

        if category:
            sql += " AND category = ?"
            params.append(category)

        if max_age_hours:
            sql += " AND timestamp >= datetime('now', ?)"
            params.append(f"-{max_age_hours} hours")

        sql += " ORDER BY timestamp DESC"
        rows = self._fetchall(sql, tuple(params))

        cols = ['id', 'timestamp', 'market_id', 'market_title', 'category',
                'side', 'amount', 'odds', 'confidence_score', 'edge',
                'reasoning', 'status', 'resolved_at', 'won', 'profit',
                'balance_before', 'balance_after', 'cycle_type', 'trade_id',
                'resolved_by', 'bet_type', 'format_type']
        return [dict(zip(cols, row)) for row in rows]

    def get_pending_count(self, category=None):
        """Get count of pending bets."""
        if category:
            row = self._fetchone(
                "SELECT COUNT(*) FROM bets WHERE status = 'pending' AND category = ?",
                (category,)
            )
        else:
            row = self._fetchone("SELECT COUNT(*) FROM bets WHERE status = 'pending'")
        return row[0]

    def get_daily_bet_count(self, category=None, cycle_type=None):
        """Get number of bets placed today."""
        sql = "SELECT COUNT(*) FROM bets WHERE DATE(timestamp) = DATE('now')"
        params = []

        if category:
            sql += " AND category = ?"
            params.append(category)
        if cycle_type:
            sql += " AND cycle_type = ?"
            params.append(cycle_type)

        row = self._fetchone(sql, tuple(params))
        return row[0]

    # ══════════════════════════════════════════════════════════════
    # BET RESOLUTION
    # ══════════════════════════════════════════════════════════════

    def resolve_bet(self, bet_id, won, profit, balance_after, resolved_by=None):
        """Mark a bet as resolved with outcome."""
        self._execute("""
            UPDATE bets
            SET status = 'resolved',
                resolved_at = ?,
                won = ?,
                profit = ?,
                balance_after = ?,
                resolved_by = ?
            WHERE id = ?
        """, (datetime.now(), int(won), profit, balance_after,
              resolved_by, bet_id), commit=True)

        # Update performance stats
        self._update_performance_stats()
        print(f"[ARCHIVIST] Bet #{bet_id} resolved: {'WIN' if won else 'LOSS'} ${profit:+.2f}")

    def record_resolution_detail(self, bet_id, resolved_by, won, profit,
                                  redeemed_amount=None, actual_data=None):
        """Record detailed resolution audit trail."""
        self._execute("""
            INSERT INTO bet_resolutions
            (bet_id, resolved_at, resolved_by, won, profit, redeemed_amount, actual_data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (bet_id, datetime.now().isoformat(), resolved_by, int(won),
              profit, redeemed_amount, actual_data), commit=True)

    def record_portfolio_check(self, portfolio_raw, redeem_response,
                                pending_count, resolved_count=0):
        """Log a portfolio check cycle."""
        self._execute("""
            INSERT INTO portfolio_checks
            (checked_at, portfolio_raw, redeem_response, pending_count, resolved_count)
            VALUES (?, ?, ?, ?, ?)
        """, (datetime.now().isoformat(), portfolio_raw, redeem_response,
              pending_count, resolved_count), commit=True)

    def set_trade_id(self, bet_id, trade_id):
        """Set the Bankr trade_id for a bet."""
        self._execute(
            "UPDATE bets SET trade_id = ? WHERE id = ?",
            (trade_id, bet_id), commit=True
        )

    # ══════════════════════════════════════════════════════════════
    # DECISION SNAPSHOTS
    # ══════════════════════════════════════════════════════════════

    def get_decision(self, bet_id):
        """Get the full decision snapshot for a bet."""
        row = self._fetchone(
            "SELECT raw_data, modifiers, decision, strategy FROM bet_decisions WHERE bet_id = ?",
            (bet_id,)
        )
        if not row:
            return None
        return {
            'raw_data': json.loads(row[0]) if row[0] else {},
            'modifiers': json.loads(row[1]) if row[1] else {},
            'decision': json.loads(row[2]) if row[2] else {},
            'strategy': json.loads(row[3]) if row[3] else {},
        }

    def query_decisions(self, category=None, bet_type=None, format_type=None,
                        won=None, days=30, limit=100):
        """Query decision snapshots with filters. For Detective forensics."""
        sql = """
            SELECT b.id, b.market_title, b.side, b.amount, b.odds,
                   b.confidence_score, b.edge, b.won, b.profit,
                   b.cycle_type, b.bet_type, b.format_type,
                   d.raw_data, d.modifiers, d.decision, d.strategy
            FROM bets b
            LEFT JOIN bet_decisions d ON b.id = d.bet_id
            WHERE b.status = 'resolved'
            AND b.timestamp >= datetime('now', ?)
        """
        params = [f"-{days} days"]

        if category:
            sql += " AND b.category = ?"
            params.append(category)
        if bet_type:
            sql += " AND b.bet_type = ?"
            params.append(bet_type)
        if format_type:
            sql += " AND b.format_type = ?"
            params.append(format_type)
        if won is not None:
            sql += " AND b.won = ?"
            params.append(int(won))

        sql += " ORDER BY b.timestamp DESC LIMIT ?"
        params.append(limit)

        rows = self._fetchall(sql, tuple(params))
        results = []
        for r in rows:
            results.append({
                'bet_id': r[0], 'market_title': r[1], 'side': r[2],
                'amount': r[3], 'odds': r[4], 'confidence_score': r[5],
                'edge': r[6], 'won': r[7], 'profit': r[8],
                'cycle_type': r[9], 'bet_type': r[10], 'format_type': r[11],
                'raw_data': json.loads(r[12]) if r[12] else {},
                'modifiers': json.loads(r[13]) if r[13] else {},
                'decision': json.loads(r[14]) if r[14] else {},
                'strategy': json.loads(r[15]) if r[15] else {},
            })
        return results

    # ══════════════════════════════════════════════════════════════
    # WEATHER DATA
    # ══════════════════════════════════════════════════════════════

    def record_weather_bet(self, bet_id, city, temp_range, forecast_temps, weighted_mean):
        """Store weather bet metadata for resolution matching."""
        try:
            self._execute("""
                INSERT INTO weather_bets (bet_id, city, temp_range, forecast_temps, weighted_mean)
                VALUES (?, ?, ?, ?, ?)
            """, (bet_id, city, temp_range,
                  json.dumps(forecast_temps) if isinstance(forecast_temps, dict) else str(forecast_temps),
                  weighted_mean), commit=True)
        except Exception as e:
            print(f"[ARCHIVIST] record_weather_bet error: {e}")

    def record_weather_prediction(self, bet_id, city, market_date, forecasts,
                                   weighted_mean, our_probability, edge):
        """Store per-source forecast data when a weather bet is placed."""
        try:
            forecast_data = {}
            for f in forecasts:
                src = f.get("source", "unknown")
                forecast_data[src] = {
                    "high_temp": f.get("high_temp", 0),
                    "low_temp": f.get("low_temp", 0),
                }

            open_meteo = next((f["high_temp"] for f in forecasts if f.get("source") == "open_meteo"), None)
            noaa = next((f["high_temp"] for f in forecasts if f.get("source") == "noaa"), None)
            weatherapi = next((f["high_temp"] for f in forecasts if f.get("source") == "weatherapi"), None)

            self._execute("""
                INSERT INTO weather_predictions
                (bet_id, city, market_date, open_meteo_high, noaa_high, weatherapi_high,
                 weighted_mean, our_probability, edge, timestamp, forecast_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (bet_id, city, market_date, open_meteo, noaa, weatherapi,
                  weighted_mean, our_probability, edge,
                  datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                  json.dumps(forecast_data)), commit=True)
        except Exception as e:
            print(f"[ARCHIVIST] record_weather_prediction error: {e}")

    def record_weather_resolution(self, bet_id, predicted_high, actual_high, error):
        """Log weather resolution for credibility tracking."""
        row = self._fetchone("SELECT won, profit FROM bets WHERE id = ?", (bet_id,))
        won = row[0] if row else 0
        profit = row[1] if row else 0.0

        self._execute("""
            INSERT INTO weather_resolutions
            (bet_id, predicted_high, actual_high, error, won, profit, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (bet_id, predicted_high, actual_high, error, won, profit,
              datetime.now().strftime('%Y-%m-%d %H:%M:%S')), commit=True)

    def record_forecast_snapshot(self, city, target_date, source, high_temp,
                                  low_temp=None, unit='F', collection_run=0):
        """Store a single weather API reading."""
        self._execute("""
            INSERT INTO forecast_snapshots
            (city, target_date, source, high_temp, low_temp, unit, fetched_at, collection_run)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (city, target_date, source, high_temp, low_temp, unit,
              datetime.now().strftime('%Y-%m-%d %H:%M:%S'), collection_run), commit=True)

    def get_latest_forecasts(self, city, target_date):
        """Get the most recent forecast from each source for a city+date."""
        rows = self._fetchall("""
            SELECT source, high_temp, low_temp, unit, fetched_at
            FROM forecast_snapshots
            WHERE city = ? AND target_date = ?
            ORDER BY fetched_at DESC
        """, (city, target_date))

        # Deduplicate: keep latest per source
        seen = {}
        results = []
        for r in rows:
            if r[0] not in seen:
                seen[r[0]] = True
                results.append({
                    'source': r[0], 'high_temp': r[1], 'low_temp': r[2],
                    'unit': r[3], 'fetched_at': r[4]
                })
        return results

    def log_forecast_collection(self, cities_scanned, total_readings, sources_failed=None):
        """Log a forecast collection run."""
        self._execute("""
            INSERT INTO forecast_collection_log (run_at, cities_scanned, total_readings, sources_failed)
            VALUES (?, ?, ?, ?)
        """, (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
              cities_scanned, total_readings, sources_failed), commit=True)

    def get_source_stats(self, city, source_name):
        """Get credibility stats for a weather source in a city."""
        row = self._fetchone("""
            SELECT credibility_weight, total_predictions, accurate_predictions,
                   avg_error, avg_bias, bias_direction, bias_consistency
            FROM weather_sources WHERE city = ? AND source_name = ?
        """, (city, source_name))
        if not row:
            return None
        return {
            'credibility_weight': row[0], 'total_predictions': row[1],
            'accurate_predictions': row[2], 'avg_error': row[3],
            'avg_bias': row[4], 'bias_direction': row[5],
            'bias_consistency': row[6]
        }

    def get_source_prediction_log(self, city, source_name, limit=30):
        """Get recent prediction errors for recency-weighted credibility."""
        rows = self._fetchall("""
            SELECT error FROM source_prediction_log
            WHERE city = ? AND source_name = ?
            ORDER BY logged_at DESC LIMIT ?
        """, (city, source_name, limit))
        return [r[0] for r in rows]

    def get_source_weight(self, city, source_name):
        """Get static credibility weight for a source."""
        row = self._fetchone(
            "SELECT credibility_weight FROM weather_sources WHERE city = ? AND source_name = ?",
            (city, source_name)
        )
        return row[0] if row else 1.0

    def update_source_credibility(self, city, source_name, credibility_weight=None,
                                   total_predictions=None, accurate_predictions=None,
                                   avg_error=None, last_error_direction=None,
                                   consecutive_same_direction=None,
                                   avg_bias=None, bias_direction=None, bias_consistency=None):
        """Update weather source credibility stats."""
        # Upsert: insert or update
        existing = self._fetchone(
            "SELECT 1 FROM weather_sources WHERE city = ? AND source_name = ?",
            (city, source_name)
        )

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if existing:
            updates = []
            params = []
            for col, val in [
                ('credibility_weight', credibility_weight),
                ('total_predictions', total_predictions),
                ('accurate_predictions', accurate_predictions),
                ('avg_error', avg_error),
                ('last_error_direction', last_error_direction),
                ('consecutive_same_direction', consecutive_same_direction),
                ('avg_bias', avg_bias),
                ('bias_direction', bias_direction),
                ('bias_consistency', bias_consistency),
            ]:
                if val is not None:
                    updates.append(f"{col} = ?")
                    params.append(val)

            if updates:
                updates.append("last_updated = ?")
                params.append(now)
                params.extend([city, source_name])
                self._execute(
                    f"UPDATE weather_sources SET {', '.join(updates)} WHERE city = ? AND source_name = ?",
                    tuple(params), commit=True
                )
        else:
            self._execute("""
                INSERT INTO weather_sources
                (city, source_name, credibility_weight, total_predictions,
                 accurate_predictions, avg_error, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (city, source_name, credibility_weight or 1.0,
                  total_predictions or 0, accurate_predictions or 0,
                  avg_error or 0.0, now), commit=True)

    def get_city_patterns(self, city):
        """Get city win rate patterns."""
        row = self._fetchone(
            "SELECT total_bets, wins, win_rate, total_profit FROM weather_city_patterns WHERE city = ?",
            (city.lower(),)
        )
        if not row:
            return {'total_bets': 0, 'wins': 0, 'win_rate': 0.0, 'total_profit': 0.0}
        return {
            'total_bets': row[0], 'wins': row[1],
            'win_rate': row[2], 'total_profit': row[3]
        }

    def get_side_patterns(self, city, side):
        """Get city+side win rate patterns."""
        row = self._fetchone(
            "SELECT total_bets, wins, win_rate, total_profit FROM weather_side_patterns WHERE city = ? AND side = ?",
            (city.lower(), side.lower())
        )
        if not row:
            return {'total_bets': 0, 'wins': 0, 'win_rate': 0.0, 'total_profit': 0.0}
        return {
            'total_bets': row[0], 'wins': row[1],
            'win_rate': row[2], 'total_profit': row[3]
        }

    def update_city_patterns(self, city, total_bets, wins, win_rate, total_profit):
        """Update city win rate pattern."""
        self._execute("""
            INSERT INTO weather_city_patterns (city, total_bets, wins, win_rate, total_profit, last_updated)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(city) DO UPDATE SET
                total_bets = ?, wins = ?, win_rate = ?, total_profit = ?, last_updated = ?
        """, (city.lower(), total_bets, wins, win_rate, total_profit,
              datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
              total_bets, wins, win_rate, total_profit,
              datetime.now().strftime('%Y-%m-%d %H:%M:%S')), commit=True)

    def update_side_patterns(self, city, side, total_bets, wins, win_rate, total_profit):
        """Update city+side win rate pattern."""
        self._execute("""
            INSERT INTO weather_side_patterns (city, side, total_bets, wins, win_rate, total_profit, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(city, side) DO UPDATE SET
                total_bets = ?, wins = ?, win_rate = ?, total_profit = ?, last_updated = ?
        """, (city.lower(), side.lower(), total_bets, wins, win_rate, total_profit,
              datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
              total_bets, wins, win_rate, total_profit,
              datetime.now().strftime('%Y-%m-%d %H:%M:%S')), commit=True)

    def get_calibration_data(self, confidence_bucket):
        """Get weather calibration data for a confidence bucket."""
        row = self._fetchone(
            "SELECT total_bets, wins, actual_win_rate FROM weather_calibration WHERE confidence_bucket = ?",
            (confidence_bucket,)
        )
        if not row:
            return {'total_bets': 0, 'wins': 0, 'actual_win_rate': 0.0}
        return {'total_bets': row[0], 'wins': row[1], 'actual_win_rate': row[2]}

    def get_source_count(self, city):
        """Get number of distinct weather sources for a city."""
        row = self._fetchone(
            "SELECT COUNT(DISTINCT source_name) FROM weather_sources WHERE city = ?",
            (city.lower(),)
        )
        return row[0] if row else 0

    def get_weather_bet_info(self, bet_id):
        """Get weather bet metadata for resolution."""
        row = self._fetchone(
            "SELECT city, temp_range, forecast_temps, weighted_mean FROM weather_bets WHERE bet_id = ?",
            (bet_id,)
        )
        if not row:
            return None
        return {
            'city': row[0], 'temp_range': row[1],
            'forecast_temps': row[2], 'weighted_mean': row[3]
        }

    # ══════════════════════════════════════════════════════════════
    # CRYPTO DATA (for Crypto Trader modifiers)
    # ══════════════════════════════════════════════════════════════

    def get_crypto_bets(self, days=30, cycle_type='crypto'):
        """Get resolved crypto bets for modifier calculation."""
        rows = self._fetchall("""
            SELECT id, timestamp, market_title, side, won, profit,
                   confidence_score, edge, bet_type, format_type
            FROM bets
            WHERE status = 'resolved'
            AND category = 'crypto'
            AND cycle_type = ?
            AND timestamp >= datetime('now', ?)
            ORDER BY timestamp DESC
        """, (cycle_type, f"-{days} days"))

        return [{
            'id': r[0], 'timestamp': r[1], 'market_title': r[2],
            'side': r[3], 'won': r[4], 'profit': r[5],
            'confidence_score': r[6], 'edge': r[7],
            'bet_type': r[8], 'format_type': r[9]
        } for r in rows]

    def get_hourly_win_rates(self, days=30):
        """Get win rates per hour for crypto modifier. Returns raw bet data
        with timestamps for the caller to convert to ET hours."""
        rows = self._fetchall("""
            SELECT timestamp, won
            FROM bets
            WHERE status = 'resolved'
            AND category = 'crypto'
            AND cycle_type = 'crypto'
            AND timestamp >= datetime('now', ?)
        """, (f"-{days} days",))
        return [{'timestamp': r[0], 'won': r[1]} for r in rows]

    def get_asset_win_rate(self, asset_pattern, days=30):
        """Get win rate for a specific asset (matched via LIKE on market_title)."""
        row = self._fetchone("""
            SELECT COUNT(*) as total, SUM(won) as wins
            FROM bets
            WHERE status = 'resolved'
            AND category = 'crypto'
            AND cycle_type = 'crypto'
            AND market_title LIKE ?
            AND timestamp >= datetime('now', ?)
        """, (f"%{asset_pattern}%", f"-{days} days"))
        total = row[0] if row else 0
        wins = row[1] if row else 0
        return {'total': total, 'wins': wins or 0,
                'win_rate': (wins / total) if total > 0 else 0.0}

    def get_baseline_crypto_wr(self, days=30):
        """Get baseline crypto win rate for asset modifier comparison."""
        row = self._fetchone("""
            SELECT COUNT(*), SUM(won)
            FROM bets
            WHERE status = 'resolved'
            AND category = 'crypto'
            AND cycle_type = 'crypto'
            AND timestamp >= datetime('now', ?)
        """, (f"-{days} days",))
        total = row[0] if row else 0
        wins = row[1] if row else 0
        return (wins / total) if total > 0 else 0.5

    # ══════════════════════════════════════════════════════════════
    # PERFORMANCE ANALYTICS
    # ══════════════════════════════════════════════════════════════

    def get_daily_roi(self, date=None):
        """Calculate ROI for a specific day."""
        if date is None:
            date = datetime.now().date()

        rows = self._fetchall("""
            SELECT balance_before, balance_after, profit
            FROM bets
            WHERE DATE(timestamp) = ?
            AND status = 'resolved'
            ORDER BY timestamp ASC
        """, (date,))

        if not rows:
            return {
                'date': date, 'starting_balance': None,
                'ending_balance': None, 'profit': 0,
                'roi': 0, 'total_bets': 0
            }

        starting_balance = rows[0][0]
        ending_balance = rows[-1][1]
        total_profit = sum(b[2] for b in rows)
        roi = (total_profit / starting_balance) if starting_balance and starting_balance > 0 else 0

        return {
            'date': date, 'starting_balance': starting_balance,
            'ending_balance': ending_balance, 'profit': total_profit,
            'roi': roi, 'total_bets': len(rows),
            'met_target': roi >= 0.40
        }

    def get_category_performance(self):
        """Get performance breakdown by category."""
        rows = self._fetchall("""
            SELECT category, COUNT(*), SUM(won),
                   CAST(SUM(won) AS REAL) / COUNT(*),
                   SUM(profit), AVG(confidence_score)
            FROM bets
            WHERE status = 'resolved'
            GROUP BY category
        """)

        results = {}
        for r in rows:
            results[r[0]] = {
                'total_bets': r[1], 'wins': r[2],
                'win_rate': r[3], 'total_profit': r[4],
                'avg_score': r[5]
            }
        return results

    def get_score_performance(self):
        """Get performance by confidence score range."""
        score_ranges = [('60-69', 60, 69), ('70-79', 70, 79),
                        ('80-89', 80, 89), ('90-100', 90, 100)]
        results = {}
        for range_name, min_s, max_s in score_ranges:
            row = self._fetchone("""
                SELECT COUNT(*), SUM(won),
                       CAST(SUM(won) AS REAL) / COUNT(*),
                       AVG(profit)
                FROM bets
                WHERE status = 'resolved'
                AND confidence_score >= ? AND confidence_score <= ?
            """, (min_s, max_s))

            if row and row[0] > 0:
                results[range_name] = {
                    'total_bets': row[0], 'wins': row[1],
                    'win_rate': row[2], 'avg_profit': row[3]
                }
        return results

    def _update_performance_stats(self):
        """Update category and score performance tables after resolution."""
        conn = self._conn()

        # Update category performance
        self._execute("DELETE FROM category_performance")
        cat_perf = self.get_category_performance()
        for cat, stats in cat_perf.items():
            self._execute("""
                INSERT INTO category_performance
                (category, total_bets, winning_bets, win_rate, total_profit,
                 avg_score, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (cat, stats['total_bets'], stats['wins'], stats['win_rate'],
                  stats['total_profit'], stats['avg_score'], datetime.now()))

        # Update score performance
        self._execute("DELETE FROM score_performance")
        score_perf = self.get_score_performance()
        for range_name, stats in score_perf.items():
            self._execute("""
                INSERT INTO score_performance
                (score_range, total_bets, winning_bets, win_rate, avg_profit, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (range_name, stats['total_bets'], stats['wins'],
                  stats['win_rate'], stats['avg_profit'], datetime.now()))

        self._commit()

    def save_daily_performance(self, starting_balance, ending_balance):
        """Save today's performance to daily_performance table."""
        today = datetime.now().date()

        row = self._fetchone("""
            SELECT COUNT(*), SUM(won), SUM(profit)
            FROM bets WHERE DATE(timestamp) = ? AND status = 'resolved'
        """, (today,))

        total_bets = row[0] or 0
        winning_bets = row[1] or 0
        total_profit = row[2] or 0.0
        win_rate = (winning_bets / total_bets) if total_bets > 0 else 0.0
        roi = (total_profit / starting_balance) if starting_balance > 0 else 0.0
        target_roi = 0.05
        met_target = 1 if roi >= target_roi else 0

        self._execute("""
            INSERT INTO daily_performance
            (date, starting_balance, ending_balance, total_bets, winning_bets,
             win_rate, profit_loss, roi, target_roi, met_target)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                ending_balance = ?, total_bets = ?, winning_bets = ?,
                win_rate = ?, profit_loss = ?, roi = ?, met_target = ?
        """, (today, starting_balance, ending_balance, total_bets, winning_bets,
              win_rate, total_profit, roi, target_roi, met_target,
              ending_balance, total_bets, winning_bets,
              win_rate, total_profit, roi, met_target), commit=True)

        print(f"[ARCHIVIST] Daily performance saved: {total_bets} bets, ${total_profit:+.2f}, ROI {roi:+.1%}")

    def save_portfolio_snapshot(self, available, deployed, positions, pending,
                                 daily_roi=None, consec_losses=0):
        """Save a point-in-time portfolio snapshot."""
        self._execute("""
            INSERT INTO portfolio_snapshots
            (timestamp, available_balance, deployed_balance, total_value,
             active_positions, pending_bets, daily_roi, consecutive_losses)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (datetime.now(), available, deployed, available + deployed,
              positions, pending, daily_roi, consec_losses), commit=True)

    def get_summary(self):
        """Get overall performance summary."""
        overall = self._fetchone("""
            SELECT COUNT(*), SUM(won), SUM(profit), AVG(confidence_score)
            FROM bets WHERE status = 'resolved'
        """)

        recent = self._fetchall("""
            SELECT DATE(timestamp), SUM(profit)
            FROM bets
            WHERE status = 'resolved' AND timestamp >= datetime('now', '-7 days')
            GROUP BY DATE(timestamp) ORDER BY DATE(timestamp) DESC
        """)

        return {
            'total_bets': overall[0] or 0,
            'total_wins': overall[1] or 0,
            'win_rate': (overall[1] / overall[0]) if overall[0] and overall[0] > 0 else 0,
            'total_profit': overall[2] or 0,
            'avg_score': overall[3] or 0,
            'recent_days': [{'date': r[0], 'profit': r[1]} for r in recent]
        }

    # ══════════════════════════════════════════════════════════════
    # STATE MANAGEMENT
    # ══════════════════════════════════════════════════════════════

    def get_state(self, key, default=None):
        """Get a persistent agent state value."""
        row = self._fetchone("SELECT value FROM agent_state WHERE key = ?", (key,))
        return row[0] if row else default

    def set_state(self, key, value):
        """Set a persistent agent state value."""
        self._execute("""
            INSERT INTO agent_state (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = ?, updated_at = ?
        """, (key, str(value), datetime.now(), str(value), datetime.now()),
            commit=True)

    # ══════════════════════════════════════════════════════════════
    # MARKET SCANS & HEARTBEAT
    # ══════════════════════════════════════════════════════════════

    def log_market_scan(self, markets_found, above_threshold, bets_placed,
                         short_open=None, long_open=None, skipped_reasons=None):
        """Log a market scan cycle result."""
        self._execute("""
            INSERT INTO market_scans
            (timestamp, markets_found, markets_above_threshold, bets_placed,
             short_slots_open, long_slots_open, skipped_reasons)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (datetime.now(), markets_found, above_threshold, bets_placed,
              short_open, long_open,
              json.dumps(skipped_reasons) if skipped_reasons else None), commit=True)

    def log_heartbeat(self, heartbeat_type, bankr_raw=None, ai_reasoning=None,
                       ai_actions=None, positions_data=None, wallet_balance=None,
                       total_value=None):
        """Log a heartbeat event with AI reasoning."""
        self._execute("""
            INSERT INTO heartbeat_logs
            (timestamp, heartbeat_type, bankr_raw, ai_reasoning, ai_actions,
             positions_data, wallet_balance, total_value)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (datetime.now(), heartbeat_type, bankr_raw, ai_reasoning,
              json.dumps(ai_actions) if ai_actions else None,
              json.dumps(positions_data) if positions_data else None,
              wallet_balance, total_value), commit=True)

    def log_weather_heartbeat(self, health, reasoning, source_adjustments=None,
                               city_recommendations=None, calibration_assessment=None,
                               patterns_detected=None, concerns=None):
        """Log a weather-specific heartbeat."""
        self._execute("""
            INSERT INTO weather_heartbeat_logs
            (timestamp, health, reasoning, source_adjustments, city_recommendations,
             calibration_assessment, patterns_detected, concerns)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
              health, reasoning, source_adjustments, city_recommendations,
              calibration_assessment, patterns_detected, concerns), commit=True)

    # ══════════════════════════════════════════════════════════════
    # SPORTS DATA
    # ══════════════════════════════════════════════════════════════

    def record_sports_market(self, market_id, event_title, market_title, sport,
                              yes_price, no_price, bookmaker_prob=None, edge=None,
                              event_slug='', market_type='', confidence=0, signals=''):
        """Record a scanned sports market."""
        self._execute("""
            INSERT INTO sports_markets
            (market_id, event_title, market_title, sport,
             polymarket_yes_price, polymarket_no_price, bookmaker_implied_prob,
             edge, scanned_at, event_slug, market_type, confidence, signals)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (market_id, event_title, market_title, sport,
              yes_price, no_price, bookmaker_prob, edge,
              datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
              event_slug, market_type, confidence, signals), commit=True)

    def record_odds_snapshot(self, event_title, bookmaker, outcome, odds, implied_prob):
        """Record bookmaker odds snapshot."""
        self._execute("""
            INSERT INTO sports_odds_snapshots
            (event_title, bookmaker, outcome, odds, implied_prob, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (event_title, bookmaker, outcome, odds, implied_prob,
              datetime.now().strftime('%Y-%m-%d %H:%M:%S')), commit=True)

    # ══════════════════════════════════════════════════════════════
    # SCALPER (UPDOWN SESSIONS)
    # ══════════════════════════════════════════════════════════════

    def record_updown_session(self, assets, budget, bet_size, min_score,
                               runtime_hours=None, dry_run=False):
        """Start a new updown trading session."""
        self._execute("""
            INSERT INTO updown_sessions
            (started_at, status, runtime_hours, assets, budget, bet_size, min_score, dry_run)
            VALUES (?, 'running', ?, ?, ?, ?, ?, ?)
        """, (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
              runtime_hours, assets, budget, bet_size, min_score,
              1 if dry_run else 0), commit=True)

        return self._execute("SELECT last_insert_rowid()").fetchone()[0]

    def update_updown_session(self, session_id, **kwargs):
        """Update updown session stats."""
        updates = []
        params = []
        for k, v in kwargs.items():
            updates.append(f"{k} = ?")
            params.append(v)

        if updates:
            params.append(session_id)
            self._execute(
                f"UPDATE updown_sessions SET {', '.join(updates)} WHERE id = ?",
                tuple(params), commit=True
            )

    def close_updown_session(self, session_id):
        """Close an updown session."""
        self._execute("""
            UPDATE updown_sessions SET status = 'completed', ended_at = ?
            WHERE id = ?
        """, (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), session_id),
            commit=True)

    def get_active_updown_sessions(self):
        """Get running updown sessions."""
        rows = self._fetchall(
            "SELECT * FROM updown_sessions WHERE status = 'running' ORDER BY started_at DESC"
        )
        return rows

    # ══════════════════════════════════════════════════════════════
    # FINANCIAL (CFO / Withdrawals / Take Profits)
    # ══════════════════════════════════════════════════════════════

    def log_withdrawal(self, amount, from_chain, to_chain, purpose, tx_hash=None):
        """Log a fund withdrawal."""
        self._execute("""
            INSERT INTO withdrawals (timestamp, amount, from_chain, to_chain, purpose, tx_hash)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (datetime.now(), amount, from_chain, to_chain, purpose, tx_hash),
            commit=True)
        print(f"[ARCHIVIST] Withdrawal: ${amount:.2f} {from_chain} -> {to_chain} ({purpose})")

    def get_today_withdrawals(self):
        """Get total withdrawn today."""
        row = self._fetchone("""
            SELECT COALESCE(SUM(amount), 0) FROM withdrawals
            WHERE DATE(timestamp) = DATE('now')
        """)
        return row[0]

    def log_take_profit(self, amount, source, destination, reason,
                         balance_before=None, balance_after=None, tx_hash=None):
        """Record a profit-taking event."""
        self._execute("""
            INSERT INTO take_profits
            (timestamp, amount, source, destination, reason, balance_before, balance_after, tx_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (datetime.now(), amount, source, destination, reason,
              balance_before, balance_after, tx_hash), commit=True)
        print(f"[ARCHIVIST] Take profit: ${amount:.2f} {source} -> {destination} ({reason})")

    # ══════════════════════════════════════════════════════════════
    # AVANTIS (LEVERAGE TRADING)
    # ══════════════════════════════════════════════════════════════

    def log_avantis_position(self, pair, side, leverage, collateral, entry_price,
                              stop_loss_pct, take_profit_pct, confidence, signal_type,
                              reasoning, trade_id=None):
        """Open a new Avantis position record."""
        self._execute("""
            INSERT INTO avantis_positions
            (timestamp, pair, side, leverage, collateral, entry_price,
             stop_loss_pct, take_profit_pct, confidence, signal_type, reasoning, trade_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (datetime.now(), pair, side, leverage, collateral, entry_price,
              stop_loss_pct, take_profit_pct, confidence, signal_type,
              reasoning, trade_id), commit=True)

        pos_id = self._execute("SELECT last_insert_rowid()").fetchone()[0]
        print(f"[ARCHIVIST] Position #{pos_id}: {leverage}x {side.upper()} {pair} @ ${entry_price:,.2f}")
        return pos_id

    def close_avantis_position(self, position_id, exit_price, pnl, pnl_pct, exit_reason):
        """Close an Avantis position."""
        self._execute("""
            UPDATE avantis_positions
            SET status = 'closed', closed_at = ?, exit_price = ?,
                pnl = ?, pnl_pct = ?, exit_reason = ?
            WHERE id = ?
        """, (datetime.now(), exit_price, pnl, pnl_pct, exit_reason, position_id),
            commit=True)

        won = "WIN" if pnl > 0 else "LOSS"
        print(f"[ARCHIVIST] Position #{position_id} CLOSED ({won}): ${pnl:+.2f} ({pnl_pct:+.1f}%)")

    def get_open_avantis_positions(self):
        """Get all open Avantis positions."""
        rows = self._fetchall("""
            SELECT id, timestamp, pair, side, leverage, collateral, entry_price,
                   stop_loss_pct, take_profit_pct, confidence, signal_type, trade_id
            FROM avantis_positions WHERE status = 'open'
            ORDER BY timestamp DESC
        """)
        return [{
            'id': r[0], 'timestamp': r[1], 'pair': r[2], 'side': r[3],
            'leverage': r[4], 'collateral': r[5], 'entry_price': r[6],
            'stop_loss_pct': r[7], 'take_profit_pct': r[8],
            'confidence': r[9], 'signal_type': r[10], 'trade_id': r[11]
        } for r in rows]

    def get_avantis_stats(self):
        """Get Avantis W/L/P&L summary."""
        open_count = self._fetchone("SELECT COUNT(*) FROM avantis_positions WHERE status = 'open'")[0]
        closed = self._fetchone("SELECT COUNT(*) FROM avantis_positions WHERE status = 'closed'")[0]
        wins = self._fetchone("SELECT COUNT(*) FROM avantis_positions WHERE status = 'closed' AND pnl > 0")[0]
        total_pnl = self._fetchone("SELECT COALESCE(SUM(pnl), 0) FROM avantis_positions WHERE status = 'closed'")[0]
        deployed = self._fetchone("SELECT COALESCE(SUM(collateral), 0) FROM avantis_positions WHERE status = 'open'")[0]

        return {
            'open': open_count, 'closed': closed, 'wins': wins,
            'losses': closed - wins,
            'win_rate': (wins / closed * 100) if closed > 0 else 0,
            'total_pnl': total_pnl, 'deployed': deployed
        }

    # ══════════════════════════════════════════════════════════════
    # STRATEGY LOG
    # ══════════════════════════════════════════════════════════════

    def log_strategy_change(self, version, change_type, description,
                             rationale=None, expected_impact=None):
        """Record a strategy change for audit trail."""
        self._execute("""
            INSERT INTO strategy_log (timestamp, version, change_type, description, rationale, expected_impact)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (datetime.now(), version, change_type, description,
              rationale, expected_impact), commit=True)

    # ══════════════════════════════════════════════════════════════
    # IMPROVEMENT CYCLE
    # ══════════════════════════════════════════════════════════════

    def log_improvement(self, improvement_type, reason, old_value=None,
                         new_value=None, data=None):
        """Log an improvement recommendation."""
        self._execute("""
            INSERT INTO improvements (timestamp, improvement_type, old_value, new_value, reason, data)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (datetime.now(), improvement_type, old_value, new_value,
              reason, json.dumps(data) if data else None), commit=True)

    def run_daily_improvement(self):
        """Run daily self-improvement cycle. Returns list of improvements."""
        improvements = []
        today_roi = self.get_daily_roi()
        cat_perf = self.get_category_performance()
        score_perf = self.get_score_performance()

        print("\n" + "=" * 60)
        print("DAILY IMPROVEMENT CYCLE")
        print("=" * 60)

        print(f"\nToday's ROI: {today_roi['roi']*100:.1f}%")
        print(f"Target: 5%")
        print(f"Met target: {'YES' if today_roi.get('met_target') else 'NO'}")

        if today_roi['roi'] < 0.05:
            improvements.append({
                'type': 'low_roi',
                'action': 'increase_activity',
                'reason': f"ROI {today_roi['roi']*100:.1f}% below 5% target"
            })

        print("\n" + "=" * 60)
        print("CATEGORY PERFORMANCE")
        print("=" * 60)

        for cat, stats in sorted(cat_perf.items(),
                                  key=lambda x: x[1]['win_rate'], reverse=True):
            print(f"\n{cat.upper()}:")
            print(f"  Bets: {stats['total_bets']}, WR: {stats['win_rate']*100:.1f}%, P&L: ${stats['total_profit']:.2f}")

            if stats['win_rate'] >= 0.75:
                improvements.append({
                    'type': 'category_adjustment', 'category': cat,
                    'action': 'lower_threshold',
                    'reason': f"High win rate {stats['win_rate']*100:.1f}%"
                })
            elif stats['win_rate'] < 0.55:
                improvements.append({
                    'type': 'category_adjustment', 'category': cat,
                    'action': 'raise_threshold',
                    'reason': f"Low win rate {stats['win_rate']*100:.1f}%"
                })

        print("\n" + "=" * 60)
        print("SCORE RANGE PERFORMANCE")
        print("=" * 60)

        for range_name, stats in sorted(score_perf.items()):
            print(f"  {range_name}: {stats['total_bets']} bets, "
                  f"WR {stats['win_rate']*100:.1f}%, avg P&L ${stats['avg_profit']:.2f}")

        # Log all improvements
        for imp in improvements:
            self.log_improvement(
                imp['type'], imp['reason'],
                data=imp
            )

        print(f"\nIMPROVEMENTS LOGGED: {len(improvements)}")
        print("=" * 60)

        return improvements

    # ══════════════════════════════════════════════════════════════
    # BACKWARD COMPATIBILITY — PerformanceTracker interface
    # ══════════════════════════════════════════════════════════════
    # These methods maintain the old PerformanceTracker API so we can
    # swap in the Archivist without changing every caller at once.

    def log_bet(self, market_id, market_title, category, side, amount, odds,
                score, edge, reasoning, balance_before):
        """COMPAT: Old PerformanceTracker.log_bet() interface.
        Delegates to record_bet() without decision snapshot."""
        return self.record_bet(
            market_id=market_id, market_title=market_title,
            category=category, side=side, amount=amount, odds=odds,
            confidence_score=score, edge=edge, reasoning=reasoning,
            balance_before=balance_before,
            cycle_type='crypto',  # Will be overwritten by Manager's UPDATE
            bet_type=None, format_type='unknown'
        )

    def log_weather_bet(self, bet_id, city, temp_range, forecast_temps, weighted_mean):
        """COMPAT: Old PerformanceTracker.log_weather_bet() interface."""
        return self.record_weather_bet(bet_id, city, temp_range, forecast_temps, weighted_mean)

    def log_weather_prediction(self, bet_id, city, market_date, forecasts,
                                weighted_mean, our_probability, edge):
        """COMPAT: Old PerformanceTracker.log_weather_prediction() interface."""
        return self.record_weather_prediction(
            bet_id, city, market_date, forecasts,
            weighted_mean, our_probability, edge
        )

    def log_weather_resolution(self, bet_id, predicted_high, actual_high, error):
        """COMPAT: Old PerformanceTracker.log_weather_resolution() interface."""
        return self.record_weather_resolution(bet_id, predicted_high, actual_high, error)
