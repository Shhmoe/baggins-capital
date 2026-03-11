"""
The DB Steward — Data & Analytics Department
Baggins Capital V3

Owns database infrastructure: schema design, migrations, index management,
table creation, and health monitoring. No role in daily trading operations.
The ONLY employee authorized to modify database structure.

Department: Data & Analytics
Reports to: The Manager
"""

from datetime import datetime
from archivist import Archivist


class DBSteward:
    """Schema, migrations, infrastructure. No trading operations."""

    def __init__(self, db_path='hedge_fund_performance.db'):
        self.db_path = db_path
        self._arch = Archivist(db_path)

    def initialize_v3_tables(self):
        """Create all V3 tables. Safe to call multiple times."""
        print("[DB STEWARD] Initializing V3 schema...")
        created = 0

        # ── Pulse Insights (Market Pulse Analyst) ──
        self._arch._execute("""
            CREATE TABLE IF NOT EXISTS pulse_insights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                department TEXT NOT NULL,
                metric_type TEXT NOT NULL,
                value REAL,
                details TEXT
            )
        """)
        self._arch._execute(
            "CREATE INDEX IF NOT EXISTS idx_pulse_dept_type ON pulse_insights(department, metric_type)")
        self._arch._execute(
            "CREATE INDEX IF NOT EXISTS idx_pulse_timestamp ON pulse_insights(timestamp)")
        created += 1

        # ── Detective Findings ──
        self._arch._execute("""
            CREATE TABLE IF NOT EXISTS detective_findings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anomaly_id TEXT,
                root_cause TEXT NOT NULL,
                confidence REAL DEFAULT 0.5,
                recommended_action TEXT,
                affected_employee TEXT,
                affected_parameter TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT NOT NULL,
                reviewed_at TEXT,
                reviewed_by TEXT,
                applied_at TEXT
            )
        """)
        self._arch._execute(
            "CREATE INDEX IF NOT EXISTS idx_detective_status ON detective_findings(status)")
        created += 1

        # ── Signals Library ──
        self._arch._execute("""
            CREATE TABLE IF NOT EXISTS signals_library (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                finding_id INTEGER,
                pattern_hash TEXT,
                category TEXT NOT NULL,
                description TEXT NOT NULL,
                recommendation TEXT,
                outcome TEXT,
                applied INTEGER DEFAULT 0,
                performance_delta REAL,
                created_at TEXT NOT NULL,
                updated_at TEXT,
                FOREIGN KEY (finding_id) REFERENCES detective_findings(id)
            )
        """)
        self._arch._execute(
            "CREATE INDEX IF NOT EXISTS idx_signals_hash ON signals_library(pattern_hash)")
        self._arch._execute(
            "CREATE INDEX IF NOT EXISTS idx_signals_category ON signals_library(category)")
        created += 1

        # ── Hook Registry (Company Clock) ──
        self._arch._execute("""
            CREATE TABLE IF NOT EXISTS hook_registry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hook_name TEXT NOT NULL,
                fire_condition TEXT,
                subscriber_module TEXT NOT NULL,
                handler_function TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                registered_at TEXT,
                UNIQUE(hook_name, subscriber_module, handler_function)
            )
        """)
        created += 1

        # ── Historian insights columns for adaptive system ──
        adaptive_columns = [
            "ALTER TABLE historian_insights ADD COLUMN blended_win_rate_7d REAL",
            "ALTER TABLE historian_insights ADD COLUMN dept_win_rate TEXT",  # JSON
            "ALTER TABLE historian_insights ADD COLUMN dept_bet_count_7d TEXT",  # JSON
            "ALTER TABLE historian_insights ADD COLUMN volatility_score TEXT",  # JSON
            "ALTER TABLE historian_insights ADD COLUMN category_avg_adverse_move TEXT",  # JSON
        ]
        for sql in adaptive_columns:
            try:
                self._arch._execute(sql)
            except Exception:
                pass  # Column already exists

        # ── Compliance blocklist V3 columns ──
        blocklist_columns = [
            "ALTER TABLE compliance_blocklist ADD COLUMN reason_category TEXT DEFAULT 'PERMANENT'",
            "ALTER TABLE compliance_blocklist ADD COLUMN expires_at TEXT",
            "ALTER TABLE compliance_blocklist ADD COLUMN submitted_by TEXT",
        ]
        for sql in blocklist_columns:
            try:
                self._arch._execute(sql)
            except Exception:
                pass

        self._arch._commit()
        print(f"[DB STEWARD] V3 schema ready. {created} table groups initialized.")
        return created

    def check_health(self):
        """Run database health checks."""
        report = {}
        try:
            # Table count
            tables = self._arch._fetchall(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            report['table_count'] = len(tables)
            report['tables'] = [t[0] for t in tables]

            # DB size
            row = self._arch._fetchone("SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()")
            if row:
                report['size_bytes'] = row[0]
                report['size_mb'] = round(row[0] / 1024 / 1024, 2)

            # WAL mode check
            row = self._arch._fetchone("PRAGMA journal_mode")
            report['journal_mode'] = row[0] if row else 'unknown'

            # Integrity check (quick)
            row = self._arch._fetchone("PRAGMA quick_check")
            report['integrity'] = row[0] if row else 'unknown'

            # Row counts for key tables
            for table in ['bets', 'bet_decisions', 'historian_insights',
                          'pulse_insights', 'detective_findings', 'signals_library']:
                try:
                    row = self._arch._fetchone(f"SELECT COUNT(*) FROM {table}")
                    report[f'rows_{table}'] = row[0] if row else 0
                except Exception:
                    report[f'rows_{table}'] = 'table_missing'

            report['status'] = 'healthy'
        except Exception as e:
            report['status'] = f'error: {e}'

        return report

    def run_migration(self, version, description, sql_statements):
        """Run a numbered migration. Idempotent."""
        try:
            existing = self._arch._fetchone(
                "SELECT version FROM schema_versions WHERE version = ?", (version,))
            if existing:
                return False  # Already applied

            for sql in sql_statements:
                self._arch._execute(sql)

            self._arch._execute(
                "INSERT INTO schema_versions (version, applied_at, description) VALUES (?, ?, ?)",
                (version, datetime.now().isoformat(), description))
            self._arch._commit()
            print(f"[DB STEWARD] Migration v{version} applied: {description}")
            return True
        except Exception as e:
            print(f"[DB STEWARD] Migration v{version} failed: {e}")
            return False

    def get_schema_version(self):
        """Get current schema version."""
        try:
            row = self._arch._fetchone("SELECT MAX(version) FROM schema_versions")
            return row[0] if row and row[0] else 0
        except Exception:
            return 0

    def vacuum(self):
        """Run VACUUM to reclaim space. Use sparingly — locks DB."""
        try:
            self._arch._conn().execute("VACUUM")
            print("[DB STEWARD] VACUUM complete.")
        except Exception as e:
            print(f"[DB STEWARD] VACUUM failed: {e}")

    def analyze(self):
        """Run ANALYZE to update query planner statistics."""
        try:
            self._arch._conn().execute("ANALYZE")
            print("[DB STEWARD] ANALYZE complete.")
        except Exception as e:
            print(f"[DB STEWARD] ANALYZE failed: {e}")
