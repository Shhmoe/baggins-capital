# Baggins OpenSource Company

An open-source autonomous agent framework for trading prediction markets. Built as a system of specialized "employees" organized into departments — each with a single responsibility, communicating through defined interfaces, and learning from its own results.

This is not a trading bot. It is a company architecture: departments, data pipelines, compliance gates, risk management, intelligence liaisons, and an extensible hiring structure. The included trading departments (crypto, weather, sports, scalper) are reference implementations. The framework is designed so you can build, customize, and plug in your own.

Powered by [Bankr](https://bankr.bot/) for trade execution and wallet management on [Polymarket](https://polymarket.com).

---

## How It Works

The Manager (CEO) runs a continuous orchestration loop. Every 2 minutes, it executes a full cycle:

```
 STEP 0: MARKET SCOUT
 ┌──────────────┐
 │ Market Scout  │──scan()──> Gamma API ──> Screen & classify ──> Route to:
 └──────────────┘                              │
                          ┌────────────────────┼────────────────────┐
                          v                    v                    v
                    crypto_queue         weather_queue         sports_queue

 STEPS 1-7: PRE-TRADING CHECKS (all must pass before any department trades)
 ┌─────────┐    ┌──────────────┐    ┌────────────┐    ┌──────────────┐
 │   CFO   │───>│ Risk Manager │───>│ Compliance │───>│ Market Pulse │
 │ (Wallet)│    │  (Circuit    │    │ (Blocklist │    │  (30-min     │
 │         │    │   Breakers)  │    │  + Caps)   │    │   refresh)   │
 └─────────┘    └──────────────┘    └────────────┘    └──────────────┘

 IF CFO unavailable ──> HALT ALL TRADING ──> sleep & retry

 STEP 8: RUN DEPARTMENTS (priority order — seniority on capital)
   Dept 1: Weather  ──> Weather Forecaster ──> WeatherAgent ──> Banker
   Dept 2: Scalper  ──> UpDown Trader ──> Banker
   Dept 3: Crypto   ──> CryptoMarketScanner ──> Banker
   Dept 4: Avantis  ──> Avantis Signals ──> On-chain execution
   Dept 5: Sports   ──> Sports Analyst ──> Banker

 POST-TRADING: SETTLEMENT + MONITORING
   Settlement Clerk ──> Position Checks ──> Manager Heartbeat
```

---

## Architecture

The core principle: **every function is an employee, every employee has one job, and data flows through defined pipelines — never shortcuts.**

### The Bet Pipeline

Every bet — regardless of department — passes through the same pipeline:

```
Market Scout ──> Intelligence Liaison ──> Risk Manager ──> Compliance
     │                  │                      │               │
  (scan &           (package              (advisory:       (HARD GATE:
   filter)          DB insights)          warnings)       pass/reject)
                                                              │
                                                              v
                                               The Banker ──> Data Intake ──> DB Writer
                                                   │              │               │
                                               (execute        (validate       (format &
                                                trade)         fields)         write SQL)
```

No department can skip Compliance. No module can write to the database directly. No trader talks to the exchange — only the Banker does.

### The Banker — Sole Bet Executor

Every department recommendation flows through the Banker's 7-gate process:

| Gate | Action |
|------|--------|
| Gate 0 | Validate required fields |
| Gate 1 | Compliance pre-flight (HARD BLOCK) |
| Gate 2 | CFO wallet check (HARD BLOCK) |
| Gate 3 | Place bet via Bankr API |
| Gate 4 | Verify execution |
| Gate 5 | Log to database (Archivist via DataIntake) |
| Gate 6 | Reserve funds in Wallet |
| Gate 7 | Track and return result |

---

## Departments

The framework ships with 5 trading departments as reference implementations. Each can be enabled, disabled, or replaced independently.

### Department 1: Weather

Forecasts temperature ranges using multi-source consensus from 7+ weather APIs. Computes weighted mean, agreement ratio, and standard deviation across sources. Each source is tracked for credibility and bias.

**Pipeline:** Weather Intel (Liaison) -> Weather Forecaster (7 APIs) -> WeatherAgent (confidence scoring) -> Banker

### Department 2: Scalper

Trades 15-minute Up/Down binary markets using multi-timeframe technical analysis on candle data.

**Pipeline:** Scalper Intel (Liaison) -> UpDown Trader (technical indicators) -> Banker

### Department 3: Crypto

Evaluates crypto, equities, commodities, and indices markets. Uses an 8-step evaluation process: distance + momentum, true probability, mispricing detection, signal stacking, strategy gates, confidence scoring, and bet sizing.

**Pipeline:** Crypto Intel (Liaison) + Crypto Analyst (8 data sources) -> CryptoMarketScanner -> Banker

### Department 4: Avantis Leverage

Leveraged trading on the Base blockchain. Operates on a separate chain with its own executor (does not use the Banker).

**Pipeline:** Avantis Signals -> Avantis Executor -> On-chain execution

### Department 5: Sports

Exploratory department for sports betting. Compares bookmaker odds to find edge.

**Pipeline:** Sports Intel (Liaison) -> Sports Analyst -> Banker

---

## Employee Roster

The framework includes 30 employees across 6 departments:

### Executive

| Employee | File | Role |
|----------|------|------|
| The Manager | `hedge_fund_active.py` | CEO / orchestrator. Runs the cycle, fires hooks, coordinates all departments. |

### Trading Desk

| Employee | File | Role |
|----------|------|------|
| Market Scout | `market_scout.py` | Scans markets, classifies by type, routes to department queues. |
| Crypto Market Scanner | `polymarket_crypto.py` | Evaluates crypto, equities, commodities, indices. |
| Crypto Analyst | (integrated) | 8-source sentiment and data analysis (Fear & Greed, news, on-chain). |
| Weather Agent | `weather_agent.py` | Multi-source weather forecasting and confidence scoring. |
| UpDown Trader | `updown_trader.py` | 15-minute binary scalping with technical indicators. |
| Sports Analyst | `sports_analyst.py` | Bookmaker odds comparison and edge detection. |
| Avantis Signals | `avantis_signals.py` | Generates leveraged trading signals. |
| Avantis Executor | `avantis_executor.py` | Executes leveraged trades on-chain. |

### Intelligence

| Employee | File | Role |
|----------|------|------|
| Crypto Intel | `intel_crypto.py` | Packages DB intelligence for Crypto department. |
| Scalper Intel | `intel_scalper.py` | Packages DB intelligence for Scalper department. |
| Weather Intel | `intel_weather.py` | Packages DB intelligence for Weather department. |
| Sports Intel | `intel_sports.py` | Packages DB intelligence for Sports department. |

### Settlement

| Employee | File | Role |
|----------|------|------|
| The Banker | `bankr.py` | Sole exchange interface. No other module touches the exchange API. |
| Bet Resolver | `bet_resolver.py` | Claims expired bets, resolves settlements. |
| Bankr MCP Client | `bankr_executor.py` | API wrapper for Bankr execution. |

### Operations

| Employee | File | Role |
|----------|------|------|
| Compliance Officer | `compliance.py` | Hard gate. `pre_flight()` — pass or reject. Performance-scaled caps. |
| Risk Manager | `risk_manager.py` | Advisory risk assessment. Statistical circuit breakers. |
| Wallet Coordinator | `wallet_coordinator.py` | CFO: capital allocation, deployment caps, position limits. |
| Company Clock | `company_clock.py` | Time authority. 19-hook event system for scheduling. |

### Data and Analytics

| Employee | File | Role |
|----------|------|------|
| Archivist | `archivist.py` | Performance tracking and archival. |
| DB Writer | `db_writer.py` | All database writes. Never reads. |
| DB Reader | `db_reader.py` | All database reads. Never writes. |
| DB Steward | `db_steward.py` | Schema, migrations, health checks. |
| Data Intake | `data_intake.py` | Validates all incoming bet/resolution data before DB. |
| Market Pulse | `market_pulse.py` | Real-time metrics feed (30-min max staleness). |
| Market Scout | `market_scout.py` | Market scanning and classification. |
| Detective | `detective.py` | 30-hour forensic investigation cycles. |
| Signals Library | `signals_library.py` | Institutional memory of Detective findings. |
| Pattern Analyzer | (integrated) | Automated strategy review. |
| Historian | `historian.py` | Daily deep analysis at reset time. |
| Bet Notifier | `bet_notifier.py` | Notification dispatcher (Telegram). |

---

## Heartbeat Schedule

| Component | Interval | Purpose |
|-----------|----------|---------|
| Market Pulse | 30 min | Win rates, streaks, circuit breaker proximity, exposure |
| UpDown Session | per cycle | Real-time budget tracking |
| Wallet Sync | 4 hours | On-chain balance verification |
| Manager AI | 6 hours | Portfolio health review via AI |
| Detective | 30 hours | Forensic pattern detection |
| Pattern Analyzer | 30 hours | Strategy effectiveness review |
| Weather AI | 48 hours | Source credibility recalibration |

---

## How to Build Your Own Department

The included departments are reference implementations. The architecture supports any market type. To add your own:

### 1. Create a Trader (`your_trader.py`)
- Receives a pre-screened market queue from Market Scout
- Receives an intelligence package from its Liaison
- Scores opportunities and builds recommendations
- Never touches the database directly
- Never talks to the exchange directly

### 2. Create a Liaison (`intel_your_dept.py`)
- Reads from `pulse_insights` and `historian_insights` tables
- Packages data into a dict the Trader can consume
- The Trader never queries analytics tables — the Liaison does that

### 3. Add Keywords to Market Scout (`market_scout.py`)
- Add classification keywords for your market type
- Add a department config block to `SCOUT_CONFIG`
- Add format compatibility rules to `FORMAT_COMPATIBILITY`

### 4. Add Configuration (`hedge_fund_config.py`)
- Bet size range, daily cap, concurrent cap, minimum edge, minimum confidence
- Each department gets its own isolated section

### 5. Wire into the Manager (`hedge_fund_active.py`)
- Add your trader to the cycle
- Pass `risk_manager`, `compliance`, and `intel_package`

### 6. Update Compliance (`compliance.py`)
- Add your department to `BASE_CAPS` and `CAP_FLOORS`

That's it. Risk Manager, Compliance, Data Intake, DB Writer, Settlement Clerk, Historian, Market Pulse, and Detective all work automatically for any new department.

---

## Key Design Patterns

### Separation of Concerns
- **Traders** decide what to bet on. They never execute trades or write to the database.
- **The Banker** executes trades. It never decides what to bet on.
- **DB Writer** writes data. **DB Reader** reads data. Neither analyzes.
- **Compliance** gates bets. **Risk Manager** advises. Different jobs.

### Intelligence Pipeline
```
DB (raw data) -> Market Pulse (real-time metrics) -> Historian (daily analysis)
                         |                               |
                  pulse_insights table            historian_insights table
                         |                               |
                    Intelligence Liaisons (package both into briefing)
                         |
                    Traders (consume briefing, never query raw tables)
```

### Adaptive Limits
Three modules recalibrate weekly at the `MONDAY_OPEN` hook:
- **CFO**: Deployment cap (60-80%), position limits, per-bet ceiling
- **Risk Manager**: Exposure ceilings, correlation thresholds, circuit breaker sensitivity
- **Compliance**: Daily bet caps (0.5x-1.5x multiplier by department win rate)

All limits are data-driven from the database, not hardcoded.

### Decision Snapshots
Every bet records a full snapshot of why it was placed — all inputs, modifiers, scores, and reasoning. This is what the Historian and Detective analyze.

### The Clock Hook System
Instead of scattering time checks everywhere, the Company Clock fires named events:
```
DAILY_RESET, PRE_RESET, MONDAY_OPEN, FRIDAY_CLOSE,
MONTH_START, MONTH_END, CIRCUIT_BREAKER, CIRCUIT_BREAKER_CLEAR,
DAILY_CAP_HIT, DAILY_CAP_WARNING, WIN_RATE_DROP, ECONOMIC_CALENDAR ...
```
Modules register callbacks. The Manager fires due hooks at the top of each cycle.

### Daily Reset (22:00 UTC)
```
PRE_RESET_HOOK (21:45) ──> Archive pulse metrics
         |
         v
Historian Daily Analysis ──> Compliance Daily Reset ──> Reset All Counters
                                                         bet_count=0
                                                         losses=0
                                                         wallet.reset
──> Summary notification ──> Save daily performance ──> sleep ──> NEXT CYCLE
```

---

## Data Sources

The framework integrates with multiple free-tier data sources. All are optional except Bankr (for execution).

| Source | Department | Notes |
|--------|-----------|-------|
| Polymarket Gamma API | All | Market scanning and classification |
| Bankr API | All | Trade execution, positions, balance |
| CoinGecko | Crypto | Real-time crypto prices |
| Yahoo Finance | Crypto | Equities, commodities, indices (+ crypto fallback) |
| Open-Meteo | Weather | Free, no key needed |
| NOAA/NWS | Weather | US cities, no key needed |
| WeatherAPI.com | Weather | Free tier |
| OpenWeatherMap | Weather | Free tier |
| Visual Crossing | Weather | Free tier |
| Weatherbit | Weather | Free tier |
| Pirate Weather | Weather | Free tier |
| Open-Meteo Archive | Weather | 2-year historical baseline |
| Binance US | Scalper | 1-min candles for technical indicators |
| The Odds API | Sports | Bookmaker odds (500 req/month free tier) |

---

## Getting Started

```bash
# 1. Clone and configure
git clone https://github.com/Shhmoe/baggins-capital.git
cd baggins-capital
cp .env.example .env
# Fill in your API keys in .env

# 2. Install dependencies
pip install requests python-dotenv anthropic

# 3. Run (dry run mode by default)
python3 hedge_fund_active.py
```

The Manager handles everything — cycle loop, hook firing, market scanning, intelligence distribution, trading, resolution, and daily resets. No cron jobs needed.

---

## Configuration

All parameters live in `hedge_fund_config.py`, organized by department. Each department has its own isolated section with no parameter overlap.

### Department Toggles
```python
ENABLE_CRYPTO_MODULE = True
ENABLE_WEATHER_MODULE = True
ENABLE_UPDOWN_MODULE = True
ENABLE_SPORTS_MODULE = False
ENABLE_AVANTIS = False
```

### Per-Department Settings
Each department defines: bet size range, daily cap, concurrent position cap, minimum edge, and minimum confidence. Customize these to match your risk tolerance and capital.

### Global Limits (CFO Enforces)
```python
TOTAL_MAX_DEPLOYMENT_PCT = 0.75    # Max 75% of capital deployed
TOTAL_MAX_CONCURRENT = 40          # Hard cap on all positions
MIN_RESERVE_BALANCE = 5.0          # Always keep reserve
MAX_BET_SIZE = 0.10                # Never >10% on single market
```

---

## Database

SQLite in WAL mode. The DB Steward manages schema and migrations automatically.

| Table | Purpose |
|-------|---------|
| `bets` | Every bet placed (market, side, amount, odds, result, profit) |
| `bet_decisions` | Full decision snapshots (JSON of all inputs/modifiers) |
| `forecast_snapshots` | Weather API readings |
| `weather_sources` | Per-source credibility, bias, average error |
| `weather_city_patterns` | Win rate per city |
| `pulse_insights` | Market Pulse real-time metrics |
| `historian_insights` | Historian daily analysis |
| `signals_library` | Detective findings |
| `detective_findings` | Individual investigation results |
| `compliance_log` | Every pre-flight decision |
| `risk_events` | Every risk assessment |
| `hook_registry` | Company Clock hook system |
| `schema_versions` | Migration tracking |

---

## License

Open source. Build your own company. Add your own departments. Customize everything.
