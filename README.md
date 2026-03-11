# Baggins Capital

An autonomous agent company that trades prediction markets on [Polymarket](https://polymarket.com). Built as a system of specialized "employees" — each one owns a single job, communicates through defined interfaces, and learns from its own results.

This isn't a trading bot with if-statements. It's a company architecture: departments, data pipelines, compliance gates, risk management, intelligence liaisons, and a hiring/firing structure. The trading departments (crypto, weather, sports, scalper) are what Baggins Capital runs — but the architecture works for anything. Build your own departments.

Powered by [Bankr](https://bankr.bot/) for trade execution and wallet management.

---

## Architecture

The core idea: **every function is an employee, every employee has one job, and data flows through defined pipelines — never shortcuts.**

```
┌─────────────────────────────────────────────────────────────────┐
│                        THE MANAGER                              │
│                   (orchestration loop)                           │
│                                                                 │
│  Every 2 minutes:                                               │
│    1. Fire scheduled hooks (Company Clock)                      │
│    2. Refresh wallet (CFO)                                      │
│    3. Scan all markets (Market Scout)                           │
│    4. Update real-time intelligence (Market Pulse)              │
│    5-8. Run each trading department                             │
│    9. Resolve settled bets (Settlement Clerk)                   │
│   10. Check investigation schedule (Detective)                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### The Bet Pipeline

Every bet — regardless of department — passes through the same pipeline:

```
Market Scout ──→ Intelligence Liaison ──→ Risk Manager ──→ Compliance
     │                  │                      │               │
  (scan &           (package              (advisory:       (HARD GATE:
   filter)          DB insights)          warnings)       pass/reject)
                                                              │
                                                              ▼
                                               The Banker ──→ Data Intake ──→ DB Writer
                                                   │              │               │
                                               (execute        (validate       (format &
                                                trade)         fields)         write SQL)
```

No department can skip Compliance. No module can write to the database directly. No trader talks to the exchange — only the Banker does.

---

## The Employees (25)

### Executive (2)

| Employee | File | Job |
|----------|------|-----|
| **The Manager** | `hedge_fund_active.py` | Runs the 10-step cycle. Fires hooks. Coordinates everyone. |
| **The CFO** | `wallet_coordinator.py` | Capital allocation. Deployment caps. Position limits. Adaptive weekly. |

### Trading Desk (9)

| Employee | File | Job |
|----------|------|-----|
| **Market Scout** | `market_scout.py` | Single API fetch, 7 disqualification checks, department queues |
| **Crypto Trader** | `polymarket_crypto.py` | Crypto, commodities, equities, indices. Data-driven modifiers. |
| **Weather Analyst** | `weather_agent.py` | 7 weather APIs, credibility engine, source agreement, dynamic city floors |
| **Scalper** | `updown_trader.py` | 15-min Up/Down markets. Technical indicators from candle data. |
| **Sports Buddy** | `sports_analyst.py` | Bookmaker odds comparison, fighter stats scraping |
| **Crypto Liaison** | `intel_crypto.py` | Pre-packages DB intelligence for Crypto Trader |
| **Weather Liaison** | `intel_weather.py` | Pre-packages DB intelligence for Weather Analyst |
| **Scalper Liaison** | `intel_scalper.py` | Pre-packages DB intelligence for Scalper |
| **Sports Liaison** | `intel_sports.py` | Pre-packages DB intelligence for Sports Buddy |

### Operations (5)

| Employee | File | Job |
|----------|------|-----|
| **The Banker** | `bankr.py` | SOLE exchange interface. No other module touches the exchange API. |
| **Settlement Clerk** | `bet_resolver.py` | Resolves bets, claims winnings, matches positions |
| **The Messenger** | `bet_notifier.py` | Queues notifications (Telegram, etc.) |
| **Compliance Officer** | `compliance.py` | Hard gate. `pre_flight()` — pass or reject. Performance-scaled caps. |
| **Data Intake** | `data_intake.py` | Validates all incoming bet/resolution data before DB |

### Data & Analytics (8)

| Employee | File | Job |
|----------|------|-----|
| **DB Writer** | `db_writer.py` | All database writes. Never reads. |
| **DB Reader** | `db_reader.py` | All database reads. Never writes. |
| **DB Steward** | `db_steward.py` | Schema, migrations, health checks |
| **The Historian** | `historian.py` | Daily deep analysis at reset time |
| **Market Pulse** | `market_pulse.py` | Real-time intelligence feed (30-min staleness max) |
| **The Detective** | `detective.py` | 30-hour forensic investigation cycles |
| **Signals Librarian** | `signals_library.py` | Institutional memory of Detective findings |
| **Risk Manager** | `risk_manager.py` | Advisory risk. Statistical circuit breaker. |

### Infrastructure (1)

| Employee | File | Job |
|----------|------|-----|
| **Company Clock** | `company_clock.py` | Time authority (ET). 19-hook event system. |

---

## How to Build Your Own Departments

The trading departments (crypto, weather, sports, scalper) are examples. The architecture doesn't care what you trade — it cares about the pipeline.

### To add a new department:

**1. Create a Trader** (`your_trader.py`)
- Receives a pre-screened market queue from Market Scout
- Receives an intelligence package from its Liaison
- Scores opportunities, picks the best, passes to the pipeline
- Never touches the database directly
- Never talks to the exchange directly

**2. Create a Liaison** (`intel_your_dept.py`)
- Reads from `pulse_insights` and `historian_insights` tables
- Packages data into a dict the Trader can consume
- The Trader never queries analytics tables — the Liaison does that

**3. Add keywords to Market Scout** (`market_scout.py`)
- Add your keywords to the classification logic
- Add a department config block to `SCOUT_CONFIG`
- Add format compatibility rules to `FORMAT_COMPATIBILITY`

**4. Add config** (`hedge_fund_config.py`)
- Bet size, daily cap, concurrent cap, min edge, min confidence
- Each department gets its own isolated section — no parameter overlap

**5. Wire it in the Manager** (`hedge_fund_active.py`)
- Add your trader to the cycle
- Pass `risk_manager`, `compliance`, and `intel_package`

**6. Update Compliance** (`compliance.py`)
- Add your department to `BASE_CAPS` and `CAP_FLOORS`

That's it. Risk Manager, Compliance, Data Intake, DB Writer, Settlement Clerk, Historian, Market Pulse, Detective — they all work automatically for any department. You only build the Trader and the Liaison.

---

## Key Design Patterns

### Separation of Concerns
- **Traders** decide what to bet on. They never execute trades or write to the database.
- **The Banker** executes trades. It never decides what to bet on.
- **DB Writer** writes data. **DB Reader** reads data. Neither analyzes.
- **Compliance** gates bets. **Risk Manager** advises. Different jobs.

### Intelligence Pipeline
```
DB (raw data) → Market Pulse (real-time metrics) → Historian (daily analysis)
                         ↓                               ↓
                  pulse_insights table            historian_insights table
                         ↓                               ↓
                    Intelligence Liaisons (package both into briefing)
                         ↓
                    Traders (consume briefing, never query raw tables)
```

### Adaptive Limits (weekly recalibration)
Three modules recalibrate at the `MONDAY_OPEN` hook:
- **CFO**: Deployment cap (60-80%), position limits, per-bet ceiling
- **Risk Manager**: Exposure ceilings, correlation thresholds, circuit breaker sensitivity
- **Compliance**: Daily bet caps (0.5x-1.5x multiplier by department win rate)

All limits are **data-driven from the database**, not hardcoded.

### Decision Snapshots
Every bet records a full snapshot of WHY it was placed — all inputs, modifiers, scores, and reasoning. This is what the Historian and Detective analyze. Without snapshots, you can't learn.

### The Clock Hook System
Instead of scattering `if hour == 22` checks everywhere, the Company Clock fires named events:
```
DAILY_RESET, PRE_RESET, MONDAY_OPEN, FRIDAY_CLOSE,
MONTH_START, MONTH_END, CIRCUIT_BREAKER, CIRCUIT_BREAKER_CLEAR,
DAILY_CAP_HIT, DAILY_CAP_WARNING, WIN_RATE_DROP, ...
```
Modules register callbacks. The Manager fires due hooks at the top of each cycle.

---

## Data Sources

### Crypto Trader
- **CoinGecko** — real-time crypto prices, 24h change, momentum
- **Yahoo Finance** — equities, commodities, indices (+ crypto fallback)

### Weather Analyst
- **Open-Meteo** — free, global forecasts (no key needed)
- **NOAA/NWS** — US cities (no key needed)
- **WeatherAPI.com** — global (free tier)
- **OpenWeatherMap** — global (free tier)
- **Visual Crossing** — global (free tier)
- **Weatherbit** — global (free tier)
- **Pirate Weather** — global (free tier, Dark Sky-compatible)
- **Open-Meteo Archive** — 2-year historical baseline

### Scalper
- **Binance US** — 1-min candles for technical indicators

### Sports Buddy
- **The Odds API** — bookmaker odds for edge calculation
- **UFC Stats** — fighter records (HTML scraping)

### Shared
- **Polymarket Gamma API** — all market data (Market Scout)
- **Bankr API** — trade execution, positions, balance

---

## Running

```bash
# 1. Clone and configure
git clone https://github.com/Shhmoe/baggins-capital.git
cd baggins-capital
cp .env.example .env
# Fill in your API keys in .env

# 2. Install dependencies
pip install requests python-dotenv anthropic

# 3. Run
python3 hedge_fund_active.py
```

The Manager handles everything — cycle loop, hook firing, market scanning, intelligence distribution, trading, resolution, and daily resets. No cron jobs needed.

---

## Configuration

All parameters live in `hedge_fund_config.py`, organized by department:

```python
# Each department has its own isolated section
CRYPTO_MAX_DAILY_BETS = 25
CRYPTO_MAX_CONCURRENT = 20
CRYPTO_MIN_EDGE = 0.12

WEATHER_MAX_DAILY_BETS = 30
WEATHER_MAX_CONCURRENT = 20
WEATHER_BETTING_WINDOWS = [(7, 11), (13, 17), (20, 24)]

UPDOWN_MAX_DAILY = 8
UPDOWN_MAX_CONCURRENT = 4

SPORTS_MAX_DAILY = 3
SPORTS_MAX_CONCURRENT = 3

# Global limits (CFO enforces)
TOTAL_MAX_CONCURRENT = 40
TOTAL_MAX_DEPLOYMENT_PCT = 0.75
MIN_RESERVE_BALANCE = 5.0
```

---

## Database

SQLite in WAL mode. The DB Steward manages all schema and migrations.

| Table | Purpose |
|-------|---------|
| `bets` | Every bet (market, side, amount, odds, result, profit) |
| `bet_decisions` | Full decision snapshots (JSON of all inputs/modifiers) |
| `forecast_snapshots` | Every weather API reading |
| `weather_sources` | Per-source credibility, bias, avg error |
| `weather_city_patterns` | Win rate per city (drives dynamic floor) |
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

Do whatever you want with it. Build your own company. Add your own departments. Make it better.
