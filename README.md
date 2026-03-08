# Baggins Capital

An autonomous hedge fund that bets on crypto prices, commodities, equities, and weather temperatures on [Polymarket](https://polymarket.com). Every component is an "employee" — a specialized agent that does its job, learns from results, and gets better over time.

Powered by [Bankr](https://bankr.bot/) for trade execution and wallet management on Polymarket.

---

## The Team

```
The Manager
    ├── The Crypto Trader      (crypto, commodities, equities)
    ├── The Weather Analyst     (temperature prediction)
    ├── The Scalper             (15-min up/down markets)
    ├── The Sports Buddy        (UFC, boxing, playoffs)
    ├── The Settlement Clerk    (resolves bets, claims winnings)
    ├── The CFO                 (balance & risk management)
    ├── The Accountant          (tracks every bet & P&L)
    ├── The Detective           (30h strategy review)
    └── The Messenger           (Telegram alerts)
```

### The Manager (`hedge_fund_active.py`)
Runs the whole operation. Orchestrates every employee on a 2-minute cycle loop. Crypto and Scalper run 24/7, Weather Analyst only bets during windows (8-10am, 2-4pm, 9-11pm ET). Triggers daily resets at 22:00 UTC.

---

## Trading Floor

### The Crypto Trader (`polymarket_crypto.py`)
Scans Polymarket for price prediction markets — crypto (BTC, ETH, SOL, XRP, DOGE, ADA, LINK, AVAX), commodities (Gold, Silver, Crude Oil), and equities (NVIDIA, Tesla). Dual price feeds: CoinGecko for crypto, Yahoo Finance for commodities/equities. Flat $3 bets, 720-hour scan window (30 days), 15% minimum edge, 15 max concurrent positions.

**Bet Classification** — Every market gets categorized by type, each with its own confidence scoring track:

| Type | What | Starts At |
|------|------|-----------|
| **HOLD** | Price already past target, just needs to stay | 50 |
| **FADE** | Betting price won't reach a far target | 45 |
| **SNAP** | Close to target, early resolution play | 40 |
| **LOTTO** | Cheap longshot, 10x+ payout potential | 30 |
| **DECAY** | Short-dated, predictable price behavior | 48 |
| **COMPRESSION** | Cheap + mispriced asymmetric play | 42 |
| **MOMENTUM** | Trend-following with confirmation | 38 |

**Early Resolution Insight**: Markets like "Will Oil hit $60 by March 31?" resolve the *moment* price touches $60 — not at the deadline. A "23 day" market could resolve in hours. The classification system is built around this.

### The Weather Analyst (`weather_agent.py`)
A whole department in one file:
- **The Weather Scout** — Scans Polymarket for daily temperature markets across 16+ cities (US + international)
- **The Meteorologist** — Pulls forecasts from 7 weather APIs (Open-Meteo, NOAA, WeatherAPI, OpenWeatherMap, Visual Crossing, Weatherbit, Pirate Weather)
- **The Auditor** — Tracks each source's accuracy per city, adjusts credibility weights after every resolution
- **The Statistician** — Calculates edge, picks YES/NO side, scores confidence 0-100
- **The Historian** — 2-year temperature baselines for sigma calibration
- **The Coach** — AI-powered strategy review, adjusts weights and recommends focus cities

**v5.1 Strategy**: Threshold-first market selection. Above/below markets (60% WR historically) get priority. Single-degree exact temp markets (0% WR) are blocked entirely. Between X-Y range markets only allowed at 10x+ payout potential.

**Forecast Collection**: Every 30 minutes, the Meteorologist collects readings from all 7 APIs for every active city and stores them in the `forecast_snapshots` table. During betting windows, accumulated snapshot data is used — multiple readings per source, with drift and stability metrics. This builds a dataset so the system knows exactly which sources to trust for which cities. API calls are staggered with 1.5s delays to avoid rate limiting.

### The Scalper (`updown_trader.py`)
Trades BTC/ETH/SOL/XRP "Up or Down" 15-minute markets. Fires at :08, :23, :38, :53 each hour (blackout 11am-2pm ET). Bets both UP and DOWN — score alone decides direction. Flat $2 bets, score range 4.5-6.5, max 12 bets/day, $10 daily drawdown limit, 45-min cooldown after any loss. Scoring: MA alignment (+/-2), candle direction (+/-1.5), RSI (+/-1), low volatility (+1), volume multiplier (x1.2). Max possible ~7.2. Price gate: only bets when our side is 35-45c (value zone).

---

## Operations

### The Banker (`bankr.py`)
Unified interface to the [Bankr API](https://bankr.bot/). Places trades, checks positions, claims redeemable shares. Circuit breaker skips for 5 min after 503 errors.

### The Settlement Clerk (`bet_resolver.py`)
Resolves bets on a regular cycle. Sends "redeem all available shares" to Bankr for bulk claiming, then re-checks the portfolio and matches positions to pending DB bets. Logs every portfolio snapshot so other employees can reference it. Also updates weather analytics after resolving weather bets.

### The CFO (`wallet_coordinator.py`)
Central balance authority. Enforces deployment cap (70% of balance), $5 reserve, single-bet limits (30% of available), and max 25 concurrent positions. Syncs with Bankr wallet periodically.

### The Accountant (`performance_tracker.py`)
Logs every bet, resolution, and daily P&L. Maintains the database — the company's single source of truth.

### The Detective (`pattern_analyzer`)
Runs a 30-hour strategy review. Analyzes bet history, finds losing patterns (city/side combos, forecast vs outcome mismatches), and sends actionable findings to Telegram.

### The Sports Buddy (`sports_analyst.py`)
Scans Polymarket for sports events (UFC, boxing, NBA/NHL playoffs, Champions League). $1 exploratory bets, max 3/day, 55+ confidence minimum.

### The Messenger (`bet_notifier.py`)
Sends Telegram alerts on every bet placement and resolution. Daily summaries with P&L breakdown.

---

## The Database

SQLite (`hedge_fund_performance.db`, WAL mode) is the central nervous system. Every employee reads from it and writes to it.

| Table | Purpose |
|-------|---------|
| `bets` | Every bet placed (market, side, amount, odds, result, profit) |
| `daily_performance` | Daily P&L, win rate, ROI |
| `forecast_snapshots` | Every weather API reading (city, date, source, temp, timestamp) |
| `weather_sources` | Per-source per-city accuracy and credibility weights |
| `weather_city_patterns` | Win rate and profit per city |
| `weather_side_patterns` | Win rate per city+side (YES/NO) |
| `weather_calibration` | Confidence bucket accuracy for self-calibration |
| `portfolio_checks` | Bankr portfolio snapshots and redeem results |

### How the DB Drives Decisions
- **Pre-bet gates**: Blocks cities with 0% win rate (2+ bets) or <30% win rate (3+ bets)
- **Source ranking**: Each weather source's prediction is compared to actual temps after resolution — updates credibility weights
- **Pattern detection**: Identifies losing city/side combos and avoids them

---

## Setup

### Prerequisites
- Python 3.10+
- A [Bankr](https://bankr.bot/) account and API key (for Polymarket trade execution)
- An [Anthropic](https://console.anthropic.com/) API key (for AI reasoning)

### 1. Clone the repo

```bash
git clone https://github.com/Shhmoe/baggins-capital.git
cd baggins-capital
```

### 2. Install dependencies

```bash
pip install requests python-dotenv anthropic
```

### 3. Configure environment

Copy the example env file and fill in your keys:

```bash
cp .env.example .env
```

**Required keys:**
| Key | What | Where to get it |
|-----|-------|-----------------|
| `BANKR_API_KEY` | Polymarket trade execution | [bankr.bot](https://bankr.bot/) |
| `API_KEY` | Anthropic (AI reasoning) | [console.anthropic.com](https://console.anthropic.com/) |

**Optional keys (unlock more features):**
| Key | What | Where to get it |
|-----|-------|-----------------|
| `WEATHERAPI_KEY` | WeatherAPI forecasts | [weatherapi.com](https://www.weatherapi.com/) (free tier) |
| `OPENWEATHERMAP_KEY` | OpenWeatherMap forecasts | [openweathermap.org](https://openweathermap.org/api) (free tier) |
| `VISUALCROSSING_KEY` | Visual Crossing forecasts | [visualcrossing.com](https://www.visualcrossing.com/) (free tier) |
| `WEATHERBIT_KEY` | Weatherbit forecasts | [weatherbit.io](https://www.weatherbit.io/) (free tier) |
| `PIRATEWEATHER_KEY` | Pirate Weather forecasts | [pirateweather.net](https://pirateweather.net/) (free tier) |
| `TELEGRAM_BOT_TOKEN` | Telegram alerts | [@BotFather](https://t.me/BotFather) |
| `TELEGRAM_CHAT_ID` | Your Telegram chat ID | Send `/start` to your bot, check updates API |

> The Weather Analyst works best with **3+ weather sources**. Open-Meteo and NOAA (for US cities) require no API key.

### 4. Configure betting parameters

Edit `hedge_fund_config.py` to set your risk levels:

```python
# Crypto / Commodities / Equities
CRYPTO_BET_SIZE = 3.0        # $ per bet
CRYPTO_MAX_DAILY = 20        # max bets per day
CRYPTO_MAX_CONCURRENT = 15   # max open positions
CRYPTO_MIN_EDGE = 0.15       # 15% minimum edge
CRYPTO_MAX_HOURS = 720       # 30-day scan window

# Weather
WEATHER_BET_SIZE = 1.0       # $ per weather bet
WEATHER_MAX_DAILY = 30       # max bets per day

# Scalper (15-min markets)
SCALPER_BET_SIZE = 2.0       # $ per scalp
SCALPER_MAX_DAILY = 12       # max bets per day
SCALPER_MIN_SCORE = 4.5      # score range 4.5-6.5
SCALPER_ASSETS = ['btc', 'eth', 'sol', 'xrp']
```

### 5. Run it

```bash
python3 hedge_fund_active.py
```

The Manager will start the 2-minute cycle loop. All employees will begin scanning, betting, and resolving automatically.

### Running as a service (Linux)

```bash
sudo tee /etc/systemd/system/baggins-capital.service > /dev/null << 'EOF'
[Unit]
Description=Baggins Capital - Autonomous Hedge Fund
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/path/to/baggins-capital
ExecStart=/usr/bin/python3 hedge_fund_active.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable baggins-capital
sudo systemctl start baggins-capital
```

---

## Architecture

```
                         ┌─────────────┐
                         │  Polymarket │
                         └──────┬──────┘
                                │
                         ┌──────▼──────┐
                    ┌────│   Bankr API  │────┐
                    │    └──────────────┘    │
                    │                        │
              ┌─────▼─────┐          ┌──────▼──────┐
              │  The Banker│          │  Settlement │
              │ (bankr.py) │          │    Clerk    │
              └─────┬─────┘          └──────┬──────┘
                    │                        │
     ┌──────────┬───┴────┬──────────┐       │
     │          │        │          │       │
┌────▼───┐ ┌───▼────┐ ┌─▼────┐ ┌──▼───┐ ┌─▼────────┐
│ Crypto │ │Weather │ │Scalp │ │Sports│ │Accountant│
│ Trader │ │Analyst │ │  er  │ │Buddy │ │ (P&L DB) │
└───┬────┘ └───┬────┘ └──────┘ └──────┘ └──────────┘
    │          │
┌───▼────┐ ┌──▼─────────┐
│Yahoo + │ │  7 Weather  │
│CoinGeck│ │    APIs     │
└────────┘ └─────────────┘
```

---

## Disclaimer

This is experimental software for educational purposes. Prediction markets involve real money and real risk. Start with small bet sizes and monitor closely. The authors are not responsible for any financial losses.

---

Built by a hobbit with a server and some weather APIs.
