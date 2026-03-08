"""
Hedge Fund Agent Configuration
Crypto Prediction Market Trader on Polymarket
"""

# OPERATION MODE
DRY_RUN = False  # LIVE MODE (set True for test_agent())
STARTING_BALANCE = 50.00  # Funded wallet balance

# BRANCH TOGGLES
ENABLE_POLYMARKET_CRYPTO = True   # Crypto prediction market betting (ACTIVE)
ENABLE_AVANTIS = True     # Avantis leverage trading (ACTIVE on Base)
ENABLE_CRYPTO_MODULE = True       # Alias for ENABLE_POLYMARKET_CRYPTO
ENABLE_WEATHER_MODULE = True      # Weather temperature betting on Polymarket
ENABLE_UPDOWN_MODULE = True       # Scalper is ACTIVE

# SCALPER (Up/Down 15-min markets) — Ultra-Selective Mode
UPDOWN_BET_SIZE = 2.0             # Flat $2 (was $5-15 score-based)
UPDOWN_MIN_SCORE = 5.0            # Only HIGH confidence (was 4.0)
UPDOWN_MAX_DAILY = 8              # Quality over quantity
UPDOWN_DRAWDOWN_LIMIT = 10.0      # $10/day loss limit (was $15)
UPDOWN_COOLDOWN_MINUTES = 45      # Cooldown after 1 loss (was 30 after 2)
UPDOWN_COOLDOWN_AFTER_LOSSES = 1  # Pause after 1 loss (was 2)
UPDOWN_UP_ONLY = False             # No DOWN bets (ties resolve UP = structural edge)
UPDOWN_MAX_PRICE = 0.45           # Only bet when UP side is cheap (was 0.48)
UPDOWN_MIN_PRICE = 0.35           # Not too cheap/risky (was 0.30)
UPDOWN_ASSETS = ['btc', 'eth']    # Focus on highest-volume assets      # Up/Down 15-min bets DISABLED (3W/5L, -$11.69)
PAUSED_POLYMARKET = False         # Crypto active
PAUSED_AVANTIS = True            # Avantis active

# WEATHER MODULE CONFIG
WEATHER_SCAN_INTERVAL = 300       # 5 min between weather scans
WEATHER_BET_SIZES = {89: 1.0, 92: 1.0, 95: 1.0}  # Flat $1 until strategy proves profitable
WEATHER_MAX_DAILY_BETS = 30       # 25 weather bets per day (bumped to test new strategy)
WEATHER_MAX_CONCURRENT = 15       # Max 15 open weather positions
WEATHER_MIN_CONFIDENCE = 85  # Conf 70=14% WR, 80=33% WR — only 85+ approaches breakeven       # Minimum confidence score (0-100)
WEATHER_MIN_EDGE = 0.05           # 5% minimum edge
WEATHER_CONVICTION_PROB = 0.70

# WEATHER BETTING WINDOWS (ET hours) — only place bets during these windows
# Outside windows, the system collects forecast data from all sources
WEATHER_BETTING_WINDOWS = [(8, 10), (14, 16), (21, 23)]  # Morning, Afternoon, Night (ET)
WEATHER_COLLECTION_INTERVAL = 1800  # 30 min between forecast collection runs    # Model prob threshold for conviction bets

# POLYMARKET API
POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com"

# SMS NOTIFICATIONS
SMS_ON_EVERY_BET = False       # Don't spam per-bet
SMS_ON_BET_RESOLVE = True      # Alert when bet resolves (win/loss)
SMS_SUMMARY_INTERVAL = 21600   # Every 6 hours (seconds)

# POSITION MANAGEMENT
POSITION_CHECK_INTERVAL = 600  # Re-check positions every 10 min

# SELF-IMPROVEMENT
IMPROVEMENT_INTERVAL = 86400  # 24 hours (daily)
MIN_BETS_FOR_ADJUSTMENT = 10

# RISK MANAGEMENT
MAX_BET_SIZE = 0.10  # Never bet more than 10% on single market
STOP_LOSS_DAILY = None  # Disabled
PAUSE_AFTER_LOSSES = None  # Disabled

# LEARNING PARAMETERS
DAILY_RESET_HOUR = 22  # 10 PM UTC - reset daily counters
PERFORMANCE_LOOKBACK_DAYS = 7
MIN_WIN_RATE_TARGET = 0.65
ADJUST_WEIGHTS_THRESHOLD = 0.05

# DATABASE
PERFORMANCE_DB = 'hedge_fund_performance.db'
CACHE_DURATION = 120  # 2 minutes cache for API data

# LOGGING
LOG_LEVEL = 'INFO'
LOG_FILE = 'hedge_fund.log'

# =============================================================
# CRYPTO BETTING CONFIG — Single cycle, 72h max resolution
# =============================================================

CRYPTO_SCAN_INTERVAL = 120       # 2 minutes between scans
CRYPTO_MAX_HOURS = 168            # 7 days out — find markets early for longer odds
CRYPTO_MAX_DAILY_BETS = 20       # 20 crypto bets per day (bumped: 56% WR, +$600 P&L)
CRYPTO_MAX_CONCURRENT = 10       # 10 open crypto positions (bumped: more capacity)
CRYPTO_MIN_CONFIDENCE = 70       # Minimum confidence score (0-100)
CRYPTO_MIN_EDGE = 0.15           # 15% minimum edge
CRYPTO_BET_MIN = 3.0             # $3 flat per bet
CRYPTO_BET_MAX = 3.0             # $3 flat per bet

# --- COMBINED RISK LIMITS ---
TOTAL_MAX_DEPLOYMENT_PCT = 0.70  # 70% cap across all modules
TOTAL_MAX_CONCURRENT = 25        # Hard cap all positions (crypto + weather + avantis)
MIN_RESERVE_BALANCE = 5.0        # Always keep $5 reserve

# =============================================================
# AVANTIS LEVERAGE TRADING CONFIG (Base chain, USDC + ETH gas)
# =============================================================
AVANTIS_SCAN_INTERVAL = 900       # 15 min between scans
AVANTIS_MAX_DAILY_TRADES = 5      # Max 5 trades per day
AVANTIS_MAX_CONCURRENT = 3        # Max 3 open positions
AVANTIS_MIN_CONFIDENCE = 0.65     # 65% minimum signal confidence
AVANTIS_COLLATERAL = 5.0          # $5 collateral per trade
AVANTIS_MAX_LEVERAGE = 20        # Was 75. Liquidation ~5% away at 20x
AVANTIS_DEFAULT_SL = 50.0         # 50% stop loss for 20x leverage
AVANTIS_DEFAULT_TP = 100.0        # 100% take profit for 20x leverage

# Crypto keywords for filtering Polymarket markets
CRYPTO_KEYWORDS = [
    'bitcoin', 'btc', 'ethereum', 'eth', 'solana', 'sol',
    'crypto', 'cryptocurrency', 'defi', 'token',
    'xrp', 'ripple', 'cardano', 'ada', 'dogecoin', 'doge',
    'polygon', 'matic', 'avalanche', 'avax', 'chainlink', 'link',
    'hyperliquid', 'hype',
]

# Keywords to EXCLUDE from crypto scanning
CRYPTO_EXCLUDE_KEYWORDS = [
    'weather', 'temperature', 'rain', 'snow', 'sports',
    'football', 'basketball', 'baseball', 'soccer',
    'election', 'president', 'congress', 'senate',
]

# CoinGecko price mapping (market keyword -> coingecko id)
CRYPTO_COINGECKO_IDS = {
    'bitcoin': 'bitcoin', 'btc': 'bitcoin',
    'ethereum': 'ethereum', 'eth': 'ethereum',
    'solana': 'solana', 'sol': 'solana',
    'xrp': 'ripple', 'ripple': 'ripple',
    'cardano': 'cardano', 'ada': 'cardano',
    'dogecoin': 'dogecoin', 'doge': 'dogecoin',
    'polygon': 'matic-network', 'matic': 'matic-network',
    'avalanche': 'avalanche-2', 'avax': 'avalanche-2',
    'chainlink': 'chainlink', 'link': 'chainlink',
    'hyperliquid': 'hyperliquid', 'hype': 'hyperliquid',
}


# =============================================================
# SPORTS ANALYST CONFIG — Baggins' Buddy
# =============================================================
ENABLE_SPORTS_MODULE = True
SPORTS_SCAN_INTERVAL = 900        # 15 min between scans
SPORTS_BET_SIZE = 1.0             # $1 per sports bet (small, exploratory)
SPORTS_MAX_DAILY = 3              # Max 3 sports bets per day
SPORTS_MIN_EDGE = 0.10            # 10% edge minimum (if bookmaker odds available)
SPORTS_MIN_CONFIDENCE = 55        # Minimum confidence score (0-100) to place bet
SPORTS_FOCUS_EVENTS = ['ufc', 'boxing', 'nba_playoffs', 'nhl_playoffs', 'champions_league']
