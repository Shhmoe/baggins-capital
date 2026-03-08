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

# SCALPER (Up/Down 15-min markets)
UPDOWN_BET_SIZE = 2.0             # Flat $2 per scalp
UPDOWN_MIN_SCORE = 4.5            # Score range: 4.5-6.5 (below = low confidence, above = momentum trap)
UPDOWN_MAX_DAILY = 12             # Max bets per day (4 assets x quality)
UPDOWN_DRAWDOWN_LIMIT = 10.0      # $10/day loss limit
UPDOWN_COOLDOWN_MINUTES = 45      # Cooldown after 1 loss
UPDOWN_COOLDOWN_AFTER_LOSSES = 1  # Pause after 1 loss
UPDOWN_UP_ONLY = False            # Score decides direction — both UP and DOWN allowed
UPDOWN_MAX_PRICE = 0.45           # Only bet when our side is cheap (value zone)
UPDOWN_MIN_PRICE = 0.35           # Not too cheap/risky
UPDOWN_ASSETS = ['btc', 'eth', 'sol', 'xrp']  # All available up/down markets
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
CRYPTO_MAX_HOURS = 720            # 30 days out — catches end-of-month commodity/equity markets
CRYPTO_MAX_DAILY_BETS = 20       # 20 crypto bets per day
CRYPTO_MAX_CONCURRENT = 15       # 15 open crypto positions
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
    'gold', 'silver', 'crude oil', 'oil',
    'nvidia', 'nvda', 'tesla', 'tsla',
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

# Yahoo Finance price mapping (for commodities/equities — no API key needed)
# Assets detected via these keywords route to Yahoo Finance instead of CoinGecko
YAHOO_FINANCE_SYMBOLS = {
    'gold': 'GC=F', 'silver': 'SI=F',
    'crude oil': 'CL=F', 'oil': 'CL=F',
    'nvidia': 'NVDA', 'nvda': 'NVDA',
    'tesla': 'TSLA', 'tsla': 'TSLA',
    'bitcoin': 'BTC-USD', 'btc': 'BTC-USD',
    'ethereum': 'ETH-USD', 'eth': 'ETH-USD',
    'solana': 'SOL-USD', 'sol': 'SOL-USD',
    'xrp': 'XRP-USD',
    'hyperliquid': 'HYPE-USD', 'hype': 'HYPE-USD',
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
