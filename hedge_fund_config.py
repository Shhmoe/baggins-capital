"""
Hedge Fund Agent Configuration — Baggins Capital V3.1
Each department has its own isolated parameter block. No overlap.
"""

# ═══════════════════════════════════════════════════════════════
# GLOBAL — shared infrastructure, not department-specific
# ═══════════════════════════════════════════════════════════════

DRY_RUN = False
STARTING_BALANCE = 50.00
PERFORMANCE_DB = 'hedge_fund_performance.db'
POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com"
CACHE_DURATION = 120
LOG_LEVEL = 'INFO'
LOG_FILE = 'hedge_fund.log'
DAILY_RESET_HOUR = 22              # 10 PM UTC
PERFORMANCE_LOOKBACK_DAYS = 7
MIN_WIN_RATE_TARGET = 0.65
ADJUST_WEIGHTS_THRESHOLD = 0.05
IMPROVEMENT_INTERVAL = 86400       # 24h
MIN_BETS_FOR_ADJUSTMENT = 10
POSITION_CHECK_INTERVAL = 600      # 10 min
SMS_ON_EVERY_BET = False
SMS_ON_BET_RESOLVE = True
SMS_SUMMARY_INTERVAL = 21600       # 6h

# ── Global risk caps (CFO enforces) ──
TOTAL_MAX_DEPLOYMENT_PCT = 0.75    # 75% cap across all modules
TOTAL_MAX_CONCURRENT = 40          # Hard cap all positions combined
MIN_RESERVE_BALANCE = 5.0          # Always keep $5 reserve
MAX_BET_SIZE = 0.10                # Never >10% on single market
STOP_LOSS_DAILY = None
PAUSE_AFTER_LOSSES = None

# ═══════════════════════════════════════════════════════════════
# DEPARTMENT TOGGLES
# ═══════════════════════════════════════════════════════════════

ENABLE_POLYMARKET_CRYPTO = True
ENABLE_CRYPTO_MODULE = True
ENABLE_WEATHER_MODULE = True
ENABLE_UPDOWN_MODULE = True
ENABLE_SPORTS_MODULE = False
ENABLE_AVANTIS = True
PAUSED_POLYMARKET = False
PAUSED_AVANTIS = True

# ═══════════════════════════════════════════════════════════════
# CRYPTO DEPARTMENT — Star performer, maximum freedom
# ═══════════════════════════════════════════════════════════════

CRYPTO_SCAN_INTERVAL = 120         # 2 min between scans
CRYPTO_MAX_HOURS = 720             # 30 days out
CRYPTO_BET_MIN = 3.0               # $3 low confidence
CRYPTO_BET_MAX = 12.0              # $12 high confidence
CRYPTO_MAX_DAILY_BETS = 25         # 25/day (was 20)
CRYPTO_MAX_CONCURRENT = 20         # 20 open positions (was 15)
CRYPTO_MIN_CONFIDENCE = 65         # Lower bar (was 70) — let it find more
CRYPTO_MIN_EDGE = 0.12             # 12% min edge (was 15%) — more markets

# Dynamic modifiers (data-driven from DB)
CRYPTO_MODIFIER_WINDOW_DAYS = 30
CRYPTO_MODIFIER_MIN_SAMPLE = 5

# ── Keywords for Market Scout to classify as crypto/financial ──
CRYPTO_KEYWORDS = [
    # Crypto — majors
    'bitcoin', 'btc', 'ethereum', 'eth', 'solana', 'sol',
    'xrp', 'ripple', 'cardano', 'ada', 'dogecoin', 'doge',
    'polygon', 'matic', 'avalanche', 'avax', 'chainlink', 'link',
    'hyperliquid', 'hype', 'litecoin', 'ltc',
    # Crypto — broad
    'crypto', 'cryptocurrency', 'defi', 'token',
    # Equities — individual stocks
    'nvidia', 'nvda', 'tesla', 'tsla',
    'apple', 'aapl', 'amazon', 'amzn',
    'google', 'goog', 'googl', 'alphabet',
    'meta', 'facebook',
    'microsoft', 'msft',
    # Indices
    'sp500', 's&p 500', 's&p', 'nasdaq', 'dow jones', 'djia',
    # Commodities
    'gold', 'silver', 'crude oil', 'oil',
    'natural gas', 'copper', 'platinum', 'palladium',
    # Monetary policy (moves all markets)
    'fed rate', 'fed fund', 'interest rate', 'federal reserve',
    # Broad financial terms (catches markets that don't name specific assets)
    'stock price', 'share price', 'market cap',
    'trading above', 'trading below',
    'close above', 'close below',
    'lighter',
]

CRYPTO_EXCLUDE_KEYWORDS = [
    'weather', 'temperature', 'rain', 'snow',
    'sports', 'football', 'basketball', 'baseball', 'soccer',
    'election', 'president', 'congress', 'senate', 'vote',
    'who will win',  # sports phrasing
]

# ── Price source mappings ──
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
    'litecoin': 'litecoin', 'ltc': 'litecoin',
}

YAHOO_FINANCE_SYMBOLS = {
    # Equities
    'nvidia': 'NVDA', 'nvda': 'NVDA',
    'tesla': 'TSLA', 'tsla': 'TSLA',
    'apple': 'AAPL', 'aapl': 'AAPL',
    'amazon': 'AMZN', 'amzn': 'AMZN',
    'google': 'GOOGL', 'goog': 'GOOGL', 'googl': 'GOOGL', 'alphabet': 'GOOGL',
    'meta': 'META', 'facebook': 'META',
    'microsoft': 'MSFT', 'msft': 'MSFT',
    # Indices
    'sp500': '^GSPC', 's&p 500': '^GSPC', 's&p': '^GSPC',
    'nasdaq': '^IXIC',
    'dow jones': '^DJI', 'djia': '^DJI',
    # Commodities
    'gold': 'GC=F',
    'silver': 'SI=F',
    'crude oil': 'CL=F', 'oil': 'CL=F',
    'natural gas': 'NG=F',
    'copper': 'HG=F',
    'platinum': 'PL=F',
    'palladium': 'PA=F',
    # Crypto via Yahoo (fallback if CoinGecko fails)
    'bitcoin': 'BTC-USD', 'btc': 'BTC-USD',
    'ethereum': 'ETH-USD', 'eth': 'ETH-USD',
    'solana': 'SOL-USD', 'sol': 'SOL-USD',
    'xrp': 'XRP-USD',
    'hyperliquid': 'HYPE-USD', 'hype': 'HYPE-USD',
    'litecoin': 'LTC-USD', 'ltc': 'LTC-USD',
}

# ═══════════════════════════════════════════════════════════════
# WEATHER DEPARTMENT — Wider windows, still tuning $1 bets
# ═══════════════════════════════════════════════════════════════

WEATHER_SCAN_INTERVAL = 300        # 5 min between scans
WEATHER_BET_SIZES = {89: 1.0, 92: 1.0, 95: 1.0}  # Flat $1 until proven
WEATHER_MAX_DAILY_BETS = 30        # 30/day
WEATHER_MAX_CONCURRENT = 20        # 20 open positions (was 15)
WEATHER_MIN_CONFIDENCE = 85        # Keep high — WR fragile below this
WEATHER_MIN_EDGE = 0.05            # 5% min edge
WEATHER_CONVICTION_PROB = 0.70

# Wider betting windows (4h each instead of 2h)
WEATHER_BETTING_WINDOWS = [(7, 11), (13, 17), (20, 24)]
WEATHER_COLLECTION_INTERVAL = 1800 # 30 min between forecast collection

# ═══════════════════════════════════════════════════════════════
# SCALPER DEPARTMENT — Conservative, controlled
# ═══════════════════════════════════════════════════════════════

UPDOWN_BET_SIZE = 2.0              # Flat $2
UPDOWN_MIN_SCORE = 5.5             # Ultra-selective
UPDOWN_MAX_DAILY = 8               # 8/day strict
UPDOWN_MAX_CONCURRENT = 4          # 4 open max (was uncapped — now explicit)
UPDOWN_DRAWDOWN_LIMIT = 12.0       # $12/day loss limit
UPDOWN_COOLDOWN_MINUTES = 45       # 45 min cooldown after loss
UPDOWN_COOLDOWN_AFTER_LOSSES = 1   # Pause after 1 loss
UPDOWN_UP_ONLY = False             # No DOWN bets
UPDOWN_MAX_PRICE = 0.42            # Better odds required
UPDOWN_MIN_PRICE = 0.35            # Not too cheap
UPDOWN_ASSETS = ['btc', 'eth']     # BTC/ETH only

# ═══════════════════════════════════════════════════════════════
# SPORTS DEPARTMENT — Small, exploratory
# ═══════════════════════════════════════════════════════════════

SPORTS_SCAN_INTERVAL = 900         # 15 min between scans
SPORTS_BET_SIZE = 1.0              # $1 flat
SPORTS_MAX_DAILY = 3               # 3/day max
SPORTS_MAX_CONCURRENT = 3          # 3 open max
SPORTS_MIN_EDGE = 0.10             # 10% min edge
SPORTS_MIN_CONFIDENCE = 55         # Low bar — exploratory
SPORTS_FOCUS_EVENTS = ['ufc', 'boxing', 'nba_playoffs', 'nhl_playoffs', 'champions_league']

# ═══════════════════════════════════════════════════════════════
# AVANTIS LEVERAGE TRADING — Separate chain (Base)
# ═══════════════════════════════════════════════════════════════

AVANTIS_SCAN_INTERVAL = 900
AVANTIS_MAX_DAILY_TRADES = 5
AVANTIS_MAX_CONCURRENT = 3
AVANTIS_MIN_CONFIDENCE = 0.65
AVANTIS_COLLATERAL = 5.0
AVANTIS_MAX_LEVERAGE = 20
AVANTIS_DEFAULT_SL = 50.0
AVANTIS_DEFAULT_TP = 100.0
