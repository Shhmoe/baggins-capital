"""
The Market Scout — Trading Desk Pre-Screener
Baggins Capital V3.1

Runs FIRST every cycle before any trader or liaison touches anything.
Pulls every available market from Polymarket, runs 7 disqualification checks
in strict order, scores survivors, and delivers one clean queue per department
to the Intelligence Liaisons.

Department: Trading Desk
Reports to: The Manager
"""

import re
import json
import time
import requests
from datetime import datetime, timezone
from db_reader import DBReader
from company_clock import get_context
import hedge_fund_config as config

GAMMA_URL = getattr(config, 'POLYMARKET_GAMMA_URL', 'https://gamma-api.polymarket.com')

# ══════════════════════════════════════════════════════════════
# KEYWORDS — centralized from all traders
# ══════════════════════════════════════════════════════════════

CRYPTO_KEYWORDS = getattr(config, 'CRYPTO_KEYWORDS', [
    'bitcoin', 'btc', 'ethereum', 'eth', 'solana', 'sol', 'crypto',
    'cryptocurrency', 'defi', 'token', 'xrp', 'ripple', 'cardano', 'ada',
    'dogecoin', 'doge', 'polygon', 'matic', 'avalanche', 'avax',
    'chainlink', 'link', 'hyperliquid', 'hype', 'gold', 'silver',
    'crude oil', 'oil', 'nvidia', 'nvda', 'tesla', 'tsla',
    'fed rate', 'fed fund', 'interest rate', 'lighter',
])

CRYPTO_EXCLUDE_KEYWORDS = getattr(config, 'CRYPTO_EXCLUDE_KEYWORDS', [
    'weather', 'temperature', 'rain', 'snow', 'sports', 'football',
    'basketball', 'baseball', 'soccer', 'election', 'president',
    'congress', 'senate',
])

WEATHER_KEYWORDS = [
    'highest temperature', 'temperature', 'temp', 'weather forecast',
    'high temp', 'low temp', 'degrees',
]

SPORTS_TAG_SLUGS = ['ufc', 'mma', 'boxing', 'nba', 'nhl', 'sports']

UPDOWN_PATTERN = re.compile(r'up or down', re.IGNORECASE)


# ══════════════════════════════════════════════════════════════
# SCOUT CONFIG — per-department thresholds
# ══════════════════════════════════════════════════════════════

SCOUT_CONFIG = {
    'crypto': {
        'min_resolution_min': 60,
        'max_resolution_min': 5160,       # 86 hours
        'min_time_to_close_min': 30,
        'min_market_age_min': 30,
        'min_liquidity_usd': 500,
        'max_spread': 0.30,
        'window_required': False,
        'windows': [],
    },
    'weather': {
        'min_resolution_min': 360,        # 6h — skip same-day
        'max_resolution_min': 2880,       # 48h
        'min_time_to_close_min': 30,
        'min_market_age_min': 30,
        'min_liquidity_usd': 100,
        'max_spread': 0.40,
        'window_required': True,
        'windows': [
            'Morning Weather Window',
            'Afternoon Weather Window',
            'Evening Weather Window',
        ],
    },
    'updown': {
        'min_resolution_min': 5,
        'max_resolution_min': 60,
        'min_time_to_close_min': 10,
        'min_market_age_min': 0,          # always fresh by design
        'min_liquidity_usd': 50,
        'max_spread': 0.15,               # tight fills needed
        'window_required': False,
        'windows': [],
    },
    'sports': {
        'min_resolution_min': 60,
        'max_resolution_min': 4320,       # 3 days
        'min_time_to_close_min': 30,
        'min_market_age_min': 30,
        'min_liquidity_usd': 200,
        'max_spread': 0.35,
        'window_required': False,
        'windows': [],
    },
}

# Format compatibility: {department: {format_id: weight}}
FORMAT_COMPATIBILITY = {
    'crypto': {
        'crypto_price': 1.0,
        'crypto_touch': 0.9,
        'crypto_range': 0.7,
    },
    'weather': {
        'temperature': 1.0,
    },
    'updown': {
        'updown': 1.0,
    },
    'sports': {
        'sports_match': 1.0,
        'sports_futures': 0.8,
    },
}

# Priority score weights
PRIORITY_WEIGHTS = {
    'time_pressure': 0.5,
    'liquidity_quality': 0.3,
    'format_match': 0.2,
}


class MarketScout:
    """Pre-screens all Polymarket markets. Delivers clean queues to Liaisons."""

    def __init__(self, db_path='hedge_fund_performance.db'):
        self._reader = DBReader(db_path)
        self._blocklist_cache = []
        self._blocklist_cache_time = None
        self._drop_log = {}
        self._last_queue_depths = {}
        self._last_total_scanned = 0
        self._raw_events = []  # stored for departments that need event-level data

    # ══════════════════════════════════════════════════════════════
    # MAIN ENTRY — called by Manager every cycle
    # ══════════════════════════════════════════════════════════════

    def scan(self):
        """Pull all markets, filter, sort into department queues.

        Returns:
            dict: {department: [market_dict, ...]} sorted by priority score descending
        """
        self._drop_log = {
            'blocklist': 0,
            'format': 0,
            'resolution_window': 0,
            'time_to_close': 0,
            'market_age': 0,
            'liquidity': 0,
            'active_window': 0,
        }

        # Step 0: Fetch everything from Gamma API
        raw_events = self._fetch_all_events()
        self._raw_events = raw_events  # keep for event-level consumers
        raw_markets = self._flatten_to_markets(raw_events)
        total_raw = len(raw_markets)

        if not raw_markets:
            self._log_summary(total_raw, {})
            return {'crypto': [], 'weather': [], 'updown': [], 'sports': []}

        # Step 0.5: Load blocklist
        self._refresh_blocklist()

        # Classify format for each market
        for mkt in raw_markets:
            mkt['format_id'] = self._classify_format(mkt)

        # Build queues per department
        queues = {dept: [] for dept in SCOUT_CONFIG}
        ctx = get_context()

        for mkt in raw_markets:
            placed_in_any = False

            # Check 1: Blocklist (one check per market, not per department)
            if self._check_blocklist(mkt):
                self._drop_log['blocklist'] += 1
                continue

            for dept, dept_config in SCOUT_CONFIG.items():
                # Check 2: Format compatibility
                fmt_weight = FORMAT_COMPATIBILITY.get(dept, {}).get(mkt['format_id'], 0)
                if fmt_weight == 0:
                    continue

                # Check 3: Resolution time window
                ttr = mkt.get('time_to_resolution_min', 0)
                if ttr < dept_config['min_resolution_min'] or ttr > dept_config['max_resolution_min']:
                    self._drop_log['resolution_window'] += 1
                    continue

                # Check 4: Minimum time to close
                if ttr < dept_config['min_time_to_close_min']:
                    self._drop_log['time_to_close'] += 1
                    continue

                # Check 5: Minimum market age
                age = mkt.get('market_age_min', 0)
                if age < dept_config['min_market_age_min']:
                    self._drop_log['market_age'] += 1
                    continue

                # Check 6: Liquidity — pool size and spread
                liq = mkt.get('liquidity_num', 0)
                spread = mkt.get('spread', 1.0)
                if liq < dept_config['min_liquidity_usd']:
                    self._drop_log['liquidity'] += 1
                    continue
                if spread > dept_config['max_spread']:
                    self._drop_log['liquidity'] += 1
                    continue

                # Check 7: Active window check
                if dept_config['window_required']:
                    window_open = any(
                        w in ctx.active_windows for w in dept_config['windows']
                    )
                    if not window_open:
                        self._drop_log['active_window'] += 1
                        continue

                # ── PASSED ALL CHECKS ──
                entry = dict(mkt)
                entry['department'] = dept
                entry['format_weight'] = fmt_weight
                entry['priority_score'] = self._compute_priority(
                    mkt, dept_config, fmt_weight
                )
                queues[dept].append(entry)
                placed_in_any = True

            # Track markets that matched a format but failed all departments
            if not placed_in_any and mkt['format_id'] != 'unknown':
                self._drop_log['format'] += 1

        # Sort each queue by priority (highest first)
        for dept in queues:
            queues[dept].sort(key=lambda m: m['priority_score'], reverse=True)

        self._last_total_scanned = total_raw
        self._last_queue_depths = {dept: len(q) for dept, q in queues.items()}
        self._log_summary(total_raw, queues)
        return queues

    # ══════════════════════════════════════════════════════════════
    # GAMMA API — single unified fetch
    # ══════════════════════════════════════════════════════════════

    def _fetch_all_events(self):
        """Fetch all open events from Gamma API. Paginates up to 1000."""
        all_events = []
        seen_ids = set()

        try:
            for offset in range(0, 1000, 100):
                params = {
                    'closed': 'false',
                    'limit': 100,
                    'offset': offset,
                    'order': 'volume',
                    'ascending': 'false',
                }
                resp = requests.get(
                    f"{GAMMA_URL}/events", params=params, timeout=15
                )
                if resp.status_code != 200:
                    print(f"  [SCOUT] Gamma API error (offset={offset}): {resp.status_code}")
                    break

                events = resp.json()
                if not isinstance(events, list) or len(events) == 0:
                    break

                new_count = 0
                for event in events:
                    eid = event.get('id', '')
                    if eid and eid not in seen_ids:
                        seen_ids.add(eid)
                        all_events.append(event)
                        new_count += 1

                if new_count == 0:
                    break

                time.sleep(0.3)

        except Exception as e:
            print(f"  [SCOUT] Gamma fetch error: {e}")

        return all_events

    def _flatten_to_markets(self, events):
        """Extract individual sub-markets from events with computed fields."""
        markets = []
        now = datetime.now(timezone.utc)

        for event in events:
            event_id = event.get('id', '')
            event_title = event.get('title', '')
            event_tags = event.get('tags', [])
            event_end = event.get('endDate', '')

            # Extract tag slugs for classification
            tag_slugs = []
            if isinstance(event_tags, list):
                for tag in event_tags:
                    if isinstance(tag, dict):
                        tag_slugs.append(tag.get('slug', '').lower())
                    elif isinstance(tag, str):
                        tag_slugs.append(tag.lower())

            sub_markets = event.get('markets', [])
            if not sub_markets:
                continue

            for sm in sub_markets:
                if sm.get('closed', False):
                    continue
                if not sm.get('active', True):
                    continue

                question = sm.get('question', sm.get('title', ''))
                end_date_str = sm.get('endDate', event_end)

                # Parse end date
                end_dt = self._parse_iso_date(end_date_str)
                time_to_resolution_min = 0
                if end_dt:
                    delta = end_dt - now
                    time_to_resolution_min = max(0, delta.total_seconds() / 60)

                # Parse created date for age
                created_str = sm.get('createdAt', '')
                market_age_min = 0
                if created_str:
                    created_dt = self._parse_iso_date(created_str)
                    if created_dt:
                        age_delta = now - created_dt
                        market_age_min = max(0, age_delta.total_seconds() / 60)

                # Parse prices
                outcome_prices_raw = sm.get('outcomePrices', '[]')
                outcome_prices = self._parse_prices(outcome_prices_raw)
                yes_price = outcome_prices[0] if len(outcome_prices) > 0 else 0
                no_price = outcome_prices[1] if len(outcome_prices) > 1 else 0

                # Spread and liquidity
                best_bid = float(sm.get('bestBid', 0) or 0)
                best_ask = float(sm.get('bestAsk', 0) or 0)
                spread = float(sm.get('spread', 0) or 0)
                if spread == 0 and best_ask > 0 and best_bid > 0:
                    spread = best_ask - best_bid

                liquidity_num = float(sm.get('liquidityNum', 0) or 0)
                if liquidity_num == 0:
                    liquidity_num = float(sm.get('liquidity', 0) or 0)

                volume_num = float(sm.get('volumeNum', 0) or 0)
                if volume_num == 0:
                    volume_num = float(sm.get('volume', 0) or 0)

                market_id = sm.get('id', sm.get('questionID', ''))

                days_until = round(time_to_resolution_min / 1440, 1)

                markets.append({
                    # Raw event context
                    'event_id': event_id,
                    'event_title': event_title,
                    'tag_slugs': tag_slugs,
                    'event_description': event.get('description', ''),

                    # Raw market data (pass-through for traders)
                    'market_id': market_id,
                    'question': question,
                    'end_date': end_date_str,
                    'created_at': created_str,
                    'outcomes': sm.get('outcomes', []),
                    'outcome_prices': outcome_prices,
                    'yes_price': yes_price,
                    'no_price': no_price,
                    'liquidity_num': liquidity_num,
                    'volume_num': volume_num,
                    'spread': spread,
                    'best_bid': best_bid,
                    'best_ask': best_ask,
                    'clob_token_ids': sm.get('clobTokenIds', ''),
                    'condition_id': sm.get('conditionId', ''),
                    'slug': sm.get('slug', ''),
                    'group_item_title': sm.get('groupItemTitle', ''),

                    # Scout-computed
                    'time_to_resolution_min': time_to_resolution_min,
                    'market_age_min': market_age_min,
                    'days_until': days_until,
                    'format_id': 'unknown',  # set in classify step

                    # Trader-compatible aliases
                    'id': str(market_id),
                    'title': question,
                    'volume': volume_num,
                    'liquidity': liquidity_num,
                    'description': event.get('description', ''),
                    'sibling_count': len(sub_markets),
                })

        return markets

    # ══════════════════════════════════════════════════════════════
    # FORMAT CLASSIFICATION
    # ══════════════════════════════════════════════════════════════

    def _classify_format(self, mkt):
        """Determine format_id from event/market data.

        Uses tags first (deterministic), then keyword heuristics.
        Returns one of: updown, temperature, sports_match, sports_futures,
                        crypto_price, crypto_touch, crypto_range, unknown
        """
        question_lower = mkt.get('question', '').lower()
        event_title_lower = mkt.get('event_title', '').lower()
        combined = question_lower + ' ' + event_title_lower
        desc_lower = mkt.get('event_description', '').lower()
        tag_slugs = mkt.get('tag_slugs', [])

        # ── Up/Down (most specific, check first) ──
        if UPDOWN_PATTERN.search(combined):
            return 'updown'

        # ── Weather (tag or keyword) ──
        if any(t in tag_slugs for t in ['weather', 'temperature']):
            return 'temperature'
        if any(re.search(r'\b' + re.escape(kw) + r'\b', combined) for kw in WEATHER_KEYWORDS):
            return 'temperature'

        # ── Sports (tag match) ──
        if any(t in tag_slugs for t in SPORTS_TAG_SLUGS):
            # Distinguish match vs futures
            if any(kw in question_lower for kw in ['vs.', 'vs ', 'who will win']):
                return 'sports_match'
            if any(kw in question_lower for kw in ['champion', 'win the 202', 'mvp', 'award']):
                return 'sports_futures'
            return 'sports_match'

        # ── Crypto (keyword match with exclusion) ──
        has_crypto = any(
            re.search(r'\b' + re.escape(kw) + r'\b', combined + ' ' + desc_lower)
            for kw in CRYPTO_KEYWORDS
        )
        has_exclude = any(kw in combined for kw in CRYPTO_EXCLUDE_KEYWORDS)

        if has_crypto and not has_exclude:
            # Sub-classify crypto format
            if any(kw in combined for kw in ['hit', 'reach', 'touch']):
                return 'crypto_touch'
            if any(kw in combined for kw in ['between', 'range', '-']):
                return 'crypto_range'
            return 'crypto_price'

        return 'unknown'

    # ══════════════════════════════════════════════════════════════
    # CHECK 1: BLOCKLIST
    # ══════════════════════════════════════════════════════════════

    def _refresh_blocklist(self):
        """Load active blocklist from DB. Cached for 5 minutes."""
        now = time.time()
        if self._blocklist_cache_time and (now - self._blocklist_cache_time) < 300:
            return

        try:
            rows = self._reader.fetchall(
                "SELECT block_type, block_value FROM compliance_blocklist WHERE active=1"
            )
            self._blocklist_cache = [(r[0], r[1]) for r in rows] if rows else []
            self._blocklist_cache_time = now
        except Exception:
            self._blocklist_cache = []
            self._blocklist_cache_time = now

    def _check_blocklist(self, mkt):
        """Returns True if market is blocklisted (should be dropped)."""
        market_id = str(mkt.get('market_id', ''))
        question_lower = mkt.get('question', '').lower()

        for block_type, block_value in self._blocklist_cache:
            if block_type == 'market_id' and block_value == market_id:
                return True
            if block_type == 'keyword' and block_value.lower() in question_lower:
                return True
        return False

    # ══════════════════════════════════════════════════════════════
    # PRIORITY SCORING
    # ══════════════════════════════════════════════════════════════

    def _compute_priority(self, mkt, dept_config, fmt_weight):
        """Score = (time_pressure * 0.5) + (liquidity_quality * 0.3) + (format_weight * 0.2)"""
        ttr = mkt.get('time_to_resolution_min', 0)
        max_ttr = dept_config['max_resolution_min']

        # Time pressure: higher as market approaches deadline (0.0 to 1.0)
        if max_ttr > 0 and ttr > 0:
            time_pressure = max(0, 1.0 - (ttr / max_ttr))
        else:
            time_pressure = 0

        # Liquidity quality: diminishing returns above 5000 USDC (0.0 to 1.0)
        liq = mkt.get('liquidity_num', 0)
        liquidity_quality = min(1.0, liq / 5000)

        # Spread tightness bonus (tighter = better)
        spread = mkt.get('spread', 1.0)
        if spread < 0.05:
            liquidity_quality = min(1.0, liquidity_quality + 0.2)

        score = (
            time_pressure * PRIORITY_WEIGHTS['time_pressure']
            + liquidity_quality * PRIORITY_WEIGHTS['liquidity_quality']
            + fmt_weight * PRIORITY_WEIGHTS['format_match']
        )
        return round(score, 4)

    # ══════════════════════════════════════════════════════════════
    # LOGGING
    # ══════════════════════════════════════════════════════════════

    def _log_summary(self, total_raw, queues):
        """Print cycle summary for Manager visibility."""
        total_queued = sum(len(q) for q in queues.values()) if queues else 0

        print(f"\n  [SCOUT] Scanned {total_raw} markets → {total_queued} queued")

        if self._drop_log:
            drops = [f"{k}={v}" for k, v in self._drop_log.items() if v > 0]
            if drops:
                print(f"  [SCOUT] Filtered: {', '.join(drops)}")

        for dept, q in (queues or {}).items():
            if q:
                print(f"  [SCOUT] {dept}: {len(q)} markets (top: {q[0]['question'][:50]}...)")
            else:
                print(f"  [SCOUT] {dept}: 0 markets")

    def get_drop_summary(self):
        """Return last scan's drop log. For Market Pulse to track."""
        return dict(self._drop_log)

    def get_queue_depths(self):
        """Return queue depths from last scan. For Pulse tracking."""
        return dict(self._last_queue_depths)

    def get_weather_events(self):
        """Return raw Gamma events that contain weather/temperature markets.

        The weather scanner needs event-level data (grouped sub-markets)
        for city/date/temp parsing. This gives it pre-fetched events
        so it skips its own Gamma API call.
        """
        weather_events = []
        for event in self._raw_events:
            title = event.get('title', '').lower()
            tags = event.get('tags', [])
            tag_slugs = []
            if isinstance(tags, list):
                for tag in tags:
                    if isinstance(tag, dict):
                        tag_slugs.append(tag.get('slug', '').lower())

            is_weather = any(t in tag_slugs for t in ['weather', 'temperature'])
            if not is_weather:
                is_weather = any(
                    re.search(r'\b' + re.escape(kw) + r'\b', title)
                    for kw in WEATHER_KEYWORDS
                )

            if is_weather:
                weather_events.append(event)

        return weather_events

    def get_sports_events(self):
        """Return raw Gamma events that contain sports markets.

        The sports analyst needs event-level data for odds comparison
        and market classification. Pre-fetched from Scout's unified scan.
        """
        sports_events = []
        for event in self._raw_events:
            tags = event.get('tags', [])
            tag_slugs = []
            if isinstance(tags, list):
                for tag in tags:
                    if isinstance(tag, dict):
                        tag_slugs.append(tag.get('slug', '').lower())

            if any(t in tag_slugs for t in SPORTS_TAG_SLUGS):
                sports_events.append(event)

        return sports_events

    # ══════════════════════════════════════════════════════════════
    # HELPERS
    # ══════════════════════════════════════════════════════════════

    @staticmethod
    def _parse_iso_date(date_str):
        """Parse ISO date string to datetime (UTC)."""
        if not date_str:
            return None
        try:
            # Handle various ISO formats
            cleaned = date_str.replace('Z', '+00:00')
            return datetime.fromisoformat(cleaned)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_prices(prices_raw):
        """Parse outcomePrices from Gamma API (can be string or list)."""
        if isinstance(prices_raw, list):
            try:
                return [float(p) for p in prices_raw]
            except (ValueError, TypeError):
                return []
        if isinstance(prices_raw, str):
            try:
                parsed = json.loads(prices_raw)
                return [float(p) for p in parsed]
            except (json.JSONDecodeError, ValueError, TypeError):
                return []
        return []
