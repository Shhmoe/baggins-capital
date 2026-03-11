"""
Polymarket Crypto Market Scanner & Edge Evaluator v2.0

Deep Strategy Framework:
1. PROBABILITY COMPRESSION - retail compresses odds toward 50%, find the gaps
2. STRUCTURAL INEFFICIENCY - exploit low liquidity, slow price discovery, emotional retail
3. INFORMATION LATENCY - our real-time CoinGecko data beats retail traders' vibes
4. FAT TAIL HARVESTING - buy cheap sides (<15c) where true prob is 2x+ market price
5. CROSS-MARKET CORRELATION - detect related markets that haven't synced
6. TIME-DECAY SQUEEZE - short-dated markets are more predictable, exploit last-day panic
7. MISPRICING CHECKLIST - multi-factor scoring before every bet

Bet Types:
- FADE: bet against extreme moves (bread & butter, high win rate)
- MOMENTUM: buy cheap asymmetric sides confirmed by strong price action
- COMPRESSION: exploit probability compression (true 20% trading at 8%)
- DECAY: exploit time-decay near expiry (prices overshoot)
"""

import re
import json
import os
import math
import time
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from hedge_fund_config import (
    PERFORMANCE_DB,
    POLYMARKET_GAMMA_URL,
    CRYPTO_KEYWORDS, CRYPTO_EXCLUDE_KEYWORDS,
    CRYPTO_COINGECKO_IDS,
    CACHE_DURATION,
    CRYPTO_MIN_CONFIDENCE, CRYPTO_MIN_EDGE, CRYPTO_BET_MIN, CRYPTO_BET_MAX,
    CRYPTO_MODIFIER_WINDOW_DAYS, CRYPTO_MODIFIER_MIN_SAMPLE,
)
from company_clock import now_utc, now_et, current_hour_et, status as clock_status

try:
    from hedge_fund_config import YAHOO_FINANCE_SYMBOLS
except ImportError:
    YAHOO_FINANCE_SYMBOLS = {}


class CryptoMarketScanner:
    """Scan Polymarket for crypto markets and evaluate betting edge."""

    def __init__(self):
        self.cache = {}
        self.price_cache = {}
        self.cache_duration = CACHE_DURATION
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), PERFORMANCE_DB)
        self._hour_mod_cache = None
        self._hour_mod_cache_time = None

    def _get_hour_modifier(self) -> Dict[int, float]:
        """Compute per-ET-hour win-rate modifier from rolling 30-day data.
        Hours with <MIN_SAMPLE bets get neutral 1.0. Cached for 5 min."""
        # Cache: avoid DB hit on every market evaluation
        if self._hour_mod_cache is not None and self._hour_mod_cache_time is not None:
            if (datetime.now() - self._hour_mod_cache_time).total_seconds() < 300:
                return self._hour_mod_cache
        try:
            from hedge_fund_config import CRYPTO_MODIFIER_WINDOW_DAYS, CRYPTO_MODIFIER_MIN_SAMPLE
        except ImportError:
            return {}
        modifiers = {}
        try:
            from archivist import Archivist
            _arch = Archivist(self.db_path)
            rows = _arch._fetchall("""
                SELECT timestamp, won FROM bets
                WHERE category = 'crypto' AND cycle_type != 'updown'
                AND status = 'resolved' AND won IS NOT NULL
                AND timestamp >= datetime('now', ?)
            """, (f'-{CRYPTO_MODIFIER_WINDOW_DAYS} days',))

            # Bucket by ET hour
            from company_clock import COMPANY_TZ
            from datetime import timezone
            hour_stats = {}  # {hour_et: [wins, total]}
            for ts_str, won in rows:
                try:
                    # Parse UTC timestamp, convert to ET
                    dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    et_hour = dt.astimezone(COMPANY_TZ).hour
                    if et_hour not in hour_stats:
                        hour_stats[et_hour] = [0, 0]
                    hour_stats[et_hour][1] += 1
                    if won:
                        hour_stats[et_hour][0] += 1
                except Exception:
                    continue

            for hour, (wins, total) in hour_stats.items():
                if total >= CRYPTO_MODIFIER_MIN_SAMPLE:
                    wr = wins / total
                    mod = 1.0 + (wr - 0.50) * 0.5
                    modifiers[hour] = max(0.7, min(1.3, mod))
                else:
                    modifiers[hour] = 1.0

            if modifiers:
                print(f"  [CRYPTO TRADER] Hour modifiers (ET): { {h: f'{m:.2f}' for h, m in sorted(modifiers.items())} }")
        except Exception as e:
            print(f"  [CRYPTO TRADER] Hour modifier error: {e}")
        self._hour_mod_cache = modifiers
        self._hour_mod_cache_time = datetime.now()
        return modifiers

    def _get_asset_modifier(self, coin_id: str) -> float:
        """Compute per-asset win-rate modifier vs baseline, rolling 30-day window."""
        try:
            from hedge_fund_config import CRYPTO_MODIFIER_WINDOW_DAYS, CRYPTO_MODIFIER_MIN_SAMPLE
        except ImportError:
            return 1.0
        try:
            from archivist import Archivist
            _arch = Archivist(self.db_path)
            window_clause = f'-{CRYPTO_MODIFIER_WINDOW_DAYS} days'

            # Baseline: all crypto WR in window
            base_row = _arch._fetchone("""
                SELECT COUNT(*), SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) FROM bets
                WHERE category = 'crypto' AND cycle_type != 'updown'
                AND status = 'resolved' AND won IS NOT NULL
                AND timestamp >= datetime('now', ?)
            """, (window_clause,))
            base_total = base_row[0] if base_row else 0
            base_wins = base_row[1] if base_row else 0
            baseline_wr = base_wins / base_total if base_total >= CRYPTO_MODIFIER_MIN_SAMPLE else 0.50

            # Asset-specific: match via market_title LIKE
            # Build search patterns from coin_id
            search_term = coin_id.replace('-', ' ')
            if coin_id == 'bitcoin':
                like_pattern = '%bitcoin%'
            elif coin_id == 'ethereum':
                like_pattern = '%ethereum%'
            elif coin_id == 'solana':
                like_pattern = '%solana%'
            elif coin_id == 'ripple':
                like_pattern = '%xrp%'
            else:
                like_pattern = f'%{search_term}%'

            asset_row = _arch._fetchone("""
                SELECT COUNT(*), SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) FROM bets
                WHERE category = 'crypto' AND cycle_type != 'updown'
                AND status = 'resolved' AND won IS NOT NULL
                AND timestamp >= datetime('now', ?)
                AND LOWER(market_title) LIKE ?
            """, (window_clause, like_pattern))

            asset_total = asset_row[0] if asset_row else 0
            asset_wins = asset_row[1] if asset_row else 0

            if asset_total < CRYPTO_MODIFIER_MIN_SAMPLE:
                return 1.0

            asset_wr = asset_wins / asset_total
            modifier = 1.0 + (asset_wr - baseline_wr) * 0.4
            modifier = max(0.75, min(1.25, modifier))
            print(f"  [CRYPTO TRADER] Asset modifier {coin_id}: {modifier:.2f} (WR={asset_wr:.0%} vs baseline={baseline_wr:.0%}, n={asset_total})")
            return modifier
        except Exception as e:
            print(f"  [CRYPTO TRADER] Asset modifier error: {e}")
            return 1.0

    def _get_analyst_overrides(self) -> Dict[str, Dict]:
        """Read analyst overrides from agent_state table. Returns dict keyed by asset name (lowercased)."""
        overrides = {}
        try:
            from archivist import Archivist
            _arch = Archivist(self.db_path)
            rows = _arch._fetchall(
                "SELECT key, value FROM agent_state WHERE key LIKE 'analyst_override_%'"
            )
            for row in rows:
                key, value = row
                asset = key.replace("analyst_override_", "").lower()
                try:
                    data = json.loads(value)
                    overrides[asset] = data
                except json.JSONDecodeError:
                    print(f"    [ANALYST] Bad JSON for override {key}")
        except Exception as e:
            print(f"    [ANALYST] Error reading overrides: {e}")
        return overrides

    def _apply_analyst_override(self, confidence: int, coin_id: str, side: str,
                                 direction: str, title: str) -> int:
        """Check for analyst overrides matching this asset. Returns adjusted confidence."""
        overrides = self._get_analyst_overrides()
        if not overrides:
            return confidence

        # Match asset name case-insensitively with partial matching
        asset_names = [coin_id.lower(), title.lower()]
        matched_key = None
        matched_override = None

        for override_asset, override_data in overrides.items():
            for name in asset_names:
                if override_asset in name or name in override_asset:
                    matched_key = override_asset
                    matched_override = override_data
                    break
            if matched_override:
                break

        if not matched_override:
            return confidence

        # Check expiry
        expires = matched_override.get("expires", "")
        if expires:
            try:
                exp_date = datetime.strptime(expires, "%Y-%m-%d")
                if datetime.now() > exp_date:
                    print(f"    [ANALYST] Override for \"{matched_key}\" expired {expires} — ignoring")
                    return confidence
            except ValueError:
                pass

        override_dir = matched_override.get("direction", "").lower()
        boost = matched_override.get("confidence_boost", 0)
        reason = matched_override.get("reason", "no reason given")

        # Determine alignment:
        # Bullish = price going up. Bearish = price going down.
        # bet_is_bullish: YES on above, or NO on below
        bet_is_bullish = (
            (side == "yes" and direction == "above") or
            (side == "no" and direction == "below")
        )
        bet_is_bearish = not bet_is_bullish

        if override_dir == "bullish":
            if bet_is_bullish:
                adjustment = boost
            else:
                adjustment = -boost
        elif override_dir == "bearish":
            if bet_is_bearish:
                adjustment = boost
            else:
                adjustment = -boost
        else:
            print(f"    [ANALYST] Unknown direction \"{override_dir}\" for {matched_key}")
            return confidence

        new_confidence = confidence + adjustment
        align_str = "ALIGNED" if adjustment > 0 else "CONFLICTING"
        print(f"    [ANALYST] Override for \"{matched_key}\": {override_dir} ({reason})")
        print(f"    [ANALYST] {align_str} with {side.upper()} on {direction} — confidence {confidence} -> {new_confidence} ({adjustment:+d})")
        return new_confidence

    def get_15min_momentum(self, coin_id):
        """Get short-term momentum from CoinGecko hourly data. Returns % change over last hour."""
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
            params = {'vs_currency': 'usd', 'days': '1'}
            response = requests.get(url, params=params, timeout=10)
            if response.status_code != 200:
                return 0.0
            data = response.json()
            prices = data.get('prices', [])
            if len(prices) < 2:
                return 0.0
            current = prices[-1][1]
            hour_ago = prices[-4][1] if len(prices) >= 4 else prices[0][1]  # ~1h ago (15min intervals)
            return ((current - hour_ago) / hour_ago) * 100
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Market scanning
    # ------------------------------------------------------------------

    def scan_crypto_markets(self) -> List[Dict]:
        """Fetch and filter crypto-related markets from Gamma API."""
        cache_key = "crypto_markets"
        if cache_key in self.cache:
            cached_time, cached_data = self.cache[cache_key]
            if (datetime.now() - cached_time).seconds < self.cache_duration:
                print(f"  [Cache] Returning {len(cached_data)} cached crypto markets")
                return cached_data

        raw_events = self._fetch_crypto_events()
        markets = self._flatten_events(raw_events)

        print(f"[CRYPTO TRADER] Found {len(raw_events)} crypto events, {len(markets)} sub-markets")
        self.cache[cache_key] = (datetime.now(), markets)
        return markets

    def _fetch_crypto_events(self) -> List[Dict]:
        """Fetch events from Gamma API and filter for crypto. Paginates up to 3 pages."""
        all_events = []
        seen_ids = set()

        try:
            url = f"{POLYMARKET_GAMMA_URL}/events"

            for offset in range(0, 600, 100):  # 6 pages — catches commodity markets deeper in results
                params = {
                    'closed': 'false',
                    'limit': 100,
                    'offset': offset,
                    'order': 'volume',
                    'ascending': 'false',
                }

                response = requests.get(url, params=params, timeout=15)
                if response.status_code != 200:
                    print(f"  [!] Gamma API error (offset={offset}): {response.status_code}")
                    break

                events = response.json()
                if not isinstance(events, list) or len(events) == 0:
                    break

                for event in events:
                    event_id = event.get('id', '')
                    if event_id in seen_ids:
                        continue
                    seen_ids.add(event_id)

                    title = event.get('title', '').lower()
                    desc = event.get('description', '').lower()
                    combined = title + ' ' + desc

                    has_crypto = any(
                        re.search(r'\b' + re.escape(kw) + r'\b', combined)
                        for kw in CRYPTO_KEYWORDS
                    )
                    if not has_crypto:
                        continue

                    has_exclude = any(kw in combined for kw in CRYPTO_EXCLUDE_KEYWORDS)
                    if has_exclude:
                        continue

                    all_events.append(event)

                time.sleep(0.5)  # Rate limit between pages

        except Exception as e:
            print(f"  [!] Gamma API fetch error: {e}")

        return all_events

    def _flatten_events(self, events: List[Dict]) -> List[Dict]:
        """Flatten events into sub-markets. Keeps <30 day markets."""
        markets = []
        now = datetime.now()

        for event in events:
            event_title = event.get('title', '')
            event_id = event.get('id', '')
            event_desc = event.get('description', '')
            sibling_count = len(event.get('markets', []))

            sub_markets = event.get('markets', [])
            for m in sub_markets:
                market_end_str = m.get('endDate', '') or event.get('endDate', '')
                if not market_end_str:
                    continue

                try:
                    end_date = datetime.fromisoformat(market_end_str.replace('Z', '+00:00'))
                    days_until = (end_date.replace(tzinfo=None) - now).total_seconds() / 86400
                    if days_until > 30 or days_until < 0:
                        continue
                except (ValueError, TypeError):
                    continue

                yes_price = 0.50
                no_price = 0.50
                try:
                    prices = json.loads(m.get('outcomePrices', '[]'))
                    if len(prices) >= 2:
                        yes_price = float(prices[0])
                        no_price = float(prices[1])
                except:
                    pass

                if yes_price <= 0.02 or no_price <= 0.02:
                    continue

                group_title = m.get('groupItemTitle', '')
                question = m.get('question', '')
                volume = m.get('volumeNum', 0) or 0
                liquidity = m.get('liquidityNum', 0) or 0

                markets.append({
                    'id': str(m.get('id', '')),
                    'event_id': str(event_id),
                    'title': question or event_title,
                    'event_title': event_title,
                    'group_title': group_title,
                    'category': 'crypto',
                    'yes_price': yes_price,
                    'no_price': no_price,
                    'volume': volume,
                    'liquidity': liquidity,
                    'end_date': market_end_str[:10],
                    'days_until': round(days_until, 1),
                    'description': event_desc,
                    'question': question,
                    'clob_token_ids': m.get('clobTokenIds', ''),
                    'sibling_count': sibling_count,
                })

        return markets

    # ------------------------------------------------------------------
    # Price & momentum (Information Latency edge)
    # ------------------------------------------------------------------

    def _bulk_fetch_prices(self, coin_ids: List[str]):
        """Batch-fetch prices for all needed coins in one API call.
        Avoids CoinGecko rate limits by combining requests.
        """
        # Filter out already-cached coins
        needed = []
        for cid in set(coin_ids):
            cache_key = f"change_{cid}"
            if cache_key in self.price_cache:
                cached_time, _ = self.price_cache[cache_key]
                if (datetime.now() - cached_time).seconds < 120:
                    continue
            needed.append(cid)

        if not needed:
            return

        try:
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {
                'ids': ','.join(needed),
                'vs_currencies': 'usd',
                'include_24hr_change': 'true',
            }

            for attempt in range(3):
                resp = requests.get(url, params=params, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    for cid in needed:
                        if cid in data:
                            result = {
                                'price': data[cid].get('usd', 0),
                                'change_24h': data[cid].get('usd_24h_change', 0),
                                'change_7d': 0,  # Not available on free tier
                            }
                            self.price_cache[f"change_{cid}"] = (datetime.now(), result)
                    return
                elif resp.status_code == 429:
                    wait = 30 * (attempt + 1)
                    print(f"  [!] CoinGecko rate limited, waiting {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"  [!] CoinGecko error: {resp.status_code}")
                    return
        except Exception as e:
            print(f"  [!] CoinGecko bulk fetch error: {e}")

    def _get_price_change(self, coin_id: str) -> Optional[Dict]:
        """Get price with 24h change from cache (populated by _bulk_fetch_prices)."""
        cache_key = f"change_{coin_id}"
        if cache_key in self.price_cache:
            cached_time, cached_data = self.price_cache[cache_key]
            if (datetime.now() - cached_time).seconds < 120:
                return cached_data

        # Fallback: single fetch with retry
        try:
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {
                'ids': coin_id,
                'vs_currencies': 'usd',
                'include_24hr_change': 'true',
            }
            for attempt in range(2):
                resp = requests.get(url, params=params, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if coin_id in data:
                        result = {
                            'price': data[coin_id].get('usd', 0),
                            'change_24h': data[coin_id].get('usd_24h_change', 0),
                            'change_7d': 0,
                        }
                        self.price_cache[cache_key] = (datetime.now(), result)
                        return result
                elif resp.status_code == 429:
                    time.sleep(30)
                else:
                    break
        except Exception as e:
            print(f"  [!] CoinGecko error ({coin_id}): {e}")

        return None

    def _get_yahoo_price(self, symbol: str):
        """Get price data from Yahoo Finance for commodities/equities."""
        cache_key = f"change_yf:{symbol}"
        if cache_key in self.price_cache:
            cached_time, cached_data = self.price_cache[cache_key]
            if (datetime.now() - cached_time).seconds < 120:
                return cached_data
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=7d"
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                result_data = data["chart"]["result"][0]
                meta = result_data["meta"]
                price = meta.get("regularMarketPrice", 0)
                prev = meta.get("chartPreviousClose", 0)
                change_24h = ((price - prev) / prev * 100) if prev else 0
                closes = result_data["indicators"]["quote"][0].get("close", [])
                closes = [c for c in closes if c is not None]
                change_7d = ((closes[-1] - closes[0]) / closes[0] * 100) if len(closes) >= 2 else 0
                result = {"price": price, "change_24h": change_24h, "change_7d": change_7d}
                self.price_cache[cache_key] = (datetime.now(), result)
                return result
        except Exception as e:
            print(f"  [!] Yahoo Finance error ({symbol}): {e}")
        return None

    def _detect_coin_in_text(self, text: str):
        """Detect which asset a market is about. Returns CoinGecko ID or yf:SYMBOL."""
        text_lower = text.lower()
        for keyword, cg_id in CRYPTO_COINGECKO_IDS.items():
            if re.search(chr(92)+chr(98) + re.escape(keyword) + chr(92)+chr(98), text_lower):
                return cg_id
        for keyword, yf_sym in YAHOO_FINANCE_SYMBOLS.items():
            if re.search(chr(92)+chr(98) + re.escape(keyword) + chr(92)+chr(98), text_lower):
                return f"yf:{yf_sym}"
        return None

    def _parse_price_target(self, text: str) -> Optional[float]:
        """Parse price target from market title."""
        patterns = [
            r'\$(\d{1,3}(?:,\d{3})+)',
            r'\$(\d+(?:\.\d+)?)\s*[kK]',
            r'\$(\d+(?:\.\d+)?)',
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                value_str = match.group(1).replace(',', '')
                value = float(value_str)
                if 'k' in text[match.start():match.end()+2].lower() and value < 1000:
                    value *= 1000
                return value
        return None

    def _determine_direction(self, text: str) -> Optional[str]:
        """Determine if market asks about price going UP or DOWN.
        Also classifies market format type as side effect."""
        text_lower = text.lower()

        # Classify format type
        self._last_format_type = 'unknown'
        if any(w in text_lower for w in ['hit', 'reach', 'touch']):
            self._last_format_type = 'touch'
        elif any(w in text_lower for w in ['be above', 'be below', 'settle above', 'settle below', 'close above', 'close below']):
            self._last_format_type = 'settlement'
        elif re.search(r'\$[\d,.]+\s*[-\u2013]\s*\$[\d,.]+', text_lower):
            self._last_format_type = 'range'

        down_words = ['dip to', 'dip below', 'drop to', 'drop below', 'fall to',
                       'fall below', 'crash', 'below', 'under', 'lower than',
                       'decline', 'less than', 'or less']
        up_words = ['above', 'over', 'reach', 'exceed', 'higher than', 'break',
                     'surpass', 'hit', 'top', 'at least', 'or more', 'greater than']

        for w in down_words:
            if w in text_lower:
                if self._last_format_type == 'unknown':
                    self._last_format_type = 'threshold_below'
                return 'below'
        for w in up_words:
            if w in text_lower:
                return 'above'

        return None

    # ------------------------------------------------------------------
    # Signal Stack (multiple independent signals)
    # ------------------------------------------------------------------

    def _build_signal_stack(self, distance_pct, momentum, change_24h,
                             change_7d, days_until, volume, liquidity,
                             buy_price, direction) -> Dict:
        """
        Build a signal stack - multiple independent signals pointing same way.
        Each signal is scored 0-1. More aligned signals = higher confidence.

        Signals:
        1. Distance signal - how far from target (relates to probability)
        2. Momentum signal - is price moving toward or away from target
        3. Volatility signal - is 24h move consistent with 7d trend
        4. Time signal - less time remaining = more predictable
        5. Liquidity signal - thin markets = more mispricing opportunity
        6. Compression signal - is the price suspiciously compressed
        """
        signals = {}
        abs_dist = abs(distance_pct)

        # 1. Distance signal: further from target = clearer outcome
        # For short-term markets (<3d), even small distances matter
        if days_until < 3:
            # Short-term: 3% distance in 1 day is very significant
            if abs_dist > 10:
                signals['distance'] = 0.9
            elif abs_dist > 5:
                signals['distance'] = 0.8
            elif abs_dist > 3:
                signals['distance'] = 0.7
            elif abs_dist > 1:
                signals['distance'] = 0.6
            else:
                signals['distance'] = 0.4
        else:
            if abs_dist > 30:
                signals['distance'] = 0.9
            elif abs_dist > 20:
                signals['distance'] = 0.7
            elif abs_dist > 15:
                signals['distance'] = 0.5
            elif abs_dist > 10:
                signals['distance'] = 0.3
            else:
                signals['distance'] = 0.1

        # 2. Momentum alignment: is price moving in our bet direction?
        if direction == 'above':
            if momentum > 2:
                signals['momentum'] = 0.9
            elif momentum > 1:
                signals['momentum'] = 0.7
            elif momentum > 0:
                signals['momentum'] = 0.5
            elif momentum > -1:
                signals['momentum'] = 0.3
            else:
                signals['momentum'] = 0.1
        else:  # below
            if momentum < -2:
                signals['momentum'] = 0.9
            elif momentum < -1:
                signals['momentum'] = 0.7
            elif momentum < 0:
                signals['momentum'] = 0.5
            elif momentum < 1:
                signals['momentum'] = 0.3
            else:
                signals['momentum'] = 0.1

        # 3. Trend consistency: does 24h align with 7d?
        if change_7d != 0:
            daily_avg_7d = change_7d / 7
            trend_same_dir = (change_24h > 0 and daily_avg_7d > 0) or \
                             (change_24h < 0 and daily_avg_7d < 0)
            if trend_same_dir and abs(change_24h) > abs(daily_avg_7d):
                signals['trend'] = 0.8  # Accelerating trend
            elif trend_same_dir:
                signals['trend'] = 0.6  # Consistent trend
            else:
                signals['trend'] = 0.3  # Conflicting signals
        else:
            signals['trend'] = 0.5  # No 7d data

        # 4. Time decay: less time = more predictable for current-state bets
        if days_until < 1:
            signals['time_decay'] = 0.95  # Almost expired - very predictable
        elif days_until < 3:
            signals['time_decay'] = 0.8
        elif days_until < 7:
            signals['time_decay'] = 0.6
        elif days_until < 14:
            signals['time_decay'] = 0.4
        else:
            signals['time_decay'] = 0.3

        # 5. Liquidity: thin markets = more mispricing (structural inefficiency)
        if liquidity < 5000:
            signals['liquidity'] = 0.9  # Very thin, likely mispriced
        elif liquidity < 20000:
            signals['liquidity'] = 0.7
        elif liquidity < 100000:
            signals['liquidity'] = 0.5
        else:
            signals['liquidity'] = 0.3  # Deep liquidity, harder to find edge

        # 6. Probability compression: is the cheap side suspiciously cheap?
        # Retail compresses true 20% events to 8-12%, true 5% to 1-2%
        if buy_price < 0.05:
            signals['compression'] = 0.8  # Very cheap, likely compressed
        elif buy_price < 0.12:
            signals['compression'] = 0.7
        elif buy_price < 0.20:
            signals['compression'] = 0.5
        elif buy_price < 0.35:
            signals['compression'] = 0.3
        else:
            signals['compression'] = 0.2  # Expensive side, less compression

        return signals

    def _count_aligned_signals(self, signals: Dict, threshold: float = 0.6) -> int:
        """Count how many signals are above threshold (aligned)."""
        return sum(1 for v in signals.values() if v >= threshold)

    # ------------------------------------------------------------------
    # Mispricing Checklist
    # ------------------------------------------------------------------

    def _mispricing_checklist(self, market, distance_pct, momentum,
                               buy_price, edge, days_until) -> Tuple[int, List[str]]:
        """
        Run the mispricing checklist. Returns (score 0-8, reasons list).
        Each checked box = 1 point. 4+ boxes = likely edge.

        Checklist:
        1. Is the market thin (low liquidity)?
        2. Is the event complex (multi-factor)?
        3. Is there a big information gap (our data vs market)?
        4. Are there correlated/sibling markets (cross-market potential)?
        5. Is there a catalyst window (time-decay squeeze)?
        6. Is the price compressed (cheap side too cheap)?
        7. Is momentum confirming our direction?
        8. Is the edge meaningful (>8%)?
        """
        checks = []
        score = 0

        # 1. Thin market
        liq = market.get('liquidity', 0)
        if liq < 20000:
            score += 1
            checks.append("thin_liquidity")

        # 2. Complex event (many sibling sub-markets = complex pricing)
        if market.get('sibling_count', 0) > 5:
            score += 1
            checks.append("complex_event")

        # 3. Information gap (our estimate vs market price diverges significantly)
        if edge > 0.10:
            score += 1
            checks.append("info_gap")

        # 4. Correlated markets exist (sibling markets that may lag)
        if market.get('sibling_count', 0) > 3:
            score += 1
            checks.append("cross_market_potential")

        # 5. Time-decay window (close to expiry, prices may overshoot)
        if days_until < 3:
            score += 1
            checks.append("time_decay_window")

        # 6. Price compression (cheap side is suspiciously cheap)
        if buy_price < 0.15:
            score += 1
            checks.append("price_compressed")

        # 7. Momentum confirms
        if abs(momentum) > 1.0:
            score += 1
            checks.append("momentum_confirms")

        # 8. Edge is meaningful
        if edge > 0.08:
            score += 1
            checks.append("strong_edge")

        return score, checks

    # ------------------------------------------------------------------
    # Core evaluation engine
    # ------------------------------------------------------------------

    def evaluate_markets(self, markets: List[Dict]) -> List[Dict]:
        """Evaluate all crypto markets and return bet recommendations."""
        # Bulk-fetch all coin prices in one API call to avoid rate limits
        coin_ids = []
        for m in markets:
            combined = (m.get('title', '') + ' ' + m.get('question', '') +
                        ' ' + m.get('description', ''))
            cid = self._detect_coin_in_text(combined)
            if cid:
                if not cid.startswith("yf:"): coin_ids.append(cid)
        if coin_ids:
            self._bulk_fetch_prices(coin_ids)

        recommendations = []

        for market in markets:
            try:
                rec = self._evaluate_single_market(market)
                if rec:
                    recommendations.append(rec)
            except Exception as e:
                print(f"  [!] Eval error for {market.get('title', '?')[:50]}: {e}")

        # Same-day priority: boost markets resolving within 12h, sort by composite score
        for rec in recommendations:
            days = rec.get('days_until', 14)
            if days <= 0.5:  # Resolves within 12h
                rec['composite_score'] = rec.get('composite_score', 0) * 1.5
                rec['same_day'] = True
            elif days <= 1.0:  # Resolves within 24h
                rec['composite_score'] = rec.get('composite_score', 0) * 1.25
                rec['same_day'] = True
            else:
                rec['same_day'] = False

        # Commodity near-target boost: commodities close to price target get priority
        # (early resolution = fast turnaround even on monthly markets)
        COMMODITY_KEYWORDS = ['oil', 'crude', 'gold', 'silver', 'copper', 'platinum']
        for rec in recommendations:
            title_lower = rec.get('title', '').lower()
            is_commodity = any(kw in title_lower for kw in COMMODITY_KEYWORDS)
            if is_commodity:
                days = rec.get('days_until', 30)
                # Prefer shorter-dated commodity markets (March over June)
                if days <= 25:
                    rec['composite_score'] = rec.get('composite_score', 0) * 1.3
                elif days <= 60:
                    rec['composite_score'] = rec.get('composite_score', 0) * 1.15
                # Extra boost if close to target (SNAP potential = fast resolution)
                dist = abs(rec.get('distance_pct', 100))
                if dist < 3:
                    rec['composite_score'] = rec.get('composite_score', 0) * 1.4
                    rec['commodity_snap'] = True
                elif dist < 5:
                    rec['composite_score'] = rec.get('composite_score', 0) * 1.2
                    rec['commodity_snap'] = True

        # Sort by composite score (with priority boosts applied)
        recommendations.sort(key=lambda x: x.get('composite_score', 0), reverse=True)
        return recommendations

    def _evaluate_single_market(self, market: Dict) -> Optional[Dict]:
        """Evaluate a single crypto market using the deep strategy framework."""
        min_conf = CRYPTO_MIN_CONFIDENCE
        min_edge = CRYPTO_MIN_EDGE
        bet_min = CRYPTO_BET_MIN
        bet_max = CRYPTO_BET_MAX

        title = market.get('title', '') or market.get('event_title', '')
        question = market.get('question', '') or title
        combined_text = title + ' ' + question + ' ' + market.get('description', '')

        # Block dip markets entirely (0W/4L, -$19 historically)
        if 'dip' in title.lower() or 'dip' in question.lower():
            return None

        coin_id = self._detect_coin_in_text(combined_text)
        if not coin_id:
            print(f"  [CRYPTO TRADER DBG] No asset detected: {title[:50]}"); return None

        price_data = self._get_yahoo_price(coin_id[3:]) if coin_id.startswith("yf:") else self._get_price_change(coin_id)
        if not price_data or not price_data.get('price'):
            return None

        current_price = price_data['price']
        change_24h = price_data.get('change_24h', 0) or 0
        change_7d = price_data.get('change_7d', 0) or 0

        title_text = title + ' ' + question
        target_price = self._parse_price_target(combined_text)
        direction = self._determine_direction(title_text)

        yes_price = market['yes_price']
        no_price = market['no_price']
        days_until = market.get('days_until', 14)

        if target_price and direction:
            return self._evaluate_price_target(
                market, coin_id, current_price, target_price, direction,
                change_24h, change_7d, yes_price, no_price, days_until,
                min_conf=min_conf, min_edge=min_edge,
                bet_min=bet_min, bet_max=bet_max,
            )

        return None

    def _evaluate_price_target(self, market, coin_id, current_price,
                                target_price, direction, change_24h,
                                change_7d, yes_price, no_price,
                                days_until, min_conf=65, min_edge=0.05,
                                bet_min=10.0, bet_max=15.0,
) -> Optional[Dict]:
        """
        Full deep-strategy evaluation of a price-target market.

        Flow:
        1. Calculate distance + momentum
        2. Estimate true probability (conservative model)
        3. Find mispriced side + calculate edge
        4. Run mispricing checklist (need 3+ checks)
        5. Build signal stack (need 3+ aligned signals)
        6. Apply strategy gates
        7. Calculate composite confidence score
        8. Size the bet based on confidence + asymmetry
        """
        title = market.get('title', '') or market.get('event_title', '')

        if target_price <= 0:
            return None

        # === STEP 1: Distance + Momentum ===
        distance_pct = ((target_price - current_price) / current_price) * 100
        momentum = change_24h  # 7d data unavailable, use full 24h change
        # Rapid cycle: incorporate 15-min momentum signal
        if False:  # 15-min momentum disabled (was rapid cycle only)
            rapid_mom = self.get_15min_momentum(coin_id)
            if abs(rapid_mom) > 0.1:
                momentum = momentum * 0.5 + rapid_mom * 0.5  # Blend with short-term signal
                print(f"    [CRYPTO TRADER] 15min momentum: {rapid_mom:+.2f}% -> blended: {momentum:+.2f}%")
        time_factor = min(1.0, days_until / 14)
        abs_dist = abs(distance_pct)

        # Prefer longer-dated markets for "already above/below" bets
        # Same-day bets on markets already past target = bad risk/reward
        # NO on "BTC >$66k" when BTC is at $68k and market closes TODAY = almost certain loss
        if distance_pct <= 0 and direction == 'above' and days_until < 1:
            # Already above target, same day — very likely to stay above, skip NO
            if abs_dist > 2:
                print(f"    [CRYPTO TRADER] same-day, already {abs_dist:.0f}% above target")
                return None
        if distance_pct >= 0 and direction == 'below' and days_until < 1:
            if abs_dist > 2:
                print(f"    [CRYPTO TRADER] same-day, already {abs_dist:.0f}% below target")
                return None

        # === STEP 2: Estimate true probability ===
        our_yes_estimate = self._estimate_probability(
            direction, distance_pct, momentum, time_factor
        )
        if our_yes_estimate is None:
            return None

        our_no_estimate = 1.0 - our_yes_estimate

        # === STEP 3: Find mispriced side ===
        yes_edge = our_yes_estimate - yes_price
        no_edge = our_no_estimate - no_price

        yes_payout = (1.0 / yes_price) if yes_price > 0 else 0
        no_payout = (1.0 / no_price) if no_price > 0 else 0

        yes_ev = yes_edge * yes_payout if yes_edge > 0 else 0
        no_ev = no_edge * no_payout if no_edge > 0 else 0

        if yes_ev > no_ev and yes_edge >= min_edge:
            side = 'yes'
            edge = yes_edge
            our_estimate = our_yes_estimate
            buy_price = yes_price
            payout_mult = yes_payout
            ev_score = yes_ev
        elif no_edge >= max(min_edge, 0.25):  # NO bets need 25%+ edge (44% WR historically)
            side = 'no'
            edge = no_edge
            our_estimate = our_no_estimate
            buy_price = no_price
            payout_mult = no_payout
            ev_score = no_ev
        else:
            return None

        print(f"  [CRYPTO TRADER DBG] {title[:50]} | {side} edge={edge:+.0%} dist={distance_pct:+.1f}%")

        # === STEP 4: Mispricing checklist (need 3+) ===
        checklist_score, checks = self._mispricing_checklist(
            market, distance_pct, momentum, buy_price, edge, days_until
        )
        min_checks = 3 if days_until <= 1 else 4
        if checklist_score < min_checks:
            print(f"    [CRYPTO TRADER] checklist {checklist_score}/{min_checks}")
            return None  # Not enough mispricing signals

        # === STEP 5: Signal stack ===
        volume = market.get('volume', 0)
        liquidity = market.get('liquidity', 0)
        signals = self._build_signal_stack(
            distance_pct, momentum, change_24h, change_7d,
            days_until, volume, liquidity, buy_price, direction
        )
        aligned_signals = self._count_aligned_signals(signals, threshold=0.65)
        signal_avg = sum(signals.values()) / len(signals) if signals else 0

        # Need at least 3 aligned signals
        if aligned_signals < 3:
            print(f"    [CRYPTO TRADER] signals {aligned_signals}/3 (avg={signal_avg:.2f})")
            return None

        # === STEP 6: Strategy gates ===
        betting_on_move = (
            (side == 'yes' and direction == 'above' and distance_pct > 0) or
            (side == 'yes' and direction == 'below' and distance_pct < 0)
        )
        betting_against_move = not betting_on_move

        # Classify bet type — understanding WHAT we're betting and WHY
        #
        # EARLY-RESOLUTION MARKETS (key insight):
        #   Markets like "Will Oil hit $60 by March 31?" don't wait until Mar 31.
        #   The MOMENT the price touches $60, it resolves YES instantly.
        #   So a "23 day" market could resolve in hours on a big move.
        #   The market prices it as a long bet, but we know it can pop anytime.
        #
        # Categories:
        #   HOLD    = Price already past target. Just needs to not crash back.
        #            E.g., BTC at $67K, "above $60K?" = YES is near-certain, hold the line.
        #   FADE    = Betting price WON'T reach a far target. High win rate.
        #            E.g., "Oil hit $130?" when oil is $90 = NO, that's a 44% pump in 23 days.
        #   SNAP    = Price is close to target. One good move = instant resolution.
        #            E.g., Oil at $90, "hit $100?" = just needs a 10% move, could be days.
        #   LOTTO   = Price is far but cheap YES side has huge payout (10x+).
        #            E.g., "ETH hit $3K?" at 0.3% = if it happens, massive return.
        #   DECAY   = Short-dated market, price behavior is more predictable.
        #            E.g., "BTC above $66K tomorrow?" when BTC is $67K = time is our friend.
        #   MOMENTUM = Trend-following, price moving toward target with confirmation.

        already_past = (
            (direction == 'above' and distance_pct <= 0) or
            (direction == 'below' and distance_pct >= 0)
        )

        # Format-aware probability adjustment (C2)
        format_type = getattr(self, '_last_format_type', 'unknown')
        if format_type == 'touch' and not already_past:
            our_yes_estimate = min(0.95, our_yes_estimate * 1.15)
            print(f"    [CRYPTO TRADER] Touch market boost: est={our_yes_estimate:.0%}")

        if already_past and abs_dist > 3:
            bet_type = "HOLD"
        elif betting_against_move and abs_dist > 20:
            bet_type = "FADE"
        elif betting_against_move:
            bet_type = "FADE"
        elif not already_past and abs_dist < 12 and days_until > 2:
            bet_type = "SNAP"  # Close to target, could resolve fast
        elif buy_price < 0.10 and payout_mult >= 10:
            bet_type = "LOTTO"
        elif days_until < 3:
            bet_type = "DECAY"
        elif buy_price < 0.15 and our_estimate > buy_price * 1.5:
            bet_type = "COMPRESSION"
        else:
            bet_type = "MOMENTUM"

        if betting_on_move:
            # Never bet YES on >40% moves
            if abs_dist > 40:
                return None

            # Crash/dump awareness: if momentum opposes our bet, be much pickier
            # "BTC needs +7% pump but is dropping 3% today" = bad bet
            momentum_opposes = (
                (direction == 'above' and momentum < -1.0) or
                (direction == 'below' and momentum > 1.0)
            )
            momentum_strongly_opposes = (
                (direction == 'above' and momentum < -3.0) or
                (direction == 'below' and momentum > 3.0)
            )

            # Strong opposing momentum (>2.5%) + needs move = skip entirely
            # "BTC dropping 3% and needs a 5% pump" = don't touch it
            if momentum_strongly_opposes and abs_dist > 3:
                print(f"    [CRYPTO TRADER] strong opposing momentum ({momentum:+.1f}%) vs {abs_dist:.0f}% move needed")
                return None

            # Opposing momentum (>1%) + needs move = need extreme mispricing
            # Must be cheap (<10c) AND model says probability is 5x+ what market prices
            if momentum_opposes and abs_dist > 2:
                if buy_price >= 0.10 or our_estimate < buy_price * 5.0:
                    print(f"    [CRYPTO TRADER] opposing momentum ({momentum:+.1f}%), need <10c + 5x mispricing")
                    return None

            # Same-day bets needing big moves are very risky
            if days_until < 1 and abs_dist > 4:
                if our_estimate < buy_price * 3.0:
                    print(f"    [CRYPTO TRADER] same-day {abs_dist:.0f}% move needs 3x mispricing")
                    return None

            # Next-day bets needing big moves in negative momentum = cautious
            if days_until <= 1 and abs_dist > 5 and momentum < 0:
                print(f"    [CRYPTO TRADER] next-day {abs_dist:.0f}% move with negative momentum ({momentum:+.1f}%)")
                return None

            # For 25-40% moves, need extreme mispricing (2x)
            if abs_dist > 25 and our_estimate < buy_price * 2.0:
                return None

            # For 15-25% moves, need momentum OR big mispricing
            if abs_dist > 15:
                has_momentum = (
                    (direction == 'above' and momentum > 2.5) or
                    (direction == 'below' and momentum < -2.5)
                )
                has_big_mispricing = our_estimate > buy_price * 1.8
                if not has_momentum and not has_big_mispricing:
                    return None

        # Expensive sides need bigger edge
        if buy_price > 0.50 and edge < 0.15:
            return None

        # === STEP 7: Composite confidence score ===
        confidence = self._calculate_confidence(
            edge, momentum, distance_pct, direction, days_until,
            buy_price, betting_against_move, payout_mult,
            checklist_score, signal_avg, aligned_signals, bet_type
        )

        # === ANALYST OVERRIDE ===
        confidence = self._apply_analyst_override(confidence, coin_id, side, direction, title)

        # === ASSET MODIFIER (data-driven) ===
        asset_mod = self._get_asset_modifier(coin_id if not coin_id.startswith('yf:') else coin_id[3:])
        confidence = max(0, min(100, int(confidence * asset_mod)))

        if confidence < min_conf:
            print(f"    [CRYPTO TRADER] confidence {confidence} < {min_conf}")
            return None

        # === STEP 8: Bet sizing ===
        # Spread small: $10 moonshots, up to $15 on high-confidence fades
        conf_range = max(1, 100 - min_conf)
        base_bet = bet_min + (bet_max - bet_min) * (
            (confidence - min_conf) / conf_range
        )
        # Longshot sizing: $5-$7 for high-payout plays
        if payout_mult > 10:
            base_bet = 5.0 + min(2.0, (payout_mult - 10) * 0.1)  # $5-$7 for 10x+
        elif payout_mult > 5:
            base_bet = min(base_bet, 7.0)  # Cap at $7 for 5-10x
        bet_amount = max(bet_min, min(bet_max, base_bet))

        # Composite score for ranking (EV weighted by checklist + signals)
        composite_score = ev_score * (1 + checklist_score * 0.15) * (1 + signal_avg * 0.3)

        reasoning = (
            f"{coin_id.upper()} ${current_price:,.0f}->${target_price:,.0f} ({direction}). "
            f"Dist:{distance_pct:+.0f}% 24h:{change_24h:+.1f}% 7d:{change_7d:+.1f}%. "
            f"{side.upper()} @{buy_price:.0%} est:{our_estimate:.0%} edge:{edge:+.0%} "
            f"{payout_mult:.1f}x {days_until:.0f}d. "
            f"Checklist:{checklist_score}/8[{','.join(checks)}] "
            f"Signals:{aligned_signals}/6"
        )

        print(f"  [{bet_type}] {title[:55]}")
        print(f"    {coin_id}: ${current_price:,.0f} -> ${target_price:,.0f} ({direction}) | {days_until:.0f}d")
        print(f"    24h:{change_24h:+.1f}% 7d:{change_7d:+.1f}% Mom:{momentum:+.2f}")
        print(f"    {side.upper()} @{buy_price:.0%} | Est:{our_estimate:.0%} | Edge:{edge:+.0%} | {payout_mult:.1f}x")
        print(f"    Checklist:{checklist_score}/8 [{','.join(checks)}]")
        print(f"    Signals:{aligned_signals}/6 avg:{signal_avg:.2f} | Conf:{confidence} | ${bet_amount:.0f}")

        return {
            'market_id': market['id'],
            'market_title': title,
            'event_title': market.get('event_title', ''),
            'category': 'crypto',
            'bet_side': side,
            'bet_odds': buy_price,
            'bet_amount': bet_amount,
            'our_estimate': our_estimate,
            'edge': edge,
            'score': confidence,
            'confidence': confidence,
            'ev_score': ev_score,
            'composite_score': composite_score,
            'payout_mult': payout_mult,
            'bet_type': bet_type,
                'format_type': getattr(self, '_last_format_type', 'unknown'),
            'checklist_score': checklist_score,
            'checklist_items': checks,
            'signal_count': aligned_signals,
            'signal_avg': signal_avg,
            'reasoning': reasoning,
            'coin_id': coin_id,
            'current_price': current_price,
            'target_price': target_price,
            'direction': direction,
            'momentum': momentum,
            'change_24h': change_24h,
            'change_7d': change_7d,
            'days_until': days_until,
            'distance_pct': distance_pct,
        }

    # ------------------------------------------------------------------
    # Probability estimation (conservative model)
    # ------------------------------------------------------------------

    def _estimate_probability(self, direction, distance_pct, momentum,
                               time_factor) -> Optional[float]:
        """
        Estimate true probability that price reaches target.

        Conservative model: large moves are exponentially less likely.
        Momentum adjusts modestly. Time remaining matters.
        """
        abs_dist = abs(distance_pct)

        if direction == 'above':
            if distance_pct <= 0:
                # Already above target — this is the KEY insight:
                # BTC at $68k with target $66k = already 3% above.
                # For BTC to lose, it needs to CRASH below target.
                # Crypto daily vol ~3-4%, but most days close near open.
                # A 3%+ crash in 1-2 days happens <15% of the time.
                if abs_dist > 40:
                    base = 0.99
                elif abs_dist > 30:
                    base = 0.98
                elif abs_dist > 20:
                    base = 0.97
                elif abs_dist > 15:
                    base = 0.96
                elif abs_dist > 10:
                    base = 0.94
                elif abs_dist > 7:
                    base = 0.92
                elif abs_dist > 5:
                    base = 0.90
                elif abs_dist > 3:
                    base = 0.85
                elif abs_dist > 2:
                    base = 0.80
                elif abs_dist > 1:
                    base = 0.70
                else:
                    base = 0.60

                # Small adjustments — don't let momentum/time swing too much
                # A 3% buffer shouldn't become 34% NO just because of momentum
                base += momentum * 0.002  # was 0.003
                base -= (1 - time_factor) * 0.03  # was 0.06
                return max(0.20, min(0.99, base))

            else:
                # Need pump
                if abs_dist > 50:
                    base = 0.02
                elif abs_dist > 40:
                    base = 0.04
                elif abs_dist > 30:
                    base = 0.07
                elif abs_dist > 25:
                    base = 0.10
                elif abs_dist > 20:
                    base = 0.14
                elif abs_dist > 15:
                    base = 0.20
                elif abs_dist > 10:
                    base = 0.30
                elif abs_dist > 5:
                    base = 0.42
                elif abs_dist > 2:
                    base = 0.46
                else:
                    base = 0.48

                if momentum > 3:
                    base *= 1.4
                elif momentum > 2:
                    base *= 1.25
                elif momentum > 1:
                    base *= 1.10
                elif momentum < -2:
                    base *= 0.6

                base *= (0.7 + time_factor * 0.3)
                return max(0.01, min(0.55, base))

        elif direction == 'below':
            if distance_pct >= 0:
                # Already below target — mirror of above logic
                if abs_dist > 40:
                    base = 0.99
                elif abs_dist > 30:
                    base = 0.98
                elif abs_dist > 20:
                    base = 0.97
                elif abs_dist > 15:
                    base = 0.96
                elif abs_dist > 10:
                    base = 0.94
                elif abs_dist > 7:
                    base = 0.92
                elif abs_dist > 5:
                    base = 0.90
                elif abs_dist > 3:
                    base = 0.85
                elif abs_dist > 2:
                    base = 0.80
                elif abs_dist > 1:
                    base = 0.70
                else:
                    base = 0.60

                base -= momentum * 0.002
                base -= (1 - time_factor) * 0.03
                return max(0.20, min(0.99, base))

            else:
                # Need dump
                drop_needed = abs_dist
                if drop_needed > 50:
                    base = 0.01
                elif drop_needed > 40:
                    base = 0.03
                elif drop_needed > 30:
                    base = 0.05
                elif drop_needed > 25:
                    base = 0.08
                elif drop_needed > 20:
                    base = 0.12
                elif drop_needed > 15:
                    base = 0.18
                elif drop_needed > 10:
                    base = 0.25
                elif drop_needed > 5:
                    base = 0.38
                elif drop_needed > 2:
                    base = 0.44
                else:
                    base = 0.48

                if momentum < -3:
                    base *= 1.4
                elif momentum < -2:
                    base *= 1.25
                elif momentum < -1:
                    base *= 1.10
                elif momentum > 2:
                    base *= 0.6

                base *= (0.7 + time_factor * 0.3)
                return max(0.01, min(0.45, base))

        return None

    # ------------------------------------------------------------------
    # Confidence scoring (multi-factor)
    # ------------------------------------------------------------------

    def _calculate_confidence(self, edge, momentum, distance_pct, direction,
                               days_until, buy_price, betting_against_move,
                               payout_mult, checklist_score, signal_avg,
                               aligned_signals, bet_type="MOMENTUM") -> int:
        """
        Multi-factor confidence scoring by bet type.

        Each bet type has a different scoring track because the risk profile
        and what makes a "good" bet differs for each:

        HOLD (starts 50)  - Price already past target. High base, penalize if close to edge.
        FADE (starts 45)  - Betting against extreme move. Distance = safety.
        SNAP (starts 40)  - Close to target, early resolution play. Momentum is king.
        LOTTO (starts 30) - Cheap longshot. Low base, needs strong signals to qualify.
        DECAY (starts 48) - Short-dated, price predictable. Time pressure = confidence.
        COMPRESSION (starts 42) - Cheap + mispriced. Asymmetry bonus.
        MOMENTUM (starts 38) - Trend-following. Needs strong confirmation.
        """
        abs_dist = abs(distance_pct)

        if bet_type == "HOLD":
            # Price already past target - we just need it to STAY
            # High base because the hard part (reaching target) is done
            score = 50

            # How far past? More buffer = safer
            if abs_dist > 15:
                score += 12  # Way past, very safe
            elif abs_dist > 10:
                score += 8
            elif abs_dist > 5:
                score += 4
            elif abs_dist < 2:
                score -= 5  # Dangerously close to flipping back

            # Edge bonus (0-12)
            score += min(12, edge * 60)

            # Counter-momentum penalty (price drifting back toward target)
            if direction == 'above' and momentum < -1:
                score -= min(10, abs(momentum) * 4)
            elif direction == 'below' and momentum > 1:
                score -= min(10, momentum * 4)

            # Short time helps HOLDs - less chance of crash
            if days_until < 2:
                score += 5
            elif days_until < 5:
                score += 3

        elif bet_type == "FADE":
            # Betting price WON'T reach a far target
            # Distance is our best friend - further = safer
            score = 45

            # Edge (0-15)
            score += min(15, edge * 80)

            # Distance safety - the core of a FADE
            if abs_dist > 30:
                score += 12
            elif abs_dist > 20:
                score += 8
            elif abs_dist > 15:
                score += 4

            # Counter-momentum helps fades (price moving AWAY from target)
            if direction == 'above' and momentum < 0:
                score += min(5, abs(momentum) * 2)
            elif direction == 'below' and momentum > 0:
                score += min(5, momentum * 2)

            # Short time = less chance target gets hit
            if days_until < 3:
                score += 5
            elif days_until < 7:
                score += 3

        elif bet_type == "SNAP":
            # Close to target - early resolution play
            # The market prices this as a long bet but it could pop anytime
            # Momentum toward target is critical
            score = 40

            # Edge (0-12)
            score += min(12, edge * 60)

            # Momentum toward target is THE key factor for SNAPs
            if direction == 'above' and momentum > 0:
                score += min(15, momentum * 5)  # Strong uptrend = snap incoming
            elif direction == 'below' and momentum < 0:
                score += min(15, abs(momentum) * 5)
            elif abs(momentum) > 1:
                score -= 6  # Moving away from target = bad for SNAP

            # Closeness bonus - closer = higher chance of snap resolution
            if abs_dist < 3:
                score += 8  # Almost there
            elif abs_dist < 6:
                score += 5
            elif abs_dist < 10:
                score += 2

            # More time = more chances for the snap to happen
            if days_until > 14:
                score += 4
            elif days_until > 7:
                score += 2

        elif bet_type == "LOTTO":
            # Cheap longshot - massive payout if it hits
            # Low base because these are inherently risky
            # Needs strong signals to overcome the low base
            score = 30

            # Edge matters a lot here - is the market REALLY mispriced?
            score += min(15, edge * 100)

            # Payout multiplier - bigger = more justified risk
            if payout_mult >= 20:
                score += 10
            elif payout_mult >= 15:
                score += 7
            elif payout_mult >= 10:
                score += 4

            # Momentum toward target
            if direction == 'above' and momentum > 2:
                score += min(8, momentum * 3)
            elif direction == 'below' and momentum < -2:
                score += min(8, abs(momentum) * 3)

            # More time = more lottery tickets
            if days_until > 14:
                score += 5
            elif days_until > 7:
                score += 3

        elif bet_type == "DECAY":
            # Short-dated, price behavior more predictable
            # Higher base because less time = less uncertainty
            score = 48

            # Edge (0-12)
            score += min(12, edge * 60)

            # Very short time is ideal for decay plays
            if days_until < 1:
                score += 6
            elif days_until < 2:
                score += 3

            # Current position relative to target
            if abs_dist > 10:
                score += 5  # Far from target with little time = safe NO
            elif abs_dist < 3:
                score += 3  # Very close with little time = safe YES

            # Momentum alignment
            if direction == 'above' and momentum > 0:
                score += min(6, momentum * 2)
            elif direction == 'below' and momentum < 0:
                score += min(6, abs(momentum) * 2)

        elif bet_type == "COMPRESSION":
            # Cheap + mispriced - asymmetric risk/reward
            score = 42

            # Edge (0-15)
            score += min(15, edge * 80)

            # Asymmetry bonus - cheaper = better R/R
            if buy_price < 0.06:
                score += 8
            elif buy_price < 0.10:
                score += 6
            elif buy_price < 0.15:
                score += 4

            # Momentum (0-8)
            if direction == 'above' and momentum > 0:
                score += min(8, momentum * 3)
            elif direction == 'below' and momentum < 0:
                score += min(8, abs(momentum) * 3)

        else:
            # MOMENTUM - trend-following, needs strong confirmation
            score = 38

            # Edge (0-15)
            score += min(15, edge * 80)

            # Momentum alignment is critical
            if direction == 'above' and momentum > 0:
                score += min(12, momentum * 4)
            elif direction == 'below' and momentum < 0:
                score += min(12, abs(momentum) * 4)
            elif abs(momentum) > 1:
                score -= 8  # Counter-momentum = bad

            # Asymmetry
            if buy_price < 0.25:
                score += 5
            elif buy_price < 0.35:
                score += 2

        # === UNIVERSAL BONUSES (apply to all bet types) ===

        # Checklist bonus (0-8 points)
        score += min(8, checklist_score * 2)

        # Signal stack bonus (0-6 points)
        if aligned_signals >= 5:
            score += 6
        elif aligned_signals >= 4:
            score += 4
        elif aligned_signals >= 3:
            score += 2

        # === DYNAMIC MODIFIERS (data-driven, from DB) ===
        hour_mod = self._get_hour_modifier().get(current_hour_et(), 1.0)
        score = int(score * hour_mod)

        return max(0, min(100, score))


# ------------------------------------------------------------------
# Test
# ------------------------------------------------------------------

def test_crypto_scanner():
    """Test the crypto market scanner."""
    scanner = CryptoMarketScanner()

    print("=" * 60)
    print("CRYPTO MARKET SCANNER v2.0 - DEEP STRATEGY")
    print("=" * 60)

    markets = scanner.scan_crypto_markets()
    print(f"\nFound {len(markets)} crypto sub-markets")

    for m in markets[:5]:
        print(f"\n  {m['title'][:70]}")
        print(f"    YES:{m['yes_price']:.0%} NO:{m['no_price']:.0%} | "
              f"Vol:${m['volume']:,.0f} Liq:${m['liquidity']:,.0f} | "
              f"{m.get('days_until', '?')}d")

    print(f"\n{'='*60}")
    print("EVALUATING MARKETS (Deep Strategy)")
    print("=" * 60)

    recs = scanner.evaluate_markets(markets)
    print(f"\n{len(recs)} recommendations:")

    for r in recs:
        print(f"\n  [{r['bet_type']}] {r['market_title'][:55]}")
        print(f"    {r['bet_side'].upper()} @{r['bet_odds']:.0%} | "
              f"Edge:{r['edge']:+.0%} | {r['payout_mult']:.1f}x | "
              f"Conf:{r['confidence']} | CL:{r['checklist_score']}/8 | "
              f"Sig:{r['signal_count']}/6")


if __name__ == "__main__":
    test_crypto_scanner()
