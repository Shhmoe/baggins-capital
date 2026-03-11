"""
Baggins' Buddy — Sports Analyst v3
====================================
Confidence-based sports betting. Scans Polymarket, scores opportunities
using multiple signals, bets when confidence is high enough.

Signals:
- Market data (price, volume, liquidity, urgency)
- Bookmaker odds comparison (if ODDS_API_KEY available)
- Fighter/team stats (UFC Stats API)
- AI reasoning gate (Anthropic)
- Prop bet models (KO rate vs market price)

Personality: The buddy who watches games on the couch.
"dude I think Oliveira is way underpriced tonight"
"""

import os
import re
import json
import time
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from hedge_fund_config import (
    ENABLE_SPORTS_MODULE, SPORTS_SCAN_INTERVAL, SPORTS_BET_SIZE,
    SPORTS_MAX_DAILY, SPORTS_MIN_EDGE, SPORTS_MAX_CONCURRENT
)

# Try to import confidence threshold
try:
    from hedge_fund_config import SPORTS_MIN_CONFIDENCE
except ImportError:
    SPORTS_MIN_CONFIDENCE = 55

GAMMA_API_URL = "https://gamma-api.polymarket.com"
ODDS_API_URL = "https://api.the-odds-api.com/v4/sports"
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hedge_fund_performance.db')

# Map event slugs to Odds API keys
SPORT_MAPPINGS = {
    '2026-nba-champion': {'odds_key': 'basketball_nba_championship_winner', 'sport': 'nba'},
    '2026-nhl-stanley-cup-champion': {'odds_key': 'icehockey_nhl_championship_winner', 'sport': 'nhl'},
    'english-premier-league-winner': {'odds_key': 'soccer_epl_winner', 'sport': 'soccer'},
}

SPORTS_EXCLUDE = [
    'election', 'president', 'senate', 'governor', 'congress',
    'party', 'primary', 'democrat', 'republican', 'oscar',
    'crypto', 'bitcoin', 'ethereum', 'temperature', 'weather',
    'nato', 'ukraine', 'troops', 'fed', 'inflation', 'trump attend',
    'larry wheels', 'mentions', 'announcers say',
]

class SportsAnalyst:
    """Baggins' Buddy — confidence-based sports betting."""

    SPORTS_TAG_SLUGS = ['ufc', 'mma', 'boxing', 'nba', 'nhl', 'sports']

    # Market types we understand and can score
    MARKET_TYPES = {
        'winner': ['vs.', 'vs ', 'who will win'],
        'ko_tko': ['ko or tko', 'knockout'],
        'distance': ['go the distance', 'distance?'],
        'rounds': ['o/u', 'over/under', 'rounds'],
        'submission': ['submission'],
        'champion': ['champion', 'win the 2026', 'win the 202'],
        'next_fight': ['fight next'],
    }

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.odds_api_key = os.getenv("ODDS_API_KEY", "").strip()
        self.ai_client = None
        self.daily_bets = 0
        self.last_scan = 0
        self.last_reset_date = None
        self._fighter_cache = {}
        self._init_ai_client()
        self._ensure_tables()

    def _init_ai_client(self):
        """Initialize AI client for reasoning gate."""
        try:
            api_key = os.getenv("API_KEY", "").strip()
            bankr_key = os.getenv("BANKR_API_KEY", "").strip()
            if bankr_key:
                import anthropic
                self.ai_client = anthropic.Anthropic(
                    api_key=bankr_key,
                    base_url="https://llm.bankr.bot"
                )
            elif api_key:
                import anthropic
                self.ai_client = anthropic.Anthropic(api_key=api_key)
        except Exception:
            pass

    def _ensure_tables(self):
        """Create sports tracking tables."""
        try:
            from db_writer import DBWriter
            _arch2 = DBWriter(self.db_path)
            _arch2.execute("""
                CREATE TABLE IF NOT EXISTS sports_markets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT,
                    event_slug TEXT,
                    event_title TEXT,
                    market_title TEXT,
                    sport TEXT,
                    market_type TEXT,
                    polymarket_yes_price REAL,
                    polymarket_no_price REAL,
                    bookmaker_implied_prob REAL,
                    confidence INTEGER,
                    edge REAL,
                    side TEXT,
                    reasoning TEXT,
                    scanned_at TEXT,
                    bet_placed INTEGER DEFAULT 0
                )
            """)
            _arch2.execute("""
                CREATE TABLE IF NOT EXISTS sports_odds_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_slug TEXT,
                    outcome TEXT,
                    bookmaker TEXT,
                    odds REAL,
                    implied_prob REAL,
                    fetched_at TEXT
                )
            """, commit=True)
        except Exception as e:
            print(f"[BUDDY] Error creating tables: {e}")

    # ==================================================================
    # Main cycle
    # ==================================================================

    def run_sports_cycle(self, bankr=None, wallet=None, dry_run=False, scout_queue=None, risk_manager=None, compliance=None, intel_package=None):
        """Run one sports analysis cycle."""
        if not ENABLE_SPORTS_MODULE:
            return

        now = time.time()
        if now - self.last_scan < SPORTS_SCAN_INTERVAL:
            return

        today = datetime.now(timezone.utc).date()
        if self.last_reset_date != today:
            self.daily_bets = 0
            self.last_reset_date = today

        # V3.1: Log liaison intelligence
        if intel_package and intel_package.get('available'):
            wr_data = intel_package.get('rolling_win_rate')
            if wr_data:
                print(f"  [SPORTS LIAISON] WR: {wr_data}")

        # Concurrent position check
        try:
            _open = self._reader.fetchone("SELECT COUNT(*) FROM bets WHERE category='sports' AND status != 'resolved'")
            _open_count = _open[0] if _open else 0
            if _open_count >= SPORTS_MAX_CONCURRENT:
                print(f"  [SPORTS] Max concurrent positions ({_open_count}/{SPORTS_MAX_CONCURRENT})")
                return
        except Exception:
            pass

        if self.daily_bets >= SPORTS_MAX_DAILY:
            return

        self.last_scan = now
        print(f"\n[BUDDY] === Sports Scan ({datetime.now(timezone.utc).strftime('%H:%M')}) ===")

        # Step 1: Scan Polymarket
        sports_events = self._scan_polymarket_events(pre_fetched_events=scout_queue)
        if not sports_events:
            print("[BUDDY] No sports events found")
            return

        total_markets = sum(len(e.get('markets', [])) for e in sports_events)
        print(f"[BUDDY] {len(sports_events)} events, {total_markets} markets")

        # Step 2: Bookmaker odds (optional)
        book_odds = {}
        if self.odds_api_key:
            book_odds = self._fetch_futures_odds(sports_events)
            if book_odds:
                print(f"[BUDDY] Bookmaker odds for {len(book_odds)} events")

        # Step 3: Score all opportunities
        scored = self._score_opportunities(sports_events, book_odds)

        if not scored:
            print("[BUDDY] Nothing scored high enough to report")
            return

        # Show top scored
        print(f"[BUDDY] Top opportunities:")
        for s in scored[:5]:
            print(f"  [{s['confidence']}] {s['market_title'][:50]}")
            print(f"       {s['side'].upper()} @ {s['poly_price']:.0%} | {s['market_type']} | {s['sport']}")
            if s.get('reasons'):
                print(f"       {', '.join(s['reasons'][:3])}")

        # Step 4: Bet on highest confidence
        best = scored[0]
        if best['confidence'] >= SPORTS_MIN_CONFIDENCE:
            if not self._already_bet(best['market_id']):
                print(f"[BUDDY] dude, {best['market_title'][:50]} is the play")
                if bankr and not dry_run:
                    self._place_bet(best, bankr, wallet, risk_manager=risk_manager, compliance=compliance)
                else:
                    print(f"[BUDDY] [DRY RUN] Would bet ${SPORTS_BET_SIZE}")
                    self._log_opportunity(best, bet_placed=False)
            else:
                print(f"[BUDDY] Already in on {best['market_title'][:40]}")
        else:
            print(f"[BUDDY] Best confidence {best['confidence']} < {SPORTS_MIN_CONFIDENCE} min")

    # ==================================================================
    # Confidence scoring engine
    # ==================================================================

    def _score_opportunities(self, events, book_odds) -> List[Dict]:
        """Score all markets using multiple signals. Returns sorted by confidence."""
        scored = []

        for event in events:
            slug = event['slug']
            event_odds = book_odds.get(slug, {})
            event_title = event['title']
            sport = event['sport']

            for market in event.get('markets', []):
                question = market['question']
                yes_price = market['yes_price']
                no_price = market['no_price']
                volume = market.get('volume', 0)
                q_lower = question.lower()

                # Classify market type
                mtype = self._classify_market(q_lower)

                # Skip types we can't score well
                if mtype in ('next_fight', 'other'):
                    continue

                # --- Build confidence from signals ---
                confidence = 0
                reasons = []

                # Signal 1: Event urgency (fights tonight >> futures in December)
                urgency = self._score_urgency(slug, event_title)
                confidence += urgency
                if urgency >= 10:
                    reasons.append(f"happening soon (+{urgency})")

                # Signal 2: Volume (high volume = real market, more reliable pricing)
                if volume > 100000:
                    confidence += 10
                    reasons.append("high volume (+10)")
                elif volume > 10000:
                    confidence += 5
                    reasons.append("decent volume (+5)")
                elif volume < 100:
                    confidence -= 10
                    reasons.append("no volume (-10)")

                # Signal 3: Price sweet spot (not too extreme)
                # Best value in 15-45% range (underdog-ish) or 60-85% (solid favorite)
                price_score, side = self._score_price(yes_price, no_price, mtype)
                confidence += price_score
                if price_score > 0:
                    reasons.append(f"price sweet spot (+{price_score})")

                # Signal 4: Bookmaker edge (if available)
                book_prob = self._match_bookmaker(q_lower, event_odds)
                edge = 0
                if book_prob is not None:
                    if side == 'yes':
                        edge = book_prob - yes_price
                    else:
                        edge = (1.0 - book_prob) - no_price
                    if edge > 0.15:
                        confidence += 25
                        reasons.append(f"books say {book_prob:.0%}, big edge (+25)")
                    elif edge > 0.05:
                        confidence += 15
                        reasons.append(f"books say {book_prob:.0%} (+15)")
                    elif edge < -0.10:
                        confidence -= 20
                        reasons.append(f"books disagree (-20)")

                # Signal 5: Fighter stats (UFC/MMA winner markets)
                if sport in ('mma', 'boxing') and mtype == 'winner':
                    stats_score, stats_reason = self._score_fighter_stats(question, side)
                    confidence += stats_score
                    if stats_reason:
                        reasons.append(stats_reason)

                # Signal 6: Prop bet logic (KO/distance markets)
                if mtype in ('ko_tko', 'distance', 'submission'):
                    prop_score, prop_reason = self._score_prop(question, mtype, yes_price)
                    confidence += prop_score
                    if prop_reason:
                        reasons.append(prop_reason)

                # Signal 7: Market type bonus
                if mtype == 'winner':
                    confidence += 5  # We understand winner markets best

                # Only keep if above minimum threshold
                if confidence >= 30:
                    scored.append({
                        'market_id': market['market_id'],
                        'event_slug': slug,
                        'event_title': event_title,
                        'market_title': question,
                        'sport': sport,
                        'market_type': mtype,
                        'side': side,
                        'poly_price': yes_price if side == 'yes' else no_price,
                        'book_prob': book_prob,
                        'edge': edge,
                        'confidence': min(confidence, 100),
                        'volume': volume,
                        'reasons': reasons,
                    })

        scored.sort(key=lambda x: x['confidence'], reverse=True)
        return scored

    def _classify_market(self, q_lower) -> str:
        """Classify market type from question text."""
        for mtype, keywords in self.MARKET_TYPES.items():
            if any(kw in q_lower for kw in keywords):
                return mtype
        return 'other'

    def _score_urgency(self, slug, title) -> int:
        """Score based on how soon the event resolves."""
        # Extract date from slug (e.g. ufc-cha1-max1-2026-03-07)
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', slug)
        if date_match:
            try:
                event_date = datetime.strptime(date_match.group(1), '%Y-%m-%d').date()
                today = datetime.now(timezone.utc).date()
                days_away = (event_date - today).days
                if days_away <= 0:  # Today!
                    return 25
                elif days_away <= 1:
                    return 18
                elif days_away <= 7:
                    return 12
                elif days_away <= 30:
                    return 5
                else:
                    return 0
            except ValueError:
                pass

        # Check title for date hints
        title_lower = title.lower()
        if 'end of 2026' in title_lower or 'december 31' in title_lower:
            return 0  # Long-dated
        if '2026' in title_lower and 'ufc' in title_lower:
            return 5  # Could be soon

        return 2  # Unknown

    def _score_price(self, yes_price, no_price, mtype) -> tuple:
        """Score based on price position. Returns (score, suggested_side)."""
        # For winner markets: look for underdog value (YES cheap) or overpriced favorite (NO cheap)
        if mtype == 'winner':
            if 0.20 <= yes_price <= 0.45:
                return (10, 'yes')  # Underdog in sweet spot
            elif 0.55 <= yes_price <= 0.80:
                return (8, 'yes')  # Moderate favorite
            elif yes_price < 0.10:
                return (0, 'no')  # Too cheap, probably for a reason
            elif yes_price > 0.90:
                return (0, 'no')  # Heavy favorite, no value
            else:
                return (5, 'yes' if yes_price < 0.5 else 'no')

        # For props (KO, distance): YES at cheap prices can be value
        if mtype in ('ko_tko', 'submission'):
            if 0.15 <= yes_price <= 0.50:
                return (8, 'yes')
            elif yes_price > 0.60:
                return (5, 'no')
            return (3, 'yes')

        if mtype == 'distance':
            return (5, 'yes' if yes_price < 0.5 else 'no')

        if mtype == 'rounds':
            return (3, 'yes' if yes_price < 0.5 else 'no')

        # Championship/futures: lean toward favorites
        if mtype == 'champion':
            if 0.15 <= yes_price <= 0.40:
                return (8, 'yes')
            return (3, 'yes' if yes_price > 0.10 else 'no')

        return (0, 'yes')

    def _match_bookmaker(self, q_lower, event_odds) -> Optional[float]:
        """Try to match market question to bookmaker outcome."""
        if not event_odds:
            return None
        for outcome_name, prob in event_odds.items():
            outcome_words = set(outcome_name.lower().split()) - {'the', 'fc', 'sc', 'city'}
            matches = sum(1 for w in outcome_words if w in q_lower and len(w) > 2)
            if matches >= 2 or (matches >= 1 and len(outcome_words) <= 3):
                return prob
        return None

    def _score_fighter_stats(self, question, side) -> tuple:
        """Score based on fighter stats for MMA/boxing markets."""
        # Extract fighter names from "UFC 326: Fighter A vs. Fighter B"
        vs_match = re.search(r':\s*(.+?)\s+vs\.?\s+(.+?)(?:\s*\(|$)', question)
        if not vs_match:
            return (0, None)

        fighter_a = vs_match.group(1).strip()
        fighter_b = vs_match.group(2).strip()

        # Fetch stats from UFC stats (free, no key needed)
        stats_a = self._get_fighter_stats(fighter_a)
        stats_b = self._get_fighter_stats(fighter_b)

        if not stats_a and not stats_b:
            return (0, None)

        score = 0
        reason_parts = []

        # Win record comparison
        if stats_a and stats_b:
            wr_a = stats_a.get('win_rate', 0)
            wr_b = stats_b.get('win_rate', 0)
            if wr_a > wr_b + 0.15:
                score += 10
                reason_parts.append(f"{fighter_a} {wr_a:.0%}WR vs {wr_b:.0%}")
            elif wr_b > wr_a + 0.15:
                score += 10
                reason_parts.append(f"{fighter_b} {wr_b:.0%}WR vs {wr_a:.0%}")

            # Win streak
            streak_a = stats_a.get('win_streak', 0)
            streak_b = stats_b.get('win_streak', 0)
            if streak_a >= 3 and streak_b < 2:
                score += 5
                reason_parts.append(f"{fighter_a} {streak_a}W streak")
            elif streak_b >= 3 and streak_a < 2:
                score += 5
                reason_parts.append(f"{fighter_b} {streak_b}W streak")

        reason = f"stats: {', '.join(reason_parts)} (+{score})" if reason_parts else None
        return (score, reason)

    def _get_fighter_stats(self, name) -> Optional[Dict]:
        """Fetch fighter stats from ufcstats.com. Uses cache."""
        if name in self._fighter_cache:
            return self._fighter_cache[name]

        try:
            # Search ufcstats.com — returns HTML table with fighter records
            # Use last name for better matching
            parts = name.strip().split()
            query = parts[-1] if parts else name  # Last name
            resp = requests.get(
                'http://www.ufcstats.com/statistics/fighters/search',
                params={'query': query},
                timeout=8
            )
            if resp.status_code != 200:
                self._fighter_cache[name] = None
                return None

            html = resp.text
            # Find the fighter detail page URL that matches our name
            # Table rows contain: First, Last, Nickname, Height, Weight, Reach, Stance, W, L, D
            name_lower = name.lower()
            first_name = parts[0].lower() if parts else ''
            last_name = parts[-1].lower() if parts else ''

            # Extract fighter detail URLs and look for matching rows
            import re as _re
            # Find all fighter-details links
            detail_links = _re.findall(
                r'href="(http://www\.ufcstats\.com/fighter-details/[a-f0-9]+)"',
                html
            )
            # Find rows with first+last name matching
            # Pattern: first name link ... last name link in same row area
            best_url = None
            for url in set(detail_links):
                # Find the context around this URL
                idx = html.find(url)
                if idx < 0:
                    continue
                # Get surrounding ~500 chars
                context = html[max(0, idx-50):idx+500].lower()
                if first_name in context and last_name in context:
                    best_url = url
                    break

            if not best_url:
                self._fighter_cache[name] = None
                return None

            # Fetch the detail page for the record
            detail_resp = requests.get(best_url, timeout=8)
            if detail_resp.status_code != 200:
                self._fighter_cache[name] = None
                return None

            detail_html = detail_resp.text
            # Extract record: "Record: 36-11-0 (1 NC)" or "Record: 26-7-0"
            record_match = _re.search(r'Record:\s*(\d+)-(\d+)-(\d+)', detail_html)
            if not record_match:
                self._fighter_cache[name] = None
                return None

            wins = int(record_match.group(1))
            losses = int(record_match.group(2))
            total = wins + losses

            # Count recent wins/losses from fight flags (win/loss text in spans)
            recent_flags = _re.findall(r'b-flag__text">(win|loss)', detail_html[:5000])
            win_streak = 0
            for flag in recent_flags:
                if flag == 'win':
                    win_streak += 1
                else:
                    break

            # Count KO wins from method details (look for "KO/TKO" in win rows)
            ko_wins = len(_re.findall(r'KO/TKO', detail_html))
            sub_wins = len(_re.findall(r'SUB', detail_html))
            # Rough estimate: divide by 2 since method appears in both fighters' rows
            ko_wins = ko_wins // 2
            sub_wins = sub_wins // 2

            stats = {
                'name': name,
                'wins': wins,
                'losses': losses,
                'win_rate': wins / total if total > 0 else 0,
                'ko_wins': ko_wins,
                'sub_wins': sub_wins,
                'win_streak': win_streak,
            }
            self._fighter_cache[name] = stats
            return stats

        except Exception:
            pass

        self._fighter_cache[name] = None
        return None

    def _score_prop(self, question, mtype, yes_price) -> tuple:
        """Score prop bets (KO/TKO, distance, submission)."""
        q_lower = question.lower()
        score = 0
        reason = None

        if mtype == 'ko_tko':
            # "Will the fight be won by KO or TKO?" or "Will Fighter X win by KO?"
            # If specific fighter mentioned, check their KO rate
            fighter_match = re.search(r'will (.+?) win by ko', q_lower)
            if fighter_match:
                fname = fighter_match.group(1).strip()
                stats = self._get_fighter_stats(fname)
                if stats and stats.get('wins', 0) > 0:
                    ko_rate = stats['ko_wins'] / stats['wins']
                    if ko_rate > 0.60 and yes_price < 0.40:
                        score += 15
                        reason = f"{fname} {ko_rate:.0%} KO rate, market at {yes_price:.0%} (+15)"
                    elif ko_rate > 0.40 and yes_price < 0.25:
                        score += 10
                        reason = f"{fname} {ko_rate:.0%} KO rate, cheap at {yes_price:.0%} (+10)"
            else:
                # General "will fight be KO/TKO" — look for cheap YES
                if yes_price < 0.30:
                    score += 5
                    reason = f"KO/TKO market cheap at {yes_price:.0%} (+5)"

        elif mtype == 'distance':
            # "Fight to Go the Distance?" — often mispriced
            if yes_price < 0.30:
                score += 5
                reason = f"distance NO is heavy favorite, might be value in YES (+5)"
            elif yes_price > 0.70:
                score += 5
                reason = f"distance YES heavy favorite (+5)"

        elif mtype == 'submission':
            fighter_match = re.search(r'will (.+?) win by submission', q_lower)
            if fighter_match:
                fname = fighter_match.group(1).strip()
                stats = self._get_fighter_stats(fname)
                if stats and stats.get('wins', 0) > 0:
                    sub_rate = stats['sub_wins'] / stats['wins']
                    if sub_rate > 0.40 and yes_price < 0.25:
                        score += 12
                        reason = f"{fname} {sub_rate:.0%} sub rate, market at {yes_price:.0%} (+12)"

        return (score, reason)

    # ==================================================================
    # AI Reasoning Gate
    # ==================================================================

    def _ai_confidence_check(self, opportunity) -> Optional[int]:
        """Ask AI to evaluate the opportunity. Returns adjusted confidence or None."""
        if not self.ai_client:
            return None

        try:
            prompt = (
                f"You are a sports betting analyst. Score this opportunity 0-100 for confidence.\n\n"
                f"Market: {opportunity['market_title']}\n"
                f"Event: {opportunity['event_title']}\n"
                f"Sport: {opportunity['sport']}\n"
                f"Side: {opportunity['side'].upper()} @ {opportunity['poly_price']:.0%}\n"
                f"Market type: {opportunity['market_type']}\n"
                f"Signals: {', '.join(opportunity.get('reasons', []))}\n"
                f"Volume: ${opportunity.get('volume', 0):,.0f}\n"
                f"\nRespond with just a number 0-100 and one sentence why."
            )

            response = self.ai_client.messages.create(
                model="deepseek-v3.2",
                max_tokens=100,
                messages=[{"role": "user", "content": prompt}]
            )
            text = response.content[0].text.strip()
            # Extract number
            num_match = re.search(r'\b(\d{1,3})\b', text)
            if num_match:
                return int(num_match.group(1))
        except Exception:
            pass
        return None

    # ==================================================================
    # Polymarket scanning (unchanged from v2)
    # ==================================================================

    def _scan_polymarket_events(self, pre_fetched_events=None) -> List[Dict]:
        """Scan Polymarket events endpoint using tag_slug.

        If pre_fetched_events is provided (from Market Scout), skips Gamma API calls.
        """
        sports_events = []
        seen_slugs = set()

        try:
            if pre_fetched_events is not None:
                # Use Scout's pre-fetched events — no Gamma API calls needed
                all_events = pre_fetched_events
                print(f"[BUDDY] Using {len(all_events)} pre-fetched events from Scout")
            else:
                # Fallback: fetch from Gamma API per tag
                all_events = []
                for tag in self.SPORTS_TAG_SLUGS:
                    resp = requests.get(
                        f"{GAMMA_API_URL}/events",
                        params={'tag_slug': tag, 'active': True, 'closed': False, 'limit': 50},
                        timeout=15
                    )
                    if resp.status_code != 200:
                        continue

                    events = resp.json()
                    all_events.extend(events)
                    time.sleep(0.5)

            for event in all_events:
                    slug = event.get('slug', '')
                    if slug in seen_slugs:
                        continue
                    seen_slugs.add(slug)

                    title = (event.get('title') or '')
                    title_lower = title.lower()
                    if any(kw in title_lower for kw in SPORTS_EXCLUDE):
                        continue

                    markets = event.get('markets') or []
                    parsed_markets = []
                    for m in markets:
                        question = m.get('question') or m.get('groupItemTitle') or ''
                        if not question:
                            continue

                        yes_price = None
                        no_price = None
                        outcome_prices = m.get('outcomePrices')
                        if outcome_prices:
                            try:
                                prices = json.loads(outcome_prices) if isinstance(outcome_prices, str) else outcome_prices
                                if len(prices) >= 2:
                                    yes_price = float(prices[0])
                                    no_price = float(prices[1])
                            except (json.JSONDecodeError, ValueError, IndexError):
                                pass

                        if yes_price is None:
                            for t in (m.get('tokens') or []):
                                outcome = (t.get('outcome') or '').lower()
                                price = float(t.get('price') or 0)
                                if outcome == 'yes':
                                    yes_price = price
                                elif outcome == 'no':
                                    no_price = price

                        if yes_price is not None and yes_price > 0:
                            parsed_markets.append({
                                'market_id': m.get('conditionId') or m.get('id', ''),
                                'question': question,
                                'yes_price': yes_price,
                                'no_price': no_price or (1.0 - yes_price),
                                'volume': float(m.get('volume') or 0),
                                'liquidity': float(m.get('liquidity') or 0),
                            })

                    if parsed_markets:
                        mapping = SPORT_MAPPINGS.get(slug, {})
                        sport = mapping.get('sport') or self._detect_sport(title_lower)
                        sports_events.append({
                            'title': title,
                            'slug': slug,
                            'sport': sport,
                            'odds_key': mapping.get('odds_key'),
                            'markets': parsed_markets,
                        })

        except Exception as e:
            print(f"[BUDDY] Events scan error: {e}")

        return sports_events

    # ==================================================================
    # Bookmaker odds (unchanged)
    # ==================================================================

    def _fetch_futures_odds(self, sports_events) -> Dict[str, Dict[str, float]]:
        """Fetch bookmaker futures odds."""
        book_odds = {}
        fetched_keys = set()

        for event in sports_events:
            odds_key = event.get('odds_key')
            if not odds_key or odds_key in fetched_keys:
                continue
            fetched_keys.add(odds_key)

            try:
                resp = requests.get(
                    f"{ODDS_API_URL}/{odds_key}/odds",
                    params={'apiKey': self.odds_api_key, 'regions': 'us',
                            'markets': 'outrights', 'oddsFormat': 'decimal'},
                    timeout=15
                )
                if resp.status_code == 200:
                    data = resp.json()
                    outcome_probs = {}
                    snapshots = []
                    for api_event in data:
                        for bm in (api_event.get('bookmakers') or []):
                            for market in (bm.get('markets') or []):
                                for outcome in (market.get('outcomes') or []):
                                    name = outcome.get('name', '')
                                    dec = float(outcome.get('price') or 1)
                                    if dec > 1:
                                        imp = 1.0 / dec
                                        outcome_probs.setdefault(name, []).append(imp)
                                        snapshots.append({'slug': event['slug'], 'outcome': name,
                                                          'bookmaker': bm.get('key', ''), 'odds': dec,
                                                          'implied_prob': imp})
                    if outcome_probs:
                        book_odds[event['slug']] = {
                            n.lower(): sum(p) / len(p) for n, p in outcome_probs.items()
                        }
                    self._store_odds_snapshots(snapshots)
                elif resp.status_code in (401, 429):
                    break
                time.sleep(1)
            except Exception as e:
                print(f"[BUDDY] Odds API error: {e}")

        return book_odds

    # ==================================================================
    # Betting + DB
    # ==================================================================

    def _place_bet(self, opp, bankr, wallet, risk_manager=None, compliance=None):
        """Place a sports bet via Bankr."""
        side = opp['side'].upper()
        title = opp['market_title']
        amount = SPORTS_BET_SIZE

        if wallet:
            available = wallet.available
            if available < amount + 5:
                print(f"[BUDDY] Not enough balance (${available:.2f})")
                return

        # V3.1: Risk Manager assessment
        if risk_manager:
            bet_dict = {
                'category': 'sports',
                'side': side.lower(),
                'amount': amount,
                'market_id': opp.get('market_id', opp.get('condition_id', '')),
                'market_title': title,
            }
            risk_ok, risk_level, risk_warnings = risk_manager.assess(bet_dict)
            if risk_warnings:
                for w in risk_warnings:
                    print(f"  [RISK MANAGER] {w}")
            if not risk_ok:
                print(f"  [RISK MANAGER] BLOCKED: {risk_warnings}")
                return

        # V3.1: Compliance pre-flight
        if compliance:
            comp_dict = {
                'category': 'sports',
                'market_id': opp.get('market_id', opp.get('condition_id', '')),
                'market_title': title,
                'amount': amount,
            }
            approved, reason, comp_warnings = compliance.pre_flight(comp_dict)
            if comp_warnings:
                for w in comp_warnings:
                    print(f"  [COMPLIANCE] {w}")
            if not approved:
                print(f"  [COMPLIANCE] REJECTED: {reason}")
                return

        result = bankr.place_bet(title, side, amount)

        if result.get('success'):
            print(f"[BUDDY] Bet placed: ${amount} on {side} — {title[:50]}")
            # Verify bet execution
            try:
                verify_result = bankr.verify_bet_execution(title, side)
                if verify_result.get("verified"):
                    print(f"  [VERIFIED] Sports bet confirmed in Bankr positions")
                else:
                    print(f"  [WARN] Sports bet unverified: {verify_result.get('reason', 'unknown')} -- logging anyway")
            except Exception as ve:
                print(f"  [WARN] Sports verification failed: {ve} -- logging anyway")
            self.daily_bets += 1
            self._log_opportunity(opp, bet_placed=True)

            try:
                from data_intake import DataIntake
                from db_writer import DBWriter
                _intake = DataIntake(self.db_path)
                archivist = DBWriter(self.db_path)

                decision_snapshot = {
                    "raw_data": {
                        "sport": opp.get("sport"),
                        "event_title": opp.get("event_title"),
                        "poly_yes_price": opp.get("poly_price"),
                        "bookmaker_odds": opp.get("bookmaker_odds"),
                        "bookmaker_implied_prob": opp.get("bookmaker_prob"),
                    },
                    "modifiers": {
                        "market_type": opp.get("market_type"),
                        "event_slug": opp.get("event_slug"),
                    },
                    "decision": {
                        "confidence": opp.get("confidence"),
                        "edge": opp.get("edge", 0),
                        "side": side.lower(),
                        "reasons": opp.get("reasons", []),
                    },
                    "strategy": {
                        "bet_type": "SPORTS",
                        "sport": opp.get("sport"),
                        "signals": opp.get("signals"),
                    },
                }

                bet_id = _intake.validate_and_write_bet(
                    market_id=opp['market_id'],
                    market_title=title,
                    category="sports",
                    side=side.lower(),
                    amount=amount,
                    odds=opp['poly_price'],
                    confidence_score=opp['confidence'],
                    edge=opp.get('edge', 0),
                    reasoning='; '.join(opp.get('reasons', [])),
                    balance_before=wallet.available if wallet else None,
                    cycle_type="sports",
                    bet_type="SPORTS",
                    format_type="sports",
                    decision_snapshot=decision_snapshot,
                )

                if bet_id and result.get('trade_id'):
                    archivist.set_trade_id(bet_id, result['trade_id'])
            except Exception as e:
                print(f"[BUDDY] DB error: {e}")
        else:
            print(f"[BUDDY] Bet failed: {result.get('error', '')[:100]}")

    def _already_bet(self, market_id):
        """Check if already have position via Archivist."""
        try:
            from db_reader import DBReader
            _reader = DBReader(self.db_path)
            return _reader.bet_exists(market_id)
        except:
            return False

    def _detect_sport(self, title_lower):
        """Detect sport from title."""
        if any(kw in title_lower for kw in ['nba', 'basketball']):
            return 'nba'
        if any(kw in title_lower for kw in ['nhl', 'hockey', 'stanley cup']):
            return 'nhl'
        if any(kw in title_lower for kw in ['nfl', 'super bowl', 'draft']):
            return 'nfl'
        if any(kw in title_lower for kw in ['ufc', 'mma', 'fight', 'bout']):
            return 'mma'
        if any(kw in title_lower for kw in ['boxing', 'belt', 'heavyweight', 'zuffa']):
            return 'boxing'
        if any(kw in title_lower for kw in ['premier league', 'champions league', 'la liga',
                                              'serie a', 'bundesliga', 'ligue 1', 'fifa',
                                              'world cup', 'europa', 'carabao', 'ballon',
                                              'goalscorer', 'soccer', 'football']):
            return 'soccer'
        if any(kw in title_lower for kw in ['masters', 'pga', 'golf']):
            return 'golf'
        return 'other'

    def _store_odds_snapshots(self, snapshots):
        if not snapshots:
            return
        try:
            from db_writer import DBWriter
            _arch = DBWriter(self.db_path)
            now = datetime.now(timezone.utc).isoformat()
            for s in snapshots:
                _arch.execute(
                    "INSERT INTO sports_odds_snapshots "
                    "(event_slug, outcome, bookmaker, odds, implied_prob, fetched_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (s['slug'], s['outcome'], s['bookmaker'], s['odds'], s['implied_prob'], now))
        except Exception as e:
            print(f"[BUDDY] Error storing odds: {e}")

    def _log_opportunity(self, opp, bet_placed=False):
        try:
            from db_writer import DBWriter
            _arch2 = DBWriter(self.db_path)
            _arch2.execute(
                "INSERT INTO sports_markets "
                "(market_id, event_slug, event_title, market_title, sport, market_type, "
                "polymarket_yes_price, bookmaker_implied_prob, confidence, edge, side, "
                "reasoning, scanned_at, bet_placed) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (opp['market_id'], opp.get('event_slug', ''), opp.get('event_title', ''),
                 opp['market_title'], opp.get('sport', ''), opp.get('market_type', ''),
                 opp['poly_price'], opp.get('book_prob'), opp['confidence'],
                 opp.get('edge', 0), opp['side'], '; '.join(opp.get('reasons', [])),
                 datetime.now(timezone.utc).isoformat(), 1 if bet_placed else 0),
                commit=True)
        except Exception as e:
            print(f"[BUDDY] Error logging: {e}")
