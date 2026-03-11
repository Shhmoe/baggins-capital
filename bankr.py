"""
Unified Bankr Client + Bet Resolver
====================================
Single module for all Bankr API operations:
- API layer: submit jobs, poll results
- Trading: place bets with precise language
- Resolution: check positions, redeem payouts, parse USDC amounts
- Wallet: balance checks, position queries
- DB: resolve bets, track pending, audit trail

All prompts use EXACT bet data (title, side, amount, date, odds) so Bankr's
LLM doesn't hallucinate or confuse positions.

v3.0 — merged bankr_executor.py + bet_resolver.py
"""

import os
import re
import json
import time
import sqlite3
import requests
from typing import Dict, List, Optional
from datetime import datetime, timedelta, timezone


class Bankr:
    """Unified Bankr API client — trading, resolution, and wallet in one place.

    Usage:
        bankr = Bankr(api_key='...', db_path='...', dry_run=False)

        # Place a bet
        bankr.place_bet('Will BTC be above $70k on March 7?', 'NO', 3.00)

        # Resolve pending bets (called every cycle)
        bankr.resolve_pending(wallet=wallet_obj, weather_agent=weather_obj)

        # Check wallet
        bankr.check_balance()
    """

    # --- Timing ---
    REDEEM_INTERVAL = 600           # 10 min between Bankr position checks
    WEATHER_CHECK_INTERVAL = 3600   # 1 hour between weather data checks
    UPDOWN_EXPIRY_HOURS = 2         # UpDown: loss if 2h past window
    CRYPTO_EXPIRY_HOURS = 24        # Crypto daily: loss if 24h past date
    MAX_BETS_PER_CHECK = 10         # Max bets per status check call

    # --- API ---
    POLL_INTERVAL = 5               # seconds between polls
    POLL_TIMEOUT = 420              # 7 min max wait for Bankr response

    def __init__(self, api_key: str = None, db_path: str = None,
                 dry_run: bool = True, wallet=None, weather_agent=None,
                 tracker=None, notifier=None):
        self.api_key = (api_key or os.getenv("BANKR_API_KEY", "")).strip()
        self.base_url = "https://api.bankr.bot"
        self.dry_run = dry_run
        self.db_path = db_path
        self.wallet = wallet
        self.weather_agent = weather_agent
        self.tracker = tracker
        self.notifier = notifier

        # Resolution timing
        self._last_redeem = None
        self._last_weather_check = None

        if not self.api_key:
            print("[!] WARNING: BANKR_API_KEY not set")

        if self.db_path:
            self._ensure_tables()

    # ==================================================================
    # API Layer — submit, poll, run
    # ==================================================================

    def _get_headers(self) -> Dict:
        return {
            'X-API-Key': self.api_key,
            'Content-Type': 'application/json'
        }

    def _submit_job(self, prompt: str) -> Optional[str]:
        """Submit a prompt to Bankr API, return job ID."""
        try:
            response = requests.post(
                f"{self.base_url}/agent/prompt",
                headers=self._get_headers(),
                json={'prompt': prompt},
                timeout=30
            )
            if response.status_code in [200, 202]:
                job_id = response.json().get('jobId')
                if job_id:
                    return job_id
                print(f"  [!] No jobId in response: {response.json()}")
            else:
                print(f"  [!] Bankr submit error: {response.status_code} - {response.text[:200]}")
        except Exception as e:
            print(f"  [!] Bankr submit failed: {e}")
        return None

    def _poll_job(self, job_id: str) -> Dict:
        """Poll job until completion or timeout."""
        url = f"{self.base_url}/agent/job/{job_id}"
        elapsed = 0

        while elapsed < self.POLL_TIMEOUT:
            try:
                response = requests.get(url, headers=self._get_headers(), timeout=20)
                if response.status_code == 200:
                    data = response.json()
                    status = data.get('status', '')
                    if status in ['completed', 'complete', 'done']:
                        return {
                            'success': True,
                            'response': data.get('response', ''),
                            'job_id': job_id,
                            'processing_time': data.get('processingTime', 0),
                            'raw': data
                        }
                    elif status in ['failed', 'error']:
                        return {
                            'success': False,
                            'error': data.get('response', 'Job failed'),
                            'job_id': job_id,
                            'raw': data
                        }
                else:
                    print(f"  [!] Poll error: {response.status_code}")
            except Exception as e:
                print(f"  [!] Poll error: {e}")

            time.sleep(self.POLL_INTERVAL)
            elapsed += self.POLL_INTERVAL

        return {'success': False, 'error': f'Job timed out after {self.POLL_TIMEOUT}s', 'job_id': job_id}

    def _run_prompt(self, prompt: str) -> Dict:
        """Submit prompt and wait for result. Core API method."""
        job_id = self._submit_job(prompt)
        if not job_id:
            return {'success': False, 'error': 'Failed to submit job'}
        return self._poll_job(job_id)

    # ==================================================================
    # Trading — place bets, verify execution
    # ==================================================================

    def place_bet(self, market_title: str, side: str, amount: float,
                  odds: float = None) -> Dict:
        """Place a bet on Polymarket.

        Uses precise language: exact title, side, amount.
        """
        command = f'bet ${amount:.2f} on {side.upper()} for "{market_title}" on Polymarket'

        if self.dry_run:
            print(f"\n[BANKER DRY RUN] Would execute: {command}")
            return self._simulate_trade(command, amount)

        if not self.api_key:
            return {'success': False, 'error': 'No API key'}

        print(f"\n[BANKER] Placing bet...")
        print(f"  Command: {command}")

        result = self._run_prompt(command)

        if result['success']:
            print(f"  [BANKER] Response: {result['response'][:200]}")
            return {
                'success': True,
                'trade_id': result['job_id'],
                'status': 'completed',
                'response': result['response'],
                'details': result.get('raw', {})
            }
        else:
            print(f"  [BANKER ERROR] {result.get('error', 'Unknown error')}")
            return {
                'success': False,
                'trade_id': result.get('job_id'),
                'error': result.get('error', 'Unknown error')
            }

    def verify_bet(self, market_title: str, side: str) -> Dict:
        """Verify a bet was placed by checking Bankr positions."""
        if self.dry_run:
            return {'verified': True, 'reason': 'dry run'}
        if not self.api_key:
            return {'verified': False, 'reason': 'no API key'}

        time.sleep(10)
        result = self._run_prompt(
            f"Do I have an active {side.upper()} position on "
            f"\"{market_title[:80]}\" on Polymarket? "
            f"Just answer yes or no."
        )

        if not result.get('success'):
            return {'verified': False, 'reason': f"query failed: {result.get('error', '')}"}

        response = result.get('response', '').lower()
        title_words = market_title.lower().split()[:5]
        has_position = any(w in response for w in title_words if len(w) > 3)
        has_yes = 'yes' in response[:50]

        if has_position or has_yes:
            return {'verified': True, 'reason': 'position found in Bankr'}
        return {'verified': False, 'reason': 'position not found in Bankr response'}

    # ==================================================================
    # Wallet — balance and position queries
    # ==================================================================

    def check_balance(self) -> Dict:
        """Check total USDC balance."""
        if not self.api_key:
            return {'success': False, 'error': 'No API key'}
        result = self._run_prompt("What is my total USDC balance across all chains?")
        if result['success']:
            return {'success': True, 'response': result['response'], 'raw': result.get('raw', {})}
        return {'success': False, 'error': result.get('error', 'Failed')}

    def get_wallet_address(self) -> Dict:
        if not self.api_key:
            return {'success': False, 'error': 'No API key'}
        result = self._run_prompt("What is my EVM deposit address?")
        if result['success']:
            return {'success': True, 'response': result['response'], 'raw': result.get('raw', {})}
        return {'success': False, 'error': result.get('error', 'Failed')}

    def get_positions(self) -> Dict:
        """Get current Polymarket positions."""
        if not self.api_key:
            return {'success': False, 'error': 'No API key'}
        result = self._run_prompt("What are my current Polymarket positions?")
        if result['success']:
            return {'success': True, 'response': result['response'], 'raw': result.get('raw', {})}
        return {'success': False, 'error': result.get('error', 'Failed')}

    def check_polygon_balance(self) -> Dict:
        if not self.api_key:
            return {'success': False, 'error': 'No API key'}
        result = self._run_prompt(
            "What is my USDC balance on the Polygon network only? "
            "Do NOT include balances from other chains."
        )
        if result['success']:
            return {'success': True, 'response': result['response'], 'raw': result.get('raw', {})}
        return {'success': False, 'error': result.get('error', 'Failed')}

    def check_base_balance(self) -> Dict:
        if not self.api_key:
            return {'success': False, 'error': 'No API key'}
        result = self._run_prompt(
            "What is my USDC and ETH balance on Base network only? "
            "Do NOT include balances from other chains."
        )
        if result['success']:
            return {'success': True, 'response': result['response'], 'raw': result.get('raw', {})}
        return {'success': False, 'error': result.get('error', 'Failed')}

    def get_avantis_markets(self) -> Dict:
        if not self.api_key:
            return {'success': False, 'error': 'No API key', 'markets': []}
        result = self._run_prompt(
            "What trading pairs/markets are available on Avantis on Base? "
            "List all available crypto/commodity/forex pairs."
        )
        if result.get('success'):
            markets = re.findall(r'\b([A-Z]{1,10})/([A-Z]{3})\b', result.get('response', '').upper())
            markets = sorted(set(f"{m[0]}/{m[1]}" for m in markets))
            return {'success': True, 'markets': markets, 'count': len(markets), 'response': result.get('response', '')}
        return {'success': False, 'error': result.get('error', 'Failed'), 'markets': []}

    # ==================================================================
    # Resolution — main entry point
    # ==================================================================

    def resolve_pending(self, wallet=None, weather_agent=None,
                        tracker=None, notifier=None):
        """Resolve pending bets. Called every cycle from main agent loop.

        Args set here override constructor args (for flexibility).
        """
        if wallet:
            self.wallet = wallet
        if weather_agent:
            self.weather_agent = weather_agent
        if tracker:
            self.tracker = tracker
        if notifier:
            self.notifier = notifier

        if not self.db_path:
            print("[SETTLEMENT CLERK] No db_path set — skipping resolution")
            return []

        resolved = []
        now = datetime.now(timezone.utc)

        # 1. Bankr position check + redeem (every 10 min)
        if (self.api_key and
            (not self._last_redeem or
             (now - self._last_redeem).total_seconds() >= self.REDEEM_INTERVAL)):
            # Step 1a: Bulk redeem all available shares (catches resolved-but-unclaimed)
            bulk_resp = None
            try:
                # Use shorter timeout for bulk redeem (2 min vs 7 min default)
                saved_timeout = self.POLL_TIMEOUT
                self.POLL_TIMEOUT = 120
                bulk = self._run_prompt(
                    'Redeem all available shares on Polymarket. '
                    'Claim everything that is redeemable. '
                    'Tell me what was redeemed and how much USDC was returned.'
                )
                self.POLL_TIMEOUT = saved_timeout
                if bulk.get('success'):
                    bulk_resp = bulk['response']
                    print(f"  [SETTLEMENT CLERK] Bulk redeem: {bulk_resp[:500]}")
                else:
                    print(f"  [SETTLEMENT CLERK] Bulk redeem: {bulk.get('error', '')[:100]}")
            except Exception as e:
                self.POLL_TIMEOUT = saved_timeout
                print(f"[SETTLEMENT CLERK] Bulk redeem error: {e}")

            # Log to portfolio_checks table so other employees can access
            try:
                from archivist import Archivist
                _arch = Archivist(self.db_path)
                _arch._execute(
                    "CREATE TABLE IF NOT EXISTS portfolio_checks ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "checked_at TEXT NOT NULL, "
                    "portfolio_raw TEXT, "
                    "redeem_response TEXT, "
                    "pending_count INTEGER, "
                    "resolved_count INTEGER DEFAULT 0)"
                )
                _arch._execute(
                    "INSERT INTO portfolio_checks (checked_at, redeem_response, pending_count) "
                    "VALUES (?, ?, ?)",
                    (now.isoformat(), bulk_resp[:5000] if bulk_resp else None, len(self.get_pending_bets())),
                    commit=True
                )
            except Exception as e:
                print(f"  [SETTLEMENT CLERK] DB log error: {e}")

            # Step 1b: Check individual positions for pending bets
            try:
                resolved += self._check_and_redeem()
            except Exception as e:
                print(f"[SETTLEMENT CLERK] Bankr error: {e}")
            self._last_redeem = now

        # 2. Expire stale bets (every cycle)
        try:
            resolved += self._expire_stale_bets()
        except Exception as e:
            print(f"[SETTLEMENT CLERK] Expiry error: {e}")

        # 3. Weather data check (hourly)
        if (self.weather_agent and
            (not self._last_weather_check or
             (now - self._last_weather_check).total_seconds() >= self.WEATHER_CHECK_INTERVAL)):
            try:
                resolved += self._weather_data_check()
            except Exception as e:
                print(f"[SETTLEMENT CLERK] Weather data check error: {e}")
            self._last_weather_check = now

        # Summary
        pending = self.get_pending_bets()
        if resolved:
            wins = sum(1 for r in resolved if r.get('won'))
            losses = len(resolved) - wins
            profit = sum(r.get('profit', 0) for r in resolved)
            print(f"[SETTLEMENT CLERK] Resolved {len(resolved)} bets "
                  f"({wins}W/{losses}L, ${profit:+.2f}) | "
                  f"{len(pending)} still pending")
        elif pending:
            print(f"[SETTLEMENT CLERK] {len(pending)} pending bets")

        return resolved

    # ==================================================================
    # Resolution — Step 1: Check position status
    # ==================================================================

    def _check_and_redeem(self):
        """Two-step: check which positions are redeemable, then redeem them."""
        pending = self.get_pending_bets()
        if not pending:
            return []

        print(f"\n[SETTLEMENT CLERK] Bankr check ({len(pending)} pending bets)...")

        # Only check bets whose market date has passed
        eligible = [b for b in pending if self._is_market_past_date(b)]
        if not eligible:
            return []

        bets_to_check = eligible[:self.MAX_BETS_PER_CHECK]
        prompt = self._build_status_prompt(bets_to_check)

        result = self._run_prompt(prompt)
        if not result.get('success'):
            print(f"  [!] Bankr check failed: {result.get('error', '')[:100]}")
            return []

        response_raw = result.get('response', '')
        response = response_raw.lower()
        print(f"  [BANKER] {response_raw[:800]}")

        redeemable = self._parse_redeemable(bets_to_check, response)
        if not redeemable:
            return []

        print(f"  [SETTLEMENT CLERK] {len(redeemable)} redeemable position(s)")

        resolved = []

        # Resolve expired 'Not found' bets as confirmed losses
        found_ids = set(b['id'] for b in redeemable)
        for bet in bets_to_check:
            if bet['id'] in found_ids:
                continue
            nearby = self._find_bet_in_response(bet, response)
            if nearby and 'not found' in nearby and self._is_market_past_date(bet):
                print(f"  [SETTLEMENT CLERK] #{bet['id']}: Not found in wallet. Resolving as loss.")
                self._resolve_bet(bet['id'], won=False, profit=-bet['amount'],
                                  source='bankr_confirmed_loss', redeemed_amount=0.0)
                resolved.append({'bet_id': bet['id'], 'won': False, 'profit': -bet['amount']})

        for bet in redeemable:
            r = self._redeem_position(bet)
            if r:
                resolved.append(r)
            time.sleep(3)

        if self.wallet and resolved:
            try:
                time.sleep(3)
                self.wallet.sync_with_wallet(update_starting=False)
            except Exception:
                pass

        return resolved

    def _build_status_prompt(self, bets):
        """Build precise prompt to check position status.

        Each bet listed with EXACT title, side, amount, and date so Bankr
        can match them accurately without confusing positions.
        """
        lines = []
        for i, bet in enumerate(bets, 1):
            side = (bet['side'] or '').upper()
            title = bet['market_title']
            amount = bet['amount']
            # Extract date from title for clarity
            date_match = re.search(r'(?:on\s+)?((?:March|February|January)\s+\d{1,2})', title, re.IGNORECASE)
            date_str = f" (market date: {date_match.group(1)})" if date_match else ""
            lines.append(f"{i}. \"{title}\" — I hold {side} shares, bet ${amount:.2f}{date_str}")

        bet_list = "\n".join(lines)

        return (
            f"Check my Polymarket wallet for these {len(bets)} positions.\n"
            f"For EACH position, tell me ONE of these statuses:\n"
            f"- 'Active' if the market is still open\n"
            f"- 'Resolved/Redeemable' if the market settled and I can redeem\n"
            f"- 'Not found' if I don't have this position\n\n"
            f"{bet_list}\n\n"
            f"List each number with its status. Do NOT include share counts or USDC values."
        )

    def _parse_redeemable(self, bets, response):
        """Parse status check response to find redeemable positions."""
        redeemable = []

        redeem_keywords = ['redeemable', 'resolved', 'redeem', 'claimable',
                           'ready to claim', 'can be redeemed', 'expired',
                           'won', 'lost', 'settled']
        skip_keywords = ['still active', 'still open', 'not found', 'active',
                         'not in your', 'no position', 'shares held',
                         'not yet resolved', 'unresolved']

        for bet in bets:
            nearby = self._find_bet_in_response(bet, response)
            if nearby is None:
                continue

            is_skip = any(kw in nearby for kw in skip_keywords)
            is_redeem = any(kw in nearby for kw in redeem_keywords)

            if is_skip and not is_redeem:
                continue
            if is_redeem:
                redeemable.append(bet)

        return redeemable

    # ==================================================================
    # Resolution — Step 2: Redeem a specific position
    # ==================================================================

    def _redeem_position(self, bet):
        """Redeem a resolved position. Uses precise language tied to bet data.

        Safeguards:
        1. Market date must have passed
        2. USDC amount must parse from redemption context
        3. Amount can't exceed max theoretical payout (sanity check)
        """
        title = bet['market_title']
        side = (bet['side'] or '').upper()
        amount = bet['amount']
        odds = bet.get('odds', 0.5)

        if not self._is_market_past_date(bet):
            print(f"  [SETTLEMENT CLERK] #{bet['id']}: Market not yet settled — {title[:50]}")
            return None

        # Calculate max possible payout for sanity check
        max_payout = amount / odds if odds > 0.001 else amount * 200

        print(f"  [SETTLEMENT CLERK] #{bet['id']}: {title[:60]}...")

        # Precise redemption prompt — includes bet details so Bankr matches correctly
        result = self._run_prompt(
            f"Redeem my {side} position on Polymarket: \"{title}\"\n"
            f"I bet ${amount:.2f} at {odds:.2f} odds on {side}.\n"
            f"Execute the redemption now. Reply with ONLY:\n"
            f"- The USDC amount returned (e.g. \"returned $2.50 USDC\")\n"
            f"- Or \"returned $0 USDC\" if the position lost\n"
            f"Do NOT mention wallet balance, POL, gas fees, or ask questions."
        )

        if not result.get('success'):
            print(f"  [!] Redeem failed: {result.get('error', '')[:100]}")
            return None

        resp = result.get('response', '')
        resp_lower = resp.lower()
        print(f"  [BANKER] {resp[:300]}")

        # Check for "no position"
        no_pos_keywords = ['not found', 'no position', "doesn't appear",
                           'not in your', 'no shares', 'position not found']
        if any(kw in resp_lower for kw in no_pos_keywords):
            print(f"  [SKIP] Position not found in wallet")
            return None

        # Parse USDC amount with strict context matching
        redeemed = self._parse_usdc_amount(resp, resp_lower, amount)

        if redeemed is None:
            print(f"  [SETTLEMENT CLERK] Could not parse USDC amount from response. "
                  f"Leaving bet #{bet['id']} pending.")
            return None

        # Sanity check: redeemed can't exceed max theoretical payout
        if redeemed > max_payout * 1.5:
            print(f"  [SETTLEMENT CLERK] Redeemed ${redeemed:.2f} exceeds max payout "
                  f"${max_payout:.2f} (odds={odds:.2f}). Likely hallucinated. "
                  f"Leaving bet #{bet['id']} pending.")
            return None

        # Determine win/loss from USDC amount
        if redeemed < 0.01:
            won = False
            profit = -amount
            redeemed = 0.0
        else:
            won = redeemed > amount
            profit = redeemed - amount

        self._resolve_bet(bet['id'], won, profit,
                          source='bankr_redeem', redeemed_amount=redeemed)

        self._log_audit('bankr_redeem',
                        [{'bet_id': bet['id'], 'won': won, 'profit': profit}], resp)

        print(f"  [SETTLEMENT CLERK] Bet #{bet['id']}: Bankr returned ${redeemed:.2f} USDC "
              f"(bet ${amount:.2f}, profit ${profit:+.2f})")

        return {'bet_id': bet['id'], 'won': won, 'profit': profit}

    # ==================================================================
    # USDC Parsing — strict, context-aware
    # ==================================================================

    def _parse_usdc_amount(self, resp, resp_lower, bet_amount):
        """Parse USDC amount from Bankr response.

        STRICT: Only accepts amounts in redemption context.
        Rejects share counts, position values, strike prices.
        Returns None if no clear amount found.
        """
        # Priority 1: Amount with redemption verb (returned/received/redeemed X USDC)
        redemption_patterns = [
            r'(?:return(?:ed)?|receive[d]?|redeem(?:ed)?|claim(?:ed)?|payout|paid\s*(?:out|back)?)\s*(?:of\s*)?(?:approximately\s*)?\$?(\d+\.?\d*)\s*USDC',
            r'(\d+\.?\d*)\s*USDC\s*(?:return(?:ed)?|redeem(?:ed)?|paid|back|to\s+(?:your\s+)?wallet)',
            r'(?:return(?:ed)?|receive[d]?|redeem(?:ed)?|payout)[^\d]{0,30}\$(\d+\.?\d+)',
        ]
        for pattern in redemption_patterns:
            m = re.search(pattern, resp, re.IGNORECASE)
            if m:
                val = float(m.group(1))
                if val < bet_amount * 200:
                    return val

        # Priority 2: Explicit loss ($0 with loss context)
        loss_patterns = [
            r'(?:lost|loss|expired|worthless|no\s+(?:payout|return)).*?\$?0(?:\.0+)?\s*(?:USDC)?',
            r'\$0(?:\.0+)?\s*(?:USDC)?\s*(?:return|paid|back)',
            r'(?:worth|resolved\s+to|currently\s+worth)\s*\*{0,2}\$?0(?:\.0+)?\s*\*{0,2}\s*(?:USDC)?',
            r'position.*?\$0(?:\.0+)?',
            r'0\s*USDC.*?(?:worth|value)',
        ]
        for pattern in loss_patterns:
            if re.search(pattern, resp_lower):
                return 0.0

        # Priority 3: Bare "X USDC" only if NOT near confusing words
        m = re.search(r'(\d+\.?\d*)\s*USDC', resp, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            start = max(0, m.start() - 40)
            end = min(len(resp), m.end() + 40)
            context = resp[start:end].lower()
            reject_words = ['shares', 'position', 'worth', 'value', 'price',
                           'market', 'contract', 'stake', 'bet of', 'wagered',
                           'wallet', 'balance', 'you have', 'hold', 'polygon',
                           'gas', 'swap', 'cover', 'fees', 'currently has']
            if not any(rw in context for rw in reject_words):
                if val < bet_amount * 200:
                    return val

        return None

    # ==================================================================
    # Response Matching — find bet sections in Bankr responses
    # ==================================================================

    def _extract_bet_signature(self, market_title):
        """Extract identifying keywords from market title."""
        title = market_title.lower()
        sig = []

        # City names (weather)
        for city in ['chicago', 'miami', 'new york', 'seattle', 'los angeles',
                     'houston', 'phoenix', 'dallas', 'denver', 'atlanta',
                     'san francisco', 'boston', 'philadelphia', 'london',
                     'paris', 'seoul', 'tokyo', 'sydney', 'berlin',
                     'sao paulo', 'toronto', 'munich', 'lucknow',
                     'wellington', 'ankara']:
            if city in title:
                sig.append(city)
                break

        # Coin names
        for coin in ['bitcoin', 'ethereum', 'solana', 'xrp', 'dogecoin']:
            if coin in title:
                sig.append(coin)
                break

        # Price: "$74,000" -> "74000"
        for m in re.finditer(r'\$([\d,]+)', title):
            sig.append(m.group(1).replace(',', ''))

        # Temperature ranges: "82-83"
        for m in re.finditer(r'(\d{1,3})-(\d{1,3})', title):
            sig.append(m.group(0))

        # Single temp before degree sign
        for m in re.finditer(r'(\d{1,3})\s*\u00b0', title):
            val = m.group(1)
            if not any(val in s for s in sig):
                sig.append(val)

        # Up/Down
        if 'up or down' in title or 'up/down' in title:
            sig.append('up or down')

        # Time windows: "5:00", "5:15"
        for m in re.finditer(r'(\d{1,2}:\d{2})', title):
            sig.append(m.group(1))

        # Verbs
        for word in ['dip', 'above', 'below']:
            if word in title:
                sig.append(word)

        # Date: "March 4"
        date_m = re.search(
            r'((?:january|february|march|april|may|june|july|august|'
            r'september|october|november|december)\s+\d{1,2})', title
        )
        if date_m:
            sig.append(date_m.group(1))

        return sig

    def _find_bet_in_response(self, bet, response_lower):
        """Find a bet's section in Bankr's numbered response.

        UpDown bets require ALL time keywords to avoid false matches.
        """
        sig = self._extract_bet_signature(bet['market_title'])
        if len(sig) < 2:
            return None

        time_keywords = [kw for kw in sig if re.match(r'\d{1,2}:\d{2}$', kw)]
        content_keywords = [kw for kw in sig if not re.match(r'\d{1,2}:\d{2}$', kw)]

        sections = re.split(r'(?:^|\n)\s*\d+[\.\)\-]\s*', response_lower)
        if len(sections) < 2:
            sections = re.split(r'[\n\u2022]', response_lower)

        best_section = None
        best_score = 0

        for section in sections:
            section = section.strip()
            if len(section) < 10:
                continue

            content_score = sum(1 for kw in content_keywords if kw in section)
            time_score = sum(1 for kw in time_keywords if kw in section)

            if time_keywords:
                if time_score < len(time_keywords) or content_score < 1:
                    continue
                total = content_score + time_score
            else:
                total = content_score
                if total < 2:
                    continue

            if total > best_score:
                best_score = total
                best_section = section

        return best_section[:500] if best_section else None

    # ==================================================================
    # Market Date Validation
    # ==================================================================

    def _is_market_past_date(self, bet):
        """Check if the market's resolution date has passed.

        Prevents premature resolution of future markets.
        UpDown markets handled separately by expiry logic.
        """
        title = bet.get('market_title', '')
        title_lower = title.lower()

        # UpDown markets use their own expiry logic
        if 'up or down' in title_lower or 'up/down' in title_lower:
            return True

        date_match = re.search(r'(?:March|Mar)\s+(\d{1,2})', title)
        if not date_match:
            return True  # can't parse, allow through

        market_day = int(date_match.group(1))
        now = datetime.now(timezone.utc)

        try:
            # Market settles at end of day UTC
            market_end = datetime(now.year, now.month, market_day, 23, 59, 59,
                                  tzinfo=timezone.utc)
            return now >= market_end
        except ValueError:
            return True

    # ==================================================================
    # Stale Bet Expiry
    # ==================================================================

    def _expire_stale_bets(self):
        """Handle stale bets — try Bankr redeem, confirm loss if no position."""
        pending = self.get_pending_bets()
        resolved = []
        now = datetime.now(timezone.utc)

        for bet in pending:
            title = bet['market_title']
            title_lower = title.lower()
            is_stale = False

            if 'up or down' in title_lower or 'up/down' in title_lower:
                expiry = self._parse_updown_expiry(title)
                if expiry:
                    is_stale = (now - expiry).total_seconds() / 3600 >= self.UPDOWN_EXPIRY_HOURS

            elif bet.get('category') == 'crypto' and ('above' in title_lower or 'below' in title_lower):
                expiry = self._parse_crypto_date_expiry(title)
                if expiry:
                    is_stale = (now - expiry).total_seconds() / 3600 >= self.CRYPTO_EXPIRY_HOURS

            if not is_stale:
                continue

            if not self.api_key:
                continue

            print(f"  [SETTLEMENT CLERK] Bet #{bet['id']} past expiry: {title[:60]}")
            r = self._redeem_position(bet)
            if r:
                resolved.append(r)
                continue

            # Last check: does Bankr even have this position?
            try:
                check = self._run_prompt(
                    f"Do I have any Polymarket position on \"{title}\"? "
                    f"Just tell me yes or no."
                )
                if check.get('success'):
                    check_resp = check.get('response', '').lower()
                    no_pos = ['no position', 'not found', 'no shares', "don't have",
                              'do not have', "doesn't appear", 'no active']
                    if any(kw in check_resp for kw in no_pos):
                        print(f"  [SETTLEMENT CLERK] #{bet['id']}: No position found. Resolving as loss.")
                        self._resolve_bet(bet['id'], won=False, profit=-bet['amount'],
                                          source='bankr_confirmed_loss', redeemed_amount=0.0)
                        resolved.append({'bet_id': bet['id'], 'won': False, 'profit': -bet['amount']})
                        continue
            except Exception as e:
                print(f"  [!] Bankr check error: {e}")

            print(f"  [WARN] #{bet['id']} stale but can't confirm. Leaving pending.")

        return resolved

    def _parse_updown_expiry(self, market_title):
        """Parse UpDown end time -> UTC datetime."""
        m = re.search(
            r'(\w+ \d{1,2}),?\s+\d{1,2}:\d{2}\s*[AP]M\s*-\s*(\d{1,2}:\d{2}\s*[AP]M)\s*ET',
            market_title, re.IGNORECASE
        )
        if not m:
            return None
        try:
            full_str = f"{m.group(1).strip()} 2026 {m.group(2).strip()}"
            for fmt in ["%B %d %Y %I:%M%p", "%B %d %Y %I:%M %p"]:
                try:
                    dt = datetime.strptime(full_str, fmt)
                    return (dt + timedelta(hours=5)).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        except Exception:
            pass
        return None

    def _parse_crypto_date_expiry(self, market_title):
        """Parse crypto daily market end date -> UTC datetime."""
        m = re.search(r'on (\w+ \d{1,2})', market_title, re.IGNORECASE)
        if not m:
            return None
        try:
            dt = datetime.strptime(f"{m.group(1)} 2026", "%B %d %Y")
            return (dt + timedelta(days=1, hours=5)).replace(tzinfo=timezone.utc)
        except Exception:
            return None

    # ==================================================================
    # Weather Resolution
    # ==================================================================

    def _weather_data_check(self):
        """Delegate weather resolution to weather agent."""
        if not self.weather_agent:
            return []
        print(f"\n[SETTLEMENT CLERK] Weather data check (hourly)...")
        resolved = self.weather_agent.check_and_resolve_weather_bets(wallet=self.wallet)
        return resolved if isinstance(resolved, list) else []

    # ==================================================================
    # DB Operations
    # ==================================================================

    def _ensure_tables(self):
        """Create resolution tracking tables."""
        from archivist import Archivist
        _arch = Archivist(self.db_path)
        _arch._execute("""
            CREATE TABLE IF NOT EXISTS bet_resolutions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bet_id INTEGER NOT NULL,
                resolved_at TEXT NOT NULL,
                resolved_by TEXT NOT NULL,
                won INTEGER,
                profit REAL,
                redeemed_amount REAL,
                actual_data TEXT,
                FOREIGN KEY (bet_id) REFERENCES bets(id)
            )
        """)
        try:
            _arch._execute("ALTER TABLE bets ADD COLUMN resolved_by TEXT DEFAULT NULL")
        except Exception:
            pass
        _arch._commit()
        

    def _resolve_bet(self, bet_id, won, profit, source='bankr_redeem',
                     redeemed_amount=None, actual_data=None):
        """Write resolution to DB. Single path for all bet types.

        If redeemed_amount provided, it overrides won/profit calculation.
        """
        from archivist import Archivist
        _arch = Archivist(self.db_path)
        _conn = _arch._conn()
        _conn.row_factory = sqlite3.Row

        bet = _conn.execute("SELECT * FROM bets WHERE id = ?", (bet_id,)).fetchone()
        if not bet:
            return

        if bet['status'] == 'resolved' and source != 'weather_data':
            return

        now = datetime.now(timezone.utc).isoformat()

        if redeemed_amount is not None:
            profit = redeemed_amount - bet['amount']
            won = redeemed_amount > bet['amount']

        balance_after = None
        if self.wallet:
            try:
                balance_after = self.wallet.available
            except Exception:
                pass

        _conn.execute("""
            UPDATE bets SET status = 'resolved', resolved_at = ?,
                won = ?, profit = ?, resolved_by = ?,
                balance_before = NULL, balance_after = ?
            WHERE id = ? AND status = 'pending'
        """, (now, int(won), profit, source, balance_after, bet_id))

        _conn.execute("""
            INSERT INTO bet_resolutions
                (bet_id, resolved_at, resolved_by, won, profit,
                 redeemed_amount, actual_data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (bet_id, now, source, int(won), profit,
              redeemed_amount,
              json.dumps(actual_data) if actual_data else None))

        _arch._commit()

        # Release wallet funds
        if self.wallet:
            module = 'weather' if bet['category'] == 'weather' else 'crypto'
            returned = (bet['amount'] + profit) if won else 0
            try:
                self.wallet.release_funds(module, bet_id, returned)
            except Exception as e:
                print(f"  [!] Wallet release error: {e}")

        if self.tracker:
            try:
                self.tracker._update_performance_stats()
            except Exception:
                pass

        if self.notifier:
            try:
                self.notifier.notify_bet_resolved(
                    market_title=bet['market_title'],
                    side=bet['side'],
                    amount=bet['amount'],
                    won=won, profit=profit,
                    balance_before=0,
                    balance_after=balance_after or 0,
                    daily_roi=0
                )
            except Exception:
                pass

        status = 'WON' if won else 'LOST'
        print(f"  [SETTLEMENT CLERK] Bet #{bet['id']} {status} | ${profit:+.2f} | via {source}")

    def get_pending_bets(self):
        """Get all pending bets from DB."""
        if not self.db_path:
            return []
        from archivist import Archivist
        _arch = Archivist(self.db_path)
        _conn = _arch._conn()
        _conn.row_factory = sqlite3.Row
        rows = _conn.execute("""
            SELECT id, market_title, category, cycle_type, side,
                   amount, odds, timestamp
            FROM bets WHERE status = 'pending'
            ORDER BY id ASC
        """).fetchall()
        return [dict(r) for r in rows]

    def _log_audit(self, source, resolved, response_raw):
        """Log resolution audit trail."""
        if not self.db_path:
            return
        now_str = datetime.now(timezone.utc).isoformat()
        from archivist import Archivist
        _arch = Archivist(self.db_path)
        _arch._execute("""
            INSERT INTO bet_resolutions
                (bet_id, resolved_at, resolved_by, won, profit,
                 redeemed_amount, actual_data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (0, now_str, f'{source}_log',
              len(resolved),
              sum(r['profit'] for r in resolved), 0,
              json.dumps({
                  'matched': len(resolved),
                  'response': response_raw[:2000]
              })))
        _arch._commit()

    # ==================================================================
    # Simulation (dry run)
    # ==================================================================

    def _simulate_trade(self, command: str, amount: float) -> Dict:
        return {
            'success': True,
            'trade_id': f"SIM_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            'status': 'simulated',
            'command': command,
            'amount': amount,
            'timestamp': datetime.now().isoformat(),
            'details': {'platform': 'polymarket', 'mode': 'dry_run'}
        }


# ==================================================================
# Backwards compatibility — old import paths still work
# ==================================================================

class BankrExecutor(Bankr):
    """Legacy alias. Use Bankr instead."""
    def __init__(self, api_key=None, dry_run=True):
        super().__init__(api_key=api_key, dry_run=dry_run)

    # Map old method names
    def execute_trade(self, command, amount=None):
        if self.dry_run:
            return self._simulate_trade(command, amount)
        if not self.api_key:
            return {'success': False, 'error': 'No API key'}
        print(f"\n[BANKER] Executing trade...")
        print(f"  Command: {command}")
        result = self._run_prompt(command)
        if result['success']:
            print(f"  [BANKER] Response: {result['response'][:200]}")
            return {
                'success': True, 'trade_id': result['job_id'],
                'status': 'completed', 'response': result['response'],
                'details': result.get('raw', {})
            }
        print(f"  [BANKER ERROR] {result.get('error', 'Unknown error')}")
        return {'success': False, 'trade_id': result.get('job_id'),
                'error': result.get('error', 'Unknown error')}

    def verify_bet_execution(self, market_title, side):
        return self.verify_bet(market_title, side)


class BetResolver:
    """Legacy wrapper. Delegates to Bankr instance.

    Usage unchanged:
        resolver = BetResolver(db_path=..., bankr=bankr_instance)
        resolver.run()
    """
    def __init__(self, db_path, bankr=None, wallet=None, weather_agent=None,
                 tracker=None, notifier=None):
        # If bankr is a Bankr instance, reuse it; otherwise create one
        if isinstance(bankr, Bankr):
            self._bankr = bankr
        else:
            self._bankr = Bankr(dry_run=False)

        # Set resolution-specific attrs on the Bankr instance
        self._bankr.db_path = db_path
        self._bankr.wallet = wallet
        self._bankr.weather_agent = weather_agent
        self._bankr.tracker = tracker
        self._bankr.notifier = notifier
        if db_path:
            self._bankr._ensure_tables()

    def run(self):
        return self._bankr.resolve_pending()

    def resolve_bet(self, bet_id, won, profit, source='bankr_redeem',
                    redeemed_amount=None, actual_data=None):
        return self._bankr._resolve_bet(bet_id, won, profit, source,
                                        redeemed_amount, actual_data)

    def get_pending_bets(self):
        return self._bankr.get_pending_bets()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    print("=" * 60)
    print("BANKR UNIFIED CLIENT TEST")
    print("=" * 60)

    client = Bankr(dry_run=False)

    print("\n[TEST] Check wallet address...")
    result = client.get_wallet_address()
    print(f"Result: {result.get('response', result.get('error', 'N/A'))}")

    print("\n[TEST] Check balance...")
    result = client.check_balance()
    print(f"Result: {result.get('response', result.get('error', 'N/A'))}")
