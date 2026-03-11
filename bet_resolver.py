"""
Unified Bet Resolver Engine v2
Handles resolution for ALL bet types: weather, crypto, and updown.

v2 changes:
- Two-step Bankr flow: check positions (1 call) -> redeem winners (1 call each)
- Natural language prompts tuned to how Bankr actually responds
- Stricter matching: time-based markets require ALL time keywords to match
- Auto-expiry: UpDown bets 2h past window -> loss, crypto 24h past date -> loss

Resolution paths:
1. Bankr check + redeem (every 10 min) — check positions, redeem any resolved
2. Time-based expiry (every cycle) — expire stale bets past their window
3. Weather data check (every 1h) — Open-Meteo actual temps
"""

import os
import re
import json
import time
from datetime import datetime, timedelta, timezone


# v2: Use smarter model for resolver Bankr calls (if Bankr API supports it)

class BetResolver:
    """Unified bet resolution engine for all modules."""

    BANKR_REDEEM_INTERVAL = 600      # 10 minutes
    WEATHER_CHECK_INTERVAL = 3600    # 1 hour
    UPDOWN_EXPIRY_HOURS = 2          # UpDown bets: loss if 2h past window
    CRYPTO_EXPIRY_HOURS = 24         # Crypto daily bets: loss if 24h past date
    MAX_BETS_PER_PROMPT = 10         # Max bets per Bankr check call

    def __init__(self, db_path, bankr=None, wallet=None, weather_agent=None,
                 tracker=None, notifier=None):
        self.db_path = db_path
        self.bankr = bankr
        self.wallet = wallet
        self.weather_agent = weather_agent
        self.tracker = tracker
        self.notifier = notifier
        self.last_bankr_redeem = None
        self.last_weather_check = None
        self._ensure_tables()

    def _ensure_tables(self):
        """Schema is now owned by The Archivist — no-op."""
        pass

    # ==================================================================
    # Main entry point
    # ==================================================================

    def run(self):
        """Main entry point. Called every cycle from main agent loop."""
        resolved = []
        now = datetime.now(timezone.utc)

        # 1. Bankr check + redeem (every 10 min)
        if (self.bankr and
            (not self.last_bankr_redeem or
             (now - self.last_bankr_redeem).total_seconds() >= self.BANKR_REDEEM_INTERVAL)):
            try:
                resolved += self._bankr_check_and_redeem()
            except Exception as e:
                print(f"[SETTLEMENT CLERK] Bankr error: {e}")
            self.last_bankr_redeem = now

        # 2. Expire stale bets (every cycle)
        try:
            resolved += self._expire_stale_bets()
        except Exception as e:
            print(f"[SETTLEMENT CLERK] Expiry error: {e}")

        # 3. Weather data path (hourly)
        if (self.weather_agent and
            (not self.last_weather_check or
             (now - self.last_weather_check).total_seconds() >= self.WEATHER_CHECK_INTERVAL)):
            try:
                resolved += self._weather_data_check()
            except Exception as e:
                print(f"[SETTLEMENT CLERK] Weather data check error: {e}")
            self.last_weather_check = now

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
    # Step 1: Check which positions are redeemable
    # ==================================================================

    def _bankr_check_and_redeem(self):
        """Portfolio-based resolution.

        Step 1: Ask Bankr for ALL Polymarket positions (one call)
        Step 2: Match redeemable positions to our pending bets
        Step 3: For $0 value redeemable = loss (resolve directly)
        Step 4: For >$0 value redeemable = win (claim via Bankr, get real USDC back)
        Step 5: Update DB from actual Bankr data only
        """
        pending = self.get_pending_bets()
        if not pending:
            return []

        print(f"\n[SETTLEMENT CLERK] Bankr check ({len(pending)} pending bets)...")

        # --- Step 1: Get full portfolio from Bankr ---
        result = self.bankr._run_prompt(
            "Show me all my Polymarket positions including: "
            "market title, my side (Yes/No/Up/Down), number of shares, "
            "current value in USDC, whether it is redeemable, and PnL. "
            "List every position, even $0 ones.",
        )
        if not result.get('success'):
            print(f"  [!] Bankr portfolio check failed: {result.get('error', '')[:100]}")
            return []

        portfolio_raw = result.get('response', '')
        portfolio = portfolio_raw.lower()
        print(f"  [SETTLEMENT CLERK] {portfolio_raw[:1500]}")

        resolved = []

        # --- Step 1.5: Bulk redeem all available shares ---
        redeem_result = self.bankr._run_prompt(
            'Redeem all available shares on Polymarket. '
            'Claim everything that is redeemable. '
            'Tell me what was redeemed and how much USDC was returned for each.',
        )
        if redeem_result.get('success'):
            redeem_resp = redeem_result.get('response', '')
            print(f'  [SETTLEMENT CLERK] Bulk redeem: {redeem_resp[:500]}')
        else:
            print(f'  [SETTLEMENT CLERK] Bulk redeem failed, continuing with individual checks')
            redeem_resp = ''

        # --- Step 1.6: Parse bulk redeem for resolved bets ---
        if redeem_resp:
            bulk_resolved = self._resolve_from_bulk_redeem(redeem_resp, pending)
            if bulk_resolved:
                resolved.extend(bulk_resolved)
                resolved_ids = set(r['bet_id'] for r in bulk_resolved)
                pending = [b for b in pending if b['id'] not in resolved_ids]
                print(f'  [SETTLEMENT CLERK] Bulk redeem resolved {len(bulk_resolved)} bets')

        time.sleep(3)

        # Re-check portfolio after bulk redeem
        result2 = self.bankr._run_prompt(
            'Show me all my Polymarket positions including: '
            'market title, my side (Yes/No/Up/Down), number of shares, '
            'current value in USDC, whether it is redeemable, and PnL. '
            'List every position, even bash ones.',
        )
        if result2.get('success'):
            portfolio_raw = result2.get('response', '')
            portfolio = portfolio_raw.lower()
            print(f'  [SETTLEMENT CLERK] Post-redeem portfolio: {portfolio_raw[:1500]}')

        # Log portfolio + redeem data to DB for other employees
        try:
            from archivist import Archivist
            _arch = Archivist(self.db_path)
            _redeem_text = redeem_resp if 'redeem_resp' in dir() else None
            _arch.record_portfolio_check(
                portfolio_raw=portfolio_raw[:5000],
                redeem_response=_redeem_text[:5000] if _redeem_text else None,
                pending_count=len(pending),
            )
        except Exception as e:
            print(f"  [SETTLEMENT CLERK] DB log error: {e}")

        # --- Step 2: Match each pending bet to portfolio ---
        for bet in pending:
            title = bet['market_title']
            title_lower = title.lower()
            amount = bet['amount']
            odds = bet.get('odds', 0.5)

            # Build search keywords from title
            keywords = self._extract_match_keywords(title)
            if not keywords:
                continue

            # Check if all keywords appear in portfolio
            if not all(kw in portfolio for kw in keywords):
                continue

            # Find the section of portfolio text about this position
            section = self._get_portfolio_section(portfolio, keywords)
            if not section:
                continue

            # Is it marked as redeemable?
            is_redeemable = any(w in section for w in ['redeemable', 'redeem', 'settled', 'resolved'])
            if not is_redeemable:
                continue  # Still active, skip

            # Check if value is $0 (clear loss) or >$0 (win, needs claiming)
            is_zero_value = any(p in section for p in ['$0.00', 'value: $0', 'value $0', 'worth $0', 'current price: $0'])

            if is_zero_value:
                # --- LOSS: position worth $0, no need to claim ---
                self.resolve_bet(bet['id'], won=False, profit=-amount,
                                 source='bankr_portfolio', redeemed_amount=0.0)
                resolved.append({'bet_id': bet['id'], 'won': False, 'profit': -amount})
                print(f"  [SETTLEMENT CLERK LOSS] #{bet['id']}: $0.00 redeemable (loss) - {title[:60]}")
            else:
                # --- POTENTIAL WIN: claim through Bankr to get actual USDC ---
                r = self._redeem_position(bet)
                if r:
                    resolved.append(r)
                time.sleep(3)

        # Sync wallet after all resolutions
        if self.wallet and resolved:
            try:
                time.sleep(3)
                self.wallet.sync_with_wallet(update_starting=False)
            except Exception:
                pass

        if not resolved:
            print(f"  [SETTLEMENT CLERK] No redeemable positions matched pending bets")

        return resolved

    def _extract_match_keywords(self, title):
        """Extract 2-3 keywords from market title for matching against portfolio."""
        t = title.lower()
        kws = []

        # City names
        for city in ['miami', 'seattle', 'chicago', 'atlanta', 'paris', 'london',
                      'seoul', 'ankara', 'munich', 'wellington', 'sao paulo', 'tokyo',
                      'new york', 'sydney', 'berlin', 'rome', 'dubai']:
            if city in t:
                kws.append(city)
                break

        # Crypto
        if 'bitcoin' in t or 'btc' in t:
            kws.append('btc' if 'btc' in t else 'bitcoin')
        if 'ethereum' in t or 'eth' in t:
            kws.append('eth' if 'eth' in t else 'ethereum')

        # Price target (e.g. $70,000 -> $70)
        import re
        price = re.search(r'\$(\d+)', t)
        if price:
            kws.append('$' + price.group(1))

        # Temperature
        temp = re.search(r'(\d+).?[cf]', t)
        if temp:
            kws.append(temp.group(1))

        # Date (march 7, mar 8)
        date = re.search(r'march\s+(\d+)', t)
        if date:
            kws.append('march ' + date.group(1).lstrip('0'))
        else:
            date2 = re.search(r'mar(?:ch)?\s+(\d+)', t)
            if date2:
                kws.append('mar' + date2.group(1).lstrip('0'))

        return kws[:3] if len(kws) >= 2 else kws

    def _get_portfolio_section(self, portfolio, keywords):
        """Find ~300 char section of portfolio containing all keywords."""
        positions = []
        for kw in keywords:
            pos = portfolio.find(kw)
            if pos >= 0:
                positions.append(pos)
        if not positions:
            return None
        start = max(0, min(positions) - 100)
        end = min(len(portfolio), max(positions) + 300)
        return portfolio[start:end]

    def _build_check_prompt(self, bets):
        """Build natural language prompt to check position status.

        Bankr responds best when given specific market titles to look up.
        It returns numbered responses with clear status like 'still active',
        'not found', 'redeemable', 'resolved'.
        """
        lines = []
        for i, bet in enumerate(bets, 1):
            side = (bet['side'] or '').upper()
            title = bet['market_title']
            lines.append(f"{i}. \"{title}\" (my side: {side})")

        bet_list = "\n".join(lines)

        return (
            f"Check my Polymarket wallet for these {len(bets)} positions. "
            f"For each one, tell me if it's still active, resolved and redeemable, "
            f"or not found in my wallet:\n\n"
            f"{bet_list}\n\n"
            f"Just give me the status for each number."
        )

    def _parse_redeemable(self, bets, response):
        """Parse Bankr check response to find redeemable positions.

        Bankr responds by number: '1. [title] — still active (X shares)'
        We look for keywords that indicate a position is resolved and redeemable.
        """
        redeemable = []

        # Redeemable indicators
        redeem_keywords = ['redeemable', 'resolved', 'redeem', 'claimable',
                           'ready to claim', 'can be redeemed', 'expired',
                           'won', 'lost', 'settled']

        # NOT redeemable indicators
        skip_keywords = ['still active', 'still open', 'not found', 'active',
                         'not in your', 'no position', 'shares held',
                         'not yet resolved', 'unresolved']

        for bet in bets:
            # v2: Don't try to resolve bets for markets that haven't ended
            if not self._is_market_past_date(bet):
                continue

            nearby = self._find_bet_in_response(bet, response)
            if nearby is None:
                continue

            # Check skip keywords first (still active / not found)
            is_skip = any(kw in nearby for kw in skip_keywords)
            is_redeem = any(kw in nearby for kw in redeem_keywords)

            # "still active" takes priority over "redeemable" if both present
            if is_skip and not is_redeem:
                continue

            if is_redeem:
                redeemable.append(bet)

        return redeemable

    # ==================================================================
    # Step 2: Redeem a specific position
    # ==================================================================

    def _redeem_position(self, bet):
        """Redeem a single resolved position via Bankr.

        v2: Added market date check + sanity check on amounts.
        STRICT RULE: Only resolve if Bankr returns a concrete USDC amount.
        Never guess, never estimate from odds, never use keyword-only logic.
        If we can't parse an exact USDC number, leave the bet pending.
        """
        title = bet['market_title']
        side = (bet['side'] or '').upper()
        amount = bet['amount']

        # v2: Don't try to redeem if market hasn't ended yet
        if not self._is_market_past_date(bet):
            print(f"  [SETTLEMENT CLERK] #{bet['id']}: Market date not yet passed — {title[:50]}")
            return None

        print(f"  [SETTLEMENT CLERK] #{bet['id']}: {title[:60]}...")

        result = self.bankr._run_prompt(
            f"Redeem my {side} shares on \"{title}\" on Polymarket. "
            f"I bet ${amount:.2f} on this position. "
            f"Tell me exactly how much USDC was returned to my wallet.",
        )

        if not result.get('success'):
            print(f"  [!] Redeem failed: {result.get('error', '')[:100]}")
            return None

        resp = result.get('response', '')
        resp_lower = resp.lower()
        print(f"  [BANKER] {resp[:300]}")

        # Check for explicit "no position" — only way to confirm $0 returned
        no_position_keywords = ['not found', 'no position', 'doesn\'t appear',
                                'not in your', 'no shares', 'position not found']
        if any(kw in resp_lower for kw in no_position_keywords):
            print(f"  [SKIP] Position not found in wallet")
            return None

        # Parse USDC amount — MUST get a concrete number or we don't resolve
        redeemed = self._parse_usdc_amount(resp, resp_lower, amount)

        if redeemed is None:
            print(f"  [SETTLEMENT CLERK] Could not parse concrete USDC amount from Bankr response. "
                  f"Leaving bet #{bet['id']} pending. Will retry next cycle.")
            return None

        # v2: Sanity check — redeemed can't exceed max theoretical payout
        odds = bet.get('odds', 0.5)
        max_payout = amount / odds if odds > 0.001 else amount * 200
        if redeemed > max_payout * 1.5:
            print(f"  [SETTLEMENT CLERK] Redeemed ${redeemed:.2f} exceeds max payout "
                  f"${max_payout:.2f} (1.5x). Bankr likely hallucinated. "
                  f"Leaving bet #{bet['id']} pending.")
            return None

        # Determine win/loss purely from the USDC amount
        if redeemed < 0.01:
            won = False
            profit = -amount
            redeemed = 0.0
        else:
            won = redeemed > amount
            profit = redeemed - amount

        self.resolve_bet(bet['id'], won, profit,
                         source='bankr_claim', redeemed_amount=redeemed)

        self._log_audit('bankr_claim', [{'bet_id': bet['id'], 'won': won, 'profit': profit}], resp)

        print(f"  [SETTLEMENT CLERK] Bet #{bet['id']}: Bankr returned ${redeemed:.2f} USDC "
              f"(bet ${amount:.2f}, profit ${profit:+.2f})")

        return {'bet_id': bet['id'], 'won': won, 'profit': profit}

    def _resolve_from_bulk_redeem(self, redeem_resp, pending):
        """Parse bulk redeem response and resolve any bets that were redeemed.

        After Bankr redeems positions, they disappear from the wallet.
        This parses the redeem response to catch them BEFORE they vanish.
        Returns list of resolved bet dicts.
        """
        if not redeem_resp:
            return []

        resp_lower = redeem_resp.lower()

        # Quick check: did anything actually get redeemed?
        if not any(kw in resp_lower for kw in ['redeemed', 'returned', 'redemption', 'success']):
            return []
        if 'no positions' in resp_lower or 'none of them' in resp_lower or 'no shares' in resp_lower:
            return []

        resolved = []

        for bet in pending:
            title = bet['market_title']
            amount = bet['amount']

            # Check if this bet title appears in the redeem response
            keywords = self._extract_match_keywords(title)
            if not keywords or len(keywords) < 2:
                continue

            if not all(kw in resp_lower for kw in keywords):
                continue

            # Found a match - extract USDC amount near the match
            section = self._get_portfolio_section(resp_lower, keywords)
            if not section:
                continue

            # Parse USDC from the section
            usdc = self._parse_usdc_amount(section, section, amount)

            if usdc is not None:
                self.resolve_bet(bet['id'], won=(usdc > amount * 0.01),
                                 profit=usdc - amount,
                                 source='bankr_bulk_redeem',
                                 redeemed_amount=usdc)
                resolved.append({'bet_id': bet['id'], 'won': usdc > amount * 0.01,
                                 'profit': usdc - amount})
                print(f"  [SETTLEMENT CLERK] Bulk redeem resolved #{bet['id']}: "
                      f"redeemed={usdc:.4f} USDC | {title[:60]}")
            else:
                # Bankr mentioned this market but no clear USDC amount
                print(f"  [SETTLEMENT CLERK] Bulk redeem mentions #{bet['id']} but no clear USDC. "
                      f"Will retry individual redeem next cycle.")

        return resolved
    def _parse_usdc_amount(self, resp, resp_lower, bet_amount):
        """Parse USDC amount from Bankr redemption response.

        v2: STRICTER parsing. Only accept amounts in clear redemption context.
        Reject numbers that are share counts, prices, or strike values.
        Returns None if no concrete number found — caller must NOT resolve.

        Accepted patterns (must have redemption context):
        - 'returned 18.25 USDC' / 'received 18.25 USDC'
        - 'redeemed 18.25 USDC' / 'claimed 18.25 USDC'
        - 'wallet: 18.25 USDC' / 'balance: 18.25 USDC' (after redemption)
        - '$0.00 returned' (explicit zero = loss)
        - 'payout of 18.25 USDC' / 'payout: 18.25'
        """
        # Priority 1: USDC amount with redemption context verb nearby
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

        # Priority 2: Explicit "$0" or "0 USDC" with loss context
        loss_patterns = [
            r'(?:lost|loss|expired|worthless|no\s+(?:payout|return)).*?\$?0(?:\.0+)?\s*(?:USDC)?',
            r'\$0(?:\.0+)?\s*(?:USDC)?\s*(?:return|paid|back)',
        ]
        for pattern in loss_patterns:
            if re.search(pattern, resp_lower):
                return 0.0

        # Priority 3: Fallback — bare "X USDC" but ONLY if no confusing context
        # Reject if near words like "shares", "position", "worth", "value", "price"
        m = re.search(r'(\d+\.?\d*)\s*USDC', resp, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            # Check surrounding text for non-redemption context
            start = max(0, m.start() - 40)
            end = min(len(resp), m.end() + 40)
            context = resp[start:end].lower()
            reject_words = ['shares', 'position', 'worth', 'value', 'price',
                           'market', 'contract', 'stake', 'bet of', 'wagered']
            if not any(rw in context for rw in reject_words):
                if val < bet_amount * 200:
                    return val

        # No concrete amount found — return None (DO NOT GUESS)
        return None

    # ==================================================================
    # Keyword matching (fixed for time-based markets)
    # ==================================================================

    def _extract_bet_signature(self, market_title):
        """Extract keywords from market title for matching Bankr responses."""
        title = market_title.lower()
        sig = []

        # City names (weather)
        for city in ['chicago', 'miami', 'new york', 'seattle', 'los angeles',
                     'houston', 'phoenix', 'dallas', 'denver', 'atlanta',
                     'san francisco', 'boston', 'philadelphia', 'london',
                     'paris', 'seoul', 'tokyo', 'sydney', 'berlin',
                     'sao paulo', 'toronto', 'munich', 'lucknow']:
            if city in title:
                sig.append(city)
                break

        # Coin names (crypto)
        for coin in ['bitcoin', 'ethereum', 'solana', 'xrp', 'dogecoin']:
            if coin in title:
                sig.append(coin)
                break

        # Price numbers: "$74,000" -> "74000"
        for m in re.finditer(r'\$([\d,]+)', title):
            digits = m.group(1).replace(',', '')
            sig.append(digits)

        # Temperature ranges: "82-83"
        for m in re.finditer(r'(\d{1,3})-(\d{1,3})', title):
            sig.append(m.group(0))

        # Single temp before degree sign
        for m in re.finditer(r'(\d{1,3})\s*\u00b0', title):
            val = m.group(1)
            if not any(val in s for s in sig):
                sig.append(val)

        # Up/Down identifier
        if 'up or down' in title or 'up/down' in title:
            sig.append('up or down')

        # Time windows: "5:00", "5:15" from "5:00PM-5:15PM" (CRITICAL for UpDown)
        for m in re.finditer(r'(\d{1,2}:\d{2})', title):
            sig.append(m.group(1))

        # Distinctive verbs
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

        Key fix: UpDown bets require ALL time keywords to match.
        'ETH Up/Down 11:00AM-11:15AM' won't false-match 'ETH Up/Down 5:45PM-6:00PM'.
        """
        sig = self._extract_bet_signature(bet['market_title'])
        if len(sig) < 2:
            return None

        # Separate time keywords from content keywords
        time_keywords = [kw for kw in sig if re.match(r'\d{1,2}:\d{2}$', kw)]
        content_keywords = [kw for kw in sig if not re.match(r'\d{1,2}:\d{2}$', kw)]

        # Split response into numbered sections (Bankr responds "1. ... 2. ... 3. ...")
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
                # TIME-BASED (UpDown): require ALL time keywords + 1 content keyword
                if time_score < len(time_keywords):
                    continue
                if content_score < 1:
                    continue
                total = content_score + time_score
            else:
                # REGULAR: require 2+ content keywords
                total = content_score
                if total < 2:
                    continue

            if total > best_score:
                best_score = total
                best_section = section

        if best_section:
            return best_section[:500]

        return None

    # ==================================================================
    # Stale bet expiry
    # ==================================================================

    def _expire_stale_bets(self):
        """Check stale bets — but NEVER auto-resolve without proof.

        Instead of guessing loss, we attempt one more Bankr redeem.
        If Bankr says no position exists, we resolve as $0 returned (proven loss).
        Otherwise we leave it pending and log a warning.

        UpDown 15-min: check after 2h past window
        Crypto daily: check after 24h past date
        """
        pending = self.get_pending_bets()
        resolved = []
        now = datetime.now(timezone.utc)

        for bet in pending:
            title = bet['market_title']
            title_lower = title.lower()
            is_stale = False

            # --- UpDown bets ---
            if 'up or down' in title_lower or 'up/down' in title_lower:
                expiry = self._parse_updown_expiry(title)
                if expiry:
                    hours_past = (now - expiry).total_seconds() / 3600
                    is_stale = hours_past >= self.UPDOWN_EXPIRY_HOURS

            # --- Crypto daily bets ---
            elif bet.get('category') == 'crypto' and ('above' in title_lower or 'below' in title_lower):
                expiry = self._parse_crypto_date_expiry(title)
                if expiry:
                    hours_past = (now - expiry).total_seconds() / 3600
                    is_stale = hours_past >= self.CRYPTO_EXPIRY_HOURS

            if not is_stale:
                continue

            # Stale bet — try to redeem via Bankr for proof
            if self.bankr:
                print(f"  [SETTLEMENT CLERK] Bet #{bet['id']} past expiry, attempting Bankr redeem for proof: "
                      f"{title[:60]}")
                r = self._redeem_position(bet)
                if r:
                    resolved.append(r)
                    continue

                # Redeem returned None — check if Bankr said "no position"
                # If _redeem_position already handled it, the bet is still pending
                # Try one explicit check
                try:
                    check = self.bankr._run_prompt(
                        f"Check my Polymarket wallet: do I have any position on "
                        f"\"{title}\"? Just tell me yes or no and any USDC amount.",
                                )
                    if check.get('success'):
                        check_resp = check.get('response', '').lower()
                        no_pos = ['no position', 'not found', 'no shares', 'don\'t have',
                                  'do not have', 'doesn\'t appear', 'no active']
                        if any(kw in check_resp for kw in no_pos):
                            print(f"  [SETTLEMENT CLERK] Bet #{bet['id']}: Bankr confirms no position. "
                                  f"Resolving as $0 returned.")
                            self.resolve_bet(bet['id'], won=False, profit=-bet['amount'],
                                             source='bankr_confirmed_loss', redeemed_amount=0.0)
                            resolved.append({'bet_id': bet['id'], 'won': False,
                                             'profit': -bet['amount']})
                            continue
                except Exception as e:
                    print(f"  [!] Bankr check error: {e}")

            # Could not confirm — leave pending
            print(f"  [WARN] Bet #{bet['id']} is stale but cannot confirm resolution. "
                  f"Leaving pending.")

        return resolved

    def _parse_updown_expiry(self, market_title):
        """Parse end time from UpDown market title -> UTC datetime.

        'Bitcoin Up or Down - March 4, 5:00PM-5:15PM ET' -> 5:15PM ET as UTC
        """
        m = re.search(
            r'(\w+ \d{1,2}),?\s+\d{1,2}:\d{2}\s*[AP]M\s*-\s*(\d{1,2}:\d{2}\s*[AP]M)\s*ET',
            market_title, re.IGNORECASE
        )
        if not m:
            return None

        date_str = m.group(1).strip()
        end_time = m.group(2).strip()

        try:
            full_str = f"{date_str} 2026 {end_time}"
            for fmt in ["%B %d %Y %I:%M%p", "%B %d %Y %I:%M %p"]:
                try:
                    dt = datetime.strptime(full_str, fmt)
                    break
                except ValueError:
                    continue
            else:
                return None
            # ET -> UTC (EST = UTC-5 before DST Mar 9)
            return (dt + timedelta(hours=5)).replace(tzinfo=timezone.utc)
        except Exception:
            return None

    def _parse_crypto_date_expiry(self, market_title):
        """Parse resolution date from crypto daily market title.

        'Will the price of Bitcoin be above $62,000 on March 5?' -> end of March 5 ET
        """
        m = re.search(r'on (\w+ \d{1,2})', market_title, re.IGNORECASE)
        if not m:
            return None
        try:
            date_str = f"{m.group(1)} 2026"
            dt = datetime.strptime(date_str, "%B %d %Y")
            # End of day ET = 5 AM UTC next day
            return (dt + timedelta(days=1, hours=5)).replace(tzinfo=timezone.utc)
        except Exception:
            return None

    # ==================================================================
    # Weather data check
    # ==================================================================

    def _weather_data_check(self):
        """Fetch actual temps for weather bets via Open-Meteo."""
        if not self.weather_agent:
            return []
        print(f"\n[SETTLEMENT CLERK] Weather data check (hourly)...")
        resolved = self.weather_agent.check_and_resolve_weather_bets(
            wallet=self.wallet
        )
        if isinstance(resolved, list):
            return resolved
        return []

    # ==================================================================
    # Universal resolve
    # ==================================================================

    def resolve_bet(self, bet_id, won, profit, source='bankr_claim',
                    redeemed_amount=None, actual_data=None):
        """Universal resolution path for ALL bet types.

        STRICT: profit is always computed as redeemed_amount - bet_amount.
        If redeemed_amount is provided, it overrides any passed-in profit.
        Balance tracking uses wallet query only (no manual calc).
        """
        from archivist import Archivist
        _arch = Archivist(self.db_path)

        bet = _arch.get_bet(bet_id)
        if not bet:
            return

        if bet['status'] == 'resolved' and source != 'weather_data':
            return

        now = datetime.now(timezone.utc).isoformat()

        # If we have a concrete redeemed_amount, compute profit from it
        if redeemed_amount is not None:
            profit = redeemed_amount - bet['amount']
            won = redeemed_amount > bet['amount']

        # LAST LINE OF DEFENSE: cap profit to max theoretical payout
        bet_odds = bet['odds'] or 0.5
        max_payout = bet['amount'] / bet_odds if bet_odds > 0.001 else bet['amount'] * 200
        max_profit = max_payout - bet['amount']
        if profit > max_profit * 1.5:
            print(f"  [SETTLEMENT CLERK] #{bet_id}: profit ${profit:.2f} exceeds max ${max_profit:.2f}. Capping.")
            profit = max_profit
            won = True

        # Balance: just query wallet directly, don't try to calculate
        balance_before = None
        balance_after = None
        if self.wallet:
            try:
                balance_after = self.wallet.available
            except Exception:
                pass

        _arch._execute("""
            UPDATE bets SET status = 'resolved', resolved_at = ?,
                won = ?, profit = ?, resolved_by = ?,
                balance_before = ?, balance_after = ?
            WHERE id = ? AND status = 'pending'
        """, (now, int(won), profit, source, balance_before, balance_after, bet_id))

        _arch._execute("""
            INSERT INTO bet_resolutions
                (bet_id, resolved_at, resolved_by, won, profit,
                 redeemed_amount, actual_data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (bet_id, now, source, int(won), profit,
              redeemed_amount,
              json.dumps(actual_data) if actual_data else None))

        # Get city for weather analytics update (before closing conn)
        _weather_city = None
        _weather_side = None
        if bet['category'] == 'weather':
            try:
                _city_row = _arch._fetchone("SELECT city FROM weather_bets WHERE bet_id = ?", (bet_id,))
                if _city_row:
                    _weather_city = _city_row[0] if isinstance(_city_row, tuple) else _city_row['city']
                    _weather_side = bet['side'] or ''
            except Exception:
                pass

        _arch._commit()

        # Update weather DB gates after resolution (fixes Seoul bypass bug)
        if _weather_city and self.weather_agent:
            try:
                self.weather_agent.update_weather_analytics_external(_weather_city, _weather_side)
                print(f"  [SETTLEMENT CLERK] Updated weather analytics for {_weather_city}/{_weather_side}")
            except Exception as e:
                print(f"  [SETTLEMENT CLERK] Weather analytics update error: {e}")

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
                    balance_before=balance_before or 0,
                    balance_after=balance_after or 0,
                    daily_roi=0
                )
            except Exception:
                pass

        status = 'WON' if won else 'LOST'
        print(f"  [SETTLEMENT CLERK] Bet #{bet['id']} {status} | ${profit:+.2f} | via {source}")

    # ==================================================================
    # Helpers
    # ==================================================================

    def _log_audit(self, source, resolved, response_raw):
        """Log resolution to audit table via Archivist."""
        from archivist import Archivist
        _arch = Archivist(self.db_path)
        _arch.record_resolution_detail(
            bet_id=0,
            resolved_by=f'{source}_log',
            won=len(resolved),
            profit=sum(r['profit'] for r in resolved),
            redeemed_amount=0,
            actual_data=json.dumps({
                'matched': len(resolved),
                'response': response_raw[:2000]
            }),
        )

    def _is_market_past_date(self, bet):
        """Check if market date has passed. Returns False if market is still live.

        For crypto daily markets ('above $X on March Y'), check if March Y has passed.
        For weather markets, same check on the date.
        UpDown markets have their own expiry logic in _expire_stale_bets.
        """
        title = bet.get('market_title', '')
        title_lower = title.lower()

        # Skip UpDown markets (handled by _expire_stale_bets)
        if 'up or down' in title_lower or 'up/down' in title_lower:
            return True  # let existing expiry logic handle these

        # Parse "March X" or "Mar X" from title
        date_match = re.search(r'(?:March|Mar)\s+(\d{1,2})', title)
        if not date_match:
            return True  # can't parse date, let it through

        market_day = int(date_match.group(1))
        now = datetime.now(timezone.utc)

        # Build market end time: end of that day in UTC
        # Markets typically settle after midnight UTC on the market date
        try:
            market_end = datetime(now.year, now.month, market_day, 23, 59, 59,
                                  tzinfo=timezone.utc)
            # If the market date hasn't ended yet, don't resolve
            if now < market_end:
                return False
        except ValueError:
            return True  # invalid date, let it through

        return True

    def get_pending_bets(self):
        """Get all pending bets from DB via Archivist."""
        from archivist import Archivist
        _arch = Archivist(self.db_path)
        rows = _arch._fetchall("""
            SELECT id, market_title, category, cycle_type, side,
                   amount, odds, timestamp
            FROM bets WHERE status = 'pending'
            ORDER BY id ASC
        """)
        cols = ['id', 'market_title', 'category', 'cycle_type', 'side',
                'amount', 'odds', 'timestamp']
        return [dict(zip(cols, r)) for r in rows]
