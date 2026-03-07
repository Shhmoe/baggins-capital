"""
Avantis Trade Executor via Bankr
Executes leverage trades on Avantis using Bankr natural language API
"""

import os
import requests
import time
from typing import Dict, List, Optional
from datetime import datetime


class AvantisExecutor:
    """Execute leveraged trades on Avantis via Bankr."""

    def __init__(self, api_key: str = None, dry_run: bool = True):
        self.api_key = (api_key or os.getenv("BANKR_API_KEY", "")).strip()
        self.base_url = "https://api.bankr.bot"
        self.dry_run = dry_run
        self.poll_interval = 2  # Bankr docs: poll every 2s
        self.poll_timeout = 420  # 3 minutes max for leverage trades (Bankr docs: 60 attempts * 2s)

        if not self.api_key:
            print("[!] WARNING: BANKR_API_KEY not set")

    def open_position(self, market: str, side: str, collateral_usd: float,
                     leverage: int, stop_loss_pct: float = None,
                     take_profit_pct: float = None) -> Dict:
        """
        Open a leveraged position on Avantis.

        Args:
            market: Trading pair (e.g. 'BTC/USD')
            side: 'long' or 'short'
            collateral_usd: Collateral amount in USDC
            leverage: Leverage multiplier
            stop_loss_pct: Optional stop loss percentage
            take_profit_pct: Optional take profit percentage

        Returns:
            Dict with success status and details
        """
        if self.dry_run:
            print(f"[DRY RUN] Would open {side.upper()} "
                  f"{market} {leverage}x with ${collateral_usd:.2f}")
            return {
                'success': True,
                'dry_run': True,
                'pair': market,
                'side': side,
                'leverage': leverage,
                'collateral': collateral_usd
            }

        try:
            # Bankr docs format: "buy $5 of BTC/USD with 10x leverage"
            # With SL/TP: "buy $5 of BTC/USD with 10x leverage, 5% stop loss, and 10% take profit"
            action = 'long' if side == 'long' else 'short'
            prompt = f"open {leverage}x {action} on {market} with ${collateral_usd:.0f}"

            if stop_loss_pct and take_profit_pct:
                prompt += f", {stop_loss_pct:.1f}% stop loss, and {take_profit_pct:.1f}% take profit"
            elif stop_loss_pct:
                prompt += f", {stop_loss_pct:.1f}% stop loss"
            elif take_profit_pct:
                prompt += f", {take_profit_pct:.1f}% take profit"

            print(f"[AVANTIS] Opening position: {prompt}")

            # Execute via Bankr
            result = self._run_prompt(prompt)

            # Bankr returns success=True even when response text says "error"
            # but the trade actually executed. Trust the job completion status.
            if result.get('success'):
                response_text = result.get('response', '')
                print(f"[AVANTIS] Bankr response: {response_text[:200]}")

                # Check for error phrases in Bankr response text
                error_phrases = [
                    'unavailable', 'experiencing issues', 'try again later',
                    'currently down', 'service issue', 'not available',
                    'unable to', 'cannot process', 'maintenance',
                ]
                resp_lower = response_text.lower()
                for phrase in error_phrases:
                    if phrase in resp_lower:
                        print(f"  [!] Bankr indicated failure: '{phrase}' found in response")
                        return {
                            'success': False,
                            'error': f'Bankr response indicates failure: {response_text[:150]}',
                            'response': response_text
                        }

                return {
                    'success': True,
                    'pair': market,
                    'side': side,
                    'leverage': leverage,
                    'collateral': collateral_usd,
                    'response': response_text
                }
            else:
                return {
                    'success': False,
                    'error': result.get('error', 'Unknown error')
                }

        except Exception as e:
            print(f"[!] Failed to open position: {e}")
            return {'success': False, 'error': str(e)}

    def close_position(self, pair: str, side: str) -> Dict:
        """
        Close an open position on Avantis.

        Args:
            pair: Trading pair (e.g. 'BTC/USD')
            side: 'long' or 'short'

        Returns:
            Dict with success status and details
        """
        if self.dry_run:
            print(f"[DRY RUN] Would close {side.upper()} {pair}")
            return {
                'success': True,
                'dry_run': True,
                'pair': pair,
                'side': side
            }

        try:
            # Bankr docs: "close my BTC position"
            pair_name = pair.split('/')[0]  # BTC/USD -> BTC
            prompt = f"close my {pair_name} position"

            print(f"[AVANTIS] Closing position: {prompt}")

            result = self._run_prompt(prompt)

            if result.get('success'):
                response_text = result.get('response', '')
                print(f"[AVANTIS] Close response: {response_text[:200]}")
                return {
                    'success': True,
                    'pair': pair,
                    'side': side,
                    'response': response_text
                }
            else:
                return {
                    'success': False,
                    'error': result.get('error', 'Unknown error')
                }

        except Exception as e:
            print(f"[!] Failed to close position: {e}")
            return {'success': False, 'error': str(e)}

    # Known Avantis pairs for response parsing
    KNOWN_PAIRS = [
        'BTC/USD', 'ETH/USD', 'SOL/USD', 'BNB/USD', 'ARB/USD',
        'DOGE/USD', 'AVAX/USD', 'LINK/USD', 'ADA/USD', 'XRP/USD',
        'MATIC/USD', 'DOT/USD', 'UNI/USD', 'OP/USD', 'NEAR/USD',
        'FET/USD', 'ORDI/USD', 'STX/USD', 'AAVE/USD', 'MKR/USD',
        'EUR/USD', 'GBP/USD', 'JPY/USD', 'AUD/USD', 'USD/CAD',
    ]

    def get_open_positions(self) -> List[Dict]:
        """
        Get open positions from Avantis via Bankr.
        Parses natural language response for known pair names.
        """
        if self.dry_run:
            return []

        try:
            prompt = "show my Avantis positions"
            result = self._run_prompt(prompt)

            if result.get('success'):
                response = result.get('response', '')
                resp_lower = response.lower()
                print(f"[AVANTIS] Positions response: {response[:200]}")

                # Check for "no positions" indicators
                no_position_phrases = [
                    "don't have any", "no open position", "no active position",
                    "not currently have", "no positions", "0 position",
                    "don't currently", "no leverage", "empty",
                ]
                for phrase in no_position_phrases:
                    if phrase in resp_lower:
                        print("  [AVANTIS] Bankr confirms: no open positions")
                        return []

                # Scan for known pairs in response
                found = []
                for pair in self.KNOWN_PAIRS:
                    # Match "BTC/USD" or "BTC" or "BTCUSD"
                    base = pair.split('/')[0]
                    if base.lower() in resp_lower or pair.lower().replace('/', '') in resp_lower:
                        # Try to detect side (long/short)
                        side = 'long'  # default
                        if 'short' in resp_lower:
                            # Check if short is near this pair mention
                            idx = resp_lower.find(base.lower())
                            nearby = resp_lower[max(0, idx-50):idx+50]
                            if 'short' in nearby:
                                side = 'short'
                        found.append({'pair': pair, 'side': side})

                if found:
                    print(f"  [AVANTIS] Detected {len(found)} open positions: {[f['pair'] for f in found]}")
                else:
                    # Response exists but we can't parse it — DON'T assume empty
                    print(f"  [AVANTIS] WARN: Could not parse positions from response — treating as ambiguous")
                    # Return None-like sentinel so caller knows not to auto-close
                    return [{'pair': 'UNKNOWN', 'side': 'unknown', 'ambiguous': True}]

                return found
            else:
                print(f"[AVANTIS] Position query failed: {result.get('error')}")
                return [{'pair': 'UNKNOWN', 'side': 'unknown', 'ambiguous': True}]

        except Exception as e:
            print(f"[!] Failed to get positions: {e}")
            return [{'pair': 'UNKNOWN', 'side': 'unknown', 'ambiguous': True}]

    def check_position_exit(self, position: Dict, current_price: float = None) -> Optional[str]:
        """
        Check if a position should be exited based on current price.

        Args:
            position: Position dict with entry_price, side, stop_loss_pct, take_profit_pct
            current_price: Current market price (optional - returns None if not provided)

        Returns:
            'stop_loss', 'take_profit', or None
        """
        if current_price is None:
            return None

        try:
            entry = position.get('entry_price', 0)
            side = position.get('side', 'long')

            if not entry or entry == 0:
                return None

            # Calculate P&L percentage
            if side == 'long':
                pnl_pct = ((current_price - entry) / entry) * 100
            else:  # short
                pnl_pct = ((entry - current_price) / entry) * 100

            # Check stop loss
            stop_loss_pct = position.get('stop_loss_pct', 0)
            if stop_loss_pct > 0 and pnl_pct <= -stop_loss_pct:
                return 'stop_loss'

            # Check take profit
            take_profit_pct = position.get('take_profit_pct', 0)
            if take_profit_pct > 0 and pnl_pct >= take_profit_pct:
                return 'take_profit'

            return None

        except Exception as e:
            print(f"[!] Error checking exit: {e}")
            return None


    def get_trade_history(self, pair: str) -> Dict:
        """
        Query Bankr for recent Avantis trade history for a pair.
        Returns actual P&L data if available.
        """
        if self.dry_run:
            return {'success': False, 'error': 'dry_run'}

        try:
            base = pair.split('/')[0]
            prompt = f"show my recent Avantis trade history for {base}"
            result = self._run_prompt(prompt)

            if result.get('success'):
                response = result.get('response', '')
                resp_lower = response.lower()
                print(f"[AVANTIS] Trade history for {pair}: {response[:200]}")

                # Try to parse P&L from response
                import re
                pnl = None

                # Look for profit/loss amounts
                pnl_patterns = [
                    r'(?:profit|pnl|p&l|gain)[:\s]*[+\-]?\$?([\-]?\d+\.?\d*)',
                    r'(?:loss|lost)[:\s]*\$?(\d+\.?\d*)',
                    r'(?:realized|net)[:\s]*[+\-]?\$?([\-]?\d+\.?\d*)',
                ]
                for pattern in pnl_patterns:
                    m = re.search(pattern, resp_lower)
                    if m:
                        val = float(m.group(1))
                        if 'loss' in pattern or 'lost' in pattern:
                            val = -abs(val)
                        pnl = val
                        break

                return {
                    'success': True,
                    'response': response,
                    'pnl': pnl,
                    'pair': pair,
                }
            else:
                return {'success': False, 'error': result.get('error')}

        except Exception as e:
            print(f"[!] Failed to get trade history: {e}")
            return {'success': False, 'error': str(e)}

    # Words that could trigger fund movements — NEVER send to Bankr
    BLOCKED_WORDS = ['bridge', 'transfer', 'send to', 'swap', 'move funds',
                     'withdraw', 'send usdc', 'send eth']

    def _run_prompt(self, prompt: str) -> Dict:
        """
        Execute a prompt via Bankr API.
        Submit job -> poll until completion.
        """
        # Safety: block any prompt that could move funds
        prompt_lower = prompt.lower()
        for word in self.BLOCKED_WORDS:
            if word in prompt_lower:
                print(f"[AVANTIS] BLOCKED: prompt contains '{word}' — refusing to send")
                return {'success': False, 'error': f'Fund movement blocked: contains "{word}"'}

        try:
            # Submit job
            job_id = self._submit_job(prompt)
            if not job_id:
                return {'success': False, 'error': 'Failed to submit job'}

            # Poll for completion
            result = self._poll_job(job_id)
            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _submit_job(self, prompt: str) -> Optional[str]:
        """Submit a prompt and return job ID."""
        try:
            url = f"{self.base_url}/agent/prompt"
            headers = {
                'X-API-Key': self.api_key,
                'Content-Type': 'application/json'
            }

            response = requests.post(
                url,
                headers=headers,
                json={'prompt': prompt},
                timeout=15
            )

            if response.status_code in [200, 202]:
                data = response.json()
                return data.get('jobId')
            else:
                print(f"  [!] Submit error: {response.status_code}")
                return None

        except Exception as e:
            print(f"  [!] Submit failed: {e}")
            return None

    def _poll_job(self, job_id: str) -> Dict:
        """Poll job until completion or timeout."""
        url = f"{self.base_url}/agent/job/{job_id}"
        headers = {'X-API-Key': self.api_key}
        elapsed = 0

        while elapsed < self.poll_timeout:
            try:
                response = requests.get(url, headers=headers, timeout=10)

                if response.status_code == 200:
                    data = response.json()
                    status = data.get('status', '')

                    if status in ['completed', 'complete', 'done']:
                        return {
                            'success': True,
                            'response': data.get('response', ''),
                            'job_id': job_id
                        }
                    elif status in ['failed', 'error']:
                        return {
                            'success': False,
                            'error': data.get('response', 'Job failed'),
                            'job_id': job_id
                        }
                    # Still pending

            except Exception as e:
                print(f"  [!] Poll error: {e}")

            time.sleep(self.poll_interval)
            elapsed += self.poll_interval

        return {
            'success': False,
            'error': f'Timeout after {self.poll_timeout}s'
        }


if __name__ == "__main__":
    print("="*60)
    print("AVANTIS EXECUTOR TEST")
    print("="*60)

    executor = AvantisExecutor(dry_run=True)

    print("\n[TEST] Opening position...")
    result = executor.open_position(
        market='BTC/USD',
        side='long',
        collateral_usd=10.0,
        leverage=5,
        stop_loss_pct=5.0,
        take_profit_pct=15.0
    )
    print(f"[TEST] Result: {result}")

    print("\n[TEST] Closing position...")
    result = executor.close_position('BTC/USD', 'long')
    print(f"[TEST] Result: {result}")

    print("\n[TEST] Getting positions...")
    result = executor.get_open_positions()
    print(f"[TEST] Result: {result}")

    print("\n[TEST] Check exit without price (should return None)...")
    result = executor.check_position_exit({'entry_price': 95000, 'side': 'long'})
    print(f"[TEST] Result: {result}")

    print("\n[TEST] Check exit with price...")
    result = executor.check_position_exit(
        {'entry_price': 95000, 'side': 'long', 'stop_loss_pct': 2.0, 'take_profit_pct': 4.0},
        current_price=99000
    )
    print(f"[TEST] Result: {result}")
