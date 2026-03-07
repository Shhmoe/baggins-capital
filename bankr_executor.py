"""
Bankr API Trade Executor
Execute Polymarket trades via Bankr natural language API

API Flow: POST /agent/prompt -> poll GET /agent/job/{jobId} -> read response
Auth: X-API-Key header
"""

import os
import requests
import json
import time
from typing import Dict, Optional
from datetime import datetime


class BankrExecutor:
    """Execute trades on Polymarket via Bankr API."""

    def __init__(self, api_key: str = None, dry_run: bool = True):
        self.api_key = (api_key or os.getenv("BANKR_API_KEY", "")).strip()
        self.base_url = "https://api.bankr.bot"
        self.dry_run = dry_run
        self.poll_interval = 5  # seconds between polls
        self.poll_timeout = 420  # v2.0: 7 min timeout (Bankr trades take 3-5 min)

        if not self.api_key:
            print("[!] WARNING: BANKR_API_KEY not set")

    def _get_headers(self) -> Dict:
        """Get API headers."""
        return {
            'X-API-Key': self.api_key,
            'Content-Type': 'application/json'
        }

    def _submit_job(self, prompt: str, model: str = None) -> Optional[str]:
        """Submit a prompt to Bankr and return the job ID."""
        try:
            url = f"{self.base_url}/agent/prompt"
            payload = {'prompt': prompt}
            if model:
                payload['model'] = model
            response = requests.post(
                url,
                headers=self._get_headers(),
                json=payload,
                timeout=30
            )

            if response.status_code in [200, 202]:
                data = response.json()
                job_id = data.get('jobId')
                if job_id:
                    return job_id
                print(f"  [!] No jobId in response: {data}")
                return None
            else:
                print(f"  [!] Bankr submit error: {response.status_code} - {response.text[:200]}")
                return None

        except Exception as e:
            print(f"  [!] Bankr submit failed: {e}")
            return None

    def _poll_job(self, job_id: str) -> Dict:
        """Poll a job until completion or timeout."""
        url = f"{self.base_url}/agent/job/{job_id}"
        elapsed = 0

        while elapsed < self.poll_timeout:
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
                    # Still pending, keep polling
                else:
                    print(f"  [!] Poll error: {response.status_code}")

            except Exception as e:
                print(f"  [!] Poll error: {e}")

            time.sleep(self.poll_interval)
            elapsed += self.poll_interval

        return {
            'success': False,
            'error': f'Job timed out after {self.poll_timeout}s',
            'job_id': job_id
        }

    def _run_prompt(self, prompt: str, model: str = None) -> Dict:
        """Submit a prompt and wait for the result."""
        job_id = self._submit_job(prompt, model=model)
        if not job_id:
            return {'success': False, 'error': 'Failed to submit job'}

        return self._poll_job(job_id)

    def execute_trade(self, command: str, amount: float = None) -> Dict:
        """Execute a trade using Bankr's natural language API."""
        if self.dry_run:
            print(f"\n[DRY RUN] Would execute: {command}")
            return self._simulate_trade(command, amount)

        if not self.api_key:
            return {'success': False, 'error': 'No API key'}

        print(f"\n[BANKR] Executing trade...")
        print(f"  Command: {command}")

        result = self._run_prompt(command)

        if result['success']:
            print(f"  [BANKR] Response: {result['response'][:200]}")
            return {
                'success': True,
                'trade_id': result['job_id'],
                'status': 'completed',
                'response': result['response'],
                'details': result.get('raw', {})
            }
        else:
            print(f"  [BANKR ERROR] {result.get('error', 'Unknown error')}")
            return {
                'success': False,
                'trade_id': result.get('job_id'),
                'error': result.get('error', 'Unknown error')
            }

    def place_bet(self, market_title: str, side: str, amount: float,
                  odds: float = None) -> Dict:
        """Place a bet on a Polymarket market.
        Uses Bankr natural language format: 'bet $X on YES/NO for "market question" on Polymarket'
        """
        command = f'bet ${amount:.2f} on {side.upper()} for "{market_title}" on Polymarket'
        return self.execute_trade(command, amount)

    def check_balance(self) -> Dict:
        """Check wallet balance via Bankr."""
        if not self.api_key:
            return {'success': False, 'error': 'No API key'}

        result = self._run_prompt("What is my total USDC balance across all chains?")

        if result['success']:
            return {
                'success': True,
                'response': result['response'],
                'raw': result.get('raw', {})
            }
        return {'success': False, 'error': result.get('error', 'Failed')}

    def get_wallet_address(self) -> Dict:
        """Get the wallet deposit address."""
        if not self.api_key:
            return {'success': False, 'error': 'No API key'}

        result = self._run_prompt("What is my EVM deposit address?")

        if result['success']:
            return {
                'success': True,
                'response': result['response'],
                'raw': result.get('raw', {})
            }
        return {'success': False, 'error': result.get('error', 'Failed')}

    def get_positions(self) -> Dict:
        """Get current Polymarket positions."""
        if not self.api_key:
            return {'success': False, 'error': 'No API key'}

        result = self._run_prompt("What are my current Polymarket positions?")

        if result['success']:
            return {
                'success': True,
                'response': result['response'],
                'raw': result.get('raw', {})
            }
        return {'success': False, 'error': result.get('error', 'Failed')}


    def check_polygon_balance(self) -> Dict:
        """v2.0: Check USDC balance on Polygon network only."""
        if not self.api_key:
            return {'success': False, 'error': 'No API key'}

        result = self._run_prompt(
            "What is my USDC balance on the Polygon network only? "
            "Do NOT include balances from other chains like Ethereum mainnet, Arbitrum, or Base."
        )

        if result['success']:
            return {
                'success': True,
                'response': result['response'],
                'raw': result.get('raw', {})
            }
        return {'success': False, 'error': result.get('error', 'Failed')}

    def check_base_balance(self) -> Dict:
        """Check USDC + ETH balance on Base network only."""
        if not self.api_key:
            return {'success': False, 'error': 'No API key'}

        result = self._run_prompt(
            "What is my USDC and ETH balance on Base network only? "
            "Do NOT include balances from other chains. Show USDC for trading and ETH for gas fees."
        )

        if result['success']:
            return {
                'success': True,
                'response': result['response'],
                'raw': result.get('raw', {})
            }
        return {'success': False, 'error': result.get('error', 'Failed')}

    def verify_bet_execution(self, market_title: str, side: str) -> Dict:
        """v2.0: Verify a bet was actually placed by checking Bankr positions.
        Waits 10s after placement, then queries positions to confirm.
        Returns {'verified': True/False, 'reason': str}.
        """
        if self.dry_run:
            return {'verified': True, 'reason': 'dry run'}

        if not self.api_key:
            return {'verified': False, 'reason': 'no API key'}

        import time
        time.sleep(10)  # Wait for Bankr to process

        result = self._run_prompt(
            f"Do I have an active position on \'{market_title[:80]}\' on Polymarket? "
            f"Just answer yes or no and show the position details if it exists."
        )

        if not result.get('success'):
            return {'verified': False, 'reason': f"query failed: {result.get('error', '')}"}

        response = result.get('response', '').lower()
        title_words = market_title.lower().split()[:5]  # first 5 words

        # Check if position appears in response
        has_position = any(w in response for w in title_words if len(w) > 3)
        has_yes = 'yes' in response[:50]  # "yes" at start means confirmed

        if has_position or has_yes:
            return {'verified': True, 'reason': 'position found in Bankr'}
        else:
            return {'verified': False, 'reason': 'position not found in Bankr response'}

    def get_avantis_markets(self) -> Dict:
        """Dynamically get all available trading pairs on Avantis via Bankr.
        Returns list of markets like 'BTC/USD', 'ETH/USD', etc.
        """
        if not self.api_key:
            return {'success': False, 'error': 'No API key', 'markets': []}

        result = self._run_prompt(
            "What trading pairs/markets are available on Avantis on Base? "
            "List all available crypto/commodity/forex pairs. Just give me the pair names like BTC/USD, ETH/USD, etc."
        )

        if result.get('success'):
            response = result.get('response', '').upper()
            # Parse market pairs from response (e.g., BTC/USD, ETH/USD)
            import re
            markets = re.findall(r'\b([A-Z]{1,10})/([A-Z]{3})\b', response)
            markets = [f"{m[0]}/{m[1]}" for m in markets]
            markets = list(set(markets))  # Deduplicate
            
            return {
                'success': True,
                'markets': sorted(markets),
                'count': len(markets),
                'response': result.get('response', '')
            }
        else:
            return {
                'success': False,
                'error': result.get('error', 'Failed to fetch markets'),
                'markets': []
            }

    def _simulate_trade(self, command: str, amount: float) -> Dict:
        """Simulate a trade for dry run mode."""
        return {
            'success': True,
            'trade_id': f"SIM_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            'status': 'simulated',
            'command': command,
            'amount': amount,
            'timestamp': datetime.now().isoformat(),
            'details': {
                'platform': 'polymarket',
                'mode': 'dry_run',
                'message': 'Trade simulated successfully'
            }
        }


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    print("="*60)
    print("BANKR API TEST")
    print("="*60)

    executor = BankrExecutor(dry_run=False)

    print("\n[TEST 1] Check wallet address...")
    result = executor.get_wallet_address()
    print(f"Result: {result.get('response', result.get('error', 'N/A'))}")

    print("\n[TEST 2] Check balance...")
    result = executor.check_balance()
    print(f"Result: {result.get('response', result.get('error', 'N/A'))}")
