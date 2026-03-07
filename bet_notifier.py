"""
Bet Notifier - Telegram notifications for every bet
Sends detailed messages on bet placement and resolution via Telegram Bot API
"""

import os
import requests
from datetime import datetime


class BetNotifier:
    """Send Telegram notifications for bets."""

    def __init__(self):
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.enabled = bool(self.bot_token and self.chat_id)
        self.silent = True  # Only hourly summary sends, everything else suppressed

        if not self.enabled:
            print("[!] Telegram notifications disabled - check TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")

    def notify_bet_placed(self, market_title, side, amount, odds, our_estimate,
                          edge, score, reasoning, balance_before, balance_after,
                          max_payout, daily_roi_target):
        """Send notification when bet is placed."""
        if self.silent:
            return False
        if not self.enabled:
            print(f"[MESSENGER TEST] Would notify: Bet placed on {market_title}")
            return False

        message = f"""BET PLACED

Market: {market_title}
Side: {side.upper()}
Amount: ${amount:.2f} ({(amount/balance_before)*100:.1f}% bankroll)
Odds: {odds:.1%} -> Our estimate: {our_estimate:.1%}
Edge: {edge:+.1%}
Score: {score}/100

Reasoning: {reasoning[:150]}

Balance: ${balance_before:.2f} -> ${balance_after:.2f}
Max payout: ${max_payout:.2f}
Target ROI today: {daily_roi_target:.0%}

Time: {datetime.now().strftime('%I:%M %p')}
"""

        return self._send_telegram(message)

    def notify_bet_resolved(self, market_title, side, amount, won, profit,
                            balance_before, balance_after, daily_roi):
        """Send notification when bet resolves."""
        if self.silent:
            return False
        if not self.enabled:
            print(f"[MESSENGER TEST] Would notify: Bet resolved - {'WON' if won else 'LOST'}")
            return False

        result = "WON" if won else "LOST"
        marker = "[WIN]" if won else "[LOSS]"

        message = f"""{marker} BET {result}

Market: {market_title}
Side: {side.upper()}
Amount: ${amount:.2f}
Profit: ${profit:+.2f}

Balance: ${balance_before:.2f} -> ${balance_after:.2f}
Daily ROI: {daily_roi:+.1%}

Time: {datetime.now().strftime('%I:%M %p')}
"""

        return self._send_telegram(message)

    def notify_daily_summary(self, date, starting_balance, ending_balance,
                            total_bets, wins, losses, profit, roi, target_met):
        """Send daily performance summary."""
        if not self.enabled:
            print(f"[MESSENGER TEST] Would send daily summary")
            return False

        status = "TARGET MET!" if target_met else "Below target"

        message = f"""DAILY SUMMARY - {date}

{status}

Starting: ${starting_balance:.2f}
Ending: ${ending_balance:.2f}
Profit: ${profit:+.2f}
ROI: {roi:+.1%} (Target: 40%)

Bets: {total_bets}
Wins: {wins}
Losses: {losses}
Win Rate: {(wins/total_bets)*100:.1f}%

{datetime.now().strftime('%I:%M %p')}
"""

        return self._send_telegram(message)

    def notify_alert(self, message):
        """Send general alert. Only HOURLY and STOP LOSS get through in silent mode."""
        if self.silent and not any(kw in message for kw in ['HOURLY', 'STOP LOSS', 'DAILY SUMMARY', 'DAILY RESET']):
            return False
        if not self.enabled:
            print(f"[MESSENGER TEST] Alert: {message}")
            return False

        alert_msg = f"""AGENT ALERT

{message}

{datetime.now().strftime('%I:%M %p')}
"""

        return self._send_telegram(alert_msg)

    def _send_telegram(self, message):
        """Send message via Telegram Bot API."""
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": message,
            }
            response = requests.post(url, json=payload, timeout=10)

            if response.status_code == 200:
                print(f"[+] Telegram notification sent")
                return True
            else:
                print(f"[-] Telegram error: {response.status_code} - {response.text[:200]}")
                return False

        except Exception as e:
            print(f"[-] Telegram error: {e}")
            return False


# Test function
def test_notifier():
    """Test Telegram notifications."""
    notifier = BetNotifier()

    # Test bet placed notification
    notifier.notify_bet_placed(
        market_title="Will it rain in Chicago on Feb 5?",
        side="yes",
        amount=5.0,
        odds=0.45,
        our_estimate=0.70,
        edge=0.25,
        score=82,
        reasoning="NOAA 70% confidence, cold front confirmed. Historical pattern matches.",
        balance_before=100.0,
        balance_after=95.0,
        max_payout=106.11,
        daily_roi_target=0.40
    )

    # Test bet resolved notification
    notifier.notify_bet_resolved(
        market_title="Will it rain in Chicago on Feb 5?",
        side="yes",
        amount=5.0,
        won=True,
        profit=6.11,
        balance_before=95.0,
        balance_after=101.11,
        daily_roi=0.011
    )

    # Test daily summary
    notifier.notify_daily_summary(
        date="2026-02-02",
        starting_balance=100.0,
        ending_balance=145.0,
        total_bets=25,
        wins=18,
        losses=7,
        profit=45.0,
        roi=0.45,
        target_met=True
    )


if __name__ == "__main__":
    test_notifier()
