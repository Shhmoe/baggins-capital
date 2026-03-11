
## v4.1 — YES Bet Revolution (Mar 6 2026)

### The Problem
- Weather module was 38% win rate (14W/22L), bleeding money
- Seattle: 1W/5L (-$13.68) — forecasts accurate but betting WRONG SIDE
- Model always bet NO (heavy favorite at 60-70c) even when forecast pointed at the range
- Structural bias: P(not in range) > P(in range) for any 2F bucket, so NO edge always > YES edge

### The Fix
- When forecast IS in a range (weighted_mean falls within it): bet YES, floor edge at 1%
- When forecast is NEAR a range (within 2F of edge): bet YES if price <= 50c
- Lowered our_prob gate from 15% to 8% (2F range naturally has 10-20% probability)
- Seattle penalty removed: was -10 (blocked bets), now 0 (neutral) — problem was side, not forecast
- Near-range distance: uses nearest edge, not midpoint

### Expected Impact (backtested)
- Seattle: -$13.68 (NO) -> +$25.05 (YES) = $38.73 swing
- YES bets pay 2-3x (buy at 30-40c), NO bets pay 0.3-0.5x (buy at 60-70c)
- Atlanta/NYC unaffected — their wins come from forecast being OFF-range (NO is correct)

### Key Insight
Accurate forecasts + YES = printer. The forecast points at the range, bet on it.
Inaccurate forecasts + NO = safe. The forecast misses, bet against the range.
