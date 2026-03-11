# Hedge Fund Agent — Version History

## v5.0 — "The YES Revolution" (Mar 6 2026)

### Summary
Complete overhaul of weather betting strategy. Discovered that forecasts were
accurate but the module was betting the WRONG SIDE (NO) when the forecast pointed
at the range. Flipped to forecast-driven YES betting. Added automated PatternAnalyzer
that finds these patterns autonomously every 30 hours.

### Changes This Session

#### Weather Strategy (weather_agent.py)
- **v4.1: YES bet logic** — When forecast predicts temp IN the range, bet YES (not NO)
  - Forecast IN range: bet YES unconditionally (floor edge at 1%)
  - Forecast NEAR range (within 2F of edge): bet YES if price <= 50c
  - Lowered probability gate from 15% to 8% (2F ranges naturally have 10-20% prob)
  - Near-range uses distance from nearest edge, not midpoint
- **v4.2: Edge weight reduction** — Edge no longer dominates decisions
  - Forecast-driven YES bets bypass WEATHER_MIN_EDGE gate entirely
  - find_best_bet ranking: confidence * (1 + edge * 0.3) instead of edge * confidence
  - Edge confidence scoring halved (max +3 instead of +6)
  - 15% ranking boost for forecast-driven bets
- **International cities re-enabled** — 0.9F avg error (most accurate!) but 0W/8L on NO
  - YES logic should fix them — same pattern as Seattle
  - Pattern analyzer will monitor and flag if still losing
- **Seattle bonus**: -10 -> 0 (neutral). Problem was side, not forecast accuracy.
- **City bonuses**: Atlanta +8, NYC +8 (confirmed winners by PatternAnalyzer)

#### PatternAnalyzer (NEW — weather_agent.py)
- Automated strategy review every 30 hours
- 5 analysis modules:
  1. City win rate segmentation
  2. Side (YES/NO) win rate analysis  
  3. Forecast accuracy vs outcome (THE GOLD — finds side selection bugs)
  4. What-if simulation (calculates opposite side P&L)
  5. Crypto side analysis
- Flags: SIDE_BUG, FLIP_CANDIDATE, LOSING, WINNING, CONFIRMED, CORRECT_SIDE
- Sends Telegram alert for actionable findings
- Logs all analyses to strategy_log DB table
- Rule: 5+ bet minimum before flagging any segment

#### Bet Resolver (bet_resolver.py)
- NEVER guesses profits — requires proof of exact USDC from Bankr
- Removed NL keyword parsing, removed odds-based estimation
- Stale bets: tries Bankr redeem, confirms "no position" before marking loss
- Weather wins wait for Bankr redemption (losses provable from actual temp)

#### Crypto Module (hedge_fund_active.py)
- Flat $3 bets (was Kelly $1-7)
- Dedup fix: _is_already_bet defaults to SKIP on DB error (was allowing duplicates)
- Added market_title fallback check
- Removed broken dead code in _should_skip_bet

#### Baggins Awareness (bagginsPersonalAi.py)
- All balance/P&L queries read from shared status file (not broken DB)
- P&L formula: wallet + deployed + take_profits - $50
- System prompt updated with current config limits
- _get_total_taken_profit() helper queries take_profits table

#### Database
- strategy_log table: tracks all strategy changes with version/rationale
- take_profits table: tracks money moved out ($75 Avantis funding)
- DB profit column marked UNRELIABLE for historical (pre-fix) bets

### Key Data Points
- Seattle backtest: NO = -$13.68, YES = +$25.05 ($38.73 swing on 6 bets)
- International backtest: NO = -$38.00, YES = +$31.65 ($69.65 swing on 8 bets)
- Both had accurate forecasts (Seattle 2.0F, Intl 0.9F avg error)
- Atlanta CONFIRMED working at 67% WR — no change needed
- NYC CONFIRMED at 100% WR — no change needed

### Config (hedge_fund_config.py)
- Weather: $1 flat, 30/day, 15 concurrent, all cities (US + international)
- Crypto: $3 flat, 10/day, 6 concurrent
- Pattern analyzer: 30h heartbeat

### Previous Version
- v3.0: Credibility engine, 7 weather APIs, crypto 4 bet types
- v4.0: City bonuses, sigma tuning, international disabled
- v4.1: YES bet logic (forecast-driven)
- v4.2: Edge weight reduction, PatternAnalyzer
- v5.0: Full release — international re-enabled, all changes integrated
