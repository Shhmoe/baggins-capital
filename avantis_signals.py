"""
Avantis Trading Signals v2.1
5-minute / 15-minute / 1-hour multi-timeframe momentum signals
for crypto perpetuals on Avantis via Bankr

Data sources:
  1. Binance public API - real 5m/15m/1h OHLCV klines (no key needed)
  2. CoinGecko - fallback for tokens not on Binance
  3. yfinance - forex, equities, commodities (Yahoo Finance, no key needed)
  4. Bankr - execution only (all trades go through Bankr)

Markets: All 94 Avantis pairs hardcoded from their on-chain registry
"""

import os
import re
import time
import requests
from typing import Dict, List, Optional
from datetime import datetime


# ======================================================================
# ALL 94 AVANTIS MARKETS
# Hardcoded from Avantis socket API (avantisfi.com)
# Each entry: avantis_pair -> {binance symbol, coingecko id, category}
# ======================================================================

AVANTIS_MARKETS = {
    # --- Crypto Major (Group 0) ---
    'ETH/USD':  {'binance': 'ETHUSDT',  'coingecko': 'ethereum',        'cat': 'crypto_major'},
    'BTC/USD':  {'binance': 'BTCUSDT',  'coingecko': 'bitcoin',         'cat': 'crypto_major'},
    'FET/USD':  {'binance': 'FETUSDT',  'coingecko': 'fetch-ai',        'cat': 'crypto_major'},
    'ORDI/USD': {'binance': 'ORDIUSDT', 'coingecko': 'ordi',            'cat': 'crypto_major'},
    'STX/USD':  {'binance': 'STXUSDT',  'coingecko': 'blockstack',      'cat': 'crypto_major'},

    # --- Crypto Large Cap (Group 1) ---
    'SOL/USD':  {'binance': 'SOLUSDT',  'coingecko': 'solana',          'cat': 'crypto_large'},
    'BNB/USD':  {'binance': 'BNBUSDT',  'coingecko': 'binancecoin',     'cat': 'crypto_large'},
    'ARB/USD':  {'binance': 'ARBUSDT',  'coingecko': 'arbitrum',        'cat': 'crypto_large'},
    'DOGE/USD': {'binance': 'DOGEUSDT', 'coingecko': 'dogecoin',        'cat': 'crypto_large'},
    'AVAX/USD': {'binance': 'AVAXUSDT', 'coingecko': 'avalanche-2',     'cat': 'crypto_large'},
    'OP/USD':   {'binance': 'OPUSDT',   'coingecko': 'optimism',        'cat': 'crypto_large'},
    'POL/USD':  {'binance': 'POLUSDT',  'coingecko': 'matic-network',   'cat': 'crypto_large'},
    'TIA/USD':  {'binance': 'TIAUSDT',  'coingecko': 'celestia',        'cat': 'crypto_large'},
    'SEI/USD':  {'binance': 'SEIUSDT',  'coingecko': 'sei-network',     'cat': 'crypto_large'},
    'ENA/USD':  {'binance': 'ENAUSDT',  'coingecko': 'ethena',          'cat': 'crypto_large'},
    'LINK/USD': {'binance': 'LINKUSDT', 'coingecko': 'chainlink',       'cat': 'crypto_large'},
    'LDO/USD':  {'binance': 'LDOUSDT',  'coingecko': 'lido-dao',        'cat': 'crypto_large'},
    'NEAR/USD': {'binance': 'NEARUSDT', 'coingecko': 'near',            'cat': 'crypto_large'},
    'INJ/USD':  {'binance': 'INJUSDT',  'coingecko': 'injective-protocol', 'cat': 'crypto_large'},
    'ZK/USD':   {'binance': 'ZKUSDT',   'coingecko': 'zksync',          'cat': 'crypto_large'},
    'ZRO/USD':  {'binance': 'ZROUSDT',  'coingecko': 'layerzero',       'cat': 'crypto_large'},
    'AAVE/USD': {'binance': 'AAVEUSDT', 'coingecko': 'aave',            'cat': 'crypto_large'},
    'SUI/USD':  {'binance': 'SUIUSDT',  'coingecko': 'sui',             'cat': 'crypto_large'},
    'EIGEN/USD': {'binance': 'EIGENUSDT', 'coingecko': 'eigenlayer',    'cat': 'crypto_large'},
    'APT/USD':  {'binance': 'APTUSDT',  'coingecko': 'aptos',           'cat': 'crypto_large'},
    'XRP/USD':  {'binance': 'XRPUSDT',  'coingecko': 'ripple',          'cat': 'crypto_large'},
    'ZEC/USD':  {'binance': 'ZECUSDT',  'coingecko': 'zcash',           'cat': 'crypto_large'},
    'XMR/USD':  {'binance': 'XMRUSDT',  'coingecko': 'monero',          'cat': 'crypto_large'},

    # --- Memecoins (Group 4) ---
    'SHIB/USD':     {'binance': 'SHIBUSDT',  'coingecko': 'shiba-inu',     'cat': 'memecoin'},
    'PEPE/USD':     {'binance': 'PEPEUSDT',  'coingecko': 'pepe',          'cat': 'memecoin'},
    'BONK/USD':     {'binance': 'BONKUSDT',  'coingecko': 'bonk',          'cat': 'memecoin'},
    'WIF/USD':      {'binance': 'WIFUSDT',   'coingecko': 'dogwifcoin',    'cat': 'memecoin'},
    'BRETT/USD':    {'binance': None,         'coingecko': 'brett',          'cat': 'memecoin'},
    'POPCAT/USD':   {'binance': None,         'coingecko': 'popcat',         'cat': 'memecoin'},
    'GOAT/USD':     {'binance': None,         'coingecko': 'goatseus-maximus', 'cat': 'memecoin'},
    'APE/USD':      {'binance': 'APEUSDT',   'coingecko': 'apecoin',       'cat': 'memecoin'},
    'CHILLGUY/USD': {'binance': None,         'coingecko': 'just-a-chill-guy', 'cat': 'memecoin'},
    'TRUMP/USD':    {'binance': 'TRUMPUSDT', 'coingecko': 'official-trump', 'cat': 'memecoin'},
    'FARTCOIN/USD': {'binance': 'FARTCOINUSDT', 'coingecko': 'fartcoin',   'cat': 'memecoin'},
    'PENGU/USD':    {'binance': 'PENGUUSDT', 'coingecko': 'pudgy-penguins', 'cat': 'memecoin'},
    'PUMP/USD':     {'binance': None,         'coingecko': 'pump',           'cat': 'memecoin'},

    # --- Crypto Mid/Small Cap (Group 5) ---
    'RENDER/USD': {'binance': 'RENDERUSDT', 'coingecko': 'render-token',   'cat': 'crypto_mid'},
    'WLD/USD':    {'binance': 'WLDUSDT',    'coingecko': 'worldcoin-wld',  'cat': 'crypto_mid'},
    'ARKM/USD':   {'binance': 'ARKMUSDT',   'coingecko': 'arkham',         'cat': 'crypto_mid'},
    'PENDLE/USD': {'binance': 'PENDLEUSDT', 'coingecko': 'pendle',         'cat': 'crypto_mid'},
    'ONDO/USD':   {'binance': 'ONDOUSDT',   'coingecko': 'ondo-finance',   'cat': 'crypto_mid'},
    'DYM/USD':    {'binance': 'DYMUSDT',    'coingecko': 'dymension',      'cat': 'crypto_mid'},
    'AERO/USD':   {'binance': None,          'coingecko': 'aerodrome-finance', 'cat': 'crypto_mid'},
    'ETHFI/USD':  {'binance': 'ETHFIUSDT',  'coingecko': 'ether-fi',       'cat': 'crypto_mid'},
    'JUP/USD':    {'binance': 'JUPUSDT',    'coingecko': 'jupiter-exchange-solana', 'cat': 'crypto_mid'},
    'REZ/USD':    {'binance': 'REZUSDT',    'coingecko': 'renzo',          'cat': 'crypto_mid'},
    'TAO/USD':    {'binance': 'TAOUSDT',    'coingecko': 'bittensor',      'cat': 'crypto_mid'},
    'HYPE/USD':   {'binance': None,          'coingecko': 'hyperliquid',    'cat': 'crypto_mid'},
    'BERA/USD':   {'binance': 'BERAUSDT',   'coingecko': 'berachain',      'cat': 'crypto_mid'},
    'KAITO/USD':  {'binance': 'KAITOUSDT',  'coingecko': 'kaito',          'cat': 'crypto_mid'},
    'VIRTUAL/USD': {'binance': None,         'coingecko': 'virtual-protocol', 'cat': 'crypto_mid'},
    'ZORA/USD':   {'binance': None,          'coingecko': 'zora',           'cat': 'crypto_mid'},
    'AVNT/USD':   {'binance': None,          'coingecko': 'avantis',             'cat': 'crypto_mid'},
    'ASTER/USD':  {'binance': None,          'coingecko': 'aster-2',             'cat': 'crypto_mid'},
    'XPL/USD':    {'binance': None,          'coingecko': 'plasma',             'cat': 'crypto_mid'},
    'MON/USD':    {'binance': None,          'coingecko': 'monad',             'cat': 'crypto_mid'},
    'LIT/USD':    {'binance': None,          'coingecko': 'lighter',             'cat': 'crypto_mid'},

    # --- Forex (Group 2) ---
    'EUR/USD': {'binance': None, 'coingecko': None, 'yfinance': 'EURUSD=X', 'cat': 'forex'},
    'USD/JPY': {'binance': None, 'coingecko': None, 'yfinance': 'JPY=X', 'cat': 'forex'},
    'GBP/USD': {'binance': None, 'coingecko': None, 'yfinance': 'GBPUSD=X', 'cat': 'forex'},
    'USD/CAD': {'binance': None, 'coingecko': None, 'yfinance': 'CAD=X', 'cat': 'forex'},
    'USD/CHF': {'binance': None, 'coingecko': None, 'yfinance': 'CHF=X', 'cat': 'forex'},
    'USD/SEK': {'binance': None, 'coingecko': None, 'yfinance': 'SEK=X', 'cat': 'forex'},
    'AUD/USD': {'binance': None, 'coingecko': None, 'yfinance': 'AUDUSD=X', 'cat': 'forex'},
    'NZD/USD': {'binance': None, 'coingecko': None, 'yfinance': 'NZDUSD=X', 'cat': 'forex'},
    'USD/SGD': {'binance': None, 'coingecko': None, 'yfinance': 'SGD=X', 'cat': 'forex'},
    'USD/TRY': {'binance': None, 'coingecko': None, 'yfinance': 'TRY=X', 'cat': 'forex'},
    'USD/CNH': {'binance': None, 'coingecko': None, 'yfinance': 'CNH=X', 'cat': 'forex'},
    'USD/INR': {'binance': None, 'coingecko': None, 'yfinance': 'INR=X', 'cat': 'forex'},
    'USD/KRW': {'binance': None, 'coingecko': None, 'yfinance': 'KRW=X', 'cat': 'forex'},
    'USD/MXN': {'binance': None, 'coingecko': None, 'yfinance': 'MXN=X', 'cat': 'forex'},
    'USD/ZAR': {'binance': None, 'coingecko': None, 'yfinance': 'ZAR=X', 'cat': 'forex'},
    'USD/BRL': {'binance': None, 'coingecko': None, 'yfinance': 'BRL=X', 'cat': 'forex'},
    'USD/IDR': {'binance': None, 'coingecko': None, 'yfinance': 'IDR=X', 'cat': 'forex'},
    'USD/TWD': {'binance': None, 'coingecko': None, 'yfinance': 'TWD=X', 'cat': 'forex'},

    # --- Commodities (Group 3) ---
    'XAU/USD':      {'binance': None, 'coingecko': None, 'yfinance': 'GC=F', 'cat': 'commodity'},
    'XAG/USD':      {'binance': None, 'coingecko': None, 'yfinance': 'SI=F', 'cat': 'commodity'},
    'USOILSPOT/USD': {'binance': None, 'coingecko': None, 'yfinance': 'CL=F', 'cat': 'commodity'},

    # --- Equities/Indices (Group 6) ---
    'SPY/USD':  {'binance': None, 'coingecko': None, 'yfinance': 'SPY', 'cat': 'equity'},
    'QQQ/USD':  {'binance': None, 'coingecko': None, 'yfinance': 'QQQ', 'cat': 'equity'},
    'COIN/USD': {'binance': None, 'coingecko': None, 'yfinance': 'COIN', 'cat': 'equity'},
    'NVDA/USD': {'binance': None, 'coingecko': None, 'yfinance': 'NVDA', 'cat': 'equity'},
    'AAPL/USD': {'binance': None, 'coingecko': None, 'yfinance': 'AAPL', 'cat': 'equity'},
    'AMZN/USD': {'binance': None, 'coingecko': None, 'yfinance': 'AMZN', 'cat': 'equity'},
    'MSFT/USD': {'binance': None, 'coingecko': None, 'yfinance': 'MSFT', 'cat': 'equity'},
    'META/USD': {'binance': None, 'coingecko': None, 'yfinance': 'META', 'cat': 'equity'},
    'TSLA/USD': {'binance': None, 'coingecko': None, 'yfinance': 'TSLA', 'cat': 'equity'},
    'GOOG/USD': {'binance': None, 'coingecko': None, 'yfinance': 'GOOG', 'cat': 'equity'},
    'HOOD/USD': {'binance': None, 'coingecko': None, 'yfinance': 'HOOD', 'cat': 'equity'},
}

# Priority scan list: always check these every cycle (high volume, most likely to move)
PRIORITY_PAIRS = [
    'BTC/USD', 'ETH/USD', 'SOL/USD', 'DOGE/USD', 'XRP/USD',
    'PEPE/USD', 'WIF/USD', 'BONK/USD', 'TRUMP/USD', 'HYPE/USD',
    'SUI/USD', 'AVAX/USD', 'LINK/USD', 'ARB/USD', 'OP/USD',
    'NEAR/USD', 'INJ/USD', 'RENDER/USD', 'TAO/USD', 'APT/USD',
]


class AvantisSignals:
    """Generate trading signals using 5m / 15m / 1h chart analysis."""

    def __init__(self, bankr_executor=None):
        self.binance_url = "https://api.binance.com/api/v3"
        self.coingecko_url = "https://api.coingecko.com/api/v3"
        self.bankr_url = "https://api.bankr.bot"
        self.bankr_key = os.getenv("BANKR_API_KEY", "").strip()
        self.bankr_executor = bankr_executor

        # Caches
        self.kline_cache = {}       # {pair: {interval: {'data': [...], 'expires': timestamp}}}
        self.price_cache = {}
        self.cache_expiry = {}

        # Rotation index for non-priority pairs
        self._rotation_idx = 0

        # Build scannable pairs (have at least one data source)
        self.scannable = {}
        for pair, info in AVANTIS_MARKETS.items():
            if info.get('binance') or info.get('coingecko') or info.get('yfinance'):
                self.scannable[pair] = info

        scannable_count = len(self.scannable)
        total_count = len(AVANTIS_MARKETS)
        yf_count = sum(1 for v in self.scannable.values() if v.get('yfinance'))
        print(f"[LEVERAGE SCOUT] {total_count} total markets, {scannable_count} scannable ({scannable_count - yf_count} crypto + {yf_count} forex/equity/commodity)")

        # Also keep PAIRS dict for backward compatibility with hedge_fund_active.py
        self.PAIRS = {pair: info.get('coingecko', '') for pair, info in self.scannable.items()}

    # ------------------------------------------------------------------
    # Main scan
    # ------------------------------------------------------------------

    def scan_opportunities(self) -> List[Dict]:
        """
        Scan pairs for trading signals using 5m/15m/1h charts.
        Priority pairs scanned every cycle, others rotated in batches.
        """
        opportunities = []

        # Build this cycle's scan list
        scan_list = self._build_scan_list()
        print(f"[LEVERAGE SCOUT] Scanning {len(scan_list)} pairs this cycle")

        for pair in scan_list:
            info = AVANTIS_MARKETS.get(pair, {})
            signal = self._analyze_pair(pair, info)
            if signal:
                opportunities.append(signal)

        opportunities.sort(key=lambda x: x['confidence'], reverse=True)
        return opportunities

    def _build_scan_list(self) -> List[str]:
        """Build list of pairs to scan this cycle. Priority + rotating batch."""
        scan = []

        # Always scan priority pairs
        for p in PRIORITY_PAIRS:
            if p in self.scannable:
                scan.append(p)

        # Rotate through remaining pairs in batches of 10
        remaining = [p for p in self.scannable if p not in scan]
        batch_size = 10
        start = self._rotation_idx % max(1, len(remaining))
        batch = remaining[start:start + batch_size]
        if len(batch) < batch_size:
            batch += remaining[:batch_size - len(batch)]
        scan.extend(batch)
        self._rotation_idx += batch_size

        return list(dict.fromkeys(scan))  # dedupe preserving order

    def get_all_markets(self) -> Dict:
        """Return all known Avantis markets for display/logging."""
        return AVANTIS_MARKETS

    # ------------------------------------------------------------------
    # Per-pair analysis
    # ------------------------------------------------------------------

    def _analyze_pair(self, pair: str, info: Dict) -> Optional[Dict]:
        """
        Full multi-timeframe analysis using Binance klines (primary),
        CoinGecko (crypto fallback), or yfinance (forex/equities/commodities).
        """
        try:
            binance_sym = info.get('binance')
            coingecko_id = info.get('coingecko')
            yf_ticker = info.get('yfinance')

            # Try Binance first (proper OHLCV candles), fall through to CoinGecko if blocked
            # CoinGecko primary for crypto (Binance API blocked from US servers)
            if coingecko_id:
                return self._analyze_via_coingecko(pair, coingecko_id)

            # Binance as fallback (works from non-US)
            if binance_sym:
                return self._analyze_via_binance(pair, binance_sym)

            # yfinance for forex, equities, commodities
            if yf_ticker:
                return self._analyze_via_yfinance(pair, yf_ticker)

            return None

        except Exception as e:
            print(f"[!] Error analyzing {pair}: {e}")
            return None

    # ------------------------------------------------------------------
    # Binance analysis (primary) - real 5m/15m/1h candles
    # ------------------------------------------------------------------

    def _analyze_via_binance(self, pair: str, symbol: str) -> Optional[Dict]:
        """Analyze using Binance klines for exact 5m/15m/1h data."""
        # Fetch all three timeframes
        k5 = self._get_binance_klines(symbol, '5m', 50)
        if not k5 or len(k5) < 20:
            return None
        time.sleep(0.1)

        k15 = self._get_binance_klines(symbol, '15m', 30)
        if not k15 or len(k15) < 10:
            return None
        time.sleep(0.1)

        k1h = self._get_binance_klines(symbol, '1h', 24)
        if not k1h or len(k1h) < 10:
            return None

        # Extract close prices
        closes_5m = [float(k[4]) for k in k5]
        closes_15m = [float(k[4]) for k in k15]
        closes_1h = [float(k[4]) for k in k1h]

        # Also get OHLCV for richer analysis
        volumes_5m = [float(k[5]) for k in k5]
        highs_5m = [float(k[2]) for k in k5]
        lows_5m = [float(k[3]) for k in k5]

        current_price = closes_5m[-1]
        if current_price <= 0:
            return None

        # Timeframe metrics from actual candle data
        m5 = self._tf_metrics_from_closes(closes_5m, 6)    # last 6 x 5m = 30 min of 5m data
        m15 = self._tf_metrics_from_closes(closes_15m, 4)   # last 4 x 15m = 1h of 15m data
        m1h = self._tf_metrics_from_closes(closes_1h, 4)    # last 4 x 1h = 4h of 1h data

        # Recent momentum (last 1 candle)
        mom_5m = self._pct_change(closes_5m, 1)
        mom_15m = self._pct_change(closes_15m, 1)
        mom_1h = self._pct_change(closes_1h, 1)

        # Multi-candle momentum (last 3 candles)
        mom_5m_3 = self._pct_change(closes_5m, 3)
        mom_15m_3 = self._pct_change(closes_15m, 3)
        mom_1h_3 = self._pct_change(closes_1h, 3)

        # EMA on 5m closes
        ema9 = self._ema(closes_5m, 9)
        ema21 = self._ema(closes_5m, 21)
        ema_cross = 'bullish' if ema9 > ema21 else 'bearish'
        ema_spread = ((ema9 - ema21) / ema21) * 100 if ema21 else 0

        # RSI on 5m data (more responsive)
        rsi = self._rsi(closes_5m, 14)

        # Volatility from 1h closes
        vol_1h = self._volatility(closes_1h[-6:])

        # 1h range for breakout detection
        high_1h = max(highs_5m[-12:]) if len(highs_5m) >= 12 else current_price
        low_1h = min(lows_5m[-12:]) if len(lows_5m) >= 12 else current_price
        range_1h = ((high_1h - low_1h) / low_1h) * 100 if low_1h > 0 else 0
        price_pos = ((current_price - low_1h) / (high_1h - low_1h)) * 100 if high_1h != low_1h else 50

        # Volume trend (is volume increasing?)
        if len(volumes_5m) >= 6:
            vol_recent = sum(volumes_5m[-3:])
            vol_prior = sum(volumes_5m[-6:-3])
            vol_ratio = vol_recent / vol_prior if vol_prior > 0 else 1.0
        else:
            vol_ratio = 1.0

        # Momentum acceleration
        accel = mom_5m - (mom_15m / 3)

        # Bollinger Bands on 5m closes
        bb = self._bollinger_bands(closes_5m, 20, 2.0)

        # Divergence detection on 5m data
        divergence = self._detect_divergence(closes_5m, 14, 10)

        # VWAP proxy from 5m data
        vwap = self._vwap_proxy(closes_5m, volumes_5m)

        # Format price display
        if current_price >= 100:
            price_fmt = f"${current_price:,.0f}"
        elif current_price >= 1:
            price_fmt = f"${current_price:,.2f}"
        else:
            price_fmt = f"${current_price:.6f}"

        print(f"[{pair}] {price_fmt} | "
              f"5m {mom_5m_3:+.2f}% | 15m {mom_15m_3:+.2f}% | 1h {mom_1h_3:+.2f}% | "
              f"RSI {rsi:.0f} | EMA {ema_cross} | vol {vol_1h:.2f}% | volR {vol_ratio:.1f}x")

        # Build combined metrics dicts
        m5_full = {**m5, 'momentum': mom_5m_3}
        m15_full = {**m15, 'momentum': mom_15m_3}
        m1h_full = {**m1h, 'momentum': mom_1h_3}

        return self._evaluate_signal(
            pair=pair, current_price=current_price,
            m5=m5_full, m15=m15_full, m1h=m1h_full,
            rsi=rsi, ema_cross=ema_cross, ema_spread=ema_spread,
            vol_1h=vol_1h, range_1h=range_1h, price_position=price_pos,
            accel=accel, vol_ratio=vol_ratio,
            bb=bb, divergence=divergence, vwap=vwap,
        )

    def _get_binance_klines(self, symbol: str, interval: str, limit: int) -> Optional[List]:
        """Fetch klines from Binance. Cached for 2 min."""
        cache_key = f"{symbol}_{interval}"
        cached = self.kline_cache.get(cache_key, {})
        if cached.get('data') and time.time() < cached.get('expires', 0):
            return cached['data']

        try:
            r = requests.get(
                f"{self.binance_url}/klines",
                params={'symbol': symbol, 'interval': interval, 'limit': limit},
                timeout=10
            )
            if r.status_code == 200:
                data = r.json()
                self.kline_cache[cache_key] = {'data': data, 'expires': time.time() + 120}
                return data
            else:
                return None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # CoinGecko analysis (fallback) - ~5min approximate data
    # ------------------------------------------------------------------

    def _analyze_via_coingecko(self, pair: str, coin_id: str) -> Optional[Dict]:
        """Fallback analysis using CoinGecko 24h chart (~5min intervals)."""
        try:
            cache_key = f"cg_{coin_id}"
            cached = self.kline_cache.get(cache_key, {})
            if cached.get('data') and time.time() < cached.get('expires', 0):
                prices_raw = cached['data']
            else:
                r = requests.get(
                    f"{self.coingecko_url}/coins/{coin_id}/market_chart",
                    params={'vs_currency': 'usd', 'days': 1},
                    timeout=15
                )
                if r.status_code != 200:
                    if r.status_code == 429:
                        print(f"  [{pair}] CoinGecko rate limited, waiting 6s...")
                        time.sleep(6)
                        r = requests.get(
                            f"{self.coingecko_url}/coins/{coin_id}/market_chart",
                            params={'vs_currency': 'usd', 'days': 1},
                            timeout=15
                        )
                        if r.status_code != 200:
                            return None
                    else:
                        return None
                prices_raw = r.json().get('prices', [])
                if prices_raw:
                    self.kline_cache[cache_key] = {'data': prices_raw, 'expires': time.time() + 300}
                time.sleep(2.5)  # CoinGecko rate limit (free tier: ~10-30/min)

            if not prices_raw or len(prices_raw) < 60:
                return None

            prices = [p[1] for p in prices_raw]
            current_price = prices[-1]
            if current_price <= 0:
                return None

            # CoinGecko gives ~5min intervals, so:
            # 1 point = ~5min, 3 points = ~15min, 12 points = ~1h
            m5 = self._tf_metrics_from_closes(prices, 1)
            m15 = self._tf_metrics_from_closes(prices, 3)
            m1h = self._tf_metrics_from_closes(prices, 12)

            mom_5m = self._pct_change(prices, 1)
            mom_15m = self._pct_change(prices, 3)
            mom_1h = self._pct_change(prices, 12)

            ema9 = self._ema(prices, 9)
            ema21 = self._ema(prices, 21)
            ema_cross = 'bullish' if ema9 > ema21 else 'bearish'
            ema_spread = ((ema9 - ema21) / ema21) * 100 if ema21 else 0

            rsi = self._rsi(prices, 14)
            vol_1h = self._volatility(prices[-12:])

            high_1h = max(prices[-12:])
            low_1h = min(prices[-12:])
            range_1h = ((high_1h - low_1h) / low_1h) * 100 if low_1h > 0 else 0
            price_pos = ((current_price - low_1h) / (high_1h - low_1h)) * 100 if high_1h != low_1h else 50

            accel = mom_5m - (mom_15m / 3)

            # BB and divergence work with closes only (no highs/lows/volumes needed)
            bb = self._bollinger_bands(prices, 20, 2.0)
            divergence = self._detect_divergence(prices, 14, 10)

            if current_price >= 100:
                price_fmt = f"${current_price:,.0f}"
            elif current_price >= 1:
                price_fmt = f"${current_price:,.2f}"
            else:
                price_fmt = f"${current_price:.6f}"

            print(f"[{pair}] {price_fmt} | "
                  f"5m {mom_5m:+.2f}% | 15m {mom_15m:+.2f}% | 1h {mom_1h:+.2f}% | "
                  f"RSI {rsi:.0f} | EMA {ema_cross} | vol {vol_1h:.2f}% (CG)")

            m5_full = {**m5, 'momentum': mom_5m}
            m15_full = {**m15, 'momentum': mom_15m}
            m1h_full = {**m1h, 'momentum': mom_1h}

            return self._evaluate_signal(
                pair=pair, current_price=current_price,
                m5=m5_full, m15=m15_full, m1h=m1h_full,
                rsi=rsi, ema_cross=ema_cross, ema_spread=ema_spread,
                vol_1h=vol_1h, range_1h=range_1h, price_position=price_pos,
                accel=accel, vol_ratio=1.0,
                bb=bb, divergence=divergence,
            )

        except Exception as e:
            import traceback; traceback.print_exc(); print(f"[!] CoinGecko analysis {pair}: {e}")
            return None


    # ------------------------------------------------------------------
    # yfinance analysis - forex, equities, commodities
    # ------------------------------------------------------------------

    def _analyze_via_yfinance(self, pair: str, yf_ticker: str) -> Optional[Dict]:
        """Analyze using Yahoo Finance data for forex/equities/commodities."""
        try:
            import yfinance as yf

            cache_key = f"yf_{yf_ticker}"
            cached = self.kline_cache.get(cache_key, {})
            if cached.get('data') and time.time() < cached.get('expires', 0):
                df = cached['data']
            else:
                ticker = yf.Ticker(yf_ticker)
                df = ticker.history(period='5d', interval='5m')
                if df is None or len(df) < 30:
                    return None
                self.kline_cache[cache_key] = {'data': df, 'expires': time.time() + 180}
                time.sleep(0.3)

            if df is None or len(df) < 30:
                return None

            closes = df['Close'].tolist()
            highs = df['High'].tolist()
            lows = df['Low'].tolist()
            volumes = df['Volume'].tolist()

            current_price = closes[-1]
            if current_price <= 0:
                return None

            m5 = self._tf_metrics_from_closes(closes, 1)
            m15 = self._tf_metrics_from_closes(closes, 3)
            m1h = self._tf_metrics_from_closes(closes, 12)

            mom_5m = self._pct_change(closes, 1)
            mom_15m = self._pct_change(closes, 3)
            mom_1h = self._pct_change(closes, 12)

            ema9 = self._ema(closes, 9)
            ema21 = self._ema(closes, 21)
            ema_cross = 'bullish' if ema9 > ema21 else 'bearish'
            ema_spread = ((ema9 - ema21) / ema21) * 100 if ema21 else 0

            rsi = self._rsi(closes, 14)
            vol_1h = self._volatility(closes[-12:])

            high_1h = max(closes[-12:])
            low_1h = min(closes[-12:])
            range_1h = ((high_1h - low_1h) / low_1h) * 100 if low_1h > 0 else 0
            price_pos = ((current_price - low_1h) / (high_1h - low_1h)) * 100 if high_1h != low_1h else 50

            accel = mom_5m - (mom_15m / 3)

            recent_vol = volumes[-3:] if len(volumes) >= 3 else volumes
            avg_vol = volumes[-30:] if len(volumes) >= 30 else volumes
            avg_v = sum(avg_vol) / len(avg_vol) if avg_vol else 1
            cur_v = sum(recent_vol) / len(recent_vol) if recent_vol else 1
            vol_ratio = cur_v / avg_v if avg_v > 0 else 1.0

            bb = self._bollinger_bands(closes, 20, 2.0)
            divergence = self._detect_divergence(closes, 14, 10)
            vwap = self._vwap_proxy(closes, volumes)

            cat = AVANTIS_MARKETS.get(pair, {}).get('cat', '')
            if cat == 'forex':
                price_fmt = f"{current_price:.4f}"
            elif current_price >= 100:
                price_fmt = f"${current_price:,.2f}"
            else:
                price_fmt = f"${current_price:.2f}"

            print(f"[{pair}] {price_fmt} | "
                  f"5m {mom_5m:+.2f}% | 15m {mom_15m:+.2f}% | 1h {mom_1h:+.2f}% | "
                  f"RSI {rsi:.0f} | EMA {ema_cross} | vol {vol_1h:.2f}% (YF)")

            m5_full = {**m5, 'momentum': mom_5m}
            m15_full = {**m15, 'momentum': mom_15m}
            m1h_full = {**m1h, 'momentum': mom_1h}

            return self._evaluate_signal(
                pair=pair, current_price=current_price,
                m5=m5_full, m15=m15_full, m1h=m1h_full,
                rsi=rsi, ema_cross=ema_cross, ema_spread=ema_spread,
                vol_1h=vol_1h, range_1h=range_1h, price_position=price_pos,
                accel=accel, vol_ratio=vol_ratio,
                bb=bb, divergence=divergence, vwap=vwap,
            )

        except Exception as e:
            print(f"[!] yfinance analysis {pair}: {e}")
            return None

    # ------------------------------------------------------------------
    # Timeframe metrics
    # ------------------------------------------------------------------

    def _tf_metrics_from_closes(self, closes: List[float], lookback: int) -> Dict:
        """Compute trend direction and strength from close prices."""
        if len(closes) < lookback + 1:
            return {'trend': 'neutral', 'strength': 0, 'candles_up': 0, 'candles_down': 0}

        window = closes[-(lookback + 1):]
        candles_up = sum(1 for i in range(1, len(window)) if window[i] > window[i-1])
        candles_down = sum(1 for i in range(1, len(window)) if window[i] < window[i-1])
        total = candles_up + candles_down
        ratio = candles_up / total if total > 0 else 0.5

        if ratio > 0.65:
            trend = 'up'
        elif ratio < 0.35:
            trend = 'down'
        else:
            trend = 'neutral'

        strength = abs(ratio - 0.5) * 2

        return {'trend': trend, 'strength': strength, 'candles_up': candles_up, 'candles_down': candles_down}

    def _pct_change(self, prices: List[float], lookback: int) -> float:
        """Percentage change over lookback periods."""
        if len(prices) < lookback + 1:
            return 0.0
        old = prices[-(lookback + 1)]
        new = prices[-1]
        return ((new - old) / old) * 100 if old else 0.0

    # ------------------------------------------------------------------
    # Technical indicators
    # ------------------------------------------------------------------

    def _ema(self, prices: List[float], period: int) -> float:
        if len(prices) < period:
            return prices[-1] if prices else 0
        k = 2.0 / (period + 1)
        ema = prices[0]
        for p in prices[1:]:
            ema = p * k + ema * (1 - k)
        return ema

    def _rsi(self, prices: List[float], period: int = 14) -> float:
        if len(prices) < period + 1:
            return 50.0
        changes = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        gains = [max(0, c) for c in changes[-period:]]
        losses = [max(0, -c) for c in changes[-period:]]
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _volatility(self, prices: List[float]) -> float:
        if len(prices) < 2:
            return 0.0
        mean = sum(prices) / len(prices)
        if mean == 0:
            return 0.0
        var = sum((p - mean) ** 2 for p in prices) / len(prices)
        return (var ** 0.5 / mean) * 100

    def _bollinger_bands(self, prices: List[float], period: int = 20, num_std: float = 2.0) -> Dict:
        """Bollinger Bands: middle (SMA), upper, lower, bandwidth, %B."""
        if len(prices) < period:
            mid = sum(prices) / len(prices)
            return {'middle': mid, 'upper': mid, 'lower': mid, 'bandwidth': 0, 'pct_b': 0.5}
        window = prices[-period:]
        mid = sum(window) / period
        std = (sum((p - mid) ** 2 for p in window) / period) ** 0.5
        upper = mid + num_std * std
        lower = mid - num_std * std
        bandwidth = ((upper - lower) / mid) * 100 if mid else 0
        pct_b = (prices[-1] - lower) / (upper - lower) if upper != lower else 0.5
        return {'middle': mid, 'upper': upper, 'lower': lower, 'bandwidth': bandwidth, 'pct_b': pct_b}

    def _atr(self, highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
        """Average True Range as percentage of price."""
        if len(highs) < period + 1:
            return 0.0
        trs = []
        for i in range(1, len(highs)):
            tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
            trs.append(tr)
        atr = sum(trs[-period:]) / period
        return (atr / closes[-1]) * 100 if closes[-1] else 0.0

    def _detect_divergence(self, prices: List[float], period: int = 14, lookback: int = 10) -> str:
        """Detect RSI divergence. Returns 'bullish', 'bearish', or 'none'."""
        if len(prices) < period + lookback + 1:
            return 'none'
        rsi_now = self._rsi(prices, period)
        rsi_prev = self._rsi(prices[:-lookback], period)
        price_now = prices[-1]
        price_prev = prices[-lookback - 1]
        # Bullish: lower price, higher RSI
        if price_now < price_prev and rsi_now > rsi_prev + 3:
            return 'bullish'
        # Bearish: higher price, lower RSI
        if price_now > price_prev and rsi_now < rsi_prev - 3:
            return 'bearish'
        return 'none'

    def _vwap_proxy(self, closes: List[float], volumes: List[float]) -> Dict:
        """Volume-weighted average price proxy. Returns VWAP and deviation %."""
        if len(closes) < 2 or len(volumes) < 2:
            return {'vwap': closes[-1] if closes else 0, 'deviation_pct': 0}
        n = min(len(closes), len(volumes))
        total_vol = sum(volumes[-n:])
        if total_vol == 0:
            return {'vwap': closes[-1], 'deviation_pct': 0}
        vwap = sum(c * v for c, v in zip(closes[-n:], volumes[-n:])) / total_vol
        deviation = ((closes[-1] - vwap) / vwap) * 100 if vwap else 0
        return {'vwap': vwap, 'deviation_pct': deviation}

    # ------------------------------------------------------------------
    # Signal evaluation
    # ------------------------------------------------------------------

    def _evaluate_signal(self, pair, current_price,
                         m5, m15, m1h,
                         rsi, ema_cross, ema_spread,
                         vol_1h, range_1h, price_position, accel,
                         vol_ratio=1.0,
                         bb=None, divergence='none', vwap=None) -> Optional[Dict]:
        """
        Multi-timeframe signal evaluation.

        14 setups:
        1.  TREND LONG       - 5m/15m/1h all bullish + EMA bullish
        2.  MOMENTUM LONG    - strong 5m+15m push, 1h not fighting
        3.  BREAKOUT LONG    - price near 1h high with momentum
        4.  REVERSAL LONG    - oversold bounce (1h down, 5m turning up)
        5.  TREND SHORT      - 5m/15m/1h all bearish + EMA bearish
        6.  MOMENTUM SHORT   - strong 5m+15m sell, 1h not fighting
        7.  BREAKOUT SHORT   - price near 1h low with sell pressure
        8.  REVERSAL SHORT   - overbought rejection (1h pumped, 5m rolling over)
        9.  SQUEEZE LONG     - BB compression breakout up with volume
        10. SQUEEZE SHORT    - BB compression breakout down with volume
        11. MEAN REV LONG    - price stretched below VWAP, stabilizing
        12. MEAN REV SHORT   - price stretched above VWAP, stalling
        13. DIVERGENCE LONG  - bullish RSI divergence
        14. DIVERGENCE SHORT - bearish RSI divergence
        """
        if vol_1h > 8.0 or vol_1h < 0.005:
            return None

        signal = None

        # ============================================================
        # LONG SETUPS
        # ============================================================

        # 1. TREND LONG: all timeframes aligned bullish
        if (m5['momentum'] > 0.05 and
            m15['momentum'] > 0.10 and
            m1h['momentum'] > 0.15 and
            ema_cross == 'bullish' and
            35 < rsi < 75):

            confidence = self._score(3, abs(m15['momentum']), True, rsi, vol_1h,
                                     (m5['strength'] + m15['strength'] + m1h['strength']) / 3,
                                     'trend_long', vol_ratio)
            if confidence >= 55:
                signal = self._build_signal(pair, 'long', current_price, confidence, vol_1h,
                    m5, m15, m1h, rsi,
                    f"Trend long: 5m {m5['momentum']:+.2f}%, 15m {m15['momentum']:+.2f}%, "
                    f"1h {m1h['momentum']:+.2f}%, EMA bull, RSI {rsi:.0f}")

        # 2. MOMENTUM LONG: strong short-term push
        if not signal and (
            m5['momentum'] > 0.12 and
            m15['momentum'] > 0.10 and
            m1h['trend'] != 'down' and
            ema_cross == 'bullish' and
            30 < rsi < 72 and accel > 0):

            confidence = self._score(2, abs(m5['momentum'] + m15['momentum']) / 2, True, rsi, vol_1h,
                                     (m5['strength'] + m15['strength']) / 2,
                                     'momentum_long', vol_ratio)
            if confidence >= 55:
                signal = self._build_signal(pair, 'long', current_price, confidence, vol_1h,
                    m5, m15, m1h, rsi,
                    f"Momentum long: 5m {m5['momentum']:+.2f}%, 15m {m15['momentum']:+.2f}%, "
                    f"accelerating, RSI {rsi:.0f}")

        # 3. BREAKOUT LONG: price testing 1h high
        if not signal and (
            price_position > 85 and
            m5['momentum'] > 0.06 and
            m15['momentum'] > 0.08 and
            range_1h > 0.25 and
            50 < rsi < 78):

            confidence = self._score(2, abs(m5['momentum']), ema_cross == 'bullish', rsi, vol_1h,
                                     m5['strength'], 'breakout_long', vol_ratio)
            if confidence >= 55:
                signal = self._build_signal(pair, 'long', current_price, confidence, vol_1h,
                    m5, m15, m1h, rsi,
                    f"Breakout long: at 1h high ({price_position:.0f}%), "
                    f"5m {m5['momentum']:+.2f}%, range {range_1h:.2f}%")

        # 4. REVERSAL LONG: oversold bounce
        if not signal and (
            m1h['momentum'] < -0.4 and
            m5['momentum'] > 0.06 and
            m15['momentum'] > -0.05 and
            20 < rsi < 42):

            confidence = self._score(1, abs(m5['momentum']), False, rsi, vol_1h,
                                     m5['strength'], 'reversal_long', vol_ratio)
            if confidence >= 58:
                signal = self._build_signal(pair, 'long', current_price, confidence, vol_1h,
                    m5, m15, m1h, rsi,
                    f"Reversal long: 1h dumped {m1h['momentum']:+.2f}%, "
                    f"5m bouncing {m5['momentum']:+.2f}%, RSI oversold {rsi:.0f}")

        # ============================================================
        # SHORT SETUPS
        # ============================================================

        # 5. TREND SHORT: all timeframes bearish
        if not signal and (
            m5['momentum'] < -0.05 and
            m15['momentum'] < -0.10 and
            m1h['momentum'] < -0.15 and
            ema_cross == 'bearish' and
            25 < rsi < 65):

            confidence = self._score(3, abs(m15['momentum']), True, rsi, vol_1h,
                                     (m5['strength'] + m15['strength'] + m1h['strength']) / 3,
                                     'trend_short', vol_ratio)
            if confidence >= 55:
                signal = self._build_signal(pair, 'short', current_price, confidence, vol_1h,
                    m5, m15, m1h, rsi,
                    f"Trend short: 5m {m5['momentum']:+.2f}%, 15m {m15['momentum']:+.2f}%, "
                    f"1h {m1h['momentum']:+.2f}%, EMA bear, RSI {rsi:.0f}")

        # 6. MOMENTUM SHORT: strong sell
        if not signal and (
            m5['momentum'] < -0.12 and
            m15['momentum'] < -0.10 and
            m1h['trend'] != 'up' and
            ema_cross == 'bearish' and
            28 < rsi < 70 and accel < 0):

            confidence = self._score(2, abs(m5['momentum'] + m15['momentum']) / 2, True, rsi, vol_1h,
                                     (m5['strength'] + m15['strength']) / 2,
                                     'momentum_short', vol_ratio)
            if confidence >= 55:
                signal = self._build_signal(pair, 'short', current_price, confidence, vol_1h,
                    m5, m15, m1h, rsi,
                    f"Momentum short: 5m {m5['momentum']:+.2f}%, 15m {m15['momentum']:+.2f}%, "
                    f"accelerating down, RSI {rsi:.0f}")

        # 7. BREAKOUT SHORT: price testing 1h low
        if not signal and (
            price_position < 15 and
            m5['momentum'] < -0.06 and
            m15['momentum'] < -0.08 and
            range_1h > 0.25 and
            22 < rsi < 50):

            confidence = self._score(2, abs(m5['momentum']), ema_cross == 'bearish', rsi, vol_1h,
                                     m5['strength'], 'breakout_short', vol_ratio)
            if confidence >= 55:
                signal = self._build_signal(pair, 'short', current_price, confidence, vol_1h,
                    m5, m15, m1h, rsi,
                    f"Breakout short: at 1h low ({price_position:.0f}%), "
                    f"5m {m5['momentum']:+.2f}%, range {range_1h:.2f}%")

        # ============================================================
        # ADVANCED SETUPS (require BB / VWAP / divergence indicators)
        # ============================================================

        # 8. REVERSAL SHORT: overbought rejection
        if not signal and (
            m1h['momentum'] > 0.4 and
            m5['momentum'] < -0.06 and
            m15['momentum'] > -0.05 and
            58 < rsi < 80):

            confidence = self._score(1, abs(m5['momentum']), False, rsi, vol_1h,
                                     m5['strength'], 'reversal_short', vol_ratio)
            if confidence >= 58:
                signal = self._build_signal(pair, 'short', current_price, confidence, vol_1h,
                    m5, m15, m1h, rsi,
                    f"Reversal short: 1h pumped {m1h['momentum']:+.2f}%, "
                    f"5m rejecting {m5['momentum']:+.2f}%, RSI overbought {rsi:.0f}")

        # 9. SQUEEZE LONG: BB compression -> expansion upward
        if not signal and bb and (
            bb['bandwidth'] < 1.5 and
            bb['pct_b'] > 0.85 and
            m5['momentum'] > 0.05 and
            vol_ratio > 1.2 and
            30 < rsi < 70):

            confidence = self._score(1, abs(m5['momentum']), ema_cross == 'bullish', rsi, vol_1h,
                                     m5['strength'], 'squeeze_long', vol_ratio)
            squeeze_bonus = min(8, (2.0 - bb['bandwidth']) * 5)
            vol_bonus = min(5, (vol_ratio - 1.0) * 8)
            confidence = min(100, confidence + squeeze_bonus + vol_bonus)
            if confidence >= 55:
                signal = self._build_signal(pair, 'long', current_price, confidence, vol_1h,
                    m5, m15, m1h, rsi,
                    f"Squeeze long: BB width {bb['bandwidth']:.1f}%, breaking up "
                    f"(%B {bb['pct_b']:.2f}), vol {vol_ratio:.1f}x, RSI {rsi:.0f}")

        # 10. SQUEEZE SHORT: BB compression -> expansion downward
        if not signal and bb and (
            bb['bandwidth'] < 1.5 and
            bb['pct_b'] < 0.15 and
            m5['momentum'] < -0.05 and
            vol_ratio > 1.2 and
            30 < rsi < 70):

            confidence = self._score(1, abs(m5['momentum']), ema_cross == 'bearish', rsi, vol_1h,
                                     m5['strength'], 'squeeze_short', vol_ratio)
            squeeze_bonus = min(8, (2.0 - bb['bandwidth']) * 5)
            vol_bonus = min(5, (vol_ratio - 1.0) * 8)
            confidence = min(100, confidence + squeeze_bonus + vol_bonus)
            if confidence >= 55:
                signal = self._build_signal(pair, 'short', current_price, confidence, vol_1h,
                    m5, m15, m1h, rsi,
                    f"Squeeze short: BB width {bb['bandwidth']:.1f}%, breaking down "
                    f"(%B {bb['pct_b']:.2f}), vol {vol_ratio:.1f}x, RSI {rsi:.0f}")

        # 11. MEAN REVERSION LONG: overextended below VWAP
        if not signal and bb and vwap and (
            vwap['deviation_pct'] < -0.3 and
            bb['pct_b'] < 0.20 and
            m5['momentum'] > -0.02 and
            m5['trend'] != 'down' and
            25 < rsi < 45 and
            accel > -0.02):

            deviation_str = abs(vwap['deviation_pct'])
            confidence = self._score(1, deviation_str * 0.5, False, rsi, vol_1h,
                                     0.3, 'mean_reversion_long', vol_ratio)
            reversion_bonus = min(8, abs(vwap['deviation_pct']) * 8)
            confidence = min(100, confidence + reversion_bonus)
            if confidence >= 57:
                signal = self._build_signal(pair, 'long', current_price, confidence, vol_1h,
                    m5, m15, m1h, rsi,
                    f"Mean reversion long: VWAP dev {vwap['deviation_pct']:+.2f}%, "
                    f"BB%B {bb['pct_b']:.2f}, RSI {rsi:.0f}, stabilizing")

        # 12. MEAN REVERSION SHORT: overextended above VWAP
        if not signal and bb and vwap and (
            vwap['deviation_pct'] > 0.3 and
            bb['pct_b'] > 0.80 and
            m5['momentum'] < 0.02 and
            m5['trend'] != 'up' and
            55 < rsi < 75 and
            accel < 0.02):

            deviation_str = abs(vwap['deviation_pct'])
            confidence = self._score(1, deviation_str * 0.5, False, rsi, vol_1h,
                                     0.3, 'mean_reversion_short', vol_ratio)
            reversion_bonus = min(8, abs(vwap['deviation_pct']) * 8)
            confidence = min(100, confidence + reversion_bonus)
            if confidence >= 57:
                signal = self._build_signal(pair, 'short', current_price, confidence, vol_1h,
                    m5, m15, m1h, rsi,
                    f"Mean reversion short: VWAP dev {vwap['deviation_pct']:+.2f}%, "
                    f"BB%B {bb['pct_b']:.2f}, RSI {rsi:.0f}, stalling")

        # 13. DIVERGENCE LONG: bullish RSI divergence
        if not signal and divergence == 'bullish' and (
            m1h['trend'] == 'down' and
            m5['momentum'] > 0.02 and
            25 < rsi < 50 and
            price_position < 40):

            confidence = self._score(1, abs(m5['momentum']), ema_cross == 'bullish', rsi, vol_1h,
                                     m5['strength'], 'divergence_long', vol_ratio)
            confidence = min(100, confidence + 6)
            if confidence >= 58:
                signal = self._build_signal(pair, 'long', current_price, confidence, vol_1h,
                    m5, m15, m1h, rsi,
                    f"Divergence long: bullish RSI divergence, 1h {m1h['trend']}, "
                    f"5m turning up {m5['momentum']:+.2f}%, RSI {rsi:.0f}")

        # 14. DIVERGENCE SHORT: bearish RSI divergence
        if not signal and divergence == 'bearish' and (
            m1h['trend'] == 'up' and
            m5['momentum'] < -0.02 and
            50 < rsi < 75 and
            price_position > 60):

            confidence = self._score(1, abs(m5['momentum']), ema_cross == 'bearish', rsi, vol_1h,
                                     m5['strength'], 'divergence_short', vol_ratio)
            confidence = min(100, confidence + 6)
            if confidence >= 58:
                signal = self._build_signal(pair, 'short', current_price, confidence, vol_1h,
                    m5, m15, m1h, rsi,
                    f"Divergence short: bearish RSI divergence, 1h {m1h['trend']}, "
                    f"5m turning down {m5['momentum']:+.2f}%, RSI {rsi:.0f}")

        return signal

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def _score(self, tf_alignment, momentum_mag, ema_confirm,
               rsi, vol, trend_strength, setup, vol_ratio=1.0) -> float:
        """Score signal 0-100."""
        score = 0

        # Timeframe alignment (max 30)
        score += tf_alignment * 10

        # Momentum magnitude (max 20)
        score += min(20, momentum_mag * 35)

        # EMA confirmation (max 10)
        if ema_confirm:
            score += 10

        # RSI sweet spot (max 15)
        if 'long' in setup:
            if 35 <= rsi <= 55:
                score += 15
            elif 25 <= rsi <= 65:
                score += 8
        elif 'short' in setup:
            if 45 <= rsi <= 65:
                score += 15
            elif 35 <= rsi <= 75:
                score += 8

        # Trend consistency (max 15)
        score += trend_strength * 15

        # Volume confirmation bonus (max 5)
        if vol_ratio > 1.3:
            score += 5
        elif vol_ratio > 1.1:
            score += 2

        # Volatility penalty
        if vol > 5:
            score -= min(10, (vol - 5) * 3)
        elif vol < 0.05:
            score -= 5

        return max(0, min(100, score))

    # ------------------------------------------------------------------
    # Build signal
    # ------------------------------------------------------------------

    def _build_signal(self, pair, side, price, confidence, vol,
                      m5, m15, m1h, rsi, reasoning) -> Dict:
        leverage = self._pick_leverage(confidence, vol)
        stop = self._pick_stop_loss(vol, leverage)
        tp = self._pick_take_profit(vol, leverage)

        return {
            'pair': pair,
            'market': pair,
            'action': 'BUY' if side == 'long' else 'SELL',
            'side': side,
            'entry_price': price,
            'leverage': leverage,
            'stop_loss': stop,
            'take_profit': tp,
            'stop_loss_pct': stop,
            'take_profit_pct': tp,
            'confidence': confidence / 100.0,
            'momentum': m15.get('momentum', 0) / 100.0,
            'volatility': vol,
            'trend': m1h.get('trend', 'neutral'),
            'rsi': rsi,
            'reasoning': reasoning,
            'timeframes': {
                '5m': m5.get('momentum', 0),
                '15m': m15.get('momentum', 0),
                '1h': m1h.get('momentum', 0),
            }
        }

    def _pick_leverage(self, confidence: float, vol: float) -> int:
        from hedge_fund_config import AVANTIS_MAX_LEVERAGE
        if confidence >= 75 and vol < 3:
            raw = 20
        elif confidence >= 65 and vol < 5:
            raw = 15
        else:
            raw = 10
        return min(raw, AVANTIS_MAX_LEVERAGE)

    def _pick_stop_loss(self, vol: float, leverage: int) -> float:
        from hedge_fund_config import AVANTIS_DEFAULT_SL
        # Wide stop loss — let trades breathe at 20x leverage
        # Higher leverage = tighter SL, lower leverage = wider SL
        if leverage >= 20:
            return AVANTIS_DEFAULT_SL  # 50%
        elif leverage >= 15:
            return AVANTIS_DEFAULT_SL * 0.8  # 40%
        else:
            return AVANTIS_DEFAULT_SL * 0.6  # 30%

    def _pick_take_profit(self, vol: float, leverage: int) -> float:
        from hedge_fund_config import AVANTIS_DEFAULT_TP
        # Wide take profit — 2:1 reward/risk at 20x leverage
        # Higher leverage = bigger TP target
        if leverage >= 20:
            return AVANTIS_DEFAULT_TP  # 100%
        elif leverage >= 15:
            return AVANTIS_DEFAULT_TP * 0.75  # 75%
        else:
            return AVANTIS_DEFAULT_TP * 0.5  # 50%


if __name__ == "__main__":
    print("=" * 60)
    print("AVANTIS SIGNALS v2 TEST")
    print("=" * 60)

    signals = AvantisSignals()

    print(f"\nTotal Avantis markets: {len(AVANTIS_MARKETS)}")
    by_cat = {}
    for p, info in AVANTIS_MARKETS.items():
        cat = info['cat']
        by_cat[cat] = by_cat.get(cat, 0) + 1
    for cat, count in sorted(by_cat.items()):
        print(f"  {cat}: {count}")

    print(f"\nScannable (have price data): {len(signals.scannable)}")
    print(f"\n[TEST] Scanning for trading opportunities...")
    opportunities = signals.scan_opportunities()

    if opportunities:
        print(f"\nFound {len(opportunities)} signals:\n")
        for i, opp in enumerate(opportunities, 1):
            print(f"{i}. {opp['pair']} {opp['side'].upper()} @${opp['entry_price']:,.2f}")
            print(f"   Leverage: {opp['leverage']}x | SL: {opp['stop_loss_pct']:.1f}% | TP: {opp['take_profit_pct']:.1f}%")
            print(f"   Confidence: {opp['confidence']:.0%} | {opp['reasoning']}")
            tf = opp.get('timeframes', {})
            print(f"   Charts: 5m {tf.get('5m', 0):+.2f}% | 15m {tf.get('15m', 0):+.2f}% | 1h {tf.get('1h', 0):+.2f}%")
            print()
    else:
        print("\nNo signals at this time")
