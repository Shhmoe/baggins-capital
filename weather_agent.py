"""
Weather Betting Module v2.0 - Hedge Fund Agent
Self-improving weather prediction market trader for Polymarket.

v2.0 Changes:
- 4 new weather APIs (OpenWeatherMap, Visual Crossing, Weatherbit, Pirate Weather)
- Historical temperature baselines (2-year Open-Meteo archive)
- Near-certainty filter (skip 95%+ markets)
- Same-day filter (skip markets resolving in <6h)
- Scaled bet sizing by confidence
- NOAA fix (User-Agent + retry + date matching)
- YES+NO bidirectional edge logging
- Bet execution verification via Bankr

Components:
- WeatherScanner: Finds daily temperature markets on Polymarket
- WeatherForecaster: Gets forecasts from up to 7 weather APIs
- HistoricalBaseline: 2-year temp stats for sigma calibration
- CredibilityEngine: Weights sources by per-city accuracy history
- EdgeCalculator: Compares our probability vs market price
- WeatherHeartbeat: 48h AI review of patterns and strategy
- WeatherAgent: Orchestrator called by main loop
"""

import os
import re
import json
import math
import time
import requests
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

from hedge_fund_config import (
    WEATHER_BET_SIZES, WEATHER_MAX_DAILY_BETS, WEATHER_MIN_CONFIDENCE,
    WEATHER_MIN_EDGE, WEATHER_CONVICTION_PROB, WEATHER_MAX_CONCURRENT, POLYMARKET_GAMMA_URL,
)


# ============================================================
# City coordinate lookup (for weather APIs)
# ============================================================
CITY_COORDS = {
    # Only cities with active Polymarket weather markets
    # New cities get added here when Polymarket lists them
    "new york": {"lat": 40.7128, "lon": -74.0060, "country": "US", "noaa": True},
    "nyc": {"lat": 40.7128, "lon": -74.0060, "country": "US", "noaa": True},
    "chicago": {"lat": 41.8781, "lon": -87.6298, "country": "US", "noaa": True},
    "dallas": {"lat": 32.7767, "lon": -96.7970, "country": "US", "noaa": True},
    "miami": {"lat": 25.7617, "lon": -80.1918, "country": "US", "noaa": True},
    "atlanta": {"lat": 33.7490, "lon": -84.3880, "country": "US", "noaa": True},
    "seattle": {"lat": 47.6062, "lon": -122.3321, "country": "US", "noaa": True},
    # International cities RE-ENABLED v5.0 — YES logic should fix 0W/8L record
    # (0.9F avg forecast error = most accurate, but were all NO bets = wrong side)
    "london": {"lat": 51.5074, "lon": -0.1278, "country": "UK", "noaa": False},
    "paris": {"lat": 48.8566, "lon": 2.3522, "country": "FR", "noaa": False},
    "seoul": {"lat": 37.5665, "lon": 126.9780, "country": "KR", "noaa": False},
    "sao paulo": {"lat": -23.5505, "lon": -46.6333, "country": "BR", "noaa": False},
    "toronto": {"lat": 43.6532, "lon": -79.3832, "country": "CA", "noaa": False},
    "tokyo": {"lat": 35.6762, "lon": 139.6503, "country": "JP", "noaa": False},
    "sydney": {"lat": -33.8688, "lon": 151.2093, "country": "AU", "noaa": False},
    "berlin": {"lat": 52.5200, "lon": 13.4050, "country": "DE", "noaa": False},
    "buenos aires": {"lat": -34.6037, "lon": -58.3816, "country": "AR", "noaa": False},
    "ankara": {"lat": 39.9334, "lon": 32.8597, "country": "TR", "noaa": False},
    "wellington": {"lat": -41.2866, "lon": 174.7756, "country": "NZ", "noaa": False},
    "lucknow": {"lat": 26.8467, "lon": 80.9462, "country": "IN", "noaa": False},
    "munich": {"lat": 48.1351, "lon": 11.5820, "country": "DE", "noaa": False},
}

# Sorted by length descending to prevent substring matching (e.g. "la" in "dallas")
CITY_NAMES_SORTED = sorted(CITY_COORDS.keys(), key=len, reverse=True)


# ============================================================
# WeatherScanner
# ============================================================
class WeatherScanner:
    """Scan Polymarket for daily temperature markets."""

    GAMMA_URL = POLYMARKET_GAMMA_URL + "/events"
    KEYWORDS = ["highest temperature", "temperature", "temp", "weather forecast", "high temp", "low temp", "degrees"]

    def scan_weather_markets(self):
        """Find open temperature prediction markets on Polymarket.
        Returns flat list of sub-market dicts.
        Filters: skip same-day markets (<6h), skip expired markets.
        """
        all_markets = []
        try:
            for offset in range(0, 200, 50):
                url = f"{self.GAMMA_URL}?closed=false&limit=50&offset={offset}&tag_id=84"
                resp = requests.get(url, timeout=15)
                if resp.status_code != 200:
                    print(f"[WEATHER ANALYST] Gamma API error: {resp.status_code}")
                    break

                events = resp.json()
                if not events:
                    break

                for event in events:
                    title = event.get("title", "")
                    title_lower = title.lower()

                    is_weather = False
                    for kw in self.KEYWORDS:
                        if re.search(r'\b' + re.escape(kw) + r'\b', title_lower):
                            is_weather = True
                            break

                    if not is_weather:
                        continue

                    city = self._parse_city(title_lower)
                    if not city:
                        print(f"[WEATHER ANALYST] Could not parse city from: {title[:60]}")
                        continue

                    market_date = self._parse_date(title)
                    if not market_date:
                        print(f"[WEATHER ANALYST] Could not parse date from: {title[:60]}")
                        continue

                    # v2.0: Skip markets resolving in <6h (same-day already decided)
                    hours_until = (market_date - datetime.now().date()).days * 24
                    if hours_until < 6:
                        if hours_until >= 0:
                            print(f"[WEATHER ANALYST] [SKIP] Same-day market (<6h): {title[:60]}")
                        continue
                    if hours_until > 30:
                        continue

                    sub_markets = event.get("markets", [])
                    for sm in sub_markets:
                        range_str = sm.get("groupItemTitle", "")
                        temp_range = self._parse_temp_range(range_str)
                        if not temp_range:
                            continue

                        prices_str = sm.get("outcomePrices", "[]")
                        try:
                            prices = json.loads(prices_str)
                            yes_price = float(prices[0])
                            no_price = float(prices[1])
                        except (json.JSONDecodeError, IndexError, ValueError):
                            continue

                        if yes_price <= 0 or yes_price >= 1:
                            continue

                        all_markets.append({
                            "event_id": event.get("id"),
                            "event_title": title,
                            "market_id": sm.get("id"),
                            "market_title": sm.get("question", title),
                            "group_title": range_str,
                            "city": city,
                            "market_date": market_date.isoformat(),
                            "temp_range": temp_range,
                            "yes_price": yes_price,
                            "no_price": no_price,
                            "hours_until": hours_until,
                        })

            print(f"[WEATHER ANALYST] Scanned Polymarket: {len(all_markets)} temperature sub-markets found")

        except Exception as e:
            print(f"[WEATHER ANALYST] Scan error: {e}")

        return all_markets

    def _parse_city(self, title_lower):
        """Extract city name from market title using word boundary matching."""
        for city in CITY_NAMES_SORTED:
            if re.search(r'\b' + re.escape(city) + r'\b', title_lower):
                return city
        return None

    def _parse_date(self, title):
        """Extract date from title like 'February 19' or 'Feb 19'."""
        import calendar

        m = re.search(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})',
            title, re.IGNORECASE
        )
        if m:
            month_name = m.group(1)
            day = int(m.group(2))
            month_num = list(calendar.month_name).index(month_name.capitalize())
            year = datetime.now().year
            try:
                return datetime(year, month_num, day).date()
            except ValueError:
                pass

        m = re.search(
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})',
            title, re.IGNORECASE
        )
        if m:
            month_abbr = m.group(1)
            day = int(m.group(2))
            month_num = list(calendar.month_abbr).index(month_abbr.capitalize())
            year = datetime.now().year
            try:
                return datetime(year, month_num, day).date()
            except ValueError:
                pass

        m = re.search(r'(\d{4})-(\d{2})-(\d{2})', title)
        if m:
            try:
                return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
            except ValueError:
                pass

        return None

    def _parse_temp_range(self, range_str):
        """Parse temperature range from groupItemTitle."""
        if not range_str:
            return None

        s = range_str.strip()
        result = {"raw": s}

        if "F" in s.upper() or "FAHRENHEIT" in s.upper():
            result["unit"] = "F"
        elif "C" in s.upper() or "CELSIUS" in s.upper():
            result["unit"] = "C"
        else:
            result["unit"] = "F"

        m = re.search(r'(-?\d+)\s*[\xb0\u00b0]?\s*(?:deg|degrees?)?\s*(?:F|C)?\s+or\s+(?:below|less|lower)', s, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            result["low"] = val - 100
            result["high"] = val
            result["is_below"] = True
            return result

        m = re.search(r'(-?\d+)\s*[\xb0\u00b0]?\s*(?:deg|degrees?)?\s*(?:F|C)?\s+or\s+(?:above|more|higher)', s, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            result["low"] = val
            result["high"] = val + 100
            result["is_above"] = True
            return result

        m = re.search(r'(-?\d+)\s*[-\x96\u2013]\s*(-?\d+)', s)
        if m:
            lo = int(m.group(1))
            hi = int(m.group(2))
            result["low"] = min(lo, hi)
            result["high"] = max(lo, hi)
            return result

        m = re.search(r'(-?\d+)\s*[\xb0\u00b0]?\s*(?:deg|degrees?)?\s*(?:F|C)', s, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            result["low"] = val
            result["high"] = val
            return result

        return None


# ============================================================
# HistoricalBaseline (v2.0)
# ============================================================
class HistoricalBaseline:
    """Historical temperature statistics from Open-Meteo Archive API.
    Downloads 2 years of daily highs/lows per city and computes
    monthly mean + std_dev. Used to calibrate sigma in probability model.
    """

    ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

    def __init__(self, db_path='hedge_fund_performance.db'):
        self.db_path = db_path
        self._loaded = False

    def ensure_loaded(self):
        """Load historical data if not already in DB. Runs once (~52s for 35 cities)."""
        if self._loaded:
            return

        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM historical_temperatures")
            count = c.fetchone()[0]
            conn.close()
            if count >= 100:
                self._loaded = True
                print(f"[HISTORIAN] Already have {count} records in DB")
                return
        except Exception:
            pass

        print(f"[HISTORIAN] Bootstrapping 2-year temperature data for {len(CITY_COORDS)} cities...")
        self._bootstrap_all_cities()
        self._loaded = True

    def _bootstrap_all_cities(self):
        """Download 2 years of data for all cities."""
        end_date = datetime.now().date() - timedelta(days=2)
        start_date = end_date - timedelta(days=730)

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute("""
            CREATE TABLE IF NOT EXISTS historical_temperatures (
                city TEXT NOT NULL,
                month INTEGER NOT NULL,
                mean_high REAL NOT NULL,
                std_high REAL NOT NULL,
                mean_low REAL NOT NULL,
                std_low REAL NOT NULL,
                data_points INTEGER NOT NULL,
                PRIMARY KEY (city, month)
            )
        """)
        conn.commit()

        cities_done = 0
        seen_coords = set()
        unique_cities = []
        for city, coords in CITY_COORDS.items():
            key = (coords["lat"], coords["lon"])
            if key not in seen_coords:
                seen_coords.add(key)
                unique_cities.append((city, coords))

        for city, coords in unique_cities:
            try:
                resp = requests.get(self.ARCHIVE_URL, params={
                    "latitude": coords["lat"],
                    "longitude": coords["lon"],
                    "daily": "temperature_2m_max,temperature_2m_min",
                    "temperature_unit": "fahrenheit",
                    "timezone": "auto",
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                }, timeout=15)

                if resp.status_code != 200:
                    print(f"  [SKIP] {city}: HTTP {resp.status_code}")
                    time.sleep(1)
                    continue

                data = resp.json()
                daily = data.get("daily", {})
                dates = daily.get("time", [])
                highs = daily.get("temperature_2m_max", [])
                lows = daily.get("temperature_2m_min", [])

                if not dates or not highs:
                    print(f"  [SKIP] {city}: no data")
                    time.sleep(1)
                    continue

                monthly = {}
                for i, d in enumerate(dates):
                    month = int(d.split("-")[1])
                    if month not in monthly:
                        monthly[month] = {"highs": [], "lows": []}
                    if highs[i] is not None:
                        monthly[month]["highs"].append(highs[i])
                    if i < len(lows) and lows[i] is not None:
                        monthly[month]["lows"].append(lows[i])

                for month, vals in monthly.items():
                    if len(vals["highs"]) < 10:
                        continue
                    mean_h = sum(vals["highs"]) / len(vals["highs"])
                    std_h = (sum((x - mean_h) ** 2 for x in vals["highs"]) / len(vals["highs"])) ** 0.5
                    mean_l = sum(vals["lows"]) / len(vals["lows"]) if vals["lows"] else mean_h - 15
                    std_l = (sum((x - mean_l) ** 2 for x in vals["lows"]) / len(vals["lows"])) ** 0.5 if len(vals["lows"]) > 1 else std_h

                    c.execute("""
                        INSERT INTO historical_temperatures (city, month, mean_high, std_high, mean_low, std_low, data_points)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(city, month) DO UPDATE SET
                            mean_high = ?, std_high = ?, mean_low = ?, std_low = ?, data_points = ?
                    """, (city, month, mean_h, std_h, mean_l, std_l, len(vals["highs"]),
                          mean_h, std_h, mean_l, std_l, len(vals["highs"])))

                cities_done += 1
                if cities_done % 5 == 0:
                    conn.commit()
                    print(f"  [HISTORIAN] {cities_done}/{len(unique_cities)} cities loaded")

                time.sleep(1.5)

            except Exception as e:
                print(f"  [ERROR] {city}: {e}")
                time.sleep(1)

        conn.commit()
        conn.close()
        print(f"[HISTORIAN] Bootstrap complete: {cities_done} cities loaded")

    def get_stats(self, city, month):
        """Get historical mean + std for a city/month."""
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute(
                "SELECT mean_high, std_high FROM historical_temperatures WHERE city = ? AND month = ?",
                (city, month)
            )
            row = c.fetchone()
            conn.close()
            if row:
                return row[0], row[1]
        except Exception:
            pass
        return None, None


# ============================================================
# WeatherForecaster (v2.0: 7 sources)
# ============================================================
class WeatherForecaster:
    """Fetch forecasts from up to 7 weather APIs."""

    def __init__(self):
        self.weatherapi_key = os.getenv("WEATHERAPI_KEY", "").strip()
        self.openweathermap_key = os.getenv("OPENWEATHERMAP_KEY", "").strip()
        self.visualcrossing_key = os.getenv("VISUALCROSSING_KEY", "").strip()
        self.weatherbit_key = os.getenv("WEATHERBIT_KEY", "").strip()
        self.pirateweather_key = os.getenv("PIRATEWEATHER_KEY", "").strip()
        self._api_cooldowns = {}  # Per-API cooldown tracking for 429s

    def get_forecasts(self, city, target_date):
        """Get forecasts from all available sources for a city/date."""
        coords = CITY_COORDS.get(city)
        if not coords:
            print(f"[WEATHER ANALYST] Unknown city coordinates: {city}")
            return []

        forecasts = []

        # Source 1: Open-Meteo (free, no key, global)
        om = self._fetch_open_meteo(coords, target_date)
        if om:
            forecasts.append(om)

        # Source 2: NOAA/NWS (US only, free) -- v2.0 fixed
        if coords.get("noaa"):
            noaa = self._fetch_noaa(coords, target_date)
            if noaa:
                forecasts.append(noaa)

        import time as _t; _t.sleep(1.5)  # Rate limit spacing
        # Source 3: WeatherAPI.com (key required)
        if self.weatherapi_key:
            wa = self._fetch_weatherapi(coords, city, target_date)
            if wa:
                forecasts.append(wa)

        _t.sleep(1.5)
        # Source 4: OpenWeatherMap (v2.0)
        if self.openweathermap_key:
            owm = self._fetch_openweathermap(coords, target_date)
            if owm:
                forecasts.append(owm)

        # Source 5: Visual Crossing (v2.0)
        _t.sleep(1.5)
        if self.visualcrossing_key:
            vc = self._fetch_visualcrossing(coords, target_date)
            if vc:
                forecasts.append(vc)

        _t.sleep(1.5)
        # Source 6: Weatherbit (v2.0)
        if self.weatherbit_key:
            wb = self._fetch_weatherbit(coords, target_date)
            if wb:
                forecasts.append(wb)

        _t.sleep(1.5)
        # Source 7: Pirate Weather (v2.0)
        if self.pirateweather_key:
            pw = self._fetch_pirateweather(coords, target_date)
            if pw:
                forecasts.append(pw)

        print(f"[WEATHER ANALYST] {city}: {len(forecasts)} forecast sources")
        for f in forecasts:
            print(f"  {f['source']}: high={f['high_temp']:.1f}{f['unit']}, low={f['low_temp']:.1f}{f['unit']}")

        return forecasts

    def _fetch_open_meteo(self, coords, target_date):
        """Open-Meteo: free, no key, global coverage."""
        try:
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "daily": "temperature_2m_max,temperature_2m_min",
                "temperature_unit": "fahrenheit",
                "timezone": "auto",
                "start_date": target_date.isoformat(),
                "end_date": target_date.isoformat(),
            }
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                print(f"[WEATHER ANALYST] Open-Meteo error: {resp.status_code}")
                return None

            data = resp.json()
            daily = data.get("daily", {})
            highs = daily.get("temperature_2m_max", [])
            lows = daily.get("temperature_2m_min", [])

            if not highs or not lows:
                return None

            return {
                "source": "open_meteo",
                "high_temp": float(highs[0]),
                "low_temp": float(lows[0]),
                "unit": "F",
            }
        except Exception as e:
            print(f"[WEATHER ANALYST] Open-Meteo error: {e}")
            return None

    def _fetch_noaa(self, coords, target_date):
        """NOAA/NWS: US only, free, 2-step lookup. v2.0: User-Agent + retry + date fix."""
        headers = {"User-Agent": "HedgeFundWeatherAgent/2.0 (weather-betting, contact@example.com)"}
        target_str = target_date.isoformat()

        for attempt in range(2):
            try:
                points_url = f"https://api.weather.gov/points/{coords['lat']},{coords['lon']}"
                resp = requests.get(points_url, headers=headers, timeout=10)
                if resp.status_code != 200:
                    if attempt == 0:
                        time.sleep(2)
                        continue
                    return None

                grid_data = resp.json()
                forecast_url = grid_data.get("properties", {}).get("forecast")
                if not forecast_url:
                    return None

                resp = requests.get(forecast_url, headers=headers, timeout=10)
                if resp.status_code != 200:
                    if attempt == 0:
                        time.sleep(2)
                        continue
                    return None

                periods = resp.json().get("properties", {}).get("periods", [])

                high_temp = None
                low_temp = None

                for period in periods:
                    start = period.get("startTime", "")
                    period_date = start[:10]  # "2026-02-21T06:00:00-05:00" -> "2026-02-21"
                    if period_date != target_str:
                        continue
                    temp = period.get("temperature")
                    is_day = period.get("isDaytime", True)
                    if temp is not None:
                        if is_day and high_temp is None:
                            high_temp = float(temp)
                        elif not is_day and low_temp is None:
                            low_temp = float(temp)

                if high_temp is None:
                    if attempt == 0:
                        time.sleep(2)
                        continue
                    return None

                return {
                    "source": "noaa",
                    "high_temp": high_temp,
                    "low_temp": low_temp if low_temp is not None else high_temp - 15,
                    "unit": "F",
                }
            except Exception as e:
                if attempt == 0:
                    time.sleep(2)
                    continue
                print(f"[WEATHER ANALYST] NOAA error: {e}")
                return None

        return None

    def _fetch_weatherapi(self, coords, city, target_date):
        """WeatherAPI.com: key required, global."""
        try:
            import time as _time
            cooldown_until = self._api_cooldowns.get('weatherapi', 0)
            if _time.time() < cooldown_until:
                print(f"[WEATHER ANALYST] WeatherAPI on cooldown, skipping")
                return None
            url = "https://api.weatherapi.com/v1/forecast.json"
            params = {
                "key": self.weatherapi_key,
                "q": f"{coords['lat']},{coords['lon']}",
                "dt": target_date.isoformat(),
            }
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                if resp.status_code == 429:
                    self._api_cooldowns['weatherapi'] = _time.time() + 300
                    print(f"[WEATHER ANALYST] WeatherAPI 429 -- 5min cooldown set")
                    return None
                print(f"[WEATHER ANALYST] WeatherAPI error: {resp.status_code}")
                return None

            data = resp.json()
            days = data.get("forecast", {}).get("forecastday", [])
            if not days:
                return None

            day_data = days[0].get("day", {})
            high_f = day_data.get("maxtemp_f")
            low_f = day_data.get("mintemp_f")

            if high_f is None:
                return None

            return {
                "source": "weatherapi",
                "high_temp": float(high_f),
                "low_temp": float(low_f) if low_f is not None else float(high_f) - 15,
                "unit": "F",
            }
        except Exception as e:
            print(f"[WEATHER ANALYST] WeatherAPI error: {e}")
            return None

    def _fetch_openweathermap(self, coords, target_date):
        """OpenWeatherMap: 5-day/3-hour forecast, extract max/min for target date."""
        try:
            url = "https://api.openweathermap.org/data/2.5/forecast"
            params = {
                "lat": coords["lat"],
                "lon": coords["lon"],
                "appid": self.openweathermap_key,
                "units": "imperial",
            }
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                print(f"[WEATHER ANALYST] OpenWeatherMap error: {resp.status_code}")
                return None

            data = resp.json()
            target_str = target_date.isoformat()
            temps = []

            for item in data.get("list", []):
                dt_txt = item.get("dt_txt", "")
                if dt_txt.startswith(target_str):
                    main = item.get("main", {})
                    t = main.get("temp")
                    if t is not None:
                        temps.append(float(t))

            if not temps:
                return None

            return {
                "source": "openweathermap",
                "high_temp": max(temps),
                "low_temp": min(temps),
                "unit": "F",
            }
        except Exception as e:
            print(f"[WEATHER ANALYST] OpenWeatherMap error: {e}")
            return None

    def _fetch_visualcrossing(self, coords, target_date):
        """Visual Crossing: Timeline API, direct daily max/min. With 429 retry."""
        try:
            import time as _time
            cooldown_until = self._api_cooldowns.get('visualcrossing', 0)
            if _time.time() < cooldown_until:
                print(f"[WEATHER ANALYST] Visual Crossing on cooldown, skipping")
                return None

            loc = f"{coords['lat']},{coords['lon']}"
            url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{loc}/{target_date.isoformat()}"
            params = {
                "unitGroup": "us",
                "key": self.visualcrossing_key,
                "include": "days",
                "contentType": "json",
            }
            delays = [2, 5]
            resp = None
            for attempt in range(3):
                resp = requests.get(url, params=params, timeout=10)
                if resp.status_code == 429:
                    if attempt < 2:
                        print(f"[WEATHER ANALYST] Visual Crossing 429 rate limit, retry in {delays[attempt]}s")
                        _time.sleep(delays[attempt])
                        continue
                    else:
                        self._api_cooldowns['visualcrossing'] = _time.time() + 300
                        print(f"[WEATHER ANALYST] Visual Crossing 429 — 5min cooldown set")
                        return None
                break

            if resp.status_code != 200:
                print(f"[WEATHER ANALYST] Visual Crossing error: {resp.status_code}")
                return None

            data = resp.json()
            days = data.get("days", [])
            if not days:
                return None

            day = days[0]
            high_f = day.get("tempmax")
            low_f = day.get("tempmin")

            if high_f is None:
                return None

            return {
                "source": "visualcrossing",
                "high_temp": float(high_f),
                "low_temp": float(low_f) if low_f is not None else float(high_f) - 15,
                "unit": "F",
            }
        except Exception as e:
            print(f"[WEATHER ANALYST] Visual Crossing error: {e}")
            return None

    def _fetch_weatherbit(self, coords, target_date):
        """Weatherbit: daily forecast API, direct max/min. With 429 retry."""
        try:
            import time as _time
            cooldown_until = self._api_cooldowns.get('weatherbit', 0)
            if _time.time() < cooldown_until:
                print(f"[WEATHER ANALYST] Weatherbit on cooldown, skipping")
                return None

            url = "https://api.weatherbit.io/v2.0/forecast/daily"
            params = {
                "lat": coords["lat"],
                "lon": coords["lon"],
                "key": self.weatherbit_key,
                "units": "I",
                "days": 5,
            }
            delays = [2, 5]
            resp = None
            for attempt in range(3):
                resp = requests.get(url, params=params, timeout=10)
                if resp.status_code == 429:
                    if attempt < 2:
                        print(f"[WEATHER ANALYST] Weatherbit 429 rate limit, retry in {delays[attempt]}s")
                        _time.sleep(delays[attempt])
                        continue
                    else:
                        self._api_cooldowns['weatherbit'] = _time.time() + 300
                        print(f"[WEATHER ANALYST] Weatherbit 429 — 5min cooldown set")
                        return None
                break

            if resp.status_code != 200:
                print(f"[WEATHER ANALYST] Weatherbit error: {resp.status_code}")
                return None

            data = resp.json()
            target_str = target_date.isoformat()

            for day in data.get("data", []):
                if day.get("valid_date") == target_str:
                    high_f = day.get("max_temp")
                    low_f = day.get("min_temp")
                    if high_f is not None:
                        return {
                            "source": "weatherbit",
                            "high_temp": float(high_f),
                            "low_temp": float(low_f) if low_f is not None else float(high_f) - 15,
                            "unit": "F",
                        }
            return None
        except Exception as e:
            print(f"[WEATHER ANALYST] Weatherbit error: {e}")
            return None

    def _fetch_pirateweather(self, coords, target_date):
        """Pirate Weather: Dark Sky compatible, daily temperatureHigh/Low."""
        try:
            import time as _time
            cooldown_until = self._api_cooldowns.get('pirateweather', 0)
            if _time.time() < cooldown_until:
                print(f"[WEATHER ANALYST] Pirate Weather on cooldown, skipping")
                return None
            url = f"https://api.pirateweather.net/forecast/{self.pirateweather_key}/{coords['lat']},{coords['lon']}"
            params = {"units": "us", "exclude": "currently,minutely,hourly,alerts"}
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 429:
                self._api_cooldowns['pirateweather'] = _time.time() + 300
                print(f"[WEATHER ANALYST] Pirate Weather 429 -- 5min cooldown set")
                return None
            if resp.status_code != 200:
                print(f"[WEATHER ANALYST] Pirate Weather error: {resp.status_code}")
                return None

            data = resp.json()
            target_str = target_date.isoformat()

            for day in data.get("daily", {}).get("data", []):
                day_time = day.get("time", 0)
                day_date = datetime.utcfromtimestamp(day_time).date().isoformat()
                if day_date == target_str:
                    high_f = day.get("temperatureHigh")
                    low_f = day.get("temperatureLow")
                    if high_f is not None:
                        return {
                            "source": "pirateweather",
                            "high_temp": float(high_f),
                            "low_temp": float(low_f) if low_f is not None else float(high_f) - 15,
                            "unit": "F",
                        }
            return None
        except Exception as e:
            print(f"[WEATHER ANALYST] Pirate Weather error: {e}")
            return None


# ============================================================
# CredibilityEngine (v2.0: historical baseline integration)
# ============================================================
class CredibilityEngine:
    """Weight forecast sources by per-city accuracy history."""

    def __init__(self, db_path='hedge_fund_performance.db'):
        self.db_path = db_path
        self.historical = HistoricalBaseline(db_path)

    def get_source_weight(self, city, source_name):
        """Get credibility weight for a source in a specific city. Default 1.0."""
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute(
                "SELECT credibility_weight FROM weather_sources WHERE city = ? AND source_name = ?",
                (city, source_name)
            )
            row = c.fetchone()
            conn.close()
            return row[0] if row else 1.0
        except Exception:
            return 1.0

    def get_weighted_forecast(self, city, forecasts):
        """Compute weighted average high temp from multiple forecast sources."""
        if not forecasts:
            return None, None, {}

        weights_used = {}
        weighted_sum = 0.0
        total_weight = 0.0
        temps = []

        for f in forecasts:
            source = f["source"]
            high = f["high_temp"]
            if f.get("unit") == "C":
                high = high * 9.0 / 5.0 + 32.0
            w = self.get_source_weight(city, source)

            # Phase 3: Bias correction (only after 10+ data points per source+city)
            bias = self._get_source_bias(city, source)
            if abs(bias) > 0.1:
                corrected_high = high - bias
                print(f"  [BIAS] {source}@{city}: {high:.1f}F -> {corrected_high:.1f}F (bias={bias:+.1f}F)")
                high = corrected_high

            weights_used[source] = w
            weighted_sum += high * w
            total_weight += w
            temps.append(high)

        if total_weight == 0:
            return None, None, {}

        weighted_mean = weighted_sum / total_weight
        spread = max(temps) - min(temps) if len(temps) > 1 else 0.0

        return weighted_mean, spread, weights_used

    def compute_probability(self, weighted_mean, spread, temp_range, city=None, target_date=None):
        """Compute probability that actual temp falls in the given range.
        v2.0: Uses historical std as sigma floor for real weather variance.
        """
        # v4.0: Sigma based on forecast agreement
        # When sources agree closely (spread <= 1F), tighter sigma = more confident
        # When spread is large, wider sigma = less confident
        if spread <= 1.0:
            sigma = 1.8  # Sources agree tightly — confident in forecast
        elif spread <= 2.0:
            sigma = 2.2  # Moderate agreement
        elif spread <= 3.0:
            sigma = 2.8  # Some disagreement
        else:
            sigma = max(3.0, spread / 2.0)  # Wide disagreement — low confidence

        # v2.0: Historical baseline sigma integration
        if city and target_date:
            self.historical.ensure_loaded()
            month = target_date.month if hasattr(target_date, 'month') else int(str(target_date).split('-')[1])
            hist_mean, hist_std = self.historical.get_stats(city, month)
            if hist_mean is not None and hist_std is not None:
                sigma = max(sigma, hist_std * 0.7)
                deviation = abs(weighted_mean - hist_mean)
                if deviation > 2 * hist_std:
                    sigma *= 1.3
                    print(f"  [HISTORIAN] {city}: forecast {weighted_mean:.1f}F is {deviation:.1f}F from seasonal norm {hist_mean:.1f}F -- widening sigma")

        low = temp_range.get("low", -999)
        high = temp_range.get("high", 999)

        if temp_range.get("unit") == "C":
            low = low * 9.0 / 5.0 + 32.0 if low > -500 else low
            high = high * 9.0 / 5.0 + 32.0 if high < 500 else high

        def norm_cdf(x, mu, sig):
            return 0.5 * (1 + math.erf((x - mu) / (sig * math.sqrt(2))))

        if temp_range.get("is_below"):
            prob = norm_cdf(high + 0.5, weighted_mean, sigma)
        elif temp_range.get("is_above"):
            prob = 1.0 - norm_cdf(low - 0.5, weighted_mean, sigma)
        else:
            prob = norm_cdf(high + 0.5, weighted_mean, sigma) - norm_cdf(low - 0.5, weighted_mean, sigma)

        return max(0.001, min(0.999, prob))

    def update_after_resolution(self, city, source_name, predicted_high, actual_high, bet_id=None, market_date=None):
        """Update source credibility based on prediction error.
        v3.0: signed error tracking, EMA bias, granular deltas, inverse-value logic.
        v3.1: Also logs to source_prediction_log for per-source per-bet tracking.
        """
        error = abs(predicted_high - actual_high)
        signed_error = predicted_high - actual_high
        direction = "high" if predicted_high > actual_high else ("low" if predicted_high < actual_high else "exact")

        # Granular weight deltas based on error magnitude
        if error <= 0.5:
            delta = 0.03
        elif error <= 1.0:
            delta = 0.02
        elif error <= 1.5:
            delta = 0.01
        elif error <= 2.0:
            delta = 0.005
        elif error <= 3.0:
            delta = 0.0
        elif error <= 4.0:
            delta = -0.01
        elif error <= 5.0:
            delta = -0.03
        else:
            delta = -0.05

        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()

            # Fetch existing row for EMA bias calculation
            c.execute("SELECT avg_bias, consecutive_same_direction, avg_error, bias_consistency FROM weather_sources WHERE city = ? AND source_name = ?",
                      (city, source_name))
            existing = c.fetchone()
            old_bias = existing[0] if existing and existing[0] is not None else 0.0
            consec = existing[1] if existing else 0
            old_avg_error = existing[2] if existing else 0.0
            old_consistency = existing[3] if existing and existing[3] is not None else 0.0

            # EMA bias update: smooth toward recent errors
            new_bias = old_bias * 0.8 + signed_error * 0.2

            # Consistency score: how predictable is this source's bias?
            # EMA of |signed_error - avg_bias| -- low = consistent, high = erratic
            # A source always +2.7F warm has low variance (high consistency)
            bias_deviation = abs(signed_error - old_bias)
            new_consistency = old_consistency * 0.7 + bias_deviation * 0.3 if old_consistency > 0 else bias_deviation

            # Inverse value logic: consistently biased sources are useful after correction
            # If source has 5+ same-direction errors and avg_error > 3.0, give moderate weight
            # instead of near-zero, because bias correction makes them valuable
            effective_delta = delta
            if consec >= 5 and old_avg_error > 3.0 and delta < 0:
                effective_delta = max(delta, -0.005)  # Limit penalty
                print(f"  [INVERSE-VALUE] {source_name}@{city}: consistent bias ({consec} same dir) -- limiting penalty")

            bias_dir = "high" if new_bias > 0.5 else ("low" if new_bias < -0.5 else "neutral")

            c.execute("""
                INSERT INTO weather_sources (city, source_name, credibility_weight, total_predictions, accurate_predictions, avg_error, last_updated, last_error_direction, consecutive_same_direction, avg_bias, bias_direction, bias_consistency)
                VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(city, source_name) DO UPDATE SET
                    credibility_weight = MAX(0.3, MIN(1.8, credibility_weight + ?)),
                    total_predictions = total_predictions + 1,
                    accurate_predictions = accurate_predictions + ?,
                    avg_error = (avg_error * total_predictions + ?) / (total_predictions + 1),
                    last_updated = ?,
                    last_error_direction = ?,
                    consecutive_same_direction = CASE
                        WHEN last_error_direction = ? THEN consecutive_same_direction + 1
                        ELSE 1
                    END,
                    avg_bias = ?,
                    bias_direction = ?,
                    bias_consistency = ?
            """, (
                city, source_name,
                max(0.3, min(1.8, 1.0 + effective_delta)),
                1 if error <= 2 else 0,
                error,
                datetime.now().isoformat(),
                direction,
                1,
                new_bias,
                bias_dir,
                new_consistency,
                effective_delta,
                1 if error <= 2 else 0,
                error,
                datetime.now().isoformat(),
                direction,
                direction,
                new_bias,
                bias_dir,
                new_consistency,
            ))

            # Inverse value floor: ensure consistently biased sources keep usable weight
            if consec >= 5 and old_avg_error > 3.0:
                c.execute("""
                    UPDATE weather_sources SET credibility_weight = MAX(credibility_weight, 0.6)
                    WHERE city = ? AND source_name = ?
                """, (city, source_name))

            conn.commit()

            # Log per-source prediction to source_prediction_log
            if bet_id is not None:
                try:
                    c.execute("""
                        INSERT INTO source_prediction_log
                            (bet_id, city, source_name, predicted_high, actual_high, error, bias, logged_at, market_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (bet_id, city, source_name, predicted_high, actual_high, error, signed_error,
                          datetime.now().strftime('%Y-%m-%d %H:%M:%S'), market_date))
                    conn.commit()
                except Exception as spl_err:
                    print(f"  [!] source_prediction_log error: {spl_err}")

            conn.close()

            consistency_label = "steady" if new_consistency < 1.5 else ("moderate" if new_consistency < 3.0 else "erratic")
            print(f"[AUDITOR] {source_name}@{city}: error={error:.1f}F ({direction}), delta={effective_delta:+.3f}, bias={new_bias:+.1f}F ({bias_dir}), consistency={new_consistency:.1f} ({consistency_label})")
        except Exception as e:
            print(f"[AUDITOR] Update error: {e}")

    def _get_source_bias(self, city, source_name):
        """Get bias correction for a source, scaled by confidence in the estimate.

        Correction scales with data:
        - 5+ predictions: 100% of avg_bias applied
        - 3-4 predictions: 75% applied
        - 2 predictions with consecutive same direction: 50% applied
        - <2 predictions: no correction (not enough data)

        A source always +3.1F warmer is GOLD after correction.
        """
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            c = conn.cursor()
            c.execute(
                "SELECT avg_bias, total_predictions, consecutive_same_direction "
                "FROM weather_sources WHERE city = ? AND source_name = ?",
                (city, source_name)
            )
            row = c.fetchone()
            conn.close()
            if not row or row[0] is None:
                return 0.0
            avg_bias, n, consec = row[0], row[1], row[2] or 0

            # Only correct meaningful biases (> 0.3F)
            if abs(avg_bias) < 0.3:
                return 0.0

            # Scale correction by confidence in the estimate
            if n >= 5:
                return avg_bias  # Full correction
            if n >= 3:
                return avg_bias * 0.75  # 75% correction
            if n >= 2 and consec >= 2:
                return avg_bias * 0.50  # 50% -- same direction twice in a row
            return 0.0
        except Exception:
            return 0.0


# ============================================================
# EdgeCalculator (v2.0: near-certainty filter + edge logging)
# ============================================================
class EdgeCalculator:
    """Calculate edge and confidence for weather bets."""

    def __init__(self, db_path='hedge_fund_performance.db'):
        self.db_path = db_path

    # US cities where our weather APIs are reliable
    # Block international cities - no NOAA coverage, poor win rate
    INTERNATIONAL_BLOCKED = {
        'london', 'paris', 'seoul', 'sao paulo', 'buenos aires',
        'ankara', 'munich', 'wellington', 'lucknow',
    }

    US_CITIES = {
        'chicago', 'miami', 'new york', 'nyc', 'seattle', 'los angeles',
        'houston', 'phoenix', 'dallas', 'denver', 'atlanta',
        'san francisco', 'boston', 'philadelphia', 'washington',
        'las vegas', 'portland', 'minneapolis', 'detroit',
        'nashville', 'charlotte', 'austin', 'orlando', 'tampa',
        'san diego', 'sacramento', 'kansas city', 'columbus',
        'indianapolis', 'memphis', 'baltimore', 'milwaukee',
        'new orleans', 'st louis', 'pittsburgh', 'cincinnati',
        'cleveland', 'salt lake', 'oklahoma city', 'raleigh',
    }

    def _is_domestic(self, city):
        """Check if city is domestic US (where our APIs are accurate)."""
        city_lower = city.lower().strip()
        for us_city in self.US_CITIES:
            if us_city in city_lower or city_lower in us_city:
                return True
        # Also check for Fahrenheit — US markets use F, international use C
        return False

    def evaluate(self, market, our_prob, spread, source_count, weights_used, weighted_mean):
        """Evaluate a single sub-market for edge and confidence.
        v3.0: International filter, recalibrated edge/confidence scoring.
        """
        yes_price = market["yes_price"]
        no_price = market["no_price"]
        city = market.get("city", "")

        # v5.0: All cities allowed — YES logic + pattern analyzer handle side selection
        # International had 0.9F avg error (most accurate) but 0W/8L on wrong side (NO)
        # Forecast-driven YES should fix this. Pattern analyzer monitors every 30h.

        # v2.0: Near-certainty filter
        if yes_price >= 0.95:
            print(f"  [SKIP] Near-certainty YES ({yes_price:.0%}): {market['group_title']}")
            return None
        if no_price >= 0.95:
            print(f"  [SKIP] Near-certainty NO ({no_price:.0%}): {market['group_title']}")
            return None

        # === v5.1: MARKET TYPE FILTER ===
        # THRESHOLD markets (above/below) are profitable: 60% WR, +$7.02
        # EXACT single-degree markets are catastrophic: 0% WR, -$34.00
        # BETWEEN X-Y markets only worth it at 10x+ payout (YES side, forecast in range)
        temp_range_data_check = market.get("temp_range_parsed", market.get("temp_range", {}))
        is_above_check = temp_range_data_check.get("is_above", False)
        is_below_check = temp_range_data_check.get("is_below", False)
        range_low_check = temp_range_data_check.get("low", -999)
        range_high_check = temp_range_data_check.get("high", 999)

        is_threshold = is_above_check or is_below_check
        is_exact_range = (range_low_check > -500 and range_high_check < 500 and
                         range_high_check - range_low_check <= 2)
        is_single_degree = (range_low_check > -500 and range_high_check < 500 and
                           range_high_check - range_low_check <= 1 and
                           not is_above_check and not is_below_check)

        if is_single_degree:
            # 0% WR across 10 bets, -$36. Block completely.
            print(f"  [SKIP] Single-degree market (0% WR historically): {market['group_title']}")
            return None

        if is_exact_range and not is_threshold:
            # Between X-Y markets: only allow if YES side is cheap enough for 10x+ payout
            if yes_price > 0.10:
                print(f"  [SKIP] Exact range market, YES not cheap enough for 10x ({yes_price:.0%}): {market['group_title']}")
                return None
            else:
                print(f"  [10X] Exact range market, YES at {yes_price:.0%} (10x+ potential): {market['group_title']}")

        if is_threshold:
            print(f"  [PREFERRED] Threshold market (above/below): {market['group_title']}")


        yes_edge = our_prob - yes_price
        no_edge = (1.0 - our_prob) - no_price

        # v4.0: Smart YES/NO selection
        # When forecast clusters near the range, prefer YES (higher payout, better odds)
        # When forecast is far from range, NO is the safe play
        temp_range_data = market.get("temp_range_parsed", market.get("temp_range", {}))
        range_low = temp_range_data.get("low", -999)
        range_high = temp_range_data.get("high", 999)
        is_above = temp_range_data.get("is_above", False)
        is_below = temp_range_data.get("is_below", False)

        # Convert Celsius ranges to Fahrenheit (weighted_mean is always in F)
        if temp_range_data.get("unit") == "C":
            if range_low > -500:
                range_low = range_low * 9.0 / 5.0 + 32.0
            if range_high < 500:
                range_high = range_high * 9.0 / 5.0 + 32.0

        # Check if weighted_mean is close to the range
        forecast_in_range = False
        forecast_near_range = False
        if is_above:
            forecast_in_range = weighted_mean >= range_low
            forecast_near_range = abs(weighted_mean - range_low) <= 2.0
        elif is_below:
            forecast_in_range = weighted_mean <= range_high
            forecast_near_range = abs(weighted_mean - range_high) <= 2.0
        elif range_low > -500 and range_high < 500:
            forecast_in_range = range_low <= weighted_mean <= range_high
            # Near = within 2F of either edge (not midpoint)
            dist_to_range = min(abs(weighted_mean - range_low), abs(weighted_mean - range_high))
            forecast_near_range = not forecast_in_range and dist_to_range <= 2.0

        print(f"  [EDGE] {market['group_title']}: YES edge={yes_edge:+.1%}, NO edge={no_edge:+.1%}", end="")

        # Decision logic (v4.1):
        # When forecast points at a range, bet YES on it (data shows this wins)
        # When forecast is far from range, NO is safe
        # Key insight: our_prob for a 2F range is naturally low (10-25%),
        # but if forecast clusters there, YES still wins more than NO
        city_lower = market.get("city", "").lower()
        
        if forecast_in_range and our_prob >= 0.20 and yes_price <= 0.40:
            # Forecast IS in the range — strong YES signal
            edge = max(yes_edge, 0.01)  # Floor edge at 1% — trust the forecast
            side = "yes"
            bet_odds = yes_price
            print(f" -> YES [FORECAST IN RANGE] (wmean={weighted_mean:.1f}F, prob={our_prob:.0%})", end="")
        elif forecast_near_range and our_prob >= 0.20 and yes_price <= 0.35:
            # Forecast is NEAR the range and YES is cheap — good value bet
            edge = max(yes_edge, 0.01)
            side = "yes"
            bet_odds = yes_price
            print(f" -> YES [FORECAST NEAR RANGE] (wmean={weighted_mean:.1f}F, prob={our_prob:.0%})", end="")
        elif yes_edge >= no_edge:
            edge = yes_edge
            side = "yes"
            bet_odds = yes_price
        else:
            # v5.1: Block NO on narrow range markets (data shows NO loses)
            if is_exact_range and not is_threshold:
                print(f"  [SKIP] Would be NO on exact range - blocked: {market['group_title']}")
                return None
            edge = no_edge
            side = "no"
            bet_odds = no_price

        is_conviction = False
        is_forecast_driven = forecast_in_range or forecast_near_range

        # --- International city block ---
        if city.lower() in self.INTERNATIONAL_BLOCKED:
            print(f" -> BLOCKED [INTERNATIONAL] {city} - no NOAA, poor WR")
            return None

        # --- DB INTELLIGENCE: Check city+side historical performance ---
        city_total, city_wins, city_wr = self._get_city_record(city)
        side_total, side_wins, side_wr, side_profit = self._get_city_side_record(city, side)

        # Block: city with 0% WR and 2+ resolved bets (proven loser)
        if city_total >= 2 and city_wr == 0.0:
            print(f" -> BLOCKED [CITY 0% WR] {city}: {city_total} bets, 0 wins")
            return None

        # Block: city+side combo with 0% WR and 2+ bets
        if side_total >= 2 and side_wr == 0.0:
            print(f" -> BLOCKED [{side.upper()} 0% WR] {city}/{side}: {side_total} bets, 0 wins")
            return None

        # Warn: city with <30% WR and 3+ bets (struggling)
        if city_total >= 3 and city_wr < 0.30:
            print(f" -> BLOCKED [CITY LOW WR] {city}: {city_wr:.0%} WR ({city_wins}W/{city_total})")
            return None

        # v4.2: Forecast-driven bets skip edge gate entirely
        # The forecast data IS the signal — edge math is misleading for YES on narrow ranges
        conviction_side_prob = our_prob if side == "yes" else (1.0 - our_prob)
        if is_forecast_driven and side == "yes":
            # Forecast says YES — trust it, don't let edge gate reject
            is_conviction = True
            print(f" -> YES [FORECAST-DRIVEN] (prob={our_prob:.0%}, edge={edge:+.1%})")
        elif edge < WEATHER_MIN_EDGE and conviction_side_prob >= WEATHER_CONVICTION_PROB:
            is_conviction = True
            print(f" -> {side.upper()} [CONVICTION] (prob={conviction_side_prob:.0%}, edge={edge:+.1%})")
        elif edge < WEATHER_MIN_EDGE:
            print(f" -> {side.upper()}")
            return None
        else:
            print(f" -> {side.upper()}")

        # Confidence scoring (0-100) v3.0: recalibrated
        confidence = 25  # Deflated from 45 — old baseline was decorative (70+ for everything)
        # v5.1: Market type bonus
        if is_threshold:
            confidence += 15  # Threshold markets have 60% WR

        if spread <= 1:
            confidence += 15
        elif spread <= 2:
            confidence += 12
        elif spread <= 3:
            confidence += 8
        elif spread <= 4:
            confidence += 3
        else:
            confidence -= 10

        # v2.0: More granular source count bonus
        if source_count >= 5:
            confidence += 15
        elif source_count >= 4:
            confidence += 12
        elif source_count >= 3:
            confidence += 10
        elif source_count >= 2:
            confidence += 5

        # v4.2: Edge has LOW weight on confidence — forecast accuracy matters more
        # Edge math is structurally biased (NO always looks better for narrow ranges)
        if edge >= 0.30:
            confidence -= 4
        elif edge >= 0.20:
            confidence += 2
        elif edge >= 0.15:
            confidence += 3
        elif edge >= 0.10:
            confidence += 3
        elif edge >= 0.05:
            confidence += 2

        # Source quality -- PRIMARY confidence factor
        source_quality = self._get_source_quality_score(weights_used)
        confidence += source_quality

        city = market.get("city", "")
        # DB-driven city bonus (replaces old static method)
        if city_total >= 3:
            if city_wr >= 0.60:
                confidence += 8  # Proven winner
            elif city_wr >= 0.45:
                confidence += 3  # Decent
            elif city_wr >= 0.30:
                confidence -= 3  # Struggling
            else:
                confidence -= 8  # Bad (shouldn't reach here due to gate above)
        else:
            # Not enough data — use old method as fallback
            city_bonus = self._get_city_bonus(city)
            confidence += city_bonus

        # Side-specific bonus
        if side_total >= 2:
            if side_wr >= 0.60:
                confidence += 5  # This side works here
            elif side_wr >= 0.40:
                confidence += 2
            elif side_wr < 0.30:
                confidence -= 5  # This side struggles here

        cal_adj = self._get_calibration_adjustment(confidence)
        confidence += cal_adj

        confidence = max(0, min(100, confidence))

        if confidence < WEATHER_MIN_CONFIDENCE:
            return None

        return {
            "market_id": market["market_id"],
            "market_title": market["market_title"],
            "event_id": market["event_id"],
            "event_title": market["event_title"],
            "city": city,
            "market_date": market["market_date"],
            "temp_range": market["temp_range"],
            "our_prob": our_prob,
            "market_price": yes_price,
            "edge": edge,
            "side": side,
            "bet_odds": bet_odds,
            "is_conviction": is_conviction,
            "confidence": confidence,
            "spread": spread,
            "source_count": source_count,
            "weighted_mean": weighted_mean,
            "weights_used": weights_used,
        }

    def _get_city_side_record(self, city, side):
        """Query DB for this city+side win rate. Returns (total, wins, wr, profit)."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            row = conn.execute(
                "SELECT total_bets, wins, win_rate, total_profit FROM weather_side_patterns WHERE city = ? AND side = ?",
                (city.lower(), side.lower())
            ).fetchone()
            conn.close()
            if row:
                return row[0], row[1], row[2], row[3]
        except Exception:
            pass
        return 0, 0, 0.0, 0.0

    def _get_city_record(self, city):
        """Query DB for this city overall win rate. Returns (total, wins, wr)."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            row = conn.execute(
                "SELECT total_bets, wins, win_rate FROM weather_city_patterns WHERE city = ?",
                (city.lower(),)
            ).fetchone()
            conn.close()
            if row:
                return row[0], row[1], row[2]
        except Exception:
            pass
        return 0, 0, 0.0

    def find_best_bet(self, evaluated_markets):
        """From a list of evaluated sub-markets, return the single best bet."""
        if not evaluated_markets:
            return None

        by_event = {}
        for em in evaluated_markets:
            eid = em["event_id"]
            if eid not in by_event:
                by_event[eid] = []
            by_event[eid].append(em)

        best_overall = None
        best_score = -1

        for eid, candidates in by_event.items():
            # v4.2: Confidence-led ranking. Edge is a small bonus, not 50% of the score.
            # Forecast-driven bets (YES when forecast in/near range) get priority.
            def _rank(x):
                base = x["confidence"] * (1.0 + x["edge"] * 0.3)  # edge is 30% weight max
                # Boost forecast-driven YES bets — the forecast data IS the signal
                if x.get("is_conviction") and x["side"] == "yes":
                    wm = x.get("weighted_mean", 0)
                    base *= 1.15  # 15% ranking boost for forecast-driven
                return base
            best_in_event = max(candidates, key=_rank)
            score = _rank(best_in_event)
            if score > best_score:
                best_score = score
                best_overall = best_in_event

        return best_overall

    def _get_source_quality_score(self, weights_used):
        """Score confidence based on source credibility weights.
        PRIMARY confidence factor -- reliable sources = reliable bets.

        Weights range: 0.3 (terrible) to 1.8 (excellent). Default 1.0.
        """
        if not weights_used:
            return 0

        weights = list(weights_used.values())
        avg_weight = sum(weights) / len(weights)

        high_cred = sum(1 for w in weights if w >= 1.2)
        low_cred = sum(1 for w in weights if w < 0.7)

        score = 0

        # Average weight of sources used
        if avg_weight >= 1.3:
            score += 12
        elif avg_weight >= 1.15:
            score += 8
        elif avg_weight >= 1.0:
            score += 5
        elif avg_weight >= 0.85:
            score += 2
        elif avg_weight >= 0.7:
            score += 0
        else:
            score -= 8

        # Multiple high-credibility sources agreeing
        if high_cred >= 3:
            score += 5
        elif high_cred >= 2:
            score += 3

        # Multiple low-credibility sources -- forecast is suspect
        if low_cred >= 3:
            score -= 5
        elif low_cred >= 2:
            score -= 3

        return score

    def _get_city_bonus(self, city):
        """City-specific confidence adjustments based on track record.

        v4.0: Data-driven. Atlanta/NYC are proven winners, Seattle is volatile.
        """
        city_lower = city.lower().strip()

        # Proven performers (high win rate)
        if 'atlanta' in city_lower:
            return 8   # 67% win rate, 4W/2L
        if 'nyc' in city_lower or 'new york' in city_lower:
            return 8   # 100% win rate, 2W/0L
        if 'dallas' in city_lower:
            return 3   # 50% win rate, small sample

        # Neutral performers
        if 'miami' in city_lower:
            return 0   # 50% win rate, largest sample
        if 'chicago' in city_lower:
            return 0   # 50% win rate

        # Seattle: forecasts are accurate but were betting wrong side (NO)
        # With YES logic fix, Seattle should perform — keep neutral
        if 'seattle' in city_lower:
            return 0  # 1W/5L was from NO bias, not bad forecasts

        return 0


    def _get_calibration_adjustment(self, raw_confidence):
        """Adjust confidence based on empirical calibration data."""
        bucket = (raw_confidence // 5) * 5
        bucket = max(60, min(95, bucket))  # Was 80-95, missed all low-confidence bets

        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute(
                "SELECT total_bets, actual_win_rate FROM weather_calibration WHERE confidence_bucket = ?",
                (int(bucket),)
            )
            row = c.fetchone()
            conn.close()

            if not row or row[0] < 5:
                return 0

            actual_rate = row[1]
            expected_rate = bucket / 100.0
            if actual_rate < expected_rate - 0.1:
                return -5
            elif actual_rate > expected_rate + 0.1:
                return 5
            return 0
        except Exception:
            return 0


# ============================================================
# WeatherHeartbeat (48h AI review)
# ============================================================
class WeatherHeartbeat:
    """48-hour AI review of weather betting patterns."""

    def __init__(self, ai_client, ai_fallback_key, db_path='hedge_fund_performance.db'):
        self.ai_client = ai_client
        self.ai_fallback_key = ai_fallback_key
        self.db_path = db_path
        self.last_run = None

    def should_run(self):
        if self.last_run is None:
            return True
        hours_since = (datetime.now() - self.last_run).total_seconds() / 3600
        return hours_since >= 48

    def run(self):
        if not self.ai_client:
            print("[WEATHER-HB] No AI client available")
            return

        print(f"\n[WEATHER-HB] Running 48h AI analysis...")
        self.last_run = datetime.now()

        try:
            recent_bets = self._get_recent_bets()
            source_data = self._get_source_data()
            city_data = self._get_city_data()
            calibration_data = self._get_calibration_data()

            if not recent_bets and not source_data:
                print("[WEATHER-HB] No weather data to analyze yet")
                return

            prompt = f"""You are the AI heartbeat for a weather prediction market hedge fund on Polymarket.
Review the accumulated data and provide strategic recommendations.

## Recent Weather Bets (last 48h):
{json.dumps(recent_bets, indent=2)}

## Source Credibility Data:
{json.dumps(source_data, indent=2)}

## City Patterns:
{json.dumps(city_data, indent=2)}

## Calibration Stats:
{json.dumps(calibration_data, indent=2)}

## Analyze and recommend:
1. Which forecast sources are most/least reliable per city?
2. Any systematic biases?
3. Which cities should we focus on / avoid?
4. Is our confidence calibration accurate?
5. Any patterns in wins vs losses?

Respond in JSON:
{{
    "health": "GOOD" / "WARNING" / "CRITICAL",
    "reasoning": "<3-4 sentences of analysis>",
    "source_adjustments": [
        {{"city": "<city>", "source": "<source>", "current_weight": 0.0, "recommended_weight": 0.0, "reason": "<why>"}}
    ],
    "city_recommendations": [
        {{"city": "<city>", "action": "focus" / "avoid" / "neutral", "reason": "<why>"}}
    ],
    "calibration_assessment": "<1-2 sentences>",
    "patterns_detected": ["<pattern1>"],
    "concerns": "<any concerns or null>"
}}"""

            analysis = self._call_ai(prompt)
            if not analysis:
                print("[WEATHER-HB] AI analysis failed")
                return

            adjustments = analysis.get("source_adjustments", [])
            for adj in adjustments:
                city = adj.get("city")
                source = adj.get("source")
                new_weight = adj.get("recommended_weight")
                if city and source and new_weight is not None:
                    new_weight = max(0.5, min(1.5, float(new_weight)))
                    self._apply_weight(city, source, new_weight)
                    print(f"  [ADJUST] {source}@{city}: -> {new_weight:.2f} ({adj.get('reason', '')})")

            self._log_heartbeat(analysis)

            health = analysis.get("health", "?")
            reasoning = analysis.get("reasoning", "")
            print(f"  [WEATHER-HB] Health: {health}")
            print(f"  [WEATHER-HB] {reasoning}")

            if analysis.get("concerns"):
                print(f"  [WEATHER-HB] Concerns: {analysis['concerns']}")

            for pattern in analysis.get("patterns_detected", []):
                print(f"  [DETECTIVE] {pattern}")

        except Exception as e:
            print(f"[WEATHER-HB] Error: {e}")

    def _call_ai(self, prompt):
        try:
            response = self.ai_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = response.content[0].text.strip()
            if '```json' in raw:
                raw = raw.split('```json')[1].split('```')[0].strip()
            elif '```' in raw:
                raw = raw.split('```')[1].split('```')[0].strip()
            return json.loads(raw)

        except Exception as e:
            if self.ai_fallback_key and hasattr(e, 'status_code') and e.status_code in (403, 502, 503):
                try:
                    print(f"  [WEATHER-HB] Bankr gateway error, falling back to Anthropic...")
                    fallback = anthropic.Anthropic(api_key=self.ai_fallback_key)
                    response = fallback.messages.create(
                        model="claude-haiku-4-5-20251001",
                        max_tokens=2000,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    raw = response.content[0].text.strip()
                    if '```json' in raw:
                        raw = raw.split('```json')[1].split('```')[0].strip()
                    elif '```' in raw:
                        raw = raw.split('```')[1].split('```')[0].strip()
                    return json.loads(raw)
                except Exception as e2:
                    print(f"  [WEATHER-HB] Fallback error: {e2}")
            else:
                print(f"  [WEATHER-HB] AI error: {e}")
            return None

    def _get_recent_bets(self):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("""
                SELECT b.id, b.market_title, b.side, b.amount, b.odds, b.confidence_score,
                       b.edge, b.status, b.won, b.profit,
                       wb.city, wb.temp_range, wb.weighted_mean
                FROM bets b
                LEFT JOIN weather_bets wb ON wb.bet_id = b.id
                WHERE b.category = 'weather'
                AND b.timestamp >= datetime('now', '-48 hours')
                ORDER BY b.timestamp DESC
            """)
            rows = c.fetchall()
            conn.close()
            return [
                {
                    "bet_id": r[0], "market": r[1], "side": r[2], "amount": r[3],
                    "odds": r[4], "confidence": r[5], "edge": r[6], "status": r[7],
                    "won": r[8], "profit": r[9], "city": r[10],
                    "temp_range": r[11], "weighted_mean": r[12]
                }
                for r in rows
            ]
        except Exception:
            return []

    def _get_source_data(self):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("SELECT city, source_name, credibility_weight, total_predictions, accurate_predictions, avg_error FROM weather_sources")
            rows = c.fetchall()
            conn.close()
            return [
                {"city": r[0], "source": r[1], "weight": r[2], "predictions": r[3],
                 "accurate": r[4], "avg_error": r[5]}
                for r in rows
            ]
        except Exception:
            return []

    def _get_city_data(self):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("SELECT city, total_bets, wins, win_rate, total_profit FROM weather_city_patterns")
            rows = c.fetchall()
            conn.close()
            return [
                {"city": r[0], "bets": r[1], "wins": r[2], "win_rate": r[3], "profit": r[4]}
                for r in rows
            ]
        except Exception:
            return []

    def _get_calibration_data(self):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("SELECT confidence_bucket, total_bets, wins, actual_win_rate FROM weather_calibration")
            rows = c.fetchall()
            conn.close()
            return [
                {"bucket": r[0], "bets": r[1], "wins": r[2], "actual_rate": r[3]}
                for r in rows
            ]
        except Exception:
            return []

    def _apply_weight(self, city, source, new_weight):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("""
                INSERT INTO weather_sources (city, source_name, credibility_weight, total_predictions, accurate_predictions, avg_error, last_updated)
                VALUES (?, ?, ?, 0, 0, 0, ?)
                ON CONFLICT(city, source_name) DO UPDATE SET
                    credibility_weight = ?,
                    last_updated = ?
            """, (city, source, new_weight, datetime.now().isoformat(), new_weight, datetime.now().isoformat()))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"  [WEIGHT] Error applying weight: {e}")

    def _log_heartbeat(self, analysis):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("""
                INSERT INTO weather_heartbeat_logs
                (timestamp, health, reasoning, source_adjustments, city_recommendations,
                 calibration_assessment, patterns_detected, concerns)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now().isoformat(),
                analysis.get("health"),
                analysis.get("reasoning"),
                json.dumps(analysis.get("source_adjustments")),
                json.dumps(analysis.get("city_recommendations")),
                analysis.get("calibration_assessment"),
                json.dumps(analysis.get("patterns_detected")),
                analysis.get("concerns"),
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"  [WEATHER-HB] Log error: {e}")


# ============================================================
# WeatherAgent (orchestrator) v2.0
# ============================================================
class WeatherAgent:
    """Orchestrates the weather betting cycle. Called by main loop."""

    def __init__(self, tracker, notifier, bankr, ai_client=None, ai_fallback_key=None):
        self.tracker = tracker
        self.notifier = notifier
        self.bankr = bankr
        self.db_path = tracker.db_path
        self.ai_client = ai_client
        self.ai_fallback_key = ai_fallback_key

        self.scanner = WeatherScanner()
        self.forecaster = WeatherForecaster()
        self.credibility = CredibilityEngine(self.db_path)
        self._ensure_snapshot_table()
        self._last_collection_time = 0  # Track last forecast collection
        self.edge_calc = EdgeCalculator(self.db_path)
        self.heartbeat = WeatherHeartbeat(ai_client, ai_fallback_key, self.db_path)

        self.weather_daily_bet_count = 0
        self.active_weather_bets = []

        self._load_active_weather_bets()

    def _load_active_weather_bets(self):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("""
                SELECT b.id, b.market_id, b.market_title, b.amount, b.side, b.odds,
                       wb.city, wb.temp_range, wb.weighted_mean
                FROM bets b
                LEFT JOIN weather_bets wb ON wb.bet_id = b.id
                WHERE b.status = 'pending' AND b.category = 'weather'
            """)
            rows = c.fetchall()

            c.execute("""
                SELECT COUNT(*) FROM bets
                WHERE DATE(timestamp) = DATE('now') AND category = 'weather'
            """)
            self.weather_daily_bet_count = c.fetchone()[0]
            conn.close()

            for r in rows:
                self.active_weather_bets.append({
                    "bet_id": r[0],
                    "market_id": str(r[1]),
                    "market_title": r[2],
                    "amount": r[3],
                    "side": r[4],
                    "odds": r[5],
                    "city": r[6],
                    "temp_range": r[7],
                    "weighted_mean": r[8],
                })

            if self.active_weather_bets:
                print(f"[WEATHER ANALYST] Loaded {len(self.active_weather_bets)} active weather bets from DB")

        except Exception as e:
            print(f"[WEATHER ANALYST] Failed to load weather bets: {e}")

    def _call_ai(self, prompt):
        """Call Claude Haiku via Bankr LLM Gateway with Anthropic direct fallback."""
        try:
            response = self.ai_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = response.content[0].text.strip()
            if '```json' in raw:
                raw = raw.split('```json')[1].split('```')[0].strip()
            elif '```' in raw:
                raw = raw.split('```')[1].split('```')[0].strip()
            return json.loads(raw)

        except Exception as e:
            if self.ai_fallback_key and hasattr(e, 'status_code') and e.status_code in (403, 502, 503):
                try:
                    print(f"  [WEATHER-AI] Bankr gateway error, falling back to Anthropic...")
                    fallback = anthropic.Anthropic(api_key=self.ai_fallback_key)
                    response = fallback.messages.create(
                        model="claude-haiku-4-5-20251001",
                        max_tokens=2000,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    raw = response.content[0].text.strip()
                    if '```json' in raw:
                        raw = raw.split('```json')[1].split('```')[0].strip()
                    elif '```' in raw:
                        raw = raw.split('```')[1].split('```')[0].strip()
                    return json.loads(raw)
                except Exception as e2:
                    print(f"  [WEATHER-AI] Fallback error: {e2}")
            else:
                print(f"  [WEATHER-AI] AI error: {e}")
            return None

    def _ai_evaluate_bet(self, rec, forecasts, spread):
        """Ask Claude Haiku to reason about a weather bet before execution."""
        if not self.ai_client:
            return None

        # Is this a forecast-driven YES bet?
        is_forecast_yes = rec.get("is_conviction") and rec["side"] == "yes"
        forecast_tag = "FORECAST-DRIVEN YES" if is_forecast_yes else "MATH-DRIVEN"

        prompt = f"""You are a weather prediction market analyst for a v5.0 strategy that uses FORECAST-DRIVEN betting.

CRITICAL STRATEGY CONTEXT:
Our data proves that when forecasts cluster near a temperature range, betting YES wins far more than NO.
- Historical backtest: betting YES when forecast is in/near range = profitable
- Historical backtest: betting NO when forecast is in/near range = loses money
- The math model's "edge" is structurally biased toward NO for narrow ranges — IGNORE edge math for YES decisions
- If the weighted mean is within 2F of the range and multiple sources agree, YES is almost always correct
- Do NOT override a YES bet to NO unless the forecast clearly points AWAY from the range

BET TYPE: {forecast_tag}

MARKET: {rec['market_title']}
CITY: {rec['city']}
DATE: {rec['market_date']}
TEMPERATURE RANGE: {rec['temp_range'].get('raw', '?')}

FORECAST DATA (all sources):
{chr(10).join(f"  - {f['source']}: {f['high_temp']:.1f}F (weight: {rec['weights_used'].get(f['source'], 1.0):.2f})" for f in forecasts)}

WEIGHTED MEAN: {rec['weighted_mean']:.1f}F
SOURCE SPREAD: {spread:.1f}F (max - min across sources)
SOURCE COUNT: {rec['source_count']}

MODEL RECOMMENDATION:
  Side: {rec['side'].upper()}
  Our probability: {rec['our_prob']:.1%}
  Market price: {rec['market_price']:.1%}
  Edge: {rec['edge']:+.1%} (LOW WEIGHT — forecast accuracy matters more than edge math)
  Confidence score: {rec['confidence']}

EVALUATION RULES:
- If this is FORECAST-DRIVEN YES: the forecast says temp will be in this range. Only override if forecast is clearly wrong.
- If weighted mean is within the range or within 2F of it, YES is the correct side. Confirm it.
- Low probability (10-25%) is NORMAL for narrow 2F ranges — do not use low probability as a reason to override YES.
- High source spread (>4F) is a reason for caution. Low spread (<2F) means sources agree = high confidence.
- For MATH-DRIVEN bets (NO side, far from range), evaluate normally.

Respond with ONLY this JSON:
{{"decision": "confirm" or "override" or "skip", "side": "yes" or "no", "confidence_adjustment": integer from -10 to +10, "reasoning": "1-2 sentence explanation"}}"""

        result = self._call_ai(prompt)
        if not result:
            return None

        decision = result.get("decision", "").lower()
        if decision not in ("confirm", "override", "skip"):
            print(f"  [AI] Invalid decision '{decision}' -- falling back to math")
            return None

        side = result.get("side", rec["side"]).lower()
        if side not in ("yes", "no"):
            side = rec["side"]

        adj = result.get("confidence_adjustment", 0)
        try:
            adj = max(-10, min(10, int(adj)))
        except (ValueError, TypeError):
            adj = 0

        print(f"  [AI] Decision: {decision.upper()} | Side: {side.upper()} | Adj: {adj:+d} | {result.get('reasoning', '')}")

        return {
            "decision": decision,
            "side": side,
            "confidence_adjustment": adj,
            "reasoning": result.get("reasoning", ""),
        }

    def _get_bet_size(self, confidence, is_conviction=False):
        """Get bet size based on confidence. Conviction bets always use minimum."""
        if is_conviction:
            return min(WEATHER_BET_SIZES.values())  # minimum for conviction bets

        bet_size = min(WEATHER_BET_SIZES.values())
        for threshold in sorted(WEATHER_BET_SIZES.keys()):
            if confidence >= threshold:
                bet_size = WEATHER_BET_SIZES[threshold]
        return bet_size



    def _ensure_snapshot_table(self):
        """Create forecast_snapshots table if it doesn't exist."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS forecast_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT NOT NULL,
                    target_date TEXT NOT NULL,
                    source TEXT NOT NULL,
                    high_temp REAL,
                    low_temp REAL,
                    unit TEXT DEFAULT 'F',
                    fetched_at TEXT NOT NULL,
                    collection_run INTEGER DEFAULT 0
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_city_date
                ON forecast_snapshots(city, target_date)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS forecast_collection_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_at TEXT NOT NULL,
                    cities_scanned INTEGER DEFAULT 0,
                    total_readings INTEGER DEFAULT 0,
                    sources_failed TEXT
                )
            """)
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[WEATHER ANALYST] Error creating snapshot tables: {e}")

    def collect_forecasts(self):
        """Collect forecasts from ALL sources for all active weather markets.
        Runs every 30 min. Stores every reading to forecast_snapshots.
        This builds the data over time so betting windows have rich data."""
        import time as _t
        from hedge_fund_config import WEATHER_COLLECTION_INTERVAL

        now = _t.time()
        if now - self._last_collection_time < WEATHER_COLLECTION_INTERVAL:
            return  # Not time yet

        self._last_collection_time = now
        print(f"\n[WEATHER ANALYST] === FORECAST COLLECTION RUN ===")

        # Scan Polymarket for active weather markets
        markets = self.scanner.scan_weather_markets()
        if not markets:
            print("[WEATHER ANALYST] No weather markets to collect for")
            return

        # Group by city+date
        city_dates = {}
        for m in markets:
            key = (m["city"], m["market_date"])
            if key not in city_dates:
                city_dates[key] = True

        total_readings = 0
        all_failed = []

        for (city, date_str) in city_dates:
            from datetime import datetime as dt
            try:
                target_date = dt.fromisoformat(date_str).date()
            except Exception:
                continue

            # Fetch from all sources
            # Try snapshot data first (accumulated from collection runs)
            snapshot_forecasts = self.get_snapshot_forecasts(city, target_date)
            if snapshot_forecasts and len(snapshot_forecasts) >= 2:
                forecasts = snapshot_forecasts
            else:
                # Fallback to live API (first cycle or no snapshots yet)
                forecasts = self.forecaster.get_forecasts(city, target_date)

            # Track which sources we got vs which we expected
            expected_sources = ['open_meteo']
            coords = CITY_COORDS.get(city, {})
            if coords.get('noaa'):
                expected_sources.append('noaa')
            if self.forecaster.weatherapi_key:
                expected_sources.append('weatherapi')
            if self.forecaster.openweathermap_key:
                expected_sources.append('openweathermap')
            if self.forecaster.visualcrossing_key:
                expected_sources.append('visualcrossing')
            if self.forecaster.weatherbit_key:
                expected_sources.append('weatherbit')
            if self.forecaster.pirateweather_key:
                expected_sources.append('pirateweather')

            got_sources = [f['source'] for f in forecasts]
            missed = [s for s in expected_sources if s not in got_sources]
            if missed:
                all_failed.extend([f"{city}:{s}" for s in missed])

            # Store each reading
            if forecasts:
                try:
                    conn = sqlite3.connect(self.db_path, timeout=30)
                    fetched_at = dt.now().isoformat()
                    for f in forecasts:
                        conn.execute(
                            "INSERT INTO forecast_snapshots (city, target_date, source, high_temp, low_temp, unit, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (city, date_str, f['source'], f['high_temp'], f.get('low_temp'), f.get('unit', 'F'), fetched_at)
                        )
                        total_readings += 1
                    conn.commit()
                    conn.close()
                except Exception as e:
                    print(f"[WEATHER ANALYST] Error storing snapshots for {city}: {e}")

            print(f"  {city} {date_str}: {len(forecasts)}/{len(expected_sources)} sources")

        # Log the collection run
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.execute(
                "INSERT INTO forecast_collection_log (run_at, cities_scanned, total_readings, sources_failed) VALUES (?, ?, ?, ?)",
                (dt.now().isoformat(), len(city_dates), total_readings, json.dumps(all_failed) if all_failed else None)
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

        print(f"[WEATHER ANALYST] Collection complete: {len(city_dates)} cities, {total_readings} readings, {len(all_failed)} source failures")

    def get_snapshot_forecasts(self, city, target_date):
        """Get the best forecast data from accumulated snapshots.
        Uses the LATEST reading from each source, plus computes stability metrics."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            date_str = target_date.isoformat() if hasattr(target_date, 'isoformat') else str(target_date)

            # Get latest reading per source
            rows = conn.execute("""
                SELECT source, high_temp, low_temp, unit, fetched_at,
                    (SELECT COUNT(*) FROM forecast_snapshots fs2
                     WHERE fs2.city = fs.city AND fs2.target_date = fs.target_date
                     AND fs2.source = fs.source) as reading_count,
                    (SELECT ROUND(AVG(high_temp), 2) FROM forecast_snapshots fs3
                     WHERE fs3.city = fs.city AND fs3.target_date = fs.target_date
                     AND fs3.source = fs.source) as avg_high,
                    (SELECT ROUND(MAX(high_temp) - MIN(high_temp), 2) FROM forecast_snapshots fs4
                     WHERE fs4.city = fs.city AND fs4.target_date = fs.target_date
                     AND fs4.source = fs.source) as source_drift
                FROM forecast_snapshots fs
                WHERE city = ? AND target_date = ?
                AND fetched_at = (
                    SELECT MAX(fetched_at) FROM forecast_snapshots fs5
                    WHERE fs5.city = fs.city AND fs5.target_date = fs.target_date
                    AND fs5.source = fs.source
                )
                GROUP BY source
                ORDER BY source
            """, (city, date_str)).fetchall()
            conn.close()

            if not rows:
                return None

            forecasts = []
            for row in rows:
                f = {
                    "source": row[0],
                    "high_temp": row[1],
                    "low_temp": row[2],
                    "unit": row[3] or "F",
                    "snapshot_readings": row[5],  # How many times we've read this source
                    "snapshot_avg_high": row[6],   # Average across all readings
                    "snapshot_drift": row[7],      # Max - Min across readings (stability)
                }
                forecasts.append(f)

            total_readings = sum(f["snapshot_readings"] for f in forecasts)
            stable_sources = sum(1 for f in forecasts if f["snapshot_drift"] is not None and f["snapshot_drift"] <= 1.0)
            print(f"  [SNAPSHOTS] {city} {date_str}: {len(forecasts)} sources, {total_readings} total readings, {stable_sources} stable (drift<=1F)")

            return forecasts

        except Exception as e:
            print(f"[WEATHER ANALYST] Error reading snapshots: {e}")
            return None

    def update_weather_analytics_external(self, city, side):
        """Public wrapper for _update_weather_analytics — called by Settlement Clerk."""
        self._update_weather_analytics(city, side)


    def run_weather_cycle(self, available_balance, wallet=None):
        """Run one weather betting cycle.
        v5.0: Collect forecasts every 30 min. Only BET during windows.
        Uses accumulated snapshot data for richer decisions.
        """
        # Always try to collect forecasts (throttled internally to every 30 min)
        self.collect_forecasts()

        # Check if we're in a betting window (ET)
        from hedge_fund_config import WEATHER_BETTING_WINDOWS
        import pytz
        try:
            et = pytz.timezone('US/Eastern')
            now_et = datetime.now(et)
            current_hour = now_et.hour
            in_window = any(start <= current_hour < end for start, end in WEATHER_BETTING_WINDOWS)
        except Exception:
            # Fallback: UTC-5 rough estimate
            utc_hour = datetime.utcnow().hour
            current_hour = (utc_hour - 5) % 24
            in_window = any(start <= current_hour < end for start, end in WEATHER_BETTING_WINDOWS)

        if not in_window:
            print(f"\n[WEATHER ANALYST] Outside betting window (ET hour={current_hour}). Collecting data only.")
            return

        print(f"\n{'='*60}")
        print(f"WEATHER CYCLE - {datetime.now().strftime('%H:%M:%S')} (Window: ET {current_hour}:00)")
        print(f"{'='*60}")
        print(f"Weather Bets Today: {self.weather_daily_bet_count}/{WEATHER_MAX_DAILY_BETS}")
        print(f"Active Weather Positions: {len(self.active_weather_bets)}/{WEATHER_MAX_CONCURRENT}")

        if self.weather_daily_bet_count >= WEATHER_MAX_DAILY_BETS:
            print(f"[WEATHER ANALYST] Daily limit reached")
            return

        if len(self.active_weather_bets) >= WEATHER_MAX_CONCURRENT:
            print(f"[WEATHER ANALYST] Max concurrent positions reached")
            return

        min_bet = min(WEATHER_BET_SIZES.values())
        if available_balance < min_bet + 5.0:
            print(f"[WEATHER ANALYST] Balance too low: ${available_balance:.2f}")
            return

        markets = self.scanner.scan_weather_markets()
        if not markets:
            print("[WEATHER ANALYST] No temperature markets found (likely seasonal - will auto-scan next cycle)")
            return

        events = {}
        for m in markets:
            eid = m["event_id"]
            if eid not in events:
                events[eid] = {"city": m["city"], "date": m["market_date"], "sub_markets": []}
            events[eid]["sub_markets"].append(m)

        print(f"[WEATHER ANALYST] Found {len(events)} temperature events across {len(set(m['city'] for m in markets))} cities")

        bets_placed = 0

        for event_id, event_data in events.items():
            if self.weather_daily_bet_count >= WEATHER_MAX_DAILY_BETS:
                break
            if len(self.active_weather_bets) >= WEATHER_MAX_CONCURRENT:
                break

            city = event_data["city"]
            target_date = datetime.fromisoformat(event_data["date"]).date()

            if any(wb.get("market_id") in [sm["market_id"] for sm in event_data["sub_markets"]]
                   for wb in self.active_weather_bets):
                print(f"[WEATHER ANALYST] Already have position on {city} {target_date}")
                continue

            forecasts = self.forecaster.get_forecasts(city, target_date)
            if len(forecasts) < 2:
                print(f"[WEATHER ANALYST] Only {len(forecasts)} sources for {city} - need 2+ to bet")
                continue

            weighted_mean, spread, weights_used = self.credibility.get_weighted_forecast(city, forecasts)
            if weighted_mean is None:
                continue

            print(f"[WEATHER ANALYST] {city}: weighted mean={weighted_mean:.1f}F, spread={spread:.1f}F")

            evaluated = []
            for sm in event_data["sub_markets"]:
                our_prob = self.credibility.compute_probability(
                    weighted_mean, spread, sm["temp_range"],
                    city=city, target_date=target_date
                )
                result = self.edge_calc.evaluate(
                    sm, our_prob, spread, len(forecasts), weights_used, weighted_mean
                )
                if result:
                    evaluated.append(result)

            if not evaluated:
                print(f"[WEATHER ANALYST] No edge found for {city} {target_date}")
                continue

            best = self.edge_calc.find_best_bet(evaluated)
            if not best:
                continue

            # AI reasoning gate
            ai_result = self._ai_evaluate_bet(best, forecasts, spread)
            if ai_result:
                if ai_result["decision"] == "skip":
                    print(f"  [AI] Skipping bet: {ai_result['reasoning']}")
                    continue

                if ai_result["decision"] == "override":
                    old_side = best["side"]
                    best["side"] = ai_result["side"]
                    if best["side"] == "yes":
                        best["edge"] = best["our_prob"] - best["market_price"]
                        best["bet_odds"] = best["market_price"]
                    else:
                        best["edge"] = (1.0 - best["our_prob"]) - (1.0 - best["market_price"])
                        best["bet_odds"] = 1.0 - best["market_price"]
                    print(f"  [AI] Overriding {old_side.upper()} -> {best['side'].upper()}")

                # Apply confidence adjustment
                best["confidence"] += ai_result["confidence_adjustment"]
                best["confidence"] = max(0, min(100, best["confidence"]))

                if best["confidence"] < WEATHER_MIN_CONFIDENCE:
                    print(f"  [AI] Confidence dropped to {best['confidence']} (below {WEATHER_MIN_CONFIDENCE}) -- skipping")
                    continue

                best["ai_reasoning"] = ai_result["reasoning"]
                best["ai_decision"] = ai_result["decision"]
            else:
                print(f"  [AI] Unavailable -- proceeding with math-only decision")

            # Deploy 1: Use wallet.can_bet() if wallet is provided
            bet_amount = self._get_bet_size(best["confidence"], best.get("is_conviction", False))
            if wallet:
                can, reason = wallet.can_bet('weather', bet_amount)
                if not can:
                    print(f"[WEATHER ANALYST] [WALLET] {reason}")
                    continue

            success = self._execute_weather_bet(best, forecasts, available_balance, wallet=wallet)
            if success:
                bets_placed += 1
                bet_size = self._get_bet_size(best["confidence"], best.get("is_conviction", False))
                available_balance -= bet_size

        if self.heartbeat.should_run():
            self.heartbeat.run()

        print(f"\n[WEATHER CYCLE COMPLETE] Placed {bets_placed} weather bets")

    def _execute_weather_bet(self, rec, forecasts, available_balance, wallet=None):
        """Execute a single weather bet via Bankr. v2.0: scaled sizing + verification.
        Deploy 1: wallet param for fund reservation after successful placement.
        """
        bet_amount = self._get_bet_size(rec["confidence"], rec.get("is_conviction", False))

        if available_balance - bet_amount < 5.0:
            print(f"[WEATHER ANALYST] Would leave balance below $5 - skipping")
            return False

        print(f"\n[WEATHER ANALYST] Executing...")
        print(f"  Market: {rec['market_title'][:60]}")
        print(f"  City: {rec['city']}")
        print(f"  Range: {rec['temp_range'].get('raw', '?')}")
        conviction_tag = " [CONVICTION]" if rec.get("is_conviction", False) else ""
        print(f"  Side: {rec['side'].upper()}{conviction_tag}")
        print(f"  Amount: ${bet_amount:.2f} (confidence {rec['confidence']})")
        print(f"  Our P: {rec['our_prob']:.1%} vs Market: {rec['market_price']:.1%}")
        print(f"  Edge: {rec['edge']:+.1%}")
        print(f"  Confidence: {rec['confidence']}")
        print(f"  Weighted Mean: {rec['weighted_mean']:.1f}F")
        print(f"  Sources: {rec['source_count']}")

        bankr_result = self.bankr.place_bet(
            market_title=rec["market_title"],
            side=rec["side"],
            amount=bet_amount,
            odds=rec["bet_odds"],
        )

        if not bankr_result["success"]:
            print(f"  [ERROR] Bankr API failed: {bankr_result.get('error')}")
            return False

        print(f"  [BANKER] Trade ID: {bankr_result['trade_id']}")

        # v2.0: Verify bet execution
        verified = True
        verify_note = ""
        try:
            verify_result = self.bankr.verify_bet_execution(rec["market_title"], rec["side"])
            if verify_result.get("verified"):
                print(f"  [VERIFIED] Bet confirmed in Bankr positions")
            else:
                verified = False
                verify_note = verify_result.get("reason", "unverified")
                print(f"  [WARN] Bet unverified: {verify_note} -- logging anyway")
        except Exception as ve:
            verify_note = f"verify error: {ve}"
            print(f"  [WARN] Verification failed: {ve} -- logging anyway")

        source_info = ", ".join(f"{f['source']}: {f['high_temp']:.0f}F" for f in forecasts)
        reasoning = (
            f"WEATHER v2: {rec['city']} {rec['market_date']} | "
            f"Range: {rec['temp_range'].get('raw', '?')} | "
            f"Sources ({rec['source_count']}): {source_info} | "
            f"Weighted mean: {rec['weighted_mean']:.1f}F | "
            f"Our P: {rec['our_prob']:.1%} vs Market: {rec['market_price']:.1%} | "
            f"Edge: {rec['edge']:+.1%} | "
            f"Bet: ${bet_amount:.2f} (conf {rec['confidence']})"
        )
        if not verified:
            reasoning += f" | UNVERIFIED: {verify_note}"
        if rec.get("ai_reasoning"):
            reasoning += f" | AI: [{rec.get('ai_decision', '?').upper()}] {rec['ai_reasoning']}"

        bet_id = self.tracker.log_bet(
            market_id=rec["market_id"],
            market_title=rec["market_title"],
            category="weather",
            side=rec["side"],
            amount=bet_amount,
            odds=rec["bet_odds"],
            score=rec["confidence"],
            edge=rec["edge"],
            reasoning=reasoning,
            balance_before=available_balance,
        )

        self.tracker.log_weather_prediction(
            bet_id=bet_id,
            city=rec["city"],
            market_date=rec["market_date"],
            forecasts=forecasts,
            weighted_mean=rec["weighted_mean"],
            our_probability=rec["our_prob"],
            edge=rec["edge"],
        )

        self.tracker.log_weather_bet(
            bet_id=bet_id,
            city=rec["city"],
            temp_range=rec["temp_range"].get("raw", ""),
            forecast_temps={f["source"]: f["high_temp"] for f in forecasts},
            weighted_mean=rec["weighted_mean"],
        )

        self.active_weather_bets.append({
            "bet_id": bet_id,
            "market_id": rec["market_id"],
            "market_title": rec["market_title"],
            "amount": bet_amount,
            "side": rec["side"],
            "odds": rec["bet_odds"],
            "city": rec["city"],
            "temp_range": rec["temp_range"].get("raw", ""),
            "weighted_mean": rec["weighted_mean"],
        })

        self.weather_daily_bet_count += 1

        print(f"  [SUCCESS] Weather bet placed (ID: {bet_id})")

        # Deploy 1: Reserve funds in wallet after successful bet logging
        if wallet:
            position_data = {
                "bet_id": bet_id,
                "market_id": rec["market_id"],
                "market_title": rec["market_title"],
                "city": rec["city"],
                "temp_range": rec["temp_range"].get("raw", ""),
                "side": rec["side"],
                "odds": rec["bet_odds"],
                "confidence": rec["confidence"],
                "edge": rec["edge"],
                "weighted_mean": rec["weighted_mean"],
                "market_date": rec["market_date"],
            }
            try:
                wallet.reserve_funds('weather', bet_amount, position_data)
                print(f"  [WALLET] Reserved ${bet_amount:.2f} for weather bet {bet_id}")
            except Exception as we:
                print(f"  [WALLET] Warning: fund reservation failed: {we} -- bet still placed")

        self.notifier.notify_alert(
            f"WEATHER BET PLACED\n\n"
            f"City: {rec['city'].title()}\n"
            f"Range: {rec['temp_range'].get('raw', '?')}\n"
            f"Side: {rec['side'].upper()} | ${bet_amount:.2f}\n"
            f"Edge: {rec['edge']:+.1%} | Conf: {rec['confidence']}\n"
            f"Mean: {rec['weighted_mean']:.1f}F | Sources: {rec['source_count']}"
        )

        return True

    def resolve_weather_bet(self, bet_id, actual_high, wallet=None):
        """Resolve a weather bet and update all learning systems.
        Deploy 1: wallet param for fund release after resolution.
        """
        bet = None
        for wb in self.active_weather_bets:
            if wb["bet_id"] == bet_id:
                bet = wb
                break

        if not bet:
            print(f"[WEATHER ANALYST] Bet {bet_id} not found in active weather bets")
            return

        city = bet.get("city", "")
        weighted_mean = bet.get("weighted_mean", 0)

        # If weighted_mean is 0, try weather_predictions table
        if weighted_mean == 0:
            try:
                conn_wm = sqlite3.connect(self.db_path, timeout=30)
                row_wm = conn_wm.execute(
                    "SELECT weighted_mean FROM weather_predictions WHERE bet_id = ?", (bet_id,)
                ).fetchone()
                conn_wm.close()
                if row_wm and row_wm[0]:
                    weighted_mean = row_wm[0]
                    print(f"[WEATHER ANALYST] Bet {bet_id}: recovered weighted_mean={weighted_mean:.1f}F from weather_predictions")
            except Exception:
                pass

        # v2.0: Try JSON format first, then legacy columns
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()

            sources = {}
            c.execute("SELECT forecast_data FROM weather_predictions WHERE bet_id = ?", (bet_id,))
            row = c.fetchone()
            if row and row[0]:
                try:
                    forecast_data = json.loads(row[0])
                    for src_name, src_data in forecast_data.items():
                        if isinstance(src_data, dict) and "high_temp" in src_data:
                            sources[src_name] = src_data["high_temp"]
                        elif isinstance(src_data, (int, float)):
                            sources[src_name] = src_data
                except (json.JSONDecodeError, TypeError):
                    pass

            if not sources:
                c.execute("""
                    SELECT open_meteo_high, noaa_high, weatherapi_high
                    FROM weather_predictions WHERE bet_id = ?
                """, (bet_id,))
                row = c.fetchone()
                if row:
                    if row[0] is not None:
                        sources["open_meteo"] = row[0]
                    if row[1] is not None:
                        sources["noaa"] = row[1]
                    if row[2] is not None:
                        sources["weatherapi"] = row[2]

            # Also grab market_date for source_prediction_log
            c2 = conn.cursor()
            c2.execute("SELECT market_date FROM weather_predictions WHERE bet_id = ?", (bet_id,))
            md_row = c2.fetchone()
            market_date = md_row[0] if md_row else None

            conn.close()
        except Exception:
            sources = {}
            market_date = None

        for source_name, predicted_high in sources.items():
            if predicted_high is not None:
                self.credibility.update_after_resolution(city, source_name, predicted_high, actual_high, bet_id=bet_id, market_date=market_date)

        error = abs(weighted_mean - actual_high)
        predicted_high = weighted_mean

        self.tracker.log_weather_resolution(
            bet_id=bet_id,
            predicted_high=predicted_high,
            actual_high=actual_high,
            error=error,
        )

        self.active_weather_bets = [wb for wb in self.active_weather_bets if wb["bet_id"] != bet_id]

        print(f"[WEATHER ANALYST] Resolved bet {bet_id}: predicted={predicted_high:.1f}F, actual={actual_high:.1f}F, error={error:.1f}F")

        # Update analytics tables after resolution
        self._update_weather_analytics(city, bet.get("side", ""))

        # Deploy 1: Release funds in wallet after resolution
        # Weather bets don't return funds through this method - claiming is handled separately
        if wallet:
            try:
                wallet.release_funds('weather', bet_id, 0)
                print(f"  [WALLET] Released reservation for weather bet {bet_id}")
            except Exception as we:
                print(f"  [WALLET] Warning: fund release failed: {we}")

    def fetch_actual_temperature(self, city, date_str):
        """Fetch the actual high temperature for a city on a given date from Open-Meteo Archive API.

        Uses the same archive API as HistoricalBaseline. Requires the market date
        to be at least 2 days in the past (Open-Meteo archive data delay).

        Args:
            city: City name (must exist in CITY_COORDS)
            date_str: Date string in ISO format (YYYY-MM-DD)

        Returns:
            Actual high temperature in Fahrenheit, or None if unavailable.
        """
        coords = CITY_COORDS.get(city)
        if not coords:
            print(f"[WEATHER ANALYST] Unknown city: {city}")
            return None

        try:
            target_date = datetime.fromisoformat(date_str).date()
        except (ValueError, TypeError):
            print(f"[WEATHER ANALYST] Invalid date string: {date_str}")
            return None

        # Open-Meteo archive has ~2 day delay
        days_ago = (datetime.now().date() - target_date).days
        if days_ago < 2:
            print(f"[WEATHER ANALYST] Date {date_str} is only {days_ago} day(s) ago -- archive not yet available")
            return None

        try:
            resp = requests.get(
                HistoricalBaseline.ARCHIVE_URL,
                params={
                    "latitude": coords["lat"],
                    "longitude": coords["lon"],
                    "daily": "temperature_2m_max",
                    "temperature_unit": "fahrenheit",
                    "timezone": "auto",
                    "start_date": date_str,
                    "end_date": date_str,
                },
                timeout=15,
            )

            if resp.status_code != 200:
                print(f"[WEATHER ANALYST] Open-Meteo archive error: HTTP {resp.status_code}")
                return None

            data = resp.json()
            daily = data.get("daily", {})
            highs = daily.get("temperature_2m_max", [])

            if not highs or highs[0] is None:
                print(f"[WEATHER ANALYST] No archive data for {city} on {date_str}")
                return None

            actual_high = float(highs[0])
            print(f"[WEATHER ANALYST] {city} {date_str}: actual high = {actual_high:.1f}F")
            return actual_high

        except Exception as e:
            print(f"[WEATHER ANALYST] Archive fetch error for {city} {date_str}: {e}")
            return None

    def _update_weather_analytics(self, city, side):
        """Rebuild city and side pattern tables after a resolution."""
        if not city:
            return
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            c = conn.cursor()

            # Update weather_city_patterns for this city
            c.execute("""
                INSERT OR REPLACE INTO weather_city_patterns (city, total_bets, wins, win_rate, total_profit, last_updated)
                SELECT COALESCE(wb.city, 'unknown'), COUNT(*), SUM(b.won),
                       ROUND(CAST(SUM(b.won) AS REAL) / COUNT(*), 4),
                       ROUND(SUM(b.profit), 2), datetime('now')
                FROM bets b LEFT JOIN weather_bets wb ON wb.bet_id = b.id
                WHERE b.category='weather' AND b.status='resolved' AND LOWER(wb.city) = LOWER(?)
                GROUP BY wb.city
            """, (city,))

            # Update weather_side_patterns for this city+side
            if side:
                c.execute("""
                    INSERT OR REPLACE INTO weather_side_patterns (city, side, total_bets, wins, win_rate, total_profit, last_updated)
                    SELECT COALESCE(wb.city, 'unknown'), b.side, COUNT(*), SUM(b.won),
                           ROUND(CAST(SUM(b.won) AS REAL) / COUNT(*), 4),
                           ROUND(SUM(b.profit), 2), datetime('now')
                    FROM bets b LEFT JOIN weather_bets wb ON wb.bet_id = b.id
                    WHERE b.category='weather' AND b.status='resolved'
                      AND LOWER(wb.city) = LOWER(?) AND LOWER(b.side) = LOWER(?)
                    GROUP BY wb.city, b.side
                """, (city, side))

            # Update calibration bucket for the confidence score
            c.execute("""
                INSERT OR REPLACE INTO weather_calibration (confidence_bucket, total_bets, wins, actual_win_rate, last_updated)
                SELECT CAST(confidence_score / 10 AS INTEGER) * 10, COUNT(*), SUM(won),
                       ROUND(CAST(SUM(won) AS REAL) / COUNT(*), 4), datetime('now')
                FROM bets WHERE category='weather' AND status='resolved' AND confidence_score IS NOT NULL
                GROUP BY CAST(confidence_score / 10 AS INTEGER) * 10
            """)

            conn.commit()
            conn.close()
        except Exception as e:
            print(f"  [WEATHER ANALYST] Analytics update error: {e}")

    def check_and_resolve_weather_bets(self, wallet=None):
        """Check for unresolved weather bets and resolve them using actual temperature data.

        Finds bets that:
        1. Have category='weather' AND status='resolved' but no matching weather_resolutions row
        2. Have category='weather' AND status='pending' whose market_date is 2+ days past

        For each, fetches the actual temperature from Open-Meteo archive, determines if
        the bet won based on the temp range, and calls resolve_weather_bet() to fire
        the full credibility update chain.

        Rate-limits Open-Meteo calls with 1s sleep between fetches.
        """
        print(f"\n[WEATHER ANALYST] Checking for unresolved weather bets...")
        resolved_count = 0

        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()

            # Find weather bets that need resolution:
            # 1. status='resolved' but no weather_resolutions entry
            # 2. status='pending' with market_date 2+ days ago
            c.execute("""
                SELECT b.id, b.status, b.side, wb.city, wb.temp_range,
                       COALESCE(wp.weighted_mean, wb.weighted_mean, 0) as weighted_mean,
                       wp.market_date
                FROM bets b
                LEFT JOIN weather_bets wb ON wb.bet_id = b.id
                LEFT JOIN weather_predictions wp ON wp.bet_id = b.id
                LEFT JOIN weather_resolutions wr ON wr.bet_id = b.id
                WHERE b.category = 'weather'
                AND wr.bet_id IS NULL
                AND (
                    b.status = 'resolved'
                    OR (b.status = 'pending' AND wp.market_date IS NOT NULL
                        AND wp.market_date <= date('now', '-2 days'))
                )
                ORDER BY b.id ASC
            """)
            rows = c.fetchall()
            conn.close()

            if not rows:
                print(f"[WEATHER ANALYST] No unresolved weather bets found")
                return []

            print(f"[WEATHER ANALYST] Found {len(rows)} unresolved weather bet(s)")

            for row in rows:
                bet_id, status, side, city, temp_range_str, weighted_mean, market_date = row

                if not city or not market_date:
                    print(f"[WEATHER ANALYST] Bet {bet_id}: missing city or market_date -- skipping")
                    continue

                print(f"[WEATHER ANALYST] Processing bet {bet_id}: {city} {market_date} (status={status})")

                # Fetch actual temperature
                actual_high = self.fetch_actual_temperature(city, market_date)
                if actual_high is None:
                    print(f"[WEATHER ANALYST] Bet {bet_id}: could not fetch actual temp -- skipping")
                    time.sleep(1)
                    continue

                # Parse temp range to determine win/loss
                if temp_range_str:
                    scanner = WeatherScanner()
                    temp_range = scanner._parse_temp_range(temp_range_str)
                else:
                    temp_range = None

                if temp_range:
                    actual_rounded = round(actual_high)
                    low = temp_range.get("low", -999)
                    high = temp_range.get("high", 999)

                    # Convert temp range to F if it's in C
                    if temp_range.get("unit") == "C":
                        low = low * 9.0 / 5.0 + 32.0 if low > -500 else low
                        high = high * 9.0 / 5.0 + 32.0 if high < 500 else high
                        actual_in_range = low <= actual_high <= high
                    else:
                        actual_in_range = low <= actual_rounded <= high

                    # Determine if bet won based on side
                    if side == "yes":
                        won = actual_in_range
                    else:
                        won = not actual_in_range

                    print(f"[WEATHER ANALYST] Bet {bet_id}: actual={actual_high:.1f}F, range={temp_range_str}, "
                          f"in_range={actual_in_range}, side={side}, won={won}")
                else:
                    print(f"[WEATHER ANALYST] Bet {bet_id}: could not parse temp range '{temp_range_str}' -- resolving with temp only")

                # Ensure bet is in active_weather_bets list for resolve_weather_bet to find
                if not any(wb["bet_id"] == bet_id for wb in self.active_weather_bets):
                    self.active_weather_bets.append({
                        "bet_id": bet_id,
                        "market_id": "",
                        "market_title": "",
                        "amount": 0,
                        "side": side or "",
                        "odds": 0,
                        "city": city or "",
                        "temp_range": temp_range_str or "",
                        "weighted_mean": weighted_mean or 0,
                    })

                # If bet is still pending, resolve it in the bets table first
                if status == 'pending' and temp_range:
                    # Weather bets: mark as resolved with actual temp data as proof.
                    # Profit will be updated when Bankr redeems and returns exact USDC.
                    # For now, set profit=0 and let bet_resolver handle the money side.
                    try:
                        conn2 = sqlite3.connect(self.db_path)
                        c2 = conn2.cursor()
                        c2.execute("SELECT amount FROM bets WHERE id = ?", (bet_id,))
                        brow = c2.fetchone()
                        conn2.close()
                        if brow:
                            bet_amt = brow[0]
                            if not won:
                                # Loss is provable: actual temp outside range = $0 returned
                                profit = -bet_amt
                                self.tracker.resolve_bet(bet_id, won, profit, 0)
                                print(f"[WEATHER ANALYST] Bet {bet_id} LOST (actual temp proves it): ${profit:+.2f}")
                            else:
                                # Win: don't guess profit from odds. Leave for Bankr to redeem.
                                # Mark status so Bankr picks it up, but profit stays at 0 until redeemed.
                                print(f"[WEATHER ANALYST] Bet {bet_id} WON (actual temp confirms). "
                                      f"Profit TBD — waiting for Bankr redemption for exact USDC.")
                                # Don't call resolve_bet yet for wins — let Bankr redeem path handle it
                    except Exception as re:
                        print(f"[WEATHER ANALYST] Error resolving bet {bet_id} in DB: {re}")

                # Call existing resolution which fires credibility updates
                self.resolve_weather_bet(bet_id, actual_high, wallet=wallet)
                resolved_count += 1

                # Rate limit Open-Meteo API calls
                time.sleep(1)

        except Exception as e:
            print(f"[WEATHER ANALYST] Error checking unresolved bets: {e}")

        print(f"[WEATHER ANALYST] Resolved {resolved_count} weather bet(s)")
        return [{"type": "weather", "count": resolved_count}] if resolved_count > 0 else []

    def reset_daily_counts(self):
        self.weather_daily_bet_count = 0
        print(f"[WEATHER ANALYST] Daily bet counter reset to 0/{WEATHER_MAX_DAILY_BETS}")

    def get_weather_positions_text(self):
        if not self.active_weather_bets:
            return "  None"
        lines = []
        for wb in self.active_weather_bets:
            city = wb.get("city", "?").title()
            rng = wb.get("temp_range", "?")
            side = wb.get("side", "?").upper()
            amt = wb.get("amount", 0)
            lines.append(f"  {side}: {city} {rng} (${amt:.2f})")
        return "\n".join(lines)



class PatternAnalyzer:
    """Automated pattern finder that analyzes betting history to find strategy improvements.

    Runs every 30 hours. Reads all resolved bets, compares:
    - What data sources predicted
    - What the agent actually bet (side + odds)
    - What the market outcome was
    - What the OTHER side would have paid

    Generates suggestions: change strategy, or confirm it working.
    v4.2 -- Mar 6 2026
    """

    MIN_BETS_FOR_PATTERN = 5

    def __init__(self, db_path, notifier=None):
        self.db_path = db_path
        self.notifier = notifier

    def run_analysis(self):
        """Main entry point. Run full pattern analysis and return report."""
        import sqlite3
        print("\n" + "=" * 60)
        print("[DETECTIVE] Running 30h strategy review...")
        print("=" * 60)

        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()

            report_sections = []

            city_report = self._analyze_by_city(c)
            if city_report:
                report_sections.append(city_report)

            side_report = self._analyze_by_side(c)
            if side_report:
                report_sections.append(side_report)

            forecast_report = self._analyze_forecast_vs_outcome(c)
            if forecast_report:
                report_sections.append(forecast_report)

            whatif_report = self._run_whatif_simulation(c)
            if whatif_report:
                report_sections.append(whatif_report)

            crypto_report = self._analyze_crypto(c)
            if crypto_report:
                report_sections.append(crypto_report)

            conn.close()

            full_report = self._compile_report(report_sections)
            print(full_report)

            self._log_analysis(full_report)

            if any(s.get("actionable") for s in report_sections):
                self._send_alert(report_sections)

            return report_sections

        except Exception as e:
            print(f"[DETECTIVE] Error: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _analyze_by_city(self, cursor):
        """Win rate by city with enough data."""
        cursor.execute("""
            SELECT
                CASE
                    WHEN market_title LIKE '%Atlanta%' THEN 'Atlanta'
                    WHEN market_title LIKE '%Seattle%' THEN 'Seattle'
                    WHEN market_title LIKE '%Miami%' THEN 'Miami'
                    WHEN market_title LIKE '%Chicago%' THEN 'Chicago'
                    WHEN market_title LIKE '%Dallas%' THEN 'Dallas'
                    WHEN market_title LIKE '%New York%' THEN 'NYC'
                    ELSE 'Other'
                END as city,
                COUNT(*) as total,
                SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN won=0 THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN side='yes' THEN 1 ELSE 0 END) as yes_bets,
                SUM(CASE WHEN side='no' THEN 1 ELSE 0 END) as no_bets,
                COALESCE(SUM(profit), 0) as total_pnl
            FROM bets WHERE category='weather' AND status='resolved'
            GROUP BY city HAVING total >= 3
            ORDER BY total DESC
        """)
        rows = cursor.fetchall()
        if not rows:
            return None

        findings = []
        for r in rows:
            wr = r["wins"] / r["total"] * 100 if r["total"] else 0
            finding = {
                "city": r["city"], "total": r["total"], "wins": r["wins"],
                "losses": r["losses"], "win_rate": wr,
                "yes_bets": r["yes_bets"], "no_bets": r["no_bets"],
                "pnl": r["total_pnl"]
            }
            if wr < 35 and r["total"] >= self.MIN_BETS_FOR_PATTERN:
                finding["flag"] = "LOSING"
                finding["suggestion"] = f"{r['city']}: {wr:.0f}% WR over {r['total']} bets. Check side selection or skip."
            elif wr > 65 and r["total"] >= self.MIN_BETS_FOR_PATTERN:
                finding["flag"] = "WINNING"
                finding["suggestion"] = f"{r['city']}: {wr:.0f}% WR over {r['total']} bets. Lean in harder."
            findings.append(finding)

        actionable = any(f.get("flag") for f in findings)
        return {"section": "City Analysis", "findings": findings, "actionable": actionable}

    def _analyze_by_side(self, cursor):
        """Win rate by side (YES vs NO) across all weather bets."""
        cursor.execute("""
            SELECT side,
                COUNT(*) as total,
                SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as wins,
                AVG(odds) as avg_odds,
                COALESCE(SUM(profit), 0) as total_pnl
            FROM bets WHERE category='weather' AND status='resolved' AND side IS NOT NULL
            GROUP BY side
        """)
        rows = cursor.fetchall()
        if not rows:
            return None

        findings = []
        for r in rows:
            wr = r["wins"] / r["total"] * 100 if r["total"] else 0
            findings.append({
                "side": r["side"], "total": r["total"], "wins": r["wins"],
                "win_rate": wr, "avg_odds": r["avg_odds"], "pnl": r["total_pnl"]
            })

        return {"section": "Side Analysis", "findings": findings, "actionable": False}

    def _analyze_forecast_vs_outcome(self, cursor):
        """THE GOLD: Find segments where forecast is accurate but we still lose.
        Accurate forecast + losing = side selection bug, not model bug."""
        cursor.execute("""
            SELECT
                CASE
                    WHEN b.market_title LIKE '%Atlanta%' THEN 'Atlanta'
                    WHEN b.market_title LIKE '%Seattle%' THEN 'Seattle'
                    WHEN b.market_title LIKE '%Miami%' THEN 'Miami'
                    WHEN b.market_title LIKE '%Chicago%' THEN 'Chicago'
                    WHEN b.market_title LIKE '%Dallas%' THEN 'Dallas'
                    WHEN b.market_title LIKE '%New York%' THEN 'NYC'
                    ELSE 'Other'
                END as city,
                COUNT(*) as total,
                SUM(CASE WHEN b.won=1 THEN 1 ELSE 0 END) as wins,
                AVG(ABS(wr.error)) as avg_error,
                AVG(wr.error) as avg_bias,
                b.side as bet_side
            FROM bets b
            JOIN weather_resolutions wr ON b.id = wr.bet_id
            WHERE b.category='weather' AND b.status='resolved'
            GROUP BY city, bet_side
            HAVING total >= 3
            ORDER BY avg_error ASC
        """)
        rows = cursor.fetchall()
        if not rows:
            return None

        findings = []
        for r in rows:
            wr = r["wins"] / r["total"] * 100 if r["total"] else 0
            finding = {
                "city": r["city"], "side": r["bet_side"], "total": r["total"],
                "wins": r["wins"], "win_rate": wr,
                "avg_error": r["avg_error"], "avg_bias": r["avg_bias"]
            }

            # THE PATTERN: accurate forecast + low win rate = side selection bug
            if r["avg_error"] and r["avg_error"] <= 3.0 and wr < 40 and r["total"] >= self.MIN_BETS_FOR_PATTERN:
                finding["flag"] = "SIDE_BUG"
                finding["suggestion"] = (
                    f"{r['city']} {r['bet_side'].upper()}: Forecast accurate "
                    f"({r['avg_error']:.1f}F avg error) but only {wr:.0f}% WR. "
                    f"Forecast is right, side is wrong. TRY OPPOSITE SIDE."
                )
            elif r["avg_error"] and r["avg_error"] <= 3.0 and wr >= 60:
                finding["flag"] = "CONFIRMED"
                finding["suggestion"] = (
                    f"{r['city']} {r['bet_side'].upper()}: Forecast accurate "
                    f"({r['avg_error']:.1f}F) and {wr:.0f}% WR. Strategy is working. No change needed."
                )
            findings.append(finding)

        actionable = any(f.get("flag") == "SIDE_BUG" for f in findings)
        return {"section": "Forecast vs Outcome (Side Bug Detection)", "findings": findings, "actionable": actionable}

    def _run_whatif_simulation(self, cursor):
        """For each city, calculate what the OTHER side would have paid."""
        cursor.execute("""
            SELECT
                CASE
                    WHEN market_title LIKE '%Atlanta%' THEN 'Atlanta'
                    WHEN market_title LIKE '%Seattle%' THEN 'Seattle'
                    WHEN market_title LIKE '%Miami%' THEN 'Miami'
                    WHEN market_title LIKE '%Chicago%' THEN 'Chicago'
                    WHEN market_title LIKE '%Dallas%' THEN 'Dallas'
                    WHEN market_title LIKE '%New York%' THEN 'NYC'
                    ELSE 'Other'
                END as city,
                side, won, odds, amount, profit
            FROM bets WHERE category='weather' AND status='resolved'
            ORDER BY city
        """)
        rows = cursor.fetchall()
        if not rows:
            return None

        from collections import defaultdict
        city_data = defaultdict(lambda: {"actual_pnl": 0, "whatif_pnl": 0, "count": 0})

        for r in rows:
            city = r["city"]
            city_data[city]["count"] += 1
            city_data[city]["actual_pnl"] += (r["profit"] or 0)

            other_price = round(1.0 - r["odds"], 2) if r["odds"] else 0.5
            if other_price <= 0.01:
                other_price = 0.01

            if r["won"] == 0:
                city_data[city]["whatif_pnl"] += r["amount"] * (1.0 / other_price - 1.0)
            else:
                city_data[city]["whatif_pnl"] -= r["amount"]

        findings = []
        for city, data in sorted(city_data.items()):
            if data["count"] < 3:
                continue
            swing = data["whatif_pnl"] - data["actual_pnl"]
            finding = {
                "city": city, "count": data["count"],
                "actual_pnl": round(data["actual_pnl"], 2),
                "whatif_pnl": round(data["whatif_pnl"], 2),
                "swing": round(swing, 2)
            }
            if swing > 5.0 and data["count"] >= self.MIN_BETS_FOR_PATTERN:
                finding["flag"] = "FLIP_CANDIDATE"
                finding["suggestion"] = (
                    f"{city}: Other side = +${data['whatif_pnl']:.2f} vs actual "
                    f"${data['actual_pnl']:+.2f} (${swing:+.2f} swing over {data['count']} bets). "
                    f"Data says flip the side."
                )
            elif swing < -10.0 and data["count"] >= self.MIN_BETS_FOR_PATTERN:
                finding["flag"] = "CORRECT_SIDE"
                finding["suggestion"] = (
                    f"{city}: Current strategy is correct. Other side would lose "
                    f"${abs(swing):.2f} more. Keep current approach."
                )
            findings.append(finding)

        actionable = any(f.get("flag") == "FLIP_CANDIDATE" for f in findings)
        return {"section": "What-If Simulation", "findings": findings, "actionable": actionable}

    def _analyze_crypto(self, cursor):
        """Basic crypto pattern analysis by side."""
        cursor.execute("""
            SELECT side,
                COUNT(*) as total,
                SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as wins,
                AVG(odds) as avg_odds,
                COALESCE(SUM(profit), 0) as total_pnl
            FROM bets WHERE category='crypto' AND status='resolved' AND side IS NOT NULL
            GROUP BY side
        """)
        rows = cursor.fetchall()
        if not rows:
            return None

        findings = []
        for r in rows:
            wr = r["wins"] / r["total"] * 100 if r["total"] else 0
            findings.append({
                "side": r["side"], "total": r["total"], "wins": r["wins"],
                "win_rate": wr, "avg_odds": r["avg_odds"], "pnl": r["total_pnl"]
            })

        return {"section": "Crypto Side Analysis", "findings": findings, "actionable": False}

    def _compile_report(self, sections):
        """Build human-readable report."""
        lines = []
        lines.append("\n" + "=" * 60)
        lines.append("PATTERN ANALYZER REPORT")
        lines.append("=" * 60)

        for section in sections:
            actionable_tag = " [ACTION NEEDED]" if section.get("actionable") else ""
            lines.append(f"\n--- {section['section']}{actionable_tag} ---")

            for f in section["findings"]:
                parts = []
                if "city" in f:
                    parts.append(f["city"])
                if "side" in f:
                    parts.append(f["side"].upper())
                if "total" in f:
                    wr = f.get("win_rate", 0)
                    wins = f.get("wins", 0)
                    losses = f["total"] - wins
                    parts.append(f"{wins}W/{losses}L ({wr:.0f}%)")
                if "avg_error" in f and f["avg_error"]:
                    parts.append(f"err:{f['avg_error']:.1f}F")
                if "avg_bias" in f and f["avg_bias"]:
                    parts.append(f"bias:{f['avg_bias']:+.1f}F")
                if "swing" in f:
                    parts.append(f"swing:${f['swing']:+.2f}")
                if "pnl" in f:
                    parts.append(f"P&L:${f['pnl']:+.2f}")

                flag = f" [{f['flag']}]" if f.get("flag") else ""
                lines.append(f"  {' | '.join(parts)}{flag}")

                if f.get("suggestion"):
                    lines.append(f"    >> {f['suggestion']}")

        lines.append("\n" + "=" * 60)
        return "\n".join(lines)

    def _log_analysis(self, report):
        """Save analysis to strategy_log table."""
        import sqlite3
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            c = conn.cursor()
            c.execute(
                "INSERT INTO strategy_log (version, change_type, description, rationale, expected_impact) VALUES (?, ?, ?, ?, ?)",
                ("auto", "PATTERN_ANALYSIS", report[:500], "Automated 30h pattern review", "Informational")
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"  [DETECTIVE] Log error: {e}")

    def _send_alert(self, sections):
        """Send actionable findings to Telegram via notifier."""
        if not self.notifier:
            return

        actionable = []
        for s in sections:
            if s.get("actionable"):
                for f in s["findings"]:
                    if f.get("suggestion") and f.get("flag") in ("SIDE_BUG", "FLIP_CANDIDATE", "LOSING"):
                        actionable.append(f["suggestion"])

        if actionable:
            msg = "PATTERN ANALYZER ALERT\n\n"
            msg += "\n\n".join(actionable[:5])
            msg += "\n\nReview and approve changes manually."
            try:
                self.notifier.notify_alert(msg)
                print("  [DETECTIVE] Alert sent to Telegram")
            except Exception as e:
                print(f"  [DETECTIVE] Alert error: {e}")
