"""
Baggins Capital — Company Clock (V3 Upgrade)
Single time authority for all 24 employees.

ET only. DST handled automatically. Hook system for coordinated scheduling.
No employee calls datetime.now() directly. Everything routes through here.

Department: Infrastructure
Reports to: The Manager
"""
import pytz
import json
import threading
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# The company runs on Eastern Time
COMPANY_TZ = pytz.timezone('America/New_York')


class ClockContext:
    """Full temporal context returned on every query."""

    def __init__(self, now_et):
        self._now = now_et

        # CURRENT MOMENT
        self.timestamp_et = now_et.isoformat()
        self.unix_timestamp = int(now_et.timestamp())
        self.timezone = 'EDT' if now_et.dst() else 'EST'
        self.utc_offset = -4 if now_et.dst() else -5

        # TIME OF DAY
        self.hour = now_et.hour
        self.minute = now_et.minute
        self.second = now_et.second
        self.time_string = now_et.strftime('%I:%M %p ET')
        self.is_dst = bool(now_et.dst())

        # DAY CONTEXT
        self.day_of_week = now_et.strftime('%A')
        self.day_of_week_num = now_et.weekday()  # 0=Monday
        self.is_weekday = now_et.weekday() < 5
        self.is_weekend = now_et.weekday() >= 5
        self.day_of_month = now_et.day
        self.day_of_year = now_et.timetuple().tm_yday

        # WEEK CONTEXT
        self.week_number = now_et.isocalendar()[1]
        self.is_first_week = now_et.day <= 7
        # Last week: next week's month != this month
        next_week = now_et + timedelta(days=7)
        self.is_last_week = next_week.month != now_et.month

        # MONTH CONTEXT
        self.month = now_et.strftime('%B')
        self.month_num = now_et.month
        self.month_short = now_et.strftime('%b')
        # Days in month
        if now_et.month == 12:
            next_month_start = now_et.replace(year=now_et.year + 1, month=1, day=1)
        else:
            next_month_start = now_et.replace(month=now_et.month + 1, day=1)
        self.days_in_month = (next_month_start - now_et.replace(day=1)).days
        self.is_first_day = now_et.day == 1
        self.is_last_day = now_et.day == self.days_in_month
        self.month_progress_pct = round(now_et.day / self.days_in_month, 2)

        # QUARTER CONTEXT
        self.quarter = (now_et.month - 1) // 3 + 1
        self.quarter_label = f'Q{self.quarter}'
        q_start_month = (self.quarter - 1) * 3 + 1
        self.quarter_start = now_et.replace(month=q_start_month, day=1).strftime('%Y-%m-%d')
        q_end_month = self.quarter * 3
        if q_end_month == 12:
            q_end = datetime(now_et.year, 12, 31, tzinfo=now_et.tzinfo)
        else:
            q_end = datetime(now_et.year, q_end_month + 1, 1, tzinfo=now_et.tzinfo) - timedelta(days=1)
        self.quarter_end = q_end.strftime('%Y-%m-%d')
        q_start_dt = now_et.replace(month=q_start_month, day=1)
        self.day_of_quarter = (now_et - q_start_dt).days + 1
        self.is_quarter_end = (q_end - now_et.replace(hour=0, minute=0, second=0)).days <= 7

        # YEAR CONTEXT
        self.year = now_et.year
        self.is_leap_year = (now_et.year % 4 == 0 and
                             (now_et.year % 100 != 0 or now_et.year % 400 == 0))
        days_in_year = 366 if self.is_leap_year else 365
        self.year_progress_pct = round(self.day_of_year / days_in_year, 2)

        # TRADING CONTEXT (computed by CompanyClock, filled in later)
        self.active_windows = []
        self.next_window_open = None
        self.minutes_to_reset = 0
        self.dst_transition_soon = False
        self.dst_transition_date = None


class CompanyClock:
    """ET time authority for the entire company with hook system."""

    # Trading window definitions: name -> (open_hour, open_min, close_hour, close_min, days)
    TRADING_WINDOWS = {
        'Morning Weather Window': (8, 0, 10, 0, range(7)),
        'Afternoon Weather Window': (14, 0, 16, 0, range(7)),
        'Evening Weather Window': (21, 0, 23, 0, range(7)),
        'Crypto Trading Window': (0, 0, 23, 59, range(7)),
        'Sports Window': (6, 0, 23, 59, range(7)),
        'Scalper Window - Fire :08': (None, 8, None, 9, range(7)),
        'Scalper Window - Fire :23': (None, 23, None, 24, range(7)),
        'Scalper Window - Fire :38': (None, 38, None, 39, range(7)),
        'Scalper Window - Fire :53': (None, 53, None, 54, range(7)),
        'Pre-Market Prep Window': (7, 45, 8, 0, range(7)),
        'Daily Reset Window': (22, 0, 22, 30, range(7)),
    }

    # Hook definitions: name -> (type, fire_condition_description)
    HOOK_DEFS = {
        'PRE_MARKET_PREP_HOOK': ('time', (7, 45)),
        'MORNING_OPEN_HOOK': ('time', (8, 0)),
        'MIDDAY_CHECK_HOOK': ('time', (12, 0)),
        'AFTERNOON_OPEN_HOOK': ('time', (14, 0)),
        'AFTERNOON_CLOSE_HOOK': ('time', (16, 0)),
        'EVENING_PREP_HOOK': ('time', (20, 45)),
        'EVENING_OPEN_HOOK': ('time', (21, 0)),
        'PRE_RESET_HOOK': ('time', (21, 45)),
        'DAILY_RESET_HOOK': ('time', (22, 0)),
        'MONDAY_OPEN_HOOK': ('calendar', 'monday_8am'),
        'FRIDAY_CLOSE_HOOK': ('calendar', 'friday_22pm'),
        'MONTH_START_HOOK': ('calendar', 'first_day_8am'),
        'MONTH_END_HOOK': ('calendar', 'last_day_8am'),
        'QUARTER_END_HOOK': ('calendar', 'quarter_end_window'),
        'DST_WARNING_HOOK': ('calendar', 'dst_7days_before'),
        'DST_TRANSITION_HOOK': ('calendar', 'dst_transition_moment'),
        # Condition hooks are fired programmatically, not by time
        'CIRCUIT_BREAKER_HOOK': ('condition', None),
        'CIRCUIT_BREAKER_CLEAR_HOOK': ('condition', None),
        'DAILY_CAP_WARNING_HOOK': ('condition', None),
        'DAILY_CAP_HIT_HOOK': ('condition', None),
        'WIN_RATE_DROP_HOOK': ('condition', None),
        'BALANCE_NEW_LOW_HOOK': ('condition', None),
        'BALANCE_NEW_HIGH_HOOK': ('condition', None),
        'ECONOMIC_CALENDAR_HOOK': ('condition', None),
    }

    def __init__(self):
        self.tz = COMPANY_TZ
        self._hook_handlers = defaultdict(list)  # hook_name -> [handler_fn, ...]
        self._hook_last_fired = {}  # hook_name -> datetime
        self._lock = threading.Lock()

    # ══════════════════════════════════════════════════════════════
    # CORE TIME QUERY
    # ══════════════════════════════════════════════════════════════

    def get_context(self):
        """Get full temporal context. Primary method for all employees."""
        now = datetime.now(self.tz)
        ctx = ClockContext(now)

        # Fill in trading context
        ctx.active_windows = self.get_active_windows()
        ctx.minutes_to_reset = self._minutes_to_reset(now)
        ctx.dst_transition_soon = self.is_dst_transition_soon()
        ctx.dst_transition_date = self._get_next_dst_transition_str()

        # Populate next_window_open
        if not ctx.active_windows:
            # Find the soonest window to open
            soonest = None
            soonest_mins = float('inf')
            for wname in self.TRADING_WINDOWS:
                mins = self.minutes_until_window(wname)
                if 0 < mins < soonest_mins:
                    soonest_mins = mins
                    soonest = wname
            ctx.next_window_open = soonest
        else:
            ctx.next_window_open = ctx.active_windows[0]

        return ctx

    # ══════════════════════════════════════════════════════════════
    # WINDOW CHECKS
    # ══════════════════════════════════════════════════════════════

    def is_window_open(self, window_name):
        """Check if a specific trading window is currently open."""
        now = datetime.now(self.tz)
        window = self.TRADING_WINDOWS.get(window_name)
        if not window:
            return False

        open_h, open_m, close_h, close_m, days = window

        if now.weekday() not in days:
            return False

        # Scalper windows: minute-based (open_h is None)
        if open_h is None:
            return open_m <= now.minute < close_m

        # Normal windows: hour+minute based
        open_mins = open_h * 60 + open_m
        close_mins = close_h * 60 + close_m
        current_mins = now.hour * 60 + now.minute

        return open_mins <= current_mins < close_mins

    def get_active_windows(self):
        """Get list of currently open trading windows."""
        return [name for name in self.TRADING_WINDOWS if self.is_window_open(name)]

    def minutes_until_window(self, window_name):
        """Minutes until a specific window opens. Returns 0 if open now."""
        if self.is_window_open(window_name):
            return 0
        window = self.TRADING_WINDOWS.get(window_name)
        if not window:
            return -1

        now = datetime.now(self.tz)
        open_h, open_m, close_h, close_m, days = window

        if open_h is None:  # Scalper minute-based
            return max(0, open_m - now.minute)

        target = now.replace(hour=open_h, minute=open_m, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return int((target - now).total_seconds() / 60)

    def is_scalper_fire_time(self):
        """Check if current minute is a Scalper firing time (:08/:23/:38/:53)."""
        minute = datetime.now(self.tz).minute
        return minute in (8, 23, 38, 53)

    # ══════════════════════════════════════════════════════════════
    # HOOK SYSTEM
    # ══════════════════════════════════════════════════════════════

    def register_hook(self, hook_name, handler):
        """Register a handler function for a hook. Called on module startup."""
        with self._lock:
            if handler not in self._hook_handlers[hook_name]:
                self._hook_handlers[hook_name].append(handler)

    def fire_hook(self, hook_name, payload=None):
        """Fire a hook — calls all registered handlers. Thread-safe."""
        handlers = self._hook_handlers.get(hook_name, [])
        if not handlers:
            return

        now = datetime.now(self.tz)

        # Idempotency: don't re-fire within 90 seconds
        last = self._hook_last_fired.get(hook_name)
        if last and (now - last).total_seconds() < 90:
            return

        self._hook_last_fired[hook_name] = now
        fired = 0

        for handler in handlers:
            try:
                handler(payload or {})
                fired += 1
            except Exception as e:
                print(f"  [CLOCK] Hook {hook_name} handler error: {e}")

        if fired:
            print(f"  [CLOCK] Fired {hook_name} -> {fired} handlers")

    def check_and_fire_hooks(self):
        """Called every Manager cycle. Checks time-based and calendar hooks."""
        now = datetime.now(self.tz)
        h, m = now.hour, now.minute

        # Time-of-day hooks (fire within 2-min window)
        time_hooks = {
            'PRE_MARKET_PREP_HOOK': (7, 45),
            'MORNING_OPEN_HOOK': (8, 0),
            'MIDDAY_CHECK_HOOK': (12, 0),
            'AFTERNOON_OPEN_HOOK': (14, 0),
            'AFTERNOON_CLOSE_HOOK': (16, 0),
            'EVENING_PREP_HOOK': (20, 45),
            'EVENING_OPEN_HOOK': (21, 0),
            'PRE_RESET_HOOK': (21, 45),
            'DAILY_RESET_HOOK': (22, 0),
        }

        for hook_name, (fire_h, fire_m) in time_hooks.items():
            if h == fire_h and fire_m <= m < fire_m + 2:
                self.fire_hook(hook_name, {'time': now.isoformat()})

        # Calendar hooks
        if now.weekday() == 0 and h == 8 and 0 <= m < 2:  # Monday 8am
            self.fire_hook('MONDAY_OPEN_HOOK', {'time': now.isoformat()})
        if now.weekday() == 4 and h == 22 and 0 <= m < 2:  # Friday 10pm
            self.fire_hook('FRIDAY_CLOSE_HOOK', {'time': now.isoformat()})
        if now.day == 1 and h == 8 and 0 <= m < 2:  # First of month
            self.fire_hook('MONTH_START_HOOK', {'time': now.isoformat()})

        # Last day of month
        ctx = ClockContext(now)
        if ctx.is_last_day and h == 8 and 0 <= m < 2:
            self.fire_hook('MONTH_END_HOOK', {'time': now.isoformat()})

        # Quarter end window (last 7 days)
        if ctx.is_quarter_end and h == 8 and 0 <= m < 2:
            self.fire_hook('QUARTER_END_HOOK', {'time': now.isoformat()})

        # DST warning (7 days before)
        if self.is_dst_transition_soon() and h == 8 and 0 <= m < 2:
            self.fire_hook('DST_WARNING_HOOK', {
                'transition_date': self._get_next_dst_transition_str()
            })

        # DST transition (day of transition, fire at 1am)
        transition = self._get_next_dst_transition()
        if transition and transition.date() == now.date() and h == 1 and 0 <= m < 2:
            self.fire_hook('DST_TRANSITION_HOOK', {
                'transition_date': transition.isoformat(),
                'new_offset': self._predict_post_transition_offset()
            })

    def _predict_post_transition_offset(self):
        """Predict UTC offset after DST transition."""
        now = datetime.now(self.tz)
        return -4 if not now.dst() else -5  # Flips on transition

    # ══════════════════════════════════════════════════════════════
    # DST HANDLING
    # ══════════════════════════════════════════════════════════════

    def is_dst_transition_soon(self):
        """True if DST transition within 7 days."""
        transition = self._get_next_dst_transition()
        if transition is None:
            return False
        now = datetime.now(self.tz)
        return 0 <= (transition - now).days <= 7

    def _get_next_dst_transition(self):
        """Get the next DST transition datetime."""
        now = datetime.now(self.tz)
        year = now.year

        # Spring forward: second Sunday of March at 2am
        march_1 = datetime(year, 3, 1, tzinfo=self.tz)
        # Find second Sunday
        day = 1
        sundays = 0
        while sundays < 2:
            d = datetime(year, 3, day, tzinfo=self.tz)
            if d.weekday() == 6:  # Sunday
                sundays += 1
            if sundays < 2:
                day += 1
        spring = self.tz.localize(datetime(year, 3, day, 2, 0))

        # Fall back: first Sunday of November at 2am
        day = 1
        while datetime(year, 11, day).weekday() != 6:
            day += 1
        fall = self.tz.localize(datetime(year, 11, day, 2, 0))

        # Return the next one
        for t in sorted([spring, fall]):
            if t > now:
                return t

        # Next year's spring
        year += 1
        day = 1
        sundays = 0
        while sundays < 2:
            d = datetime(year, 3, day)
            if d.weekday() == 6:
                sundays += 1
            if sundays < 2:
                day += 1
        return self.tz.localize(datetime(year, 3, day, 2, 0))

    def _get_next_dst_transition_str(self):
        """String representation of next DST transition date."""
        t = self._get_next_dst_transition()
        return t.strftime('%Y-%m-%d') if t else None

    def _minutes_to_reset(self, now):
        """Minutes until 22:00 ET daily reset."""
        reset = now.replace(hour=22, minute=0, second=0, microsecond=0)
        if now >= reset:
            reset += timedelta(days=1)
        return int((reset - now).total_seconds() / 60)

    # ══════════════════════════════════════════════════════════════
    # BACKWARD-COMPATIBLE MODULE-LEVEL FUNCTIONS
    # ══════════════════════════════════════════════════════════════


# Singleton instance
_clock = CompanyClock()


def get_clock():
    """Get the singleton CompanyClock instance."""
    return _clock


def get_context():
    """Get full temporal context."""
    return _clock.get_context()


# ── Backward-compatible functions (existing code uses these) ──

def now_et():
    """Get current time in Eastern Time (handles DST)."""
    return datetime.now(COMPANY_TZ)


def now_utc():
    """Get current time in UTC."""
    return datetime.now(timezone.utc)


def is_weekend():
    """Saturday=5, Sunday=6."""
    return now_et().weekday() >= 5


def is_weekday():
    return not is_weekend()


def current_hour_et():
    """Current hour in ET (0-23)."""
    return now_et().hour


def current_day_name():
    """e.g. 'Monday', 'Saturday'."""
    return now_et().strftime('%A')


def in_hours(hours_list):
    """Check if current ET hour is in the given list."""
    return current_hour_et() in hours_list


def in_window(windows):
    """Check if current ET hour falls in any (start, end) window."""
    h = current_hour_et()
    return any(start <= h < end for start, end in windows)


def status():
    """Print company clock status."""
    et = now_et()
    day = current_day_name()
    weekend = " (WEEKEND)" if is_weekend() else ""
    return f"{day} {et.strftime('%I:%M %p')} ET{weekend}"
