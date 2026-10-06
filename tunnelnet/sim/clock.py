"""Virtual clock used inside a simulation round.

The real wall clock is forbidden during a round so that replaying the same
round always produces the same timestamps. Time advances in fixed, monotonic
steps from a constant epoch.
"""

from __future__ import annotations

EPOCH = "2026-01-01T00:00:00Z"


def stamp_at(tick: int) -> str:
    """Deterministic timestamp for ``tick`` seconds past the epoch."""

    if tick <= 0:
        return EPOCH
    day, second = divmod(tick, 86400)
    hour, second = divmod(second, 3600)
    minute, second = divmod(second, 60)
    date = _date_for(day)
    return f"{date}T{hour:02d}:{minute:02d}:{second:02d}Z"


def _date_for(day: int) -> str:
    year, first_day = 2026, 0
    while True:
        year_days = 366 if _leap(year) else 365
        if first_day + year_days > day:
            break
        first_day += year_days
        year += 1
    month_days = (31, 29 if _leap(year) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    into_year = day - first_day
    month = 1
    for span in month_days:
        if into_year < span:
            break
        into_year -= span
        month += 1
    return f"{year:04d}-{month:02d}-{into_year + 1:02d}"


def _leap(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


class DeterministicClock:
    """Hands out one timestamp per logical tick."""

    def __init__(self, step_seconds: int = 60) -> None:
        if step_seconds < 1:
            raise ValueError("clock step must be positive")
        self._step = step_seconds
        self._tick = 0

    def now(self) -> str:
        return stamp_at(self._tick * self._step)

    def advance(self) -> str:
        self._tick += 1
        return self.now()

    def tick(self) -> int:
        return self._tick
