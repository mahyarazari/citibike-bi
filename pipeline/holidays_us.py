"""US federal holidays (observed) computed by rule - no external package needed."""
from __future__ import annotations

from datetime import date, timedelta


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    d = date(year, month, 1)
    d += timedelta(days=(weekday - d.weekday()) % 7)
    return d + timedelta(weeks=n - 1)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    d = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    return d - timedelta(days=(d.weekday() - weekday) % 7)


def _observed(d: date) -> date:
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


def us_holidays(start_year: int, end_year: int) -> dict[date, str]:
    out: dict[date, str] = {}
    for y in range(start_year, end_year + 1):
        out[_observed(date(y, 1, 1))] = "New Year's Day"
        out[_nth_weekday(y, 1, 0, 3)] = "Martin Luther King Jr. Day"
        out[_nth_weekday(y, 2, 0, 3)] = "Presidents' Day"
        out[_last_weekday(y, 5, 0)] = "Memorial Day"
        out[_observed(date(y, 6, 19))] = "Juneteenth"
        out[_observed(date(y, 7, 4))] = "Independence Day"
        out[_nth_weekday(y, 9, 0, 1)] = "Labor Day"
        out[_nth_weekday(y, 10, 0, 2)] = "Columbus Day"
        out[_observed(date(y, 11, 11))] = "Veterans Day"
        out[_nth_weekday(y, 11, 3, 4)] = "Thanksgiving"
        out[_observed(date(y, 12, 25))] = "Christmas Day"
    return out
