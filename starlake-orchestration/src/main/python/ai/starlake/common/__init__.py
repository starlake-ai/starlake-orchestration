#
# Copyright © 2025 Starlake AI (https://starlake.ai)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import functools

import re

from croniter import croniter
from croniter.croniter import CroniterBadCronError

from datetime import datetime, timedelta
from dateutil import parser

import pytz

from typing import Dict, List, Optional, Tuple, Union

def keep_ascii_only(text):
    return re.sub(r'[^\x00-\x7F]+', '_', text)

def sanitize_id(id: str):
    return keep_ascii_only(re.sub("[^a-zA-Z0-9\\-_]", "_", id.replace("$", "S")))

class MissingEnvironmentVariable(Exception):
    pass

from enum import Enum

class StarlakeCronPeriod(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"

    def __str__(self):
        return self.value

    @classmethod
    def from_str(cls, value: str):
        """Returns an instance of StarlakeCronPeriod if the value is valid, otherwise raise a ValueError exception."""
        try:
            return cls(value.lower())
        except ValueError:
            raise ValueError(f"Unsupported cron period: {value}")

# Starlake parameters
class StarlakeParameters(str, Enum):
    # The parameters are prefixed with "sl_" to avoid conflicts with other parameters.
    __SL_PREFIX = "sl_"

    SCHEDULED_DATE_PARAMETER = f"{__SL_PREFIX}scheduled_date"
    URI_PARAMETER = f"{__SL_PREFIX}uri"
    SINK_PARAMETER = f"{__SL_PREFIX}sink"
    CRON_PARAMETER = f"{__SL_PREFIX}cron"
    FRESHNESS_PARAMETER = f"{__SL_PREFIX}freshness"
    DATA_INTERVAL_START_PARAMETER = f"{__SL_PREFIX}data_interval_start"
    DATA_INTERVAL_END_PARAMETER = f"{__SL_PREFIX}data_interval_end"
    DRY_RUN_PARAMETER = f"{__SL_PREFIX}dry_run"
    # Runtime starlake --options carried by dataset events, as a dict of sections:
    # {"all": {key: value}, "<domain.task>": {key: value}} — the "all" section
    # applies to every task of the triggered pipeline, a task-specific section
    # only to that task (precedence: static options < "all" < task-specific).
    OPTIONS_PARAMETER = f"{__SL_PREFIX}options"
    # How a key of OPTIONS_PARAMETER merges when coalesced events carry it, same
    # sections: {"all": {key: {"policy": "union", "separator": "|"}}} — see
    # ai.starlake.sl_options (issue #158).
    OPTIONS_MERGE_PARAMETER = f"{__SL_PREFIX}options_merge"

    def __str__(self):
        return self.value

TODAY = datetime.today().strftime('%Y-%m-%d')

def asQueryParameters(parameters: Union[dict,None]=None) -> str:
    from urllib.parse import quote
    if parameters is None:
        parameters = dict()
    if parameters.__len__() > 0:
        return '?' + '&'.join(list(f'{quote(str(k))}={quote(str(v))}' for (k, v) in parameters.items()))
    else:
        return ''

def cron_start_time(timezone: str = 'UTC') -> datetime:
    return datetime.fromtimestamp(datetime.now().timestamp()).astimezone(pytz.timezone(timezone))

sl_schedule_format = '%Y%m%dT%H%M'

def sl_schedule(cron: str, start_time: Optional[Union[str, datetime]] = None, format: str = sl_schedule_format, timezone: str = 'UTC') -> str:
    if start_time is None:
        start_time = cron_start_time(timezone)
    elif isinstance(start_time, str):
        start_time = parser.isoparse(start_time).astimezone(pytz.timezone(timezone))
    return croniter(cron, start_time).get_prev(datetime).strftime(format)

def get_cron_frequency(cron_expression) -> timedelta:
    """
    Calculate the timedelta between 2 executions of a cron expression.
    :param cron_expression: A string representing the cron expression.
    :raise ValueError: If the cron expression is invalid.
    :return: The timedelta between 2 executions of the cron expression.
    """
    if not is_valid_cron(cron_expression):
        raise ValueError(f"Invalid cron expression: {cron_expression}")
    iter = croniter(cron_expression)
    next_run = iter.get_next(datetime)
    next_run_2 = iter.get_next(datetime)
    return next_run_2 - next_run

def sort_crons_by_frequency(cron_expressions, reference_time: Optional[datetime] = None, **kwargs) -> Tuple[Dict[int, List[str]], List[str]]:
    """
    Sort cron expressions by their frequency.

    :param cron_expressions: A list of cron expressions.
    :param reference_time: The optional reference date time for calculating next executions. If None, Defaults to today at 00:00:00 UTC.
    :raise ValueError: If the cron expression is invalid.
    :return: A tuple of (cron expressions grouped by frequency, list of cron expressions ordered by frequency in ascending order). For each frequency, sort the list of cron expressions based on the next execution date, from the furthest to the soonest
    """
    if reference_time is None:
        from datetime import date, time
        # Today at midnight UTC
        today_utc_midnight = datetime.combine(
            date.today(),
            time(0, 0, 0)
        )
        reference_time = datetime.fromtimestamp(today_utc_midnight.timestamp()).astimezone(pytz.timezone('UTC'))

    pairs = [(expr, int(get_cron_frequency(expr).total_seconds())) for expr in cron_expressions]
    from collections import defaultdict

    # Create a dictionary to store cron expressions by their frequency
    grouped_crons = defaultdict(list)

    for expr, freq in pairs:
        grouped_crons[freq].append(expr)

    sorted_groups = {}

    for freq, cron_list in grouped_crons.items():
        # For each cron expression, compute its next execution datetime
        next_times = [
            (cron_expr, croniter(cron_expr, reference_time).get_next(datetime))
            for cron_expr in cron_list
        ]
        # Sort by datetime descending (furthest first)
        next_times.sort(key=lambda pair: pair[1], reverse=True)
        # Rebuild the list of cron expressions in that order
        sorted_groups[freq] = [cron_expr for cron_expr, _ in next_times]

    flattened: List[str] = []

    # Iterate frequencies from smallest to largest timedelata
    for freq in sorted(sorted_groups.keys(), reverse=False):
        for expr in sorted_groups[freq]:
            flattened.append(expr)
    return (sorted_groups, flattened)

sl_timestamp_format = '%Y-%m-%dT%H:%M:%S%z'

def sl_cron_start_end_dates(cron_expr: str, start_time: datetime = cron_start_time(), format: str = sl_timestamp_format) -> str:
    """
    Returns the start and end dates for a cron expression.

    :param cron_expr: The cron expression.
    :param start_time: The start time.
    :param format: The format to return the dates in.
    """
    (sl_start_date, sl_end_date) = scheduled_dates_range(cron_expr, start_time)
    return f"{StarlakeParameters.DATA_INTERVAL_START_PARAMETER.value}='{sl_start_date.strftime(format)}',{StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value}='{sl_end_date.strftime(format)}'"

def sl_scheduled_date(cron: Optional[str], ts: Union[datetime, str], timezone: str = 'UTC') -> datetime:
    """
    Returns the scheduled date for a cron expression and timestamp or the timestamp itself if no cron expression is provided.
    Args:
        cron (str): The optional cron expression.
        ts (Union[datetime, str]): The timestamp.
        timezone (str): The timezone to use if ts is a string. Defaults to 'UTC'.
    Returns:
        datetime: The scheduled date.
    """
    try:
        if isinstance(ts, str):
            # Convert ts to a datetime object
            start_time = parser.isoparse(ts).astimezone(pytz.timezone(timezone))
        else:
            start_time = ts
        if cron and not is_valid_cron(cron):
            raise ValueError(f"Invalid cron expression: {cron}")
        elif cron:
            (_, en_date) = scheduled_dates_range(cron, start_time)
            return en_date
        else:
            return start_time
    except Exception as e:
        print(f"Error converting timestamp to datetime: {e}")
        raise e

def sl_scheduled_dataset(dataset: str, cron: Optional[str], ts:  Union[datetime, str], parameter_name: str = StarlakeParameters.SCHEDULED_DATE_PARAMETER.value, format: str = sl_timestamp_format, timezone: str = 'UTC') -> str:
    """
    Returns the dataset url with the schedule parameter added if a cron expression has been provided.
    Args:
        dataset (str): The dataset name.
        cron (str): The optional cron expression.
        ts (Union[datetime, str]): The timestamp.
        parameter_name (str): The parameter name. Defaults to StarlakeParameters.SCHEDULED_DATE_PARAMETER.
        format (str): The format to return the schedule in. Defaults to '%Y%m%dT%H%M'.
        timezone (str): The timezone to use if ts is a string. Defaults to 'UTC'.
    Returns:
        str: The dataset url with the schedule parameter added if a cron expression has been provided.
    """
    if cron:
        scheduled_date = sl_scheduled_date(cron, ts, timezone=timezone)
        parameters = dict()
        parameters[parameter_name] = scheduled_date.strftime(format)
        return f"{sanitize_id(dataset).lower()}{asQueryParameters(parameters)}"
    return sanitize_id(dataset).lower()

def is_valid_cron(cron_expr: str) -> bool:
    try:
        # Attempt to instantiate a croniter object
        croniter(cron_expr)
        return True
    except (CroniterBadCronError, ValueError, AttributeError) as e:
        # Handle the exception if the cron expression is invalid
        print(f"Invalid cron expression: {cron_expr}. Error: {e}")
        # Return False if the cron expression is invalid
        return False

def most_frequent_crons(all_crons: List[str]) -> Optional[List[str]]:
    sorted_crons_by_frequency: Tuple[Dict[int, List[str]], List[str]] = sort_crons_by_frequency(all_crons)
    if sorted_crons_by_frequency:
        sorted_groups = sorted_crons_by_frequency[0]
        if sorted_groups:
            most_frequent_key = sorted(sorted_groups.keys(), reverse=False)[0]
            most_frequent = sorted_groups[most_frequent_key]
            print(f"most frequent crons -> {most_frequent}")
            return most_frequent
    return None

def scheduled_dates_range(cron: str, scheduled_date: datetime) -> Tuple[datetime, datetime]:
    iter = croniter(cron, scheduled_date)
    curr = iter.get_current(datetime)
    previous = iter.get_prev(datetime)
    next = croniter(cron, previous).get_next(datetime)
    if curr == next :
        scheduled_date_to_check_max = curr
    else:
        scheduled_date_to_check_max = previous
    scheduled_date_to_check_min = croniter(cron, scheduled_date_to_check_max).get_prev(datetime)
    return (scheduled_date_to_check_min, scheduled_date_to_check_max)

# The reference span over which fires_in_every_period walks the periods of a
# data cycle: from 1 January 2001 (UTC), within the 28 years of a full cycle of
# weekdays and leap years, cut after DATA_CYCLE_REFERENCE_MAX_PERIODS periods
# (about five years of a daily cycle, 83 days of an hourly one).
DATA_CYCLE_REFERENCE_START = datetime(2001, 1, 1, tzinfo=pytz.UTC)
DATA_CYCLE_REFERENCE_END = datetime(2029, 1, 1, tzinfo=pytz.UTC)
DATA_CYCLE_REFERENCE_MAX_PERIODS = 2000

@functools.lru_cache(maxsize=256)
def fires_in_every_period(cron: Optional[str], data_cycle: Optional[str]) -> bool:
    """Whether `cron` fires at least once inside every period (tick, next tick]
    of the `data_cycle` cron (issue #167): "0 6 1-3 * *" in a monthly cycle (three
    attempts of one monthly delivery), "0 6 * * *" in a daily one, but neither
    "0 6 * * 1-5" in a daily cycle (weekends) nor "0 6 29 * *" in a monthly one
    (February).

    A tick on the period's end does not count: it closes the period that ends
    there, so an event dated on a data-cycle tick belongs to that period, and a
    dependency that fires only on the data-cycle ticks ("0 0 1 * *" in a monthly
    cycle, or the data cycle itself) would never have an event in the window of
    an anchor that follows the tick. Such a dependency keeps its own schedule
    window (dataset mode).

    The periods of the reference span are walked one by one, so the result
    never depends on the wall clock: comparing the gaps between ticks would,
    and would fail either way (the largest gap of "0 6 1-3 * *", 29 days, is
    longer than February; the smallest gap of "0 6 1,2 1 *" is one day, although
    it misses eleven months)."""
    if not cron or not data_cycle or not is_valid_cron(cron) or not is_valid_cron(data_cycle):
        return False
    cycle = croniter(data_cycle, DATA_CYCLE_REFERENCE_START)
    period_start = cycle.get_next(datetime)
    periods = 0
    for _ in range(DATA_CYCLE_REFERENCE_MAX_PERIODS):
        period_end = cycle.get_next(datetime)
        if period_end > DATA_CYCLE_REFERENCE_END:
            break
        if croniter(cron, period_start).get_next(datetime) >= period_end:
            return False
        periods += 1
        period_start = period_end
    # a cycle without a single period in the reference span (a cron restricted
    # to other years) proves nothing
    return periods > 0

def data_cycle_window(data_cycle: str, anchor: datetime) -> Tuple[datetime, datetime]:
    """The window (min, max] in which a dependency checked in data-cycle mode
    has its events admitted (issue #167): from the start of the data-cycle
    period that holds the anchor (the run's scheduled date) up to the anchor.
    The period start is the latest data-cycle tick strictly before the anchor,
    so an anchor exactly on a tick belongs to the period that ends there, like
    the (previous tick, current tick] windows of scheduled_dates_range."""
    return (croniter(data_cycle, anchor).get_prev(datetime), anchor)

# The unit labels of scala.concurrent.duration.Duration, with which Starlake
# core reads a freshness duration, in nanoseconds: the first label of each unit
# has no plural form, the others also accept a trailing "s".
_DURATION_UNIT_NANOS: Dict[str, int] = {}
for _nanos, _labels in (
    (86_400_000_000_000, "d day"),
    (3_600_000_000_000, "h hr hour"),
    (60_000_000_000, "m min minute"),
    (1_000_000_000, "s sec second"),
    (1_000_000, "ms milli millisecond"),
    (1_000, "µs micro microsecond"),
    (1, "ns nano nanosecond"),
):
    _first, *_others = _labels.split()
    _DURATION_UNIT_NANOS[_first] = _nanos
    for _label in _others:
        _DURATION_UNIT_NANOS[_label] = _DURATION_UNIT_NANOS[_label + "s"] = _nanos

_DURATION_NUMBER = re.compile(r"[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?")
_DURATION_MAX_NANOS = 2**63 - 1

def freshness_seconds(error: Optional[str]) -> int:
    """The seconds of a freshness `error` duration (e.g. "2 days"), computed
    as Starlake core's Freshness.acceptability() does (issue #166):
    Duration(error).toSeconds, never below 0, and 0 when no duration is
    declared. Same grammar as scala.concurrent.duration.Duration: whitespace
    ignored, a number followed by a unit label, rounded to the nanosecond, then
    truncated to the second.

    :raise ValueError: if the duration does not follow that grammar.
    """
    if not error:
        return 0
    text = "".join(str(error).split())
    unit = re.search(r"[^\W\d_]*$", text).group(0)
    number = text[:len(text) - len(unit)]
    nanos_per_unit = _DURATION_UNIT_NANOS.get(unit)
    nanos = round(float(number) * nanos_per_unit) if nanos_per_unit and _DURATION_NUMBER.fullmatch(number) else None
    # Duration counts nanoseconds in a Long
    if nanos is None or abs(nanos) > _DURATION_MAX_NANOS:
        raise ValueError(f"Invalid freshness duration: {error!r} (expected a number followed by a unit, e.g. '2 days')")
    return max(nanos // 1_000_000_000, 0)
