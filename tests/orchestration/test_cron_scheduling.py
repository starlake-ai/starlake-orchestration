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

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytz
import pytest

from ai.starlake.common import (
    cron_start_time,
    get_cron_frequency,
    is_valid_cron,
    scheduled_dates_range,
    sl_schedule,
    sort_crons_by_frequency,
)
from ai.starlake.orchestration.starlake_schedules import (
    StarlakeDomain,
    StarlakeSchedule,
    StarlakeTable,
)
from ai.starlake.dataset.starlake_dataset import StarlakeDatasetType
from ai.starlake.orchestration.starlake_dependencies import (
    StarlakeDependencies,
    StarlakeDependency,
    StarlakeDependencyType,
)


# ---------------------------------------------------------------------------
# 4.1  is_valid_cron()
# ---------------------------------------------------------------------------

class TestIsValidCron:
    @pytest.mark.parametrize(
        "expr",
        [
            "0 * * * *",
            "0 0 * * *",
            "*/5 * * * *",
            "0 0 1 * *",
            "0 0 * * 0",
        ],
    )
    def test_valid_expressions(self, expr):
        assert is_valid_cron(expr) is True

    @pytest.mark.parametrize(
        "expr",
        [
            "not_a_cron",
            "60 * * * *",
            "",
        ],
    )
    def test_invalid_expressions(self, expr):
        assert is_valid_cron(expr) is False


# ---------------------------------------------------------------------------
# 4.2  get_cron_frequency()
# ---------------------------------------------------------------------------

class TestGetCronFrequency:
    def test_hourly(self):
        freq = get_cron_frequency("0 * * * *")
        assert freq == timedelta(hours=1)

    def test_daily(self):
        freq = get_cron_frequency("0 0 * * *")
        assert freq == timedelta(days=1)

    def test_every_5_minutes(self):
        freq = get_cron_frequency("*/5 * * * *")
        assert freq == timedelta(minutes=5)

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid cron expression"):
            get_cron_frequency("not_valid")


# ---------------------------------------------------------------------------
# 4.3  sort_crons_by_frequency()
# ---------------------------------------------------------------------------

class TestSortCronsByFrequency:
    def test_groups_by_frequency(self):
        crons = ["0 * * * *", "0 0 * * *", "30 * * * *"]
        groups, flattened = sort_crons_by_frequency(crons)
        # hourly crons (3600s) should be grouped together
        assert 3600 in groups
        assert len(groups[3600]) == 2  # "0 * * * *" and "30 * * * *"
        # daily (86400s)
        assert 86400 in groups
        assert len(groups[86400]) == 1

    def test_flattened_order_ascending(self):
        crons = ["0 0 * * *", "0 * * * *"]
        _, flattened = sort_crons_by_frequency(crons)
        # Most frequent (hourly) should come first
        assert flattened[0] == "0 * * * *"
        assert flattened[1] == "0 0 * * *"


# ---------------------------------------------------------------------------
# 4.4  cron_start_time()
# ---------------------------------------------------------------------------

class TestCronStartTime:
    def test_returns_utc_datetime(self):
        dt = cron_start_time("UTC")
        assert dt.tzinfo is not None
        assert str(dt.tzinfo) == "UTC"

    def test_returns_eastern_datetime(self):
        dt = cron_start_time("US/Eastern")
        assert dt.tzinfo is not None
        assert "Eastern" in str(dt.tzinfo) or "EST" in str(dt.tzinfo) or "EDT" in str(dt.tzinfo)

    def test_utc_and_eastern_differ(self):
        utc_dt = cron_start_time("UTC")
        eastern_dt = cron_start_time("US/Eastern")
        # UTC offset is always 0; US/Eastern is -5 or -4 (DST)
        assert utc_dt.utcoffset() != eastern_dt.utcoffset()
        assert utc_dt.tzname() != eastern_dt.tzname()


# ---------------------------------------------------------------------------
# 4.5  sl_schedule()
# ---------------------------------------------------------------------------

class TestSlSchedule:
    def test_returns_formatted_string(self):
        start = datetime(2025, 6, 15, 12, 0, 0, tzinfo=pytz.UTC)
        result = sl_schedule("0 * * * *", start_time=start)
        # Previous cron execution from noon UTC with hourly cron is 12:00
        assert isinstance(result, str)
        assert len(result) > 0

    def test_default_format(self):
        start = datetime(2025, 6, 15, 12, 30, 0, tzinfo=pytz.UTC)
        result = sl_schedule("0 * * * *", start_time=start, format="%Y%m%dT%H%M")
        # Previous hourly execution from 12:30 is 12:00
        assert result == "20250615T1200"


# ---------------------------------------------------------------------------
# 4.6  scheduled_dates_range()
# ---------------------------------------------------------------------------

class TestScheduledDatesRange:
    def test_returns_tuple(self):
        scheduled = datetime(2025, 6, 15, 12, 0, 0, tzinfo=pytz.UTC)
        start, end = scheduled_dates_range("0 * * * *", scheduled)
        assert isinstance(start, datetime)
        assert isinstance(end, datetime)
        assert start < end

    def test_range_matches_cron_window(self):
        scheduled = datetime(2025, 6, 15, 12, 0, 0, tzinfo=pytz.UTC)
        start, end = scheduled_dates_range("0 0 * * *", scheduled)
        # For a daily cron at midnight, the window should be ~24h
        delta = end - start
        assert delta == timedelta(days=1)


# ---------------------------------------------------------------------------
# 4.7  StarlakeSchedule construction
# ---------------------------------------------------------------------------

class TestStarlakeScheduleConstruction:
    def test_valid_construction(self):
        schedule = StarlakeSchedule(
            name="hourly",
            cron="0 * * * *",
            domains=[
                StarlakeDomain(
                    name="domain1",
                    final_name="domain1",
                    tables=[StarlakeTable(name="table1")],
                )
            ],
        )
        assert schedule.name == "hourly"
        assert schedule.cron == "0 * * * *"
        assert len(schedule.domains) == 1

    def test_none_name_handled(self):
        schedule = StarlakeSchedule(
            name="None",
            cron="0 0 * * *",
            domains=[],
        )
        assert schedule.name is None

    def test_none_cron_allowed(self):
        schedule = StarlakeSchedule(name="test", cron=None, domains=[])
        assert schedule.cron is None

    def test_none_string_cron_becomes_none(self):
        schedule = StarlakeSchedule(name="test", cron="none", domains=[])
        assert schedule.cron is None


# ---------------------------------------------------------------------------
# 4.8  StarlakeSchedule with invalid cron raises
# ---------------------------------------------------------------------------

class TestStarlakeScheduleInvalidCron:
    def test_invalid_cron_raises(self):
        with pytest.raises(ValueError, match="Invalid cron expression"):
            StarlakeSchedule(
                name="bad",
                cron="not_a_cron",
                domains=[],
            )


# ---------------------------------------------------------------------------
# 4.9  StarlakeDependencies construction from JSON
# ---------------------------------------------------------------------------

class TestStarlakeDependenciesFromJson:
    def test_parse_json_string(self):
        json_str = json.dumps([
            {
                "data": {
                    "name": "domain1.table1",
                    "typ": "table",
                    "cron": "0 0 * * *",
                },
                "children": [],
            }
        ])
        deps = StarlakeDependencies(json_str)
        assert len(deps.dependencies) == 1
        assert deps.dependencies[0].name == "domain1.table1"
        assert deps.dependencies[0].dependency_type == StarlakeDependencyType.TABLE
        assert deps.dependencies[0].cron == "0 0 * * *"

    def test_parse_with_children(self):
        json_str = json.dumps([
            {
                "data": {
                    "name": "parent_task",
                    "typ": "task",
                },
                "children": [
                    {
                        "data": {
                            "name": "child.table",
                            "typ": "table",
                            "cron": "0 * * * *",
                        },
                        "children": [],
                    }
                ],
            }
        ])
        deps = StarlakeDependencies(json_str)
        assert len(deps.dependencies) == 1
        parent = deps.dependencies[0]
        assert parent.dependency_type == StarlakeDependencyType.TASK
        assert len(parent.dependencies) == 1
        assert parent.dependencies[0].name == "child.table"


# ---------------------------------------------------------------------------
# 4.10  StarlakeDependency.computed_cron
# ---------------------------------------------------------------------------

class TestStarlakeDependencyComputedCron:
    def test_explicit_cron_returned(self):
        dep = StarlakeDependency(
            name="domain.table",
            dependency_type=StarlakeDependencyType.TABLE,
            cron="0 0 * * *",
        )
        assert dep.computed_cron == "0 0 * * *"

    def test_inferred_from_children(self):
        child = StarlakeDependency(
            name="child.table",
            dependency_type=StarlakeDependencyType.TABLE,
            cron="0 * * * *",
        )
        parent = StarlakeDependency(
            name="parent.table",
            dependency_type=StarlakeDependencyType.TABLE,
            cron=None,
            dependencies=[child],
        )
        # computed_cron should infer from child's cron
        assert parent.computed_cron is not None

    def test_none_when_no_cron_no_deps_no_freshness(self):
        dep = StarlakeDependency(
            name="domain.table",
            dependency_type=StarlakeDependencyType.TABLE,
            cron=None,
        )
        assert dep.computed_cron is None


# ---------------------------------------------------------------------------
# 4.11  StarlakeDependency.to_dataset()
# ---------------------------------------------------------------------------

class TestStarlakeDependencyToDataset:
    def test_table_type_produces_load_dataset(self):
        dep = StarlakeDependency(
            name="domain.table",
            dependency_type=StarlakeDependencyType.TABLE,
            cron="0 0 * * *",
        )
        dataset = dep.to_dataset()
        assert dataset is not None
        assert dataset.name == "domain.table"
        assert dataset.datasetType == StarlakeDatasetType.LOAD

    def test_task_type_produces_transform_dataset(self):
        dep = StarlakeDependency(
            name="my_transform",
            dependency_type=StarlakeDependencyType.TASK,
            cron="0 * * * *",
        )
        dataset = dep.to_dataset()
        assert dataset is not None
        assert dataset.datasetType == StarlakeDatasetType.TRANSFORM

    def test_dataset_has_correct_properties(self):
        dep = StarlakeDependency(
            name="domain.table",
            dependency_type=StarlakeDependencyType.TABLE,
            cron="0 0 * * *",
            freshness=3600,
        )
        dataset = dep.to_dataset()
        assert dataset.freshness == 3600
        assert dataset.cron is not None


# ---------------------------------------------------------------------------
# 4.12  Timezone equivalence of the core scheduling helpers
# ---------------------------------------------------------------------------

class TestScheduledDateTimezoneEquivalence:
    """The same instant expressed in different timezones yields the same
    scheduled date from the core helpers — timezone handling is consistent
    across orchestrators because they all share these helpers."""

    def test_same_instant_two_timezones_same_scheduled_date(self):
        from ai.starlake.common import sl_scheduled_date
        # 12:30 UTC == 14:30 Paris (CEST, June)
        utc_result = sl_scheduled_date(
            "0 2 * * *", "2025-06-15T12:30:00+00:00"
        )
        paris_result = sl_scheduled_date(
            "0 2 * * *", "2025-06-15T14:30:00+02:00"
        )
        assert utc_result == paris_result

    def test_scheduled_dates_range_is_timezone_aware(self):
        paris = pytz.timezone("Europe/Paris")
        ts = paris.localize(datetime(2025, 6, 15, 14, 30, 0))
        start, end = scheduled_dates_range("0 2 * * *", ts)
        assert start.tzinfo is not None
        assert end.tzinfo is not None
        assert start < end
