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

"""The freshness published by the load event of a table (issue #166).

Starlake core gave the freshness of a load table (``metadata.freshness.error``)
to the dependencies of the transform DAGs only: the load DAG published every
event with freshness 0. The load templates now hand the duration to the
``StarlakeTable`` they build, which stores it in seconds as core's
``Freshness.acceptability()`` computes them, and ``AbstractPipeline.sl_load``
gives it to the dataset of the load event, and to nothing else.
"""

from __future__ import annotations

import pytest

from ai.starlake.common import freshness_seconds
from ai.starlake.orchestration import StarlakeSchedule
from ai.starlake.orchestration.starlake_schedules import StarlakeDomain, StarlakeTable


class TestFreshnessSeconds:
    @pytest.mark.parametrize(
        ("duration", "seconds"),
        [
            ("2 days", 172800),
            ("48 hours", 172800),
            ("90min", 5400),
            ("1.5 hours", 5400),
            # rounded to the nanosecond before the truncation, like Duration:
            # 0.7 * 3600 is 2519.999... in floating point
            ("0.7 h", 2520),
            ("2d", 172800),
            ("1 day", 86400),
            (" 3  hours ", 10800),
            ("10 seconds", 10),
            ("500 ms", 0),
            (None, 0),
            ("", 0),
        ],
    )
    def test_durations(self, duration, seconds):
        assert freshness_seconds(duration) == seconds

    @pytest.mark.parametrize("duration", ["2 dayss", "2", "days", "2 weeks", "inf days", "two days", "1_000 days", "1e9 days"])
    def test_invalid_durations_fail_loud(self, duration):
        # a duration needs a number, written as Java parses it, and a unit;
        # Duration counts nanoseconds in a Long, about 106,751 days
        with pytest.raises(ValueError, match="Invalid freshness duration"):
            freshness_seconds(duration)

    def test_first_label_of_a_unit_has_no_plural(self):
        with pytest.raises(ValueError):
            freshness_seconds("2 ds")
        assert freshness_seconds("2 hrs") == 7200


class TestStarlakeTableFreshness:
    def test_declared_freshness_is_stored_in_seconds(self):
        assert StarlakeTable(name="balances", freshness="2 days").freshness == 172800

    def test_no_freshness_is_zero(self):
        assert StarlakeTable(name="balances").freshness == 0

    def test_a_number_is_a_number_of_seconds(self):
        assert StarlakeTable(name="balances", freshness=172800).freshness == 172800

    def test_an_invalid_duration_publishes_zero_without_breaking_the_dag_file(self, caplog):
        # the load DAG file builds every table of the schedule at import: one
        # invalid duration must not remove all of them
        assert StarlakeTable(name="balances", freshness="1 week").freshness == 0
        assert "Invalid freshness duration: '1 week'" in caplog.text


def _schedule(**freshness_by_table):
    return StarlakeSchedule(
        name="monthly",
        cron="0 6 1-3 * *",
        domains=[
            StarlakeDomain(
                name="finance",
                final_name="finance",
                tables=[StarlakeTable(name=name, freshness=duration) for name, duration in freshness_by_table.items()],
            )
        ],
    )


class TestLoadEventFreshness:
    def _load(self, stub_job, stub_orchestration, schedule, table):
        with stub_orchestration:
            pipeline = stub_orchestration.sl_create_pipeline(schedule=schedule)
            with pipeline:
                task = pipeline.sl_load(task_id=f"load_finance_{table}", domain="finance", table=table)
        return pipeline, task

    def test_the_load_event_carries_the_table_freshness(self, stub_job, stub_orchestration):
        pipeline, _ = self._load(stub_job, stub_orchestration, _schedule(balances="2 days", contracts=None), "balances")
        asset = next(asset for asset in pipeline.assets if asset.name == "finance.balances")
        assert asset.freshness == 172800

    def test_a_table_without_freshness_publishes_zero(self, stub_job, stub_orchestration):
        pipeline, _ = self._load(stub_job, stub_orchestration, _schedule(balances="2 days", contracts=None), "contracts")
        asset = next(asset for asset in pipeline.assets if asset.name == "finance.contracts")
        assert asset.freshness == 0

    def test_a_table_the_schedule_does_not_hold_publishes_zero(self, stub_job, stub_orchestration):
        pipeline, _ = self._load(stub_job, stub_orchestration, _schedule(balances="2 days"), "other")
        asset = next(asset for asset in pipeline.assets if asset.name == "finance.other")
        assert asset.freshness == 0

    def test_the_freshness_never_reaches_the_load_task(self, stub_job, stub_orchestration):
        # every other keyword argument of sl_load reaches the operator, which
        # would reject an argument it does not know
        _, task = self._load(stub_job, stub_orchestration, _schedule(balances="2 days"), "balances")
        assert "freshness" not in task.task["kwargs"]
