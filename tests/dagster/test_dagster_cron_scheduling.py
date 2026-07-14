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

from typing import Optional, Tuple

import pytest

from ai.starlake.common import get_cron_frequency
from ai.starlake.orchestration import StarlakeSchedule

from tests.dagster.dagster_test_mixin import DagsterTestMixin
from tests.shared.base_test_cron_scheduling import (
    BaseTestCronScheduling,
    COMMON_CRON_PATTERNS,
)


class TestDagsterCronScheduling(DagsterTestMixin, BaseTestCronScheduling):

    def get_native_schedule(
        self, pipeline, orchestration
    ) -> Optional[Tuple[Optional[str], Optional[str]]]:
        # JobDefinition + partitions are built in DagsterPipeline.__exit__ —
        # the shared helper has already exited the pipeline context.
        partitions_def = pipeline.dag.partitions_def
        if partitions_def is None:
            return None
        return (partitions_def.cron_schedule, partitions_def.timezone)

    # -- Dagster-specific: ScheduleDefinition registration (AC #1) --------

    def _build_with_definitions(self, expr, options=None):
        """``ScheduleDefinition`` is created in
        ``DagsterOrchestration.__exit__`` — BOTH context managers must exit
        before ``orch.definitions`` exists."""
        with self.create_orchestration(options=options) as orch:
            schedule = StarlakeSchedule(name=None, cron=expr, domains=[])
            pipeline = orch.sl_create_pipeline(schedule=schedule)
            with pipeline:
                start = pipeline.start_task()
                end = pipeline.end_task()
                start >> end
        return pipeline, orch

    def test_cron_produces_schedule_definition(self):
        expr = "0 2 * * *"
        pipeline, orch = self._build_with_definitions(expr)
        schedules = list(orch.definitions.schedules)
        assert len(schedules) == 1, (
            f"Expected 1 schedule, got {len(schedules)}"
        )
        sched = schedules[0]
        assert sched.cron_schedule == expr
        assert sched.job_name == pipeline.pipeline_id
        # execution_timezone must be explicit (previously unset)
        assert sched.execution_timezone == "UTC"

    def test_schedule_definition_honors_configured_timezone(self):
        _, orch = self._build_with_definitions(
            "0 2 * * *", options={"timezone": "Europe/Paris"}
        )
        sched = list(orch.definitions.schedules)[0]
        assert sched.execution_timezone == "Europe/Paris"

    # -- Dagster-specific: partition window semantics (AC #1) -------------

    @pytest.mark.parametrize("expr", COMMON_CRON_PATTERNS)
    def test_partition_window_width_matches_cron_frequency(self, expr):
        from datetime import timedelta
        pipeline, _ = self._build_scheduled_pipeline(expr)
        partitions_def = pipeline.dag.partitions_def
        # start_date derives from the caller file's mtime ("today"), and
        # get_partition_keys() at the DEFAULT current_time=now excludes the
        # still-incomplete first window — daily/weekly/2am patterns would
        # return ZERO keys on most runs. Pass an explicit far-future
        # current_time to make every pattern deterministic.
        current_time = pipeline.job.start_date + timedelta(days=60)
        keys = partitions_def.get_partition_keys(current_time=current_time)
        assert len(keys) > 0, "Expected at least one partition key"
        window = partitions_def.time_window_for_partition_key(keys[0])
        assert window.end - window.start == get_cron_frequency(expr)
