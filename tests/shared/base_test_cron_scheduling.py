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

import sys
from abc import abstractmethod
from typing import Optional, Tuple

import pytest

from ai.starlake.orchestration import (
    AbstractOrchestration,
    AbstractPipeline,
    StarlakeDependencies,
    StarlakeDependency,
    StarlakeDependencyType,
    StarlakeSchedule,
)

from tests.shared.base_test_orchestration import BaseTestOrchestration


# The portable cron subset: standard 5-field expressions.
# (croniter also accepts @-aliases and 6-field second-resolution crons,
# but Snowflake's USING CRON only supports 5-field syntax — the portable
# contract across all orchestrators is therefore the standard 5-field
# expression. DAG configs must stick to 5-field expressions.)
COMMON_CRON_PATTERNS = [
    pytest.param("0 * * * *", id="hourly"),
    pytest.param("0 0 * * *", id="daily"),
    pytest.param("0 0 * * 0", id="weekly"),
    pytest.param("0 2 * * *", id="custom-daily-2am"),
    pytest.param("*/15 * * * *", id="custom-every-15min"),
]

INVALID_CRON_PATTERNS = [
    pytest.param("not_a_cron", id="garbage"),
    pytest.param("60 * * * *", id="minute-out-of-range"),
    pytest.param("0 0 * *", id="too-few-fields"),
]


class BaseTestCronScheduling(BaseTestOrchestration):
    """Shared cron-scheduling consistency tests (cross-orchestrator).

    Concrete subclasses provide ``get_native_schedule()`` on top of the
    three abstract methods from ``BaseTestOrchestration`` (all three come
    from the orchestrator's existing test mixin).
    """

    @abstractmethod
    def get_native_schedule(
        self,
        pipeline: AbstractPipeline,
        orchestration: AbstractOrchestration,
    ) -> Optional[Tuple[Optional[str], Optional[str]]]:
        """Return ``(cron_expr, timezone_name)`` from the orchestrator's
        NATIVE scheduling construct, or ``None`` when the pipeline is
        unscheduled.

        - Airflow:   (timetable expression, str(dag.timezone))
        - Dagster:   (partitions_def.cron_schedule, partitions_def.timezone)
        - Snowflake: (dag.schedule.expr, dag.schedule.timezone)
        """

    # -- helpers ---------------------------------------------------------

    def _build_scheduled_pipeline(
        self, cron: Optional[str], options: Optional[dict] = None
    ) -> Tuple[AbstractPipeline, AbstractOrchestration]:
        """Full orchestration → pipeline path with a populated task graph."""
        orchestration = self.create_orchestration(options=options)
        schedule = StarlakeSchedule(name=None, cron=cron, domains=[])
        pipeline = orchestration.sl_create_pipeline(schedule=schedule)
        with pipeline:
            start = pipeline.start_task()
            end = pipeline.end_task()
            start >> end
        return pipeline, orchestration

    # -- AC #1 / #4: common patterns propagate verbatim ------------------

    @pytest.mark.parametrize("expr", COMMON_CRON_PATTERNS)
    def test_cron_pattern_propagates_to_native_schedule(self, expr):
        pipeline, orchestration = self._build_scheduled_pipeline(expr)
        assert pipeline.cron == expr
        assert pipeline.computed_cron_expr == expr
        native = self.get_native_schedule(pipeline, orchestration)
        assert native is not None, (
            f"Expected a native schedule for cron {expr!r}, got None"
        )
        native_expr, native_tz = native
        assert native_expr == expr, (
            f"Native schedule carries {native_expr!r}, expected {expr!r}"
        )
        # Default job timezone is UTC (core IStarlakeJob 'timezone' option)
        assert native_tz == "UTC", (
            f"Native timezone is {native_tz!r}, expected 'UTC'"
        )

    # -- AC #2: invalid crons rejected (schedule path) --------------------

    @pytest.mark.parametrize("expr", INVALID_CRON_PATTERNS)
    def test_invalid_cron_rejected_at_schedule_construction(self, expr):
        with pytest.raises(ValueError, match="Invalid cron expression"):
            StarlakeSchedule(name="bad", cron=expr, domains=[])

    # -- AC #2: invalid crons rejected (DAG-globals / dependencies path) --

    def test_invalid_cron_in_dag_globals_rejected(self, monkeypatch):
        """``AbstractPipeline.__init__`` validates the caller-module ``cron``
        global in dependencies mode, before any orchestrator construct is
        built."""
        orchestration = self.create_orchestration()
        caller_module = sys.modules[orchestration.job.caller_module_name]
        monkeypatch.setattr(caller_module, "cron", "not_a_cron", raising=False)
        dependencies = StarlakeDependencies([
            StarlakeDependency(
                name="starbake.orders",
                dependency_type=StarlakeDependencyType.TABLE,
            ),
        ])
        with pytest.raises(ValueError, match="Invalid cron expression"):
            orchestration.sl_create_pipeline(dependencies=dependencies)

    # -- unscheduled pipeline ---------------------------------------------

    def test_no_cron_produces_unscheduled_pipeline(self):
        pipeline, orchestration = self._build_scheduled_pipeline(None)
        assert pipeline.cron is None
        assert self.get_native_schedule(pipeline, orchestration) is None

    # -- AC #3: timezone consistency --------------------------------------

    def test_default_timezone_is_utc(self):
        pipeline, orchestration = self._build_scheduled_pipeline("0 2 * * *")
        assert pipeline.job.timezone == "UTC"
        _, native_tz = self.get_native_schedule(pipeline, orchestration)
        assert native_tz == "UTC"

    def test_configured_timezone_propagates(self):
        """A non-UTC ``timezone`` option must reach the native construct."""
        pipeline, orchestration = self._build_scheduled_pipeline(
            "0 2 * * *", options={"timezone": "Europe/Paris"}
        )
        assert pipeline.job.timezone == "Europe/Paris"
        _, native_tz = self.get_native_schedule(pipeline, orchestration)
        assert native_tz == "Europe/Paris"
