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

from tests.airflow.airflow_test_mixin import AirflowTestMixin
from tests.shared.base_test_cron_scheduling import (
    BaseTestCronScheduling,
    COMMON_CRON_PATTERNS,
)


class TestAirflowCronScheduling(AirflowTestMixin, BaseTestCronScheduling):

    def get_native_schedule(
        self, pipeline, orchestration
    ) -> Optional[Tuple[Optional[str], Optional[str]]]:
        from ai.starlake.airflow.compat import supports_assets
        dag = pipeline.dag
        if supports_assets():  # Airflow 3
            timetable = dag.timetable
            expr = getattr(timetable, "expression", None)
        else:  # Airflow 2
            expr = dag.schedule_interval
        if expr is None or not isinstance(expr, str):
            # NullTimetable / dataset-triggered — unscheduled for our purposes
            return None
        # dag.timezone is a pendulum Timezone on both majors; its `name`
        # is the IANA identifier
        tz = getattr(dag.timezone, "name", str(dag.timezone))
        return (expr, tz)

    # -- Airflow-specific: data_interval semantics (AC #1) ----------------

    @pytest.mark.parametrize("expr", COMMON_CRON_PATTERNS)
    def test_data_interval_width_matches_cron_frequency(self, expr):
        """The scheduler-side data interval for a cron schedule spans exactly
        one cron period — the framework's canonical window
        (scheduled_dates_range) has the same width by construction.

        The two majors differ STRUCTURALLY here:
        - Airflow 2.10: ``create_cron_data_intervals`` defaults to True, so
          ``dag.timetable`` is ``airflow.timetables.interval.
          CronDataIntervalTimetable`` and exposes
          ``infer_manual_data_interval(*, run_after)`` directly.
        - Airflow 3.3: ``create_cron_data_intervals`` defaults to False and
          the sdk DAG builds ``airflow.sdk.definitions.timetables.trigger.
          CronTriggerTimetable`` — a definition-only stub with NO
          ``infer_manual_data_interval`` and NO ``next_dagrun_info``.
          Interval computation lives scheduler-side in airflow-core, whose
          ``airflow.timetables.interval.CronDataIntervalTimetable`` is still
          importable and functional. We instantiate it from the DAG's own
          (expression, timezone) to validate the scheduler-side
          interpretation of the same cron.
        """
        from ai.starlake.airflow.compat import supports_assets
        import pendulum
        pipeline, _ = self._build_scheduled_pipeline(expr)
        timetable = pipeline.dag.timetable
        # Fixed run_after keeps the assertion deterministic — do NOT
        # replace with pendulum.now().
        run_after = pendulum.datetime(2026, 6, 15, 12, 0, 0, tz="UTC")
        if supports_assets():  # Airflow 3
            assert type(timetable).__name__ in (
                "CronTriggerTimetable",
                "CronDataIntervalTimetable",
            ), f"Unexpected timetable {type(timetable).__name__}"
            assert timetable.expression == expr
            from airflow.timetables.interval import CronDataIntervalTimetable
            core_timetable = CronDataIntervalTimetable(
                expr, pipeline.dag.timezone
            )
            interval = core_timetable.infer_manual_data_interval(
                run_after=run_after
            )
        else:  # Airflow 2
            assert type(timetable).__name__ == "CronDataIntervalTimetable", (
                f"Expected CronDataIntervalTimetable, "
                f"got {type(timetable).__name__}"
            )
            interval = timetable.infer_manual_data_interval(
                run_after=run_after
            )
        # pendulum.Interval subclasses datetime.timedelta, so equality with
        # get_cron_frequency's timedelta is well-defined.
        assert interval.end - interval.start == get_cron_frequency(expr)
