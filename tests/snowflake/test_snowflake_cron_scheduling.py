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

from datetime import timedelta
from typing import Optional, Tuple

from snowflake.core.task import Cron

from tests.snowflake.snowflake_test_mixin import SnowflakeTestMixin
from tests.shared.base_test_cron_scheduling import BaseTestCronScheduling


class TestSnowflakeCronScheduling(SnowflakeTestMixin, BaseTestCronScheduling):

    def get_native_schedule(
        self, pipeline, orchestration
    ) -> Optional[Tuple[Optional[str], Optional[str]]]:
        schedule = pipeline.dag.schedule
        if isinstance(schedule, Cron):
            return (schedule.expr, schedule.timezone)
        return None  # timedelta fallback == unscheduled

    # -- Snowflake-specific: no-cron fallback (AC #1) ----------------------

    def test_no_cron_falls_back_to_min_timedelta(self):
        """``SnowflakeDag`` defaults to
        ``timedelta(seconds=min_timedelta_between_runs)`` when no cron is
        provided (job default 900s)."""
        pipeline, _ = self._build_scheduled_pipeline(None)
        assert pipeline.dag.schedule == timedelta(seconds=900)
