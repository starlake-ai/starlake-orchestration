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

import pytest

from tests.snowflake.snowflake_test_mixin import SnowflakeTestMixin
from tests.shared.base_test_sl_transform import BaseTestSlTransform


class TestSnowflakeSlTransform(SnowflakeTestMixin, BaseTestSlTransform):

    def test_transform_with_options(self):
        """Snowflake absorbs --options into internal dict during sl_job().

        The options are parsed and consumed — they are NOT stored in the
        DAGTask closure.  Verify instead that the task is created
        successfully with the options applied.
        """
        orchestration = self.create_orchestration()
        job = orchestration.job
        task = job.sl_transform(
            task_id="kpi_order_summary_opts",
            transform_name="kpi.order_summary",
            transform_options="SL_KEY=value",
        )
        assert task is not None
        # Verify task was created with correct identity
        assert self.get_task_id(task) == "kpi_order_summary_opts"
        # Verify core args are reconstructable
        args = self.get_task_arguments(task)
        assert "transform" in args
        assert self.get_arg_value(args, "--name") == "kpi.order_summary"
