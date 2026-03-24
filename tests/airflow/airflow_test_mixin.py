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

import re
import shlex
from typing import Any, List, Optional

from ai.starlake.airflow import AirflowOrchestration
from ai.starlake.airflow.bash import StarlakeAirflowBashJob
from ai.starlake.orchestration import AbstractOrchestration

from tests.airflow.conftest import _AIRFLOW_TEST_MODULE_NAME


class AirflowTestMixin:
    """Shared implementation of the three abstract methods for Airflow 2 tests.

    Every concrete test class (TestAirflowSlLoad, etc.) should inherit
    from this mixin AND its corresponding base class.
    """

    def create_orchestration(
        self, options: Optional[dict] = None
    ) -> AbstractOrchestration:
        job = StarlakeAirflowBashJob(
            filename="test_airflow.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options=options or {},
        )
        return AirflowOrchestration(job=job)

    def get_task_arguments(self, task: Any) -> List[str]:
        """Extract Starlake CLI arguments from the BashOperator's bash_command.

        The bash_command format is:
            starlake <command> [--scheduledDate '...'] --flag value ...
        For xcom_push wrapped commands the starlake invocation is on its
        own line inside a ``bash -c '...'`` block.
        """
        cmd = task.bash_command
        for line in cmd.split("\n"):
            stripped = line.strip()
            match = re.match(r"(.*\bstarlake)\s+(.*)", stripped)
            if match:
                args_str = match.group(2)
                try:
                    return shlex.split(args_str)
                except ValueError:
                    return args_str.split()
        return []

    def get_task_id(self, task: Any) -> str:
        return task.task_id
