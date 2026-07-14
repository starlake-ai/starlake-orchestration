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

from tests.airflow.airflow_test_mixin import AirflowTestMixin
from tests.shared.base_test_connection_portability import (
    BACKEND_ENVS,
    BaseTestConnectionPortability,
)


class TestAirflowConnectionPortability(AirflowTestMixin, BaseTestConnectionPortability):
    """Airflow pass-through: inherited base tests + operator env checks."""

    @pytest.mark.parametrize("backend", sorted(BACKEND_ENVS))
    def test_operator_env_contains_backend_vars(self, backend):
        """The BashOperator env dict carries every backend var verbatim.

        Airflow's SubprocessHook REPLACES os.environ with task.env when
        env is non-empty — so task.env IS the exact environment the
        Starlake CLI would see.  task.env is built as
        {**sl_os_env_vars, **sl_env_vars} (starlake_airflow_bash_job.py:86).
        """
        task, env = self._make_load_task(backend)
        for key, value in env.items():
            assert task.env.get(key) == value, (
                f"{backend}: task.env[{key!r}] = {task.env.get(key)!r}, "
                f"expected {value!r}"
            )
