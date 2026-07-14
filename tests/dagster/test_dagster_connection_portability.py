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

from typing import Any, Dict

import pytest

from tests.dagster.dagster_test_mixin import DagsterTestMixin
from tests.shared.base_test_connection_portability import (
    BACKEND_ENVS,
    BaseTestConnectionPortability,
)


class TestDagsterConnectionPortability(DagsterTestMixin, BaseTestConnectionPortability):
    """Dagster pass-through: inherited base tests + op-closure env checks."""

    def _get_op_env(self, task: Any) -> Dict[str, str]:
        """Extract the ``env`` closure free var from the @op function.

        Same closure-inspection technique as
        DagsterTestMixin.get_task_arguments — the op passes this exact
        dict to execute_shell_command(env=...) at runtime
        (starlake_dagster_shell_job.py:172).
        """
        compute_fn = task._compute_fn
        fn = getattr(compute_fn, "decorated_fn", compute_fn)
        freevars = fn.__code__.co_freevars
        assert "env" in freevars, f"'env' not captured by op closure: {freevars}"
        return fn.__closure__[freevars.index("env")].cell_contents

    @pytest.mark.parametrize("backend", sorted(BACKEND_ENVS))
    def test_op_env_contains_backend_vars(self, backend):
        """The op's subprocess env carries every backend var verbatim.

        Dagster merges: env = os.environ.copy() + sl_env_vars
        (starlake_dagster_shell_job.py:62-63) — sl_env_vars win.
        """
        task, env = self._make_load_task(backend)
        op_env = self._get_op_env(task)
        for key, value in env.items():
            assert op_env.get(key) == value, (
                f"{backend}: op env[{key!r}] = {op_env.get(key)!r}, "
                f"expected {value!r}"
            )
