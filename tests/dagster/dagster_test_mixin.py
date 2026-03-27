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

from typing import Any, List, Optional

from ai.starlake.dagster import DagsterOrchestration
from ai.starlake.dagster.shell import StarlakeDagsterShellJob
from ai.starlake.orchestration import AbstractOrchestration

from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME


class DagsterTestMixin:
    """Shared implementation of the three abstract methods for Dagster tests.

    Every concrete test class (TestDagsterSlLoad, etc.) should inherit
    from this mixin AND its corresponding base class.
    """

    def create_orchestration(
        self, options: Optional[dict] = None
    ) -> AbstractOrchestration:
        job = StarlakeDagsterShellJob(
            filename="test_dagster.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options=options or {},
        )
        return DagsterOrchestration(job=job)

    def get_task_arguments(self, task: Any) -> List[str]:
        """Extract Starlake CLI arguments from the Dagster op's closure.

        The ``@op``-decorated function captures the ``arguments`` list
        as a free variable.  We inspect the closure to retrieve it
        without executing the op (which would mutate the list via
        ``arguments.pop(0)``).

        The internal attribute ``_compute_fn`` is part of Dagster's
        ``OpDefinition`` and may change across major versions.  If the
        structure changes, the ``AttributeError`` is caught and an
        empty list is returned so tests can still detect the problem.
        """
        try:
            compute_fn = task._compute_fn
            fn = getattr(compute_fn, "decorated_fn", compute_fn)
            freevars = fn.__code__.co_freevars
            if "arguments" in freevars:
                idx = freevars.index("arguments")
                arguments = fn.__closure__[idx].cell_contents
                return list(arguments)  # defensive copy
        except (AttributeError, TypeError, ValueError):
            pass
        return []

    def get_task_id(self, task: Any) -> str:
        return task.name
