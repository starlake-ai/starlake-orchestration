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

from abc import ABC, abstractmethod
from typing import Any, List

from ai.starlake.orchestration import AbstractOrchestration


class BaseTestOrchestration(ABC):
    """Shared abstract base for all orchestration test classes.

    Defines the abstract methods that every concrete orchestrator
    test class must implement.  All operation-specific base classes
    (BaseTestSlLoad, BaseTestSlTransform, etc.) extend this class.
    """

    @abstractmethod
    def create_orchestration(self) -> AbstractOrchestration:
        """Each orchestrator provides its concrete orchestration."""

    @abstractmethod
    def get_task_arguments(self, task: Any) -> List[str]:
        """Extract CLI arguments from an orchestrator-specific task.

        Concrete implementations inspect the native task object
        (e.g. BashOperator.bash_command for Airflow) and return
        the list of arguments passed to the Starlake CLI.
        """

    @abstractmethod
    def get_task_id(self, task: Any) -> str:
        """Extract the task identifier from an orchestrator-specific task.

        E.g. ``task.task_id`` for Airflow, ``task.name`` for Dagster.
        """

    def get_arg_value(self, args: List[str], flag: str) -> str:
        """Return the value following *flag* in *args*.

        Raises a clear ``AssertionError`` instead of an ``IndexError``
        when *flag* is missing or is the last element in the list.
        """
        assert flag in args, f"Expected flag '{flag}' not found in args: {args}"
        idx = args.index(flag)
        assert idx + 1 < len(args), (
            f"Flag '{flag}' has no value (last element in args): {args}"
        )
        return args[idx + 1]
