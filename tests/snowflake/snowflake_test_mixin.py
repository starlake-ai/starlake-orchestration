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

from ai.starlake.snowflake import SnowflakeOrchestration, StarlakeSnowflakeJob
from ai.starlake.orchestration import AbstractOrchestration

from tests.snowflake.conftest import _SNOWFLAKE_TEST_MODULE_NAME


class SnowflakeTestMixin:
    """Shared implementation of the three abstract methods for Snowflake tests.

    Every concrete test class (TestSnowflakeSlLoad, etc.) should inherit
    from this mixin AND its corresponding base class.
    """

    def create_orchestration(
        self, options: Optional[dict] = None
    ) -> AbstractOrchestration:
        merged = {
            "stage_location": "staging",
            "warehouse": "COMPUTE_WH",
            "sl_incoming_file_stage": "@incoming_stage",
        }
        if options:
            merged.update(options)
        job = StarlakeSnowflakeJob(
            filename="test_snowflake.py",
            module_name=_SNOWFLAKE_TEST_MODULE_NAME,
            options=merged,
        )
        return SnowflakeOrchestration(job=job)

    def get_task_arguments(self, task: Any) -> List[str]:
        """Reconstruct CLI-like arguments from DAGTask metadata.

        Snowflake's ``fun()`` closure does NOT capture the original
        ``arguments`` list.  Instead, we reconstruct from:

        - ``task.comment``: e.g. ``"Starlake load starbake.customers"``
          or ``"Starlake transform kpi.order_summary"``
        - Closure free variables: ``sink`` (captured by ``fun()``)
        """
        comment = getattr(task, "comment", "") or ""

        # Determine action type from comment
        if comment.startswith("Starlake load "):
            action = "load"
            target = comment[len("Starlake load "):]
        elif comment.startswith("Starlake transform "):
            action = "transform"
            target = comment[len("Starlake transform "):]
        elif comment.startswith("Starlake preload "):
            action = "preload"
            # Comment format: "Starlake preload {strategy} {domain}"
            target = comment[len("Starlake preload "):]
        elif comment.startswith("Starlake ") and comment.endswith(" task"):
            action = "stage"
            target = comment[len("Starlake "):-len(" task")]
        else:
            return []

        # Try to extract sink from closure free variables as fallback
        sink = target
        try:
            func = task.definition.func
            if func is not None:
                freevars = func.__code__.co_freevars
                closure = func.__closure__
                if closure and "sink" in freevars:
                    idx = freevars.index("sink")
                    sink = closure[idx].cell_contents
        except (AttributeError, TypeError, ValueError):
            pass

        if not sink:
            return [action]

        domain_table = sink.split(".")
        domain = domain_table[0]
        table = domain_table[-1]

        if action == "load":
            return ["load", "--domains", domain, "--tables", table]
        elif action == "transform":
            # Attempt to recover --options from closure free variables
            options_str = None
            try:
                func = task.definition.func
                if func is not None:
                    freevars = func.__code__.co_freevars
                    closure = func.__closure__
                    if closure and "options" in freevars:
                        idx = freevars.index("options")
                        options_dict = closure[idx].cell_contents
                        if isinstance(options_dict, dict) and options_dict:
                            options_str = ",".join(
                                f"{k}={v}" for k, v in options_dict.items()
                            )
            except (AttributeError, TypeError, ValueError):
                pass
            args = ["transform", "--name", sink]
            if options_str:
                args.extend(["--options", options_str])
            return args
        elif action == "stage":
            return ["stage", "--domains", domain]
        elif action == "preload":
            # Extract strategy from comment: "Starlake preload {strategy} {target}"
            parts = target.split(None, 1)
            strategy = parts[0] if parts else "imported"
            return ["preload", "--strategy", strategy, "--domain", domain]
        return []

    def get_task_id(self, task: Any) -> str:
        return task.name
