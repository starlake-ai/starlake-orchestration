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

"""Shared triggering-strategy scenarios (story: cross-orchestrator
dataset triggering strategy validation).

Pure-data scenario constants consumed by every orchestrator's
triggering-strategy tests.  Cross-orchestrator equivalence is
established transitively: each orchestrator asserts its NATIVE
triggering artifact against the SAME ``EXPECTED_URIS`` sets — never
via a single test importing several orchestrator modules (the CI legs
are dependency-isolated).

Core imports only (NFR13).
"""

from __future__ import annotations

from typing import Optional, Tuple

from ai.starlake.orchestration import (
    StarlakeDependencies,
    StarlakeDependency,
    StarlakeDependencyType,
)

# Each scenario is a tuple of (table_name, cron) upstream pairs.
# cron=None declares a NOT-scheduled upstream.
TWO_UPSTREAMS: Tuple[Tuple[str, Optional[str]], ...] = (
    ("starbake.orders", "0 * * * *"),
    ("starbake.customers", "0 0 * * *"),
)
SINGLE_UPSTREAM: Tuple[Tuple[str, Optional[str]], ...] = (
    ("starbake.orders", "0 * * * *"),
)
NO_UPSTREAM: Tuple[Tuple[str, Optional[str]], ...] = ()
MIXED_UPSTREAMS: Tuple[Tuple[str, Optional[str]], ...] = (
    ("starbake.orders", "0 * * * *"),
    ("starbake.stock", None),
)

# Cross-orchestrator equivalence anchor (AC #3): every orchestrator's
# triggering artifact must consume EXACTLY these upstream URIs.
# uri = sanitize_id(sink).lower() — dots become underscores.
EXPECTED_URIS = {
    TWO_UPSTREAMS: frozenset({"starbake_orders", "starbake_customers"}),
    SINGLE_UPSTREAM: frozenset({"starbake_orders"}),
    NO_UPSTREAM: frozenset(),
    MIXED_UPSTREAMS: frozenset({"starbake_orders", "starbake_stock"}),
}


def make_dependencies(
    upstreams: Tuple[Tuple[str, Optional[str]], ...],
    task_name: str = "overall_kpis",
) -> StarlakeDependencies:
    """Build the StarlakeDependencies for a triggering scenario.

    One first-level TASK dependency whose sub-dependencies (TABLE) are
    the upstream datasets — the exact shape used by the Epic 1 event
    combination tests.
    """
    return StarlakeDependencies([
        StarlakeDependency(
            name=task_name,
            dependency_type=StarlakeDependencyType.TASK,
            dependencies=[
                StarlakeDependency(
                    name=name,
                    dependency_type=StarlakeDependencyType.TABLE,
                    cron=cron,
                )
                for name, cron in upstreams
            ],
        ),
    ])
