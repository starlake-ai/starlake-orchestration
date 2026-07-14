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

"""Orchestrator-agnostic lineage helpers and the canonical expected structure.

Cross-orchestrator equivalence is asserted TRANSITIVELY: CI installs exactly
one orchestrator per leg, so no test can compare two orchestrators in-process.
Instead, every orchestrator leg normalizes its generated output and asserts it
equals the canonical golden form below — all legs equal the same golden form
implies all pairwise equal.

NFR13: this module must stay orchestrator-agnostic — it operates on generated
file text, parsed JSON, and ``AbstractPipeline``-level properties only.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Matches the transform template's embedded lineage:
#   dependencies=StarlakeDependencies(dependencies="""[ ... ]""", ...)
# Source: starlake-orchestration/src/main/resources/templates/dags/transform/
# __scheduled_task_tpl.py.j2 (line 23)
_DEPENDENCIES_RE = re.compile(
    r'StarlakeDependencies\(dependencies="""(.*?)"""', re.DOTALL
)


def is_transform_dag(py_file: Path) -> bool:
    """A generated transform DAG embeds a StarlakeDependencies JSON literal."""
    return "StarlakeDependencies(dependencies=" in py_file.read_text(encoding="utf-8")


def extract_embedded_lineage(py_file: Path) -> List[dict]:
    """Extract and parse the CLI-computed lineage JSON from a generated transform DAG."""
    source = py_file.read_text(encoding="utf-8")
    match = _DEPENDENCIES_RE.search(source)
    assert match is not None, f"No embedded lineage JSON found in {py_file.name}"
    return json.loads(match.group(1))


def normalize_lineage(tasks: List[dict]) -> Dict[str, object]:
    """Canonical form of the embedded lineage: node->typ map + sorted edge list.

    Edges are (upstream, downstream): a child feeds its parent entry.
    Children can nest recursively — walk everything, sets dedup repeats.

    Reads ONLY ``name``/``typ``/``children``. The 1.5.x CLI misplaces table
    children's cron into their ``sink`` field, and future CLI versions may add
    or fix fields — this minimalism IS the NFR7 tolerance mechanism. Never
    extend it to ``sink``/``cron``/``writeStrategy``.
    """
    nodes: Dict[str, str] = {}
    edges: Set[Tuple[str, str]] = set()

    def walk(entry: dict) -> None:
        data = entry.get("data", {})
        assert "name" in data, f"Malformed lineage entry (no data.name): {entry!r}"
        name = data["name"].lower()
        nodes.setdefault(name, data.get("typ", "unknown"))
        for child in entry.get("children", []):
            child_data = child.get("data", {})
            assert "name" in child_data, (
                f"Malformed lineage child (no data.name): {child!r}"
            )
            edges.add((child_data["name"].lower(), name))
            walk(child)

    for entry in tasks:
        walk(entry)
    return {"nodes": nodes, "edges": sorted(edges)}


def normalize_load_schedules(pipelines) -> Set[Tuple[str, str, str]]:
    """Canonical load surface: {(cron, domain, table)} across all load pipelines.

    Orchestrator-agnostic: only touches ``AbstractPipeline.schedule``
    (StarlakeSchedule -> StarlakeDomain -> StarlakeTable).
    """
    triples: Set[Tuple[str, str, str]] = set()
    for p in pipelines:
        schedule = p.schedule
        assert schedule is not None, f"{p.pipeline_id} has no schedule"
        assert schedule.cron is not None, (
            f"{p.pipeline_id} schedule {schedule.name} has no cron"
        )
        for domain in schedule.domains:
            for table in domain.tables:
                triples.add((schedule.cron, domain.name.lower(), table.name.lower()))
    return triples


# ---------------------------------------------------------------------------
# Canonical expected structure for tests/sample-project (the golden form every
# orchestrator leg must reproduce — transitive cross-orchestrator equivalence).
# Confirmed against real dag-generate output (metadata/.build/dags artifacts):
# names lowercase, typs task/table, crons after schedulePresets resolution.
# ---------------------------------------------------------------------------

EXPECTED_TRANSFORM_NODES = {
    "kpi.order_summary": "task",
    "kpi.top_customers": "task",
    "starbake.orders": "table",
    "starbake.customers": "table",
    "starbake.products": "table",
}

EXPECTED_TRANSFORM_EDGES = [
    ("kpi.order_summary", "kpi.top_customers"),
    ("starbake.customers", "kpi.order_summary"),
    ("starbake.orders", "kpi.order_summary"),
    ("starbake.products", "kpi.order_summary"),
]

EXPECTED_LOAD_TRIPLES = {
    ("0 * * * *", "starbake", "orders"),
    ("0 0 * * *", "starbake", "customers"),
    ("0 0 * * *", "starbake", "products"),
}
