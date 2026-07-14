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

from tests.shared.lineage_utils import (
    EXPECTED_LOAD_TRIPLES,
    EXPECTED_TRANSFORM_EDGES,
    EXPECTED_TRANSFORM_NODES,
    extract_embedded_lineage,
    is_transform_dag,
    normalize_lineage,
    normalize_load_schedules,
)

pytestmark = [
    pytest.mark.integration,
]


@pytest.fixture(scope="module")
def generated_python_files(runtime_dags):
    """All generated .py DAG files (same fixture shape as the integration tests)."""
    dags_dir, _, _ = runtime_dags
    files = sorted(dags_dir.glob("*.py"))
    assert len(files) > 0, f"No .py files in {dags_dir}"
    return files


@pytest.fixture(scope="module")
def transform_pipelines(generated_python_files, runtime_env_vars):
    """Load the (single) generated transform DAG through load_pipelines()."""
    from ai.starlake.orchestration.__main__ import load_pipelines

    transform_files = [f for f in generated_python_files if is_transform_dag(f)]
    assert len(transform_files) == 1, (
        f"Expected exactly one transform DAG, got: {[f.name for f in transform_files]}"
    )
    pipelines = load_pipelines(str(transform_files[0]))
    assert pipelines, "No pipelines loaded from the transform DAG"
    return pipelines


class TestDagsterLineageConsumption:

    # -- AC #1 / #3: CLI → template seam -------------------------------------

    def test_embedded_lineage_matches_canonical_graph(self, generated_python_files):
        """The CLI-computed lineage JSON embedded in the generated transform DAG
        normalizes to the canonical graph shared by every orchestrator."""
        transform_files = [f for f in generated_python_files if is_transform_dag(f)]
        assert len(transform_files) == 1
        normalized = normalize_lineage(extract_embedded_lineage(transform_files[0]))
        assert normalized["nodes"] == EXPECTED_TRANSFORM_NODES
        assert normalized["edges"] == EXPECTED_TRANSFORM_EDGES

    # -- AC #1 / #3: template → framework seam -------------------------------

    def test_transform_pipeline_dependency_structure(self, transform_pipelines):
        """load_pipelines() yields the same logical task graph on every orchestrator."""
        assert len(transform_pipelines) == 1
        pipeline = transform_pipelines[0]

        # CLI emits cron="None" (string); the framework must normalize it to None
        assert pipeline.cron is None

        # graphs: with load_dependencies=False only kpi.top_customers is a root
        # graph, its filtered parents == {kpi.order_summary}
        graphs = pipeline.graphs
        assert graphs is not None
        assert {g.id for g in graphs} == {"kpi.top_customers"}
        (graph,) = tuple(graphs)
        assert {p.id for p in graph.parents} == {"kpi.order_summary"}

        # framework task graph — upstream_dependencies[A] lists A's DOWNSTREAMS
        assert "kpi_top_customers_task" in pipeline.upstream_dependencies.get(
            "kpi_order_summary_task", []
        )
        assert "kpi_order_summary_task" in pipeline.downstream_dependencies.get(
            "kpi_top_customers_task", []
        )
        for task_id in ("kpi_order_summary_task", "kpi_top_customers_task"):
            assert task_id in pipeline.tasks_names

    # -- AC #2: sl_load() → sl_transform() lineage preservation --------------

    def test_load_to_transform_lineage_preserved(self, generated_python_files):
        """Every table node in the transform lineage is exactly a table the load
        DAGs cover — the load → transform chain is closed."""
        transform_files = [f for f in generated_python_files if is_transform_dag(f)]
        lineage = normalize_lineage(extract_embedded_lineage(transform_files[0]))
        table_nodes = {n for n, typ in lineage["nodes"].items() if typ == "table"}
        load_tables = {f"{d}.{t}" for (_, d, t) in EXPECTED_LOAD_TRIPLES}
        assert table_nodes == load_tables

    # -- AC #1 / #3: load-side logical surface --------------------------------

    def test_load_pipelines_cover_canonical_tables(
        self, generated_python_files, runtime_env_vars
    ):
        """All load pipelines together cover exactly the canonical
        {(cron, domain, table)} set — file layout may differ per orchestrator
        (Snowflake generates one DAG per table), the logical surface may not."""
        from ai.starlake.orchestration.__main__ import load_pipelines

        load_files = [f for f in generated_python_files if not is_transform_dag(f)]
        assert load_files, "No load DAGs generated"
        pipelines = []
        for f in load_files:
            pipelines.extend(load_pipelines(str(f)) or [])
        assert normalize_load_schedules(pipelines) == EXPECTED_LOAD_TRIPLES
