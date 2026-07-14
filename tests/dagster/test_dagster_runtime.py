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

"""Dagster runtime integration tests.

These tests actually execute Dagster jobs via ``execute_in_process()``,
validate data in DuckDB, verify ``AssetMaterialization`` event
production, and test ``MultiAssetSensorDefinition`` with ANY / ALL
strategies.

All heavy operations (dag-generate, load_pipelines, execute) are
module-scoped to avoid redundant JVM startups.

Prerequisites: dagster, dagster-shell, Starlake CLI, Java 17+.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest

from dagster import AssetKey, DagsterInstance

from tests.shared.conftest import get_duckdb, restore_env, set_env
from tests.shared.expected_results import (
    EXPECTED_KPI_SNAPSHOTS,
    EXPECTED_ROW_COUNTS,
    EXPECTED_TABLE_SNAPSHOTS,
    TOP_CUSTOMERS_MAX_ROWS,
    table_snapshot,
)

pytestmark = [
    pytest.mark.integration,
]

EXECUTION_DATE = datetime.now(timezone.utc).replace(microsecond=0)


# ===================================================================
# Module-scoped fixtures — Dagster-specific DAG loading and execution
# ===================================================================

@pytest.fixture(scope="module")
def load_pipelines_loaded(runtime_dags):
    """Load generated load DAGs via load_pipelines()."""
    dags_dir, isolated, env = runtime_dags
    orig = set_env(env)
    try:
        from ai.starlake.orchestration.__main__ import load_pipelines

        pls = []
        for f in sorted(dags_dir.glob("*_tables.py")):
            r = load_pipelines(str(f))
            if r:
                pls.extend(r)
        assert pls, "No load pipelines found"
        return pls, isolated, env
    finally:
        restore_env(orig)


@pytest.fixture(scope="module")
def transform_pipelines_loaded(runtime_dags):
    """Load generated transform DAGs via load_pipelines()."""
    dags_dir, isolated, env = runtime_dags
    orig = set_env(env)
    try:
        from ai.starlake.orchestration.__main__ import load_pipelines

        pls = []
        for f in sorted(dags_dir.glob("*_tasks.py")):
            r = load_pipelines(str(f))
            if r:
                pls.extend(r)
        assert pls, "No transform pipelines found"
        return pls, isolated, env
    finally:
        restore_env(orig)


@pytest.fixture(scope="module")
def executed_load_jobs(load_pipelines_loaded):
    """Execute all load jobs once via execute_in_process()."""
    from ai.starlake.common import sl_timestamp_format

    pls, isolated, env = load_pipelines_loaded
    orig = set_env(env)
    try:
        results = []
        for p in pls:
            logical_datetime = EXECUTION_DATE.strftime(sl_timestamp_format)
            run_config = p._ops_config(logical_datetime)
            with DagsterInstance.ephemeral() as instance:
                result = p.dag.execute_in_process(
                    run_config=run_config,
                    instance=instance,
                )
            results.append((p, result))
        return results, isolated, env
    finally:
        restore_env(orig)


@pytest.fixture(scope="module")
def executed_transform_jobs(executed_load_jobs, transform_pipelines_loaded):
    """Execute all transform jobs after load."""
    from ai.starlake.common import sl_timestamp_format

    _, isolated, env = executed_load_jobs
    pls, _, _ = transform_pipelines_loaded
    orig = set_env(env)
    try:
        results = []
        for p in pls:
            logical_datetime = EXECUTION_DATE.strftime(sl_timestamp_format)
            run_config = p._ops_config(logical_datetime)
            with DagsterInstance.ephemeral() as instance:
                result = p.dag.execute_in_process(
                    run_config=run_config,
                    instance=instance,
                )
            results.append((p, result))
        return results, isolated, env
    finally:
        restore_env(orig)


# ===================================================================
# TestDagsterRuntimeLoad
# ===================================================================

class TestDagsterRuntimeLoad:
    """Run the load job through Dagster and validate DuckDB + materializations."""

    def test_load_job_executes_successfully(self, executed_load_jobs):
        """execute_in_process() returns success."""
        results, _, _ = executed_load_jobs
        for pipeline, result in results:
            assert result.success, (
                f"Load job {pipeline.pipeline_id} failed"
            )

    def test_load_job_populates_duckdb_tables(self, executed_load_jobs):
        """After execution, DuckDB has the expected tables and row counts."""
        _, isolated, _ = executed_load_jobs
        conn = get_duckdb(isolated)
        try:
            for table, expected_count in EXPECTED_ROW_COUNTS.items():
                count = conn.execute(
                    f"SELECT count(*) FROM {table}"
                ).fetchone()[0]
                assert count == expected_count, (
                    f"Expected {expected_count} rows in {table}, got {count}"
                )
        finally:
            conn.close()

    def test_duckdb_state_matches_canonical_snapshots(self, executed_load_jobs):
        """NFR1: the loaded data equals the canonical cross-orchestrator snapshot."""
        _, isolated, _ = executed_load_jobs
        conn = get_duckdb(isolated)
        try:
            for table, expected_rows in EXPECTED_TABLE_SNAPSHOTS.items():
                assert table_snapshot(conn, table) == expected_rows, (
                    f"{table}: DuckDB state diverges from the canonical snapshot"
                )
        finally:
            conn.close()

    def test_load_ops_produce_asset_materializations(self, executed_load_jobs):
        """Load ops produce AssetMaterialization events with correct asset keys."""
        results, _, _ = executed_load_jobs
        all_asset_keys = set()
        for pipeline, result in results:
            for event in result.get_asset_materialization_events():
                mat = event.step_materialization_data.materialization
                assert isinstance(mat.asset_key, AssetKey)
                all_asset_keys.add(mat.asset_key.to_user_string())

        assert len(all_asset_keys) > 0, (
            "Expected at least one AssetMaterialization from load ops"
        )
        # Verify materialized assets include the expected tables
        for table in ("customers", "orders", "products"):
            expected = "starbake_{}".format(table)
            assert any(expected in key for key in all_asset_keys), (
                "Expected asset key containing '{}' in {}".format(
                    expected, all_asset_keys
                )
            )


# ===================================================================
# TestDagsterRuntimeTransform
# ===================================================================

class TestDagsterRuntimeTransform:
    """Run load + transform jobs, validate transform results in DuckDB."""

    def test_transform_job_executes_after_load(self, executed_transform_jobs):
        """Execute load then transform — kpi tables are populated."""
        results, isolated, _ = executed_transform_jobs
        for pipeline, result in results:
            assert result.success, (
                f"Transform job {pipeline.pipeline_id} failed"
            )

        conn = get_duckdb(isolated)
        try:
            order_summary = conn.execute(
                "SELECT count(*) FROM kpi.order_summary"
            ).fetchone()[0]
            assert order_summary > 0, "kpi.order_summary is empty"

            top_customers = conn.execute(
                "SELECT count(*) FROM kpi.top_customers"
            ).fetchone()[0]
            assert top_customers > 0, "kpi.top_customers is empty"
            assert top_customers <= TOP_CUSTOMERS_MAX_ROWS, (
                f"top_customers has {top_customers} rows, "
                f"expected <= {TOP_CUSTOMERS_MAX_ROWS}"
            )
        finally:
            conn.close()

    def test_transform_results_match_canonical_snapshots(
        self, executed_transform_jobs
    ):
        """NFR1: transform outputs equal the canonical cross-orchestrator snapshot."""
        _, isolated, _ = executed_transform_jobs
        conn = get_duckdb(isolated)
        try:
            for table, expected_rows in EXPECTED_KPI_SNAPSHOTS.items():
                assert table_snapshot(conn, table) == expected_rows, (
                    f"{table}: DuckDB state diverges from the canonical snapshot"
                )
        finally:
            conn.close()

    def test_transform_ops_produce_asset_materializations(self, executed_transform_jobs):
        """Transform ops produce AssetMaterialization events with correct asset keys."""
        results, _, _ = executed_transform_jobs
        all_asset_keys = set()
        for pipeline, result in results:
            for event in result.get_asset_materialization_events():
                mat = event.step_materialization_data.materialization
                assert isinstance(mat.asset_key, AssetKey)
                all_asset_keys.add(mat.asset_key.to_user_string())

        assert len(all_asset_keys) > 0, (
            "Expected at least one AssetMaterialization from transform ops"
        )
        # Verify at least one kpi-related asset was materialized
        assert any("kpi" in key or "order_summary" in key or "top_customers" in key
                    for key in all_asset_keys), (
            "Expected at least one kpi-related asset in {}".format(all_asset_keys)
        )


# ===================================================================
# TestDagsterDatasetTriggering
# ===================================================================

class TestDagsterDatasetTriggering:
    """Verify sensor creation for dataset-triggered scheduling."""

    def test_transform_pipeline_has_sensor(self, transform_pipelines_loaded):
        """Generated transform pipeline produces a sensor definition."""
        pls, _, _ = transform_pipelines_loaded
        for pipeline in pls:
            assert len(pipeline.events) > 0, (
                f"Pipeline {pipeline.pipeline_id} has no dataset events"
            )

    def _make_sensor_orchestration(self, strategy):
        """Build a DagsterOrchestration with the given dataset triggering strategy."""
        from ai.starlake.dagster import DagsterOrchestration
        from ai.starlake.dagster.shell import StarlakeDagsterShellJob
        from ai.starlake.orchestration import (
            StarlakeDependencies,
            StarlakeDependency,
            StarlakeDependencyType,
        )
        from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME

        job = StarlakeDagsterShellJob(
            filename="test_{}_strategy.py".format(strategy),
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={"dataset_triggering_strategy": strategy},
        )
        with DagsterOrchestration(job=job) as orch:
            dependencies = StarlakeDependencies([
                StarlakeDependency(
                    name="overall_kpis",
                    dependency_type=StarlakeDependencyType.TASK,
                    dependencies=[
                        StarlakeDependency(
                            name="starbake.orders",
                            dependency_type=StarlakeDependencyType.TABLE,
                            cron="0 * * * *",
                        ),
                        StarlakeDependency(
                            name="starbake.customers",
                            dependency_type=StarlakeDependencyType.TABLE,
                            cron="0 0 * * *",
                        ),
                    ],
                ),
            ])
            pipeline = orch.sl_create_pipeline(dependencies=dependencies)
            with pipeline:
                pass
        return pipeline, orch

    def test_all_strategy_produces_sensor(self):
        """ALL strategy builds a MultiAssetSensorDefinition for the pipeline."""
        pipeline, orch = self._make_sensor_orchestration("all")

        assert hasattr(orch, "definitions")
        sensors = orch.definitions.sensors
        assert len(sensors) == 1, "Expected 1 sensor, got {}".format(len(sensors))

        # Pipeline events are the AssetKeys passed to the sensor's monitored_assets
        event_paths = {e.path[0] for e in pipeline.events}
        assert "starbake_orders" in event_paths, (
            "Expected 'starbake_orders' in event paths, got {}".format(event_paths)
        )
        assert "starbake_customers" in event_paths, (
            "Expected 'starbake_customers' in event paths, got {}".format(event_paths)
        )

        # Verify sensor is named after the pipeline
        assert pipeline.pipeline_id in sensors[0].name, (
            "Sensor name '{}' should contain pipeline id '{}'".format(
                sensors[0].name, pipeline.pipeline_id
            )
        )

    def test_any_strategy_produces_sensor(self):
        """ANY strategy builds a MultiAssetSensorDefinition for the pipeline."""
        pipeline, orch = self._make_sensor_orchestration("any")

        sensors = orch.definitions.sensors
        assert len(sensors) == 1, "Expected 1 sensor, got {}".format(len(sensors))

        # Pipeline events are the AssetKeys passed to the sensor's monitored_assets
        event_paths = {e.path[0] for e in pipeline.events}
        assert "starbake_orders" in event_paths, (
            "Expected 'starbake_orders' in event paths, got {}".format(event_paths)
        )
        assert "starbake_customers" in event_paths, (
            "Expected 'starbake_customers' in event paths, got {}".format(event_paths)
        )

        # Verify sensor is named after the pipeline
        assert pipeline.pipeline_id in sensors[0].name, (
            "Sensor name '{}' should contain pipeline id '{}'".format(
                sensors[0].name, pipeline.pipeline_id
            )
        )

    @pytest.mark.parametrize("strategy", ["all", "any"])
    def test_sensor_skip_reason_when_no_materializations(self, tmp_path, strategy):
        """Sensor returns no RunRequests when no upstream assets materialized."""
        from dagster import build_multi_asset_sensor_context
        from ai.starlake.dagster import DagsterOrchestration
        from ai.starlake.dagster.shell import StarlakeDagsterShellJob
        from ai.starlake.orchestration import (
            StarlakeDependencies,
            StarlakeDependency,
            StarlakeDependencyType,
        )
        from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME

        job = StarlakeDagsterShellJob(
            filename="test_skip_{}.py".format(strategy),
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={"dataset_triggering_strategy": strategy},
        )
        with DagsterOrchestration(job=job) as orch:
            dependencies = StarlakeDependencies([
                StarlakeDependency(
                    name="overall_kpis",
                    dependency_type=StarlakeDependencyType.TASK,
                    dependencies=[
                        StarlakeDependency(
                            name="starbake.orders",
                            dependency_type=StarlakeDependencyType.TABLE,
                            cron="0 * * * *",
                        ),
                    ],
                ),
            ])
            pipeline = orch.sl_create_pipeline(dependencies=dependencies)
            with pipeline:
                pass

        sensors = orch.definitions.sensors
        assert len(sensors) == 1, "Expected 1 sensor, got {}".format(len(sensors))
        sensor_def = sensors[0]

        # build_multi_asset_sensor_context needs a persistent instance
        instance = DagsterInstance.local_temp(tempdir=str(tmp_path))
        try:
            context = build_multi_asset_sensor_context(
                monitored_assets=[AssetKey("starbake_orders")],
                instance=instance,
                definitions=orch.definitions,
            )
            result = sensor_def.evaluate_tick(context)
            # With no materializations, sensor should skip
            assert len(result.run_requests) == 0, (
                "Expected no RunRequests for {} strategy with no materializations, "
                "got {}".format(strategy, len(result.run_requests))
            )
        finally:
            instance.dispose()
