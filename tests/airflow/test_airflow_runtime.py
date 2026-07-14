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

"""Airflow runtime integration tests (dual-version: Airflow 2 and 3).

These tests actually execute DAGs through Airflow's ``dag.test()`` engine,
validate data in DuckDB, verify Dataset/Asset outlet production, and test
transform DAG triggering with ANY / ALL strategies.

Version divergence is confined to module level:

- Airflow 2 names (``Dataset``/``DatasetAll``/``DatasetAny``,
  ``timetable.dataset_condition``, ``dag.test(execution_date=...)``) map to
  Airflow 3 assets (``Asset``/``AssetAll``/``AssetAny``,
  ``timetable.asset_condition``, ``dag.test(logical_date=...)``).
- DAG registration: Airflow 2 requires ``DAG.bulk_write_to_db``; Airflow 3's
  ``dag.test()`` self-syncs the owning DAG bundle — the fixtures configure
  an explicit ``LocalDagBundle`` over the generated DAGs directory via
  ``AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST``.

All heavy operations (dag-generate, load_pipelines, dag.test) are
module-scoped to avoid redundant JVM startups.

Prerequisites: Apache Airflow 2.x or 3.x, Starlake CLI, Java 17+.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tests.shared.conftest import (
    get_duckdb,
    resolve_duckdb_connection_db_path,
    restore_env,
    set_env,
)

try:
    import airflow

    AIRFLOW_AVAILABLE = True
    AIRFLOW_VERSION = tuple(int(x) for x in airflow.__version__.split(".")[:2])
    SUPPORTS_ASSETS = AIRFLOW_VERSION >= (3, 0)
    if SUPPORTS_ASSETS:
        from airflow.sdk import Asset as Dataset
        from airflow.sdk import AssetAll as DatasetAll
        from airflow.sdk import AssetAny as DatasetAny
    else:
        from airflow.datasets import Dataset, DatasetAll, DatasetAny
    from airflow.models.dag import DAG
    from airflow.utils.state import DagRunState
except ImportError:
    AIRFLOW_AVAILABLE = False
    AIRFLOW_VERSION = (0, 0)
    SUPPORTS_ASSETS = False

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not AIRFLOW_AVAILABLE,
        reason="Requires Apache Airflow",
    ),
]

# The name of the condition attribute on dataset/asset-triggered timetables.
_CONDITION_ATTR = "asset_condition" if SUPPORTS_ASSETS else "dataset_condition"

# Use "now" as execution date — the DAG's start_date is derived from
# the generated file's mtime, which is always "today".  An execution
# date in the past would cause Airflow to skip all tasks.
EXECUTION_DATE = datetime.now(timezone.utc).replace(microsecond=0)


def _runtime_env(env, dags_dir):
    """Return the process env for runtime fixtures (version-aware).

    On Airflow 3, ``dag.test()`` registers DAGs by syncing the owning DAG
    bundle, which parses the bundle's path — point it at the generated DAGs
    directory.  The bundle path must be given EXPLICITLY in the bundle
    config: with no ``path`` kwarg, ``LocalDagBundle`` falls back to
    ``settings.DAGS_FOLDER``, a constant frozen when ``airflow.settings``
    was first imported, which no ``dags_folder`` override can reach.
    On Airflow 2 registration is explicit (``DAG.bulk_write_to_db``) and
    the bundle machinery does not exist.
    """
    e = dict(env)
    # Isolate the metadata DB per session AND per major version.  The
    # unit-test config's default (unittests.db under the import-time
    # AIRFLOW_HOME) is shared state: an Airflow 3 run migrates it to the
    # 3.x schema and a subsequent Airflow 2 run then fails on the renamed
    # columns (and vice versa).  Env vars beat the test-config file layer,
    # so this wins over load_test_config's value.
    e["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
        f"sqlite:///{dags_dir.parent}/airflow_v{AIRFLOW_VERSION[0]}_metadata.db"
    )
    if SUPPORTS_ASSETS:
        import json

        e["AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"] = json.dumps([
            {
                "name": "dags-folder",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(dags_dir)},
            }
        ])
    return e


def _init_airflow_db():
    """Load the test config and initialize the metadata database.

    ``settings.configure_orm()`` must be re-run after the config change:
    the engine was built at import time from the pre-fixture config, so
    without it ``initdb`` (and everything after) would keep talking to
    the import-time database, ignoring the per-version connection set in
    :func:`_runtime_env`.
    """
    from airflow.configuration import initialize_config

    initialize_config().load_test_config()

    from airflow import settings

    settings.configure_vars()
    settings.configure_orm()

    from airflow.utils.db import initdb

    initdb()


def _register_dags(pipelines):
    """Register pipeline DAGs in the metadata DB (Airflow 2 only).

    ``DAG.bulk_write_to_db`` no longer exists on Airflow 3, where
    ``dag.test()`` self-syncs the DAG bundle from the dags folder.
    """
    if not SUPPORTS_ASSETS:
        DAG.bulk_write_to_db([p.dag for p in pipelines])


def _dag_test(dag):
    """Run ``dag.test()`` with version-appropriate kwargs.

    Airflow 3 renamed ``execution_date`` to ``logical_date`` and serializes
    ``run_conf`` as strict JSON — pass the start date as an ISO string there
    (the same shape a REST-triggered run's conf has in production).
    """
    if SUPPORTS_ASSETS:
        run_conf = {"start_date": EXECUTION_DATE.isoformat(), "backfill": False}
        return dag.test(logical_date=EXECUTION_DATE, run_conf=run_conf)
    run_conf = {"start_date": EXECUTION_DATE, "backfill": False}
    return dag.test(execution_date=EXECUTION_DATE, run_conf=run_conf)


# ===================================================================
# Module-scoped fixtures — Airflow-specific DAG loading and execution
# ===================================================================

@pytest.fixture(scope="module")
def load_pipelines_loaded(runtime_dags, airflow_home):
    """Load generated load DAGs and register them in Airflow metadata DB."""
    dags_dir, isolated, env = runtime_dags
    orig = set_env(_runtime_env(env, dags_dir))
    try:
        _init_airflow_db()

        from ai.starlake.orchestration.__main__ import load_pipelines

        pls = []
        for f in sorted(dags_dir.glob("*_tables.py")):
            r = load_pipelines(str(f))
            if r:
                pls.extend(r)
        assert pls, "No load pipelines found"
        _register_dags(pls)
        return pls, isolated, env
    finally:
        restore_env(orig)


@pytest.fixture(scope="module")
def transform_pipelines_loaded(runtime_dags, airflow_home):
    """Load generated transform DAGs and register in Airflow metadata DB."""
    dags_dir, isolated, env = runtime_dags
    orig = set_env(_runtime_env(env, dags_dir))
    try:
        _init_airflow_db()

        from ai.starlake.orchestration.__main__ import load_pipelines

        pls = []
        for f in sorted(dags_dir.glob("*_tasks.py")):
            r = load_pipelines(str(f))
            if r:
                pls.extend(r)
        assert pls, "No transform pipelines found"
        _register_dags(pls)
        return pls, isolated, env
    finally:
        restore_env(orig)


@pytest.fixture(scope="module")
def executed_load_dags(runtime_dags, load_pipelines_loaded):
    """Execute all load DAGs once via dag.test() — shared by load tests."""
    dags_dir, _, _ = runtime_dags
    pls, isolated, env = load_pipelines_loaded
    orig = set_env(_runtime_env(env, dags_dir))
    try:
        _init_airflow_db()

        results = []
        for p in pls:

            dr = _dag_test(p.dag)
            results.append((p, dr))
        return results, isolated, env
    finally:
        restore_env(orig)


@pytest.fixture(scope="module")
def executed_transform_dags(runtime_dags, executed_load_dags, transform_pipelines_loaded):
    """Execute all transform DAGs after load — shared by transform tests."""
    dags_dir, _, _ = runtime_dags
    _, isolated, env = executed_load_dags
    pls, _, _ = transform_pipelines_loaded
    orig = set_env(_runtime_env(env, dags_dir))
    try:
        _init_airflow_db()

        results = []
        for p in pls:

            dr = _dag_test(p.dag)
            results.append((p, dr))
        return results, isolated, env
    finally:
        restore_env(orig)


# ===================================================================
# TestAirflowRuntimeLoad
# ===================================================================

class TestAirflowRuntimeLoad:
    """Run the load DAG through Airflow and validate DuckDB + outlets."""

    def test_load_dag_executes_successfully(self, executed_load_dags):
        """dag.test() returns DagRun(s) with SUCCESS state."""
        results, _, _ = executed_load_dags
        for pipeline, dr in results:
            assert dr.state == DagRunState.SUCCESS, (
                f"Load DAG {pipeline.pipeline_id} ended in state {dr.state}"
            )

    def test_load_dag_populates_duckdb_tables(self, executed_load_dags):
        """After execution, DuckDB has the expected tables and row counts."""
        _, isolated, _ = executed_load_dags
        conn = get_duckdb(isolated)
        try:
            customers = conn.execute(
                "SELECT count(*) FROM starbake.customers"
            ).fetchone()[0]
            assert customers == 7, f"Expected 7 customers, got {customers}"

            orders = conn.execute(
                "SELECT count(*) FROM starbake.orders"
            ).fetchone()[0]
            assert orders == 10, f"Expected 10 orders, got {orders}"

            products = conn.execute(
                "SELECT count(*) FROM starbake.products"
            ).fetchone()[0]
            assert products == 5, f"Expected 5 products, got {products}"
        finally:
            conn.close()

    def test_load_tasks_produce_dataset_outlets(self, load_pipelines_loaded):
        """Load tasks declare outlet Datasets/Assets for each table.

        Outlets are collected across ALL load pipelines (daily + hourly)
        since tables are split by schedule frequency.
        """
        pls, _, _ = load_pipelines_loaded
        all_outlet_uris = set()
        for pipeline in pls:
            for task in pipeline.dag.tasks:
                for outlet in getattr(task, "outlets", []):
                    if isinstance(outlet, Dataset):
                        all_outlet_uris.add(outlet.uri)

        for table in ("customers", "orders", "products"):
            expected = f"starbake_{table}"
            assert expected in all_outlet_uris or any(
                expected in uri for uri in all_outlet_uris
            ), (
                f"Expected outlet URI containing '{expected}' "
                f"in {all_outlet_uris}"
            )


# ===================================================================
# TestAirflowRuntimeTransform
# ===================================================================

class TestAirflowRuntimeTransform:
    """Run load + transform DAGs, validate transform results in DuckDB."""

    def test_transform_dag_executes_after_load(self, executed_transform_dags):
        """Execute load then transform — kpi tables are populated."""
        results, isolated, _ = executed_transform_dags
        for pipeline, dr in results:
            assert dr.state == DagRunState.SUCCESS, (
                f"Transform DAG {pipeline.pipeline_id} ended in {dr.state}"
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
            assert top_customers <= 5, (
                f"top_customers has {top_customers} rows, expected <= 5"
            )
        finally:
            conn.close()


# ===================================================================
# TestAirflowDatasetTriggering
# ===================================================================

class TestAirflowDatasetTriggering:
    """Verify transform DAG dataset/asset-triggered scheduling with ANY / ALL."""

    def test_transform_dag_uses_dataset_triggered_timetable(
        self, transform_pipelines_loaded
    ):
        """Generated transform DAG has a Dataset/AssetTriggeredTimetable."""
        pls, _, _ = transform_pipelines_loaded
        expected_marker = "Asset" if SUPPORTS_ASSETS else "Dataset"
        for pipeline in pls:
            timetable_cls = type(pipeline.dag.timetable).__name__
            assert expected_marker in timetable_cls, (
                f"Expected {expected_marker.lower()} timetable, got {timetable_cls}"
            )
            assert len(pipeline.events) > 0, (
                f"Pipeline {pipeline.pipeline_id} has no dataset events"
            )

    def test_any_strategy_uses_or_combination(self, transform_pipelines_loaded):
        """Default strategy (ANY) builds the condition with DatasetAny/AssetAny."""
        pls, _, _ = transform_pipelines_loaded
        for pipeline in pls:
            timetable = pipeline.dag.timetable
            assert hasattr(timetable, _CONDITION_ATTR), (
                f"Timetable {type(timetable).__name__} has no {_CONDITION_ATTR}"
            )
            condition = getattr(timetable, _CONDITION_ATTR)
            if len(pipeline.events) > 1:
                assert isinstance(condition, DatasetAny), (
                    f"Expected {DatasetAny.__name__} for ANY strategy, "
                    f"got {type(condition).__name__}"
                )
                condition_uris = {d.uri for d in condition.objects}
                event_uris = {e.uri for e in pipeline.events}
                assert condition_uris == event_uris, (
                    f"Condition URIs {condition_uris} != event URIs {event_uris}"
                )

    def test_all_strategy_uses_and_combination(self, airflow_home):
        """ALL strategy builds the condition with DatasetAll/AssetAll (AND)."""
        from ai.starlake.airflow import AirflowOrchestration
        from ai.starlake.airflow.bash import StarlakeAirflowBashJob
        from ai.starlake.orchestration import (
            StarlakeDependencies,
            StarlakeDependency,
            StarlakeDependencyType,
        )
        from tests.airflow.conftest import _AIRFLOW_TEST_MODULE_NAME

        job = StarlakeAirflowBashJob(
            filename="test_all_strategy.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options={"dataset_triggering_strategy": "all"},
        )
        orch = AirflowOrchestration(job=job)
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

        timetable = pipeline.dag.timetable
        assert hasattr(timetable, _CONDITION_ATTR)
        condition = getattr(timetable, _CONDITION_ATTR)

        assert isinstance(condition, DatasetAll), (
            f"Expected {DatasetAll.__name__} for ALL strategy, "
            f"got {type(condition).__name__}"
        )
        condition_uris = {d.uri for d in condition.objects}
        assert "starbake_orders" in condition_uris
        assert "starbake_customers" in condition_uris


# ===================================================================
# TestConnectionEndToEnd (AC #2)
# ===================================================================

class TestConnectionEndToEnd:
    """Data lands exactly where application.sl.yml's connection points.

    The orchestrator never read the connection URL — Starlake CLI
    resolved it from SL_ROOT/SL_ENV alone.  Re-deriving the location
    from the user-facing YAML and finding the loaded rows there proves
    the connection chain end-to-end.
    """

    def test_load_written_to_connection_url_database(self, executed_load_dags):
        _, isolated, _ = executed_load_dags
        db_path = resolve_duckdb_connection_db_path(isolated)
        assert db_path == isolated / "datasets" / "duckdb.db"
        assert db_path.is_file(), f"No DuckDB database at {db_path}"
        conn = get_duckdb(isolated)
        try:
            customers = conn.execute(
                "SELECT count(*) FROM starbake.customers"
            ).fetchone()[0]
            # MERGE NOTE: after PR #68 (tests/shared/expected_results.py)
            # merges, use EXPECTED_ROW_COUNTS["customers"] instead of the
            # literal 7 (matching the module's row-count tests above).
            assert customers == 7, f"Expected 7 customers, got {customers}"
        finally:
            conn.close()
