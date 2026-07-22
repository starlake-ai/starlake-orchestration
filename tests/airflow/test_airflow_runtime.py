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

from tests.shared.conftest import get_duckdb, resolve_duckdb_connection_db_path, restore_env, set_env
from tests.shared.expected_results import (
    EXPECTED_KPI_SNAPSHOTS,
    EXPECTED_ROW_COUNTS,
    EXPECTED_TABLE_SNAPSHOTS,
    TOP_CUSTOMERS_MAX_ROWS,
    table_snapshot,
)

from tests.airflow.dataset_compat import (
    AIRFLOW_AVAILABLE,
    AIRFLOW_VERSION,
    SUPPORTS_ASSETS,
    SUPPORTS_CONDITION_INTROSPECTION,
    Dataset,
    DatasetAll,
    DatasetAny,
    CONDITION_ATTR as _CONDITION_ATTR,
)

try:
    from airflow.models.dag import DAG
    from airflow.utils.state import DagRunState
except ImportError:  # pragma: no cover — collection guard only
    pass

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not AIRFLOW_AVAILABLE,
        reason="Requires Apache Airflow",
    ),
]

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
    """Run ``dag.test()`` with version-appropriate kwargs and return the DagRun.

    Airflow 3 renamed ``execution_date`` to ``logical_date`` and serializes
    ``run_conf`` as strict JSON — pass the start date as an ISO string there
    (the same shape a REST-triggered run's conf has in production).

    Airflow < 2.10's ``dag.test()`` executes the DAG but returns ``None`` (the
    DagRun return value was added later). Fetch the persisted DagRun from the
    metadata DB in that case so callers can still assert on its final state.
    """
    if SUPPORTS_ASSETS:
        run_conf = {"start_date": EXECUTION_DATE.isoformat(), "backfill": False}
        dr = dag.test(logical_date=EXECUTION_DATE, run_conf=run_conf)
    else:
        run_conf = {"start_date": EXECUTION_DATE, "backfill": False}
        dr = dag.test(execution_date=EXECUTION_DATE, run_conf=run_conf)
    if dr is None:
        from airflow.models import DagRun
        from airflow.utils.session import create_session

        # Fetch the exact run dag.test() just executed (filter by the same
        # execution_date), not merely the latest run for this dag_id — a
        # dataset-triggered DAG can persist downstream runs at later dates.
        with create_session() as session:
            dr = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == dag.dag_id,
                    DagRun.execution_date == EXECUTION_DATE,
                )
                .one_or_none()
            )
        assert dr is not None, (
            f"dag.test() persisted no DagRun for {dag.dag_id} @ {EXECUTION_DATE}"
        )
    return dr


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
            for table, expected_count in EXPECTED_ROW_COUNTS.items():
                count = conn.execute(
                    f"SELECT count(*) FROM {table}"
                ).fetchone()[0]
                assert count == expected_count, (
                    f"Expected {expected_count} rows in {table}, got {count}"
                )
        finally:
            conn.close()

    def test_duckdb_state_matches_canonical_snapshots(self, executed_load_dags):
        """NFR1: the loaded data equals the canonical cross-orchestrator snapshot."""
        _, isolated, _ = executed_load_dags
        conn = get_duckdb(isolated)
        try:
            for table, expected_rows in EXPECTED_TABLE_SNAPSHOTS.items():
                assert table_snapshot(conn, table) == expected_rows, (
                    f"{table}: DuckDB state diverges from the canonical snapshot"
                )
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

    def test_load_events_carry_runtime_extra(self, executed_load_dags):
        """The persisted Dataset/Asset events carry Starlake's runtime extra —
        the real end-to-end producer round-trip against Airflow's own emission
        path (issue #125).

        On Airflow < 2.10 this exercises the ``register_dataset_change`` wrapper
        (the extra would otherwise be dropped); on 2.10+ the ``outlet_events``
        accessor; on 3.x the asset event path. This is the assertion the mocked
        unit test cannot make — it validates the *real* method signature and
        that the extra actually reaches the emitted event.
        """
        _, _, _ = executed_load_dags
        from airflow.utils.session import create_session
        from ai.starlake.common import StarlakeParameters

        URI = StarlakeParameters.URI_PARAMETER.value
        SINK = StarlakeParameters.SINK_PARAMETER.value
        if SUPPORTS_ASSETS:
            from airflow.models.asset import AssetEvent as EventModel, AssetModel as UriModel
            id_col = "asset_id"
        else:
            from airflow.models.dataset import DatasetEvent as EventModel, DatasetModel as UriModel
            id_col = "dataset_id"

        with create_session() as session:
            uri_by_id = {row.id: row.uri for row in session.query(UriModel).all()}
            table_extras = {}
            for event in session.query(EventModel).all():
                uri = uri_by_id.get(getattr(event, id_col))
                # The per-table load outlets have uri "starbake_<table>"; the
                # DAG-level completion dataset ("airflow_starbake_tables_*") is a
                # different concern and is excluded by the startswith filter.
                if uri and uri.startswith("starbake_"):
                    table_extras[uri] = event.extra or {}

        assert table_extras, "the load run emitted no per-table dataset/asset events"
        # issue #125 regression guard: each table event must carry the full
        # runtime metadata onto the event, not be dropped to {}.
        for uri, extra in table_extras.items():
            assert extra.get(URI), f"event for {uri} lost {URI} (extra={extra})"
            assert extra.get(SINK), f"event for {uri} lost {SINK} (extra={extra})"
            assert "ts" in extra, f"event for {uri} lost ts (extra={extra})"
        for table in ("customers", "orders", "products"):
            assert any(table in uri for uri in table_extras), (
                f"no per-table event for '{table}'; saw {set(table_extras)}"
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
            assert top_customers <= TOP_CUSTOMERS_MAX_ROWS, (
                f"top_customers has {top_customers} rows, "
                f"expected <= {TOP_CUSTOMERS_MAX_ROWS}"
            )
        finally:
            conn.close()

    def test_transform_results_match_canonical_snapshots(
        self, executed_transform_dags
    ):
        """NFR1: transform outputs equal the canonical cross-orchestrator snapshot."""
        _, isolated, _ = executed_transform_dags
        conn = get_duckdb(isolated)
        try:
            for table, expected_rows in EXPECTED_KPI_SNAPSHOTS.items():
                assert table_snapshot(conn, table) == expected_rows, (
                    f"{table}: DuckDB state diverges from the canonical snapshot"
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
        """Default strategy (ANY) builds the condition with DatasetAny/AssetAny.

        The timetable only exposes the condition object from Airflow 2.10 on;
        below that (incl. 2.9, where DatasetAny builds but isn't introspectable,
        and 2.5-2.8, where ANY degrades to a flat list) we can only assert the
        DAG is dataset-triggered (issue #125)."""
        pls, _, _ = transform_pipelines_loaded
        for pipeline in pls:
            timetable = pipeline.dag.timetable
            if not SUPPORTS_CONDITION_INTROSPECTION:
                assert "Dataset" in type(timetable).__name__, (
                    f"Expected a dataset-triggered timetable, got "
                    f"{type(timetable).__name__}"
                )
                assert len(pipeline.events) > 0
                continue
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
        if not SUPPORTS_CONDITION_INTROSPECTION:
            # Below Airflow 2.10 the timetable does not expose the condition
            # object (2.9 builds DatasetAll but hides it; 2.5-2.8 use a native
            # flat list). Either way the DAG is dataset-triggered.
            assert "Dataset" in type(timetable).__name__
            assert len(pipeline.events) == 2
            return

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
