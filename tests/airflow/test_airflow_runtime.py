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

"""Airflow 2 runtime integration tests.

These tests actually execute DAGs through Airflow's ``dag.test()`` engine,
validate data in DuckDB, verify Airflow Dataset outlet event production,
and test transform DAG triggering with ANY / ALL strategies.

All heavy operations (dag-generate, load_pipelines, dag.test) are
module-scoped to avoid redundant JVM startups.

Prerequisites: Apache Airflow 2.x, Starlake CLI, Java 17+.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple

from datetime import datetime, timezone

import duckdb
import pytest

try:
    import airflow
    from airflow.datasets import Dataset, DatasetAll, DatasetAny
    from airflow.models.dag import DAG
    from airflow.models.dataset import DatasetEvent, DatasetModel
    from airflow.utils.state import DagRunState

    AIRFLOW_AVAILABLE = True
    AIRFLOW_VERSION = tuple(int(x) for x in airflow.__version__.split(".")[:2])
except ImportError:
    AIRFLOW_AVAILABLE = False
    AIRFLOW_VERSION = (0, 0)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not AIRFLOW_AVAILABLE or AIRFLOW_VERSION >= (3, 0),
        reason="Requires Apache Airflow 2.x",
    ),
]

# Use "now" as execution date — the DAG's start_date is derived from
# the generated file's mtime, which is always "today".  An execution
# date in the past would cause Airflow to skip all tasks.
EXECUTION_DATE = datetime.now(timezone.utc).replace(microsecond=0)


# ===================================================================
# Module-scoped fixtures — expensive setup runs ONCE per module
# ===================================================================

@pytest.fixture(scope="module")
def runtime_env(
    sample_project_path, starlake_cli, starlake_env, java_home, tmp_path_factory
):
    """Module-scoped isolated project with incoming data and env vars."""
    project = tmp_path_factory.mktemp("runtime_project")
    isolated = project / "sample-project"
    shutil.copytree(sample_project_path, isolated)

    # Stage incoming data for the IMPORTED pre-load strategy
    incoming = isolated / "datasets" / "incoming" / "starbake"
    incoming.mkdir(parents=True, exist_ok=True)
    for csv in (isolated / "datasets" / "starbake").glob("*.csv"):
        shutil.copy2(csv, incoming / csv.name)
    (incoming / "ack").touch(exist_ok=True)

    env = dict(starlake_env)
    env["SL_ROOT"] = str(isolated)
    env["LOAD_DAG_REF"] = "airflow_load_shell"
    env["TRANSFORM_DAG_REF"] = "airflow_transform_shell"
    env["JAVA_HOME"] = java_home
    starlake_dir = str(Path(starlake_cli).parent)
    full_path = starlake_dir + os.pathsep + env.get(
        "PATH", os.environ.get("PATH", "")
    )
    env["PATH"] = full_path

    # Inject PATH and JAVA_HOME into the ``sl_env_var`` option of each
    # Airflow DAG config so they are included in the BashOperator's env
    # dict (SubprocessHook replaces os.environ with the task env dict).
    import json as _json
    dags_dir = isolated / "metadata" / "dags"
    for yml in dags_dir.glob("airflow_*.sl.yml"):
        content = yml.read_text()
        # The sl_env_var value is an escaped JSON string in the YAML:
        #   sl_env_var: "{\"SL_ROOT\": \"...\", \"SL_ENV\": \"DUCKDB\"}"
        # Add JAVA_HOME and PATH as additional keys in the JSON dict.
        java_esc = java_home.replace('\\', '\\\\').replace('"', '\\"')
        path_esc = full_path.replace('\\', '\\\\').replace('"', '\\"')
        content = content.replace(
            '\\\"SL_ENV\\\": \\\"DUCKDB\\\"',
            '\\\"SL_ENV\\\": \\\"DUCKDB\\\", '
            f'\\\"JAVA_HOME\\\": \\\"{java_esc}\\\", '
            f'\\\"PATH\\\": \\\"{path_esc}\\\"',
        )
        yml.write_text(content)

    return isolated, env, starlake_cli


@pytest.fixture(scope="module")
def runtime_dags(runtime_env, tmp_path_factory):
    """Generate DAG files once for the module."""
    isolated, env, starlake_cli = runtime_env
    out = tmp_path_factory.mktemp("runtime_dags")
    result = subprocess.run(
        [starlake_cli, "dag-generate", "--outputDir", str(out)],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"dag-generate failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    return out, isolated, env


def _set_env(env):
    """Set os.environ from dict, return restore map."""
    original = {}
    for k, v in env.items():
        original[k] = os.environ.get(k)
        os.environ[k] = v
    return original


def _restore_env(original):
    """Restore os.environ from snapshot."""
    for k, v in original.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v



@pytest.fixture(scope="module")
def load_pipelines_loaded(runtime_dags, airflow_home):
    """Load generated load DAGs and register them in Airflow metadata DB."""
    dags_dir, isolated, env = runtime_dags
    orig = _set_env(env)
    try:
        from airflow.configuration import initialize_config
        initialize_config().load_test_config()
        from airflow.utils.db import initdb
        initdb()

        from ai.starlake.orchestration.__main__ import load_pipelines

        pls = []
        for f in sorted(dags_dir.glob("*_tables.py")):
            r = load_pipelines(str(f))
            if r:
                pls.extend(r)
        assert pls, "No load pipelines found"
        for p in pls:
            DAG.bulk_write_to_db([p.dag])
        return pls, isolated, env
    finally:
        _restore_env(orig)


@pytest.fixture(scope="module")
def transform_pipelines_loaded(runtime_dags, airflow_home):
    """Load generated transform DAGs and register in Airflow metadata DB."""
    dags_dir, isolated, env = runtime_dags
    orig = _set_env(env)
    try:
        from airflow.configuration import initialize_config
        initialize_config().load_test_config()
        from airflow.utils.db import initdb
        initdb()

        from ai.starlake.orchestration.__main__ import load_pipelines

        pls = []
        for f in sorted(dags_dir.glob("*_tasks.py")):
            r = load_pipelines(str(f))
            if r:
                pls.extend(r)
        assert pls, "No transform pipelines found"
        for p in pls:
            DAG.bulk_write_to_db([p.dag])
        return pls, isolated, env
    finally:
        _restore_env(orig)


@pytest.fixture(scope="module")
def executed_load_dags(load_pipelines_loaded):
    """Execute all load DAGs once via dag.test() — shared by load tests."""
    pls, isolated, env = load_pipelines_loaded
    orig = _set_env(env)
    try:
        from airflow.configuration import initialize_config
        initialize_config().load_test_config()
        from airflow.utils.db import initdb
        initdb()

        results = []
        for p in pls:

            run_conf = {"start_date": EXECUTION_DATE, "backfill": False}
            dr = p.dag.test(
                execution_date=EXECUTION_DATE, run_conf=run_conf
            )
            results.append((p, dr))
        return results, isolated, env
    finally:
        _restore_env(orig)


@pytest.fixture(scope="module")
def executed_transform_dags(executed_load_dags, transform_pipelines_loaded):
    """Execute all transform DAGs after load — shared by transform tests."""
    _, isolated, env = executed_load_dags
    pls, _, _ = transform_pipelines_loaded
    orig = _set_env(env)
    try:
        from airflow.configuration import initialize_config
        initialize_config().load_test_config()
        from airflow.utils.db import initdb
        initdb()

        results = []
        for p in pls:

            run_conf = {"start_date": EXECUTION_DATE, "backfill": False}
            dr = p.dag.test(
                execution_date=EXECUTION_DATE, run_conf=run_conf
            )
            results.append((p, dr))
        return results, isolated, env
    finally:
        _restore_env(orig)


def _get_duckdb(project_path):
    """Open a read-only DuckDB connection to the project's database."""
    return duckdb.connect(str(project_path / "datasets" / "duckdb.db"), read_only=True)


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
        conn = _get_duckdb(isolated)
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
        """Load tasks declare outlet Datasets for each table.

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

        conn = _get_duckdb(isolated)
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
    """Verify transform DAG dataset-triggered scheduling with ANY / ALL."""

    def test_transform_dag_uses_dataset_triggered_timetable(
        self, transform_pipelines_loaded
    ):
        """Generated transform DAG has a DatasetTriggeredTimetable."""
        pls, _, _ = transform_pipelines_loaded
        for pipeline in pls:
            timetable_cls = type(pipeline.dag.timetable).__name__
            assert "Dataset" in timetable_cls, (
                f"Expected dataset timetable, got {timetable_cls}"
            )
            assert len(pipeline.events) > 0, (
                f"Pipeline {pipeline.pipeline_id} has no dataset events"
            )

    def test_any_strategy_uses_or_combination(self, transform_pipelines_loaded):
        """Default strategy (ANY) builds dataset_condition with DatasetAny."""
        pls, _, _ = transform_pipelines_loaded
        for pipeline in pls:
            timetable = pipeline.dag.timetable
            assert hasattr(timetable, "dataset_condition"), (
                f"Timetable {type(timetable).__name__} has no dataset_condition"
            )
            condition = timetable.dataset_condition
            if len(pipeline.events) > 1:
                assert isinstance(condition, DatasetAny), (
                    f"Expected DatasetAny for ANY strategy, "
                    f"got {type(condition).__name__}"
                )
                condition_uris = {d.uri for d in condition.objects}
                event_uris = {e.uri for e in pipeline.events}
                assert condition_uris == event_uris, (
                    f"Condition URIs {condition_uris} != event URIs {event_uris}"
                )

    def test_all_strategy_uses_and_combination(self, airflow_home):
        """ALL strategy builds dataset_condition with DatasetAll (AND)."""
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
        assert hasattr(timetable, "dataset_condition")
        condition = timetable.dataset_condition

        assert isinstance(condition, DatasetAll), (
            f"Expected DatasetAll for ALL strategy, got {type(condition).__name__}"
        )
        condition_uris = {d.uri for d in condition.objects}
        assert "starbake_orders" in condition_uris
        assert "starbake_customers" in condition_uris
