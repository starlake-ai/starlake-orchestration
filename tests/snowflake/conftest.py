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

import json
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Snowflake SDK availability guard — skip all tests if not installed
# ---------------------------------------------------------------------------

try:
    from snowflake.core.task import StoredProcedureCall  # noqa: F401
    from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation  # noqa: F401
    from snowflake.snowpark import Session  # noqa: F401
    SNOWFLAKE_SDK_AVAILABLE = True
except ImportError:
    SNOWFLAKE_SDK_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not SNOWFLAKE_SDK_AVAILABLE,
    reason="Requires snowflake-snowpark-python and snowflake.core",
)

# SL_ROOT is set by the shared ``sl_root_env`` fixture (tests/conftest.py).

# ---------------------------------------------------------------------------
# Fake caller module — IStarlakeJob.__init__ reads sys.modules[module_name]
# ---------------------------------------------------------------------------

_SNOWFLAKE_TEST_MODULE_NAME = "tests.snowflake._snowflake_caller"


@pytest.fixture(autouse=True, scope="session")
def _register_snowflake_stub_module():
    """Inject a fake caller module with required statements and json_context."""
    _mod = types.ModuleType(_SNOWFLAKE_TEST_MODULE_NAME)
    _mod.__file__ = __file__

    # Required for sl_transform() — statements per sink
    _mod.statements = {
        "kpi.order_summary": {
            "preActions": [],
            "preSqls": [],
            "addSCD2ColumnsSqls": [],
            "mainSqlIfExists": ["SELECT 1"],
            "mainSqlIfNotExists": [
                "CREATE TABLE kpi.order_summary AS SELECT 1"
            ],
            "postsql": [],
            "targetSchema": [],
            "syncStrategy": None,
        },
        "kpi.top_customers": {
            "preActions": [],
            "preSqls": [],
            "addSCD2ColumnsSqls": [],
            "mainSqlIfExists": ["SELECT 1"],
            "mainSqlIfNotExists": [
                "CREATE TABLE kpi.top_customers AS SELECT 1"
            ],
            "postsql": [],
            "targetSchema": [],
            "syncStrategy": None,
        },
    }

    # Required for sl_load() — json_context per sink
    _csv_metadata = {"format": "DSV", "withHeader": True, "separator": ","}
    _mod.json_context = json.dumps({
        "starbake.customers": {
            "tempTableName": "starbake.customers",
            "variant": "false",
            "schema": {
                "pattern": "customers.*\\.csv",
                "metadata": _csv_metadata,
                "presql": [],
                "postsql": [],
            },
        },
        "starbake.orders": {
            "tempTableName": "starbake.orders",
            "variant": "false",
            "schema": {
                "pattern": "orders.*\\.csv",
                "metadata": _csv_metadata,
                "presql": [],
                "postsql": [],
            },
        },
        "starbake.products": {
            "tempTableName": "starbake.products",
            "variant": "false",
            "schema": {
                "pattern": "products.*\\.csv",
                "metadata": _csv_metadata,
                "presql": [],
                "postsql": [],
            },
        },
    })

    # Required for sl_load() — statements for load sinks
    _mod.statements.update({
        "starbake.customers": {
            "steps": "1",
            "writeStrategy": None,
            "createTable": [
                "CREATE TABLE IF NOT EXISTS starbake.customers "
                "(id INT, name VARCHAR)"
            ],
            "schemaString": "id INT, name VARCHAR",
        },
        "starbake.orders": {
            "steps": "1",
            "writeStrategy": None,
            "createTable": [
                "CREATE TABLE IF NOT EXISTS starbake.orders "
                "(id INT, amount DECIMAL)"
            ],
            "schemaString": "id INT, amount DECIMAL",
        },
        "starbake.products": {
            "steps": "1",
            "writeStrategy": None,
            "createTable": [
                "CREATE TABLE IF NOT EXISTS starbake.products "
                "(id INT, name VARCHAR)"
            ],
            "schemaString": "id INT, name VARCHAR",
        },
    })

    # Required for audit/expectations
    _mod.audit = {}
    _mod.expectations = {}
    _mod.expectation_items = {}

    sys.modules[_SNOWFLAKE_TEST_MODULE_NAME] = _mod
    yield
    sys.modules.pop(_SNOWFLAKE_TEST_MODULE_NAME, None)


# ---------------------------------------------------------------------------
# Mock zip_selected_packages to avoid slow package zipping
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True, scope="session")
def _mock_zip_selected_packages():
    """Prevent actual package zipping — return a dummy path.

    ``StarlakeSnowflakeJob`` binds the name via
    ``from ai.starlake.helper import zip_selected_packages``, so the patch
    must target the name as imported *into the job module*; patching
    ``ai.starlake.helper.zip_selected_packages`` would not intercept the
    already-bound reference and the real (slow, /tmp-leaking) zipping runs.
    """
    with patch(
        "ai.starlake.snowflake.starlake_snowflake_job.zip_selected_packages",
        return_value="/tmp/fake_ai.zip",
    ):
        yield


# ---------------------------------------------------------------------------
# Core Snowflake fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    """Mock Snowpark Session — prevents any real Snowflake connection."""
    session = MagicMock(spec=Session)
    session.sql.return_value.collect.return_value = []
    session.call.return_value = None
    return session


@pytest.fixture
def snowflake_job():
    """Create a StarlakeSnowflakeJob instance with mocked SDK."""
    from ai.starlake.snowflake import StarlakeSnowflakeJob
    return StarlakeSnowflakeJob(
        filename="test_snowflake.py",
        module_name=_SNOWFLAKE_TEST_MODULE_NAME,
        options={
            "stage_location": "staging",
            "warehouse": "COMPUTE_WH",
            "sl_incoming_file_stage": "@incoming_stage",
        },
    )


@pytest.fixture
def snowflake_dag_context(snowflake_job):
    """Push a DAG onto the Snowflake _dag_context_stack for direct task creation.

    DAGTask requires a DAG context via ``_get_current_dag()``.
    This fixture provides one for tests that call job methods directly.
    """
    from snowflake.core.task import Cron
    from snowflake.core.task.dagv1 import DAG, _dag_context_stack

    dag = DAG(
        name="test_dag",
        schedule=Cron("0 * * * *", "UTC"),
        stage_location=snowflake_job.stage_location,
        packages=snowflake_job.packages,
    )
    _dag_context_stack.append(dag)
    yield dag
    if _dag_context_stack:
        _dag_context_stack.pop()


@pytest.fixture
def snowflake_orchestration(snowflake_job):
    """Create a SnowflakeOrchestration wrapping the snowflake_job."""
    from ai.starlake.snowflake import SnowflakeOrchestration
    return SnowflakeOrchestration(job=snowflake_job)


# ---------------------------------------------------------------------------
# DAG context stack cleanup — prevent context bleed between tests
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clean_dag_context_stack():
    """Ensure Snowflake _dag_context_stack has a default DAG for each test.

    DAGTask requires a DAG context via ``_get_current_dag()``.
    Base tests call ``job.sl_load()`` / ``job.sl_transform()`` outside
    a ``with pipeline:`` block, so we provide a fallback DAG.
    Pipeline tests push their own DAG on top via ``__enter__``.
    """
    if SNOWFLAKE_SDK_AVAILABLE:
        from snowflake.core.task.dagv1 import DAG, _dag_context_stack
        _dag_context_stack.clear()
        _default_dag = DAG(
            name="__test_default__",
            schedule=None,
            stage_location="staging",
            packages=["croniter", "python-dateutil", "snowflake-snowpark-python"],
        )
        _dag_context_stack.append(_default_dag)
        yield
        _dag_context_stack.clear()
    else:
        yield


# _clean_context_stack is provided by tests/conftest.py (root) as an
# autouse fixture — no need to duplicate it here.


# ---------------------------------------------------------------------------
# Runtime config — consumed by shared ``runtime_env`` / ``runtime_dags``
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def runtime_config():
    """Snowflake-specific runtime configuration for shared runtime fixtures."""
    return {
        "load_dag_ref": "snowflake_load_sql",
        "transform_dag_ref": "snowflake_transform_sql",
        "dag_config_glob": "snowflake_*.sl.yml",
    }


# ---------------------------------------------------------------------------
# Runtime pipeline fixtures — shared by test_snowflake_runtime.py and
# test_snowflake_parser_validation.py (moved here from
# test_snowflake_runtime.py, story 3.2 — behavior-neutral: pytest resolves
# them from the package conftest; each consuming module still gets its own
# module-scoped instances).
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def generated_python_files(runtime_dags):
    """Return all generated ``*.py`` DAG files from ``runtime_dags``."""
    dags_dir, _, _ = runtime_dags
    files = sorted(dags_dir.glob("*.py"))
    assert len(files) > 0, f"No .py files in {dags_dir}"
    return files


@pytest.fixture(scope="module")
def runtime_mock_session():
    """Module-scoped mock Snowpark Session (named to avoid shadowing conftest)."""
    session = MagicMock()
    session.sql.return_value.collect.return_value = []
    session.call.return_value = None
    return session


# MERGE NOTE (story 3.1 / PR #66 coordination): PR #66 promotes an identical
# ``runtime_env_vars`` fixture to tests/shared/conftest.py. Once #66 is
# merged, DELETE this copy — a package-conftest copy would SHADOW the shared
# one. It lives here (not in a test module) because ``loaded_pipelines``
# below must resolve it from conftest scope on this branch.
@pytest.fixture(scope="module")
def runtime_env_vars(runtime_dags):
    """Set runtime env vars for the module and restore on teardown."""
    from tests.shared.conftest import restore_env, set_env

    _, _, env = runtime_dags
    original = set_env(env)
    yield env
    restore_env(original)


@pytest.fixture(scope="module")
def loaded_pipelines(generated_python_files, runtime_env_vars, runtime_mock_session):
    """Load all pipelines from generated DAG files."""
    from ai.starlake.orchestration.__main__ import load_pipelines

    all_pipelines = []
    for py_file in generated_python_files:
        pipelines = load_pipelines(str(py_file))
        if pipelines:
            all_pipelines.extend(pipelines)
    assert len(all_pipelines) > 0, "No pipelines loaded from generated files"
    return all_pipelines
