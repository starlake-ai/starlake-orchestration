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

import os
import sys
import types
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Airflow 2 availability guard — skip all tests if not installed or if v3+
# ---------------------------------------------------------------------------

try:
    import airflow
    AIRFLOW_AVAILABLE = True
    AIRFLOW_VERSION = tuple(int(x) for x in airflow.__version__.split(".")[:2])
except ImportError:
    AIRFLOW_AVAILABLE = False
    AIRFLOW_VERSION = (0, 0)

pytestmark = pytest.mark.skipif(
    not AIRFLOW_AVAILABLE or AIRFLOW_VERSION >= (3, 0),
    reason="Requires Apache Airflow 2.x",
)

# ---------------------------------------------------------------------------
# AIRFLOW_HOME — must be set before Airflow modules are used
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True, scope="session")
def airflow_home(tmp_path_factory):
    """Set AIRFLOW_HOME to a temporary directory for test isolation.

    Also creates the ``dags/`` subdirectory required by ``AirflowPipeline.deploy()``.
    """
    home = tmp_path_factory.mktemp("airflow_home")
    (home / "dags").mkdir()
    os.environ["AIRFLOW_HOME"] = str(home)
    return home


# ---------------------------------------------------------------------------
# Fake caller module — IStarlakeJob.__init__ reads sys.modules[module_name]
# ---------------------------------------------------------------------------

_AIRFLOW_TEST_MODULE_NAME = "tests.airflow._airflow_caller"


@pytest.fixture(autouse=True, scope="session")
def _register_airflow_stub_module():
    """Inject a fake caller module into sys.modules for the test session."""
    _mod = types.ModuleType(_AIRFLOW_TEST_MODULE_NAME)
    _mod.__file__ = __file__
    sys.modules[_AIRFLOW_TEST_MODULE_NAME] = _mod
    yield
    sys.modules.pop(_AIRFLOW_TEST_MODULE_NAME, None)


# ---------------------------------------------------------------------------
# Airflow-specific environment variables
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True, scope="session")
def airflow_env(sample_project_path):
    """Inject SL_ROOT and other required env vars for Airflow tests."""
    original_sl_root = os.environ.get("SL_ROOT")
    os.environ["SL_ROOT"] = str(sample_project_path)
    yield
    if original_sl_root is not None:
        os.environ["SL_ROOT"] = original_sl_root
    else:
        os.environ.pop("SL_ROOT", None)


# ---------------------------------------------------------------------------
# Core Airflow fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def airflow_job():
    """Create a StarlakeAirflowBashJob instance for Shell executor tests."""
    from ai.starlake.airflow.bash import StarlakeAirflowBashJob
    return StarlakeAirflowBashJob(
        filename="test_airflow.py",
        module_name=_AIRFLOW_TEST_MODULE_NAME,
        options={},
    )


@pytest.fixture
def airflow_orchestration(airflow_job):
    """Create an AirflowOrchestration wrapping the airflow_job."""
    from ai.starlake.airflow import AirflowOrchestration
    return AirflowOrchestration(job=airflow_job)


# _clean_context_stack is provided by tests/conftest.py (root) as an
# autouse fixture — no need to duplicate it here.


# ---------------------------------------------------------------------------
# Mock Airflow REST API calls — no running Airflow server in unit tests
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _mock_airflow_api(monkeypatch):
    """Neutralise HTTP calls made by AirflowPipeline.run() and .delete().

    ``run(mode=RUN)`` POSTs to the Airflow REST API and then polls state.
    ``delete()`` sends a DELETE request.  Neither is available without a
    running Airflow web server so we mock ``requests`` to return
    successful stub responses.
    """
    from unittest.mock import MagicMock

    def _mock_response(status_code=200, json_data=None):
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = json_data or {}
        resp.raise_for_status.return_value = None
        return resp

    def _mock_post(*args, **kwargs):
        return _mock_response(
            json_data={
                "dag_run_id": "mock_run_1",
                "state": "success",
            },
        )

    def _mock_get(*args, **kwargs):
        return _mock_response(json_data={"state": "success"})

    def _mock_delete(*args, **kwargs):
        return _mock_response()

    import requests as _requests_mod
    monkeypatch.setattr(_requests_mod, "post", _mock_post)
    monkeypatch.setattr(_requests_mod, "get", _mock_get)
    monkeypatch.setattr(_requests_mod, "delete", _mock_delete)


# ---------------------------------------------------------------------------
# Runtime config — consumed by shared ``runtime_env`` / ``runtime_dags``
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def runtime_config():
    """Airflow-specific runtime configuration for shared runtime fixtures."""
    return {
        "load_dag_ref": "airflow_load_shell",
        "transform_dag_ref": "airflow_transform_shell",
        "dag_config_glob": "airflow_*.sl.yml",
    }
