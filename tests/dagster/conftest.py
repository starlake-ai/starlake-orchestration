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

import sys
import types

import pytest

# ---------------------------------------------------------------------------
# Dagster availability guard — skip all tests if not installed
# ---------------------------------------------------------------------------

try:
    import dagster  # noqa: F401
    import dagster_shell  # noqa: F401
    DAGSTER_AVAILABLE = True
except ImportError:
    DAGSTER_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not DAGSTER_AVAILABLE,
    reason="Requires dagster and dagster-shell",
)

# SL_ROOT is set by the shared ``sl_root_env`` fixture (tests/conftest.py).

# ---------------------------------------------------------------------------
# Fake caller module — IStarlakeJob.__init__ reads sys.modules[module_name]
# ---------------------------------------------------------------------------

_DAGSTER_TEST_MODULE_NAME = "tests.dagster._dagster_caller"


@pytest.fixture(autouse=True, scope="session")
def _register_dagster_stub_module():
    """Inject a fake caller module into sys.modules for the test session."""
    _mod = types.ModuleType(_DAGSTER_TEST_MODULE_NAME)
    _mod.__file__ = __file__
    sys.modules[_DAGSTER_TEST_MODULE_NAME] = _mod
    yield
    sys.modules.pop(_DAGSTER_TEST_MODULE_NAME, None)


# ---------------------------------------------------------------------------
# Core Dagster fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def dagster_job():
    """Create a StarlakeDagsterShellJob instance for Shell executor tests."""
    from ai.starlake.dagster.shell import StarlakeDagsterShellJob
    return StarlakeDagsterShellJob(
        filename="test_dagster.py",
        module_name=_DAGSTER_TEST_MODULE_NAME,
        options={},
    )


@pytest.fixture
def dagster_orchestration(dagster_job):
    """Create a DagsterOrchestration wrapping the dagster_job."""
    from ai.starlake.dagster import DagsterOrchestration
    return DagsterOrchestration(job=dagster_job)


# _clean_context_stack is provided by tests/conftest.py (root) as an
# autouse fixture — no need to duplicate it here.


# ---------------------------------------------------------------------------
# Runtime config — consumed by shared ``runtime_env`` / ``runtime_dags``
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def runtime_config():
    """Dagster-specific runtime configuration for shared runtime fixtures."""
    return {
        "load_dag_ref": "dagster_load_shell",
        "transform_dag_ref": "dagster_transform_shell",
        "dag_config_glob": "dagster_*.sl.yml",
    }
