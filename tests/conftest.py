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

# Re-export shared fixtures so that orchestrator test directories
# (tests/airflow/, tests/dagster/, tests/snowflake/) can discover them.
# pytest only auto-discovers fixtures from conftest.py files in parent
# directories, not from sibling directories like tests/shared/.
#
# Using explicit imports rather than pytest_plugins to avoid
# double-registration when running ``pytest tests/shared/`` (pytest
# would auto-discover tests/shared/conftest.py AND load it again
# via the pytest_plugins directive).

import pytest

from tests.shared.conftest import (  # noqa: F401
    sl_root_env,
    sample_project_path,
    starlake_cli,
    java_home,
    starlake_env,
    duckdb_connection,
    isolated_project,
    runtime_env,
    runtime_dags,
)


# ---------------------------------------------------------------------------
# TaskGroupContext cleanup — prevent context bleed between tests
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clean_context_stack():
    """Ensure TaskGroupContext stack is clean before/after each test."""
    from ai.starlake.orchestration import TaskGroupContext
    TaskGroupContext._context_stack.clear()
    yield
    TaskGroupContext._context_stack.clear()
