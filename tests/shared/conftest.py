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

import logging
import os
import shutil
from pathlib import Path
from types import MappingProxyType
from typing import Generator, Mapping, Tuple

import duckdb
import pytest
from dotenv import dotenv_values

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# .env loading: values from .env serve as defaults; system environment
# variables take precedence.
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DOT_ENV_PATH = _PROJECT_ROOT / ".env"
_DOT_ENV = dotenv_values(_DOT_ENV_PATH) if _DOT_ENV_PATH.is_file() else dotenv_values(_PROJECT_ROOT / ".env.example")

# Defaults used when neither .env nor system env defines a variable.
_DEFAULTS = {
    "SL_VERSION": "1.5.7",
    "SL_ENV": "DUCKDB",
}


_REQUIRED_VARS = {"SL_ENV", "SL_VERSION"}


def _env_var(name: str) -> str:
    """Resolve a variable: system env > .env(.example) > built-in default."""
    value = os.environ.get(name, _DOT_ENV.get(name, _DEFAULTS.get(name, "")))
    if not value and name in _REQUIRED_VARS:
        raise EnvironmentError(
            f"Required variable {name} is empty. "
            f"Set it in system env, .env, or .env.example"
        )
    return value


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def sample_project_path() -> Path:
    """Path to the shared sample Starlake project used by all tests."""
    path = Path(__file__).resolve().parent.parent / "sample-project"
    assert path.is_dir(), f"Sample project not found at {path}"
    return path


@pytest.fixture(scope="session")
def starlake_cli() -> str:
    """Verify Starlake CLI is available and return its path.

    Checks for ``starlake`` on PATH or at ``~/starlake/starlake``.
    Skips the test session if the CLI cannot be found.
    """
    cli_path = shutil.which("starlake")
    if cli_path is None:
        home_cli = Path.home() / "starlake" / "starlake"
        if home_cli.is_file():
            cli_path = str(home_cli)
    if cli_path is None:
        pytest.skip("Starlake CLI not found on PATH or at ~/starlake/starlake")
    return cli_path


@pytest.fixture(scope="session")
def java_home() -> str:
    """Return JAVA_HOME resolved from system env, .env, or .env.example."""
    value = os.environ.get("JAVA_HOME", _DOT_ENV.get("JAVA_HOME", ""))
    if value and Path(value).is_dir():
        return value
    pytest.skip(
        "JAVA_HOME not set or points to a non-existent directory — "
        "set it in system env or .env (see .env.example)"
    )


@pytest.fixture(scope="session")
def starlake_env(sample_project_path: Path, java_home: str) -> Mapping[str, str]:
    """Shared environment variables for running Starlake CLI commands.

    Returns an immutable mapping.  Orchestrator-specific variables
    (e.g. LOAD_DAG_REF, TRANSFORM_DAG_REF) should be added by copying
    via ``dict(starlake_env)`` in each orchestrator test module.
    """
    env = os.environ.copy()
    env["SL_ROOT"] = str(sample_project_path)
    env["SL_ENV"] = _env_var("SL_ENV")
    env["SL_VERSION"] = _env_var("SL_VERSION")
    env["JAVA_HOME"] = java_home
    return MappingProxyType(env)


@pytest.fixture(scope="session")
def duckdb_connection(
    sample_project_path: Path,
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Session-scoped read-only DuckDB connection for result validation.

    Connects to the database that Starlake CLI populates at
    ``{SL_ROOT}/datasets/duckdb.db``.  Tests use this to assert that
    loads and transforms produced the expected results.

    Since Starlake is the only writer and tests only read, there are
    no write-lock conflicts with a single shared connection.
    """
    db_path = sample_project_path / "datasets" / "duckdb.db"
    if not db_path.is_file():
        pytest.skip(
            f"DuckDB not yet populated at {db_path} — run starlake load first"
        )
    conn = duckdb.connect(str(db_path), read_only=True)
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def isolated_project(
    sample_project_path: Path, tmp_path: Path, starlake_env: Mapping[str, str]
) -> Tuple[Path, dict]:
    """Copy the sample project to a temp directory for write isolation.

    Returns a tuple of ``(project_path, env)`` where:
    - ``project_path`` is the isolated copy in ``tmp_path``
    - ``env`` is a Starlake environment dict with SL_ROOT pointing to
      the copy, ready for Starlake CLI commands that write to DuckDB.

    Each test gets its own copy so tests can run in parallel without
    DuckDB write-lock conflicts.
    """
    isolated_path = tmp_path / "sample-project"
    shutil.copytree(sample_project_path, isolated_path)
    env = dict(starlake_env)
    env["SL_ROOT"] = str(isolated_path)
    return isolated_path, env
