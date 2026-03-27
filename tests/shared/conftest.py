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
import subprocess
from pathlib import Path
from types import MappingProxyType
from typing import Dict, Generator, Mapping, Tuple

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


@pytest.fixture(autouse=True, scope="session")
def sl_root_env(sample_project_path):
    """Set SL_ROOT in os.environ for the test session.

    All orchestrator tests need SL_ROOT pointing at the sample project
    so that ``IStarlakeJob.__init__()`` can resolve project paths.
    """
    original = os.environ.get("SL_ROOT")
    os.environ["SL_ROOT"] = str(sample_project_path)
    yield
    if original is not None:
        os.environ["SL_ROOT"] = original
    else:
        os.environ.pop("SL_ROOT", None)


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
    """Return JAVA_HOME resolved from system env, .env(.example), or built-in default."""
    value = _env_var("JAVA_HOME")
    if value and Path(value).is_dir():
        return value
    logger.warning(
        "JAVA_HOME is not set or points to a non-existent directory (%s). "
        "All tests requiring the Starlake CLI will be skipped. "
        "Set JAVA_HOME in system env or .env (see .env.example).",
        value or "<empty>",
    )
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


# ---------------------------------------------------------------------------
# Runtime test utilities — used by orchestrator runtime integration tests
# ---------------------------------------------------------------------------


def set_env(env: Dict[str, str]) -> Dict[str, str]:
    """Set os.environ from *env* dict, return a restore map.

    The restore map captures the previous value (or ``None`` if the key
    did not exist) so that :func:`restore_env` can undo the changes.
    """
    original = {}  # type: Dict[str, str]
    for k, v in env.items():
        original[k] = os.environ.get(k)
        os.environ[k] = v
    return original


def restore_env(original: Dict[str, str]) -> None:
    """Restore os.environ from a snapshot produced by :func:`set_env`."""
    for k, v in original.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def get_duckdb(project_path: Path) -> duckdb.DuckDBPyConnection:
    """Open a read-only DuckDB connection to *project_path*/datasets/duckdb.db."""
    return duckdb.connect(
        str(project_path / "datasets" / "duckdb.db"), read_only=True
    )


# ---------------------------------------------------------------------------
# Runtime fixtures — module-scoped, parameterised via ``runtime_config``
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def runtime_env(
    runtime_config,
    sample_project_path,
    starlake_cli,
    starlake_env,
    java_home,
    tmp_path_factory,
):
    """Module-scoped isolated project with incoming data and env vars.

    Requires a ``runtime_config`` fixture (provided by each orchestrator
    conftest) returning a dict with keys:

    - ``load_dag_ref``  — e.g. ``"airflow_load_shell"``
    - ``transform_dag_ref`` — e.g. ``"airflow_transform_shell"``
    - ``dag_config_glob`` — e.g. ``"airflow_*.sl.yml"``
    """
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
    env["LOAD_DAG_REF"] = runtime_config["load_dag_ref"]
    env["TRANSFORM_DAG_REF"] = runtime_config["transform_dag_ref"]
    env["JAVA_HOME"] = java_home
    starlake_dir = str(Path(starlake_cli).parent)
    full_path = starlake_dir + os.pathsep + env.get(
        "PATH", os.environ.get("PATH", "")
    )
    env["PATH"] = full_path

    # Inject PATH and JAVA_HOME into the ``sl_env_var`` option of each
    # DAG config so they are available inside the operator's env dict.
    dags_dir = isolated / "metadata" / "dags"
    for yml in dags_dir.glob(runtime_config["dag_config_glob"]):
        content = yml.read_text()
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
    """Generate DAG files once for the module via ``starlake dag-generate``."""
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
