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

import ast
import os

import pytest

from ai.starlake.orchestration import AbstractPipeline
from tests.shared.conftest import set_env, restore_env


# Mark all tests — they require Starlake CLI + Java runtime.
pytestmark = [
    pytest.mark.integration,
]


@pytest.fixture(scope="module")
def generated_python_files(runtime_dags):
    """Return all generated ``*.py`` DAG files from ``runtime_dags``."""
    dags_dir, _, _ = runtime_dags
    files = sorted(dags_dir.glob("*.py"))
    assert len(files) > 0, f"No .py files in {dags_dir}"
    return files


@pytest.fixture(scope="module")
def runtime_env_vars(runtime_dags):
    """Set runtime env vars for the module and restore on teardown."""
    _, _, env = runtime_dags
    original = set_env(env)
    yield env
    restore_env(original)


# ------------------------------------------------------------------
# 3.2  dag-generate produces valid Python files
# ------------------------------------------------------------------

class TestSnowflakeIntegration:

    def test_dag_generate_produces_valid_python(self, generated_python_files):
        """Verify that dag-generate produced Python files that parse cleanly."""
        for py_file in generated_python_files:
            source = py_file.read_text(encoding="utf-8")
            try:
                ast.parse(source, filename=str(py_file))
            except SyntaxError as exc:
                pytest.fail(
                    f"Generated file {py_file.name} has syntax error: {exc}"
                )

    # ------------------------------------------------------------------
    # 3.3  load_pipelines returns Snowflake pipelines
    # ------------------------------------------------------------------

    def test_load_pipelines_returns_snowflake_pipelines(
        self, generated_python_files, runtime_env_vars
    ):
        """Import generated DAGs via load_pipelines() and validate types."""
        from ai.starlake.orchestration.__main__ import load_pipelines

        for py_file in generated_python_files:
            pipelines = load_pipelines(str(py_file))
            assert pipelines is not None, (
                f"No pipelines returned from {py_file.name}"
            )
            assert isinstance(pipelines, list)
            for p in pipelines:
                assert isinstance(p, AbstractPipeline), (
                    f"Pipeline from {py_file.name} is {type(p)}, "
                    f"expected AbstractPipeline"
                )

    # ------------------------------------------------------------------
    # 3.4  pipeline dry_run with mocked session
    # ------------------------------------------------------------------

    def test_pipeline_dry_run(self, generated_python_files, runtime_env_vars):
        """Load pipelines and call dry_run() on each with mocked session."""
        from unittest.mock import MagicMock, patch

        from ai.starlake.orchestration.__main__ import load_pipelines
        from ai.starlake.snowflake.starlake_snowflake_orchestration import (
            SnowflakePipeline,
        )

        mock_session = MagicMock()
        mock_session.sql.return_value.collect.return_value = []

        for py_file in generated_python_files:
            pipelines = load_pipelines(str(py_file))
            if not pipelines:
                continue
            for p in pipelines:
                with patch.object(
                    SnowflakePipeline, "session", return_value=mock_session
                ):
                    p.dry_run()

    # ------------------------------------------------------------------
    # 3.5  pipeline properties populated
    # ------------------------------------------------------------------

    def test_pipeline_properties(self, generated_python_files, runtime_env_vars):
        """Verify loaded pipelines have populated properties."""
        from ai.starlake.orchestration.__main__ import load_pipelines

        for py_file in generated_python_files:
            pipelines = load_pipelines(str(py_file))
            if not pipelines:
                continue
            for p in pipelines:
                assert p.pipeline_id is not None
                assert isinstance(p.tasks, list)
                assert len(p.tasks) > 0
                assert isinstance(p.tasks_names, list)
                assert len(p.tasks_names) > 0
