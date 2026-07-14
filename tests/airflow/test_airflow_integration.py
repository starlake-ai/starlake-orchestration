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


# Mark all tests in this module — they require both the Starlake CLI
# and a working Java runtime, which may not be available in CI.
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


# ------------------------------------------------------------------
# 3.2  dag-generate produces valid Python files
# ------------------------------------------------------------------

class TestAirflowIntegration:

    def test_dag_generate_produces_valid_python(self, generated_python_files):
        """Verify that dag-generate produced Python files that parse cleanly."""
        for py_file in generated_python_files:
            source = py_file.read_text(encoding="utf-8")
            try:
                ast.parse(source, filename=str(py_file))
            except SyntaxError as exc:
                pytest.fail(f"Generated file {py_file.name} has syntax error: {exc}")

    # ------------------------------------------------------------------
    # 3.3  load_pipelines returns Airflow pipelines
    # ------------------------------------------------------------------

    def test_load_pipelines_returns_airflow_pipelines(
        self, generated_python_files, runtime_dags
    ):
        """Import generated DAGs via load_pipelines() and validate types."""
        _, _, env = runtime_dags
        for key, val in env.items():
            os.environ.setdefault(key, val)

        from ai.starlake.orchestration.__main__ import load_pipelines

        for py_file in generated_python_files:
            pipelines = load_pipelines(str(py_file))
            assert pipelines is not None, f"No pipelines returned from {py_file.name}"
            assert isinstance(pipelines, list)
            for p in pipelines:
                assert isinstance(p, AbstractPipeline), (
                    f"Pipeline from {py_file.name} is {type(p)}, "
                    f"expected AbstractPipeline"
                )

    # ------------------------------------------------------------------
    # 3.4  pipeline dry_run
    # ------------------------------------------------------------------

    def test_pipeline_dry_run(self, generated_python_files, runtime_dags):
        """Load pipelines and call dry_run() on each."""
        _, _, env = runtime_dags
        for key, val in env.items():
            os.environ.setdefault(key, val)

        from ai.starlake.orchestration.__main__ import load_pipelines

        for py_file in generated_python_files:
            pipelines = load_pipelines(str(py_file))
            if not pipelines:
                continue
            for p in pipelines:
                p.dry_run()

    # ------------------------------------------------------------------
    # 3.5  pipeline properties populated
    # ------------------------------------------------------------------

    def test_pipeline_properties(self, generated_python_files, runtime_dags):
        """Verify loaded pipelines have populated properties."""
        _, _, env = runtime_dags
        for key, val in env.items():
            os.environ.setdefault(key, val)

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
