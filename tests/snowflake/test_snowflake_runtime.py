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

from unittest.mock import patch

import pytest


# Mark all tests — they require Starlake CLI + Java runtime.
pytestmark = [
    pytest.mark.integration,
]

# The module-scoped runtime fixtures (``generated_python_files``,
# ``runtime_mock_session``, ``runtime_env_vars``, ``loaded_pipelines``)
# moved to tests/snowflake/conftest.py (story 3.2) — they are shared with
# test_snowflake_parser_validation.py.


# ------------------------------------------------------------------
# 5.1  Mocked runtime — StoredProcedureCall validation
# ------------------------------------------------------------------

class TestSnowflakeRuntimeLoad:
    """Validate generated load pipelines produce correct task structures."""

    def test_load_pipelines_have_tasks(self, loaded_pipelines):
        """Each pipeline has at least one task."""
        for p in loaded_pipelines:
            assert len(p.tasks) > 0, (
                f"Pipeline {p.pipeline_id} has no tasks"
            )

    def test_load_pipelines_task_names(self, loaded_pipelines):
        """Each pipeline has non-empty task names."""
        for p in loaded_pipelines:
            assert len(p.tasks_names) > 0, (
                f"Pipeline {p.pipeline_id} has no task names"
            )

    def test_pipeline_id_is_set(self, loaded_pipelines):
        """Each pipeline has a pipeline_id."""
        for p in loaded_pipelines:
            assert p.pipeline_id is not None
            assert len(p.pipeline_id) > 0


# ------------------------------------------------------------------
# 5.2  Mocked runtime — DAG structure validation
# ------------------------------------------------------------------

class TestSnowflakeRuntimeDagStructure:
    """Validate DAG structure of generated Snowflake pipelines."""

    def test_dag_has_name(self, loaded_pipelines):
        """Each pipeline's DAG has a name matching pipeline_id."""
        for p in loaded_pipelines:
            assert p.dag is not None
            assert p.dag.name == p.pipeline_id

    def test_dag_has_definition(self, loaded_pipelines):
        """Each pipeline's DAG has a root definition (StoredProcedureCall)."""
        from snowflake.core.task import StoredProcedureCall

        for p in loaded_pipelines:
            dag = p.dag
            assert dag.definition is not None
            assert isinstance(dag.definition, StoredProcedureCall)

    def test_dag_tasks_have_definitions(self, loaded_pipelines):
        """Each task in the DAG has a definition."""
        for p in loaded_pipelines:
            for task in p.dag.tasks:
                assert task.definition is not None, (
                    f"Task {task.name} in pipeline {p.pipeline_id} "
                    f"has no definition"
                )


# ------------------------------------------------------------------
# 5.3  Mocked runtime — dry_run with mocked session
# ------------------------------------------------------------------

class TestSnowflakeRuntimeDryRun:
    """Validate dry_run execution with mocked Snowflake session."""

    def test_dry_run_completes(self, loaded_pipelines, runtime_mock_session):
        """dry_run() completes without error on mocked session."""
        from ai.starlake.snowflake.starlake_snowflake_orchestration import (
            SnowflakePipeline,
        )

        for p in loaded_pipelines:
            with patch.object(
                SnowflakePipeline, "session", return_value=runtime_mock_session
            ):
                p.dry_run()
