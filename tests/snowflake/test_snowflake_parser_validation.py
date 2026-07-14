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

"""AC3 / NFR8 — Snowflake task validation of generated DAGs.

Full chain: ``starlake dag-generate`` (shared ``runtime_dags`` fixture)
→ generated ``.py`` files → ``load_pipelines()`` → built
``snowflake.core.task.dag.DAG`` object graph with ``StoredProcedureCall``
definitions. DAG construction happens in the pipeline's ``__exit__`` with
no live Session (the mocked-lifecycle boundary established in Epic 1) —
no new mocking required.

Fixtures (``generated_python_files``, ``runtime_mock_session``,
``runtime_env_vars``, ``loaded_pipelines``) live in
``tests/snowflake/conftest.py`` — shared with ``test_snowflake_runtime.py``.
"""

from __future__ import annotations

import pytest

from tests.shared.template_test_utils import assert_dag_generate_idempotent

# Mark all tests — they require Starlake CLI + Java runtime.
pytestmark = [
    pytest.mark.integration,
]


class TestSnowflakeTaskValidation:
    """AC3 / NFR8 — Snowflake task validation of generated DAGs."""

    def test_pipelines_build_snowflake_dags(self, loaded_pipelines):
        for p in loaded_pipelines:
            assert p.dag is not None
            assert p.dag.name == p.pipeline_id

    def test_dag_root_definition_is_stored_procedure_call(self, loaded_pipelines):
        from snowflake.core.task import StoredProcedureCall

        for p in loaded_pipelines:
            assert isinstance(p.dag.definition, StoredProcedureCall)

    def test_every_task_has_definition_and_unique_name(self, loaded_pipelines):
        for p in loaded_pipelines:
            names = [t.name for t in p.dag.tasks]
            assert len(names) == len(set(names)), (
                f"Duplicate task names in {p.pipeline_id}: {names}"
            )
            for task in p.dag.tasks:
                assert task.definition is not None, (
                    f"Task {task.name} in {p.pipeline_id} has no definition"
                )


class TestSnowflakeDagGenerateIdempotence:
    """AC2 / NFR2 — dag-generate output is byte-identical across runs.

    KNOWN VIOLATION (issue #70): the CLI's Snowflake statement compilation
    embeds generation-time values in every generated file — timestamped
    audit task names (``audit.audit-<table>-<epoch millis>``, load AND
    transform) and a random ``tempStage`` suffix (load). The byte-identity
    oracle is NOT weakened (no normalization): the test is pinned
    xfail(strict=True) so a CLI fix surfaces as XPASS and forces removal
    of the marker. Airflow/Dagster equivalents pass strictly.
    """

    @pytest.mark.xfail(
        raises=AssertionError,
        strict=True,
        reason="starlake-ai/starlake-orchestration#70 — snowflake dag-generate "
        "embeds timestamped audit names and random tempStage suffixes (NFR2)",
    )
    def test_dag_generate_is_idempotent(self, runtime_dags, tmp_path_factory):
        assert_dag_generate_idempotent(runtime_dags, tmp_path_factory)
