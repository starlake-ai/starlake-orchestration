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
from unittest.mock import MagicMock, patch

from ai.starlake.job import StarlakeExecutionMode

from tests.snowflake.snowflake_test_mixin import SnowflakeTestMixin
from tests.shared.base_test_pipeline_lifecycle import BaseTestPipelineLifecycle


class TestSnowflakePipelineLifecycle(SnowflakeTestMixin, BaseTestPipelineLifecycle):
    """Snowflake pipeline lifecycle tests.

    Unlike Airflow/Dagster where ``deploy()`` and ``delete()`` are
    no-ops, SnowflakePipeline overrides all lifecycle methods with
    real implementations that require a live Snowflake Session.
    All lifecycle tests must mock the session and DAGOperation.

    **Deviation from base class**: The overridden tests
    (deploy/delete/run/dry_run) call lifecycle methods **after**
    exiting the ``with pipeline:`` block, not inside it.  This is
    intentional — ``SnowflakePipeline.__exit__()`` finalises the
    ``SnowflakeDag``, which must be fully built before deploy/run
    can operate on it.  ``super()`` is not called because the base
    implementations invoke these methods inside the context manager.
    If the base class adds new assertions, review these overrides.

    The inherited backfill and properties tests run inside the
    context manager without session mocking, which is safe because
    backfill validation raises ``ValueError`` before reaching any
    session code, and ``test_pipeline_properties`` only reads
    attributes populated by ``__exit__()``.
    """

    def _mock_session_and_op(self, pipeline):
        """Return nested context managers that mock session + DAGOperation."""
        mock_session = MagicMock()
        mock_session.sql.return_value.collect.return_value = []
        mock_session.custom_package_usage_config = {}

        mock_op = MagicMock()
        mock_op.deploy.return_value = None
        mock_op.delete.return_value = None

        session_patch = patch.object(
            pipeline.__class__, "session", return_value=mock_session
        )
        op_patch = patch.object(
            pipeline, "get_dag_operation", return_value=mock_op
        )
        env_patch = patch.dict(os.environ, {
            "SNOWFLAKE_DB": "test_db",
            "SNOWFLAKE_SCHEMA": "public",
            "SNOWFLAKE_ACCOUNT": "test_account",
            "SNOWFLAKE_USER": "test_user",
            "SNOWFLAKE_PASSWORD": "test_pass",
        })
        return session_patch, op_patch, env_patch, mock_op

    def test_pipeline_deploy(self):
        """deploy() delegates to DAGOperation.deploy(dag, mode=or_replace)."""
        from snowflake.core._common import CreateMode
        pipeline = self._make_pipeline()
        with pipeline:
            self._populate_pipeline(pipeline)
        session_p, op_p, env_p, mock_op = self._mock_session_and_op(pipeline)
        with session_p, op_p, env_p:
            result = pipeline.deploy()
            assert result is None
            # The whole point of deploy() is to push the built DAG to Snowflake.
            mock_op.deploy.assert_called_once_with(
                pipeline.dag, mode=CreateMode.or_replace
            )

    def test_pipeline_delete(self):
        """delete() delegates to DAGOperation.delete(pipeline_id)."""
        pipeline = self._make_pipeline()
        with pipeline:
            self._populate_pipeline(pipeline)
        session_p, op_p, env_p, mock_op = self._mock_session_and_op(pipeline)
        with session_p, op_p, env_p:
            result = pipeline.delete()
            assert result is None
            mock_op.delete.assert_called_once_with(pipeline.pipeline_id)

    def test_pipeline_run(self):
        """Call pipeline.run() in DRY_RUN mode with mocked session."""
        pipeline = self._make_pipeline()
        with pipeline:
            self._populate_pipeline(pipeline)
        session_p, op_p, env_p, _ = self._mock_session_and_op(pipeline)
        with session_p, op_p, env_p:
            pipeline.run(mode=StarlakeExecutionMode.DRY_RUN)

    def test_pipeline_dry_run(self):
        """Call pipeline.dry_run() with mocked session, verify delegation."""
        pipeline = self._make_pipeline()
        with pipeline:
            self._populate_pipeline(pipeline)
        session_p, op_p, env_p, _ = self._mock_session_and_op(pipeline)
        with session_p, op_p, env_p:
            with patch.object(pipeline, "run", wraps=pipeline.run) as spy_run:
                pipeline.dry_run()
                spy_run.assert_called_once()
                call_kwargs = spy_run.call_args
                mode = call_kwargs.kwargs.get("mode")
                assert mode == StarlakeExecutionMode.DRY_RUN
