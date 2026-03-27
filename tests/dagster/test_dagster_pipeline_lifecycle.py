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

from ai.starlake.job import StarlakeExecutionMode

from tests.dagster.dagster_test_mixin import DagsterTestMixin
from tests.shared.base_test_pipeline_lifecycle import BaseTestPipelineLifecycle


class TestDagsterPipelineLifecycle(DagsterTestMixin, BaseTestPipelineLifecycle):
    """Dagster pipeline lifecycle tests.

    Dagster builds the ``JobDefinition`` (``self.dag``) and
    ``GraphDefinition`` (``self.graph``) inside ``__exit__()``, so
    ``run()`` / ``dry_run()`` can only be called **after** the context
    manager exits — unlike Airflow which creates the DAG in
    ``__init__()``.  The two affected tests are overridden here to
    call ``run()`` after the ``with`` block.
    """

    def test_pipeline_run(self):
        """Call pipeline.run() after context exit, verify no exception."""
        pipeline = self._make_pipeline()
        with pipeline:
            self._populate_pipeline(pipeline)
        pipeline.run()

    def test_pipeline_dry_run(self):
        """Call pipeline.dry_run() after context exit, verify delegation."""
        pipeline = self._make_pipeline()
        with pipeline:
            self._populate_pipeline(pipeline)
        with patch.object(pipeline, "run", wraps=pipeline.run) as spy_run:
            pipeline.dry_run()
            spy_run.assert_called_once()
            call_kwargs = spy_run.call_args
            mode = call_kwargs.kwargs.get("mode")
            assert mode == StarlakeExecutionMode.DRY_RUN
