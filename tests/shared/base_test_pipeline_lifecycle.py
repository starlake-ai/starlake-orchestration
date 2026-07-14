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

from typing import Optional
from unittest.mock import patch

import pytest

from ai.starlake.job import StarlakeExecutionMode

from ai.starlake.orchestration import (
    AbstractPipeline,
    StarlakeSchedule,
)

from tests.shared.base_test_orchestration import BaseTestOrchestration


class BaseTestPipelineLifecycle(BaseTestOrchestration):
    """Abstract base for pipeline lifecycle shared tests.

    Validates ``run``, ``dry_run``, ``deploy``, ``delete``, and
    ``backfill`` operations on a pipeline created through the
    concrete orchestration with a minimal task graph.
    """

    def _make_pipeline(self, cron: Optional[str] = None) -> AbstractPipeline:
        """Return a pipeline with an optional cron expression."""
        schedule = StarlakeSchedule(name=None, cron=cron, domains=[])
        return self.create_test_pipeline(schedule=schedule)

    def _populate_pipeline(self, pipeline: AbstractPipeline) -> None:
        """Add a start >> dummy >> end task graph to the pipeline."""
        start = pipeline.start_task()
        middle = pipeline.dummy_task(task_id="fake_task")
        end = pipeline.end_task()
        start >> middle >> end

    # ------------------------------------------------------------------
    # 3.3  deploy — no-op, no exception
    # ------------------------------------------------------------------

    def test_pipeline_deploy(self):
        """Call pipeline.deploy(), verify no exception raised."""
        pipeline = self._make_pipeline()
        with pipeline:
            self._populate_pipeline(pipeline)
            result = pipeline.deploy()
            assert result is None

    # ------------------------------------------------------------------
    # 3.4  run — concrete orchestrators implement the abstract run()
    # ------------------------------------------------------------------

    def test_pipeline_run(self):
        """Call pipeline.run(), verify no exception raised."""
        pipeline = self._make_pipeline()
        with pipeline:
            self._populate_pipeline(pipeline)
            pipeline.run()

    # ------------------------------------------------------------------
    # 3.5  dry_run — delegates to run(mode=DRY_RUN)
    # ------------------------------------------------------------------

    def test_pipeline_dry_run(self):
        """Call pipeline.dry_run(), verify it delegates to run(mode=DRY_RUN)."""
        pipeline = self._make_pipeline()
        with pipeline:
            self._populate_pipeline(pipeline)
            with patch.object(
                pipeline, "run", wraps=pipeline.run
            ) as spy_run:
                pipeline.dry_run()
                spy_run.assert_called_once()
                call_kwargs = spy_run.call_args
                # dry_run passes mode as keyword arg
                mode = call_kwargs.kwargs.get("mode")
                assert mode == StarlakeExecutionMode.DRY_RUN

    # ------------------------------------------------------------------
    # 3.6  delete — no-op, no exception
    # ------------------------------------------------------------------

    def test_pipeline_delete(self):
        """Call pipeline.delete(), verify no exception raised."""
        pipeline = self._make_pipeline()
        with pipeline:
            self._populate_pipeline(pipeline)
            result = pipeline.delete()
            assert result is None

    # ------------------------------------------------------------------
    # 3.7  backfill — requires cron
    # ------------------------------------------------------------------

    def test_pipeline_backfill_requires_cron(self):
        """Backfill on a pipeline without cron raises ValueError."""
        pipeline = self._make_pipeline(cron=None)
        with pipeline:
            self._populate_pipeline(pipeline)
            with pytest.raises(
                ValueError,
                match="The pipeline must have a cron expression to backfill",
            ):
                pipeline.backfill()

    # ------------------------------------------------------------------
    # 3.8  backfill — requires start_date
    # ------------------------------------------------------------------

    def test_pipeline_backfill_requires_start_date(self):
        """Backfill with cron but no start_date raises ValueError."""
        pipeline = self._make_pipeline(cron="0 * * * *")
        with pipeline:
            self._populate_pipeline(pipeline)
            with pytest.raises(
                ValueError,
                match="The pipeline must have a start date to backfill",
            ):
                pipeline.backfill()

    # ------------------------------------------------------------------
    # 3.9  backfill — invalid date range
    # ------------------------------------------------------------------

    def test_pipeline_backfill_invalid_date_range(self):
        """Backfill with start_date > end_date raises ValueError."""
        pipeline = self._make_pipeline(cron="0 * * * *")
        with pipeline:
            self._populate_pipeline(pipeline)
            with pytest.raises(
                ValueError,
                match="The start date must be before the end date",
            ):
                pipeline.backfill(
                    start_date="2026-06-01T00:00:00+00:00",
                    end_date="2026-01-01T00:00:00+00:00",
                )

    # ------------------------------------------------------------------
    # 3.10  pipeline properties
    # ------------------------------------------------------------------

    def test_pipeline_properties(self):
        """Verify core pipeline properties are accessible after population."""
        pipeline = self._make_pipeline(cron="0 * * * *")
        with pipeline:
            self._populate_pipeline(pipeline)
        # After exiting the context, __exit__ walks the tree and
        # populates tasks / tasks_names
        assert pipeline.pipeline_id is not None
        assert isinstance(pipeline.tasks, list)
        assert len(pipeline.tasks) > 0
        assert isinstance(pipeline.tasks_names, list)
        assert len(pipeline.tasks_names) > 0
        assert pipeline.computed_cron_expr == "0 * * * *"
