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

from ai.starlake.dataset import (
    DatasetTriggeringStrategy,
)

from ai.starlake.orchestration import (
    AbstractPipeline,
    StarlakeDependencies,
    StarlakeDependency,
    StarlakeDependencyType,
)

from tests.shared.base_test_orchestration import BaseTestOrchestration


class BaseTestEventCombinations(BaseTestOrchestration):
    """Abstract base for event combination shared tests.

    Validates ``DatasetTriggeringStrategy`` enum semantics, that the
    strategy option is correctly propagated to the job, and that
    ``pipeline.events`` produces native events from datasets.
    """

    def _make_pipeline_with_datasets(
        self,
        strategy: Optional[str] = None,
    ) -> AbstractPipeline:
        """Return a pipeline whose ``events`` property is non-empty.

        Uses dependencies with sub-dependencies carrying cron
        expressions so that ``retrieve_datasets`` produces datasets.
        The *strategy* option is forwarded to the job via
        ``create_orchestration(options=...)``.
        """
        options = {}
        if strategy is not None:
            options["dataset_triggering_strategy"] = strategy
        dependencies = StarlakeDependencies([
            StarlakeDependency(
                name="overall_kpis",
                dependency_type=StarlakeDependencyType.TASK,
                dependencies=[
                    StarlakeDependency(
                        name="starbake.orders",
                        dependency_type=StarlakeDependencyType.TABLE,
                        cron="0 * * * *",
                    ),
                    StarlakeDependency(
                        name="starbake.customers",
                        dependency_type=StarlakeDependencyType.TABLE,
                        cron="0 * * * *",
                    ),
                ],
            ),
        ])
        return self.create_test_pipeline(
            dependencies=dependencies,
            options=options if options else None,
        )

    # ------------------------------------------------------------------
    # 2.4  DatasetTriggeringStrategy.ANY value
    # ------------------------------------------------------------------

    def test_dataset_triggering_strategy_any(self):
        """Verify DatasetTriggeringStrategy.ANY is valid and equals 'any'."""
        assert DatasetTriggeringStrategy.ANY.value == "any"
        assert str(DatasetTriggeringStrategy.ANY) == "any"

    # ------------------------------------------------------------------
    # 2.5  DatasetTriggeringStrategy.ALL value
    # ------------------------------------------------------------------

    def test_dataset_triggering_strategy_all(self):
        """Verify DatasetTriggeringStrategy.ALL is valid and equals 'all'."""
        assert DatasetTriggeringStrategy.ALL.value == "all"
        assert str(DatasetTriggeringStrategy.ALL) == "all"

    # ------------------------------------------------------------------
    # 2.6  DatasetTriggeringStrategy validation
    # ------------------------------------------------------------------

    def test_dataset_triggering_strategy_validation(self):
        """Verify is_valid() accepts valid strategies and rejects invalid."""
        assert DatasetTriggeringStrategy.is_valid("any") is True
        assert DatasetTriggeringStrategy.is_valid("all") is True
        assert DatasetTriggeringStrategy.is_valid("invalid") is False
        assert DatasetTriggeringStrategy.is_valid("") is False

    # ------------------------------------------------------------------
    # 2.7  create events from pipeline datasets
    # ------------------------------------------------------------------

    def test_create_events_from_datasets(self):
        """Create pipeline with datasets, verify events are non-empty."""
        pipeline = self._make_pipeline_with_datasets()
        with pipeline:
            events = pipeline.events
            assert events is not None
            assert len(events) > 0

    # ------------------------------------------------------------------
    # 2.8  ANY strategy propagated to job
    # ------------------------------------------------------------------

    def test_strategy_any_propagated_to_job(self):
        """Verify dataset_triggering_strategy=any is reflected on the job."""
        pipeline = self._make_pipeline_with_datasets(strategy="any")
        with pipeline:
            assert pipeline.job.dataset_triggering_strategy == DatasetTriggeringStrategy.ANY

    # ------------------------------------------------------------------
    # 2.9  ALL strategy propagated to job
    # ------------------------------------------------------------------

    def test_strategy_all_propagated_to_job(self):
        """Verify dataset_triggering_strategy=all is reflected on the job."""
        pipeline = self._make_pipeline_with_datasets(strategy="all")
        with pipeline:
            assert pipeline.job.dataset_triggering_strategy == DatasetTriggeringStrategy.ALL
