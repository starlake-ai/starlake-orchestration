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

from typing import Optional, Tuple

from ai.starlake.dataset import DatasetTriggeringStrategy
from ai.starlake.orchestration import AbstractPipeline

from tests.shared.base_test_orchestration import BaseTestOrchestration
from tests.shared.triggering_scenarios import (
    EXPECTED_URIS,
    MIXED_UPSTREAMS,
    NO_UPSTREAM,
    SINGLE_UPSTREAM,
    TWO_UPSTREAMS,
    make_dependencies,
)


class BaseTestTriggeringStrategy(BaseTestOrchestration):
    """Shared, orchestrator-agnostic triggering-strategy tests.

    Only uses framework surface that already exists on every
    orchestrator (pipeline.events / pipeline.datasets /
    job.dataset_triggering_strategy) — the native ``|``/``&``
    combination semantics are validated in each orchestrator's
    test_{orch}_triggering_strategy.py module.

    Adds ZERO abstract members: everything needed is already exposed
    by ``BaseTestOrchestration.create_test_pipeline`` and the pipeline
    / job properties.
    """

    def _make_pipeline(
        self,
        upstreams: Tuple[Tuple[str, Optional[str]], ...],
        strategy: Optional[str] = None,
    ) -> AbstractPipeline:
        options = (
            {"dataset_triggering_strategy": strategy} if strategy else None
        )
        return self.create_test_pipeline(
            dependencies=make_dependencies(upstreams),
            options=options,
        )

    def test_default_strategy_is_any(self):
        """No option provided -> job strategy defaults to ANY."""
        pipeline = self._make_pipeline(TWO_UPSTREAMS)
        with pipeline:
            pass
        assert (
            pipeline.job.dataset_triggering_strategy
            == DatasetTriggeringStrategy.ANY
        )

    def test_invalid_strategy_falls_back_to_any(self):
        """PIN: an invalid strategy string silently falls back to ANY.

        Current behavior (ai/starlake/job/starlake_job.py — the
        ``dataset_triggering_strategy`` option resolution): an invalid
        string is silently replaced by the default (ANY), no error.
        Whether it should raise instead is a follow-up product decision
        (the sibling pre_load_strategy option IS strict); if strict
        validation is ever extended to this option, this pin flipping
        to ValueError is the intended signal — not a flake.
        """
        pipeline = self._make_pipeline(TWO_UPSTREAMS, strategy="bogus")
        with pipeline:
            pass
        assert (
            pipeline.job.dataset_triggering_strategy
            == DatasetTriggeringStrategy.ANY
        )

    def test_two_upstreams_produce_two_events(self):
        pipeline = self._make_pipeline(TWO_UPSTREAMS)
        with pipeline:
            pass
        events = pipeline.events
        # events is annotated Optional but is always a list — the guard
        # is executable documentation of that misleading annotation.
        assert events is not None
        assert len(events) == len(TWO_UPSTREAMS)

    def test_single_upstream_produces_single_event(self):
        pipeline = self._make_pipeline(SINGLE_UPSTREAM)
        with pipeline:
            pass
        assert len(pipeline.events) == 1

    def test_no_upstream_produces_no_events(self):
        """events is an EMPTY LIST (not None) when there are no upstreams."""
        pipeline = self._make_pipeline(NO_UPSTREAM)
        with pipeline:
            pass
        assert pipeline.events == []
        assert pipeline.cron is None

    def test_mixed_upstreams_partition_scheduled_and_not_scheduled(self):
        """cron-less upstreams land in not_scheduled_datasets."""
        pipeline = self._make_pipeline(MIXED_UPSTREAMS)
        with pipeline:
            pass
        assert len(pipeline.datasets or []) == len(MIXED_UPSTREAMS)
        not_scheduled = pipeline.not_scheduled_datasets
        assert len(not_scheduled) == 1
        assert not_scheduled[0].uri == "starbake_stock"
        assert len(pipeline.scheduled_datasets) == 1

    def test_strategy_does_not_change_datasets(self):
        """ANY vs ALL only affects combination — never dataset discovery."""
        uris_any = None
        uris_all = None
        for strategy in ("any", "all"):
            pipeline = self._make_pipeline(TWO_UPSTREAMS, strategy=strategy)
            with pipeline:
                pass
            uris = frozenset(d.uri for d in pipeline.datasets or [])
            if strategy == "any":
                uris_any = uris
            else:
                uris_all = uris
        assert uris_any == uris_all == EXPECTED_URIS[TWO_UPSTREAMS]
