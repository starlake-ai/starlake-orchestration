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

"""Snowflake triggering-strategy validation.

Snowflake is ALL-only BY ORCHESTRATOR DESIGN (maintainer-confirmed
2026-07-14, issue #79): the platform has no event-based triggering
mechanism, so ``dataset_triggering_strategy`` is never consulted.
Stream-backed upstreams become a SYSTEM$STREAM_HAS_DATA condition
(OR across most-frequent scheduled streams, AND across not-scheduled
streams); non-stream upstreams are checked at runtime by the start
task with hard-wired ALL semantics (any missing dataset skips the run).
"""

from __future__ import annotations

from typing import Optional

from tests.snowflake.snowflake_test_mixin import SnowflakeTestMixin
from tests.shared.base_test_triggering_strategy import BaseTestTriggeringStrategy
from tests.shared.triggering_scenarios import TWO_UPSTREAMS, make_dependencies


class TestSnowflakeTriggeringStrategy(SnowflakeTestMixin, BaseTestTriggeringStrategy):
    """Snowflake concrete implementation of the shared triggering tests."""


def _make_stream_dependencies(specs):
    """Build dependencies whose upstreams are stream-backed.

    specs: iterable of (name, cron, stream) tuples.
    """
    from ai.starlake.orchestration import (
        StarlakeDependencies,
        StarlakeDependency,
        StarlakeDependencyType,
    )

    return StarlakeDependencies([
        StarlakeDependency(
            name="overall_kpis",
            dependency_type=StarlakeDependencyType.TASK,
            dependencies=[
                StarlakeDependency(
                    name=name,
                    dependency_type=StarlakeDependencyType.TABLE,
                    cron=cron,
                    stream=stream,
                )
                for name, cron, stream in specs
            ],
        ),
    ])


class TestSnowflakeTriggeringStructure(SnowflakeTestMixin):
    """Structural validation of the SnowflakeDag trigger condition.

    Snowflake has no native ANY/ALL combination: stream-backed upstreams
    become a SYSTEM$STREAM_HAS_DATA condition (OR across most-frequent
    scheduled streams, AND across not-scheduled streams) and non-stream
    upstreams are checked at runtime by the start task with hard-wired
    ALL semantics (any missing dataset skips the run).

    NFR3 note: ``streams`` / ``not_scheduled_streams`` are Python sets,
    so the joined condition string order is NON-deterministic — the
    tests assert per-stream substrings + the connective, never the
    exact string.
    """

    def _make_pipeline(self, dependencies, strategy: Optional[str] = None):
        options = (
            {"dataset_triggering_strategy": strategy} if strategy else None
        )
        orchestration = self.create_orchestration(options=options)
        pipeline = orchestration.sl_create_pipeline(
            dependencies=dependencies
        )
        with pipeline:
            pass
        return pipeline

    def test_non_stream_upstreams_registered_as_datasets(self):
        """Every non-stream upstream sink is runtime-checked (ALL semantics).

        ``dag.datasets`` keys are DOTTED sinks (``domain.table``) —
        unlike dataset URIs, which are underscored.
        """
        pipeline = self._make_pipeline(make_dependencies(TWO_UPSTREAMS))
        dag = pipeline.dag
        assert set(dag.datasets.keys()) == {
            "starbake.orders",
            "starbake.customers",
        }
        assert dag.condition is None
        assert not dag.has_streams()

    def test_scheduled_streams_combined_with_or(self):
        """Most-frequent scheduled streams join with OR.

        Both upstreams share the SAME cron so both are 'most frequent'
        (a least-frequent stream dataset is audit-checked, not streamed).
        """
        pipeline = self._make_pipeline(_make_stream_dependencies([
            ("starbake.orders", "0 * * * *", "STARBAKE_ORDERS_STREAM"),
            ("starbake.customers", "0 * * * *", "STARBAKE_CUSTOMERS_STREAM"),
        ]))
        condition = pipeline.dag.condition
        assert condition is not None
        assert "SYSTEM$STREAM_HAS_DATA('STARBAKE_ORDERS_STREAM')" in condition
        assert (
            "SYSTEM$STREAM_HAS_DATA('STARBAKE_CUSTOMERS_STREAM')" in condition
        )
        assert " OR " in condition
        assert " AND " not in condition

    def test_not_scheduled_streams_combined_with_and(self):
        pipeline = self._make_pipeline(_make_stream_dependencies([
            ("starbake.orders", None, "STARBAKE_ORDERS_STREAM"),
            ("starbake.customers", None, "STARBAKE_CUSTOMERS_STREAM"),
        ]))
        condition = pipeline.dag.condition
        assert condition is not None
        assert "SYSTEM$STREAM_HAS_DATA('STARBAKE_ORDERS_STREAM')" in condition
        assert (
            "SYSTEM$STREAM_HAS_DATA('STARBAKE_CUSTOMERS_STREAM')" in condition
        )
        assert " AND " in condition
        assert " OR " not in condition

    def test_mixed_streams_or_group_anded_with_not_scheduled(self):
        """(scheduled ORs) AND (not-scheduled ANDs) — shape only."""
        pipeline = self._make_pipeline(_make_stream_dependencies([
            ("starbake.orders", "0 * * * *", "STARBAKE_ORDERS_STREAM"),
            ("starbake.stock", None, "STARBAKE_STOCK_STREAM"),
        ]))
        condition = pipeline.dag.condition
        assert condition is not None
        assert condition.startswith("(")
        assert ") AND (" in condition
        assert "SYSTEM$STREAM_HAS_DATA('STARBAKE_ORDERS_STREAM')" in condition
        assert "SYSTEM$STREAM_HAS_DATA('STARBAKE_STOCK_STREAM')" in condition

    def test_strategy_has_no_structural_effect(self):
        """PIN (issue #79 — ALL-only by orchestrator design, 2026-07-14):
        dataset_triggering_strategy is ignored by the Snowflake module —
        ANY and ALL produce identical DAG structure.

        The runtime dataset check is hard-wired ALL: Snowflake has no
        event-based triggering mechanism, and the run must ensure all
        depended-upon datasets were published within the window frame.
        ANY will NOT be implemented; if that ruling is ever revisited,
        this pin flips loudly.
        """
        p_any = self._make_pipeline(
            make_dependencies(TWO_UPSTREAMS), strategy="any"
        )
        p_all = self._make_pipeline(
            make_dependencies(TWO_UPSTREAMS), strategy="all"
        )
        assert p_any.dag.condition == p_all.dag.condition
        assert set(p_any.dag.datasets.keys()) == set(p_all.dag.datasets.keys())
        assert p_any.dag.has_streams() == p_all.dag.has_streams()
