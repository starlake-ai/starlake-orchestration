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

from ai.starlake.orchestration import (
    AbstractPipeline,
    StarlakeSchedule,
)

from tests.shared.base_test_orchestration import BaseTestOrchestration


class BaseTestDependencyChaining(BaseTestOrchestration):
    """Abstract base for dependency chaining (>> and <<) shared tests.

    Validates that operator composition correctly populates
    upstream_dependencies and downstream_dependencies when tasks
    are created through the full orchestration pipeline.
    """

    def _make_pipeline(self) -> AbstractPipeline:
        """Return a pipeline with a minimal empty schedule."""
        schedule = StarlakeSchedule(name=None, cron=None, domains=[])
        return self.create_test_pipeline(schedule=schedule)

    # ------------------------------------------------------------------
    # 1.2  rshift two tasks: A >> B
    # ------------------------------------------------------------------

    def test_rshift_two_tasks(self):
        """Chain two tasks with >>, verify upstream_dependencies."""
        pipeline = self._make_pipeline()
        with pipeline:
            t_a = pipeline.sl_load(
                task_id="load_starbake_orders",
                domain="starbake",
                table="orders",
            )
            t_b = pipeline.sl_load(
                task_id="load_starbake_customers",
                domain="starbake",
                table="customers",
            )
            t_a >> t_b

            # upstream_dependencies[A] contains B (B is downstream of A)
            assert "load_starbake_customers" in pipeline.upstream_dependencies.get(
                "load_starbake_orders", []
            )
            # downstream_dependencies[B] contains A (A is upstream of B)
            assert "load_starbake_orders" in pipeline.downstream_dependencies.get(
                "load_starbake_customers", []
            )

    # ------------------------------------------------------------------
    # 1.3  lshift two tasks: B << A
    # ------------------------------------------------------------------

    def test_lshift_two_tasks(self):
        """Chain two tasks with <<, verify reverse direction."""
        pipeline = self._make_pipeline()
        with pipeline:
            t_a = pipeline.sl_load(
                task_id="load_starbake_orders",
                domain="starbake",
                table="orders",
            )
            t_b = pipeline.sl_load(
                task_id="load_starbake_customers",
                domain="starbake",
                table="customers",
            )
            t_b << t_a  # A is upstream of B

            assert "load_starbake_customers" in pipeline.upstream_dependencies.get(
                "load_starbake_orders", []
            )
            assert "load_starbake_orders" in pipeline.downstream_dependencies.get(
                "load_starbake_customers", []
            )

    # ------------------------------------------------------------------
    # 1.4  rshift chain three tasks: A >> B >> C
    # ------------------------------------------------------------------

    def test_rshift_chain_three_tasks(self):
        """Chain A >> B >> C, verify A->B and B->C."""
        pipeline = self._make_pipeline()
        with pipeline:
            t_a = pipeline.sl_load(
                task_id="load_starbake_orders",
                domain="starbake",
                table="orders",
            )
            t_b = pipeline.sl_load(
                task_id="load_starbake_customers",
                domain="starbake",
                table="customers",
            )
            t_c = pipeline.sl_load(
                task_id="load_starbake_products",
                domain="starbake",
                table="products",
            )
            t_a >> t_b >> t_c

            assert "load_starbake_customers" in pipeline.upstream_dependencies.get(
                "load_starbake_orders", []
            )
            assert "load_starbake_products" in pipeline.upstream_dependencies.get(
                "load_starbake_customers", []
            )

    # ------------------------------------------------------------------
    # 1.5  fan-out: A >> [B, C]
    # ------------------------------------------------------------------

    def test_fan_out(self):
        """Fan-out A >> [B, C], verify A is upstream of both B and C."""
        pipeline = self._make_pipeline()
        with pipeline:
            t_a = pipeline.sl_load(
                task_id="load_starbake_orders",
                domain="starbake",
                table="orders",
            )
            t_b = pipeline.sl_load(
                task_id="load_starbake_customers",
                domain="starbake",
                table="customers",
            )
            t_c = pipeline.sl_load(
                task_id="load_starbake_products",
                domain="starbake",
                table="products",
            )
            t_a >> [t_b, t_c]

            downstream_of_a = pipeline.upstream_dependencies.get(
                "load_starbake_orders", []
            )
            assert "load_starbake_customers" in downstream_of_a
            assert "load_starbake_products" in downstream_of_a

    # ------------------------------------------------------------------
    # 1.6  fan-in: C << [A, B]
    # ------------------------------------------------------------------

    def test_fan_in(self):
        """Fan-in C << [A, B], verify both A and B are upstream of C."""
        pipeline = self._make_pipeline()
        with pipeline:
            t_a = pipeline.sl_load(
                task_id="load_starbake_orders",
                domain="starbake",
                table="orders",
            )
            t_b = pipeline.sl_load(
                task_id="load_starbake_customers",
                domain="starbake",
                table="customers",
            )
            t_c = pipeline.sl_load(
                task_id="load_starbake_products",
                domain="starbake",
                table="products",
            )
            t_c << [t_a, t_b]

            # A and B are upstream of C
            upstream_of_c = pipeline.downstream_dependencies.get(
                "load_starbake_products", []
            )
            assert "load_starbake_orders" in upstream_of_c
            assert "load_starbake_customers" in upstream_of_c
            # C is downstream of both A and B
            assert "load_starbake_products" in pipeline.upstream_dependencies.get(
                "load_starbake_orders", []
            )
            assert "load_starbake_products" in pipeline.upstream_dependencies.get(
                "load_starbake_customers", []
            )

    # ------------------------------------------------------------------
    # 1.7  roots and leaves detection
    # ------------------------------------------------------------------

    def test_roots_and_leaves(self):
        """After chaining A >> B >> C, verify roots=[A] and leaves=[C]."""
        pipeline = self._make_pipeline()
        with pipeline:
            t_a = pipeline.sl_load(
                task_id="load_starbake_orders",
                domain="starbake",
                table="orders",
            )
            t_b = pipeline.sl_load(
                task_id="load_starbake_customers",
                domain="starbake",
                table="customers",
            )
            t_c = pipeline.sl_load(
                task_id="load_starbake_products",
                domain="starbake",
                table="products",
            )
            t_a >> t_b >> t_c

            root_ids = pipeline.roots_keys
            leaf_ids = pipeline.leaves_keys
            assert "load_starbake_orders" in root_ids
            assert "load_starbake_products" in leaf_ids
            # B is neither root nor leaf
            assert "load_starbake_customers" not in root_ids
            assert "load_starbake_customers" not in leaf_ids

    # ------------------------------------------------------------------
    # 1.8  no chaining — all tasks are both roots and leaves
    # ------------------------------------------------------------------

    def test_no_chaining_all_roots(self):
        """Register tasks without chaining, verify all are roots and leaves."""
        pipeline = self._make_pipeline()
        with pipeline:
            pipeline.sl_load(
                task_id="load_starbake_orders",
                domain="starbake",
                table="orders",
            )
            pipeline.sl_load(
                task_id="load_starbake_customers",
                domain="starbake",
                table="customers",
            )

            root_ids = pipeline.roots_keys
            leaf_ids = pipeline.leaves_keys
            assert "load_starbake_orders" in root_ids
            assert "load_starbake_customers" in root_ids
            assert "load_starbake_orders" in leaf_ids
            assert "load_starbake_customers" in leaf_ids
