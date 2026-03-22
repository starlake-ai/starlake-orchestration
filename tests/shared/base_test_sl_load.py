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

from ai.starlake.job import StarlakePreLoadStrategy

from ai.starlake.orchestration import (
    AbstractTask,
    TaskGroupContext,
)

from tests.shared.base_test_orchestration import BaseTestOrchestration


class BaseTestSlLoad(BaseTestOrchestration):
    """Abstract base for sl_load() shared functional tests."""

    # ------------------------------------------------------------------
    # Single table load
    # ------------------------------------------------------------------

    def test_single_table_load(self):
        """Load one table (customers) via sl_load(), verify task is created
        with correct domain/table args."""
        orchestration = self.create_orchestration()
        job = orchestration.job
        task = job.sl_load(
            task_id="load_starbake_customers",
            domain="starbake",
            table="customers",
        )
        assert task is not None
        args = self.get_task_arguments(task)
        assert "load" in args
        assert "--domains" in args
        assert "starbake" in args
        assert "--tables" in args
        assert "customers" in args

    # ------------------------------------------------------------------
    # Multiple table load with dependency chaining
    # ------------------------------------------------------------------

    def test_multiple_table_load_with_dependencies(self):
        """Load orders and customers, chain with >>, verify ordering."""
        orchestration = self.create_orchestration()
        job = orchestration.job
        raw_orders = job.sl_load(
            task_id="load_starbake_orders",
            domain="starbake",
            table="orders",
        )
        raw_customers = job.sl_load(
            task_id="load_starbake_customers",
            domain="starbake",
            table="customers",
        )
        assert raw_orders is not None
        assert raw_customers is not None

        ctx = TaskGroupContext("test_load_deps", orchestration)
        with ctx:
            t_orders = AbstractTask("load_starbake_orders", raw_orders)
            t_customers = AbstractTask("load_starbake_customers", raw_customers)
            t_orders >> t_customers

            # Verify ordering: orders is upstream of customers
            downstream_ids = ctx.upstream_dependencies.get(
                "load_starbake_orders", []
            )
            assert "load_starbake_customers" in downstream_ids

    # ------------------------------------------------------------------
    # Pre-load → import → load chaining
    # ------------------------------------------------------------------

    def test_load_with_pre_load_strategy(self):
        """Verify sl_pre_load(IMPORTED) >> sl_import() >> sl_load() chaining.

        With the IMPORTED strategy, the full chain is:
        pre_load (check incoming files) >> import (stage) >> load.
        """
        orchestration = self.create_orchestration()
        job = orchestration.job

        pre_load_task = job.sl_pre_load(
            domain="starbake",
            pre_load_strategy=StarlakePreLoadStrategy.IMPORTED,
        )
        import_task = job.sl_import(
            task_id="import_starbake",
            domain="starbake",
        )
        load_task = job.sl_load(
            task_id="load_starbake_customers",
            domain="starbake",
            table="customers",
        )
        assert pre_load_task is not None
        assert import_task is not None
        assert load_task is not None

        # Verify the framework generated the expected task ID
        pre_load_id = self.get_task_id(pre_load_task)
        assert pre_load_id == "check_starbake_incoming_files"

        ctx = TaskGroupContext("test_preload_chain", orchestration)
        with ctx:
            t_pre = AbstractTask(pre_load_id, pre_load_task)
            t_import = AbstractTask("import_starbake", import_task)
            t_load = AbstractTask("load_starbake_customers", load_task)
            t_pre >> t_import >> t_load

            # Verify full chain: pre_load >> import >> load
            assert "import_starbake" in ctx.upstream_dependencies.get(
                pre_load_id, []
            )
            assert "load_starbake_customers" in ctx.upstream_dependencies.get(
                "import_starbake", []
            )
