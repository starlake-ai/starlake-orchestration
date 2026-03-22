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
    AbstractTask,
    TaskGroupContext,
)

from tests.shared.base_test_orchestration import BaseTestOrchestration


class BaseTestSlTransform(BaseTestOrchestration):
    """Abstract base for sl_transform() shared functional tests."""

    # ------------------------------------------------------------------
    # Single transform
    # ------------------------------------------------------------------

    def test_single_transform(self):
        """Run sl_transform('kpi.order_summary'), verify task created
        with correct transform_name passed as --name arg."""
        orchestration = self.create_orchestration()
        job = orchestration.job
        task = job.sl_transform(
            task_id="kpi_order_summary",
            transform_name="kpi.order_summary",
        )
        assert task is not None
        args = self.get_task_arguments(task)
        assert "transform" in args
        assert self.get_arg_value(args, "--name") == "kpi.order_summary"

    # ------------------------------------------------------------------
    # Transform with dependencies
    # ------------------------------------------------------------------

    def test_transform_with_dependencies(self):
        """Chain order_summary >> top_customers, verify dependency ordering."""
        orchestration = self.create_orchestration()
        job = orchestration.job

        raw_summary = job.sl_transform(
            task_id="kpi_order_summary",
            transform_name="kpi.order_summary",
        )
        raw_top = job.sl_transform(
            task_id="kpi_top_customers",
            transform_name="kpi.top_customers",
        )
        assert raw_summary is not None
        assert raw_top is not None

        ctx = TaskGroupContext("test_transform_deps", orchestration)
        with ctx:
            t_summary = AbstractTask("kpi_order_summary", raw_summary)
            t_top = AbstractTask("kpi_top_customers", raw_top)
            t_summary >> t_top

            downstream_ids = ctx.upstream_dependencies.get(
                "kpi_order_summary", []
            )
            assert "kpi_top_customers" in downstream_ids

    # ------------------------------------------------------------------
    # Transform with options
    # ------------------------------------------------------------------

    def test_transform_with_options(self):
        """Verify transform_options parameter is passed through to CLI args."""
        orchestration = self.create_orchestration()
        job = orchestration.job
        task = job.sl_transform(
            task_id="kpi_order_summary_opts",
            transform_name="kpi.order_summary",
            transform_options="SL_KEY=value",
        )
        assert task is not None
        args = self.get_task_arguments(task)
        assert "SL_KEY=value" in self.get_arg_value(args, "--options")
