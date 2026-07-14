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

import pytest

from ai.starlake.orchestration import AbstractPipeline, StarlakeSchedule

from tests.shared.base_test_orchestration import BaseTestOrchestration
from tests.shared.expected_results import CLI_CONTRACT, EXPECTED_TASK_IDS


class BaseTestSlActionConsistency(BaseTestOrchestration):
    """Cross-orchestrator sl_* action contract (NFR1 structural layer + NFR11).

    Every concrete orchestrator subclass runs the SAME scenario against the
    SAME canonical constants — equivalence across orchestrators is transitive
    through tests/shared/expected_results.py.
    """

    def _make_pipeline(self, options: Optional[dict] = None) -> AbstractPipeline:
        schedule = StarlakeSchedule(name=None, cron=None, domains=[])
        return self.create_test_pipeline(schedule=schedule, options=options)

    def _assert_contract(self, args, contract_key):
        contract = CLI_CONTRACT[contract_key]
        assert args, f"no CLI arguments extracted for {contract_key}"
        assert args[0] == contract["verb"], (
            f"{contract_key}: expected verb '{contract['verb']}', got '{args[0]}' in {args}"
        )
        for flag, value in contract["flags"].items():
            assert self.get_arg_value(args, flag) == value

    # --- canonical action contracts (through the pipeline, never raw) ---

    def test_canonical_load_contract(self):
        pipeline = self._make_pipeline()
        with pipeline:
            t = pipeline.sl_load(
                task_id=EXPECTED_TASK_IDS["load_customers"],
                domain="starbake",
                table="customers",
            )
        assert t is not None
        assert t.task_id == EXPECTED_TASK_IDS["load_customers"]
        self._assert_contract(self.get_task_arguments(t.task), "load")

    def test_canonical_transform_contract(self):
        pipeline = self._make_pipeline()
        with pipeline:
            t = pipeline.sl_transform(
                task_id=EXPECTED_TASK_IDS["transform_order_summary"],
                transform_name="kpi.order_summary",
            )
        assert t is not None
        assert t.task_id == EXPECTED_TASK_IDS["transform_order_summary"]
        self._assert_contract(self.get_task_arguments(t.task), "transform")

    def test_canonical_import_contract(self):
        pipeline = self._make_pipeline()
        with pipeline:
            t = pipeline.sl_import(
                task_id=EXPECTED_TASK_IDS["import"],
                domain="starbake",
            )
        assert t is not None
        assert t.task_id == EXPECTED_TASK_IDS["import"]
        self._assert_contract(self.get_task_arguments(t.task), "import")

    def test_canonical_pre_load_chain(self):
        """IMPORTED chain: sl_pre_load >> sl_import >> sl_load (never skip import)."""
        pipeline = self._make_pipeline(options={"pre_load_strategy": "imported"})
        with pipeline:
            t_pre = pipeline.sl_pre_load(domain="starbake", tables=set())
            t_import = pipeline.sl_import(
                task_id=EXPECTED_TASK_IDS["import"], domain="starbake"
            )
            t_load = pipeline.sl_load(
                task_id=EXPECTED_TASK_IDS["load_customers"],
                domain="starbake",
                table="customers",
            )
            assert t_pre is not None
            assert t_pre.task_id == EXPECTED_TASK_IDS["pre_load_imported"]
            self._assert_contract(self.get_task_arguments(t_pre.task), "pre_load")
            t_pre >> t_import >> t_load
            assert EXPECTED_TASK_IDS["import"] in pipeline.upstream_dependencies.get(
                EXPECTED_TASK_IDS["pre_load_imported"], []
            )
            assert EXPECTED_TASK_IDS["load_customers"] in pipeline.upstream_dependencies.get(
                EXPECTED_TASK_IDS["import"], []
            )

    # --- NFR11: orchestrator-named errors on invalid strategy ---

    def test_invalid_pre_load_strategy_argument_raises_with_orchestrator_name(self):
        """The job-API error contract: AbstractPipeline.sl_pre_load deliberately
        exposes no strategy parameter, so the argument path is exercised on the
        job (the only public surface that takes a strategy argument)."""
        orchestration = self.create_orchestration()
        job = orchestration.job
        orchestrator_name = str(job.sl_orchestrator())
        with pytest.raises(ValueError) as exc_info:
            job.sl_pre_load(domain="starbake", pre_load_strategy="imprted")  # typo on purpose
        message = str(exc_info.value)
        assert orchestrator_name in message
        assert "imprted" in message
        for valid in ("imported", "ack", "pending", "none"):
            assert valid in message

    def test_invalid_pre_load_strategy_option_raises_with_orchestrator_name(self):
        with pytest.raises(ValueError) as exc_info:
            self.create_orchestration(options={"pre_load_strategy": "bogus"})
        message = str(exc_info.value)
        assert "bogus" in message
        assert "pre_load_strategy" in message
