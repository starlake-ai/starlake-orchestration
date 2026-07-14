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

from tests.shared.base_test_orchestration import BaseTestOrchestration


class BaseTestSlPreLoad(BaseTestOrchestration):
    """Abstract base for sl_pre_load() shared functional tests."""

    # ------------------------------------------------------------------
    # IMPORTED strategy
    # ------------------------------------------------------------------

    def test_pre_load_strategy_imported(self):
        """Verify IMPORTED creates task with 'check_{domain}_incoming_files' ID
        and correct strategy in args."""
        orchestration = self.create_orchestration()
        job = orchestration.job
        task = job.sl_pre_load(
            domain="starbake",
            pre_load_strategy=StarlakePreLoadStrategy.IMPORTED,
        )
        assert task is not None
        actual_id = self.get_task_id(task)
        assert actual_id == "check_starbake_incoming_files"
        args = self.get_task_arguments(task)
        assert "preload" in args
        assert self.get_arg_value(args, "--strategy") == "imported"

    # ------------------------------------------------------------------
    # PENDING strategy
    # ------------------------------------------------------------------

    def test_pre_load_strategy_pending(self):
        """Verify PENDING creates task with 'check_{domain}_pending_files' ID."""
        orchestration = self.create_orchestration()
        job = orchestration.job
        task = job.sl_pre_load(
            domain="starbake",
            pre_load_strategy=StarlakePreLoadStrategy.PENDING,
        )
        assert task is not None
        actual_id = self.get_task_id(task)
        assert actual_id == "check_starbake_pending_files"
        args = self.get_task_arguments(task)
        assert self.get_arg_value(args, "--strategy") == "pending"

    # ------------------------------------------------------------------
    # ACK strategy
    # ------------------------------------------------------------------

    def test_pre_load_strategy_ack(self):
        """Verify ACK creates task with 'check_{domain}_ack_file' ID
        and includes retry_delay via kwargs."""
        orchestration = self.create_orchestration()
        job = orchestration.job
        task = job.sl_pre_load(
            domain="starbake",
            pre_load_strategy=StarlakePreLoadStrategy.ACK,
        )
        assert task is not None
        actual_id = self.get_task_id(task)
        assert actual_id == "check_starbake_ack_file"
        args = self.get_task_arguments(task)
        assert self.get_arg_value(args, "--strategy") == "ack"
        # ACK strategy adds --globalAckFilePath to arguments
        assert "--globalAckFilePath" in args

    # ------------------------------------------------------------------
    # NONE strategy
    # ------------------------------------------------------------------

    def test_pre_load_strategy_none(self):
        """Verify NONE returns None (no task created)."""
        orchestration = self.create_orchestration()
        job = orchestration.job
        task = job.sl_pre_load(
            domain="starbake",
            pre_load_strategy=StarlakePreLoadStrategy.NONE,
        )
        assert task is None
