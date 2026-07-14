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

import pytest

from ai.starlake.job.starlake_pre_load_strategy import StarlakePreLoadStrategy

from tests.orchestration.conftest import StubJob, _STUB_MODULE_NAME


class TestPreLoadStrategyValidation:
    """Strict pre-load strategy resolution (NFR11) — core contract."""

    def test_invalid_string_in_options_raises(self):
        with pytest.raises(ValueError) as exc_info:
            StubJob(
                filename="test.py",
                module_name=_STUB_MODULE_NAME,
                options={"pre_load_strategy": "imprted"},
            )
        message = str(exc_info.value)
        assert "STUB" in message
        assert "imprted" in message
        assert "pre_load_strategy" in message
        for valid in ("imported", "ack", "pending", "none"):
            assert valid in message

    def test_invalid_string_argument_raises(self, stub_job):
        with pytest.raises(ValueError, match="not-a-strategy"):
            stub_job.sl_pre_load(domain="starbake", pre_load_strategy="not-a-strategy")

    def test_invalid_string_argument_message_contract(self, stub_job):
        with pytest.raises(ValueError) as exc_info:
            stub_job.sl_pre_load(domain="starbake", pre_load_strategy="imprted")
        message = str(exc_info.value)
        assert "STUB" in message
        assert "imprted" in message
        assert "sl_pre_load" in message
        for valid in ("imported", "ack", "pending", "none"):
            assert valid in message

    @pytest.mark.parametrize("value,expected", [
        ("imported", StarlakePreLoadStrategy.IMPORTED),
        ("ack", StarlakePreLoadStrategy.ACK),
        ("pending", StarlakePreLoadStrategy.PENDING),
        ("none", StarlakePreLoadStrategy.NONE),
    ])
    def test_valid_strings_still_resolve(self, value, expected):
        job = StubJob(
            filename="test.py",
            module_name=_STUB_MODULE_NAME,
            options={"pre_load_strategy": value},
        )
        assert job.pre_load_strategy == expected

    def test_valid_string_argument_still_resolves(self, stub_job):
        task = stub_job.sl_pre_load(domain="starbake", pre_load_strategy="imported")
        assert task is not None
        assert task["task_id"] == "check_starbake_incoming_files"
        args = task["arguments"]
        assert args[0] == "preload"
        idx = args.index("--strategy")
        assert args[idx + 1] == "imported"

    def test_none_and_empty_fall_back_to_default(self):
        job = StubJob(filename="test.py", module_name=_STUB_MODULE_NAME, options={})
        assert job.pre_load_strategy == StarlakePreLoadStrategy.NONE
        # empty string behaves like "not configured" (backward compatible)
        job2 = StubJob(
            filename="test.py",
            module_name=_STUB_MODULE_NAME,
            options={"pre_load_strategy": ""},
        )
        assert job2.pre_load_strategy == StarlakePreLoadStrategy.NONE

    def test_helper_enum_passthrough_and_default(self):
        assert (
            StubJob.sl_resolve_pre_load_strategy(StarlakePreLoadStrategy.ACK)
            == StarlakePreLoadStrategy.ACK
        )
        assert (
            StubJob.sl_resolve_pre_load_strategy(
                None, default=StarlakePreLoadStrategy.PENDING
            )
            == StarlakePreLoadStrategy.PENDING
        )
        assert StubJob.sl_resolve_pre_load_strategy("") is None

    def test_helper_unknown_orchestrator_falls_back_to_unknown(self):
        from ai.starlake.job.starlake_job import IStarlakeJob

        with pytest.raises(ValueError) as exc_info:
            IStarlakeJob.sl_resolve_pre_load_strategy("bogus")
        assert "[unknown]" in str(exc_info.value)
