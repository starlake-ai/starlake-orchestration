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

"""Story 6.2 (issue #86) — optional sensor mode for the pre-load task.

Core contract: ``IStarlakeJob.sl_pre_load`` resolves the ``pre_load_sensor``
option (or the explicit ``sensor`` kwarg) and, when enabled, forwards the four
``pre_load_*`` kwargs to ``sl_job``.  When disabled the kwargs reaching
``sl_job`` are byte-identical to today's (zero-change guarantee).
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from tests.orchestration.conftest import StubJob, _STUB_MODULE_NAME

SENSOR_KWARGS = (
    "pre_load_sensor",
    "pre_load_poke_interval",
    "pre_load_timeout",
    "pre_load_sensor_soft_fail",
)


def _make_job(options: dict) -> StubJob:
    return StubJob(
        filename="test_pre_load_sensor.py",
        module_name=_STUB_MODULE_NAME,
        options=options,
    )


# ---------------------------------------------------------------------------
# 1. Zero-change guarantee — option off / kwarg absent
# ---------------------------------------------------------------------------

class TestSensorOffZeroChange:

    def test_option_unset_no_sensor_kwargs_reach_sl_job(self):
        job = _make_job({"pre_load_strategy": "imported"})
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert task is not None
        captured = task["kwargs"]
        for key in SENSOR_KWARGS:
            assert key not in captured
        assert "sensor" not in captured

    def test_option_false_no_sensor_kwargs_reach_sl_job(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            "pre_load_sensor": "false",
        })
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        captured = task["kwargs"]
        for key in SENSOR_KWARGS:
            assert key not in captured

    def test_option_off_arguments_and_task_id_unchanged(self):
        job = _make_job({"pre_load_strategy": "imported"})
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert task["task_id"] == "check_starbake_incoming_files"
        args = task["arguments"]
        assert args[0] == "preload"
        assert args[args.index("--strategy") + 1] == "imported"


# ---------------------------------------------------------------------------
# 2. Option on — defaults and explicit values
# ---------------------------------------------------------------------------

class TestSensorOnOptionResolution:

    def test_option_true_injects_defaults(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            "pre_load_sensor": "true",
        })
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        captured = task["kwargs"]
        assert captured["pre_load_sensor"] is True
        assert captured["pre_load_poke_interval"] == 300
        assert captured["pre_load_timeout"] == 3600
        assert captured["pre_load_sensor_soft_fail"] is False

    def test_option_true_explicit_values(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            "pre_load_sensor": "true",
            "pre_load_poke_interval": "42",
            "pre_load_timeout": "120",
            "pre_load_sensor_soft_fail": "true",
        })
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        captured = task["kwargs"]
        assert captured["pre_load_sensor"] is True
        assert captured["pre_load_poke_interval"] == 42
        assert captured["pre_load_timeout"] == 120
        assert captured["pre_load_sensor_soft_fail"] is True

    def test_sensor_mode_keeps_task_id_and_cli_arguments(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            "pre_load_sensor": "true",
        })
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert task["task_id"] == "check_starbake_incoming_files"
        args = task["arguments"]
        assert args[0] == "preload"
        assert args[args.index("--domain") + 1] == "starbake"
        assert args[args.index("--strategy") + 1] == "imported"


# ---------------------------------------------------------------------------
# 3. Kwarg precedence — explicit sensor kwarg wins over the option
# ---------------------------------------------------------------------------

class TestSensorKwargPrecedence:

    def test_sensor_true_kwarg_with_option_unset_enables(self):
        job = _make_job({"pre_load_strategy": "imported"})
        task = job.sl_pre_load(
            domain="starbake", tables={"customers"}, sensor=True
        )
        captured = task["kwargs"]
        assert captured["pre_load_sensor"] is True
        assert captured["pre_load_poke_interval"] == 300
        assert captured["pre_load_timeout"] == 3600
        assert captured["pre_load_sensor_soft_fail"] is False

    def test_sensor_false_kwarg_with_option_true_disables(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            "pre_load_sensor": "true",
        })
        task = job.sl_pre_load(
            domain="starbake", tables={"customers"}, sensor=False
        )
        captured = task["kwargs"]
        for key in SENSOR_KWARGS:
            assert key not in captured

    def test_sensor_kwarg_flows_through_pipeline(self, stub_orchestration, stub_schedule):
        """AC7 — orchestration → pipeline → pipeline.sl_pre_load kwargs path."""
        job = _make_job({"pre_load_strategy": "imported"})
        orchestration = stub_orchestration.__class__(job=job)
        with orchestration:
            pipeline = orchestration.sl_create_pipeline(schedule=stub_schedule)
            with pipeline:
                task = pipeline.sl_pre_load(
                    domain="starbake", tables={"customers"}, sensor=True
                )
        assert task is not None
        captured = task.task["kwargs"]
        assert captured["pre_load_sensor"] is True
        assert captured["pre_load_poke_interval"] == 300


# ---------------------------------------------------------------------------
# 4. Strict validation (NFR11)
# ---------------------------------------------------------------------------

class TestSensorValidation:

    @pytest.mark.parametrize("bad_interval", ["abc", "0", "-5"])
    def test_invalid_poke_interval_raises(self, bad_interval):
        job = _make_job({
            "pre_load_strategy": "imported",
            "pre_load_sensor": "true",
            "pre_load_poke_interval": bad_interval,
        })
        with pytest.raises(ValueError) as exc_info:
            job.sl_pre_load(domain="starbake", tables={"customers"})
        message = str(exc_info.value)
        assert "STUB" in message
        assert "pre_load_poke_interval" in message
        assert bad_interval in message

    @pytest.mark.parametrize("bad_timeout", ["abc", "0", "-5"])
    def test_invalid_timeout_raises(self, bad_timeout):
        job = _make_job({
            "pre_load_strategy": "imported",
            "pre_load_sensor": "true",
            "pre_load_timeout": bad_timeout,
        })
        with pytest.raises(ValueError) as exc_info:
            job.sl_pre_load(domain="starbake", tables={"customers"})
        message = str(exc_info.value)
        assert "STUB" in message
        assert "pre_load_timeout" in message
        assert bad_timeout in message

    def test_timeout_smaller_than_poke_interval_raises(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            "pre_load_sensor": "true",
            "pre_load_poke_interval": "300",
            "pre_load_timeout": "60",
        })
        with pytest.raises(ValueError) as exc_info:
            job.sl_pre_load(domain="starbake", tables={"customers"})
        message = str(exc_info.value)
        assert "STUB" in message
        assert "pre_load_timeout" in message
        assert "pre_load_poke_interval" in message
        assert "60" in message
        assert "300" in message

    def test_non_boolean_sensor_option_raises(self):
        """'yes' must NOT silently map to False (the permissive == 'true' trap)."""
        job = _make_job({
            "pre_load_strategy": "imported",
            "pre_load_sensor": "yes",
        })
        with pytest.raises(ValueError) as exc_info:
            job.sl_pre_load(domain="starbake", tables={"customers"})
        message = str(exc_info.value)
        assert "STUB" in message
        assert "pre_load_sensor" in message
        assert "yes" in message

    def test_non_boolean_soft_fail_option_raises(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            "pre_load_sensor": "true",
            "pre_load_sensor_soft_fail": "maybe",
        })
        with pytest.raises(ValueError) as exc_info:
            job.sl_pre_load(domain="starbake", tables={"customers"})
        message = str(exc_info.value)
        assert "STUB" in message
        assert "pre_load_sensor_soft_fail" in message
        assert "maybe" in message


# ---------------------------------------------------------------------------
# 5. ACK strategy interaction
# ---------------------------------------------------------------------------

class TestAckSensorInteraction:

    def test_ack_with_sensor_skips_retry_delay_keeps_ack_file(self):
        job = _make_job({
            "pre_load_strategy": "ack",
            "pre_load_sensor": "true",
        })
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert task["task_id"] == "check_starbake_ack_file"
        assert "--globalAckFilePath" in task["arguments"]
        captured = task["kwargs"]
        assert "retry_delay" not in captured
        assert captured["pre_load_sensor"] is True

    def test_ack_without_sensor_keeps_retry_delay_injection(self):
        """Regression — the historical ACK retry-as-wait idiom is untouched."""
        job = _make_job({"pre_load_strategy": "ack"})
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert "--globalAckFilePath" in task["arguments"]
        captured = task["kwargs"]
        assert captured["retry_delay"] == timedelta(seconds=3600)
        for key in SENSOR_KWARGS:
            assert key not in captured


# ---------------------------------------------------------------------------
# 6. NONE strategy
# ---------------------------------------------------------------------------

class TestNoneStrategyWithSensor:

    def test_none_strategy_still_returns_none_with_sensor_option_on(self):
        job = _make_job({
            "pre_load_strategy": "none",
            "pre_load_sensor": "true",
        })
        assert job.sl_pre_load(domain="starbake", tables={"customers"}) is None
