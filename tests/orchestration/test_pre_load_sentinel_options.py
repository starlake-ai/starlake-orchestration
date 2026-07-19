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

"""Story 6.12 (issue #122) — core ``sl_pre_load`` sentinel wiring.

When ``pre_load_not_ready_sentinel_path`` resolves, ``sl_pre_load`` appends
``--notReadySentinel <resolved path>`` to the CLI arguments (all three
strategies, outside the ACK branch) and injects ``kwargs['sentinel_path']``
for the engines. When off: byte-identical arguments/kwargs (6.2 StubJob
capture pattern).
"""

from __future__ import annotations

import pytest

from ai.starlake.sentinel import SENTINEL_OPTION, SENTINEL_SCOPE_TOKEN

from tests.orchestration.conftest import StubJob, _STUB_MODULE_NAME


def _make_job(options: dict) -> StubJob:
    return StubJob(
        filename="test_pre_load_sentinel.py",
        module_name=_STUB_MODULE_NAME,
        options=options,
    )


def _sentinel_args(task) -> list:
    return task["arguments"]


# ---------------------------------------------------------------------------
# 1. Zero-change guarantee — option off
# ---------------------------------------------------------------------------

class TestSentinelOffZeroChange:

    @pytest.mark.parametrize("options", [
        {"pre_load_strategy": "imported"},
        {"pre_load_strategy": "imported", SENTINEL_OPTION: ""},
        {"pre_load_strategy": "imported", SENTINEL_OPTION: "   "},
    ])
    def test_off_is_byte_identical(self, options):
        baseline = _make_job({"pre_load_strategy": "imported"}).sl_pre_load(
            domain="starbake", tables={"customers"}
        )
        task = _make_job(options).sl_pre_load(domain="starbake", tables={"customers"})
        assert task["arguments"] == baseline["arguments"]
        assert task["kwargs"] == baseline["kwargs"]
        assert "--notReadySentinel" not in task["arguments"]
        assert "sentinel_path" not in task["kwargs"]


# ---------------------------------------------------------------------------
# 2. Option on — argument + kwarg injection, all three strategies
# ---------------------------------------------------------------------------

class TestSentinelInjection:

    EXPECTED_PATH = f"gs://bucket/sentinels/starbake/{SENTINEL_SCOPE_TOKEN}.notready"

    @pytest.mark.parametrize("strategy", ["imported", "pending", "ack"])
    def test_all_strategies_get_flag_and_kwarg(self, strategy):
        job = _make_job({
            "pre_load_strategy": strategy,
            SENTINEL_OPTION: "gs://bucket/sentinels",
        })
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        args = _sentinel_args(task)
        assert args[args.index("--notReadySentinel") + 1] == self.EXPECTED_PATH
        assert task["kwargs"]["sentinel_path"] == self.EXPECTED_PATH

    def test_path_embeds_scope_token_not_jinja(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            SENTINEL_OPTION: "gs://bucket/sentinels/",
        })
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        path = task["kwargs"]["sentinel_path"]
        assert SENTINEL_SCOPE_TOKEN in path
        assert "{{" not in path and "}}" not in path

    def test_ack_keeps_global_ack_file_path(self):
        job = _make_job({
            "pre_load_strategy": "ack",
            SENTINEL_OPTION: "gs://bucket/sentinels",
        })
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        args = _sentinel_args(task)
        assert "--globalAckFilePath" in args
        # sentinel flag appended after the base args (incl. the ACK ones)
        assert args.index("--notReadySentinel") > args.index("--globalAckFilePath")

    def test_domain_is_scoped_into_the_path(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            SENTINEL_OPTION: "gs://bucket/sentinels",
        })
        task = job.sl_pre_load(domain="sales", tables=set())
        assert "/sales/" in task["kwargs"]["sentinel_path"]


# ---------------------------------------------------------------------------
# 3. NONE strategy short-circuits before any sentinel work
# ---------------------------------------------------------------------------

class TestNoneStrategy:

    def test_none_returns_none_even_with_sentinel_on(self):
        job = _make_job({
            "pre_load_strategy": "none",
            SENTINEL_OPTION: "gs://bucket/sentinels",
        })
        assert job.sl_pre_load(domain="starbake", tables={"customers"}) is None

    def test_none_with_invalid_sentinel_value_does_not_raise(self):
        """NONE returns before the sentinel option is even resolved (same
        contract as the 6.2 sensor options)."""
        job = _make_job({
            "pre_load_strategy": "none",
            SENTINEL_OPTION: "hdfs://bad/scheme",
        })
        assert job.sl_pre_load(domain="starbake", tables={"customers"}) is None


# ---------------------------------------------------------------------------
# 4. Strict validation at definition time
# ---------------------------------------------------------------------------

class TestSentinelValidation:

    def test_unknown_scheme_raises_at_definition_time(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            SENTINEL_OPTION: "hdfs://nn/sentinels",
        })
        with pytest.raises(ValueError):
            job.sl_pre_load(domain="starbake", tables={"customers"})

    def test_relative_local_prefix_raises(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            SENTINEL_OPTION: "relative/dir",
        })
        with pytest.raises(ValueError) as exc_info:
            job.sl_pre_load(domain="starbake", tables={"customers"})
        assert "ABSOLUTE" in str(exc_info.value)

    def test_non_string_option_raises(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            SENTINEL_OPTION: 42,
        })
        with pytest.raises(ValueError) as exc_info:
            job.sl_pre_load(domain="starbake", tables={"customers"})
        assert SENTINEL_OPTION in str(exc_info.value)


# ---------------------------------------------------------------------------
# 5. Sensor + sentinel coexistence — both kwargs sets reach sl_job
# ---------------------------------------------------------------------------

class TestSensorSentinelCoexistence:

    def test_both_kwargs_sets_coexist(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            "pre_load_sensor": "true",
            "pre_load_poke_interval": "42",
            "pre_load_timeout": "120",
            SENTINEL_OPTION: "gs://bucket/sentinels",
        })
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        captured = task["kwargs"]
        assert captured["pre_load_sensor"] is True
        assert captured["pre_load_poke_interval"] == 42
        assert captured["pre_load_timeout"] == 120
        assert captured["sentinel_path"].endswith(".notready")
        assert "--notReadySentinel" in task["arguments"]

    def test_sentinel_without_sensor_keeps_sensor_kwargs_out(self):
        job = _make_job({
            "pre_load_strategy": "imported",
            SENTINEL_OPTION: "gs://bucket/sentinels",
        })
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        captured = task["kwargs"]
        assert "pre_load_sensor" not in captured
        assert captured["sentinel_path"].endswith(".notready")


# ---------------------------------------------------------------------------
# 6. Pipeline path (feedback_test_via_pipeline)
# ---------------------------------------------------------------------------

class TestSentinelThroughPipeline:

    def test_sentinel_kwarg_flows_through_pipeline(self, stub_orchestration, stub_schedule):
        job = _make_job({
            "pre_load_strategy": "imported",
            SENTINEL_OPTION: "gs://bucket/sentinels",
        })
        orchestration = stub_orchestration.__class__(job=job)
        with orchestration:
            pipeline = orchestration.sl_create_pipeline(schedule=stub_schedule)
            with pipeline:
                task = pipeline.sl_pre_load(domain="starbake", tables={"customers"})
        assert task is not None
        captured = task.task["kwargs"]
        assert captured["sentinel_path"] == (
            f"gs://bucket/sentinels/starbake/{SENTINEL_SCOPE_TOKEN}.notready"
        )
