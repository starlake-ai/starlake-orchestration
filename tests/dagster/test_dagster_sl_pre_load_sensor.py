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

"""Story 6.2 (issue #86) — Dagster shell poke loop for the pre-load task.

Dagster has no reschedule primitive: sensor mode is an in-op wall-clock poke
loop around ``execute_shell_command`` (the op holds its executor slot while
poking).  Soft-fail timeout routes to the existing optional-output skip;
hard timeout raises ``Failure`` and must NOT be swallowed by the forced
``skip_or_start=True``.
"""

from __future__ import annotations

import time

import pytest

from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME

PRELOAD_TASK_ID = "check_starbake_incoming_files"

SENSOR_OPTIONS = {
    "pre_load_strategy": "imported",
    "pre_load_sensor": "true",
    "pre_load_poke_interval": "42",
    "pre_load_timeout": "120",
}

RUN_CONFIG = {
    "ops": {
        PRELOAD_TASK_ID: {
            "config": {
                "logical_datetime": "2026-07-16T00:00:00+00:00",
                "dry_run": False,
            }
        }
    }
}


def _make_job(options: dict):
    from ai.starlake.dagster.shell import StarlakeDagsterShellJob
    return StarlakeDagsterShellJob(
        filename="test_dagster_sensor.py",
        module_name=_DAGSTER_TEST_MODULE_NAME,
        options=options,
    )


def _shell_module():
    import ai.starlake.dagster.shell.starlake_dagster_shell_job as shell_mod
    return shell_mod


def _make_preload_node(options: dict):
    from ai.starlake.job import StarlakePreLoadStrategy
    job = _make_job(options)
    return job.sl_pre_load(
        domain="starbake",
        tables={"customers"},
        pre_load_strategy=StarlakePreLoadStrategy.IMPORTED,
    )


def _execute(node, raise_on_error=True):
    from dagster import GraphDefinition
    graph = GraphDefinition(name="preload_sensor_graph", node_defs=[node])
    return graph.execute_in_process(
        run_config=RUN_CONFIG, raise_on_error=raise_on_error
    )


class _FakeClock:
    """Deterministic monotonic clock advanced by fake sleeps."""

    def __init__(self):
        self.now = 1000.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def fake_clock(monkeypatch):
    clock = _FakeClock()
    # the implementation must call time.monotonic()/time.sleep() through the
    # time module precisely so tests can patch them here
    monkeypatch.setattr(time, "monotonic", clock.monotonic)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    return clock


# ---------------------------------------------------------------------------
# 1. Regression — sensor off keeps the single-shot execution
# ---------------------------------------------------------------------------

class TestSensorOffRegression:

    def test_sensor_off_single_execute_call(self, monkeypatch):
        shell_mod = _shell_module()
        calls = []

        def fake_execute(shell_command, **kwargs):
            calls.append(shell_command)
            return ("ok", 0)

        monkeypatch.setattr(shell_mod, "execute_shell_command", fake_execute)

        node = _make_preload_node({"pre_load_strategy": "imported"})
        result = _execute(node)
        assert result.success
        assert len(calls) == 1


# ---------------------------------------------------------------------------
# 2. Poke loop — non-zero exits poke again until success
# ---------------------------------------------------------------------------

class TestPokeLoop:

    def test_pokes_until_success_with_sleeps_between(
        self, monkeypatch, fake_clock
    ):
        shell_mod = _shell_module()
        calls = []

        def fake_execute(shell_command, **kwargs):
            calls.append(shell_command)
            return ("out", 1 if len(calls) < 3 else 0)

        monkeypatch.setattr(shell_mod, "execute_shell_command", fake_execute)

        node = _make_preload_node(dict(SENSOR_OPTIONS))
        result = _execute(node)

        assert result.success
        assert len(calls) == 3
        assert fake_clock.sleeps == [42, 42]
        # success still yields the result Output (downstream gating proceeds)
        assert result.output_for_node(PRELOAD_TASK_ID, "result") is not None


# ---------------------------------------------------------------------------
# 3. Timeout + soft_fail=true — skip downstream (optional output not yielded)
# ---------------------------------------------------------------------------

class TestSoftFailTimeout:

    def test_soft_fail_timeout_skips_downstream(self, monkeypatch, fake_clock):
        from dagster import (
            DependencyDefinition,
            GraphDefinition,
            In,
            op,
        )
        from dagster._core.events import DagsterEventType

        shell_mod = _shell_module()
        calls = []

        def fake_execute(shell_command, **kwargs):
            calls.append(shell_command)
            return ("still nothing", 1)

        monkeypatch.setattr(shell_mod, "execute_shell_command", fake_execute)

        node = _make_preload_node(
            dict(SENSOR_OPTIONS, pre_load_sensor_soft_fail="true")
        )

        downstream_calls = []

        @op(ins={"start": In(str)})
        def downstream_load(start):
            downstream_calls.append(start)
            return start

        graph = GraphDefinition(
            name="preload_sensor_soft_fail_graph",
            node_defs=[node, downstream_load],
            dependencies={
                "downstream_load": {
                    "start": DependencyDefinition(PRELOAD_TASK_ID, "result")
                }
            },
        )
        result = graph.execute_in_process(run_config=RUN_CONFIG)

        assert result.success
        # deadline: pokes at t=0/42/84, next poke would land at 126 > 120
        assert len(calls) == 3
        assert fake_clock.sleeps == [42, 42]
        assert downstream_calls == []
        skipped = [
            event.step_key
            for event in result.all_events
            if event.event_type == DagsterEventType.STEP_SKIPPED
        ]
        assert "downstream_load" in skipped


# ---------------------------------------------------------------------------
# 4. Timeout + soft_fail=false — hard Failure (not swallowed by skip_or_start)
# ---------------------------------------------------------------------------

class TestHardFailTimeout:

    def test_hard_timeout_raises_failure_with_timed_out_description(
        self, monkeypatch, fake_clock
    ):
        from dagster._core.events import DagsterEventType

        shell_mod = _shell_module()

        def fake_execute(shell_command, **kwargs):
            return ("still nothing", 1)

        monkeypatch.setattr(shell_mod, "execute_shell_command", fake_execute)

        node = _make_preload_node(dict(SENSOR_OPTIONS))  # soft_fail defaults false
        result = _execute(node, raise_on_error=False)

        assert not result.success
        failures = [
            event
            for event in result.all_events
            if event.event_type == DagsterEventType.STEP_FAILURE
            and event.step_key == PRELOAD_TASK_ID
        ]
        assert len(failures) == 1
        assert "timed out" in str(failures[0].event_specific_data.error)


# ---------------------------------------------------------------------------
# 5. Cloud variants reject sensor mode
# ---------------------------------------------------------------------------

class TestCloudVariantRejection:

    @pytest.mark.parametrize("env_name", ["cloud_run", "dataproc", "fargate"])
    def test_sensor_flag_raises_naming_environment(self, env_name):
        from ai.starlake.dagster import StarlakeDagsterJob

        kwargs = {
            "pre_load_sensor": True,
            "pre_load_poke_interval": 42,
            "pre_load_timeout": 120,
            "pre_load_sensor_soft_fail": False,
        }
        with pytest.raises(ValueError) as exc_info:
            StarlakeDagsterJob._reject_pre_load_sensor_kwargs(kwargs, env_name)
        message = str(exc_info.value)
        assert env_name in message
        assert "pre_load_sensor" in message
        # no retry-based workaround on Dagster: sl_pre_load forces retries=0
        # on every pre-load op, so the message must say one-shot, not point
        # at the retries/retry_delay options
        assert "retries=0" in message
        assert "one-shot" in message

    def test_without_flag_kwargs_popped_cleanly(self):
        from ai.starlake.dagster import StarlakeDagsterJob

        kwargs = {
            "pre_load_sensor": False,
            "pre_load_poke_interval": 42,
            "pre_load_timeout": 120,
            "pre_load_sensor_soft_fail": False,
            "retries": 0,
        }
        StarlakeDagsterJob._reject_pre_load_sensor_kwargs(kwargs, "cloud_run")
        assert kwargs == {"retries": 0}


# ---------------------------------------------------------------------------
# 6. RetryPolicy stays None on the preload op
# ---------------------------------------------------------------------------

class TestPreloadRetryPolicy:

    def test_retry_policy_none_in_sensor_mode(self):
        node = _make_preload_node(dict(SENSOR_OPTIONS))
        assert node.retry_policy is None

    def test_retry_policy_none_without_sensor(self):
        node = _make_preload_node({"pre_load_strategy": "imported"})
        assert node.retry_policy is None
