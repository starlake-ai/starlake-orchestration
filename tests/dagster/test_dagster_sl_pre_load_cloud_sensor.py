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

"""Story 6.7 (issue #94) — cloud pre-load poke loops (fargate, cloud_run, dataproc).

Extends the story 6.2 in-op wall-clock poke loop to the Dagster cloud
variants: each poke re-submits the cloud job (full submission overhead per
attempt) and interprets its terminal state through the shared provider-free
seams ``StarlakeDagsterJob._sl_resolve_pre_load_poke`` /
``_sl_pre_load_poke_loop``.  The 6.2 cloud rejection
(``_reject_pre_load_sensor_kwargs``) is removed.

The dataproc variant imports ``dagster_gcp`` — its tests are skip-guarded
(CI installs no dagster-gcp) and run in the local provider venv.  The
cloud_run and fargate variants only need ``dagster_shell`` + stdlib and are
exercised in CI.
"""

from __future__ import annotations

import os
import tempfile
import time
import types

import pytest

from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME

try:
    import dagster_gcp  # noqa: F401
    DAGSTER_GCP_AVAILABLE = True
except ImportError:
    DAGSTER_GCP_AVAILABLE = False

PRELOAD_TASK_ID = "check_starbake_incoming_files"

SENSOR_OPTIONS = {
    "pre_load_strategy": "imported",
    "pre_load_sensor": "true",
    "pre_load_poke_interval": "42",
    "pre_load_timeout": "120",
}

CLOUD_RUN_OPTIONS = {
    "cloud_run_job_name": "sl-preload-job",
    "cloud_run_project_id": "test-project",
    "cloud_run_job_region": "europe-west1",
}

FARGATE_OPTIONS = {
    "aws_cluster_name": "test-cluster",
    "aws_task_definition_name": "sl-task",
    "aws_task_definition_container_name": "sl-container",
}

DATAPROC_OPTIONS = {
    "dataproc_project_id": "test-project",
    "spark_jar_list": "gs://test-bucket/starlake.jar",
    "spark_bucket": "test-bucket",
}

RUN_CONFIG = {
    "ops": {
        PRELOAD_TASK_ID: {
            "config": {
                "logical_datetime": "2026-07-18T00:00:00+00:00",
                "dry_run": False,
            }
        }
    }
}


class _FakeClock:
    """Deterministic monotonic clock advanced by fake sleeps (6.2 pattern)."""

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


@pytest.fixture
def stub_context():
    return types.SimpleNamespace(
        log=types.SimpleNamespace(info=lambda *args, **kwargs: None)
    )


def _make_preload_node(job):
    from ai.starlake.job import StarlakePreLoadStrategy
    return job.sl_pre_load(
        domain="starbake",
        tables={"customers"},
        pre_load_strategy=StarlakePreLoadStrategy.IMPORTED,
    )


def _execute(node, raise_on_error=True):
    from dagster import GraphDefinition
    graph = GraphDefinition(name="preload_cloud_sensor_graph", node_defs=[node])
    return graph.execute_in_process(
        run_config=RUN_CONFIG, raise_on_error=raise_on_error
    )


def _execute_dry_run(node):
    from dagster import GraphDefinition
    dry_run_config = {
        "ops": {
            PRELOAD_TASK_ID: {
                "config": {
                    "logical_datetime": "2026-07-18T00:00:00+00:00",
                    "dry_run": True,
                }
            }
        }
    }
    graph = GraphDefinition(name="preload_cloud_sensor_dry_graph", node_defs=[node])
    return graph.execute_in_process(run_config=dry_run_config)


def _execute_with_downstream(node):
    from dagster import DependencyDefinition, GraphDefinition, In, op

    downstream_calls = []

    @op(ins={"start": In(str)})
    def downstream_load(start):
        downstream_calls.append(start)
        return start

    graph = GraphDefinition(
        name="preload_cloud_sensor_downstream_graph",
        node_defs=[node, downstream_load],
        dependencies={
            "downstream_load": {
                "start": DependencyDefinition(PRELOAD_TASK_ID, "result")
            }
        },
    )
    return graph.execute_in_process(run_config=RUN_CONFIG), downstream_calls


def _step_failures(result):
    from dagster._core.events import DagsterEventType
    return [
        event
        for event in result.all_events
        if event.event_type == DagsterEventType.STEP_FAILURE
        and event.step_key == PRELOAD_TASK_ID
    ]


def _skipped_steps(result):
    from dagster._core.events import DagsterEventType
    return [
        event.step_key
        for event in result.all_events
        if event.event_type == DagsterEventType.STEP_SKIPPED
    ]


# ---------------------------------------------------------------------------
# 1. Shared resolver — strict parsing, unconditional pop, rejection removed
# ---------------------------------------------------------------------------

class TestResolvePreLoadPoke:

    def test_absent_kwargs_return_none(self):
        from ai.starlake.dagster import StarlakeDagsterJob

        kwargs = {"retries": 0}
        assert StarlakeDagsterJob._sl_resolve_pre_load_poke(kwargs) is None
        assert kwargs == {"retries": 0}

    def test_off_pops_all_four_kwargs(self):
        from ai.starlake.dagster import StarlakeDagsterJob

        kwargs = {
            "pre_load_sensor": False,
            "pre_load_poke_interval": 42,
            "pre_load_timeout": 120,
            "pre_load_sensor_soft_fail": False,
            "retries": 0,
        }
        assert StarlakeDagsterJob._sl_resolve_pre_load_poke(kwargs) is None
        assert kwargs == {"retries": 0}

    def test_on_returns_exact_values(self):
        from ai.starlake.dagster import StarlakeDagsterJob

        kwargs = {
            "pre_load_sensor": True,
            "pre_load_poke_interval": 42,
            "pre_load_timeout": 120,
            "pre_load_sensor_soft_fail": True,
        }
        poke = StarlakeDagsterJob._sl_resolve_pre_load_poke(kwargs)
        assert kwargs == {}
        assert poke.poke_interval == 42
        assert poke.timeout == 120
        assert poke.soft_fail is True

    def test_defaults_when_only_sensor_flag(self):
        from ai.starlake.dagster import StarlakeDagsterJob

        poke = StarlakeDagsterJob._sl_resolve_pre_load_poke(
            {"pre_load_sensor": True}
        )
        assert poke.poke_interval == 300
        assert poke.timeout == 3600
        assert poke.soft_fail is False

    @pytest.mark.parametrize(
        "kwargs, option",
        [
            ({"pre_load_sensor": "yes"}, "pre_load_sensor"),
            (
                {"pre_load_sensor": True, "pre_load_poke_interval": "abc"},
                "pre_load_poke_interval",
            ),
            (
                {"pre_load_sensor": True, "pre_load_poke_interval": "0"},
                "pre_load_poke_interval",
            ),
            (
                {"pre_load_sensor": True, "pre_load_poke_interval": "-5"},
                "pre_load_poke_interval",
            ),
            (
                {
                    "pre_load_sensor": True,
                    "pre_load_poke_interval": "300",
                    "pre_load_timeout": "60",
                },
                "pre_load_timeout",
            ),
            (
                {"pre_load_sensor": True, "pre_load_sensor_soft_fail": "maybe"},
                "pre_load_sensor_soft_fail",
            ),
        ],
    )
    def test_strict_validation_errors(self, kwargs, option):
        from ai.starlake.dagster import StarlakeDagsterJob

        with pytest.raises(ValueError) as exc_info:
            StarlakeDagsterJob._sl_resolve_pre_load_poke(dict(kwargs))
        assert option in str(exc_info.value)

    def test_invalid_value_still_pops_kwargs(self):
        from ai.starlake.dagster import StarlakeDagsterJob

        kwargs = {"pre_load_sensor": "yes", "pre_load_timeout": 120}
        with pytest.raises(ValueError):
            StarlakeDagsterJob._sl_resolve_pre_load_poke(kwargs)
        assert kwargs == {}

    def test_reject_helper_removed(self):
        from ai.starlake.dagster import StarlakeDagsterJob

        assert not hasattr(StarlakeDagsterJob, "_reject_pre_load_sensor_kwargs")


# ---------------------------------------------------------------------------
# 2. Shared loop — engine-agnostic unit tests (fake run_once / is_success)
# ---------------------------------------------------------------------------

class TestPreLoadPokeLoop:

    def _poke(self, soft_fail=False):
        from ai.starlake.dagster.starlake_dagster_job import PreLoadPoke
        return PreLoadPoke(poke_interval=42, timeout=120, soft_fail=soft_fail)

    def test_success_on_third_attempt(self, fake_clock, stub_context):
        from ai.starlake.dagster import StarlakeDagsterJob

        attempts = []

        def run_once():
            attempts.append(fake_clock.now)
            return ("out", 1 if len(attempts) < 3 else 0)

        result = StarlakeDagsterJob._sl_pre_load_poke_loop(
            stub_context, run_once, lambda r: not r[1], self._poke(), "starlake preload"
        )
        assert result == ("out", 0)
        assert len(attempts) == 3
        assert fake_clock.sleeps == [42, 42]

    def test_success_on_first_attempt_no_sleep(self, fake_clock, stub_context):
        from ai.starlake.dagster import StarlakeDagsterJob

        result = StarlakeDagsterJob._sl_pre_load_poke_loop(
            stub_context, lambda: ("ok", 0), lambda r: not r[1], self._poke(), "cmd"
        )
        assert result == ("ok", 0)
        assert fake_clock.sleeps == []

    def test_soft_fail_deadline_returns_none(self, fake_clock, stub_context):
        from ai.starlake.dagster import StarlakeDagsterJob

        attempts = []

        def run_once():
            attempts.append(fake_clock.now)
            return ("nothing", 1)

        result = StarlakeDagsterJob._sl_pre_load_poke_loop(
            stub_context,
            run_once,
            lambda r: not r[1],
            self._poke(soft_fail=True),
            "cmd",
        )
        assert result is None
        # pokes at t=+0/+42/+84; the next poke would land at +126 > 120
        assert len(attempts) == 3
        assert fake_clock.sleeps == [42, 42]

    def test_hard_deadline_raises_failure(self, fake_clock, stub_context):
        from dagster import Failure

        from ai.starlake.dagster import StarlakeDagsterJob

        with pytest.raises(Failure) as exc_info:
            StarlakeDagsterJob._sl_pre_load_poke_loop(
                stub_context,
                lambda: ("nothing", 1),
                lambda r: not r[1],
                self._poke(),
                "starlake preload --domain starbake",
            )
        assert "timed out waiting for files after 120s" in str(exc_info.value)
        assert "starlake preload --domain starbake" in str(exc_info.value)


# ---------------------------------------------------------------------------
# 3. Cloud Run — poke = full gcloud execution re-submission
# ---------------------------------------------------------------------------

class TestCloudRunPokeLoop:

    def _make_job(self, options: dict):
        from ai.starlake.dagster.gcp import StarlakeDagsterCloudRunJob
        return StarlakeDagsterCloudRunJob(
            filename="test_dagster_cloud_sensor.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={**CLOUD_RUN_OPTIONS, **options},
        )

    def _patch_execute(self, monkeypatch, return_codes):
        import ai.starlake.dagster.gcp.starlake_dagster_cloud_run_job as mod

        calls = []

        def fake_execute(shell_command, **kwargs):
            calls.append(shell_command)
            code = return_codes[min(len(calls), len(return_codes)) - 1]
            return ("out", code)

        monkeypatch.setattr(mod, "execute_shell_command", fake_execute)
        return calls

    def test_sensor_off_single_execution(self, monkeypatch):
        calls = self._patch_execute(monkeypatch, [0])
        node = _make_preload_node(self._make_job({"pre_load_strategy": "imported"}))
        result = _execute(node)
        assert result.success
        assert len(calls) == 1

    def test_pokes_until_success_resubmitting_same_command(
        self, monkeypatch, fake_clock
    ):
        calls = self._patch_execute(monkeypatch, [1, 1, 0])
        node = _make_preload_node(self._make_job(dict(SENSOR_OPTIONS)))
        result = _execute(node)

        assert result.success
        assert len(calls) == 3
        # each poke re-submits a gcloud execution of the same job
        assert all(
            "gcloud beta run jobs execute sl-preload-job" in call for call in calls
        )
        assert fake_clock.sleeps == [42, 42]
        assert result.output_for_node(PRELOAD_TASK_ID, "result") is not None

    def test_per_call_sensor_kwarg_enables_loop_without_option(
        self, monkeypatch, fake_clock
    ):
        from ai.starlake.job import StarlakePreLoadStrategy

        # pre_load_sensor option NOT set — the sl_pre_load(sensor=True) kwarg
        # alone must enable the poke loop (kwarg > option precedence in core)
        calls = self._patch_execute(monkeypatch, [1, 0])
        job = self._make_job(
            {
                "pre_load_strategy": "imported",
                "pre_load_poke_interval": "42",
                "pre_load_timeout": "120",
            }
        )
        node = job.sl_pre_load(
            domain="starbake",
            tables={"customers"},
            pre_load_strategy=StarlakePreLoadStrategy.IMPORTED,
            sensor=True,
        )
        result = _execute(node)
        assert result.success
        assert len(calls) == 2
        assert fake_clock.sleeps == [42]

    def test_dry_run_never_pokes(self, monkeypatch, fake_clock):
        calls = self._patch_execute(monkeypatch, [1])
        node = _make_preload_node(self._make_job(dict(SENSOR_OPTIONS)))
        result = _execute_dry_run(node)
        assert result.success
        assert calls == []
        assert fake_clock.sleeps == []

    def test_soft_fail_timeout_skips_downstream(self, monkeypatch, fake_clock):
        calls = self._patch_execute(monkeypatch, [1])
        node = _make_preload_node(
            self._make_job(dict(SENSOR_OPTIONS, pre_load_sensor_soft_fail="true"))
        )
        result, downstream_calls = _execute_with_downstream(node)

        assert result.success
        assert len(calls) == 3
        assert fake_clock.sleeps == [42, 42]
        assert downstream_calls == []
        assert "downstream_load" in _skipped_steps(result)

    def test_hard_timeout_raises_failure(self, monkeypatch, fake_clock):
        self._patch_execute(monkeypatch, [1])
        node = _make_preload_node(self._make_job(dict(SENSOR_OPTIONS)))
        result = _execute(node, raise_on_error=False)

        assert not result.success
        failures = _step_failures(result)
        assert len(failures) == 1
        assert "timed out" in str(failures[0].event_specific_data.error)

    def test_retry_policy_stays_none(self):
        node = _make_preload_node(self._make_job(dict(SENSOR_OPTIONS)))
        assert node.retry_policy is None


# ---------------------------------------------------------------------------
# 4. Fargate — poke = fresh task script generated, executed, unlinked
# ---------------------------------------------------------------------------

class TestFargatePokeLoop:

    def _make_job(self, options: dict):
        from ai.starlake.dagster.aws import StarlakeDagsterFargateJob
        return StarlakeDagsterFargateJob(
            filename="test_dagster_cloud_sensor.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={**FARGATE_OPTIONS, **options},
        )

    def _patch_fargate(self, monkeypatch, return_codes):
        import ai.starlake.dagster.aws.starlake_dagster_fargate_job as mod
        from ai.starlake.aws import StarlakeFargateHelper

        generated = []
        calls = []

        def fake_generate_script(self):
            fd, path = tempfile.mkstemp(suffix=".sh", prefix="sl_fargate_test_")
            os.close(fd)
            generated.append(path)
            return path

        def fake_execute(shell_script_path, **kwargs):
            calls.append(shell_script_path)
            code = return_codes[min(len(calls), len(return_codes)) - 1]
            return ("out", code)

        monkeypatch.setattr(
            StarlakeFargateHelper, "generate_script", fake_generate_script
        )
        monkeypatch.setattr(mod, "execute_shell_script", fake_execute)
        return generated, calls

    def test_sensor_off_single_execution(self, monkeypatch):
        generated, calls = self._patch_fargate(monkeypatch, [0])
        node = _make_preload_node(self._make_job({"pre_load_strategy": "imported"}))
        result = _execute(node)
        assert result.success
        assert len(calls) == 1
        assert len(generated) == 1
        assert not os.path.exists(generated[0])

    def test_pokes_until_success_with_fresh_script_each_time(
        self, monkeypatch, fake_clock
    ):
        generated, calls = self._patch_fargate(monkeypatch, [1, 1, 0])
        node = _make_preload_node(self._make_job(dict(SENSOR_OPTIONS)))
        result = _execute(node)

        assert result.success
        assert len(calls) == 3
        # one fresh script per poke, all cleaned up
        assert len(generated) == 3
        assert len(set(generated)) == 3
        assert all(not os.path.exists(path) for path in generated)
        assert fake_clock.sleeps == [42, 42]
        assert result.output_for_node(PRELOAD_TASK_ID, "result") is not None

    def test_soft_fail_timeout_skips_downstream(self, monkeypatch, fake_clock):
        generated, calls = self._patch_fargate(monkeypatch, [1])
        node = _make_preload_node(
            self._make_job(dict(SENSOR_OPTIONS, pre_load_sensor_soft_fail="true"))
        )
        result, downstream_calls = _execute_with_downstream(node)

        assert result.success
        assert len(calls) == 3
        assert all(not os.path.exists(path) for path in generated)
        assert downstream_calls == []
        assert "downstream_load" in _skipped_steps(result)

    def test_hard_timeout_raises_failure(self, monkeypatch, fake_clock):
        generated, _ = self._patch_fargate(monkeypatch, [1])
        node = _make_preload_node(self._make_job(dict(SENSOR_OPTIONS)))
        result = _execute(node, raise_on_error=False)

        assert not result.success
        failures = _step_failures(result)
        assert len(failures) == 1
        assert "timed out" in str(failures[0].event_specific_data.error)
        # scripts must not leak even on the failing path
        assert all(not os.path.exists(path) for path in generated)

    def test_dry_run_never_pokes_but_still_generates_script(
        self, monkeypatch, fake_clock
    ):
        # fargate deliberately keeps the pre-6.7 dry-run behavior: the task
        # script is still generated (and cleaned up), only execution is skipped
        generated, calls = self._patch_fargate(monkeypatch, [1])
        node = _make_preload_node(self._make_job(dict(SENSOR_OPTIONS)))
        result = _execute_dry_run(node)
        assert result.success
        assert calls == []
        assert len(generated) == 1
        assert not os.path.exists(generated[0])
        assert fake_clock.sleeps == []

    def test_retry_policy_stays_none(self):
        node = _make_preload_node(self._make_job(dict(SENSOR_OPTIONS)))
        assert node.retry_policy is None


# ---------------------------------------------------------------------------
# 5. Dataproc — poke = re-submission with a FRESH unique job id
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not DAGSTER_GCP_AVAILABLE,
    reason="Requires dagster-gcp (CI installs none — run in the local provider venv)",
)
class TestDataprocPokeLoop:

    def _make_job(self, options: dict):
        from ai.starlake.dagster.gcp import StarlakeDagsterDataprocJob
        return StarlakeDagsterDataprocJob(
            filename="test_dagster_cloud_sensor.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={**DATAPROC_OPTIONS, **options},
        )

    def _patch_poke_client(self, monkeypatch, states):
        """Poke-mode seam: submit_job returns a NON-terminal submission
        response (PENDING, as the real dagster-gcp client does); the terminal
        state comes from wait_for_job + get_job — a non-DONE attempt raises
        DataprocError from wait_for_job."""
        import copy

        from dagster_gcp.dataproc.types import DataprocError

        from ai.starlake.dagster.gcp import StarlakeDagsterDataprocJob

        submitted = []
        waited = []

        class FakeClient:
            def submit_job(self, job_details):
                submitted.append(copy.deepcopy(job_details))
                return {"status": {"state": "PENDING"}}

            def wait_for_job(self, job_id, wait_timeout=None):
                waited.append((job_id, wait_timeout))
                state = states[min(len(submitted), len(states)) - 1]
                if state != "DONE":
                    raise DataprocError(f"Job error: {state}")

            def get_job(self, job_id):
                return {"status": {"state": "DONE"}}

        monkeypatch.setattr(
            StarlakeDagsterDataprocJob, "__client__", lambda self: FakeClient()
        )
        return submitted, waited

    @staticmethod
    def _job_ids(submitted):
        return [
            details["job"]["reference"]["job_id"] for details in submitted
        ]

    def test_sensor_off_single_submission(self, monkeypatch):
        # since story 6.8 (issue #109) the off path ALSO polls to the
        # terminal state — full off-path coverage lives in
        # test_dagster_dataproc_terminal_state.py
        submitted, waited = self._patch_poke_client(monkeypatch, ["DONE"])
        node = _make_preload_node(self._make_job({"pre_load_strategy": "imported"}))
        result = _execute(node)
        assert result.success
        assert len(submitted) == 1
        assert len(waited) == 1
        assert result.output_for_node(PRELOAD_TASK_ID, "result") == self._job_ids(
            submitted
        )[0]

    def test_pokes_until_success_with_fresh_job_id_each_time(
        self, monkeypatch, fake_clock
    ):
        submitted, waited = self._patch_poke_client(
            monkeypatch, ["ERROR", "ERROR", "DONE"]
        )
        node = _make_preload_node(self._make_job(dict(SENSOR_OPTIONS)))
        result = _execute(node)

        assert result.success
        assert len(submitted) == 3
        job_ids = self._job_ids(submitted)
        # a Dataproc job id is unique per project: every poke must re-submit
        # with a fresh id
        assert len(set(job_ids)) == 3
        assert all(job_id.startswith(f"{PRELOAD_TASK_ID}_") for job_id in job_ids)
        # every submission is polled to its TERMINAL state (the submission
        # response is PENDING, never DONE), with the wait bounded by the
        # pre_load_timeout window
        assert [job_id for job_id, _ in waited] == job_ids
        assert all(wait_timeout == 120 for _, wait_timeout in waited)
        assert fake_clock.sleeps == [42, 42]
        # the yielded Output carries the SUCCESSFUL attempt's job id
        assert result.output_for_node(PRELOAD_TASK_ID, "result") == job_ids[2]

    def test_soft_fail_timeout_skips_downstream(self, monkeypatch, fake_clock):
        submitted, _ = self._patch_poke_client(monkeypatch, ["ERROR"])
        node = _make_preload_node(
            self._make_job(dict(SENSOR_OPTIONS, pre_load_sensor_soft_fail="true"))
        )
        result, downstream_calls = _execute_with_downstream(node)

        assert result.success
        assert len(submitted) == 3
        assert downstream_calls == []
        assert "downstream_load" in _skipped_steps(result)

    def test_hard_timeout_raises_failure(self, monkeypatch, fake_clock):
        self._patch_poke_client(monkeypatch, ["ERROR"])
        node = _make_preload_node(self._make_job(dict(SENSOR_OPTIONS)))
        result = _execute(node, raise_on_error=False)

        assert not result.success
        failures = _step_failures(result)
        assert len(failures) == 1
        assert "timed out" in str(failures[0].event_specific_data.error)

    def test_dry_run_never_pokes(self, monkeypatch, fake_clock):
        submitted, waited = self._patch_poke_client(monkeypatch, ["ERROR"])
        node = _make_preload_node(self._make_job(dict(SENSOR_OPTIONS)))
        result = _execute_dry_run(node)
        assert result.success
        assert submitted == []
        assert waited == []
        assert fake_clock.sleeps == []

    def test_retry_policy_stays_none(self):
        node = _make_preload_node(self._make_job(dict(SENSOR_OPTIONS)))
        assert node.retry_policy is None


# ---------------------------------------------------------------------------
# 6. gcp package importability without dagster-gcp (issue #108)
# ---------------------------------------------------------------------------

class TestGcpPackageImport:

    def test_cloud_run_importable_and_star_import_safe(self):
        import ai.starlake.dagster.gcp as gcp_pkg
        assert hasattr(gcp_pkg, "StarlakeDagsterCloudRunJob")
        # star-import must not trigger the dataproc module when dagster-gcp
        # is absent — __all__ only advertises it when the import succeeded
        if DAGSTER_GCP_AVAILABLE:
            assert "starlake_dagster_dataproc_job" in gcp_pkg.__all__
        else:
            assert gcp_pkg.__all__ == ["starlake_dagster_cloud_run_job"]

    @pytest.mark.skipif(
        DAGSTER_GCP_AVAILABLE,
        reason="Placeholder only exists when dagster-gcp is absent",
    )
    def test_dataproc_placeholder_raises_informative_error(self):
        from ai.starlake.dagster.gcp import StarlakeDagsterDataprocJob

        with pytest.raises(ModuleNotFoundError) as exc_info:
            StarlakeDagsterDataprocJob(
                filename="x.py", module_name=_DAGSTER_TEST_MODULE_NAME
            )
        assert "dagster-gcp" in str(exc_info.value)
