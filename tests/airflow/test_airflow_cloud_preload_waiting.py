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

"""Story 6.5 (issue #93) — pre-load waiting on the Airflow cloud engines.

``pre_load_sensor=true`` is now HONORED (not rejected) on cloud_run, dataproc
and fargate: the engine builds either a deferrable operator (retries/retry_delay
re-submit preload = poke) when the operator supports ``deferrable`` and
``pre_load_deferrable`` is enabled, or a reschedule-mode sensor-flavor that
submits one preload run per poke otherwise. Both paths preserve the
``skip_or_start`` XCom composition; neither routes through the #92 one-shot
swallow.

Two layers:

- provider-free contract tests (run in CI, which installs NO google/amazon
  providers): capability detection, mode selection, option→retry mapping,
  wait-config resolution, the poke/deferrable verdict helpers, and the generic
  ``StarlakeCloudPreloadSensor`` poke — all with fakes;
- provider-guarded tests (skipped without the amazon/google providers): the
  real per-engine construction (deferrable vs sensor), the deferrable resume
  verdict, and the ``skip_or_start`` wiring.
"""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from tests.airflow.conftest import _AIRFLOW_TEST_MODULE_NAME, AIRFLOW_AVAILABLE

pytestmark = pytest.mark.skipif(
    not AIRFLOW_AVAILABLE,
    reason="Requires Apache Airflow",
)

try:
    import airflow.providers.amazon.aws.operators.ecs  # noqa: F401
    AMAZON_AVAILABLE = True
except Exception:
    AMAZON_AVAILABLE = False

try:
    import airflow.providers.google.cloud.operators.cloud_run  # noqa: F401
    import google.cloud.run_v2  # noqa: F401
    GOOGLE_AVAILABLE = True
except Exception:
    GOOGLE_AVAILABLE = False

amazon_only = pytest.mark.skipif(
    not AMAZON_AVAILABLE, reason="Requires apache-airflow-providers-amazon"
)
google_only = pytest.mark.skipif(
    not GOOGLE_AVAILABLE, reason="Requires apache-airflow-providers-google"
)

CLOUD_RUN_OPTIONS = {
    "cloud_run_job_name": "test-job",
    "cloud_run_project_id": "test-project",
    "cloud_run_job_region": "europe-west1",
    "pre_load_strategy": "imported",
}

FARGATE_OPTIONS = {
    "aws_cluster_name": "test-cluster",
    "aws_task_definition_name": "test-task-def",
    "aws_task_definition_container_name": "test-container",
    "pre_load_strategy": "imported",
}

# the four core-injected sensor kwargs + strategy; poke_interval/timeout picked
# so the deferrable mapping is a non-trivial retries = 120 // 30 = 4
SENSOR_OPTIONS = {
    "pre_load_sensor": "true",
    "pre_load_poke_interval": "30",
    "pre_load_timeout": "120",
    "pre_load_sensor_soft_fail": "true",
}


def _dag():
    from airflow import DAG
    from datetime import datetime
    return DAG(dag_id="test_cloud_preload_waiting", start_date=datetime(2024, 1, 1), schedule=None)


# ---------------------------------------------------------------------------
# fakes for the provider-free decision logic
# ---------------------------------------------------------------------------

class _FakeDeferrable:
    def __init__(self, task_id, deferrable=False, **kwargs):
        pass


class _FakeNonDeferrable:
    def __init__(self, task_id, **kwargs):
        pass


# ---------------------------------------------------------------------------
# 1. Provider-free — capability detection
# ---------------------------------------------------------------------------

class TestDeferrableCapabilityDetection:

    def test_class_with_deferrable_param_is_supported(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        assert StarlakeAirflowJob._sl_operator_supports_deferrable(_FakeDeferrable) is True

    def test_class_without_deferrable_param_is_unsupported(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        assert StarlakeAirflowJob._sl_operator_supports_deferrable(_FakeNonDeferrable) is False

    def test_unintrospectable_never_raises(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        # a builtin whose __init__ signature cannot be introspected → False
        assert StarlakeAirflowJob._sl_operator_supports_deferrable(object) is False


# ---------------------------------------------------------------------------
# 2. Provider-free — mode selection truth table
# ---------------------------------------------------------------------------

class TestWaitModeSelection:

    @pytest.mark.parametrize(
        "supports, enabled, expected",
        [
            (True, True, "deferrable"),
            (True, False, "sensor"),
            (False, True, "sensor"),
            (False, False, "sensor"),
        ],
    )
    def test_truth_table(self, supports, enabled, expected):
        from ai.starlake.airflow import StarlakeAirflowJob
        assert StarlakeAirflowJob._sl_select_pre_load_wait_mode(supports, enabled) == expected


# ---------------------------------------------------------------------------
# 3. Provider-free — deferrable retry-param mapping
# ---------------------------------------------------------------------------

class TestDeferrableRetryParams:

    @pytest.mark.parametrize(
        "poke_interval, timeout, retries",
        [
            (300, 3600, 12),   # 3600 // 300
            (300, 300, 1),     # equal → floor of 1
            (300, 301, 1),     # sub-interval remainder floored
            (60, 3600, 60),
            (30, 120, 4),
        ],
    )
    def test_mapping(self, poke_interval, timeout, retries):
        from ai.starlake.airflow import StarlakeAirflowJob
        got_retries, got_delay = StarlakeAirflowJob._sl_deferrable_retry_params(poke_interval, timeout)
        assert got_retries == retries
        assert got_delay == timedelta(seconds=poke_interval)

    def test_zero_poke_interval_does_not_crash(self):
        """Defensive: a 0 interval reaching the helper is floored to 1 instead
        of a ZeroDivisionError crashing DAG parsing."""
        from ai.starlake.airflow import StarlakeAirflowJob
        retries, delay = StarlakeAirflowJob._sl_deferrable_retry_params(0, 120)
        assert retries == 120
        assert delay == timedelta(seconds=1)


# ---------------------------------------------------------------------------
# 4. Provider-free — wait-config resolution
# ---------------------------------------------------------------------------

class TestResolveCloudPreLoadWait:

    def test_off_returns_none_and_pops_kwargs(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        kwargs = {"pool": "default_pool"}  # no pre_load_sensor
        wait = StarlakeAirflowJob._sl_resolve_cloud_pre_load_wait(kwargs, {}, _FakeDeferrable)
        assert wait is None
        assert kwargs == {"pool": "default_pool"}

    def test_off_with_stray_kwargs_pops_them_all(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        kwargs = {
            "pre_load_sensor": False,
            "pre_load_poke_interval": 42,
            "pre_load_timeout": 120,
            "pre_load_sensor_soft_fail": False,
            "pool": "default_pool",
        }
        wait = StarlakeAirflowJob._sl_resolve_cloud_pre_load_wait(kwargs, {}, _FakeDeferrable)
        assert wait is None
        assert kwargs == {"pool": "default_pool"}

    def test_on_deferrable_by_default(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        kwargs = {
            "pre_load_sensor": True,
            "pre_load_poke_interval": 30,
            "pre_load_timeout": 120,
            "pre_load_sensor_soft_fail": True,
        }
        wait = StarlakeAirflowJob._sl_resolve_cloud_pre_load_wait(kwargs, {}, _FakeDeferrable)
        assert wait.mode == "deferrable"
        assert wait.poke_interval == 30
        assert wait.timeout == 120
        assert wait.soft_fail is True
        assert wait.retries == 4
        assert wait.retry_delay == timedelta(seconds=30)
        # kwargs fully consumed
        assert kwargs == {}

    def test_on_opt_out_forces_sensor(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        kwargs = {"pre_load_sensor": True, "pre_load_poke_interval": 30, "pre_load_timeout": 120}
        wait = StarlakeAirflowJob._sl_resolve_cloud_pre_load_wait(
            kwargs, {"pre_load_deferrable": "false"}, _FakeDeferrable
        )
        assert wait.mode == "sensor"

    def test_on_non_deferrable_operator_selects_sensor(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        kwargs = {"pre_load_sensor": True}
        wait = StarlakeAirflowJob._sl_resolve_cloud_pre_load_wait(kwargs, {}, _FakeNonDeferrable)
        assert wait.mode == "sensor"

    def test_on_operator_cls_none_selects_sensor(self):
        """The gcloud path has no deferrable operator → operator_cls=None."""
        from ai.starlake.airflow import StarlakeAirflowJob
        kwargs = {"pre_load_sensor": True}
        wait = StarlakeAirflowJob._sl_resolve_cloud_pre_load_wait(kwargs, {}, None)
        assert wait.mode == "sensor"

    def test_invalid_deferrable_option_raises(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        kwargs = {"pre_load_sensor": True}
        with pytest.raises(ValueError) as exc:
            StarlakeAirflowJob._sl_resolve_cloud_pre_load_wait(
                kwargs, {"pre_load_deferrable": "yes"}, _FakeDeferrable
            )
        assert "pre_load_deferrable" in str(exc.value)

    def test_pre_load_deferrable_kwarg_popped_and_honored(self):
        """A per-call pre_load_deferrable kwarg is popped (never leaks into the
        operator ctor) and forces the sensor even for a deferrable operator."""
        from ai.starlake.airflow import StarlakeAirflowJob
        kwargs = {"pre_load_sensor": True, "pre_load_deferrable": False}
        wait = StarlakeAirflowJob._sl_resolve_cloud_pre_load_wait(kwargs, {}, _FakeDeferrable)
        assert wait.mode == "sensor"
        assert "pre_load_deferrable" not in kwargs

    def test_pre_load_deferrable_kwarg_wins_over_option(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        kwargs = {"pre_load_sensor": True, "pre_load_deferrable": True}
        wait = StarlakeAirflowJob._sl_resolve_cloud_pre_load_wait(
            kwargs, {"pre_load_deferrable": "false"}, _FakeDeferrable
        )
        assert wait.mode == "deferrable"

    def test_pre_load_deferrable_kwarg_popped_even_when_sensor_off(self):
        """Off + a stray pre_load_deferrable kwarg → None, kwarg still popped
        (else it reaches EcsRunTaskOperator(**kwargs) as an unexpected kwarg)."""
        from ai.starlake.airflow import StarlakeAirflowJob
        kwargs = {"pre_load_deferrable": False, "pool": "default_pool"}
        wait = StarlakeAirflowJob._sl_resolve_cloud_pre_load_wait(kwargs, {}, _FakeDeferrable)
        assert wait is None
        assert kwargs == {"pool": "default_pool"}


# ---------------------------------------------------------------------------
# 5. Provider-free — verdict helpers (distinct from the #92 swallow)
# ---------------------------------------------------------------------------

class TestPokeVerdict:

    def test_success(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        verdict = StarlakeAirflowJob._sl_pre_load_poke_verdict(True)
        assert verdict.is_done is True
        assert verdict.xcom_value is True

    def test_no_files_pokes_again(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        assert StarlakeAirflowJob._sl_pre_load_poke_verdict(False) is None


class TestDeferrableVerdict:

    def test_success_returns_true(self):
        from ai.starlake.airflow import StarlakeAirflowJob
        assert StarlakeAirflowJob._sl_deferrable_pre_load_verdict(True, False, False, "msg") is True

    def test_non_last_failure_retries(self):
        from airflow.exceptions import AirflowException
        from ai.starlake.airflow import StarlakeAirflowJob
        with pytest.raises(AirflowException, match="msg"):
            StarlakeAirflowJob._sl_deferrable_pre_load_verdict(False, False, True, "msg")

    def test_last_failure_soft_fail_skips(self):
        from ai.starlake.airflow.compat import AirflowSkipException
        from ai.starlake.airflow import StarlakeAirflowJob
        with pytest.raises(AirflowSkipException, match="msg"):
            StarlakeAirflowJob._sl_deferrable_pre_load_verdict(False, True, True, "msg")

    def test_last_failure_hard_raises(self):
        from airflow.exceptions import AirflowException
        from ai.starlake.airflow.compat import AirflowSkipException
        from ai.starlake.airflow import StarlakeAirflowJob
        with pytest.raises(AirflowException) as exc:
            StarlakeAirflowJob._sl_deferrable_pre_load_verdict(False, True, False, "msg")
        # a hard failure is NOT a skip
        assert not isinstance(exc.value, AirflowSkipException)


class TestIsLastAttempt:

    @pytest.mark.parametrize(
        "try_number, max_tries, expected",
        [(1, 1, False), (2, 1, True), (1, 0, True), (5, 4, True), (4, 4, False)],
    )
    def test_boundary(self, try_number, max_tries, expected):
        from ai.starlake.airflow import StarlakeAirflowJob
        assert StarlakeAirflowJob._sl_is_last_attempt(try_number, max_tries) is expected


# ---------------------------------------------------------------------------
# 6. Provider-free — the generic sensor-flavor poke
# ---------------------------------------------------------------------------

class TestCloudPreloadSensorPoke:

    def _make_sensor(self, submit_and_wait):
        from ai.starlake.airflow import StarlakeCloudPreloadSensor
        with _dag():
            return StarlakeCloudPreloadSensor(
                task_id="check_starbake_incoming_files",
                dataset=None,
                source=None,
                submit_and_wait=submit_and_wait,
                poke_interval=30,
                timeout=120,
                soft_fail=True,
            )

    def test_defaults_to_reschedule_mode(self):
        sensor = self._make_sensor(lambda ctx: True)
        assert sensor.mode == "reschedule"
        assert sensor.poke_interval == 30
        assert sensor.timeout == 120
        assert sensor.soft_fail is True

    def test_success_completes_truthy(self):
        sensor = self._make_sensor(lambda ctx: True)
        verdict = sensor.poke(context={})
        assert verdict.is_done is True
        assert verdict.xcom_value is True

    def test_no_files_pokes_again(self):
        sensor = self._make_sensor(lambda ctx: False)
        assert sensor.poke(context={}) is None

    def test_submission_error_pokes_again(self):
        def boom(ctx):
            raise RuntimeError("transient submit error")
        sensor = self._make_sensor(boom)
        assert sensor.poke(context={}) is None


# ---------------------------------------------------------------------------
# 7. Fargate — construction (provider-guarded)
# ---------------------------------------------------------------------------

@amazon_only
class TestFargatePreloadWaiting:

    def _make_job(self, extra_options=None):
        from ai.starlake.airflow.aws import StarlakeAirflowFargateJob
        options = dict(FARGATE_OPTIONS)
        options.update(extra_options or {})
        return StarlakeAirflowFargateJob(
            filename="test_cloud_preload_waiting.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options=options,
        )

    def test_sensor_off_stays_one_shot(self):
        """Regression: no pre_load_sensor → the ordinary (non-sensor) task."""
        from ai.starlake.airflow import StarlakeCloudPreloadSensor
        job = self._make_job({"fargate_async": "false"})
        with _dag():
            task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert not isinstance(task, StarlakeCloudPreloadSensor)
        assert getattr(task, "deferrable", False) is False

    def test_deferrable_mode_builds_single_deferrable_task(self):
        from ai.starlake.airflow.aws.starlake_airflow_fargate_job import FargateTaskOperator
        job = self._make_job(dict(SENSOR_OPTIONS))
        with _dag():
            task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert isinstance(task, FargateTaskOperator)
        assert task.deferrable is True
        assert task.wait_for_completion is True
        assert task.preload is True
        assert task.pre_load_wait is not None
        # options mapped onto retry semantics: 120 // 30 = 4
        assert task.retries == 4
        assert task.retry_delay == timedelta(seconds=30)

    def test_sensor_mode_builds_preload_sensor(self):
        from ai.starlake.airflow import StarlakeCloudPreloadSensor
        job = self._make_job(dict(SENSOR_OPTIONS, pre_load_deferrable="false"))
        with _dag():
            task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert isinstance(task, StarlakeCloudPreloadSensor)
        assert task.poke_interval == 30
        assert task.timeout == 120
        assert task.soft_fail is True
        assert task.mode == "reschedule"
        assert task.retries == 0

    def test_deferrable_resume_success_returns_true(self, monkeypatch):
        from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
        from ai.starlake.airflow.aws.starlake_airflow_fargate_job import FargateTaskOperator
        from ai.starlake.airflow import PreLoadWait
        monkeypatch.setattr(EcsRunTaskOperator, "execute_complete", lambda self, context, event=None: "log line")
        with _dag():
            op = FargateTaskOperator(
                task_id="fargate_preload", dataset=None, source=None,
                task_definition="d", cluster="c", overrides={},
                wait_for_completion=True, deferrable=True, preload=True,
                pre_load_wait=PreLoadWait("deferrable", 30, 120, False, 4, timedelta(seconds=30)),
            )
        assert op.execute_complete(context={}, event={}) is True

    def test_deferrable_resume_non_last_failure_reraises(self, monkeypatch):
        from airflow.exceptions import AirflowException
        from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
        from ai.starlake.airflow.aws.starlake_airflow_fargate_job import FargateTaskOperator
        from ai.starlake.airflow import PreLoadWait

        def boom(self, context, event=None):
            raise AirflowException("no files")
        monkeypatch.setattr(EcsRunTaskOperator, "execute_complete", boom)
        with _dag():
            op = FargateTaskOperator(
                task_id="fargate_preload", dataset=None, source=None,
                task_definition="d", cluster="c", overrides={},
                wait_for_completion=True, deferrable=True, preload=True,
                pre_load_wait=PreLoadWait("deferrable", 30, 120, True, 4, timedelta(seconds=30)),
            )
        ti = MagicMock(try_number=1, max_tries=4)  # not last
        with pytest.raises(AirflowException):
            op.execute_complete(context={"ti": ti}, event={})

    def test_deferrable_resume_last_soft_fail_skips(self, monkeypatch):
        from airflow.exceptions import AirflowException
        from ai.starlake.airflow.compat import AirflowSkipException
        from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
        from ai.starlake.airflow.aws.starlake_airflow_fargate_job import FargateTaskOperator
        from ai.starlake.airflow import PreLoadWait

        def boom(self, context, event=None):
            raise AirflowException("no files")
        monkeypatch.setattr(EcsRunTaskOperator, "execute_complete", boom)
        with _dag():
            op = FargateTaskOperator(
                task_id="fargate_preload", dataset=None, source=None,
                task_definition="d", cluster="c", overrides={},
                wait_for_completion=True, deferrable=True, preload=True,
                pre_load_wait=PreLoadWait("deferrable", 30, 120, True, 4, timedelta(seconds=30)),
            )
        ti = MagicMock(try_number=5, max_tries=4)  # last attempt
        with pytest.raises(AirflowSkipException):
            op.execute_complete(context={"ti": ti}, event={})

    def test_skip_or_start_composition_in_sensor_mode(self):
        """The waiting task still wires pre_load >> skip_or_start >> import."""
        from ai.starlake.airflow import AirflowOrchestration
        from ai.starlake.orchestration import StarlakeSchedule
        orchestration = AirflowOrchestration(
            job=self._make_job(dict(SENSOR_OPTIONS, pre_load_deferrable="false"))
        )
        schedule = StarlakeSchedule(name=None, cron="0 0 * * *", domains=[])
        with orchestration:
            pipeline = orchestration.sl_create_pipeline(schedule=schedule)
            with pipeline:
                pre_load = pipeline.sl_pre_load(domain="starbake", tables={"customers"})
                skip_or_start = pipeline.skip_or_start(
                    task_id="skip_or_start_loading_starbake", upstream_task=pre_load
                )
                sl_import = pipeline.sl_import(
                    task_id="import_starbake", domain="starbake", tables={"customers"}
                )
                pre_load >> skip_or_start
                skip_or_start >> sl_import
        dag = pipeline.dag
        check_task = dag.get_task("check_starbake_incoming_files")
        assert "skip_or_start_loading_starbake" in check_task.downstream_task_ids

    def test_sensor_poke_submits_ecs_run_and_interprets(self, monkeypatch):
        """Exercise the sensor's submit_and_wait closure: a fresh ECS run per
        poke → success completes truthy, a failure (no files) pokes again."""
        from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
        job = self._make_job(dict(SENSOR_OPTIONS, pre_load_deferrable="false"))
        with _dag():
            sensor = job.sl_pre_load(domain="starbake", tables={"customers"})
        monkeypatch.setattr(EcsRunTaskOperator, "execute", lambda self, context: None)
        verdict = sensor.poke(context={})
        assert verdict.is_done is True and verdict.xcom_value is True

        def boom(self, context):
            raise RuntimeError("ECS task exited non-zero")
        monkeypatch.setattr(EcsRunTaskOperator, "execute", boom)
        assert sensor.poke(context={}) is None


# ---------------------------------------------------------------------------
# 8. Cloud Run — construction (provider-guarded)
# ---------------------------------------------------------------------------

@google_only
class TestCloudRunPreloadWaiting:

    def _make_job(self, extra_options=None):
        from ai.starlake.airflow.gcp import StarlakeAirflowCloudRunJob
        options = dict(CLOUD_RUN_OPTIONS)
        options.update(extra_options or {})
        return StarlakeAirflowCloudRunJob(
            filename="test_cloud_preload_waiting.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options=options,
        )

    def test_python_deferrable_mode(self):
        from ai.starlake.airflow.gcp.starlake_airflow_cloud_run_job import CloudRunJobOperator
        job = self._make_job(dict(SENSOR_OPTIONS, use_gcloud="false"))
        with _dag():
            task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert isinstance(task, CloudRunJobOperator)
        assert task.deferrable is True
        assert task.preload is True
        assert task.pre_load_wait is not None
        assert task.retries == 4
        assert task.retry_delay == timedelta(seconds=30)

    def test_python_sensor_mode(self):
        from ai.starlake.airflow import StarlakeCloudPreloadSensor
        job = self._make_job(dict(SENSOR_OPTIONS, use_gcloud="false", pre_load_deferrable="false"))
        with _dag():
            task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert isinstance(task, StarlakeCloudPreloadSensor)
        assert task.poke_interval == 30
        assert task.timeout == 120
        assert task.soft_fail is True
        assert task.mode == "reschedule"

    def test_gcloud_path_always_sensor(self):
        """gcloud has no deferrable operator — even with pre_load_deferrable
        default true, the gcloud path builds a reschedule BashSensor poking
        `--wait` (the raw command; no echo-wrapper)."""
        from ai.starlake.airflow.bash import StarlakePreloadBashSensor
        job = self._make_job(dict(SENSOR_OPTIONS, use_gcloud="true"))
        with _dag():
            task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert isinstance(task, StarlakePreloadBashSensor)
        assert task.mode == "reschedule"
        assert task.poke_interval == 30
        assert task.timeout == 120
        assert task.soft_fail is True
        cmd = task.bash_command
        assert "gcloud beta run jobs execute" in cmd
        assert "--wait" in cmd
        # raw command — the true exit code drives the poke (no echo-wrapper)
        assert "return_code=$?" not in cmd

    def test_deferrable_resume_last_hard_fail_raises(self, monkeypatch):
        from airflow.exceptions import AirflowException
        from ai.starlake.airflow.compat import AirflowSkipException
        from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
        from ai.starlake.airflow.gcp.starlake_airflow_cloud_run_job import CloudRunJobOperator, CloudRunMode
        from ai.starlake.airflow import PreLoadWait

        def boom(self, context, event=None):
            raise AirflowException("no files")
        monkeypatch.setattr(CloudRunExecuteJobOperator, "execute_complete", boom)
        with _dag():
            op = CloudRunJobOperator(
                task_id="cloud_run_preload", dataset=None, source=None,
                project_id="p", region="r", job_name="j", overrides={},
                mode=CloudRunMode.SYNC, preload=True,
                pre_load_wait=PreLoadWait("deferrable", 30, 120, False, 4, timedelta(seconds=30)),
            )
        ti = MagicMock(try_number=5, max_tries=4)  # last attempt, hard fail
        with pytest.raises(AirflowException) as exc:
            op.execute_complete(context={"ti": ti}, event={})
        assert not isinstance(exc.value, AirflowSkipException)

    def test_python_sensor_poke_submits_execution_and_interprets(self, monkeypatch):
        from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
        job = self._make_job(dict(SENSOR_OPTIONS, use_gcloud="false", pre_load_deferrable="false"))
        with _dag():
            sensor = job.sl_pre_load(domain="starbake", tables={"customers"})
        monkeypatch.setattr(CloudRunExecuteJobOperator, "execute", lambda self, context: {"ok": True})
        verdict = sensor.poke(context={})
        assert verdict.is_done is True and verdict.xcom_value is True

        def boom(self, context):
            raise RuntimeError("execution failed")
        monkeypatch.setattr(CloudRunExecuteJobOperator, "execute", boom)
        assert sensor.poke(context={}) is None


# ---------------------------------------------------------------------------
# 9b. Dataproc — sl_job wiring via a minimal cluster (provider-guarded)
# ---------------------------------------------------------------------------

@google_only
class TestDataprocPreloadWaitingSlJob:

    def _make_cluster(self):
        from ai.starlake.airflow.gcp.starlake_airflow_dataproc_job import (
            StarlakeAirflowDataprocCluster, StarlakeAirflowDataprocClusterConfig,
        )
        opts = {
            "dataproc_project_id": "p",
            "dataproc_region": "europe-west1",
            "spark_jar_list": "gs://b/x.jar",
            "spark_bucket": "b",
            "spark_job_main_class": "ai.starlake.job.Main",
        }
        cfg = StarlakeAirflowDataprocClusterConfig(
            cluster_id="c", dataproc_name="c", master_config=None, worker_config=None,
            secondary_worker_config=None, idle_delete_ttl=None, single_node=None, options=opts,
        )
        return StarlakeAirflowDataprocCluster(cluster_config=cfg, options=opts, pool="default_pool")

    def _wait(self, mode, **kw):
        from ai.starlake.airflow import PreLoadWait
        defaults = dict(mode=mode, poke_interval=30, timeout=120, soft_fail=True, retries=4, retry_delay=timedelta(seconds=30))
        defaults.update(kw)
        return PreLoadWait(**defaults)

    def _submit(self, cluster, wait):
        from ai.starlake.job import TaskType
        with _dag():
            return cluster.submit_starlake_job(
                task_id="check_x", arguments=["preload"], source=None,
                task_type=TaskType.PRELOAD, pre_load_wait=wait,
            )

    def test_deferrable_job_id_templated_per_attempt(self):
        from ai.starlake.airflow.gcp.starlake_airflow_dataproc_job import DataprocJobOperator
        task = self._submit(self._make_cluster(), self._wait("deferrable"))
        assert isinstance(task, DataprocJobOperator)
        assert task.deferrable is True
        assert task.preload is True
        assert task.retries == 4
        # a fresh job_id per retry — Jinja keyed on the run + attempt (job is a
        # template field, re-rendered each try) so re-submission never collides
        job_id = task.job["reference"]["job_id"]
        assert "{{ ts_nodash }}" in job_id
        assert "{{ ti.try_number }}" in job_id

    def test_sensor_poke_mints_fresh_job_id_each_poke(self, monkeypatch):
        from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
        from ai.starlake.airflow import StarlakeCloudPreloadSensor
        sensor = self._submit(self._make_cluster(), self._wait("sensor", retries=0))
        assert isinstance(sensor, StarlakeCloudPreloadSensor)
        seen = []

        def capture(self, context):
            seen.append(self.job["reference"]["job_id"])
            return "job-id"
        monkeypatch.setattr(DataprocSubmitJobOperator, "execute", capture)
        sensor.poke(context={})
        sensor.poke(context={})
        assert len(seen) == 2
        assert seen[0] != seen[1]  # unique dataproc job_id per poke (no AlreadyExists)


# ---------------------------------------------------------------------------
# 9. Dataproc — operator-level (provider-guarded; no sl_job cluster harness)
# ---------------------------------------------------------------------------

@google_only
class TestDataprocPreloadWaitingOperator:

    def _make_operator(self, *, preload, pre_load_wait):
        from ai.starlake.airflow.gcp.starlake_airflow_dataproc_job import DataprocJobOperator
        with _dag():
            return DataprocJobOperator(
                task_id="dataproc_preload", dataset=None, source=None,
                project_id="p", region="r",
                job={"reference": {"project_id": "p", "job_id": "j"}, "placement": {"cluster_name": "c"}, "spark_job": {}},
                preload=preload, pre_load_wait=pre_load_wait,
            )

    def test_deferrable_resume_success_returns_true(self, monkeypatch):
        from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
        from ai.starlake.airflow import PreLoadWait
        monkeypatch.setattr(DataprocSubmitJobOperator, "execute_complete", lambda self, context, event=None: "job-id")
        op = self._make_operator(preload=True, pre_load_wait=PreLoadWait("deferrable", 30, 120, False, 4, timedelta(seconds=30)))
        assert op.execute_complete(context={}, event={}) is True

    def test_deferrable_resume_last_soft_fail_skips(self, monkeypatch):
        from airflow.exceptions import AirflowException
        from ai.starlake.airflow.compat import AirflowSkipException
        from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
        from ai.starlake.airflow import PreLoadWait

        def boom(self, context, event=None):
            raise AirflowException("job failed")
        monkeypatch.setattr(DataprocSubmitJobOperator, "execute_complete", boom)
        op = self._make_operator(preload=True, pre_load_wait=PreLoadWait("deferrable", 30, 120, True, 4, timedelta(seconds=30)))
        ti = MagicMock(try_number=5, max_tries=4)
        with pytest.raises(AirflowSkipException):
            op.execute_complete(context={"ti": ti}, event={})

    def test_non_preload_execute_complete_delegates(self, monkeypatch):
        """A non-preload deferrable dataproc job keeps the provider's own
        execute_complete (returns the job id, no verdict override)."""
        from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
        monkeypatch.setattr(DataprocSubmitJobOperator, "execute_complete", lambda self, context, event=None: "job-id")
        op = self._make_operator(preload=False, pre_load_wait=None)
        assert op.execute_complete(context={}, event={}) == "job-id"
