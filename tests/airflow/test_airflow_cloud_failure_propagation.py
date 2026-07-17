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

"""Story 6.3 (issue #92) — cloud engines must fail the task chain on a failed job.

Contract: a failed Starlake job fails the Airflow task for every task type
except PRELOAD (the one type designed around the ``skip_or_start`` XCom
gating), under default options, on every cloud engine/mode.

Two layers:

- provider-free contract tests (run in CI, which installs NO google/amazon
  providers): the shared wrapper builder and the swallow/poke verdict
  helpers on ``StarlakeAirflowJob``, plus the bash-job wrapper regression;
- provider-guarded operator tests (skipped without
  ``apache-airflow-providers-amazon`` / ``-google``): the real
  ``FargateTaskOperator`` / ``CloudRunJobOperator`` / completion sensors and
  the ``sl_job`` wrapper selection per task type.
"""

from __future__ import annotations

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
    not AMAZON_AVAILABLE,
    reason="Requires apache-airflow-providers-amazon",
)
google_only = pytest.mark.skipif(
    not GOOGLE_AVAILABLE,
    reason="Requires apache-airflow-providers-google",
)

ACTIVE_EXIT = "if [ $return_code -ne 0 ]; then"

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


def _dag():
    from airflow import DAG
    from datetime import datetime
    return DAG(dag_id="test_cloud_failure_propagation", start_date=datetime(2024, 1, 1), schedule=None)


# ---------------------------------------------------------------------------
# 1. Provider-free — shared echo/XCom wrapper builder
# ---------------------------------------------------------------------------

class TestXComWrappedCommand:

    def test_preload_variant_swallows_exit_code(self):
        from ai.starlake.airflow import StarlakeAirflowJob

        wrapped = StarlakeAirflowJob._sl_xcom_wrapped_command("starlake preload", preload=True)
        assert "starlake preload" in wrapped
        assert "return_code=$?" in wrapped
        assert "echo $return_code" in wrapped
        # the swallow: NO active exit trailer at all
        assert ACTIVE_EXIT not in wrapped
        assert "exit $return_code" not in wrapped

    def test_non_preload_variant_propagates_exit_code(self):
        from ai.starlake.airflow import StarlakeAirflowJob

        wrapped = StarlakeAirflowJob._sl_xcom_wrapped_command("starlake load", preload=False)
        assert "starlake load" in wrapped
        assert "return_code=$?" in wrapped
        assert "echo $return_code" in wrapped
        # ACTIVE (uncommented) exit trailer — a failed job fails the task
        assert ACTIVE_EXIT in wrapped
        assert "exit $return_code" in wrapped
        assert "# if [ $return_code -ne 0 ]; then" not in wrapped
        assert "#     exit $return_code" not in wrapped

    def test_non_preload_variant_byte_equal_to_pre_refactor_bash_wrapper(self):
        """AC6 — the helper's non-preload variant is byte-identical to the
        wrapper string the bash job shipped before the refactor."""
        from ai.starlake.airflow import StarlakeAirflowJob

        expected = (
            "\n"
            "                set -e\n"
            "                bash -c '\n"
            "                starlake load\n"
            "                return_code=$?\n"
            "\n"
            "                # Push the return code to XCom\n"
            "                echo $return_code\n"
            "\n"
            "                # Exit with the captured return code if non-zero\n"
            "                if [ $return_code -ne 0 ]; then\n"
            "                    exit $return_code\n"
            "                fi\n"
            "                '\n"
            "                "
        )
        assert StarlakeAirflowJob._sl_xcom_wrapped_command("starlake load", preload=False) == expected


# ---------------------------------------------------------------------------
# 2. Provider-free — swallow verdict matrix (the issue #92 contract)
# ---------------------------------------------------------------------------

class TestCloudFailureSwallowedVerdict:

    @pytest.mark.parametrize(
        "preload, retry_on_failure, swallowed",
        [
            (False, False, False),  # THE bug: load/transform must propagate by default
            (False, True, False),
            (True, False, True),    # preload keeps its swallow (skip_or_start gating)
            (True, True, False),    # retries-as-poke workaround (#91) re-raises
        ],
    )
    def test_matrix(self, preload, retry_on_failure, swallowed):
        from ai.starlake.airflow import StarlakeAirflowJob

        assert StarlakeAirflowJob._sl_cloud_failure_swallowed(preload, retry_on_failure) is swallowed


class TestCloudPokeFailureVerdict:

    def test_preload_completes_with_falsy_xcom(self):
        from ai.starlake.airflow import StarlakeAirflowJob

        verdict = StarlakeAirflowJob._sl_cloud_poke_failure(True, "job failed")
        assert verdict.is_done is True
        assert verdict.xcom_value is False

    def test_non_preload_raises(self):
        from airflow.exceptions import AirflowException
        from ai.starlake.airflow import StarlakeAirflowJob

        with pytest.raises(AirflowException, match="job failed"):
            StarlakeAirflowJob._sl_cloud_poke_failure(False, "job failed")


# ---------------------------------------------------------------------------
# 3. Provider-free — bash job wrapper regression (refactor onto the helper)
# ---------------------------------------------------------------------------

class TestBashJobWrapperRegression:

    def _make_job(self):
        from ai.starlake.airflow.bash import StarlakeAirflowBashJob
        return StarlakeAirflowBashJob(
            filename="test_cloud_failure_propagation.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options={"pre_load_strategy": "imported"},
        )

    def test_preload_keeps_swallow_wrapper(self):
        job = self._make_job()
        task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert "return_code=$?" in task.bash_command
        assert "echo $return_code" in task.bash_command
        assert ACTIVE_EXIT not in task.bash_command

    def test_load_with_xcom_push_keeps_active_exit(self):
        job = self._make_job()
        task = job.sl_load(
            task_id="load_customers",
            domain="starbake",
            table="customers",
            do_xcom_push=True,
        )
        assert "return_code=$?" in task.bash_command
        assert ACTIVE_EXIT in task.bash_command

    def test_load_without_xcom_push_stays_raw(self):
        job = self._make_job()
        task = job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        assert "return_code=$?" not in task.bash_command


# ---------------------------------------------------------------------------
# 4. Fargate — operator failure propagation (provider-guarded)
# ---------------------------------------------------------------------------

@amazon_only
class TestFargateTaskOperatorExecute:

    def _make_operator(self, monkeypatch, *, preload, retry_on_failure, error):
        from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
        from ai.starlake.airflow.aws.starlake_airflow_fargate_job import FargateTaskOperator

        def boom(self, context):
            raise error

        monkeypatch.setattr(EcsRunTaskOperator, "execute", boom)
        with _dag():
            op = FargateTaskOperator(
                task_id="fargate_task",
                dataset=None,
                source=None,
                task_definition="test-task-def",
                cluster="test-cluster",
                overrides={},
                wait_for_completion=True,
                retry_on_failure=retry_on_failure,
                preload=preload,
            )
        return op

    def test_non_preload_default_reraises(self, monkeypatch):
        """THE issue #92 fargate bug: a failed load/transform must fail the
        task even with retry_on_failure=false (the default)."""
        error = RuntimeError("ECS task failed")
        op = self._make_operator(monkeypatch, preload=False, retry_on_failure=False, error=error)
        with pytest.raises(RuntimeError, match="ECS task failed"):
            op.execute(context={})

    def test_non_preload_retry_on_failure_reraises(self, monkeypatch):
        error = RuntimeError("ECS task failed")
        op = self._make_operator(monkeypatch, preload=False, retry_on_failure=True, error=error)
        with pytest.raises(RuntimeError):
            op.execute(context={})

    def test_preload_default_swallows_returning_false(self, monkeypatch):
        """The returned False becomes the return_value XCom (do_xcom_push is
        forced by sl_pre_load) — skip_or_start then skips downstream."""
        error = RuntimeError("ECS task failed")
        op = self._make_operator(monkeypatch, preload=True, retry_on_failure=False, error=error)
        assert op.execute(context={}) is False

    def test_preload_retry_on_failure_reraises(self, monkeypatch):
        """Preload + retry_on_failure=true is the retries-as-poke workaround
        (#91) — it must still re-raise."""
        error = RuntimeError("ECS task failed")
        op = self._make_operator(monkeypatch, preload=True, retry_on_failure=True, error=error)
        with pytest.raises(RuntimeError):
            op.execute(context={})

    def test_success_returns_true(self, monkeypatch):
        from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
        from ai.starlake.airflow.aws.starlake_airflow_fargate_job import FargateTaskOperator

        monkeypatch.setattr(EcsRunTaskOperator, "execute", lambda self, context: None)
        with _dag():
            op = FargateTaskOperator(
                task_id="fargate_task",
                dataset=None,
                source=None,
                task_definition="test-task-def",
                cluster="test-cluster",
                overrides={},
                wait_for_completion=True,
            )
        assert op.execute(context={}) is True


@amazon_only
class TestFargateTaskStateSensorPoke:

    def _make_sensor(self, monkeypatch, *, preload, describe_result):
        from ai.starlake.airflow.aws.starlake_airflow_fargate_job import FargateTaskStateSensor

        with _dag():
            sensor = FargateTaskStateSensor(
                task_id="fargate_check_completion",
                dataset=None,
                source=None,
                cluster="test-cluster",
                task="arn:aws:ecs:task/test",
                preload=preload,
            )
        mock_hook = MagicMock()
        mock_hook.conn.describe_tasks.return_value = describe_result
        monkeypatch.setattr(type(sensor), "hook", property(lambda self: mock_hook))
        return sensor

    FAILED_TASK = {"tasks": [{"lastStatus": "STOPPED", "containers": [{"exitCode": 1}]}]}
    SUCCEEDED_TASK = {"tasks": [{"lastStatus": "STOPPED", "containers": [{"exitCode": 0}]}]}

    def test_non_preload_failure_raises(self, monkeypatch):
        """V1 (issue #92): PokeReturnValue(True, False) COMPLETED the sensor
        on a failed ECS task — non-preload must now raise."""
        from airflow.exceptions import AirflowException

        sensor = self._make_sensor(monkeypatch, preload=False, describe_result=self.FAILED_TASK)
        with pytest.raises(AirflowException):
            sensor.poke(context={})

    def test_preload_failure_completes_with_falsy_xcom(self, monkeypatch):
        sensor = self._make_sensor(monkeypatch, preload=True, describe_result=self.FAILED_TASK)
        verdict = sensor.poke(context={})
        assert verdict.is_done is True
        assert verdict.xcom_value is False

    def test_success_completes_truthy(self, monkeypatch):
        sensor = self._make_sensor(monkeypatch, preload=False, describe_result=self.SUCCEEDED_TASK)
        verdict = sensor.poke(context={})
        assert verdict.is_done is True
        assert verdict.xcom_value is True

    def test_describe_error_non_preload_raises(self, monkeypatch):
        from airflow.exceptions import AirflowException
        from ai.starlake.airflow.aws.starlake_airflow_fargate_job import FargateTaskStateSensor

        with _dag():
            sensor = FargateTaskStateSensor(
                task_id="fargate_check_completion",
                dataset=None,
                source=None,
                cluster="test-cluster",
                task="arn:aws:ecs:task/test",
                preload=False,
            )
        mock_hook = MagicMock()
        mock_hook.conn.describe_tasks.side_effect = RuntimeError("boto error")
        monkeypatch.setattr(type(sensor), "hook", property(lambda self: mock_hook))
        with pytest.raises(AirflowException):
            sensor.poke(context={})

    def test_hook_airflow_exception_preload_still_swallowed(self, monkeypatch):
        """An AirflowException raised by the hook itself must not bypass the
        preload swallow (the verdict is emitted outside the try block)."""
        from airflow.exceptions import AirflowException
        from ai.starlake.airflow.aws.starlake_airflow_fargate_job import FargateTaskStateSensor

        with _dag():
            sensor = FargateTaskStateSensor(
                task_id="fargate_check_completion",
                dataset=None,
                source=None,
                cluster="test-cluster",
                task="arn:aws:ecs:task/test",
                preload=True,
            )
        mock_hook = MagicMock()
        mock_hook.conn.describe_tasks.side_effect = AirflowException("conn 'aws_default' not found")
        monkeypatch.setattr(type(sensor), "hook", property(lambda self: mock_hook))
        verdict = sensor.poke(context={})
        assert verdict.is_done is True
        assert verdict.xcom_value is False

    def test_running_status_pokes_again(self, monkeypatch):
        sensor = self._make_sensor(
            monkeypatch, preload=False,
            describe_result={"tasks": [{"lastStatus": "RUNNING", "containers": []}]},
        )
        assert sensor.poke(context={}) is None


@amazon_only
class TestFargateSlJobConstruction:

    def _make_job(self, extra_options=None):
        from ai.starlake.airflow.aws import StarlakeAirflowFargateJob
        options = dict(FARGATE_OPTIONS)
        options.update(extra_options or {})
        return StarlakeAirflowFargateJob(
            filename="test_cloud_failure_propagation.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options=options,
        )

    def test_sync_load_task_is_not_preload(self):
        job = self._make_job({"fargate_async": "false"})
        with _dag():
            task = job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        assert task.preload is False
        assert task.retry_on_failure is False
        assert task.wait_for_completion is True

    def test_sync_preload_task_is_preload(self):
        job = self._make_job({"fargate_async": "false"})
        with _dag():
            task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert task.preload is True

    def test_async_group_threads_preload_flag(self):
        job = self._make_job()
        with _dag() as dag:
            job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        submission = dag.get_task("load_customers_wait.load_customers")
        completion = dag.get_task("load_customers_wait.load_customers_check_completion")
        assert submission.preload is False
        assert completion.preload is False

    def test_async_preload_group_threads_preload_flag(self):
        job = self._make_job()
        with _dag() as dag:
            job.sl_pre_load(domain="starbake", tables={"customers"})
        submission, completion = (
            dag.get_task(tid) for tid in sorted(dag.task_ids)
        )
        assert submission.preload is True
        assert completion.preload is True


# ---------------------------------------------------------------------------
# 5. Cloud Run — gcloud wrapper selection + operator/sensor verdicts
# ---------------------------------------------------------------------------

@google_only
class TestCloudRunGcloudWrapperSelection:

    def _make_job(self, extra_options=None):
        from ai.starlake.airflow.gcp import StarlakeAirflowCloudRunJob
        options = dict(CLOUD_RUN_OPTIONS)
        options.update(extra_options or {})
        return StarlakeAirflowCloudRunJob(
            filename="test_cloud_failure_propagation.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options=options,
        )

    def test_async_status_task_load_has_active_exit(self):
        """THE issue #92 cloud_run bug: the _get_completion_status task's
        wrapper had the exit block commented out — a failed execution ended
        the chain green for load/transform under default options."""
        job = self._make_job()
        with _dag() as dag:
            job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        status = dag.get_task("load_customers_wait.load_customers_get_completion_status")
        assert "return_code=$?" in status.bash_command
        assert ACTIVE_EXIT in status.bash_command
        assert "# if [ $return_code -ne 0 ]; then" not in status.bash_command

    def test_async_submission_task_keeps_xcom_push(self):
        job = self._make_job()
        with _dag() as dag:
            job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        submission = dag.get_task("load_customers_wait.load_customers")
        # structural: the completion sensor pulls the execution name from it
        assert submission.do_xcom_push is True

    def test_async_status_task_preload_keeps_swallow(self):
        job = self._make_job()
        with _dag() as dag:
            job.sl_pre_load(domain="starbake", tables={"customers"})
        status = next(
            dag.get_task(tid) for tid in dag.task_ids if tid.endswith("_get_completion_status")
        )
        assert "return_code=$?" in status.bash_command
        assert "echo $return_code" in status.bash_command
        assert ACTIVE_EXIT not in status.bash_command

    def test_completion_sensor_never_wrapped(self):
        """V4 (issue #92): the retry_on_failure sensor command was wrapped in
        the swallow wrapper (always exit 0), destroying the sensor's
        0/retry_exit_code/1 protocol."""
        job = self._make_job({"retry_on_failure": "true"})
        with _dag() as dag:
            job.sl_load(task_id="load_customers", domain="starbake", table="customers")
        sensor = dag.get_task("load_customers_wait.load_customers_check_completion")
        assert "return_code=$?" not in sensor.bash_command
        assert "exit 2" in sensor.bash_command  # retry_exit_code protocol intact

    def test_sync_gcloud_load_with_xcom_push_has_active_exit(self):
        job = self._make_job({"cloud_run_async": "false"})
        with _dag():
            task = job.sl_load(
                task_id="load_customers", domain="starbake", table="customers",
                do_xcom_push=True,
            )
        assert ACTIVE_EXIT in task.bash_command

    def test_sync_gcloud_preload_keeps_swallow(self):
        job = self._make_job({"cloud_run_async": "false"})
        with _dag():
            task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert "return_code=$?" in task.bash_command
        assert ACTIVE_EXIT not in task.bash_command


@google_only
class TestCloudRunJobOperatorExecute:

    def _make_operator(self, monkeypatch, *, preload, retry_on_failure=False, error=None):
        from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
        from ai.starlake.airflow.gcp.starlake_airflow_cloud_run_job import (
            CloudRunJobOperator, CloudRunMode,
        )

        if error is not None:
            def boom(self, context):
                raise error
            monkeypatch.setattr(CloudRunExecuteJobOperator, "execute", boom)
        with _dag():
            op = CloudRunJobOperator(
                task_id="cloud_run_task",
                dataset=None,
                source=None,
                project_id="test-project",
                region="europe-west1",
                job_name="test-job",
                overrides={},
                mode=CloudRunMode.SYNC,
                preload=preload,
                retry_on_failure=retry_on_failure,
            )
        # Airflow 3 operators have no xcom_push attribute — raising=False
        monkeypatch.setattr(op, "xcom_push", MagicMock(), raising=False)
        return op

    def test_sync_non_preload_failure_reraises(self, monkeypatch):
        """V2 (issue #92): the sync python-operator path returned False on
        exception — the task ended green on a failed job."""
        op = self._make_operator(monkeypatch, preload=False, error=RuntimeError("job failed"))
        with pytest.raises(RuntimeError, match="job failed"):
            op.execute(context={})

    def test_sync_preload_failure_returns_false(self, monkeypatch):
        op = self._make_operator(monkeypatch, preload=True, error=RuntimeError("job failed"))
        assert op.execute(context={}) is False

    def test_sync_preload_retry_on_failure_reraises(self, monkeypatch):
        """retry_on_failure=true re-raises even for preload — the #91
        retries-as-poke workaround on the cloud_run python sync path."""
        op = self._make_operator(
            monkeypatch, preload=True, retry_on_failure=True,
            error=RuntimeError("job failed"),
        )
        with pytest.raises(RuntimeError, match="job failed"):
            op.execute(context={})

    def test_sync_python_sl_job_threads_flags(self):
        """The non-gcloud sync construction site threads preload AND
        retry_on_failure from the job configuration."""
        from ai.starlake.airflow.gcp import StarlakeAirflowCloudRunJob
        options = dict(CLOUD_RUN_OPTIONS)
        options.update({
            "cloud_run_async": "false",
            "use_gcloud": "false",
            "retry_on_failure": "true",
        })
        job = StarlakeAirflowCloudRunJob(
            filename="test_cloud_failure_propagation.py",
            module_name=_AIRFLOW_TEST_MODULE_NAME,
            options=options,
        )
        with _dag():
            load_task = job.sl_load(task_id="load_customers", domain="starbake", table="customers")
            preload_task = job.sl_pre_load(domain="starbake", tables={"customers"})
        assert load_task.preload is False
        assert load_task.retry_on_failure is True
        assert preload_task.preload is True
        assert preload_task.retry_on_failure is True


@google_only
class TestCloudRunJobCompletionSensorPoke:

    def _make_sensor(self, monkeypatch, *, preload, error_bytes):
        import ai.starlake.airflow.gcp.starlake_airflow_cloud_run_job as cloud_run_module
        from ai.starlake.airflow.gcp.starlake_airflow_cloud_run_job import (
            CloudRunJobCompletionSensor,
        )

        with _dag():
            sensor = CloudRunJobCompletionSensor(
                task_id="cloud_run_check_completion",
                dataset=None,
                source=None,
                source_task_id="cloud_run_task",
                preload=preload,
            )
        operation = MagicMock()
        operation.done = True
        operation.error.SerializeToString.return_value = error_bytes
        operation.error.message = "job failed"
        operation.error.code = 3
        mock_hook_cls = MagicMock()
        mock_hook_cls.return_value.get_conn.return_value.get_operation.return_value = operation
        monkeypatch.setattr(cloud_run_module, "CloudRunHook", mock_hook_cls)
        monkeypatch.setattr(sensor, "xcom_pull", MagicMock(return_value="operation-name"))
        return sensor

    def test_non_preload_failure_raises(self, monkeypatch):
        """V3 (issue #92): do_xcom_push defaults to True on BaseOperator, so
        the old do_xcom_push-keyed branch swallowed every failure."""
        from airflow.exceptions import AirflowException

        sensor = self._make_sensor(monkeypatch, preload=False, error_bytes=b"\x08\x03")
        with pytest.raises(AirflowException, match="job failed"):
            sensor.poke(context={})

    def test_preload_failure_completes_with_falsy_xcom(self, monkeypatch):
        sensor = self._make_sensor(monkeypatch, preload=True, error_bytes=b"\x08\x03")
        verdict = sensor.poke(context={})
        assert verdict.is_done is True
        assert verdict.xcom_value is False

    def test_success_completes_truthy(self, monkeypatch):
        sensor = self._make_sensor(monkeypatch, preload=False, error_bytes=b"")
        verdict = sensor.poke(context={})
        assert verdict.is_done is True
        assert verdict.xcom_value is True
