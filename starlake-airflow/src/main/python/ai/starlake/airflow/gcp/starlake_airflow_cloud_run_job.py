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

import os

from datetime import timedelta

import logging

from typing import Any, Dict, Optional, Sequence, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from ai.starlake.airflow import StarlakeAirflowJob, StarlakeDatasetMixin, StarlakeCloudPreloadSensor, PreLoadWait

from ai.starlake.airflow.bash import StarlakeBashOperator, StarlakePreloadBashSensor

from airflow.exceptions import AirflowException

from ai.starlake.airflow.compat import (
    BaseOperator,
    BaseSensorOperator,
    BashOperator,
    BashSensor,
    PokeReturnValue,
    TaskGroup,
)

from airflow.providers.google.cloud.hooks.cloud_run import CloudRunHook
from airflow.providers.google.cloud.operators.cloud_run import  CloudRunExecuteJobOperator

from airflow.utils.context import Context

from google.cloud.run_v2.types import Execution
from google.longrunning import operations_pb2

from enum import Enum
CloudRunMode = Enum("CloudRunMode", ["SYNC", "DEFER", "ASYNC"])

class StarlakeAirflowCloudRunJob(StarlakeAirflowJob):
    """Airflow Starlake Cloud Run Job."""
    def __init__(
            self, 
            filename: str = None, 
            module_name: str = None,
            pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None,
            project_id: str=None,
            cloud_run_job_name: str=None,
            cloud_run_job_region: str=None,
            cloud_run_service_account: str = None,
            options: dict=None,
            cloud_run_async:bool=None,
            cloud_run_async_poke_interval: float=None,
            retry_on_failure: bool=None,
            retry_delay_in_seconds: float=None,
            separator:str = ' ',
            **kwargs):
        super().__init__(filename, module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.project_id = __class__.get_context_var(var_name='cloud_run_project_id', default_value=os.getenv("GCP_PROJECT"), options=self.options) if not project_id else project_id
        self.cloud_run_job_name = __class__.get_context_var(var_name='cloud_run_job_name', options=self.options) if not cloud_run_job_name else cloud_run_job_name
        self.cloud_run_job_region = __class__.get_context_var('cloud_run_job_region', default_value=os.getenv("GCP_REGION"), options=self.options) if not cloud_run_job_region else cloud_run_job_region
        self.cloud_run_service_account = __class__.get_context_var(var_name='cloud_run_service_account', default_value="", options=self.options) if not cloud_run_service_account else cloud_run_service_account
        if self.cloud_run_service_account:
            self.impersonate_service_account = f"--impersonate-service-account {self.cloud_run_service_account}"
        else:
            self.impersonate_service_account = ""
        self.cloud_run_async = __class__.get_context_var(var_name='cloud_run_async', default_value="True", options=self.options).lower() == "true" if cloud_run_async is None else cloud_run_async
        self.cloud_run_async_poke_interval = float(__class__.get_context_var('cloud_run_async_poke_interval', "30", self.options)) if not cloud_run_async_poke_interval else cloud_run_async_poke_interval
        self.separator = separator if separator != ',' else ' '
        self.update_env_vars = self.separator.join([(f"--update-env-vars \"^{self.separator}^" if i == 0 else "") + f"{key}={value}" for i, (key, value) in enumerate(self.sl_env_vars.items())]) + "\""
        self.retry_on_failure = __class__.get_context_var("retry_on_failure", "False", self.options).lower() == 'true' if retry_on_failure is None else retry_on_failure
        self.retry_delay_in_seconds = float(__class__.get_context_var("retry_delay_in_seconds", "10", self.options)) if retry_delay_in_seconds is None else retry_delay_in_seconds
        self.use_gcloud = __class__.get_context_var("use_gcloud", "True", self.options).lower() == 'true'

    @classmethod
    def sl_execution_environment(self) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.CLOUD_RUN

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]=None, task_type: Optional[TaskType] = None, **kwargs) -> BaseOperator:
        """Overrides StarlakeAirflowJob.sl_job()
        Generate the Airflow task that will run the starlake command.
        
        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (Optional[StarlakeSparkConfig], optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.
            task_type (Optional[TaskType], optional): The optional task type. Defaults to None.
            
        Returns:
            BaseOperator: The Airflow task.
        """
        # story 6.3 (issue #92) — PRELOAD is the only task type whose failure
        # is swallowed (XCom-gated via skip_or_start); every other task type
        # must fail the chain on a failed job
        preload = task_type == TaskType.PRELOAD
        # story 6.5 (issue #93) — cloud pre-load waiting (deferrable-first,
        # sensor-flavor fallback). Pops the four pre_load_* sensor kwargs; None
        # when off (byte-identical one-shot construction below). The gcloud path
        # has no deferrable operator, so pass operator_cls=None there to force
        # the sensor-flavor.
        pre_load_wait = self.__class__._sl_resolve_cloud_pre_load_wait(
            kwargs,
            self.options,
            None if self.use_gcloud else CloudRunExecuteJobOperator,
        )
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        kwargs.update({'retry_delay': timedelta(seconds=self.retry_delay_in_seconds)})
        # explicit --scheduledDate override — popped unconditionally: BaseOperator
        # would reject the kwarg
        scheduled_date = kwargs.pop('scheduled_date', None)
        if task_type is not None and (task_type == TaskType.LOAD or task_type == TaskType.TRANSFORM):
            arguments = [] if not arguments else arguments
            params: dict = kwargs.get('params', dict())
            cron = params.get('cron_expr', params.get('cron', None))
            params.update({'cron': cron})
            kwargs.update({'params': params})
            tmp_arguments = []
            tmp_arguments.append("--scheduledDate")
            # issue #99 — the scheduledDate value must NOT be single-quoted here.
            # No cloud_run consumption path has a shell that consumes the quotes:
            # the gcloud paths embed the value inside the double-quoted --args
            # "..." (bash keeps single quotes inside double quotes) and the API
            # path passes the argument list verbatim to the Cloud Run container.
            # Literal quotes reach the container CLI; LoadCmd strips them but
            # TransformCmd does not, so TRANSFORM audit/SQL saw a quoted date.
            if scheduled_date:
                tmp_arguments.append(f"{scheduled_date}")
            else:
                tmp_arguments.append("{{sl_scheduled_date(params.cron, ts_as_datetime(data_interval_end | ts)).strftime('%Y-%m-%dT%H:%M:%S%z')}}")
            command = arguments.pop(0)
            arguments = [command] + tmp_arguments + arguments
        command = f'^{self.separator}^' + self.separator.join(arguments)

        if pre_load_wait is not None:
            # story 6.5 (issue #93) — PRELOAD waiting on cloud_run.
            if self.use_gcloud:
                # gcloud path: no deferrable operator — a reschedule BashSensor
                # pokes `gcloud ... run jobs execute --wait`. The TRUE exit code
                # drives the poke (0=files present=done, non-zero=no files=poke
                # again), so the RAW command (no echo/XCom wrapper); the shell
                # preload sensor's execute→True records the truthy skip_or_start
                # XCom on success.
                bash_command = (
                    f"gcloud beta run jobs execute {self.cloud_run_job_name} "
                    f"--args \"{command}\" "
                    f"{self.update_env_vars} "
                    f"--wait --region {self.cloud_run_job_region} --project {self.project_id} --format='get(metadata.name)' {self.impersonate_service_account}"
                )
                kwargs.pop('retry_delay', None)
                kwargs.setdefault('retries', 0)
                return StarlakePreloadBashSensor(
                    task_id=task_id,
                    dataset=dataset,
                    source=self.source,
                    bash_command=bash_command,
                    poke_interval=pre_load_wait.poke_interval,
                    timeout=pre_load_wait.timeout,
                    soft_fail=pre_load_wait.soft_fail,
                    mode='reschedule',
                    **kwargs
                )
            # python path. Engine kwargs (explicit CloudRunExecuteJobOperator
            # params such as gcp_conn_id) are split off: they belong on the
            # Cloud Run submission, not on the sensor, whose BaseSensorOperator
            # ctor would reject them at DAG parse.
            engine_kwargs = self.__class__._sl_pop_engine_kwargs(kwargs, CloudRunExecuteJobOperator)
            engine_kwargs.pop('deferrable', None)
            engine_kwargs.pop('overrides', None)
            common = dict(
                project_id=self.project_id,
                job_name=self.cloud_run_job_name,
                region=self.cloud_run_job_region,
                impersonation_chain=self.impersonate_service_account,
            )
            common.update(engine_kwargs)
            container_overrides: Dict[str, Any] = {
                "env": [
                    {"name": key, "value": value} for key, value in self.sl_env_vars.items()
                ]
            }
            container_overrides["args"] = arguments
            job_overrides = {"container_overrides": [container_overrides]}
            if pre_load_wait.mode == 'deferrable':
                # a single deferrable execution defers to the triggerer and
                # raises on a failed execution; retries/retry_delay re-submit
                # preload (retry = poke). The retries mapping IS the poke window.
                kwargs.update({
                    'retries': pre_load_wait.retries,
                    'retry_delay': pre_load_wait.retry_delay,
                })
                return CloudRunJobOperator(
                    task_id=task_id,
                    dataset=dataset,
                    source=self.source,
                    overrides=job_overrides,
                    mode=CloudRunMode.SYNC,
                    deferrable=True,
                    preload=True,
                    pre_load_wait=pre_load_wait,
                    **common,
                    **kwargs
                )
            # python sensor-flavor: one execution submitted + awaited per poke.
            # A bare CloudRunExecuteJobOperator RAISES on a failed execution (no
            # files) — the sensor's poke catches it and pokes again. The
            # overrides payload is rendered by the sensor (template field) and
            # handed to the closure — the ad-hoc operator below is never a live
            # task instance, so it cannot render Jinja itself.
            def _submit_and_wait(context, payload, _common=common):
                run_op = CloudRunExecuteJobOperator(
                    task_id=f"{task_id}_poke",
                    overrides=payload,
                    do_xcom_push=False,
                    **_common
                )
                run_op.execute(context)
                return True
            kwargs.pop('retry_delay', None)
            kwargs.setdefault('retries', 0)
            return StarlakeCloudPreloadSensor(
                task_id=task_id,
                dataset=dataset,
                source=self.source,
                submit_and_wait=_submit_and_wait,
                payload=job_overrides,
                poke_interval=pre_load_wait.poke_interval,
                timeout=pre_load_wait.timeout,
                soft_fail=pre_load_wait.soft_fail,
                **kwargs
            )

        if self.cloud_run_async: # asynchronous job
            with TaskGroup(group_id=f'{task_id}_wait') as task_completion_sensors:
                if self.use_gcloud: # use gcloud
                    kwargs.update({'do_xcom_push': True})
                    job_task = BashOperator(
                        task_id=task_id,
                        bash_command=(
                            f"gcloud beta run jobs execute {self.cloud_run_job_name} "
                            f"--args \"{command}\" "
                            f"{self.update_env_vars} "
                            f"--async --region {self.cloud_run_job_region} --project {self.project_id} --format='get(metadata.name)' {self.impersonate_service_account}" #--task-timeout 300
                        ),
                        **kwargs
                    )
                    # check job completion
                    check_completion_id = task_id + '_check_completion'
                    completion_sensor = GCloudRunJobCompletionSensor(
                        task_id=check_completion_id,
                        dataset=dataset if self.retry_on_failure else None,
                        source=self.source,
                        project_id=self.project_id,
                        cloud_run_job_region=self.cloud_run_job_region,
                        source_task_id=job_task.task_id,
                        retry_on_failure=self.retry_on_failure,
                        poke_interval=self.cloud_run_async_poke_interval,
                        impersonate_service_account = self.impersonate_service_account,
                        **kwargs
                    )
                    if self.retry_on_failure:
                        job_task >> completion_sensor
                    else:
                        # check job status
                        get_completion_status_id = task_id + '_get_completion_status'
                        source_task_id=job_task.task_id
                        bash_command = (f"value=`gcloud beta run jobs executions describe {{{{task_instance.xcom_pull(key=None, task_ids='{source_task_id}')}}}} --region {self.cloud_run_job_region} --project {self.project_id} --format='value(status.failedCount, status.cancelledCounts)' {self.impersonate_service_account}| sed 's/[[:blank:]]//g'`; test -z \"$value\"")
                        # story 6.3 (issue #92) — do_xcom_push is forced True
                        # above for the submission XCom (structural); it must
                        # NOT select the exit-swallowing wrapper here: only
                        # PRELOAD swallows, every other task type keeps the
                        # active `exit $return_code` trailer so a failed
                        # execution fails the chain. The command is passed
                        # untouched — the wrapper owns the quoting contract
                        # (story 6.4, issue #95)
                        bash_command = StarlakeAirflowJob._sl_xcom_wrapped_command(bash_command, preload)
                        job_status = StarlakeBashOperator(
                            task_id=get_completion_status_id,
                            dataset=dataset,
                            source=self.source,
                            bash_command=bash_command,
                            **kwargs
                        )
                        job_task >> completion_sensor >> job_status

                else:
                    container_overrides: Dict[str, Any] = {
                        "env": [
                            {"name": key, "value": value} for key, value in self.sl_env_vars.items()
                        ]
                    }
                    container_overrides["args"] = arguments
                    job_overrides = {"container_overrides": [container_overrides]}
                    job_task = CloudRunJobOperator(
                        task_id=task_id,
                        dataset=None,
                        source=self.source,
                        project_id=self.project_id,
                        job_name=self.cloud_run_job_name,
                        region=self.cloud_run_job_region,
                        overrides=job_overrides,
                        mode=CloudRunMode.ASYNC,
                        impersonation_chain=self.impersonate_service_account,
                        preload=preload,
                        retry_on_failure=self.retry_on_failure,
                        **kwargs
                    )
                    check_completion_id = task_id + '_check_completion'
                    completion_sensor = CloudRunJobCompletionSensor(
                        task_id=check_completion_id,
                        dataset=dataset,
                        source=self.source,
                        source_task_id=job_task.task_id,
                        impersonation_chain=self.impersonate_service_account,
                        preload=preload,
                        **kwargs
                    )

                    job_task >> completion_sensor

            return task_completion_sensors

        else: # synchronous job
            if self.use_gcloud:
                bash_command = (
                    f"gcloud beta run jobs execute {self.cloud_run_job_name} "
                    f"--args \"{command}\" "
                    f"{self.update_env_vars} "
                    f"--wait --region {self.cloud_run_job_region} --project {self.project_id} --format='get(metadata.name)' {self.impersonate_service_account}" #--task-timeout 300 
                )
                if kwargs.get('do_xcom_push', False):
                    # story 6.3 (issue #92) — wrapper variant keyed on the
                    # task type, not on do_xcom_push: preload swallows the
                    # exit code (XCom-gated), every other task type keeps the
                    # active `exit $return_code` trailer. The command is
                    # passed untouched — the wrapper owns the quoting
                    # contract (story 6.4, issue #95): the old blanket
                    # .replace("'", '"') turned the single quotes of
                    # --scheduledDate '...' into double quotes that
                    # terminated --args "..." early for LOAD/TRANSFORM
                    bash_command = StarlakeAirflowJob._sl_xcom_wrapped_command(bash_command, preload)
                kwargs.pop('do_xcom_push', None)
                return StarlakeBashOperator(
                    task_id=task_id,
                    dataset=dataset,
                    source=self.source,
                    bash_command=bash_command,
                    do_xcom_push=True,
                    **kwargs
                )
            else:
                container_overrides: Dict[str, Any] = {
                    "env": [
                        {"name": key, "value": value} for key, value in self.sl_env_vars.items()
                    ]
                }
                container_overrides["args"] = arguments
                job_overrides = {"container_overrides": [container_overrides]}
                return CloudRunJobOperator(
                    task_id=task_id,
                    dataset=dataset,
                    source=self.source,
                    project_id=self.project_id,
                    job_name=self.cloud_run_job_name,
                    region=self.cloud_run_job_region,
                    overrides=job_overrides,
                    mode=CloudRunMode.SYNC,
                    impersonation_chain=self.impersonate_service_account,
                    preload=preload,
                    retry_on_failure=self.retry_on_failure,
                    **kwargs
                )

class GCloudRunJobCompletionSensor(StarlakeDatasetMixin, BashSensor):
    '''
    This sensor checks the completion of a cloud run job using gcloud.
    '''
    def __init__(self, 
                 *, 
                 task_id: str, 
                 dataset: Optional[Union[StarlakeDataset, str]],
                 source: Optional[str],
                 project_id: str, 
                 cloud_run_job_region: str, 
                 source_task_id: str, 
                 retry_on_failure: bool=None, 
                 impersonate_service_account: str=None, 
                 **kwargs
        ) -> None:
        if retry_on_failure:
            kwargs.update({'retry_exit_code': 2})
            bash_command = (
                "check_completion=`gcloud beta run jobs executions describe "
                f"{{{{ task_instance.xcom_pull(key='return_value', task_ids='{source_task_id}') }}}} "
                f"--region {cloud_run_job_region} "
                f"--project {project_id} "
                "--format='value(status.completionTime, status.cancelledCounts)' "
                "| sed 's/[[:blank:]]//g'`; "
                "if [ -z \"$check_completion\" ]; then exit 2; else "
                "check_status=`gcloud beta run jobs executions describe "
                f"{{{{ task_instance.xcom_pull(key='return_value', task_ids='{source_task_id}') }}}} "
                f"--region {cloud_run_job_region} "
                f"--project {project_id} "
                f"--format='value(status.failedCount, status.cancelledCounts)' {impersonate_service_account}"
                "| sed 's/[[:blank:]]//g'`; "
                "test -z \"$check_status\" && exit 0 || exit 1; fi"
            )
        else:
            bash_command=(f"value=`gcloud beta run jobs executions describe {{{{task_instance.xcom_pull(key='return_value', task_ids='{source_task_id}')}}}}  --region {cloud_run_job_region} --project {project_id} --format='value(status.completionTime, status.cancelledCounts)' {impersonate_service_account}| sed 's/[[:blank:]]//g'`; test -n \"$value\"")

        # story 6.3 (issue #92) — the echo/XCom wrapper is NEVER applied to a
        # sensor command: BashSensor's protocol needs the true exit code
        # (0=done, retry_exit_code=2=poke again, 1=fail); the wrapper always
        # exited 0 and would complete the sensor on the first poke regardless
        # of the execution state
        super().__init__(
            task_id=task_id,
            dataset=dataset,
            source=source,
            bash_command=bash_command,
            mode="reschedule",
            **kwargs
        )

class CloudRunJobOperator(StarlakeDatasetMixin, CloudRunExecuteJobOperator):
    """
    This extends official CloudRunExecuteJobOperator in order to implement asynchronous job.
    """

    def __init__(
        self,
        task_id: str,
        dataset: Optional[Union[StarlakeDataset, str]],
        source: Optional[str],
        mode: CloudRunMode = CloudRunMode.SYNC,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Union[str, Sequence[str], None] = None,
        preload: bool = False,
        retry_on_failure: bool = False,
        pre_load_wait: Optional[PreLoadWait] = None,
        **kwargs,
    ):
        super().__init__(  # type: ignore
            task_id=task_id,
            dataset=dataset,
            source=source,
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs
        )
        self.mode = mode
        self.preload = preload
        self.retry_on_failure = retry_on_failure
        # story 6.5 (issue #93) — set on the deferrable pre-load waiting task
        # only; None otherwise.
        self.pre_load_wait = pre_load_wait

    def execute(self, context: Context):
        logger = logging.getLogger(__name__)
        # story 6.5 (issue #93) — deferrable pre-load waiting: submit + defer via
        # the native CloudRunExecuteJobOperator (self.deferrable=True), the
        # verdict is applied on resume in execute_complete. Bypass both the ASYNC
        # custom-hook path and the SYNC swallow. TaskDeferred is a BaseException,
        # so the except below cannot catch the defer control flow. A SUBMISSION-
        # phase failure (cloud API error before the defer) routes through the
        # same waiting verdict as the resume phase, so soft_fail is honored
        # whichever phase the terminal attempt fails in.
        if self.preload and self.pre_load_wait is not None:
            try:
                return super(CloudRunJobOperator, self).execute(context)
            except Exception as e:
                return StarlakeAirflowJob._sl_deferrable_wait_failure(
                    context, self.pre_load_wait, self.task_id, e
                )
        if self.mode == CloudRunMode.ASYNC:
            hook: CloudRunHook = CloudRunHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
            )
            self.operation = hook.execute_job(
                region=self.region,
                project_id=self.project_id,
                job_name=self.job_name,
                overrides=self.overrides,
            )
            execution = Execution.deserialize(self.operation.operation.metadata.value)
            job_id = execution.name.split("/")[-1]
            logger.info(
                f"https://console.cloud.google.com/run/jobs/executions/details/{self.region}/{job_id}/tasks?project={self.project_id}"
            )
            logger.info(execution.log_uri)
            return self.operation.operation.name

        else:
            try:
                job = super(CloudRunJobOperator, self).execute(context)
                # Airflow 3 Task-SDK operators have no xcom_push attribute —
                # the "job" XCom is a best-effort extra, never gated on
                if self.do_xcom_push and hasattr(self, "xcom_push"):
                    self.xcom_push(context, key="job", value=job)
                return True
            except Exception as e:
                logger.exception(msg=f"Task {self.task_id} has failed")
                # story 6.3 (issue #92) — single verdict source: only preload
                # with retry_on_failure=false swallows (the False return value
                # feeds the skip_or_start XCom gating); a failed
                # load/transform/stage always fails the task, and
                # retry_on_failure=true re-raises even for preload
                # (retries-as-poke workaround, #91)
                if not StarlakeAirflowJob._sl_cloud_failure_swallowed(self.preload, self.retry_on_failure):
                    raise e
                return False

    def execute_complete(self, context: Context, event: dict = None):
        # story 6.5 (issue #93) — deferrable pre-load waiting resume. Success →
        # truthy XCom (skip_or_start proceeds). A within-window failure
        # (CloudRunExecuteJobOperator raises on a failed execution = no files)
        # re-raises so Airflow retries (re-submit = next poke); the terminal
        # attempt maps to a skip (soft_fail) or a hard failure. Never routes
        # through the #92 swallow.
        if not (self.preload and self.pre_load_wait is not None):
            return super().execute_complete(context, event)
        try:
            super().execute_complete(context, event)
        except Exception as e:
            return StarlakeAirflowJob._sl_deferrable_wait_failure(
                context, self.pre_load_wait, self.task_id, e
            )
        return True

class CloudRunJobCompletionSensor(StarlakeDatasetMixin, BaseSensorOperator):

    template_fields = ("gcp_conn_id", "impersonation_chain")

    def __init__(
        self,
        *,
        task_id: str,
        dataset: Optional[Union[StarlakeDataset, str]],
        source: Optional[str],
        source_task_id: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Union[str, Sequence[str], None] = None,
        preload: bool = False,
        **kwargs,
    ):
        super().__init__(
            task_id=task_id,
            dataset=dataset,
            source=source,
            mode="reschedule",
            **kwargs
        )
        self.source_task_id = source_task_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.preload = preload

    def poke(self, context: Context):
        hook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        operation_name = self.xcom_pull(context, task_ids=self.source_task_id)
        operation_request = operations_pb2.GetOperationRequest(name=operation_name)
        operation: operations_pb2.Operation = hook.get_conn().get_operation(
            operation_request
        )
        if operation.done:
            # An operation can only have one of those two combinations: if it is failed, then
            # the error field will be populated, else, then the response field will be.
            if operation.error.SerializeToString():
                self.log.error(
                    f"{operation.error.message} [{operation.error.code}]"
                )
                # story 6.3 (issue #92) — verdict keyed on the task type, NOT
                # on do_xcom_push (which defaults to True on BaseOperator and
                # silently selected the swallow branch for every task type):
                # preload completes with a falsy XCom (skip_or_start gating),
                # anything else raises
                return StarlakeAirflowJob._sl_cloud_poke_failure(
                    self.preload,
                    f"{operation.error.message} [{operation.error.code}]",
                )
            return PokeReturnValue(True, True)
        return PokeReturnValue(False, False)
