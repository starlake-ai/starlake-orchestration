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

from typing import Optional, Sequence, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from ai.starlake.airflow import StarlakeAirflowJob, StarlakeDatasetMixin

from airflow.exceptions import AirflowException

from airflow.sdk.bases.operator import BaseOperator

from airflow.providers.google.cloud.hooks.cloud_run import CloudRunHook
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

from airflow.sdk.bases.sensor import BaseSensorOperator, PokeReturnValue

from airflow.utils.context import Context
from airflow.sdk import TaskGroup

from google.cloud.run_v2.types import Execution

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
            cloud_run_async_poke_interval: float=None,
            retry_on_failure: bool=None,
            retry_delay_in_seconds: float=None,
            **kwargs):
        super().__init__(filename, module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.project_id = __class__.get_context_var(var_name='cloud_run_project_id', default_value=os.getenv("GCP_PROJECT"), options=self.options) if not project_id else project_id
        self.cloud_run_job_name = __class__.get_context_var(var_name='cloud_run_job_name', options=self.options) if not cloud_run_job_name else cloud_run_job_name
        self.cloud_run_job_region = __class__.get_context_var('cloud_run_job_region', default_value=os.getenv("GCP_REGION"), options=self.options) if not cloud_run_job_region else cloud_run_job_region
        self.cloud_run_service_account = __class__.get_context_var(var_name='cloud_run_service_account', default_value="", options=self.options) if not cloud_run_service_account else cloud_run_service_account
        self.cloud_run_async_poke_interval = float(__class__.get_context_var('cloud_run_async_poke_interval', "10", self.options)) if not cloud_run_async_poke_interval else cloud_run_async_poke_interval
        self.retry_on_failure = __class__.get_context_var("retry_on_failure", "False", self.options).lower() == 'true' if retry_on_failure is None else retry_on_failure
        self.retry_delay_in_seconds = float(__class__.get_context_var("retry_delay_in_seconds", "10", self.options)) if retry_delay_in_seconds is None else retry_delay_in_seconds

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.CLOUD_RUN

    def _build_container_overrides(self, arguments):
        """Build container overrides for Cloud Run API calls."""
        return {"container_overrides": [{
            "env": [{"name": k, "value": v} for k, v in self.sl_env_vars.items()],
            "args": arguments
        }]}

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
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        kwargs.update({'retry_delay': timedelta(seconds=self.retry_delay_in_seconds)})
        arguments = self._inject_scheduled_date(arguments, task_type, kwargs)

        with TaskGroup(group_id=f'{task_id}_wait') as task_completion_sensors:
            job_task = CloudRunJobOperator(
                task_id=task_id,
                dataset=None,
                source=self.source,
                project_id=self.project_id,
                job_name=self.cloud_run_job_name,
                region=self.cloud_run_job_region,
                overrides=self._build_container_overrides(arguments),
                impersonation_chain=self.cloud_run_service_account or None,
                **kwargs
            )
            completion_sensor = CloudRunJobCompletionSensor(
                task_id=f'{task_id}_check_completion',
                dataset=dataset,
                source=self.source,
                source_task_id=job_task.task_id,
                retry_on_failure=self.retry_on_failure,
                poke_interval=self.cloud_run_async_poke_interval,
                impersonation_chain=self.cloud_run_service_account or None,
                **kwargs
            )
            job_task >> completion_sensor
        return task_completion_sensors

class CloudRunJobOperator(StarlakeDatasetMixin, CloudRunExecuteJobOperator):
    """
    This extends official CloudRunExecuteJobOperator to execute Cloud Run jobs asynchronously.
    It fires the job and returns the operation name for a completion sensor to poll.
    """

    def __init__(
        self,
        task_id: str,
        dataset: Optional[Union[StarlakeDataset, str]],
        source: Optional[str],
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Union[str, Sequence[str], None] = None,
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

    def execute(self, context: Context):
        logger = logging.getLogger(__name__)
        hook: CloudRunHook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            transport="rest",
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

class CloudRunJobCompletionSensor(StarlakeDatasetMixin, BaseSensorOperator):

    template_fields = ("gcp_conn_id", "impersonation_chain")

    def __init__(
        self,
        *,
        task_id: str,
        dataset: Optional[Union[StarlakeDataset, str]],
        source: Optional[str],
        source_task_id: str,
        retry_on_failure: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Union[str, Sequence[str], None] = None,
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
        self.retry_on_failure = retry_on_failure
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def poke(self, context: Context):
        from google.auth.transport.requests import AuthorizedSession
        hook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        operation_name = context['ti'].xcom_pull(task_ids=self.source_task_id)
        session = AuthorizedSession(hook.get_credentials())
        response = session.get(
            f"https://run.googleapis.com/v2/{operation_name}",
            timeout=120,
        )
        response.raise_for_status()
        result = response.json()
        if result.get("done", False):
            error = result.get("error")
            if error and (error.get("code", 0) != 0 or error.get("message", "")):
                error_msg = f"{error.get('message', 'Unknown error')} [{error.get('code', 'Unknown')}]"
                if self.retry_on_failure:
                    raise AirflowException(error_msg)
                if self.do_xcom_push:
                    self.log.error(error_msg)
                    return PokeReturnValue(True, False)
                else:
                    raise AirflowException(error_msg)
            return PokeReturnValue(True, True)
        return PokeReturnValue(False, False)
