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

from typing import Optional, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from ai.starlake.airflow import StarlakeAirflowJob, StarlakeDatasetMixin

from ai.starlake.aws import StarlakeFargateHelper

from airflow.exceptions import AirflowException

from airflow.sdk.bases.operator import BaseOperator

from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.sensors.ecs import EcsTaskStateSensor
from airflow.providers.amazon.aws.hooks.ecs import EcsTaskStates

from airflow.sdk.bases.sensor import PokeReturnValue

from airflow.sdk import TaskGroup

import logging

class StarlakeAirflowFargateJob(StarlakeAirflowJob):
    def __init__(self, filename: str=None, module_name: str=None, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None] = None, options: Optional[dict] = None, **kwargs):
        super().__init__(filename, module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.aws_conn_id = kwargs.get("aws_conn_id", self.caller_globals.get("aws_conn_id", __class__.get_context_var("aws_conn_id", "aws_default", self.options)))
        self.fargate_async_poke_interval = float(__class__.get_context_var('fargate_async_poke_interval', "30", self.options))
        self.retry_on_failure = __class__.get_context_var("retry_on_failure", "False", self.options).lower() == 'true'

    def sl_job(self, task_id: str, arguments: list, spark_config: Optional[StarlakeSparkConfig] = None, dataset: Optional[Union[StarlakeDataset, str]]= None, task_type: Optional[TaskType] = None, **kwargs) -> BaseOperator:
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
        arguments = self._inject_scheduled_date(arguments, task_type, kwargs)

        fargate = StarlakeFargateHelper(job=self, arguments=arguments, **kwargs)

        overrides = kwargs.pop("overrides", fargate.overrides)
        aws_conn_id = kwargs.pop("aws_conn_id", self.aws_conn_id)
        network_configuration = kwargs.pop("network_configuration", {
            "awsvpcConfiguration": {
                "subnets": fargate.subnets,
                "securityGroups": fargate.security_groups,
                "assignPublicIp": "DISABLED"
            }
        })

        kwargs.update({'pool': kwargs.get('pool', self.pool)})

        with TaskGroup(group_id=f"{task_id}_wait") as task_completion_sensors:
            run_task = FargateTaskOperator(
                task_id=task_id,
                dataset=None,
                source=self.source,
                task_definition=fargate.task_definition,
                cluster=fargate.cluster,
                overrides=overrides,
                aws_conn_id=aws_conn_id,
                region=fargate.region,
                launch_type="FARGATE",
                network_configuration=network_configuration,
                wait_for_completion=False,
                **kwargs
            )
            completion_sensor = FargateTaskStateSensor(
                task_id=f'{task_id}_check_completion',
                dataset=dataset,
                source=self.source,
                cluster=fargate.cluster,
                task=run_task.output["ecs_task_arn"],
                retry_on_failure=self.retry_on_failure,
                poke_interval=self.fargate_async_poke_interval,
                pool=kwargs.get('pool', self.pool),
            )
            run_task >> completion_sensor
        return task_completion_sensors

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.FARGATE

class FargateTaskOperator(StarlakeDatasetMixin, EcsRunTaskOperator):
    def __init__(
        self,
        task_id: str,
        dataset: Optional[Union[StarlakeDataset, str]],
        source: Optional[str],
        **kwargs
    ) -> None:
        super().__init__(
            task_id=task_id,
            dataset=dataset,
            source=source,
            **kwargs
        )

class FargateTaskStateSensor(StarlakeDatasetMixin, EcsTaskStateSensor):
    """
    This sensor waits until the ECS Task has completed by providing the target_state and failure_states parameters.
    """
    def __init__(
        self,
        task_id: str,
        dataset: Optional[Union[StarlakeDataset, str]],
        source: Optional[str],
        cluster: str,
        task: str,
        retry_on_failure: bool = False,
        **kwargs
    ) -> None:
        super().__init__(
            task_id=task_id,
            dataset=dataset,
            source=source,
            cluster=cluster,
            task=task,
            target_state=EcsTaskStates.STOPPED,
            failure_states={EcsTaskStates.NONE},
            **kwargs
        )
        self.retry_on_failure = retry_on_failure

    def poke(self, context):
        logger = logging.getLogger(__name__)
        logger.info(f"Checking task {self.task} state")
        try:
            tasks = self.hook.conn.describe_tasks(cluster=self.cluster, tasks=[self.task]).get("tasks", [])
            if not tasks:
                return PokeReturnValue(False, None)

            task = tasks[0]
            status: str = task.get("lastStatus", None)

            if not status:
                logger.error(f"Task {self.task} has failed with no status")
                return PokeReturnValue(True, False)

            logger.info(f"Task {self.task} state: {status}")

            if EcsTaskStates(status) in self.failure_states:
                error_msg = f"Task {self.task} has failed with status {status}"
                logger.error(error_msg)
                if self.retry_on_failure:
                    raise AirflowException(error_msg)
                return PokeReturnValue(True, False)

            if EcsTaskStates(status) == self.target_state:
                containers = task.get("containers", [])
                if containers and containers[0].get("exitCode", 1) == 0:
                    logger.info(f"Task {self.task} has succeeded")
                    return PokeReturnValue(True, True)
                error_msg = f"Task {self.task} has failed"
                logger.error(error_msg)
                if self.retry_on_failure:
                    raise AirflowException(error_msg)
                return PokeReturnValue(True, False)

            return PokeReturnValue(False, None)
        except AirflowException:
            raise
        except Exception:
            logger.exception(f"Task {self.task} has failed")
            return PokeReturnValue(True, False)
