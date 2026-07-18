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

from typing import Any, Dict, Optional, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from ai.starlake.airflow import StarlakeAirflowJob, StarlakeDatasetMixin, StarlakeCloudPreloadSensor, PreLoadWait

from ai.starlake.aws import StarlakeFargateHelper

from ai.starlake.airflow.compat import BaseOperator, PokeReturnValue, TaskGroup

from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.sensors.ecs import EcsTaskStateSensor
from airflow.providers.amazon.aws.hooks.ecs import EcsTaskStates

import logging

class StarlakeAirflowFargateJob(StarlakeAirflowJob):
    def __init__(self, filename: str=None, module_name: str=None, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None] = None, options: Optional[dict] = None, **kwargs):
        super().__init__(filename, module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.aws_conn_id = kwargs.get("aws_conn_id", self.caller_globals.get("aws_conn_id", __class__.get_context_var("aws_conn_id", "aws_default", self.options)))
        self.fargate_async = __class__.get_context_var(var_name='fargate_async', default_value="True", options=self.options).lower() == "true" 
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
        # story 6.3 (issue #92) — PRELOAD is the only task type whose failure
        # is swallowed (XCom-gated via skip_or_start); every other task type
        # must fail the chain on a failed job
        preload = task_type == TaskType.PRELOAD
        # story 6.5 (issue #93) — cloud pre-load waiting (deferrable-first,
        # sensor-flavor fallback). Pops the four pre_load_* sensor kwargs;
        # returns None when sensor mode is off, so the one-shot construction
        # below stays byte-identical to today. Only PRELOAD carries these
        # kwargs (core injects them for PRELOAD only).
        pre_load_wait = self.__class__._sl_resolve_cloud_pre_load_wait(
            kwargs, self.options, EcsRunTaskOperator
        )
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
            # issue #101 (companion to #99) — no single quotes: these arguments
            # flow through StarlakeFargateHelper into the ECS
            # containerOverrides[].command (exec form, no shell) and into the
            # generated aws-cli script as --overrides '{json}', where an embedded
            # single quote breaks the shell argument outright. No shell consumes
            # the quotes, so literal quotes would reach the container CLI
            # (TransformCmd, unlike LoadCmd, does not strip them).
            if scheduled_date:
                tmp_arguments.append(f"{scheduled_date}")
            else:
                tmp_arguments.append("{{sl_scheduled_date(params.cron, ts_as_datetime(data_interval_end | ts)).strftime('%Y-%m-%dT%H:%M:%S%z')}}")
            command = arguments.pop(0)
            arguments = [command] + tmp_arguments + arguments

        fargate = StarlakeFargateHelper(job=self, arguments=arguments, **kwargs)

        overrides = kwargs.get("overrides", fargate.overrides)
        kwargs.pop("overrides", None)

        aws_conn_id = kwargs.get("aws_conn_id", self.aws_conn_id)
        kwargs.pop("aws_conn_id", None)

        network_configuration = kwargs.get("network_configuration", {
            "awsvpcConfiguration": {
                "subnets": fargate.subnets,
                "securityGroups": fargate.security_groups,
                "assignPublicIp": "DISABLED"
            }
        })
        kwargs.pop("network_configuration", None)

        wait_for_completion = kwargs.get("wait_for_completion", not self.fargate_async)
        kwargs.pop("wait_for_completion", None)

        kwargs.update({'pool': kwargs.get('pool', self.pool)})

        if pre_load_wait is not None:
            # story 6.5 (issue #93) — PRELOAD waiting. Shared ECS run parameters
            # for both the deferrable operator and the sensor-flavor's per-poke
            # submission. Engine kwargs (explicit EcsRunTaskOperator params such
            # as capacity_provider_strategy) are split off: they belong on the
            # ECS submission, not on the sensor, whose BaseSensorOperator ctor
            # would reject them at DAG parse.
            engine_kwargs = self.__class__._sl_pop_engine_kwargs(kwargs, EcsRunTaskOperator)
            engine_kwargs.pop('deferrable', None)
            common = dict(
                task_definition=fargate.task_definition,
                cluster=fargate.cluster,
                aws_conn_id=aws_conn_id,
                region=fargate.region,
                launch_type="FARGATE",
                network_configuration=network_configuration,
            )
            common.update(engine_kwargs)
            if pre_load_wait.mode == 'deferrable':
                # a single deferrable task submits + defers to the triggerer (no
                # worker slot held), resumes on completion and raises on failure;
                # retries/retry_delay re-submit preload (retry = poke). The
                # retries mapping IS the poke window here — it overrides any
                # ambient default_args retries.
                kwargs.update({
                    'retries': pre_load_wait.retries,
                    'retry_delay': pre_load_wait.retry_delay,
                })
                return FargateTaskOperator(
                    task_id=task_id,
                    dataset=dataset,
                    source=self.source,
                    overrides=overrides,
                    wait_for_completion=True,
                    deferrable=True,
                    preload=True,
                    pre_load_wait=pre_load_wait,
                    **common,
                    **kwargs
                )
            # sensor-flavor fallback (deferrable unsupported or opted out): one
            # ECS run submitted + awaited per poke. A bare EcsRunTaskOperator
            # RAISES on a non-zero container exit (no files) — the sensor's poke
            # catches it and pokes again. A retried sensor restarts the whole
            # window, so retries default to 0. The overrides payload is rendered
            # by the sensor (template field) and handed to the closure — the
            # ad-hoc operator below is never a live task instance, so it cannot
            # render Jinja itself.
            def _submit_and_wait(context, payload, _common=common):
                run_op = EcsRunTaskOperator(
                    task_id=f"{task_id}_poke",
                    overrides=payload,
                    wait_for_completion=True,
                    do_xcom_push=False,
                    **_common
                )
                run_op.execute(context)
                return True
            kwargs.setdefault('retries', 0)
            return StarlakeCloudPreloadSensor(
                task_id=task_id,
                dataset=dataset,
                source=self.source,
                submit_and_wait=_submit_and_wait,
                payload=overrides,
                poke_interval=pre_load_wait.poke_interval,
                timeout=pre_load_wait.timeout,
                soft_fail=pre_load_wait.soft_fail,
                **kwargs
            )

        if wait_for_completion:
            return FargateTaskOperator(
                task_id=task_id,
                dataset=dataset,
                source=self.source,
                task_definition=fargate.task_definition,
                cluster=fargate.cluster,
                overrides=overrides,
                aws_conn_id=aws_conn_id,
                region=fargate.region,
                launch_type="FARGATE",
                network_configuration=network_configuration,
                wait_for_completion=True,
                retry_on_failure=self.retry_on_failure,
                preload=preload,
                **kwargs
            )
        else:
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
                    preload=preload,
                    **kwargs
                )
                check_completion_id = task_id + '_check_completion'
                completion_sensor = FargateTaskStateSensor(
                    task_id=check_completion_id,
                    dataset=dataset,
                    source=self.source,
                    cluster=fargate.cluster,
                    task=run_task.output["ecs_task_arn"],
                    poke_interval=self.fargate_async_poke_interval,
                    pool=kwargs.get('pool', self.pool),
                    preload=preload,
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
        task_definition: str,
        cluster: str,
        overrides: Dict[str, Any],
        aws_conn_id: str = 'aws_default',
        region: Optional[str] = None,
        launch_type: Optional[str] = None,
        network_configuration: Optional[Dict[str, Any]] = None,
        wait_for_completion: bool = True,
        retry_on_failure: bool = False,
        preload: bool = False,
        pre_load_wait: Optional[PreLoadWait] = None,
        **kwargs
    ) -> None:
        super().__init__(
            task_id=task_id,
            dataset=dataset,
            source=source,
            task_definition=task_definition,
            cluster=cluster,
            overrides=overrides,
            aws_conn_id=aws_conn_id,
            region=region,
            launch_type=launch_type,
            network_configuration=network_configuration,
            wait_for_completion=wait_for_completion,
            **kwargs
        )
        self.retry_on_failure = retry_on_failure
        self.preload = preload
        # story 6.5 (issue #93) — set on the deferrable pre-load waiting task
        # only; None for one-shot preload and every non-preload task.
        self.pre_load_wait = pre_load_wait

    def execute(self, context):
        logger = logging.getLogger(__name__)
        logger.info(f"Running fargate task {self.task_id}")
        # story 6.5 (issue #93) — deferrable pre-load waiting: submit + defer,
        # the verdict is applied on resume in execute_complete. Bypass the 6.3
        # swallow entirely: EcsRunTaskOperator.execute raises TaskDeferred as
        # control flow — a BaseException, so the except below cannot catch it.
        # A SUBMISSION-phase failure (cloud API error before the defer) routes
        # through the same waiting verdict as the resume phase, so soft_fail
        # is honored whichever phase the terminal attempt fails in.
        if self.preload and self.pre_load_wait is not None:
            try:
                return super().execute(context)
            except Exception as e:
                return StarlakeAirflowJob._sl_deferrable_wait_failure(
                    context, self.pre_load_wait, self.task_id, e
                )
        try:
            super().execute(context)
            if self.wait_for_completion:
                return True
            else:
                return None
        except Exception as e:
            logger.exception(msg = f"Task {self.task_id} has failed")
            # story 6.3 (issue #92) — single verdict source: only preload with
            # retry_on_failure=false swallows (a failed load/transform/stage
            # always fails the task; retry_on_failure=true re-raises even for
            # preload, the retries-as-poke workaround of #91)
            if not StarlakeAirflowJob._sl_cloud_failure_swallowed(self.preload, self.retry_on_failure):
                raise e
            if self.wait_for_completion:
                # False becomes the return_value XCom (do_xcom_push is forced
                # by sl_pre_load) so skip_or_start skips downstream; returning
                # it — instead of the previous explicit self.xcom_push — works
                # on both majors (Airflow 3 operators have no xcom_push)
                return False
            return None

    def execute_complete(self, context, event=None):
        # story 6.5 (issue #93) — deferrable pre-load waiting resume. Success →
        # truthy XCom (skip_or_start proceeds). A failure (EcsRunTaskOperator
        # raises on a non-success event or a non-zero container exit) is NOT the
        # 6.3 swallow: a within-window failure re-raises so Airflow retries
        # (re-submit = next poke); the terminal attempt maps to a skip
        # (soft_fail) or a hard failure. Never routes through
        # _sl_cloud_failure_swallowed.
        if not (self.preload and self.pre_load_wait is not None):
            return super().execute_complete(context, event)
        try:
            super().execute_complete(context, event)
        except Exception as e:
            return StarlakeAirflowJob._sl_deferrable_wait_failure(
                context, self.pre_load_wait, self.task_id, e
            )
        return True

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
        preload: bool = False,
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
        self.preload = preload

    def poke(self, context):
        # story 6.3 (issue #92) — completing the sensor with a falsy XCom
        # (PokeReturnValue truthiness is is_done) is correct gating for
        # preload but a silent success for every other task type: the failure
        # verdict is keyed on the task type via _sl_cloud_poke_failure. The
        # verdict is emitted OUTSIDE the try block so an AirflowException
        # raised by the hook itself cannot bypass the preload swallow.
        logger = logging.getLogger(__name__)
        logger.info(f"Checking task {self.task} state")
        failure_message = None
        try:
            tasks = self.hook.conn.describe_tasks(cluster=self.cluster, tasks=[self.task]).get("tasks", [])
            if not tasks:
                return None
            task = tasks[0]
            status: str = task.get("lastStatus", None)
            if not status:
                failure_message = f"Task {self.task} has failed with no status"
            else:
                logger.info(f"Task {self.task} state: {status}")
                if EcsTaskStates(status) in self.failure_states:
                    failure_message = f"Task {self.task} has failed with status {status}"
                elif EcsTaskStates(status) == self.target_state:
                    containers = task.get("containers", [])
                    if containers and containers[0].get("exitCode", 1) == 0:
                        logger.info(f"Task {self.task} has succeeded")
                        return PokeReturnValue(True, True)
                    failure_message = f"Task {self.task} has failed"
                else:
                    return None
        except Exception as e:
            failure_message = f"Task {self.task} has failed: {e}"
        logger.error(msg = failure_message)
        return StarlakeAirflowJob._sl_cloud_poke_failure(self.preload, failure_message)
