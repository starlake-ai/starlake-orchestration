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

from typing import List, Optional, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.dagster import StarlakeDagsterJob, StarlakeDagsterUtils, DagsterLogicalDatetimeConfig

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from dagster import Failure, Output, AssetMaterialization, AssetKey, Out, op, RetryPolicy, OpExecutionContext

from dagster._core.definitions import NodeDefinition

from dagster_shell import execute_shell_command

class StarlakeDagsterCloudRunJob(StarlakeDagsterJob):
    """A StarlakeDagsterJob that runs a starlake command on Google Cloud Run."""

    def __init__(
            self, 
            filename: str=None, 
            module_name: str=None,
            pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, 
            project_id: str=None,
            cloud_run_job_name: str=None,
            cloud_run_job_region: str=None,
            cloud_run_service_account: str = None,
            options: dict=None,
            separator:str = ' ',
            **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.project_id = __class__.get_context_var(var_name='cloud_run_project_id', default_value=os.getenv("GCP_PROJECT"), options=self.options) if not project_id else project_id
        self.cloud_run_job_name = __class__.get_context_var(var_name='cloud_run_job_name', options=self.options) if not cloud_run_job_name else cloud_run_job_name
        self.cloud_run_job_region = __class__.get_context_var('cloud_run_job_region', default_value=os.getenv("GCP_REGION"), options=self.options) if not cloud_run_job_region else cloud_run_job_region
        self.cloud_run_service_account = __class__.get_context_var(var_name='cloud_run_service_account', default_value="", options=self.options) if not cloud_run_service_account else cloud_run_service_account
        if self.cloud_run_service_account:
            self.impersonate_service_account = f"--impersonate-service-account {self.cloud_run_service_account}"
        else:
            self.impersonate_service_account = ""
        self.separator = separator if separator != ',' else ' '
        self.update_env_vars = self.separator.join([(f"--update-env-vars \"^{self.separator}^" if i == 0 else "") + f"{key}={value}" for i, (key, value) in enumerate(self.sl_env_vars.items())]) + "\""

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.CLOUD_RUN

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]= None, task_type: Optional[TaskType] = None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_job()
        Generate the Dagster node that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (Optional[StarlakeSparkConfig], optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.
            task_type (Optional[TaskType], optional): The optional task type. Defaults to None.

        Returns:
            NodeDefinition: The Dagster node.
        """
        # story 6.7 (issue #94) — sensor mode: popped BEFORE the op
        # construction and captured by the op closure below
        pre_load_poke = self.__class__._sl_resolve_pre_load_poke(kwargs)
        env = self.sl_env(args=arguments)

        sl_command = f"{self.__class__.get_context_var('GOOGLE_CLOUD_SDK', '/usr/local/google-cloud-sdk', self.options)}/bin/gcloud beta run jobs execute {self.cloud_run_job_name} "

        separator = self.separator
        update_env_vars = self.update_env_vars
        region = self.cloud_run_job_region
        project = self.project_id
        impersonate_service_account = self.impersonate_service_account

        if not task_type and len(arguments) > 0:
            task_type = TaskType.from_str(arguments[0])
        transform = task_type == TaskType.TRANSFORM
        params = kwargs.get('params', dict())

        # static sl_options sections to publish on the materialization (see
        # DagsterLogicalDatetimeConfig.sl_options for the runtime counterpart)
        extra = kwargs.pop("extra", None)

        assets: List[AssetKey] = kwargs.get("assets", [])

        ins=kwargs.get("ins", {})

        out:str=kwargs.get("out", "result")
        failure:str=kwargs.get("failure", None)
        skip_or_start = bool(kwargs.get("skip_or_start", False))
        outs=kwargs.get("outs", {out: Out(str, is_required=not skip_or_start and failure is None)})
        if failure:
            outs.update({failure: Out(str, is_required=False)})

        max_retries = int(kwargs.get("retries", self.retries))
        if max_retries > 0:
            retry_policy = RetryPolicy(max_retries=max_retries, delay=self.retry_delay)
        else:
            retry_policy = None

        @op(
            name=task_id,
            ins=ins,
            out=outs,
            retry_policy=retry_policy,
        )
        def job(context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, **kwargs):
            # per-attempt copy (issue #115): appending to the closure list
            # would make a RetryPolicy re-execution yield one duplicate
            # AssetMaterialization per prior attempt (and the list is the
            # caller's kwargs list, shared across graph rebuilds)
            attempt_assets: List[AssetKey] = list(assets)
            if dataset:
                attempt_assets.append(StarlakeDagsterUtils.get_asset(context, config, dataset))

            tmp_arguments = []
            tmp_arguments.append("--scheduledDate")
            from ai.starlake.common import sl_timestamp_format
            logical_datetime: str = StarlakeDagsterUtils.get_logical_datetime(context, config).strftime(sl_timestamp_format)
            # UNQUOTED (issue #113, mirrors Airflow #99/#101): the value sits
            # INSIDE the double-quoted --args string, so the local shell
            # never consumes the quotes and gcloud would ship them literally
            # into the container argv; sl_timestamp_format is space-free
            tmp_arguments.append(logical_datetime)
            # read WITHOUT mutating (issue #111): `arguments` is the closure
            # list — a RetryPolicy re-execution of this op function would
            # otherwise pop the next element as the command
            command = arguments[0]
            command_with_arguments = [command] + tmp_arguments + arguments[1:]

            if transform:
                # --options may be ABSENT (core sl_transform only appends it
                # when there ARE options) — locate it instead of assuming the
                # last argument (issue #114: splitting/rejoining [-1] used to
                # comma-merge the runtime options into the transform --name)
                extra_opts = [
                    opt
                    for opt in StarlakeDagsterUtils.get_transform_options(context, config, params).split(',')
                    if opt
                ]
                env.update({
                    key: value
                    for opt in extra_opts
                    if "=" in opt  # Only process valid key=value pairs
                    for key, value in [opt.split("=")]
                })
                # runtime sl_options carried by the run (sensor RunRequest or manual
                # launch) — appended last so they override the static ones (starlake
                # keeps the last occurrence of a duplicate key): precedence
                # static < 'all' < task-specific.
                runtime_options = StarlakeDagsterUtils.get_sl_options(context, config, task_id)
                if runtime_options:
                    env.update({key: str(value) for key, value in runtime_options.items()})
                    extra_opts.extend([f"{key}={value}" for key, value in runtime_options.items()])
                options_index = None
                for i, arg in enumerate(command_with_arguments[:-1]):
                    if arg == "--options":
                        options_index = i + 1
                        break
                if options_index is not None:
                    command_with_arguments[options_index] = ",".join(
                        command_with_arguments[options_index].split(",") + extra_opts
                    )
                elif extra_opts:
                    command_with_arguments.extend(["--options", ",".join(extra_opts)])

            args = f'^{separator}^' + separator.join(command_with_arguments)

            command = (
                f"{sl_command}"
                f"--args \"{args}\" "
                f"{update_env_vars} "
                f"--wait --region {region} --project {project} --format='get(metadata.name)' {impersonate_service_account}" #--task-timeout 300
            )

            if config.dry_run:
                output, return_code = f"Starlake command {command} execution skipped due to dry run mode.", 0
                context.log.info(output)
            else:
                def _run_command():
                    # execute the Cloud Run job and wait for its terminal
                    # state (gcloud --wait propagates it as the exit code)
                    return execute_shell_command(
                        shell_command=command,
                        output_logging="STREAM",
                        log=context.log,
    #                cwd=self.sl_root,
                        env=env,
                        log_shell_command=True,
                    )

                if pre_load_poke:
                    # story 6.7 (issue #94) — cloud poke = a full Cloud Run
                    # execution re-submission per attempt (shared wall-clock
                    # loop; the op holds its executor slot while poking, the
                    # heavy work runs cloud-side between checks). Soft-fail
                    # deadline → None → bare return (optional-output skip);
                    # hard timeout Failure raised inside the loop so the
                    # skip_or_start bare-return branch below cannot swallow it.
                    poked = self.__class__._sl_pre_load_poke_loop(
                        context,
                        _run_command,
                        lambda result: not result[1],
                        pre_load_poke,
                        command,
                    )
                    if poked is None:
                        return
                    output, return_code = poked
                else:
                    output, return_code = _run_command()

            if return_code:
                value=f"Starlake command {command} execution failed with output: {output}"
                if retry_policy:
                    retry_count = context.retry_number
                    if retry_count < retry_policy.max_retries:
                        raise Failure(description=value)
                if failure:
                    yield Output(value=value, output_name=failure)
                elif skip_or_start:
                    context.log.info(f"Skipping Starlake command {command} execution due to skip_or_start flag.")
                    return
                else:
                    raise Failure(description=value)
            else:
                for asset in attempt_assets:
                    yield AssetMaterialization(asset_key=asset.path, description=kwargs.get("description", f"Starlake command {command} execution succeeded"))
                if dataset:
                    yield StarlakeDagsterUtils.get_materialization(context, config, dataset, extra=extra, **kwargs)

                yield Output(value=output, output_name=out)

        return job
