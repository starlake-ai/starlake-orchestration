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

# NOTE (issue #107): do NOT add `from __future__ import annotations` here —
# postponed evaluation turns the op functions' `config:
# DagsterLogicalDatetimeConfig` annotations into strings that Dagster's
# pythonic-config inspection cannot resolve, and every @op in the module
# raises at definition time.

import json

import uuid

from typing import List, Optional, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.dagster import StarlakeDagsterJob, StarlakeDagsterUtils, DagsterLogicalDatetimeConfig

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from ai.starlake.common import TODAY

from ai.starlake.gcp import StarlakeDataprocClusterConfig

from dagster import Failure, Output, AssetMaterialization, AssetKey, Out, op, RetryPolicy, OpExecutionContext

from dagster._core.definitions import NodeDefinition

from dagster_gcp import DataprocResource

from dagster_gcp.dataproc.resources import DataprocClient

class StarlakeDagsterDataprocJob(StarlakeDagsterJob):
    """A StarlakeDagsterJob that runs a starlake command on Google Cloud Dataproc."""

    def __init__(
            self, 
            filename: str=None, 
            module_name: str=None,
            pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, 
            cluster_config: StarlakeDataprocClusterConfig=None, 
            options: dict=None,
            **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.cluster_config = StarlakeDataprocClusterConfig.from_module(filename, module_name, self.options) if not cluster_config else cluster_config
        cluster_id = self.cluster_config.cluster_id
        cluster_name = f"{self.cluster_config.dataproc_name}-{cluster_id.replace('_', '-')}-{TODAY}"
        self.__dataproc__  = DataprocResource(
            project_id=self.cluster_config.project_id,
            region=self.cluster_config.region,
            cluster_name=cluster_name,
            cluster_config_dict=self.cluster_config.__config__()
        )

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.DATAPROC

    def __client__(self) -> DataprocClient:
        """Get the Dataproc client."""
        return self.__dataproc__.get_client()

    def pre_tasks(self, *args, **kwargs) -> Optional[NodeDefinition]:
        """Overrides IStarlakeJob.pre_tasks()"""
        task_id = kwargs.get('task_id', f"create_{self.cluster_config.cluster_id.replace('-', '_')}_cluster")
        kwargs.pop('task_id', None)

        asset_key: Union[AssetKey, None] = kwargs.get("asset", None)

        @op(
            name=task_id,
            ins=kwargs.get("ins", {}),
            out={kwargs.get("out", "result"): Out(str)},
        )
        def create_dataproc_cluster(context, config: DagsterLogicalDatetimeConfig, **kwargs):
            if config.dry_run:
                output = f"Dataproc cluster {self.__dataproc__.cluster_name} creation skipped due to dry run mode."
                context.log.info(output)
            else:
                context.log.info(f"Creating Dataproc cluster {self.__dataproc__.cluster_name} with cluster details: \n{json.dumps(self.__dataproc__.cluster_config_dict, indent=2)}")
                self.__client__().create_cluster()
            if asset_key:
                yield AssetMaterialization(asset_key=asset_key.path, description=f"Dataproc cluster {self.__dataproc__.cluster_name} created")
            yield Output(value=task_id, output_name="result")

        return create_dataproc_cluster

    def post_tasks(self, *args, **kwargs) -> Optional[NodeDefinition]:
        """Overrides IStarlakeJob.post_tasks()"""

        task_id = kwargs.get('task_id', f"delete_{self.cluster_config.cluster_id.replace('-', '_')}_cluster")
        kwargs.pop('task_id', None)

        asset_key: Union[AssetKey, None] = kwargs.get("asset", None)

        @op(
            name=task_id,
            ins=kwargs.get("ins", {}),
            out={kwargs.get("out", "result"): Out(str)},
        )
        def delete_dataproc_cluster(context, config: DagsterLogicalDatetimeConfig, **kwargs):
            if config.dry_run:
                output = f"Dataproc cluster {self.__dataproc__.cluster_name} deletion skipped due to dry run mode."
                context.log.info(output)
            else:
                context.log.info(f"Deleting Dataproc cluster {self.__dataproc__.cluster_name}")
                self.__client__().delete_cluster()
            if asset_key:
                yield AssetMaterialization(asset_key=asset_key.path, description=f"Dataproc cluster {self.__dataproc__.cluster_name} deleted")
            yield Output(value=task_id, output_name="result")

        return delete_dataproc_cluster

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]= None, task_type: Optional[TaskType] = None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_job()
        Generate the Dagster node that will run the starlake command within the dataproc cluster by submitting the corresponding spark job.

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
        # story 6.12 (issue #122) — not-ready sentinel: popped BEFORE the op
        # construction, captured by the closure; dataproc consumes gs:// only
        sentinel_path = self.__class__._sl_resolve_sentinel(kwargs, ('gs',), 'dataproc')
        # issue #109 — the sensor-off path polls the job to its terminal
        # state; the wait budget is configurable (dagster-gcp's 20-minute
        # default is too short for real Spark jobs)
        raw_wait_timeout = __class__.get_context_var('dataproc_job_wait_timeout', '3600', self.options)
        # isdigit keeps the parse strict: "+120", "1_000" or "-5" are rejected
        # (plain int() would accept the first two)
        if str(raw_wait_timeout).strip().isdigit():
            job_wait_timeout = int(str(raw_wait_timeout).strip())
        else:
            job_wait_timeout = None
        if job_wait_timeout is None or job_wait_timeout <= 0:
            raise ValueError(
                f"[{self.__class__.sl_orchestrator()}] sl_job: invalid value '{raw_wait_timeout}' "
                f"for option 'dataproc_job_wait_timeout' — expected a positive integer number of seconds"
            )
        jar_list = __class__.get_context_var(var_name="spark_jar_list", options=self.options).split(",")
        main_class = __class__.get_context_var("spark_job_main_class", "ai.starlake.job.Main", self.options)

        sparkBucket = __class__.get_context_var(var_name="spark_bucket", options=self.options)
        spark_properties = {
            "spark.hadoop.fs.defaultFS": f"gs://{sparkBucket}",
            "spark.eventLog.enabled": "true",
            "spark.sql.sources.partitionOverwriteMode": "DYNAMIC",
            "spark.sql.legacy.parquet.int96RebaseModeInWrite": "CORRECTED",
            "spark.sql.catalogImplementation": "in-memory",
            "spark.datasource.bigquery.temporaryGcsBucket": sparkBucket,
            "spark.datasource.bigquery.allowFieldAddition": "true",
            "spark.datasource.bigquery.allowFieldRelaxation": "true",
            "spark.dynamicAllocation.enabled": "false",
            "spark.shuffle.service.enabled": "false"
        }
        spark_config = StarlakeSparkConfig(memory=None, cores=None, instances=None, cls_options=self, options=self.options, **spark_properties) if not spark_config else StarlakeSparkConfig(memory=spark_config.memory, cores=spark_config.cores, instances=spark_config.instances, cls_options=self, options=self.options, **dict(spark_properties, **spark_config.spark_properties))

        job_id = task_id + "_" + str(uuid.uuid4())[:8]

        job_details = {
            "project_id": self.__dataproc__.project_id,
            "region": self.__dataproc__.region,
            "job": {
                "reference": {
                    "project_id": self.__dataproc__.project_id,
                    "job_id": job_id
                },
                "placement": {
                    "cluster_name": self.__dataproc__.cluster_name
                },
                "spark_job": {
                    "jar_file_uris": jar_list,
                    "main_class": main_class,
                    "args": arguments,
                    "properties": {
                        **spark_config.__config__()
                    }
                }
            }
        }

        if not task_type and len(arguments) > 0:
            task_type = TaskType.from_str(arguments[0])
        transform = task_type == TaskType.TRANSFORM
        params = kwargs.get('params', dict())

        if task_type != TaskType.PRELOAD:
            sentinel_path = None

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
        def submit_dataproc_job(context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, **kwargs):
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
            # UNQUOTED (issue #113, mirrors Airflow #99/#101): spark_job.args
            # reach the Spark job argv directly — no shell ever consumes
            # quotes on this path
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
                # runtime sl_options carried by the run (sensor RunRequest or manual
                # launch) — appended last so they override the static ones (starlake
                # keeps the last occurrence of a duplicate key): precedence
                # static < 'all' < task-specific.
                runtime_options = StarlakeDagsterUtils.get_sl_options(context, config, task_id)
                if runtime_options:
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

            if sentinel_path:
                # story 6.12 — run-time scope substitution over the
                # per-attempt vector (the --notReadySentinel value embeds the
                # scope token; spark_job.args reach the driver argv directly)
                command_with_arguments = self.__class__._sl_sentinel_substitute_args(
                    command_with_arguments, context
                )

            # ship the scheduledDate-carrying vector for ALL task types
            # (issue #113): args stayed at the build-time `arguments` outside
            # the transform branch, so the Spark CLI fell back to its own
            # current time on every non-transform task
            job = job_details.get("job", {})
            spark_job = job.get("spark_job", {})
            spark_job["args"] = command_with_arguments
            job["spark_job"] = spark_job
            job_details["job"] = job

            # issue #109 — job ids are unique per project: generate a fresh id
            # per ATTEMPT at execute time (a RetryPolicy retry must not reuse
            # the previous attempt's id); poke mode re-generates one per poke
            # and injects its own id into job_details
            effective_job_id = task_id + "_" + str(uuid.uuid4())[:8]

            if config.dry_run:
                output = f"Starlake command {' '.join(command_with_arguments)} execution skipped due to dry run mode."
                context.log.info(output)
                result = {"status": {"state": "DONE"}}
            elif pre_load_poke:
                # story 6.7 (issue #94) — cloud poke = a full Dataproc job
                # re-submission per attempt (shared wall-clock loop; the op
                # holds its executor slot while poking, the heavy work runs
                # cloud-side between checks). A Dataproc job id is unique per
                # project, so every poke MUST re-submit with a fresh id.
                # Soft-fail deadline → None → bare return (optional-output
                # skip); hard timeout Failure raised inside the loop so the
                # skip_or_start bare-return branch below cannot swallow it.
                def _submit_once():
                    poke_job_id = task_id + "_" + str(uuid.uuid4())[:8]
                    job_details["job"]["reference"]["job_id"] = poke_job_id
                    context.log.info(f"Submitting Spark job {poke_job_id} to Dataproc cluster {self.__dataproc__.cluster_name} with job details: \n{json.dumps(job_details, indent=2)}")
                    client = self.__client__()
                    try:
                        client.submit_job(job_details=job_details)
                        # the submission response is NOT terminal (state
                        # PENDING — submit_job just submits): poll the job to
                        # its terminal state before interpreting it
                        client.wait_for_job(job_id=poke_job_id, wait_timeout=pre_load_poke.timeout)
                        return poke_job_id, client.get_job(job_id=poke_job_id)
                    except Exception as e:
                        # DataprocError (job ERROR/CANCELLED or poll timeout)
                        # or a transient submission error — a failed poke, not
                        # an op crash: the wall-clock window and soft_fail
                        # keep governing the outcome
                        import traceback
                        context.log.warning(f"Preload job {poke_job_id} did not succeed: {e}\n{traceback.format_exc()}")
                        return poke_job_id, {"status": {"state": "ERROR", "details": str(e)}}

                if sentinel_path:
                    # story 6.12 — three-way verdict per poke: a non-DONE
                    # terminal state is a REAL failure (fail fast — 'not
                    # ready' exits 0/DONE in sentinel mode); DONE + sentinel
                    # → consume → poke again; DONE → ready
                    def _submit_once_sentinel():
                        poke_job_id, poke_result = _submit_once()
                        if poke_result.get("status", {}).get("state") != "DONE":
                            raise Failure(description=(
                                f"Spark job {poke_job_id} did not succeed — a "
                                f"real failure ('not ready' ends DONE in "
                                f"sentinel mode): {poke_result}"
                            ))
                        ready = self.__class__._sl_sentinel_ready(context, sentinel_path)
                        return poke_job_id, poke_result, ready

                    poked = self.__class__._sl_pre_load_poke_loop(
                        context,
                        _submit_once_sentinel,
                        lambda submission: submission[2],
                        pre_load_poke,
                        ' '.join(command_with_arguments),
                    )
                    if poked is None:
                        return
                    effective_job_id, result, _ = poked
                else:
                    poked = self.__class__._sl_pre_load_poke_loop(
                        context,
                        _submit_once,
                        lambda submission: submission[1].get("status", {}).get("state") == "DONE",
                        pre_load_poke,
                        ' '.join(command_with_arguments),
                    )
                    if poked is None:
                        return
                    effective_job_id, result = poked
            else:
                job_details["job"]["reference"]["job_id"] = effective_job_id
                context.log.info(f"Submitting Spark job {effective_job_id} to Dataproc cluster {self.__dataproc__.cluster_name} with job details: \n{json.dumps(job_details, indent=2)}")
                client = self.__client__()
                try:
                    client.submit_job(job_details=job_details)
                    # the submission response is NOT terminal (state PENDING —
                    # submit_job just submits): poll the job to its terminal
                    # state before interpreting it (issue #109)
                    client.wait_for_job(job_id=effective_job_id, wait_timeout=job_wait_timeout)
                    result = client.get_job(job_id=effective_job_id)
                except Exception as e:
                    # DataprocError (job ERROR/CANCELLED or poll timeout) or a
                    # transient submission error — routed into the failure
                    # branch below so the retry_policy / failure-output /
                    # skip_or_start semantics keep applying. NOTE: a poll
                    # timeout ABANDONS the still-running job (the dagster-gcp
                    # client has no cancel primitive) — a retry then submits a
                    # fresh-id duplicate while the abandoned job may still
                    # complete; size dataproc_job_wait_timeout accordingly.
                    import traceback
                    context.log.warning(f"Spark job {effective_job_id} did not succeed: {e}\n{traceback.format_exc()}")
                    result = {"status": {"state": "ERROR", "details": str(e)}}

            if result.get("status", {}).get("state") != "DONE":
                value=f"Spark job {effective_job_id} did not succeed with result: {result}"
                if sentinel_path:
                    # story 6.12 — sentinel mode: 'not ready' ends DONE, so a
                    # non-DONE state is a REAL failure; the skip_or_start
                    # swallow no longer applies
                    raise Failure(description=(
                        f"{value} — a real failure ('not ready' ends DONE in "
                        f"sentinel mode)"
                    ))
                if retry_policy:
                    retry_count = context.retry_number
                    if retry_count < retry_policy.max_retries:
                        raise Failure(description=value)
                if failure:
                    yield Output(value=value, output_name=failure)
                elif skip_or_start:
                    context.log.info(f"Skipping Starlake command {' '.join(command_with_arguments)} execution due to skip_or_start flag.")
                    return
                else:
                    raise Failure(description=value)
            else:
                if sentinel_path and not config.dry_run and not pre_load_poke:
                    # story 6.12 — one-shot consume-then-signal: sentinel
                    # present → not ready → optional-output skip (poke mode
                    # consumed it inside the loop; dry runs never consume)
                    if not self.__class__._sl_sentinel_ready(context, sentinel_path):
                        context.log.info(
                            f"Spark job {effective_job_id}: files not ready "
                            f"(sentinel consumed) — skipping downstream tasks."
                        )
                        return
                for asset in attempt_assets:
                    yield AssetMaterialization(asset_key=asset.path, description=f"Spark job {effective_job_id} submitted to Dataproc cluster {self.__dataproc__.cluster_name}")
                if dataset:
                    yield StarlakeDagsterUtils.get_materialization(context, config, dataset, extra=extra, **kwargs)

                yield Output(value=effective_job_id, output_name=out)

        return submit_dataproc_job
