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

import copy
import sys

import uuid

from typing import Optional, Union

from ai.starlake.common import TODAY

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.gcp import StarlakeDataprocClusterConfig, StarlakeDataprocMasterConfig, StarlakeDataprocWorkerConfig

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from ai.starlake.airflow import StarlakeAirflowJob, StarlakeAirflowOptions, StarlakeDatasetMixin, StarlakeCloudPreloadSensor, PreLoadWait

from ai.starlake.airflow.compat import BaseOperator, TriggerRule

class StarlakeAirflowDataprocMasterConfig(StarlakeDataprocMasterConfig, StarlakeAirflowOptions):
    def __init__(self, machine_type: str, disk_type: str, disk_size: int, options: dict, **kwargs):
        super().__init__(
            machine_type=machine_type,
            disk_type=disk_type,
            disk_size=disk_size,
            options=options,
            **kwargs
        )

class StarlakeAirflowDataprocWorkerConfig(StarlakeDataprocWorkerConfig, StarlakeAirflowOptions):
    def __init__(self, num_instances: int, machine_type: str, disk_type: str, disk_size: int, options: dict, **kwargs):
        super().__init__(
            num_instances=num_instances,
            machine_type=machine_type,
            disk_type=disk_type,
            disk_size=disk_size,
            options=options,
            **kwargs
        )

class StarlakeAirflowDataprocClusterConfig(StarlakeDataprocClusterConfig, StarlakeAirflowOptions):
    def __init__(self, cluster_id:str, dataproc_name:str, master_config: StarlakeAirflowDataprocMasterConfig, worker_config: StarlakeAirflowDataprocWorkerConfig, secondary_worker_config: StarlakeAirflowDataprocWorkerConfig, idle_delete_ttl: int, single_node: bool, options: dict, **kwargs):
        super().__init__(
            cluster_id=cluster_id,
            dataproc_name=dataproc_name,
            master_config=master_config,
            worker_config=worker_config,
            secondary_worker_config=secondary_worker_config,
            idle_delete_ttl=idle_delete_ttl,
            single_node=single_node,
            options=options,
            **kwargs
        )

    @classmethod
    def from_module(cls, filename: str, module_name: str, options: dict):
        caller_globals = sys.modules[module_name].__dict__
        cluster_config_name = cls.get_context_var("cluster_config_name", filename.replace(".py", "").replace(".pyc", "").lower(), options)
        cluster_id = caller_globals.get("cluster_id", cluster_config_name)
        dataproc_name = caller_globals.get("dataproc_name", None)
        master_config = StarlakeAirflowDataprocMasterConfig.from_module(filename, module_name, options)
        worker_config = StarlakeAirflowDataprocWorkerConfig.from_module(filename, module_name, options)
        dataproc_secondary_worker_config = getattr(module_name, "get_dataproc_secondary_worker_config", lambda dag_name: None)
        secondary_worker_config = dataproc_secondary_worker_config(cluster_config_name)
        idle_delete_ttl=caller_globals.get('dataproc_idle_delete_ttl', None)
        single_node=caller_globals.get('dataproc_single_node', None)
        return cls(
            cluster_id=cluster_id,
            dataproc_name=dataproc_name,
            master_config=master_config,
            worker_config=worker_config,
            secondary_worker_config=secondary_worker_config,
            idle_delete_ttl=idle_delete_ttl,
            single_node=single_node,
            options=options,
            **caller_globals.get('dataproc_cluster_properties', {})
        )

class StarlakeAirflowDataprocCluster(StarlakeAirflowOptions):
    def __init__(self, cluster_config: StarlakeAirflowDataprocClusterConfig, options: dict, pool:str, **kwargs):
        super().__init__()

        self.options = {} if not options else options

        self.clusters = {}

        self.cluster_config = StarlakeAirflowDataprocClusterConfig(
            cluster_id=None,
            dataproc_name=None,
            master_config=None,
            worker_config=None,
            secondary_worker_config=None,
            idle_delete_ttl=None,
            single_node=None,
            options=self.options,
            **kwargs
        ) if not cluster_config else cluster_config

        self.pool = pool

    def create_dataproc_cluster(
            self,
            cluster_id: str=None,
            task_id: str=None,
            cluster_name: str=None,
            **kwargs) -> BaseOperator:
        """
        Create the Cloud Dataproc cluster.
        This operator will be flagged a success if the cluster by this name already exists.
        """
        cluster_id = self.cluster_config.cluster_id if not cluster_id else cluster_id
        nb_clusters = len(self.clusters) + 1
        cluster_name = f"{cluster_id.replace('_', '-')}-{nb_clusters}-{TODAY}"[0:51] if not cluster_name else cluster_name[0:51]
        if cluster_name[-1] == '-':
            cluster_name = cluster_name[0:-1] + 'Z'

        cluster = self.clusters.get(cluster_name, None)

        if not cluster:
            task_id = f"create_{cluster_id.replace('-', '_')}_cluster" if not task_id else task_id

            kwargs.update({
                'pool': kwargs.get('pool', self.pool),
                'trigger_rule': kwargs.get('trigger_rule', TriggerRule.ALL_SUCCESS)
            })

            spark_events_bucket = f'dataproc-{self.cluster_config.project_id}'

            from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator

            cluster = DataprocCreateClusterOperator(
                task_id=task_id,
                project_id=self.cluster_config.project_id,
                cluster_name=cluster_name,
                cluster_config=self.cluster_config.__config__(**{
                    "dataproc:job.history.to-gcs.enabled": "true",
                    "spark:spark.history.fs.logDirectory": f"gs://{spark_events_bucket}/tmp/spark-events/{{{{ds}}}}",
                    "spark:spark.eventLog.dir": f"gs://{spark_events_bucket}/tmp/spark-events/{{{{ds}}}}",
                }),
                region=self.cluster_config.region,
                **kwargs
            )

            self.clusters.update({cluster_name: cluster})

        return cluster

    def delete_dataproc_cluster(
            self,
            cluster_id: str=None,
            task_id: str=None,
            cluster_name: str=None,
            **kwargs) -> BaseOperator:
        """Tears down the cluster even if there are failures in upstream tasks."""
        cluster_id = self.cluster_config.cluster_id if not cluster_id else cluster_id
        nb_clusters = len(self.clusters)
        cluster_name = f"{cluster_id.replace('_', '-')}-{nb_clusters}-{TODAY}"[0:51] if not cluster_name else cluster_name[0:51]
        if cluster_name[-1] == '-':
            cluster_name = cluster_name[0:-1] + 'Z'
        task_id = f"delete_{cluster_id.replace('-', '_')}_cluster" if not task_id else task_id
        kwargs.update({
            'pool': kwargs.get('pool', self.pool),
            'trigger_rule': kwargs.get('trigger_rule', TriggerRule.ALL_DONE)
        })

        from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

        delete_cluster = DataprocDeleteClusterOperator(
            task_id=task_id,
            project_id=self.cluster_config.project_id,
            cluster_name=cluster_name,
            region=self.cluster_config.region,
            **kwargs
        )

        cluster = self.clusters.get(cluster_name, None)

        if cluster:
            # setup/teardown as of Apache Airflow v2.7.0
            delete_cluster = delete_cluster.as_teardown(setups = cluster)

        return delete_cluster

    def submit_starlake_job(
        self,
        cluster_id: str=None,
        task_id: str=None,
        cluster_name: str=None,
        spark_config: StarlakeSparkConfig=None,
        jar_list: list=None,
        main_class: str=None,
        arguments: list=None,
        dataset: Optional[Union[StarlakeDataset, str]]=None,
        source: Optional[str]=None,
        task_type: Optional[TaskType] = None,
        pre_load_wait: Optional[PreLoadWait] = None,
        **kwargs) -> BaseOperator:
        """Create a dataproc job on the specified cluster"""
        cluster_id = self.cluster_config.cluster_id if not cluster_id else cluster_id
        nb_clusters = len(self.clusters)
        cluster_name = f"{cluster_id.replace('_', '-')}-{nb_clusters}-{TODAY}"[0:51] if not cluster_name else cluster_name[0:51]
        if cluster_name[-1] == '-':
            cluster_name = cluster_name[0:-1] + 'Z'
        task_id = f"{cluster_id}_submit" if not task_id else task_id
        arguments = [] if not arguments else arguments
        # story 6.12 (issue #122) — not-ready sentinel: popped unconditionally
        # (BaseOperator would reject the kwarg); only PRELOAD consumes it
        sentinel_path = kwargs.pop('sentinel_path', None)
        if task_type != TaskType.PRELOAD:
            sentinel_path = None
        if sentinel_path:
            from ai.starlake.sentinel import require_scheme
            # engine-aware scheme gate: dataproc consumes gs:// only
            require_scheme(sentinel_path, ('gs',), 'dataproc')
            if pre_load_wait is None and kwargs.get('deferrable'):
                # a user-forced deferral on a ONE-SHOT preload would resume
                # through execute_complete without any sentinel consult —
                # the verdict would be silently lost (false READY)
                raise ValueError(
                    "[dataproc] pre_load_not_ready_sentinel_path is not "
                    "compatible with an explicit deferrable=True on a "
                    "one-shot preload — use pre_load_sensor=true for waiting"
                )
        # explicit --scheduledDate override — popped unconditionally: BaseOperator
        # would reject the kwarg
        scheduled_date = kwargs.pop('scheduled_date', None)
        if task_type is not None and (task_type == TaskType.LOAD or task_type == TaskType.TRANSFORM):
            params: dict = kwargs.get('params', dict())
            cron = params.get('cron_expr', params.get('cron', None))
            params.update({'cron': cron})
            kwargs.update({'params': params})
            tmp_arguments = []
            tmp_arguments.append("--scheduledDate")
            # issue #101 (companion to #99) — no single quotes: these arguments
            # are placed verbatim into job["spark_job"]["args"] and handed to the
            # Dataproc API as the Spark driver argv. No shell consumes the quotes,
            # so literal quotes would reach the container CLI (TransformCmd, unlike
            # LoadCmd, does not strip them).
            if scheduled_date:
                tmp_arguments.append(f"{scheduled_date}")
            else:
                tmp_arguments.append("{{sl_scheduled_date(params.cron, ts_as_datetime(data_interval_end | ts)).strftime('%Y-%m-%dT%H:%M:%S%z')}}")
            command = arguments.pop(0)
            arguments = [command] + tmp_arguments + arguments
        jar_list = __class__.get_context_var(var_name="spark_jar_list", options=self.options).split(",") if not jar_list else jar_list
        main_class = __class__.get_context_var("spark_job_main_class", "ai.starlake.job.Main", self.options) if not main_class else main_class

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

        kwargs.update({
            'pool': kwargs.get('pool', self.pool),
            'trigger_rule': kwargs.get('trigger_rule', TriggerRule.ALL_SUCCESS)
        })

        job_id = task_id + "_" + str(uuid.uuid4())[:8]

        job = {
            "reference": {
                "project_id": self.cluster_config.project_id,
                "job_id": job_id
            },
            "placement": {
                "cluster_name": cluster_name
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

        if pre_load_wait is not None:
            # story 6.5 (issue #93) — PRELOAD waiting on dataproc. Every poke /
            # retry is a NEW dataproc submission, and Dataproc rejects a
            # re-submitted job_id with AlreadyExists — so the frozen parse-time
            # job_id above must be made unique PER submission or the wait can
            # never observe files that arrive after the first (empty) poke.
            # Engine kwargs (explicit DataprocSubmitJobOperator params such as
            # gcp_conn_id or impersonation_chain) are split off: they belong on
            # the dataproc submission, not on the sensor, whose
            # BaseSensorOperator ctor would reject them at DAG parse.
            engine_kwargs = StarlakeAirflowJob._sl_pop_engine_kwargs(kwargs, DataprocSubmitJobOperator)
            engine_kwargs.pop('deferrable', None)
            engine_kwargs.pop('job', None)
            engine_kwargs.pop('asynchronous', None)
            common = dict(
                project_id=self.cluster_config.project_id,
                region=self.cluster_config.region,
            )
            common.update(engine_kwargs)
            if pre_load_wait.mode == 'deferrable':
                # a single deferrable submit defers to the triggerer and raises
                # on a failed job; retries/retry_delay re-submit preload (retry
                # = poke). The retries mapping IS the poke window. The unique
                # per-attempt job_id is minted at EXECUTE time by
                # DataprocJobOperator (execute re-runs on every retry) — not by
                # a Jinja template, which would break on Airflow 3 runs without
                # a logical_date and collide on a deleted-and-retriggered run.
                kwargs.update({
                    'retries': pre_load_wait.retries,
                    'retry_delay': pre_load_wait.retry_delay,
                })
                return DataprocJobOperator(
                    task_id=task_id,
                    dataset=dataset,
                    source=source,
                    job=job,
                    deferrable=True,
                    preload=True,
                    pre_load_wait=pre_load_wait,
                    sentinel_path=sentinel_path,
                    **common,
                    **kwargs
                )
            # sensor-flavor fallback: one synchronous dataproc submit per poke.
            # DataprocSubmitJobOperator RAISES on a failed job (no files) — the
            # sensor's poke catches it and pokes again. A retried sensor
            # restarts the whole window, so retries default to 0. The `job`
            # payload is rendered by the sensor (template field) and handed to
            # the closure — the ad-hoc poke operator is NOT a real task
            # instance, so it can render neither the payload nor a job_id
            # template: mint a fresh uuid job_id per poke directly.
            def _submit_and_wait(context, payload, _common=common):
                poke_job = copy.deepcopy(payload)
                poke_job.setdefault("reference", {})["job_id"] = task_id + "_" + str(uuid.uuid4())[:8]
                run_op = DataprocSubmitJobOperator(
                    task_id=f"{task_id}_poke",
                    job=poke_job,
                    asynchronous=False,
                    do_xcom_push=False,
                    **_common
                )
                run_op.execute(context)
                return True
            kwargs.setdefault('retries', 0)
            return StarlakeCloudPreloadSensor(
                task_id=task_id,
                dataset=dataset,
                source=source,
                submit_and_wait=_submit_and_wait,
                payload=job,
                poke_interval=pre_load_wait.poke_interval,
                timeout=pre_load_wait.timeout,
                soft_fail=pre_load_wait.soft_fail,
                # story 6.12 — sentinel verdict per poke via GCSHook handlers
                # (lazy import), honoring the submission's credential wiring
                sentinel_path=sentinel_path,
                sentinel_handlers=StarlakeAirflowJob._sl_gcs_sentinel_hook_handlers(
                    gcp_conn_id=common.get('gcp_conn_id', 'google_cloud_default'),
                    impersonation_chain=common.get('impersonation_chain', None),
                ) if sentinel_path else None,
                **kwargs
            )

        return DataprocJobOperator(
            task_id=task_id,
            dataset=dataset,
            source=source,
            project_id=self.cluster_config.project_id,
            region=self.cluster_config.region,
            job=job,
            preload=task_type == TaskType.PRELOAD,
            sentinel_path=sentinel_path,
            **kwargs
        )

class StarlakeAirflowDataprocJob(StarlakeAirflowJob):
    """Airflow Starlake Dataproc Job."""
    def __init__(self, filename: str=None, module_name: str=None, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, cluster: Optional[StarlakeAirflowDataprocCluster]=None, options: dict=None, **kwargs):
        super().__init__(filename, module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.cluster = StarlakeAirflowDataprocCluster(StarlakeAirflowDataprocClusterConfig.from_module(filename, module_name, self.options), options=self.options, pool=self.pool) if not cluster else cluster

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
        # story 6.5 (issue #93) — cloud pre-load waiting (deferrable-first,
        # sensor-flavor fallback). Pops the four pre_load_* sensor kwargs; None
        # when sensor mode is off (byte-identical one-shot construction below).
        from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
        pre_load_wait = self.__class__._sl_resolve_cloud_pre_load_wait(
            kwargs, self.options, DataprocSubmitJobOperator
        )
        return self.cluster.submit_starlake_job(
            task_id=task_id,
            arguments=arguments,
            spark_config=spark_config,
            dataset=dataset,
            source=self.source,
            task_type=task_type,
            pre_load_wait=pre_load_wait,
            **kwargs
        )

    def pre_tasks(self, *args, **kwargs) -> Optional[BaseOperator]:
        """Overrides StarlakeAirflowJob.pre_tasks()"""
        return self.cluster.create_dataproc_cluster(
            *args,
            **kwargs
        )

    def post_tasks(self, *args, **kwargs) -> Optional[BaseOperator]:
        """Overrides StarlakeAirflowJob.post_tasks()"""
        return self.cluster.delete_dataproc_cluster(
            *args,
            **kwargs
        )

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.DATAPROC

from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

class DataprocJobOperator(StarlakeDatasetMixin, DataprocSubmitJobOperator):
    """Dataproc Job Operator"""
    def __init__(
            self,
            task_id: str,
            dataset: Optional[Union[StarlakeDataset, str]],
            source: Optional[str],
            project_id: str,
            region: str,
            job: dict,
            preload: bool = False,
            pre_load_wait: Optional[PreLoadWait] = None,
            sentinel_path: Optional[str] = None,
            **kwargs
        ):
        kwargs.pop("asynchronous", None) # TODO handle asynchronous dataproc jobs
        super().__init__(
            task_id=task_id,
            dataset=dataset,
            source=source,
            project_id=project_id,
            region=region,
            job=job,
            asynchronous=False,
            **kwargs
        )
        self.preload = preload
        # story 6.5 (issue #93) — set on the deferrable pre-load waiting task
        # only; None otherwise.
        self.pre_load_wait = pre_load_wait
        # story 6.12 (issue #122) — not-ready sentinel (preload only); the
        # pristine job template keeps re-executions of the same operator
        # object from submitting a previous run's scope
        self.sentinel_path = sentinel_path
        self._sl_sentinel_job_template = copy.deepcopy(job) if sentinel_path else None

    def _sl_sentinel_hook_handlers(self):
        """GCSHook-based sentinel handlers honoring this operator's
        gcp_conn_id + impersonation_chain (lazy import, story 6.12)."""
        return StarlakeAirflowJob._sl_gcs_sentinel_hook_handlers(
            gcp_conn_id=getattr(self, 'gcp_conn_id', 'google_cloud_default'),
            impersonation_chain=getattr(self, 'impersonation_chain', None),
        )()

    def execute(self, context):
        # story 6.12 — runtime scope substitution into the submitted job (the
        # --notReadySentinel arg travels in spark_job.args); per-attempt and
        # idempotent (the token is gone after the first application)
        if self.preload and self.sentinel_path:
            self.job = StarlakeAirflowJob._sl_sentinel_substitute_payload(
                self._sl_sentinel_job_template, context
            )
        # story 6.5 (issue #93) — deferrable pre-load waiting: submit + defer,
        # verdict applied on resume in execute_complete. Bypass the xcom
        # bookkeeping below (TaskDeferred is a BaseException, so the except
        # blocks here cannot catch the defer control flow).
        if self.preload and self.pre_load_wait is not None:
            # mint a unique dataproc job_id per ATTEMPT: Dataproc rejects a
            # re-submitted job_id (AlreadyExists) and execute re-runs on every
            # retry (= poke). Minting at execute time is Airflow-major-proof —
            # a Jinja job_id keyed on ts_nodash breaks on Airflow 3 runs
            # without a logical_date (StrictUndefined render error) and
            # collides when a run is deleted and re-triggered for the same
            # logical date (try_number restarts).
            # dataproc job ids allow only [a-zA-Z0-9_-] (self.task_id may carry
            # a dotted TaskGroup prefix) and cap at 100 chars
            import re
            safe_task_id = re.sub(r'[^a-zA-Z0-9_-]', '_', self.task_id)[:90]
            job = copy.deepcopy(self.job)
            job.setdefault("reference", {})["job_id"] = f"{safe_task_id}_{str(uuid.uuid4())[:8]}"
            self.job = job
            # a SUBMISSION-phase failure routes through the same waiting
            # verdict as the resume phase, so soft_fail is honored whichever
            # phase the terminal attempt fails in.
            try:
                return super().execute(context)
            except Exception as e:
                if self.sentinel_path:
                    # story 6.12 — 'not ready' exits 0 in sentinel mode: an
                    # engine failure is REAL → fail fast, do not burn the
                    # retries-as-poke budget
                    StarlakeAirflowJob._sl_sentinel_engine_failure(self.task_id, e)
                return StarlakeAirflowJob._sl_deferrable_wait_failure(
                    context, self.pre_load_wait, self.task_id, e
                )
        try:
            job_id = super().execute(context)
            if self.preload and self.sentinel_path:
                # story 6.12 — keep the job_id XCom (best-effort extra, never
                # gated on — Airflow 3 operators have no xcom_push), then
                # consume-then-signal: sentinel present → falsy return_value
                # XCom → skip_or_start skips downstream
                if self.do_xcom_push and hasattr(self, "xcom_push"):
                    self.xcom_push(context, key="job_id", value=job_id)
                exists_fn, delete_fn = self._sl_sentinel_hook_handlers()
                return StarlakeAirflowJob._sl_sentinel_ready(
                    self.sentinel_path, context, exists_fn, delete_fn
                )
            if self.do_xcom_push:
                self.xcom_push(context, key="job_id", value=job_id)
                return True
            else:
                return job_id
        except Exception as e:
            if self.do_xcom_push:
                self.xcom_push(context, key="return_value", value=False)
            raise e

    def execute_complete(self, context, event=None):
        # story 6.5 (issue #93) — deferrable pre-load waiting resume. Success →
        # truthy XCom (skip_or_start proceeds). A within-window failure
        # (DataprocSubmitJobOperator raises on job ERROR/CANCELLED = no files)
        # re-raises so Airflow retries (re-submit = next poke); the terminal
        # attempt maps to a skip (soft_fail) or a hard failure.
        if not (self.preload and self.pre_load_wait is not None):
            return super().execute_complete(context, event)
        try:
            super().execute_complete(context, event)
        except Exception as e:
            if self.sentinel_path:
                # story 6.12 — engine failure in sentinel mode is REAL →
                # fail fast (no retries-as-poke consumption)
                StarlakeAirflowJob._sl_sentinel_engine_failure(self.task_id, e)
            return StarlakeAirflowJob._sl_deferrable_wait_failure(
                context, self.pre_load_wait, self.task_id, e
            )
        if self.sentinel_path:
            # story 6.12 — successful terminal state: consume the sentinel;
            # NOT READY maps to the existing retries-as-poke raise
            exists_fn, delete_fn = self._sl_sentinel_hook_handlers()
            return StarlakeAirflowJob._sl_sentinel_deferrable_success(
                context, self.pre_load_wait, self.task_id,
                self.sentinel_path, exists_fn, delete_fn,
            )
        return True
