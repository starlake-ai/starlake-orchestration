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

from __future__ import annotations

import logging

from jinja2 import pass_context

from ai.starlake.airflow.starlake_airflow_job import StarlakeAirflowJob, AirflowDataset

from ai.starlake.common import sl_cron_start_end_dates, sl_scheduled_date, sl_scheduled_dataset, sl_timestamp_format, StarlakeParameters

from ai.starlake.dataset import DatasetTriggeringStrategy

from ai.starlake.job import StarlakeOrchestrator, StarlakeExecutionMode

from ai.starlake.orchestration import AbstractOrchestration, StarlakeSchedule, StarlakeDependencies, AbstractPipeline, AbstractTaskGroup, AbstractTask

from airflow import DAG

from ai.starlake.airflow.compat import BaseOperator, Dataset, TaskGroup, get_current_context, supports_dataset_conditions, airflow_version

from ai.starlake.airflow.starlake_airflow_api import StarlakeAirflowApiClient

from airflow.utils.context import Context

from airflow.utils.state import DagRunState

from typing import Any, List, Optional, TypeVar, Union

J = TypeVar("J", bound=StarlakeAirflowJob)

class AirflowPipeline(AbstractPipeline[DAG, BaseOperator, TaskGroup, Dataset], AirflowDataset):
    """
    Airflow implementation of the Starlake Pipeline.
    
    This class orchestrates Starlake domains and tables by generating Airflow DAGs.
    It supports:
    - Loading (ingestion) DAGs.
    - Transform DAGs.
    - Export DAGs.
    - Job DAGs.
    
    It maps Starlake concepts (Domains, Tables) to Airflow execution units.
    """
    def __init__(self, job: J, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, orchestration: Optional[AbstractOrchestration[DAG, BaseOperator, TaskGroup, Dataset]] = None, **kwargs) -> None:
        def fun(upstream: Union[BaseOperator, TaskGroup], downstream: Union[BaseOperator, TaskGroup]) -> None:
            downstream.set_upstream(upstream)

        super().__init__(job, orchestration_cls=AirflowOrchestration, dag=None, schedule=schedule, dependencies=dependencies, orchestration=orchestration, add_dag_dependency = fun, **kwargs)

        airflow_schedule: Union[str, List[Dataset], None] = None

        events = self.events

        j: StarlakeAirflowJob = job
        max_active_runs: int = j.max_active_runs

        # options-derived args (retries, retry_delay, start_date and the
        # default_dag_args JSON option) win over the caller-module snapshot;
        # snapshot-only keys (e.g. 'owner') survive (issue #87)
        default_args = {
            **job.caller_globals.get('default_dag_args', {}),
            **job.default_dag_args()
        }

        # AssetOrTimeSchedule is not supported yet within SL
        if self.cron is not None:
            airflow_schedule = self.cron
        elif events:
            max_active_runs = 1
            default_args.update({'max_active_runs': 1})
            from functools import reduce
            if len(events) == 1:
                # A single triggering dataset. reduce(| / &) over one element
                # returns the *bare* Dataset, and Airflow 2.9 serializes a
                # bare-Dataset schedule as a dict — not the array that
                # `dataset_triggers` requires — so the scheduler fails schema
                # validation ("... is not of type 'array'") even though DAG
                # import succeeds (issue #130). A one-element flat list
                # serializes as a DATASET_ALL array on every 2.4+ version and is
                # semantically identical (ANY == ALL for one dataset). 2.10
                # tolerated the bare Dataset, which is why the break only
                # surfaced on Airflow 2.9.
                airflow_schedule = list(events)
            elif supports_dataset_conditions():
                # Airflow >= 2.9: conditional dataset expressions.
                # ANY → DatasetAny (|), ALL → DatasetAll (&).
                if job.dataset_triggering_strategy == DatasetTriggeringStrategy.ANY:
                    airflow_schedule = reduce(lambda a, b: a | b, events)
                else:
                    airflow_schedule = reduce(lambda a, b: a & b, events)
            else:
                # Airflow < 2.9: DatasetAny/DatasetAll (| and &) do not exist.
                # A flat list schedule triggers when ALL datasets are updated
                # (native 2.4+ semantics). ANY (OR) cannot be expressed, so it
                # degrades to ALL (flat list) with a warning rather than failing
                # DAG construction (issue #125).
                if job.dataset_triggering_strategy == DatasetTriggeringStrategy.ANY:
                    logging.getLogger(__name__).warning(
                        "ANY dataset-triggering strategy requires Airflow >= 2.9 "
                        "(conditional dataset scheduling); falling back to ALL "
                        "(flat-list) semantics on Airflow %s.",
                        airflow_version(),
                    )
                airflow_schedule = list(events)
            if self.job.data_cycle_enabled and not self.job.data_cycle:
                self.job.data_cycle = self.computed_cron_expr
                    

        # These macros run inside Jinja at template-render time. ``@pass_context``
        # makes Jinja hand its render context (which carries ``task_instance``)
        # to the macro, so we do NOT depend on ``get_current_context()`` — that
        # contextvar is only established during rendering from Airflow 2.10 on,
        # so relying on it broke dataset-triggered DAG execution on 2.5-2.9
        # (issue #125). Template call sites are UNCHANGED: Jinja injects the
        # context automatically, so ``ts_as_datetime(data_interval_end | ts)``
        # still passes a single visible argument. The get_current_context()
        # branch is a defensive fallback for a Jinja render whose context has no
        # ``task_instance`` (Airflow always supplies it during task rendering);
        # these macros are only ever invoked from templates, never called
        # directly from Python.
        @pass_context
        def ts_as_datetime(context, ts):
            from datetime import datetime
            ti = (context.get("task_instance") if hasattr(context, "get") else None)
            if ti is None:
                ti = get_current_context()["task_instance"]
            sl_logical_date = ti.xcom_pull(task_ids="start", key=StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value)
            if sl_logical_date:
                ts = sl_logical_date
            if isinstance(ts, str):
                from dateutil import parser
                import pytz
                return parser.isoparse(ts).astimezone(pytz.timezone('UTC'))
            elif isinstance(ts, datetime):
                return ts

        from datetime import datetime
        @pass_context
        def sl_dates(context, cron_expr: str, start_time: datetime) -> str:
            ti = (context.get("task_instance") if hasattr(context, "get") else None)
            if ti is None:
                ti = get_current_context()["task_instance"]
            sl_data_interval_start = ti.xcom_pull(task_ids="start", key=StarlakeParameters.DATA_INTERVAL_START_PARAMETER.value)
            sl_data_interval_end = ti.xcom_pull(task_ids="start", key=StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value)
            if sl_data_interval_start and sl_data_interval_end:
                return f"{StarlakeParameters.DATA_INTERVAL_START_PARAMETER.value}='{sl_data_interval_start.strftime(sl_timestamp_format)}',{StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value}='{sl_data_interval_end.strftime(sl_timestamp_format)}'"
            return sl_cron_start_end_dates(cron_expr, start_time, sl_timestamp_format)

        user_defined_macros = kwargs.get('user_defined_macros', job.caller_globals.get('user_defined_macros', dict()))
        kwargs.pop('user_defined_macros', None)
        user_defined_macros["sl_dates"] = sl_dates
        user_defined_macros["ts_as_datetime"] = ts_as_datetime
        user_defined_macros["sl_scheduled_dataset"] = sl_scheduled_dataset
        user_defined_macros["sl_scheduled_date"] = sl_scheduled_date
        from ai.starlake.airflow import sl_options_from_events
        user_defined_macros["sl_options_from_events"] = sl_options_from_events

        user_defined_filters = kwargs.get('user_defined_filters', job.caller_globals.get('user_defined_filters', None))
        kwargs.pop('user_defined_filters', None)

        access_control = kwargs.get('access_control', job.caller_globals.get('access_control', None))
        kwargs.pop('access_control', None)

        self.dag = DAG(
            dag_id=self.pipeline_id, 
            schedule=airflow_schedule,
            catchup=self.catchup,
            tags=list(set([tag.upper() for tag in self.tags])), 
            default_args=default_args,
            description=job.caller_globals.get('description', ""),
            start_date=job.start_date,
            end_date=job.end_date,
            user_defined_macros=user_defined_macros,
            user_defined_filters=user_defined_filters,
            access_control=access_control,
            max_active_runs=max_active_runs,
            **kwargs
        )

    def __enter__(self):
        self.dag.__enter__()
        return super().__enter__()
    
    def __exit__(self, exc_type, exc_value, traceback):
        super().__exit__(exc_type, exc_value, traceback)
        self.dag.__exit__(exc_type, exc_value, traceback)

    def sl_transform_options(self, cron_expr: Optional[str] = None) -> Optional[str]:
        if cron_expr:
            return "{{sl_dates(params.cron_expr, ts_as_datetime(data_interval_end | ts))}}"
        return None

    def deploy(self, **kwargs) -> None:
        """Deploy the pipeline."""
        import os
        env = os.environ.copy() # Copy the current environment variables
        DAG_ID = self.pipeline_id
        AIRFLOW_HOME = kwargs.get('AIRFLOW_HOME', env.get('AIRFLOW_HOME', "/opt/airflow"))
        AIRFLOW_DAGS = f"{AIRFLOW_HOME}/dags"
        import shutil
        from pathlib import Path
        DAG_FILE = f"{AIRFLOW_DAGS}/{DAG_ID}.py"
        shutil.copyfile(Path(self.job.caller_globals['__file__']), Path(DAG_FILE))
        print(f"Pipeline {DAG_ID} deployed to {DAG_FILE}")

    @staticmethod
    def __api_client(**kwargs) -> StarlakeAirflowApiClient:
        """Client targeting the Airflow instance given by kwargs/environment.

        AIRFLOW_BASE_URL / AIRFLOW_USERNAME / AIRFLOW_PASSWORD select the
        instance and its REST credentials; the client picks the version-
        appropriate API (/api/v1 + basic auth on Airflow 2, /api/v2 + JWT on
        Airflow 3) and never touches the local metadata database when a base
        URL is explicitly targeted.
        """
        import os
        env = os.environ.copy()
        return StarlakeAirflowApiClient(
            base_url=kwargs.get('AIRFLOW_BASE_URL', env.get('AIRFLOW_BASE_URL', "http://localhost:8080")),
            username=kwargs.get('AIRFLOW_USERNAME', env.get('AIRFLOW_USERNAME', None)),
            password=kwargs.get('AIRFLOW_PASSWORD', env.get('AIRFLOW_PASSWORD', None)),
        )

    def delete(self, **kwargs) -> None:
        """Delete the pipeline (best-effort: a warning is printed when the
        targeted Airflow instance cannot be reached)."""
        try:
            client = self.__api_client(**kwargs)
            client.delete_dag(self.pipeline_id)
            print(f"Pipeline {self.pipeline_id} deleted")
        except Exception as e:
            print(f"Pipeline {self.pipeline_id} could not be deleted: {str(e)}")

    def run(self, logical_date: Optional[str] = None, timeout: str = '120', mode: StarlakeExecutionMode = StarlakeExecutionMode.RUN, **kwargs) -> None:
        """Run the pipeline.
        Args:
            logical_date (Optional[str]): the logical date.
            timeout (str): the timeout in seconds.
            mode (StarlakeExecutionMode): the execution mode.
        """
        DAG_ID = self.pipeline_id
        if mode == StarlakeExecutionMode.DRY_RUN:
            # Test the pipeline with the given configuration
            from datetime import datetime
            import pendulum
            utc = pendulum.UTC
            execution_date = datetime.now(tz=utc)
            conf = dict()
            conf.update(kwargs)
            conf.update({'start_date': execution_date, 'backfill': False})
            try:
                from airflow.configuration import initialize_config
                initialize_config().load_test_config()
            except ImportError:
                # Airflow 3: initialize_config was removed
                from airflow.configuration import conf as airflow_conf
                airflow_conf.load_test_config()
            try:
                print(f"Testing pipeline {DAG_ID} with execution date {execution_date} and  configuration {conf}")
                self.dag.test(execution_date=execution_date, run_conf=conf)
            except Exception as e:
                print(f"Pipeline {DAG_ID} failed with error {str(e)}")

        elif mode == StarlakeExecutionMode.RUN:
            import time
            import uuid
            # Run the pipeline with the given configuration through the
            # version-appropriate API (see StarlakeAirflowApiClient)
            client = self.__api_client(**kwargs)
            # generate a unique dag_run_id
            dag_run_id = kwargs.get('dag_run_id', f"manual_run_{uuid.uuid4()}")
            run_logical_date = logical_date + 'Z' if logical_date else None
            print(f"Starting pipeline {DAG_ID} with dag_run_id {dag_run_id} and logical date {run_logical_date}")
            try:
                run = client.trigger_dag_run(
                    DAG_ID,
                    dag_run_id=dag_run_id,
                    logical_date=run_logical_date,
                    conf=kwargs.get('conf', None),
                )
            except Exception as e:
                print(f"Pipeline {DAG_ID} failed with error {str(e)}")
                return
            dag_run_id = (run or {}).get('dag_run_id', None)
            if dag_run_id:
                print(f"Pipeline {DAG_ID} started with dag_run_id {dag_run_id}")
                def check_state() -> bool:
                    dag_run = client.get_dag_run(DAG_ID, dag_run_id)
                    state = (dag_run or {}).get('state', None)
                    if state == DagRunState.FAILED:
                        raise Exception(f"Pipeline {DAG_ID} failed")
                    elif state == DagRunState.SUCCESS:
                        print(f"Pipeline {DAG_ID} succeeded")
                        return True
                    elif state == DagRunState.QUEUED:
                        print(f"Pipeline {DAG_ID} is queued")
                        time.sleep(5)
                        return check_state()
                    elif state == DagRunState.RUNNING:
                        print(f"Pipeline {DAG_ID} is running")
                        time.sleep(5)
                        return check_state()
                    else:
                        print(f"Pipeline {DAG_ID} is in state {state}")
                        return False
                check_state()
            else:
                raise Exception(f"Pipeline {DAG_ID} failed")

        elif mode == StarlakeExecutionMode.BACKFILL:
            # Backfill the pipeline with the given configuration
            if not logical_date:
                raise ValueError("The logical date must be provided for backfilling")
            conf = kwargs.get('conf', {})
            conf['backfill'] = True
            kwargs.update({'conf': conf})
            self.run(logical_date=logical_date, timeout=timeout, mode=StarlakeExecutionMode.RUN, **kwargs)

        else:
            raise ValueError(f"Execution mode {mode} is not supported")


class AirflowTaskGroup(AbstractTaskGroup[TaskGroup]):
    def __init__(self, group_id: str, group: TaskGroup, **kwargs) -> None:
        super().__init__(group_id, orchestration_cls=AirflowOrchestration, group=group)

    def __enter__(self):
        self.group.__enter__()
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        super().__exit__(exc_type, exc_value, traceback)
        self.group.__exit__(exc_type, exc_value, traceback)


class AirflowOrchestration(AbstractOrchestration[DAG, BaseOperator, TaskGroup, Dataset]):
    def __init__(self, job: J, **kwargs) -> None:
        """Overrides AbstractOrchestration.__init__()
        Args:
            job (J): The job that will generate the tasks within the pipeline.
        """
        super().__init__(job, **kwargs) 

    @classmethod
    def sl_orchestrator(cls) -> str:
        return StarlakeOrchestrator.AIRFLOW

    def sl_create_pipeline(self, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, **kwargs) -> AbstractPipeline[DAG, BaseOperator, TaskGroup, Dataset]:
        """Create the Starlake pipeline to orchestrate.

        Args:
            schedule (Optional[StarlakeSchedule]): The optional schedule
            dependencies (Optional[StarlakeDependencies]): The optional dependencies
        
        Returns:
            AbstractPipeline[DAG, BaseOperator, TaskGroup, Dataset]: The pipeline to orchestrate.
        """
        return AirflowPipeline(
            self.job, 
            schedule, 
            dependencies, 
            self
        )

    def sl_create_task(self, task_id: str, task: Optional[Union[BaseOperator, TaskGroup]], pipeline: AbstractPipeline[DAG, BaseOperator, TaskGroup, Dataset]) -> Optional[Union[AbstractTask[BaseOperator], AbstractTaskGroup[TaskGroup]]]:
        if task is None:
            return None

        task.dag = pipeline.dag

        if isinstance(task, TaskGroup):
            task_group = AirflowTaskGroup(
                group_id = task.group_id.split('.')[-1],
                group = task, 
                dag = pipeline.dag,
            )

            with task_group:

                tasks = list(task.children.values())
                # sorted_tasks = []
                visited = {}

                def visit(t: Union[BaseOperator, TaskGroup]) -> Optional[Union[AbstractTask[BaseOperator], AbstractTaskGroup[TaskGroup]]]:
                    if isinstance(t, TaskGroup):
                        v_task_id = t.group_id
                    else:
                        v_task_id = t.task_id
                    if v_task_id in visited.keys():
                        return visited.get(v_task_id)
                    v = self.sl_create_task(v_task_id.split('.')[-1], t, pipeline)
                    visited.update({v_task_id: v})
                    for upstream in t.upstream_list:  # Visite récursive des tâches en amont
                        if upstream in tasks:
                            v_upstream = visit(upstream)
                            if v_upstream:
                                task_group.set_dependency(v_upstream, v)
                    # sorted_tasks.append(t)
                    return v

                for t in tasks:
                    visit(t)

            return task_group

        else:
            return AbstractTask(task_id, task)

    def sl_create_task_group(self, group_id: str, pipeline: AbstractPipeline[DAG, BaseOperator, TaskGroup, Dataset], **kwargs) -> AbstractTaskGroup[TaskGroup]:
        return AirflowTaskGroup(
            group_id, 
            group=TaskGroup(group_id=group_id, dag=pipeline.dag, **kwargs),
            dag=pipeline.dag, 
            **kwargs
        )

    @classmethod
    def from_native(cls, native: Any) -> Optional[Union[AbstractTask[BaseOperator], AbstractTaskGroup[TaskGroup]]]:
        """Create a task or task group from a native object.
        Args:
            native (Any): the native object.
        Returns:
            Optional[Union[AbstractTask[BaseOperator], AbstractTaskGroup[TaskGroup]]]: the task or task group.
        """
        if isinstance(native, TaskGroup):
            return AirflowTaskGroup(native.group_id, native)
        elif isinstance(native, BaseOperator):
            return AbstractTask(native.task_id, native)
        else:
            return None
