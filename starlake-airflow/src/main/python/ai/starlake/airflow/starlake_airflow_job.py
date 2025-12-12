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

from datetime import timedelta, datetime

from typing import Any, Dict, Optional, List, Union

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOrchestrator, TaskType

from ai.starlake.airflow.starlake_airflow_options import StarlakeAirflowOptions

from ai.starlake.airflow.starlake_airflow_api import supports_assets, supports_datasets, supports_inlet_events, DotDict, StarlakeAirflowApiClient

from ai.starlake.common import MissingEnvironmentVariable, get_cron_frequency, is_valid_cron, StarlakeParameters, sl_timestamp_format, most_frequent_crons, scheduled_dates_range, sl_schedule_format

from ai.starlake.job.starlake_job import StarlakeOrchestrator

from ai.starlake.dataset import StarlakeDataset, AbstractEvent

try:
    from airflow.sdk import Asset as Dataset, AssetEvent as DatasetEvent   # Airflow 3.x
except ImportError:
    from airflow.datasets import Dataset       # Airflow 2.x
    from airflow.models.dataset import DatasetEvent

from airflow.models import DagRun, TaskInstance, Operator

from airflow.models.serialized_dag import SerializedDagModel

from airflow.models.baseoperator import BaseOperator

from airflow.operators.empty import EmptyOperator

from airflow.operators.python import ShortCircuitOperator

from airflow.utils.context import Context

from airflow.utils.session import provide_session

from airflow.utils.task_group import TaskGroup

from sqlalchemy.orm.session import Session

import logging

import pytz

DEFAULT_POOL:str ="default_pool"

DEFAULT_DAG_ARGS = {
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

class AirflowDataset(AbstractEvent[Dataset]):
    @classmethod
    def to_event(cls, dataset: StarlakeDataset, source: Optional[str] = None) -> Dataset:
        extra = {
            StarlakeParameters.URI_PARAMETER.value: dataset.uri,
            StarlakeParameters.CRON_PARAMETER.value: dataset.cron,
            StarlakeParameters.FRESHNESS_PARAMETER.value: dataset.freshness,
        }
        if source:
            extra["source"] = source
        if not supports_inlet_events():
            return Dataset(dataset.refresh().url, extra)
        return Dataset(dataset.uri, extra)

class StarlakeAirflowJob(IStarlakeJob[BaseOperator, Dataset], StarlakeAirflowOptions, AirflowDataset):
    def __init__(self, filename: Optional[str] = None, module_name: Optional[str] = None, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None] = None, options: dict = {}, **kwargs) -> None:
        """Overrides IStarlakeJob.__init__()
        Args:
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The pre-load strategy to use.
            options (dict): The options to use.
        """
        super().__init__(filename, module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.pool = str(__class__.get_context_var(var_name='default_pool', default_value=DEFAULT_POOL, options=self.options))
        self.outlets: List[Dataset] = kwargs.get('outlets', [])
        # set end_date
        import re
        pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
        try:
            ed = __class__.get_context_var(var_name='end_date', options=self.options)
        except MissingEnvironmentVariable:
            ed = ""
        if pattern.fullmatch(ed):
            self.__end_date = datetime.strptime(ed, '%Y-%m-%d').astimezone(pytz.timezone(self.timezone))
        else:
            self.__end_date = None
        # set max_active_runs
        self.__max_active_runs = int(__class__.get_context_var(var_name='max_active_runs', default_value="3", options=self.options))

    @property
    def end_date(self) -> Optional[datetime]:
        """Get the end date value."""
        return self.__end_date

    @property
    def max_active_runs(self) -> int:
        """Get maximum active DAG execution runs"""
        return self.__max_active_runs

    @classmethod
    def sl_orchestrator(cls) -> Union[StarlakeOrchestrator, str]:
        return StarlakeOrchestrator.AIRFLOW

    def sl_import(self, task_id: str, domain: str, tables: set=set(), **kwargs) -> BaseOperator:
        """Overrides IStarlakeJob.sl_import()
        Generate the Airflow task that will run the starlake `import` command.

        Args:
            task_id (str): The optional task id ({domain}_import by default).
            domain (str): The required domain to import.
            tables (set): The optional tables to import.

        Returns:
            BaseOperator: The Airflow task.
        """
        kwargs.update({'doc': kwargs.get('doc', f'Import tables {",".join(list(tables or []))} within {domain}.')})
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        return super().sl_import(task_id=task_id, domain=domain, tables=tables, **kwargs)

    def start_op(self, task_id, scheduled: bool, not_scheduled_datasets: Optional[List[StarlakeDataset]], least_frequent_datasets: Optional[List[StarlakeDataset]], most_frequent_datasets: Optional[List[StarlakeDataset]], **kwargs) -> Optional[BaseOperator]:
        """Overrides IStarlakeJob.start_op()
        It represents the first task of a pipeline, it will define the optional condition that may trigger the DAG.
        Args:
            task_id (str): The required task id.
            scheduled (bool): whether the dag is scheduled or not.
            not_scheduled_datasets (Optional[List[StarlakeDataset]]): The optional not scheduled datasets.
            least_frequent_datasets (Optional[List[StarlakeDataset]]): The optional least frequent datasets.
            most_frequent_datasets (Optional[List[StarlakeDataset]]): The optional most frequent datasets.
        Returns:
            Optional[BaseOperator]: The optional Airflow task.
        """
        if not supports_inlet_events() and not scheduled and least_frequent_datasets:
            with TaskGroup(group_id=f'{task_id}') as start:
                with TaskGroup(group_id=f'trigger_least_frequent_datasets') as trigger_least_frequent_datasets:
                     for dataset in least_frequent_datasets:
                         StarlakeEmptyOperator(
                             task_id=f"trigger_{dataset.uri}",
                             dataset=dataset,
                             source=self.source,
                             **kwargs.copy())
            return start 
        elif supports_inlet_events() and not scheduled:
            datasets: List[Dataset] = []
            datasets += list(map(lambda dataset: self.to_event(dataset=dataset), not_scheduled_datasets or []))
            datasets += list(map(lambda dataset: self.to_event(dataset=dataset), least_frequent_datasets or []))
            datasets += list(map(lambda dataset: self.to_event(dataset=dataset), most_frequent_datasets or []))

            dag_id = kwargs.get('dag_id', None)
            if not dag_id:
                dag_id = self.source

            def get_scheduled_datetime(dataset: Dataset) -> Optional[datetime]:
                extra = dataset.extra or {}
                scheduled_date = extra.get(StarlakeParameters.SCHEDULED_DATE_PARAMETER.value, extra.get('scheduled_date', None))
                if not scheduled_date:
                    # for backward compatibility
                    from urllib.parse import urlparse, parse_qs
                    parsed_url = urlparse(dataset.uri)
                    query_string = parsed_url.query
                    params = parse_qs(query_string)
                    scheduled_dates = params.get(StarlakeParameters.SCHEDULED_DATE_PARAMETER.value, params.get('sl_schedule', None))
                    if scheduled_dates:
                        try:
                            scheduled_date = scheduled_dates[-1]
                            return datetime.strptime(scheduled_date, sl_schedule_format).replace(tzinfo=pytz.timezone('UTC'))
                        except Exception:
                            scheduled_date = None
                if scheduled_date:
                    from dateutil import parser
                    return parser.isoparse(scheduled_date).astimezone(pytz.timezone('UTC'))
                else:
                    print(f"Dataset {dataset.uri} has no scheduled date in its extra data. Please ensure that the dataset has a '{StarlakeParameters.SCHEDULED_DATE_PARAMETER.value}' key in its extra data.")
                    return None

            def get_triggering_datasets(context: Context = None) -> List[Dataset]:
                if not context:
                    from airflow.operators.python import get_current_context
                    context = get_current_context()

                ti = context["task_instance"]
                template_ctx = ti.get_template_context()

                # Airflow 2.x: triggering_dataset_events
                # Airflow 3.x: triggering_asset_events
                triggering_dataset_events = []
                if "triggering_dataset_events" in template_ctx:
                    triggering_dataset_events = template_ctx["triggering_dataset_events"]
                elif "triggering_asset_events" in template_ctx:
                    triggering_dataset_events = template_ctx["triggering_asset_events"]

                if not triggering_dataset_events:
                    # No triggering datasets/assets
                    return []

                triggering_uris = {}
                dataset_uris = {}
                for uri, events in triggering_dataset_events.items():
                    if not isinstance(events, list):
                        continue

                    for event in events:
                        if not isinstance(event, DatasetEvent):
                            continue

                        extra = event.extra or {}
                        if not extra.get("ts", None):
                            extra.update({"ts": event.timestamp})
                        ds = Dataset(uri=uri, extra=extra)
                        if uri not in triggering_uris:
                            triggering_uris[uri] = event
                            dataset_uris.update({uri: ds})
                        else:
                            previous_event: DatasetEvent = triggering_uris[uri]
                            if event.timestamp > previous_event.timestamp:
                                triggering_uris[uri] = event
                                dataset_uris.update({uri: ds})

                return list(dataset_uris.values())

            def find_previous_dag_runs_api(
                    dag,
                    client: StarlakeAirflowApiClient,
                    scheduled_date: datetime,
                    at_scheduled_date: bool = False,
            ) -> List[DotDict]:
                """
                Find previous successful DagRuns for the current DAG, excluding runs
                where at least one leaf task is SKIPPED.

                Uses Airflow public API via StarlakeAirflowApiClient.
                """

                # ----------------------------------------------------------------------
                # 1. Identify leaf tasks
                # ----------------------------------------------------------------------
                leaves = dag.leaves
                leaf_task_ids = [t.task_id for t in leaves]
                logging.info("Leaf tasks to check: [%s]", ",".join(leaf_task_ids))

                # ----------------------------------------------------------------------
                # 2. Build DagRun query params
                # ----------------------------------------------------------------------
                dr_params: Dict[str, Any] = {
                    "state": "success",
                    "limit": 100,
                    "order_by": ["-data_interval_end", "-start_date"],
                }

                if at_scheduled_date:
                    dr_params["end_date_lte"] = scheduled_date.isoformat()
                else:
                    dr_params["end_date_lt"] = scheduled_date.isoformat()

                dag_runs = client.list_dag_runs(dag_id, **dr_params)

                # Extract end_date values
                end_dates = [dr.end_date for dr in dag_runs if dr.end_date]
                logging.info(
                    "Found %d candidate DagRuns, end_dates: [%s]",
                    len(dag_runs),
                    ",".join(end_dates),
                )

                max_end_date = max(end_dates) if end_dates else scheduled_date.isoformat()

                filtered_runs: List[DotDict] = []

                # ----------------------------------------------------------------------
                # 3. Build TaskInstance params (only valid keys)
                # ----------------------------------------------------------------------
                from airflow.utils.state import State

                ti_base_params = {
                    "limit": 1000,
                    "end_date_lte": max_end_date,
                    "order_by": ["-end_date", "-start_date"],
                    "state": State.SKIPPED,
                }

                # ----------------------------------------------------------------------
                # 4. Optimization: only one leaf → one API call
                # ----------------------------------------------------------------------
                if len(leaf_task_ids) == 1:
                    leaf = leaf_task_ids[0]
                    logging.info("Optimizing: only one leaf task (%s)", leaf)

                    ti_params = ti_base_params.copy()
                    ti_params["task_id"] = leaf

                    # "~" = all dag runs for this DAG
                    ti_list = client.list_task_instances(dag_id, "~", params=ti_params)

                    skipped_run_ids = {ti.dag_run_id for ti in ti_list}

                    for dr in dag_runs:
                        if dr.dag_run_id not in skipped_run_ids:
                            filtered_runs.append(dr)

                else:
                    # ------------------------------------------------------------------
                    # 5. Standard path: check each DagRun individually
                    # ------------------------------------------------------------------
                    for dr in dag_runs:
                        dag_run_id = dr.dag_run_id

                        ti_params = ti_base_params.copy()

                        ti_list = client.list_task_instances(
                            dag_id,
                            dag_run_id,
                            params=ti_params,
                        )

                        if any(ti.task_id in leaf_task_ids for ti in ti_list):
                            continue

                        filtered_runs.append(dr)

                # ----------------------------------------------------------------------
                # 6. Final sort (API already sorts, but we ensure correctness)
                # ----------------------------------------------------------------------
                def sort_key(run):
                    return (run.data_interval_end, run.start_date)

                filtered_runs.sort(key=sort_key, reverse=True)

                return filtered_runs

            @provide_session
            def find_previous_dag_runs(dag, scheduled_date: datetime, session: Session=None, at_scheduled_date: bool = False) -> List[DagRun]:
                # we look for the first non skipped dag run before the scheduled date 
                from airflow.utils.state import State

                leaves = dag.leaves
                last_tasks_id = [task.task_id for task in leaves]
                print(f"non skipped tasks to check [{','.join(last_tasks_id)}]")

                from sqlalchemy import and_
                from sqlalchemy.orm import aliased

                TI = aliased(TaskInstance)

                if at_scheduled_date:
                    # if we are at the scheduled date, we look for the last successful dag run before or at the scheduled date
                    print(f"Finding previous dag runs for {dag_id} at scheduled date {scheduled_date.strftime(sl_timestamp_format)}")
                    base_query = (
                        session.query(DagRun)
                        .filter(
                            DagRun.dag_id == dag_id,
                            DagRun.state == State.SUCCESS,
                            DagRun.data_interval_end <= scheduled_date
                        )
                        .order_by(DagRun.data_interval_end.desc(), DagRun.start_date.desc())
                    )

                    skipped_query = (
                        session.query(DagRun.id)
                        .join(TI, and_(
                            DagRun.dag_id == TI.dag_id,
                            DagRun.run_id == TI.run_id,
                            DagRun.state == State.SUCCESS,
                            DagRun.data_interval_end <= scheduled_date,
                            TI.task_id.in_(last_tasks_id),
                            TI.state == State.SKIPPED
                        ))
                        .distinct()
                    )

                else:
                    # if we are not at the scheduled date, we look for the last successful dag run before the scheduled date
                    print(f"Finding previous dag runs for {dag_id} before scheduled date {scheduled_date.strftime(sl_timestamp_format)}")
                    base_query = (
                        session.query(DagRun)
                        .filter(
                            DagRun.dag_id == dag_id,
                            DagRun.state == State.SUCCESS,
                            DagRun.data_interval_end < scheduled_date
                        )
                        .order_by(DagRun.data_interval_end.desc(), DagRun.start_date.desc())
                    )

                    skipped_query = (
                        session.query(DagRun.id)
                        .join(TI, and_(
                            DagRun.dag_id == TI.dag_id,
                            DagRun.run_id == TI.run_id,
                            DagRun.state == State.SUCCESS,
                            DagRun.data_interval_end < scheduled_date,
                            TI.task_id.in_(last_tasks_id),
                            TI.state == State.SKIPPED
                        ))
                        .distinct()
                    )

                filtered_query = (
                    base_query
                    .filter(~DagRun.id.in_(skipped_query))
                )

                return filtered_query.all()

            def find_datasets_events_api(
                    client: StarlakeAirflowApiClient,
                    uri: str,
                    scheduled_date_to_check_min: datetime,
                    scheduled_date_to_check_max: datetime,
                    ts: datetime,
                    scheduled_date: datetime,
            ) -> List[DotDict]:
                """
                API-based equivalent of the original SQLAlchemy-based find_dataset_events.

                Steps:
                1. Resolve dataset/asset ID from URI.
                2. Fetch events for this ID with timestamp <= ts, ordered by timestamp.
                3. Collect all producing DAG IDs from these events.
                4. For each producing DAG, list DagRuns in the data_interval_end window.
                5. Cross events with DagRuns via (source_dag_id, source_run_id).
                6. Sort events by producing DagRun.data_interval_end ascending.
                """

                # ----------------------------------------------------------------------
                # 1. Check feature support and resolve dataset/asset by URI
                # ----------------------------------------------------------------------
                if not supports_datasets() and not supports_assets():
                    logging.info("Datasets/assets not supported on this Airflow version")
                    return []

                logging.info("Resolving dataset/asset by uri=%s", uri)

                dataset: Optional[DotDict] = client.get_dataset_by_uri(uri)

                if not dataset:
                    logging.info("No dataset/asset found for uri=%s", uri)
                    return []

                dataset_or_asset_id: int = dataset.id

                logging.info(
                    "Resolved dataset/asset id=%s for uri=%s",
                    dataset_or_asset_id,
                    uri,
                )

                # ----------------------------------------------------------------------
                # 2. Fetch events for this ID with timestamp <= ts (ordered)
                # ----------------------------------------------------------------------
                logging.info(
                    "Listing dataset/asset events for id=%s with timestamp <= %s",
                    dataset_or_asset_id,
                    ts.isoformat(),
                )

                params_events: Dict[str, Any] = {
                    "timestamp_lte": ts.isoformat(),
                    "order_by": ["timestamp"],
                    "limit": 1000,
                }

                if supports_assets():
                    params_events["asset_id"] = dataset_or_asset_id
                else:
                    params_events["dataset_id"] = dataset_or_asset_id

                events: List[DotDict] = client.list_events(params=params_events)

                if not events:
                    logging.info(
                        "No events found for uri=%s and timestamp <= %s",
                        uri,
                        ts.isoformat(),
                    )
                    return []

                logging.info("Found %d events for uri=%s", len(events), uri)

                # ----------------------------------------------------------------------
                # 3. Collect producing DAG IDs from events
                # ----------------------------------------------------------------------
                producing_dag_ids = {ev.source_dag_id for ev in events if ev.source_dag_id}
                if not producing_dag_ids:
                    logging.info("No producing DAG IDs found in events for uri=%s", uri)
                    return []

                logging.info(
                    "Producing DAG IDs for uri=%s: [%s]",
                    uri,
                    ",".join(sorted(producing_dag_ids)),
                )

                # ----------------------------------------------------------------------
                # 4. For each producing DAG, list DagRuns in the data_interval_end window
                # ----------------------------------------------------------------------
                dag_run_index: Dict[tuple, DotDict] = {}

                dr_params_base: Dict[str, Any] = {
                    "order_by": ["data_interval_end", "start_date"],
                    "limit": 1000,
                }

                if scheduled_date_to_check_max > scheduled_date:
                    logging.info(
                        "Filtering DagRuns with data_interval_end >= %s and <= %s",
                        scheduled_date_to_check_min.isoformat(),
                        scheduled_date.isoformat(),
                    )
                    dr_params_base["data_interval_end_gte"] = scheduled_date_to_check_min.isoformat()
                    dr_params_base["data_interval_end_lte"] = scheduled_date.isoformat()
                else:
                    logging.info(
                        "Filtering DagRuns with data_interval_end > %s and <= %s",
                        scheduled_date_to_check_min.isoformat(),
                        scheduled_date_to_check_max.isoformat(),
                    )
                    dr_params_base["data_interval_end_gt"] = scheduled_date_to_check_min.isoformat()
                    dr_params_base["data_interval_end_lte"] = scheduled_date_to_check_max.isoformat()

                # Load DagRuns per producing DAG and index them by (dag_id, run_id)
                for prod_dag_id in producing_dag_ids:
                    try:
                        dag_runs = client.list_dag_runs(prod_dag_id, **dr_params_base)
                    except Exception as e:
                        logging.warning(
                            "Failed to list DagRuns for producing DAG %s: %s",
                            prod_dag_id,
                            e,
                        )
                        continue

                    for dr in dag_runs:
                        key = (dr.dag_id, dr.run_id)
                        dag_run_index[key] = dr

                if not dag_run_index:
                    logging.info(
                        "No DagRuns found in the data_interval_end window for any producing DAG of uri=%s",
                        uri,
                    )
                    return []

                logging.info(
                    "Indexed %d DagRuns for producing DAGs of uri=%s",
                    len(dag_run_index),
                    uri,
                )

                # ----------------------------------------------------------------------
                # 5. Cross events with DagRuns and filter on data_interval_end window
                # ----------------------------------------------------------------------
                filtered_events: List[DotDict] = []

                for ev in events:
                    key = (ev.source_dag_id, ev.source_run_id)
                    dr = dag_run_index.get(key)
                    if not dr:
                        # Either no DagRun for this event in the window or missing run
                        continue

                    ev.update({"dataset": dataset or {"extra": {}}})

                    # At this point, dr.data_interval_end is already within the desired window
                    filtered_events.append(ev)

                if not filtered_events:
                    logging.info(
                        "No events remained after crossing with DagRuns for uri=%s",
                        uri,
                    )
                    return []

                # ----------------------------------------------------------------------
                # 6. Sort by producing DagRun.data_interval_end ascending
                # ----------------------------------------------------------------------
                def sort_key(ev: DotDict):
                    dr = dag_run_index[(ev.source_dag_id, ev.source_run_id)]
                    return dr.data_interval_end

                filtered_events.sort(key=sort_key)

                logging.info(
                    "Returning %d filtered events for uri=%s",
                    len(filtered_events),
                    uri,
                )

                return filtered_events

            @provide_session
            def find_dataset_events(uri: str, scheduled_date_to_check_min: datetime, scheduled_date_to_check_max: datetime, ts: datetime, scheduled_date: datetime, session: Session=None) -> List[DatasetEvent]:
                from sqlalchemy import and_, asc
                from sqlalchemy.orm import joinedload
                from airflow.models.dataset import DatasetModel
                base_query = (
                    session.query(DatasetEvent)
                    .options(joinedload(DatasetEvent.dataset))
                    .join(DagRun, and_(
                        DatasetEvent.source_dag_id == DagRun.dag_id,
                        DatasetEvent.source_run_id == DagRun.run_id,
                        DatasetEvent.timestamp <= ts
                    ))
                    .join(DatasetModel, DatasetEvent.dataset_id == DatasetModel.id)
                )
                if scheduled_date_to_check_max > scheduled_date: 
                    # we should include the previous execution of the corresponding dataset
                    print(f'Finding dataset events for {uri} with data_interval_end >= {scheduled_date_to_check_min.strftime(sl_timestamp_format)} and <= {scheduled_date.strftime(sl_timestamp_format)}, and with timestamp <= {ts.strftime(sl_timestamp_format)}')
                    filtered_query = (
                        base_query.filter(
                            DagRun.data_interval_end >= scheduled_date_to_check_min,
                            DagRun.data_interval_end <= scheduled_date,
                            DatasetModel.uri == uri
                        )
                    )
                else:
                    print(f'Finding dataset events for {uri} with data_interval_end > {scheduled_date_to_check_min.strftime(sl_timestamp_format)} and <= {scheduled_date_to_check_max.strftime(sl_timestamp_format)}, and with timestamp <= {ts.strftime(sl_timestamp_format)}')
                    filtered_query = (
                        base_query.filter(
                            DagRun.data_interval_end > scheduled_date_to_check_min,
                            DagRun.data_interval_end <= scheduled_date_to_check_max,
                            DatasetModel.uri == uri
                        )
                    )
                
                return filtered_query.order_by(asc(DagRun.data_interval_end)).all()

            def ts_as_datetime(ts: Any) -> datetime:
                if isinstance(ts, datetime):
                    return ts
                else:
                    from dateutil import parser
                    import pytz
                    return parser.isoparse(str(ts)).astimezone(pytz.timezone('UTC'))

            @provide_session
            def check_datasets(scheduled_date: datetime, datasets: List[Dataset], ts: datetime, context: Context, session: Session=None) -> bool:
                from croniter import croniter
                missing_datasets = []
                max_scheduled_date = scheduled_date

                previous_dag_checked: Optional[datetime] = None
                last_dag_checked: Optional[datetime] = None
                last_dag_ts: Optional[datetime] = None

                dag = context["dag"]
                if supports_assets():
                    client = StarlakeAirflowApiClient()
                else:
                    client = None

                # we look for the first succeeded dag run before the scheduled date
                if client:
                    __dag_runs = find_previous_dag_runs_api(dag=dag, client=client, scheduled_date=scheduled_date, at_scheduled_date=False)
                else:
                    __dag_runs = find_previous_dag_runs(dag=dag, scheduled_date=scheduled_date, session=session, at_scheduled_date=False)

                if __dag_runs and len(__dag_runs) > 0:
                    # we take the first dag run before the scheduled date
                    __dag_run = __dag_runs[0]
                    previous_dag_checked = ts_as_datetime(__dag_run.data_interval_end)
                    print(f"Found previous succeeded dag run {__dag_run.dag_id} with scheduled date {previous_dag_checked} and start date {__dag_run.start_date}")

                if client:
                    __dag_runs = find_previous_dag_runs_api(dag=dag, client=client, scheduled_date=scheduled_date, at_scheduled_date=True)
                else:
                    __dag_runs = find_previous_dag_runs(dag=dag, scheduled_date=scheduled_date, session=session, at_scheduled_date=True)
                if __dag_runs and len(__dag_runs) > 0:
                    # we take the first dag run before the scheduled date
                    __dag_run = __dag_runs[0]
                    last_dag_checked = ts_as_datetime(__dag_run.data_interval_end)
                    last_dag_ts = ts_as_datetime(__dag_run.start_date)
                    print(f"Found last succeeded dag run {__dag_run.dag_id} with scheduled date {last_dag_checked} and start date {last_dag_ts}")

                if not previous_dag_checked:
                    # if the dag never run successfuly, 
                    # we set the previous dag checked to the start date of the dag
                    previous_dag_checked = dag.start_date
                    print(f"No previous succeeded dag run found, we set the previous dag checked to the start date of the dag {previous_dag_checked}")

                if last_dag_ts and last_dag_checked:
                    if last_dag_checked.strftime(sl_timestamp_format) == scheduled_date.strftime(sl_timestamp_format):
                        diff: timedelta = ts - last_dag_ts
                        if diff.total_seconds() <= self.min_timedelta_between_runs:
                            # we just run successfuly this dag, we should skip the current execution
                            print(f"The last succeeded dag run with scheduled date {last_dag_checked} started less than {self.min_timedelta_between_runs} seconds ago ({diff.seconds} seconds)... The current DAG execution at {ts.strftime(sl_timestamp_format)} will be skipped")
                            return False
                        else:
                            print(f"The last succeeded dag run with scheduled date {last_dag_checked} started more than {self.min_timedelta_between_runs} seconds ago ({diff.seconds} seconds)...")
                    else:
                        print(f"The last succeeded dag run with scheduled date {last_dag_checked} started at {last_dag_ts.strftime(sl_timestamp_format)}...")

                data_cycle_freshness = None
                if self.data_cycle:
                    # the freshness of the data cycle is the time delta between 2 iterations of its schedule
                    data_cycle_freshness = get_cron_frequency(self.data_cycle)

                print(f"Start date is {ts.strftime(sl_timestamp_format)} and scheduled date is {scheduled_date.strftime(sl_timestamp_format)}")

                # we retrieve the most frequent cron(s)
                all_crons = set()
                most_frequent = set()
                for dataset in datasets:
                    extra = dataset.extra or {}
                    cron = extra.get(StarlakeParameters.CRON_PARAMETER.value, None)
                    if cron:
                        all_crons.add(cron)
                if all_crons and len(all_crons) > 0:
                    most_frequent = set(most_frequent_crons(all_crons))

                # we check the datasets
                for dataset in datasets:
                    extra = dataset.extra or {}
                    original_cron = extra.get(StarlakeParameters.CRON_PARAMETER.value, None)
                    cron = original_cron or self.data_cycle
                    scheduled = cron and is_valid_cron(cron)
                    freshness = int(extra.get(StarlakeParameters.FRESHNESS_PARAMETER.value, 0))
                    optional = False
                    beyond_data_cycle_allowed = False
                    if data_cycle_freshness:
                        original_scheduled = original_cron and is_valid_cron(original_cron)
                        if self.optional_dataset_enabled:
                            # we check if the dataset is optional by comparing its freshness with that of the data cycle
                            # the freshness of a scheduled dataset is the time delta between 2 iterations of its schedule
                            # the freshness of a non scheduled dataset is defined by its freshness parameter
                            optional = (original_scheduled and abs(data_cycle_freshness.total_seconds()) < abs(get_cron_frequency(original_cron).total_seconds())) or (not original_scheduled and abs(data_cycle_freshness.total_seconds()) < freshness)
                        if self.beyond_data_cycle_enabled:
                            # we check if the dataset scheduled date is allowed to be beyond the data cycle by comparing its freshness with that of the data cycle
                            beyond_data_cycle_allowed = (original_scheduled and abs(data_cycle_freshness.total_seconds()) < abs(get_cron_frequency(original_cron).total_seconds() + freshness)) or (not original_scheduled and abs(data_cycle_freshness.total_seconds()) < freshness)
                    found = False
                    if optional:
                        print(f"Dataset {dataset.uri} is optional, we skip it")
                        continue
                    elif scheduled:
                        if not cron in most_frequent or cron.startswith('0 0') or get_cron_frequency(cron).days == 0:
                            dates_range = scheduled_dates_range(cron, scheduled_date)
                        else:
                            dates_range = scheduled_dates_range(cron, croniter(cron, scheduled_date.replace(hour=0, minute=0, second=0, microsecond=0)).get_next(datetime))
                        scheduled_date_to_check_min = dates_range[0]
                        scheduled_date_to_check_max = dates_range[1]
                        if not original_cron and previous_dag_checked > scheduled_date_to_check_min:
                            scheduled_date_to_check_min = previous_dag_checked
                        if beyond_data_cycle_allowed:
                            scheduled_date_to_check_min = scheduled_date_to_check_min - timedelta(seconds=freshness)
                            scheduled_date_to_check_max = scheduled_date_to_check_max + timedelta(seconds=freshness)
                        scheduled_datetime = get_scheduled_datetime(dataset)
                        if scheduled_datetime:
                            # we check if the scheduled datetime is between the scheduled date to check min and max
                            if scheduled_date_to_check_min >= scheduled_datetime or scheduled_datetime > scheduled_date_to_check_max:
                                # we will check within the inlet events
                                print(f"Triggering dataset {dataset.uri} with scheduled datetime {scheduled_datetime} not between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                            else:
                                found = True
                                print(f"Found trigerring dataset {dataset.uri} with scheduled datetime {scheduled_datetime} between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                                if scheduled_datetime > max_scheduled_date:
                                    max_scheduled_date = scheduled_datetime
                        if not found:
                            if client:
                                events = find_datasets_events_api(client=client, uri=dataset.uri, scheduled_date_to_check_min=scheduled_date_to_check_min, scheduled_date_to_check_max=scheduled_date_to_check_max, ts=ts, scheduled_date=scheduled_date)
                            else:
                                events = find_dataset_events(uri=dataset.uri, scheduled_date_to_check_min=scheduled_date_to_check_min, scheduled_date_to_check_max=scheduled_date_to_check_max, ts=ts, scheduled_date=scheduled_date, session=session)
                            if events:
                                dataset_events: Union[List[DotDict], List[DatasetEvent]] = events
                                nb_events = len(events)
                                print(f"Found {nb_events} dataset event(s) for {dataset.uri} between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                                dataset_event: Optional[Union[DotDict, DatasetEvent]] = None
                                i = 1
                                # we check the dataset events in reverse order
                                while i <= nb_events and not found:
                                    event: Union[DotDict, DatasetEvent] = dataset_events[-i]
                                    extra = event.extra or event.dataset.extra or dataset.extra or {}
                                    scheduled_datetime = get_scheduled_datetime(Dataset(uri=dataset.uri, extra=extra))
                                    if scheduled_datetime:
                                        if scheduled_date_to_check_min >= scheduled_datetime or scheduled_datetime > scheduled_date_to_check_max:
                                            print(f"Dataset event {event.id} for {dataset.uri} with scheduled datetime {scheduled_datetime} not between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                                            i += 1
                                        else:
                                            found = True
                                            print(f"Dataset event {event.id} for {dataset.uri} with scheduled datetime {scheduled_datetime} between {scheduled_date_to_check_min} and {scheduled_date_to_check_max} found")
                                            dataset_event = event
                                            if scheduled_datetime > max_scheduled_date:
                                                max_scheduled_date = scheduled_datetime
                                            break
                                    else:
                                        i += 1
                            if not found:
                                missing_datasets.append(dataset)
                    else:
                        # we check if one dataset event at least has been published since the previous dag checked and around the scheduled date +- freshness in seconds - it should be the closest one
                        scheduled_date_to_check_min = previous_dag_checked - timedelta(seconds=freshness)
                        scheduled_date_to_check_max = scheduled_date + timedelta(seconds=freshness)
                        scheduled_datetime: Optional[datetime] = None
                        dataset_event: Optional[Union[DotDict, DatasetEvent]] = None
                        if client:
                            events = find_datasets_events_api(client=client, uri=dataset.uri, scheduled_date_to_check_min=scheduled_date_to_check_min, scheduled_date_to_check_max=scheduled_date_to_check_max, ts=ts, scheduled_date=scheduled_date)
                        else:
                            events = find_dataset_events(uri=dataset.uri, scheduled_date_to_check_min=scheduled_date_to_check_min, scheduled_date_to_check_max=scheduled_date_to_check_max, ts=ts, scheduled_date=scheduled_date, session=session)
                        if events:
                            dataset_events: Union[List[DotDict], List[DatasetEvent]] = events
                            nb_events = len(events)
                            print(f"Found {nb_events} dataset event(s) for {dataset.uri} between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                            i = 1
                            # we check the dataset events in reverse order
                            while i <= nb_events and not found:
                                event: Union[DotDict, DatasetEvent] = dataset_events[-i]
                                extra = event.extra or event.dataset.extra or dataset.extra or {}
                                scheduled_datetime = get_scheduled_datetime(Dataset(uri=dataset.uri, extra=extra))
                                if scheduled_datetime:
                                    if scheduled_datetime > previous_dag_checked:
                                        if scheduled_date_to_check_min > scheduled_datetime:
                                            # we stop because all previous dataset events would be also before the scheduled date to check
                                            break
                                        elif scheduled_datetime > scheduled_date_to_check_max:
                                            i += 1
                                        else:
                                            found = True
                                            print(f"Dataset event {event.id} for {dataset.uri} with scheduled datetime {scheduled_datetime} after {previous_dag_checked} and  around the scheduled date {scheduled_date} +- {freshness} in seconds found")
                                            dataset_event = event
                                            if scheduled_datetime <= scheduled_date:
                                                # we stop because all previous dataset events would be also before the scheduled date but not closer than the current one
                                                break
                                    else:
                                        # we stop because all previous dataset events would be also before the previous dag checked
                                        break
                                else:
                                    i += 1
                        if not found or not scheduled_datetime:
                            missing_datasets.append(dataset)
                            print(f"No dataset event for {dataset.uri} found since the previous dag checked {previous_dag_checked} and around the scheduled date {scheduled_date} +- {freshness} in seconds")
                        else:
                            print(f"Found dataset event {dataset_event.id} for {dataset.uri} after the previous dag checked {previous_dag_checked}  and  around the scheduled date {scheduled_date} +- {freshness} in seconds")
                            if scheduled_datetime > max_scheduled_date:
                                max_scheduled_date = scheduled_datetime
                # if all the required datasets have been found, we can continue the dag
                checked = not missing_datasets
                if checked:
                    print(f"All datasets checked: {', '.join([dataset.uri for dataset in datasets])}")
                    print(f"Starlake start date will be set to {previous_dag_checked}")
                    context['task_instance'].xcom_push(key=StarlakeParameters.DATA_INTERVAL_START_PARAMETER.value, value=previous_dag_checked)
                    print(f"Starlake end date will be set to {max_scheduled_date}")
                    context['task_instance'].xcom_push(key=StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value, value=max_scheduled_date)
                return checked

            def should_continue(start_date: str = None, **context) -> bool:
                triggering_datasets = get_triggering_datasets(context)
                if not triggering_datasets:
                    print("No triggering datasets found. Manually triggered.")
                    return True
                else:
                    from dateutil import parser
                    import pytz
                    ts = parser.isoparse(start_date).astimezone(pytz.timezone('UTC'))
                    triggering_uris = {dataset.uri: dataset for dataset in triggering_datasets}
                    datasets_uris = {dataset.uri: dataset for dataset in datasets}
                    # we first retrieve the scheduled datetime of all the triggering datasets
                    triggering_scheduled = {dataset.uri: get_scheduled_datetime(dataset) for dataset in triggering_datasets}
                    # then we retrieve the triggering dataset with the greatest scheduled datetime
                    greatest_triggering_dataset: tuple = max(triggering_scheduled.items(), key=lambda x: x[1] or datetime.min, default=(None, None))
                    greatest_triggering_dataset_uri = greatest_triggering_dataset[0]
                    greatest_triggering_dataset_datetime = greatest_triggering_dataset[1]
                    # we then check the other datasets
                    checking_uris = list(set(datasets_uris.keys()) - set(greatest_triggering_dataset_uri))
                    checking_triggering_datasets = [dataset for dataset in triggering_datasets if dataset.uri in checking_uris]
                    checking_missing_datasets = [dataset for dataset in datasets if dataset.uri in list(set(checking_uris) - set(triggering_uris.keys()))]
                    checking_datasets = checking_triggering_datasets + checking_missing_datasets
                    return check_datasets(greatest_triggering_dataset_datetime or ts, checking_datasets, ts, context)

            inlets: list = kwargs.get("inlets", [])
            inlets += datasets
            kwargs.update({'inlets': inlets})
            kwargs.update({'doc': kwargs.get('doc', f'Check if the DAG should be started.')})
            kwargs.update({'pool': kwargs.get('pool', self.pool)})
            kwargs.update({'do_xcom_push': True})

            if len(datasets) > 0:
                return ShortCircuitOperator(
                        task_id = "start",
                        python_callable = should_continue,
                        op_args=[
                            "{{ dag_run.start_date }}"
                        ],
                        op_kwargs=kwargs,
                        trigger_rule = 'all_done',
                        max_active_tis_per_dag = 1,
                        **kwargs
                    )
            else:
                return super().start_op(task_id, scheduled, not_scheduled_datasets, least_frequent_datasets, most_frequent_datasets, **kwargs)
        else:
            return super().start_op(task_id, scheduled, not_scheduled_datasets, least_frequent_datasets, most_frequent_datasets, **kwargs)

    def sl_pre_load(self, domain: str, tables: set=set(), pre_load_strategy: Union[StarlakePreLoadStrategy, str, None] = None, **kwargs) -> Optional[BaseOperator]:
        """Overrides IStarlakeJob.sl_pre_load()
        Generate the Airflow group of tasks that will check if the conditions are met to load the specified domain according to the pre-load strategy choosen.

        Args:
            domain (str): The required domain to pre-load.
            tables (set): The optional tables to pre-load.
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The optional pre-load strategy to use.
        
        Returns:
            Optional[BaseOperator]: The Airflow task or None.
        """
        pre_load_strategy = self.pre_load_strategy if not pre_load_strategy else pre_load_strategy
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        kwargs.update({'do_xcom_push': True})
        kwargs.update({'doc': kwargs.get('doc', f'Pre-load for tables {",".join(list(tables or []))} within {domain} using {pre_load_strategy.value} strategy.')})
        return super().sl_pre_load(domain=domain, tables=tables, pre_load_strategy=pre_load_strategy, **kwargs)

    def skip_or_start_op(self, task_id: str, upstream_task: BaseOperator, **kwargs) -> Optional[BaseOperator]:
        """
        Args:
            task_id (str): The required task id.
            upstream_task (BaseOperator): The upstream task.
            **kwargs: The optional keyword arguments.

        Returns:
            Optional[BaseOperator]: The Airflow task or None.
        """
        def f_skip_or_start(upstream_task_id: str, **kwargs) -> bool:
            logger = logging.getLogger(__name__)

            return_value = kwargs['ti'].xcom_pull(task_ids=upstream_task_id, key='return_value')

            logger.warning(f"Upstream task {upstream_task_id} return value: {return_value}[{type(return_value)}]")

            if return_value is None:
                failed = True
                logger.error("No return value found in XCom.")
            elif isinstance(return_value, bool):
                failed = not return_value
            elif isinstance(return_value, int):
                failed = return_value
            elif isinstance(return_value, str):
                try:
                    import ast
                    parsed_return_value = ast.literal_eval(return_value)
                    if isinstance(parsed_return_value, bool):
                        failed = not parsed_return_value
                    elif isinstance(parsed_return_value, int):
                        failed = parsed_return_value
                    elif isinstance(parsed_return_value, str) and parsed_return_value:
                        failed = int(parsed_return_value.strip())
                    else:
                        failed = True
                        logger.error(f"Parsed return value {parsed_return_value}[{type(parsed_return_value)}] is not a valid bool, integer or is empty.")
                except (ValueError, SyntaxError) as e:
                    failed = True
                    logger.error(f"Error parsing return value: {e}")
            else:
                failed = True
                logger.error("Return value is not a valid bool, integer or string.")

            logger.warning(f"Failed: {failed}")

            return not failed

        kwargs.update({'pool': kwargs.get('pool', self.pool)})

        if not isinstance(upstream_task, BaseOperator):
            raise ValueError("The upstream task must be an instance of BaseOperator.")
        upstream_task_id = upstream_task.task_id
        task_id = task_id or f"validating_{upstream_task_id.split('.')[-1]}"
        kwargs.pop("task_id", None)

        return ShortCircuitOperator(
            task_id = task_id,
            python_callable = f_skip_or_start,
            op_args=[upstream_task_id],
            op_kwargs=kwargs,
            trigger_rule = 'all_done',
            **kwargs
        )

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: Optional[StarlakeSparkConfig] = None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> BaseOperator:
        """Overrides IStarlakeJob.sl_load()
        Generate the Airflow task that will run the starlake `load` command.

        Args:
            task_id (str): The optional task id ({domain}_{table}_load by default).
            domain (str): The required domain of the table to load.
            table (str): The required table to load.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
            dataset (Optional[Union[StarlakeDataset, str]]): The optional dataset to materialize.
        
        Returns:
            BaseOperator: The Airflow task.
        """
        kwargs.update({'doc': kwargs.get('doc', f'Load table {table} within {domain} domain.')})
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        return super().sl_load(task_id=task_id, domain=domain, table=table, spark_config=spark_config, dataset=dataset, **kwargs)

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str=None, spark_config: Optional[StarlakeSparkConfig] = None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> BaseOperator:
        """Overrides IStarlakeJob.sl_transform()
        Generate the Airflow task that will run the starlake `transform` command.

        Args:
            task_id (str): The optional task id ({transform_name} by default).
            transform_name (str): The transform to run.
            transform_options (str): The optional transform options to use.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
            dataset (Optional[Union[StarlakeDataset, str]]): The optional dataset to materialize.
        
        Returns:
            BaseOperator: The Airflow task.
        """
        kwargs.update({'doc': kwargs.get('doc', f'Run {transform_name} transform.')})
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        return super().sl_transform(task_id=task_id, transform_name=transform_name, transform_options=transform_options, spark_config=spark_config, dataset=dataset,  **kwargs)

    def dummy_op(self, task_id, events: Optional[List[Dataset]] = None, task_type: Optional[TaskType] = TaskType.EMPTY, **kwargs) -> BaseOperator :
        """Dummy op.
        Generate a Airflow dummy op.

        Args:
            task_id (str): The required task id.
            events (Optional[List[Dataset]]): The optional events to materialize.
            task_type (Optional[TaskType]): The optional task type.

        Returns:
            BaseOperator: The Airflow task.
        """

        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        outlets: List[Dataset] = kwargs.get("outlets", [])
        if events:
            outlets += events
        kwargs.update({'outlets': outlets})
        return EmptyOperator(task_id=task_id, **kwargs)

    def default_dag_args(self) -> dict:
        import json
        from json.decoder import JSONDecodeError
        dag_args = DEFAULT_DAG_ARGS
        try:
            dag_args.update(json.loads(__class__.get_context_var(var_name="default_dag_args", options=self.options)))
        except (MissingEnvironmentVariable, JSONDecodeError):
            pass
        dag_args.update({'start_date': self.start_date, 'retry_delay': timedelta(seconds=self.retry_delay), 'retries': self.retries})
        return dag_args

from airflow.lineage import prepare_lineage
import jinja2

class StarlakeDatasetMixin:
    """Mixin to update Airflow outlets with Starlake datasets."""
    def __init__(self, 
                 task_id: str, 
                 dataset: Optional[Union[str, StarlakeDataset]] = None, 
                 source: Optional[str] = None, 
                 **kwargs
                 ) -> None:
        self.task_id = task_id
        params: dict = kwargs.get("params", dict())
        # cron: Optional[str] = params.get('cron', None)
        outlets: list = kwargs.get("outlets", [])
        extra = dict()
        extra.update({"source": source})
        if dataset:
            if isinstance(dataset, StarlakeDataset):
                params.update({
                    'uri': dataset.uri,
                    'cron': dataset.cron, # cron or dataset.cron
                    'sl_schedule_parameter_name': dataset.sl_schedule_parameter_name, 
                    'sl_schedule_format': dataset.sl_schedule_format
                })
                kwargs['params'] = params
                extra.update({
                    StarlakeParameters.URI_PARAMETER.value: dataset.uri,
                    StarlakeParameters.SINK_PARAMETER.value: dataset.sink,
                    StarlakeParameters.CRON_PARAMETER.value: dataset.cron, # cron or dataset.cron
                    StarlakeParameters.FRESHNESS_PARAMETER.value: dataset.freshness,
                })
                if dataset.cron: # if the dataset is scheduled
                    self.scheduled_dataset = "{{sl_scheduled_dataset(params.uri, params.cron, ts_as_datetime(data_interval_end | ts), params.sl_schedule_parameter_name, params.sl_schedule_format)}}"
                else:
                    self.scheduled_dataset = None
                self.scheduled_date = "{{sl_scheduled_date(params.cron, ts_as_datetime(data_interval_end | ts))}}"
                uri = dataset.uri
            else:
                self.scheduled_dataset = None
                uri = dataset
                params.update({
                    'uri': uri,
                    'cron': None,
                    'sl_schedule_parameter_name': None,
                    'sl_schedule_format': None
                })
                kwargs['params'] = params
                self.scheduled_date = "{{sl_scheduled_date(params.cron, ts_as_datetime(data_interval_end | ts))}}"
            outlets.append(Dataset(uri=uri, extra=extra))
            kwargs["outlets"] = outlets
            self.template_fields = getattr(self, "template_fields", tuple()) + ("scheduled_dataset", "scheduled_date",)
        else:
            self.scheduled_dataset = None
            self.scheduled_date = None
        self.extra = extra
        super().__init__(task_id=task_id, **kwargs)  # Appelle l'init de l'opérateur principal

    def render_template_fields(
            self,
            context: Context,
            jinja_env: jinja2.Environment | None = None,
        ) -> None:
        dag = context.get('dag')
        __ts_as_datetime = dag.user_defined_macros.get('ts_as_datetime', None) if dag.user_defined_macros else None
        if not __ts_as_datetime:
            def ts_as_datetime(ts, context: Context = None):
                from datetime import datetime
                if not context:
                    from airflow.operators.python import get_current_context
                    context = get_current_context()
                ti: TaskInstance = context["task_instance"]
                sl_logical_date = ti.xcom_pull(task_ids="start", key=StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value)
                if sl_logical_date:
                    ts = sl_logical_date
                if isinstance(ts, str):
                    from dateutil import parser
                    import pytz
                    return parser.isoparse(ts).astimezone(pytz.timezone('UTC'))
                elif isinstance(ts, datetime):
                    return ts

            print(f"add 'ts_as_datetime' to context")
            context['ts_as_datetime'] = ts_as_datetime

        __sl_scheduled_dataset = dag.user_defined_macros.get('sl_scheduled_dataset', None) if dag.user_defined_macros else None
        if not __sl_scheduled_dataset:
            print(f"add 'sl_scheduled_dataset' to context")
            from ai.starlake.common import sl_scheduled_dataset
            context['sl_scheduled_dataset'] = sl_scheduled_dataset

        __sl_scheduled_date = dag.user_defined_macros.get('sl_scheduled_date', None) if dag.user_defined_macros else None
        if not __sl_scheduled_date:
            print(f"add 'sl_scheduled_date' to context")
            from ai.starlake.common import sl_scheduled_date
            context['sl_scheduled_date'] = sl_scheduled_date

        return super().render_template_fields(context, jinja_env)

    @prepare_lineage
    def pre_execute(self, context: Context):
        if not context:
            from airflow.operators.python import get_current_context
            context = get_current_context()

        ti: TaskInstance = context.get('ti')
        ts: datetime = ti.start_date or datetime.fromtimestamp(datetime.now().timestamp()).astimezone(pytz.timezone('UTC'))

        self.extra.update({"ts": ts.strftime(sl_timestamp_format)})
        if self.scheduled_date:
            self.extra.update({StarlakeParameters.SCHEDULED_DATE_PARAMETER.value: self.scheduled_date})
        if self.scheduled_dataset:
            dataset = Dataset(uri=self.scheduled_dataset, extra=self.extra)
            self.outlets.append(dataset)
        for outlet in self.outlets:
            outlet_event = context["outlet_events"][outlet]
            self.log.info(f"updating outlet event {outlet_event} with extra {self.extra}")
            outlet_event.extra = self.extra
        return super().pre_execute(context)

class StarlakeEmptyOperator(StarlakeDatasetMixin, EmptyOperator):
    """StarlakeEmptyOperator."""
    def __init__(self, 
                 task_id: str, 
                 dataset: Optional[Union[str, StarlakeDataset]] = None, 
                 source: Optional[str] = None, 
                 **kwargs
        ) -> None:
        super().__init__(
            task_id=task_id, 
            dataset=dataset,
            source=source,
            **kwargs
        )
