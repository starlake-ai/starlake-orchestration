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

from typing import Any, Dict, NamedTuple, Optional, List, Union

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOrchestrator, TaskType

from ai.starlake.airflow.starlake_airflow_options import StarlakeAirflowOptions

from ai.starlake.airflow.starlake_airflow_api import DotDict, StarlakeAirflowApiClient

from ai.starlake.common import MissingEnvironmentVariable, get_cron_frequency, is_valid_cron, StarlakeParameters, sl_timestamp_format, most_frequent_crons, scheduled_dates_range, sl_schedule_format

from ai.starlake.job.starlake_job import StarlakeOrchestrator

from ai.starlake.dataset import StarlakeDataset, AbstractEvent


from ai.starlake.airflow.compat import (
    AirflowSkipException,
    BaseOperator,
    BaseSensorOperator,
    Dataset,
    EmptyOperator,
    PokeReturnValue,
    ShortCircuitOperator,
    TaskGroup,
    get_current_context,
    supports_assets,
    supports_inlet_events,
)

from airflow.exceptions import AirflowException

from airflow.models import DagRun, TaskInstance

from airflow.utils.context import Context

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

class PreLoadWait(NamedTuple):
    """Resolved pre-load waiting configuration for a cloud execution
    environment (story 6.5, issue #93).

    ``mode`` is ``'deferrable'`` (the native deferrable operator re-submits
    preload via Airflow ``retries``/``retry_delay``) or ``'sensor'`` (a
    reschedule-mode ``BaseSensorOperator`` submits one preload run per poke).
    The four ``pre_load_*`` values are resolved and validated by core
    ``sl_pre_load`` (story 6.2); ``retries``/``retry_delay`` are the deferrable
    mapping of ``timeout``/``poke_interval``.
    """
    mode: str
    poke_interval: int
    timeout: int
    soft_fail: bool
    retries: int
    retry_delay: timedelta

def sl_options_from_events(triggering_dataset_events, dag_run=None, name: Optional[str] = None) -> str:
    """Jinja macro rendering the runtime starlake --options fragment carried by the
    triggering dataset events (StarlakeParameters.OPTIONS_PARAMETER in the event
    extra, as a dict of sections: {"all": {key: value}, "<domain.task>": {key: value}}).
    The 'all' section applies to every transformation of the triggered pipeline; the
    section keyed by the transformation name only to that one. The fragment is
    appended last to the command's --options, whose duplicate keys are resolved
    last-wins by starlake — hence the precedence: static options < 'all' < task-specific.
    Merging is fail-loud: the same key carried with different values by coalesced
    events raises (conflicting run variables must stop the run, not silently pick
    one). dag_run.conf[StarlakeParameters.OPTIONS_PARAMETER] overrides events — the
    manual-recovery escape hatch. Returns 'key=value[,key=value...]', or
    'sl_options_applied=0' when nothing applies (never empty, so the fragment is
    always a valid --options token)."""
    sections: dict = {}
    for uri, events in (triggering_dataset_events or {}).items():
        for event in events or []:
            extra = getattr(event, "extra", None) or {}
            event_sections = extra.get(StarlakeParameters.OPTIONS_PARAMETER.value) or {}
            if not isinstance(event_sections, dict):
                continue
            for section, opts in event_sections.items():
                if not isinstance(opts, dict):
                    continue
                merged = sections.setdefault(section, {})
                for key, value in opts.items():
                    if key in merged and str(merged[key]) != str(value):
                        from airflow.exceptions import AirflowException
                        raise AirflowException(
                            f"Conflicting values for {StarlakeParameters.OPTIONS_PARAMETER.value}['{section}']['{key}'] across triggering dataset events ({uri}): "
                            f"'{merged[key]}' != '{value}'. Coalesced events carry different run variables — re-trigger the runs one by one, "
                            f"passing dag_run.conf['{StarlakeParameters.OPTIONS_PARAMETER.value}']."
                        )
                    merged[key] = value
    conf = getattr(dag_run, "conf", None) or {}
    conf_sections = conf.get(StarlakeParameters.OPTIONS_PARAMETER.value) or {}
    if isinstance(conf_sections, dict):
        for section, opts in conf_sections.items():
            if isinstance(opts, dict):
                sections.setdefault(section, {}).update(opts)
    options = dict(sections.get("all", {}))
    if name:
        options.update(sections.get(name, {}))
    if not options:
        return "sl_options_applied=0"
    return ",".join(f"{key}={value}" for key, value in options.items())

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
        return Dataset(uri=dataset.uri, extra=extra)

class StarlakeAirflowJob(IStarlakeJob[BaseOperator, Dataset], StarlakeAirflowOptions, AirflowDataset):
    """
    Starlake Airflow 3 Job Operator.

    This operator handles the execution of Starlake jobs within Airflow 3.
    It supports:
    - Automatic dataset/asset awareness: It can check if input datasets (Assets) are fresh enough before running.
    - Data Cycle management: Verification of data dependencies (acks/nacks).
    - Execution of Starlake commands (extract, load, transform).
    """
    ui_color = '#7c7287'
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

    def start_op(self, task_id: str, scheduled: bool, not_scheduled_datasets: Optional[List[StarlakeDataset]], least_frequent_datasets: Optional[List[StarlakeDataset]], most_frequent_datasets: Optional[List[StarlakeDataset]], **kwargs) -> Optional[BaseOperator]:
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
        if not scheduled:
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
                    context = get_current_context()

                ti = context["task_instance"]
                template_ctx = ti.get_template_context()

                # Airflow 3.x: triggering_asset_events / Airflow 2.4+: triggering_dataset_events
                triggering_dataset_events = []
                if "triggering_asset_events" in template_ctx:
                    triggering_dataset_events = template_ctx["triggering_asset_events"]
                elif "triggering_dataset_events" in template_ctx:
                    triggering_dataset_events = template_ctx["triggering_dataset_events"]

                if not triggering_dataset_events:
                    # No triggering assets
                    return []

                triggering_uris = {}
                dataset_uris = {}
                for uri, events in triggering_dataset_events.items():
                    if not isinstance(events, list):
                        continue

                    for event in events:
                        if type(event).__name__ not in ("AssetEvent", "DatasetEvent"):
                            continue

                        extra = event.extra or {}
                        if not extra.get("ts", None):
                            extra.update({"ts": event.timestamp})
                        ds = Dataset(uri=uri, extra=extra)
                        if uri not in triggering_uris:
                            triggering_uris[uri] = event
                            dataset_uris.update({uri: ds})
                        else:
                            previous_event = triggering_uris[uri]
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

                Executed as a single anti-join query against the Airflow 2 metadata
                database, or composed from the paginated REST primitives otherwise
                (see StarlakeAirflowApiClient.find_previous_dag_runs).
                """
                leaf_task_ids = [task.task_id for task in dag.leaves]
                logging.info("Leaf tasks to check: [%s]", ",".join(leaf_task_ids))
                return client.find_previous_dag_runs(
                    dag_id,
                    scheduled_date,
                    leaf_task_ids,
                    at_scheduled_date=at_scheduled_date,
                )



            def find_datasets_events_api(
                    client: StarlakeAirflowApiClient,
                    uri: str,
                    scheduled_date_to_check_min: datetime,
                    scheduled_date_to_check_max: datetime,
                    ts: datetime,
                    scheduled_date: datetime,
            ) -> List[DotDict]:
                """
                Dataset/asset events for ``uri`` produced by DagRuns whose
                data_interval_end falls in the checked window, sorted by that
                data_interval_end ascending, each with the dataset attached.

                The window applies to the producing run, not to event recency, so
                replaying a DAG for an arbitrarily old date finds its events.
                Executed as a single joined query against the Airflow 2 metadata
                database, or composed from the paginated REST primitives otherwise
                (see StarlakeAirflowApiClient.find_dataset_events).
                """
                if scheduled_date_to_check_max > scheduled_date:
                    # we should include the previous execution of the corresponding dataset
                    logging.info(
                        "Finding dataset events for %s with data_interval_end >= %s and <= %s, and with timestamp <= %s",
                        uri,
                        scheduled_date_to_check_min.isoformat(),
                        scheduled_date.isoformat(),
                        ts.isoformat(),
                    )
                    window = {
                        "data_interval_end_gte": scheduled_date_to_check_min,
                        "data_interval_end_lte": scheduled_date,
                    }
                else:
                    logging.info(
                        "Finding dataset events for %s with data_interval_end > %s and <= %s, and with timestamp <= %s",
                        uri,
                        scheduled_date_to_check_min.isoformat(),
                        scheduled_date_to_check_max.isoformat(),
                        ts.isoformat(),
                    )
                    window = {
                        "data_interval_end_gt": scheduled_date_to_check_min,
                        "data_interval_end_lte": scheduled_date_to_check_max,
                    }
                events = client.find_dataset_events(uri, ts, **window)
                logging.info("Returning %d filtered events for uri=%s", len(events), uri)
                return events



            def ts_as_datetime(ts: Any) -> datetime:
                if isinstance(ts, datetime):
                    return ts
                else:
                    from dateutil import parser
                    import pytz
                    return parser.isoparse(str(ts)).astimezone(pytz.timezone('UTC'))

            def check_datasets(scheduled_date: datetime, datasets: List[Dataset], ts: datetime, context: Context) -> bool:
                from croniter import croniter
                # We start by initializing the result to True (datasets are present)
                # We will set it to False if any required dataset is missing.
                dataset_res = True

                # We also track if we found at least one dataset event if checked via API
                found_at_least_one = False

                # Iterate over all datasets that this job depends on
                missing_datasets = []
                max_scheduled_date = scheduled_date

                previous_dag_checked: Optional[datetime] = None
                last_dag_checked: Optional[datetime] = None
                last_dag_ts: Optional[datetime] = None

                dag = context["dag"]
                client = StarlakeAirflowApiClient()

                # we look for the first succeeded dag run before the scheduled date
                __dag_runs = find_previous_dag_runs_api(dag=dag, client=client, scheduled_date=scheduled_date, at_scheduled_date=False)

                if __dag_runs and len(__dag_runs) > 0:
                    # we take the first dag run before the scheduled date
                    __dag_run = __dag_runs[0]
                    previous_dag_checked = ts_as_datetime(__dag_run.data_interval_end)
                    print(f"Found previous succeeded dag run {__dag_run.dag_id} with scheduled date {previous_dag_checked} and start date {__dag_run.start_date}")

                __dag_runs = find_previous_dag_runs_api(dag=dag, client=client, scheduled_date=scheduled_date, at_scheduled_date=True)
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
                            events = find_datasets_events_api(client=client, uri=dataset.uri, scheduled_date_to_check_min=scheduled_date_to_check_min, scheduled_date_to_check_max=scheduled_date_to_check_max, ts=ts, scheduled_date=scheduled_date)
                            if events:
                                dataset_events: Union[List[DotDict], List] = events
                                nb_events = len(events)
                                print(f"Found {nb_events} dataset event(s) for {dataset.uri} between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                                dataset_event = None
                                i = 1
                                # we check the dataset events in reverse order
                                while i <= nb_events and not found:
                                    event = dataset_events[-i]
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
                        scheduled_datetime = None
                        dataset_event = None
                        events = find_datasets_events_api(client=client, uri=dataset.uri, scheduled_date_to_check_min=scheduled_date_to_check_min, scheduled_date_to_check_max=scheduled_date_to_check_max, ts=ts, scheduled_date=scheduled_date)
                        if events:
                            dataset_events = events
                            nb_events = len(events)
                            print(f"Found {nb_events} dataset event(s) for {dataset.uri} between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                            i = 1
                            # we check the dataset events in reverse order
                            while i <= nb_events and not found:
                                event = dataset_events[-i]
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
        pre_load_strategy = self.__class__.sl_resolve_pre_load_strategy(
            pre_load_strategy,
            default=self.pre_load_strategy,
            action="sl_pre_load(pre_load_strategy=...)",
        )
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        kwargs.update({'do_xcom_push': True})
        kwargs.update({'doc': kwargs.get('doc', f'Pre-load for tables {",".join(list(tables or []))} within {domain} using {pre_load_strategy.value} strategy.')})
        return super().sl_pre_load(domain=domain, tables=tables, pre_load_strategy=pre_load_strategy, **kwargs)

    # -- story 6.5 (issue #93): pre-load waiting on cloud execution ----------
    # environments. The four ``pre_load_*`` sensor kwargs (resolved + validated
    # by core ``sl_pre_load``, story 6.2) are now HONORED on cloud engines, not
    # rejected. All the decision logic (capability detection, mode selection,
    # option→retry mapping, terminal-state verdicts) lives here in the
    # provider-free base module so it is unit-testable in CI, which installs no
    # google/amazon provider packages; the per-engine operator/sensor subclasses
    # in the gcp/aws modules stay thin and provider-guarded.

    @classmethod
    def _sl_operator_supports_deferrable(cls, operator_cls) -> bool:
        """Definition-time detection of deferrable support.

        Returns whether the operator class's ``__init__`` accepts a
        ``deferrable`` parameter (the primary, version-independent signal;
        callers may layer a provider-version heuristic on top). A running
        triggerer cannot be verified at parse time — the ``pre_load_deferrable``
        opt-out forces the sensor-flavor fallback when none is available.

        Never raises: an unintrospectable class yields ``False`` (→ sensor).
        """
        import inspect
        try:
            signature = inspect.signature(operator_cls.__init__)
        except (TypeError, ValueError):
            return False
        return 'deferrable' in signature.parameters

    @classmethod
    def _sl_select_pre_load_wait_mode(cls, supports_deferrable: bool, deferrable_enabled: bool) -> str:
        """Pick the waiting implementation: ``'deferrable'`` iff the operator
        supports it AND ``pre_load_deferrable`` is enabled, else ``'sensor'``."""
        return 'deferrable' if (supports_deferrable and deferrable_enabled) else 'sensor'

    @classmethod
    def _sl_deferrable_retry_params(cls, poke_interval: int, timeout: int):
        """Map the sensor option surface onto Airflow retry semantics for the
        deferrable path: ``retries = max(1, timeout // poke_interval)`` and
        ``retry_delay = timedelta(seconds=poke_interval)``, so ``retries``
        re-submit preload naturally within (a floor of) the wall-clock window.

        Known trade-off (documented): each empty poke is a recorded task
        failure — an attempt-count window rather than pure wall-clock.
        """
        # max(1, ...) on the interval guards against a ZeroDivisionError at DAG
        # parse (core validates positivity, but this helper is independently
        # callable and the resolver re-reads the raw kwarg).
        poke_interval = max(1, int(poke_interval))
        timeout = int(timeout)
        retries = max(1, timeout // poke_interval)
        return retries, timedelta(seconds=poke_interval)

    @classmethod
    def _sl_is_last_attempt(cls, try_number: int, max_tries: int) -> bool:
        """Whether the current attempt is the terminal one (no retry left).

        ``max_tries`` equals the operator ``retries``; Airflow runs attempts
        ``try_number`` ``1..max_tries+1`` and the last is ``try_number > max_tries``.
        Isolated here because the ``try_number``/``max_tries`` semantics are
        version-fragile — read them at the call site, test the pure rule.
        """
        return try_number > max_tries

    @classmethod
    def _sl_resolve_cloud_pre_load_wait(cls, kwargs: dict, options: dict, operator_cls) -> Optional['PreLoadWait']:
        """Pop the four ``pre_load_*`` sensor kwargs and resolve the cloud
        waiting configuration (story 6.5, issue #93).

        Returns ``None`` when sensor mode is off — the kwargs are popped so the
        one-shot operator construction is byte-identical to today (zero-change
        guarantee). When on: resolves the cloud-Airflow-only ``pre_load_deferrable``
        opt-out (default true, strict NFR11 bool) from ``options``, detects
        deferrable support on ``operator_cls`` and selects the mode.

        ``operator_cls`` is passed in by the (provider-importing) cloud module
        so this resolver stays provider-free and unit-testable with fakes. Pass
        ``None`` when the engine has no deferrable operator (e.g. the cloud_run
        gcloud/bash path) to force the sensor-flavor.
        """
        pre_load_sensor = bool(kwargs.pop('pre_load_sensor', False))
        poke_interval = int(kwargs.pop('pre_load_poke_interval', 300))
        timeout = int(kwargs.pop('pre_load_timeout', 3600))
        soft_fail = bool(kwargs.pop('pre_load_sensor_soft_fail', False))
        # pre_load_deferrable is a cloud-Airflow-only option core knows nothing
        # about — pop it UNCONDITIONALLY (even when sensor mode is off) so it can
        # never leak into a provider operator constructor as an unexpected kwarg.
        # A per-call kwarg wins over the option (consistent with the four sensor
        # kwargs, which core resolves kwarg > option).
        deferrable_kwarg = kwargs.pop('pre_load_deferrable', None)
        if not pre_load_sensor:
            return None
        deferrable_enabled = cls._sl_parse_strict_bool(
            'pre_load_deferrable',
            deferrable_kwarg if deferrable_kwarg is not None
            else cls.get_context_var(
                var_name='pre_load_deferrable',
                default_value='true',
                options=options or {},
            ),
        )
        supports_deferrable = operator_cls is not None and cls._sl_operator_supports_deferrable(operator_cls)
        mode = cls._sl_select_pre_load_wait_mode(supports_deferrable, deferrable_enabled)
        retries, retry_delay = cls._sl_deferrable_retry_params(poke_interval, timeout)
        return PreLoadWait(
            mode=mode,
            poke_interval=poke_interval,
            timeout=timeout,
            soft_fail=soft_fail,
            retries=retries,
            retry_delay=retry_delay,
        )

    @classmethod
    def _sl_pre_load_poke_verdict(cls, succeeded: bool) -> Optional[PokeReturnValue]:
        """Sensor-flavor poke verdict for a preload submission (story 6.5).

        Success → ``PokeReturnValue(True, True)`` (done; truthy ``return_value``
        XCom → ``skip_or_start`` proceeds). No files yet → ``None`` (poke again
        after ``poke_interval``). Never raises: the wall-clock ``timeout`` /
        ``soft_fail`` window is ``BaseSensorOperator``'s own concern. This is
        DISTINCT from the #92 one-shot swallow, which does not distinguish
        no-files-yet from terminal — do not route waiting through it.
        """
        if succeeded:
            return PokeReturnValue(True, True)
        return None

    @classmethod
    def _sl_deferrable_pre_load_verdict(cls, succeeded: bool, is_last_attempt: bool, soft_fail: bool, message: str) -> bool:
        """Deferrable-path terminal-state verdict for a preload attempt (story 6.5).

        Success → ``True`` (truthy ``return_value`` XCom → ``skip_or_start``
        proceeds). A non-terminal failure raises ``AirflowException`` so Airflow
        re-submits preload on the next attempt (retry = poke). The terminal
        attempt maps to a ``AirflowSkipException`` (green skip, no XCom →
        downstream skipped) when ``soft_fail`` else ``AirflowException`` (red).

        Raises:
            AirflowSkipException | AirflowException: on failure (see above).
        """
        if succeeded:
            return True
        if is_last_attempt and soft_fail:
            raise AirflowSkipException(message)
        raise AirflowException(message)

    @classmethod
    def _sl_xcom_wrapped_command(cls, command: str, preload: bool) -> str:
        """Wrap a bash command in the echo/XCom wrapper (story 6.3, issue #92).

        Single source for the wrapper previously duplicated across the bash
        and cloud_run gcloud paths. Two variants, selected by the task type
        (never by ``do_xcom_push``, which defaults to True and is forced for
        structural XCom plumbing):

        - ``preload=True``: the exit code is SWALLOWED — echoed to XCom for
          the downstream ``skip_or_start`` ShortCircuitOperator (``0`` →
          proceed, non-zero → skip); the task itself ends green. This is the
          one task type designed around XCom gating.
        - ``preload=False`` (load/transform/stage): the exit code is echoed
          AND re-raised via the active ``exit $return_code`` trailer — a
          failed job must fail the task.

        The wrapper owns the quoting contract (story 6.4, issue #95): it is
        a FLAT script — no nested ``bash -c '...'`` — so the command's own
        quotes (``--scheduledDate '...'``, apostrophes in ``--options``
        values, gcloud ``--format='...'``) are parsed exactly once, by the
        same bash that runs the raw unwrapped command. Call sites must pass
        the command untouched (no escaping, no quote substitution). No
        ``set -e`` either: it would abort a failing command before
        ``return_code=$?`` captures it. The echo stays the LAST line of the
        preload variant — BashOperator pushes the last stdout line as the
        ``return_value`` XCom that ``f_skip_or_start`` int-parses.

        Lives in this provider-free base module so the contract stays
        testable without the google/amazon provider packages.
        """
        if preload:
            return f"""
{command}
return_code=$?

# Push the return code to XCom
echo $return_code
"""
        else:
            return f"""
{command}
return_code=$?

# Push the return code to XCom
echo $return_code

# Exit with the captured return code if non-zero
if [ $return_code -ne 0 ]; then
    exit $return_code
fi
"""

    @classmethod
    def _sl_cloud_failure_swallowed(cls, preload: bool, retry_on_failure: bool) -> bool:
        """Whether a cloud operator may swallow a failed job (story 6.3, issue #92).

        Only PRELOAD with ``retry_on_failure=false`` swallows (its failure is
        gated through the ``skip_or_start`` XCom composition); every other
        combination must propagate — a failed load/transform/stage reports a
        failed task, and ``retry_on_failure=true`` re-raises even for preload
        (the retries-as-poke workaround documented in #91).
        """
        return preload and not retry_on_failure

    @classmethod
    def _sl_cloud_poke_failure(cls, preload: bool, message: str) -> PokeReturnValue:
        """Failure verdict for a cloud completion sensor poke (story 6.3, issue #92).

        For PRELOAD the sensor completes with a falsy XCom
        (``PokeReturnValue(True, False)``) so ``skip_or_start`` skips the
        downstream loads — the swallow is the gating design. For every other
        task type the sensor must FAIL the chain: ``PokeReturnValue`` truthiness
        is ``is_done``, so returning it would end the sensor green.

        Raises:
            AirflowException: when ``preload`` is False.
        """
        if preload:
            return PokeReturnValue(True, False)
        raise AirflowException(message)

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

        The transform options are extended at runtime with the sl_options carried
        by the triggering dataset events (StarlakeParameters.OPTIONS_PARAMETER in
        the event extra): the 'all' section applies to every transformation, the
        section keyed by the transformation name only to this one. See
        sl_options_from_events for the merge/precedence/fail-loud semantics.
        (The template context key is `triggering_dataset_events` since Airflow 2.5,
        renamed `triggering_asset_events` in Airflow 3.)

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
        events_context_key = "triggering_asset_events" if supports_assets() else "triggering_dataset_events"
        runtime_options = "{{sl_options_from_events(" + events_context_key + ", dag_run, '" + transform_name + "')}}"
        transform_options = ",".join(filter(None, [transform_options, runtime_options]))
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
        # Precedence contract (issue #87):
        #   DEFAULT_DAG_ARGS (framework constants)
        #     < default_dag_args JSON option
        #     < explicitly provided retries / retry_delay options
        # start_date is always framework-derived (computed from the DAG file).
        # copy: the shared module constant must never be mutated — the Airflow
        # scheduler parses many DAG modules in one interpreter.
        dag_args = dict(DEFAULT_DAG_ARGS)
        try:
            dag_args.update(json.loads(__class__.get_context_var(var_name="default_dag_args", options=self.options)))
        except (MissingEnvironmentVariable, JSONDecodeError):
            pass
        dag_args.update({'start_date': self.start_date})
        # only an explicitly provided option may override the JSON option —
        # the core fallbacks (retries=1, retry_delay=300) must not clobber it
        try:
            dag_args.update({'retries': int(__class__.get_context_var(var_name='retries', options=self.options))})
        except (MissingEnvironmentVariable, ValueError):
            pass
        try:
            dag_args.update({'retry_delay': timedelta(seconds=int(__class__.get_context_var(var_name='retry_delay', options=self.options)))})
        except (MissingEnvironmentVariable, ValueError):
            pass
        return dag_args

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
        inlets: list = kwargs.get("inlets", [])
        if inlets:
            # Airflow 2's lineage hook JSON-serializes inlets to XCom in post_execute;
            # raw StarlakeDataset objects are not serializable (Dataset, an attrs class, is)
            kwargs["inlets"] = [
                AirflowDataset.to_event(inlet) if isinstance(inlet, StarlakeDataset) else inlet
                for inlet in inlets
            ]
        outlets: list = kwargs.get("outlets", [])
        # popped: BaseOperator would reject the unknown kwarg. extra is a template
        # field (see below) so Jinja/XCom values inside it (e.g. runtime sl_options)
        # are rendered before pre_execute copies it onto the outlet events.
        extra = kwargs.pop("extra", dict())
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
                    self.scheduled_dataset = "{{sl_scheduled_dataset(params.uri, params.cron, ts_as_datetime(dag_run.data_interval_end | ts), params.sl_schedule_parameter_name, params.sl_schedule_format)}}"
                else:
                    self.scheduled_dataset = None
                self.scheduled_date = "{{sl_scheduled_date(params.cron, ts_as_datetime(dag_run.data_interval_end | ts))}}"
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
                self.scheduled_date = "{{sl_scheduled_date(params.cron, ts_as_datetime(dag_run.data_interval_end | ts))}}"
            outlets.append(Dataset(uri=uri, extra=extra))
            kwargs["outlets"] = outlets
            self.template_fields = getattr(self, "template_fields", tuple()) + ("scheduled_dataset", "scheduled_date", "extra",)
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

        __sl_options_from_events = dag.user_defined_macros.get('sl_options_from_events', None) if dag.user_defined_macros else None
        if not __sl_options_from_events:
            print(f"add 'sl_options_from_events' to context")
            context['sl_options_from_events'] = sl_options_from_events

        return super().render_template_fields(context, jinja_env)

    
    def pre_execute(self, context: Context):
        if not context:
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

class StarlakeCloudPreloadSensor(StarlakeDatasetMixin, BaseSensorOperator):
    """Sensor-flavor cloud pre-load waiting (story 6.5, issue #93).

    The reschedule-mode fallback used when the engine's run operator does not
    support ``deferrable`` (old provider) or the user opted out via
    ``pre_load_deferrable=false`` (e.g. no triggerer running). Each ``poke``
    submits ONE preload run through the engine-supplied ``submit_and_wait``
    closure and interprets the terminal state via
    ``StarlakeAirflowJob._sl_pre_load_poke_verdict``:

    - success → ``PokeReturnValue(True, True)`` — done; the truthy ``return_value``
      XCom lets the downstream ``skip_or_start`` ShortCircuit proceed;
    - no files yet / submission error → ``None`` — poke again after
      ``poke_interval``.

    ``BaseSensorOperator`` owns the wall-clock ``timeout`` + ``soft_fail`` +
    reschedule window: on timeout, ``soft_fail=True`` → SKIPPED (no XCom →
    downstream skipped), else FAILED. The per-poke job-submission overhead is
    accepted against the 300 s default interval. ``submit_and_wait`` is a
    closure so this sensor stays provider-free and unit-testable; the gcp/aws
    modules supply the provider-specific submission.
    """
    def __init__(
        self,
        *,
        task_id: str,
        dataset: Optional[Union[str, StarlakeDataset]],
        source: Optional[str],
        submit_and_wait,
        **kwargs
    ) -> None:
        kwargs.setdefault('mode', 'reschedule')
        super().__init__(
            task_id=task_id,
            dataset=dataset,
            source=source,
            **kwargs
        )
        self._submit_and_wait = submit_and_wait

    def poke(self, context) -> Optional[PokeReturnValue]:
        try:
            succeeded = bool(self._submit_and_wait(context))
        except Exception as e:
            # A failed submission (no files yet, or a transient error) pokes
            # again — the wall-clock timeout/soft_fail window is the terminal
            # concern, exactly like the shell sensor's retry_exit_code=None.
            logging.getLogger(__name__).info(
                f"Preload poke for {self.task_id}: no files yet or submission error "
                f"({e}); will poke again"
            )
            succeeded = False
        return StarlakeAirflowJob._sl_pre_load_poke_verdict(succeeded)

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
