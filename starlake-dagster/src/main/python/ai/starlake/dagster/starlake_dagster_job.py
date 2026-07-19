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

from typing import Callable, List, NamedTuple, Optional, TypeVar, Union

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOptions, StarlakeOrchestrator, TaskType

from ai.starlake.common import is_valid_cron, sl_cron_start_end_dates, sl_timestamp_format, StarlakeParameters

from ai.starlake.dataset import StarlakeDataset, AbstractEvent

from dagster import AssetKey, Failure, Output, Out, op, AssetMaterialization, OpExecutionContext

from dagster._core.definitions import NodeDefinition

from dagster._core.definitions.partition import PARTITION_NAME_TAG

from datetime import datetime

import pytz

# used as time.monotonic()/time.sleep() (module-attribute calls) so tests can
# patch the poke-loop clock (stories 6.2/6.7)
import time

class DagsterDataset(AbstractEvent[AssetKey]):
    @classmethod
    def to_event(cls, dataset: StarlakeDataset, source: Optional[str] = None) -> AssetKey:
        return AssetKey(dataset.uri)

class PreLoadPoke(NamedTuple):
    """Resolved pre-load sensor configuration for the in-op wall-clock poke
    loop (story 6.2 shell, story 6.7 cloud variants — issue #94)."""
    poke_interval: int
    timeout: int
    soft_fail: bool

T = TypeVar("T")

class StarlakeDagsterJob(IStarlakeJob[NodeDefinition, AssetKey], StarlakeOptions, DagsterDataset):
    def __init__(self, filename: str=None, module_name: str=None, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)

    @classmethod
    def sl_orchestrator(cls) -> Union[StarlakeOrchestrator, str]:
         return StarlakeOrchestrator.DAGSTER

    def sl_pre_load(self, domain: str, tables: set=set(), pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, **kwargs) -> Optional[NodeDefinition]:
        """Overrides IStarlakeJob.sl_pre_load()
        Generate the Dagster node that will check if the conditions are met to load the specified domain according to the pre-load strategy choosen.

        Args:
            domain (str): The required domain to pre-load.
            tables (set): The optional tables to pre-load.
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The optional pre-load strategy to use.
        
        Returns:
            Optional[NodeDefinition]: The Dagster node or None.
        """

        if isinstance(pre_load_strategy, str):
            pre_load_strategy = self.__class__.sl_resolve_pre_load_strategy(
                pre_load_strategy,
                default=self.pre_load_strategy,
                action="sl_pre_load(pre_load_strategy=...)",
            )

        pre_load_strategy = self.pre_load_strategy if not pre_load_strategy else pre_load_strategy

        if pre_load_strategy != StarlakePreLoadStrategy.NONE:
            kwargs.update({'skip_or_start': True,})
            kwargs.update({'retries': 0,})

        return super().sl_pre_load(domain=domain, tables=tables, pre_load_strategy=pre_load_strategy, **kwargs)

    @classmethod
    def _sl_resolve_pre_load_poke(cls, kwargs: dict) -> Optional[PreLoadPoke]:
        """Pop the four ``pre_load_*`` sensor kwargs and resolve the poke-loop
        configuration (story 6.7, issue #94 — supersedes the story 6.2 cloud
        rejection).

        Returns ``None`` when sensor mode is off — the kwargs are popped so the
        one-shot op construction is byte-identical to today (zero-change
        guarantee; a popped-but-false flag can never leak into an op).  The
        kwargs are popped FIRST (even an invalid value must not leak), then
        parsed strictly with the NFR11 core helpers: core ``sl_pre_load``
        already validates the pipeline path; the strict re-parse here covers
        direct ``sl_job`` calls (``bool('false')`` would otherwise silently
        turn sensor mode ON, and a zero/negative interval would hot-loop).
        """
        raw_sensor = kwargs.pop('pre_load_sensor', False)
        raw_poke_interval = kwargs.pop('pre_load_poke_interval', 300)
        raw_timeout = kwargs.pop('pre_load_timeout', 3600)
        raw_soft_fail = kwargs.pop('pre_load_sensor_soft_fail', False)
        pre_load_sensor = cls._sl_parse_strict_bool('pre_load_sensor', raw_sensor)
        if not pre_load_sensor:
            return None
        poke_interval = cls._sl_parse_strict_positive_int('pre_load_poke_interval', raw_poke_interval)
        timeout = cls._sl_parse_strict_positive_int('pre_load_timeout', raw_timeout)
        soft_fail = cls._sl_parse_strict_bool('pre_load_sensor_soft_fail', raw_soft_fail)
        if timeout < poke_interval:
            raise cls._sl_pre_load_option_error(
                'pre_load_timeout', raw_timeout,
                f"an integer number of seconds >= pre_load_poke_interval ({poke_interval})",
            )
        return PreLoadPoke(poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail)

    # -- story 6.12 (issue #122): pre-load not-ready sentinel seams ----------

    @classmethod
    def _sl_resolve_sentinel(cls, kwargs: dict, allowed_schemes: tuple, engine: str) -> Optional[str]:
        """Pop the ``sentinel_path`` kwarg (unconditionally — a leaked kwarg
        would corrupt the op construction) and validate its scheme against
        the engine (definition time). Returns ``None`` when the sentinel
        feature is off (byte-identical op construction)."""
        sentinel_path = kwargs.pop('sentinel_path', None)
        if not sentinel_path:
            return None
        from ai.starlake.sentinel import require_scheme
        require_scheme(sentinel_path, allowed_schemes, engine)
        return sentinel_path

    @classmethod
    def _sl_sentinel_substitute_args(cls, arguments: List[str], context) -> List[str]:
        """Run-time scope substitution over a per-attempt COPY of the command
        vector: the SENTINEL_SCOPE_TOKEN embedded in the --notReadySentinel
        value becomes the sanitized ``<job_name>__<run_id>`` scope. Never
        mutates the closure list (6.9/6.10 rule)."""
        from ai.starlake.sentinel import substitute_scope
        return [
            substitute_scope(argument, context.job_name, context.run_id)
            if isinstance(argument, str) else argument
            for argument in arguments
        ]

    @classmethod
    def _sl_sentinel_ready(cls, context, sentinel_path: str) -> bool:
        """Consume-then-signal verdict after a SUCCESSFUL preload run, using
        the core default handlers (``ai.starlake.gcp``/``ai.starlake.aws``
        for gs:// and s3://, stdlib for local paths). ``True`` = READY."""
        from ai.starlake.sentinel import (
            consume_sentinel,
            default_sentinel_handlers,
            substitute_scope,
        )
        path = substitute_scope(sentinel_path, context.job_name, context.run_id)
        exists_fn, delete_fn = default_sentinel_handlers(path)
        return consume_sentinel(path, exists_fn, delete_fn)

    @classmethod
    def _sl_pre_load_poke_loop(
        cls,
        context,
        run_once: Callable[[], T],
        is_success: Callable[[T], bool],
        poke: PreLoadPoke,
        command_label: str,
    ) -> Optional[T]:
        """In-op wall-clock poke loop shared by the shell and cloud variants
        (story 6.2 shape, extracted for story 6.7 / issue #94).

        Dagster has no reschedule primitive, so the op HOLDS ITS EXECUTOR SLOT
        while poking (up to ``poke.timeout`` seconds).  Each iteration calls
        ``run_once()`` — a full (re-)submission on the cloud variants — and
        interprets its result with ``is_success``.  ``time.monotonic()`` /
        ``time.sleep()`` are called through the ``time`` module so tests can
        patch the clock.

        Returns the successful ``run_once`` result, or ``None`` when the
        deadline is reached with ``poke.soft_fail`` — the caller must then
        bare-``return`` so the existing optional-output gating skips the
        downstream tasks.  The loop therefore presumes the PRELOAD op
        composition (``sl_pre_load`` forces ``skip_or_start=True`` → optional
        output): a direct ``sl_job`` call combining ``pre_load_sensor=True``
        with a required output or a ``failure=`` output is unsupported (the
        soft-fail bare-return would violate the required output, and the hard
        timeout raises instead of routing to the failure output).

        The deadline is only evaluated BETWEEN pokes: a single hung
        ``run_once`` submission can hold the executor slot past
        ``poke.timeout`` (engines should bound their own submission wait).

        Raises:
            Failure: on deadline without ``soft_fail`` — raised HERE so the
                forced ``skip_or_start`` bare-return branch of the callers
                cannot swallow the hard timeout.
        """
        deadline = time.monotonic() + poke.timeout
        while True:
            result = run_once()
            if is_success(result):
                return result
            # sleep only when another poke still fits in the window
            if time.monotonic() + poke.poke_interval >= deadline:
                timeout_message = (
                    f"Starlake command {command_label} timed out waiting "
                    f"for files after {poke.timeout}s"
                )
                if poke.soft_fail:
                    context.log.info(
                        f"{timeout_message} — skipping downstream tasks "
                        f"(pre_load_sensor_soft_fail=true)."
                    )
                    return None
                raise Failure(description=timeout_message)
            time.sleep(poke.poke_interval)

    def sl_import(self, task_id: str, domain: str, tables: set=set(), **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_import()
        Generate the Dagster node that will run the starlake `import` command.

        Args:
            task_id (str): The optional task id ({domain}_import by default).
            domain (str): The required domain to import.
            tables (set): The optional tables to import.

        Returns:
            NodeDefinition: The Dagster node.
        """
        kwargs.update({'description': f"Starlake domain '{domain}' imported"})
        return super().sl_import(task_id=task_id, domain=domain, tables=tables, **kwargs)

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_load()
        Generate the Dagster node that will run the starlake `load` command.

        Args:
            task_id (str): The optional task id ({domain}_{table}_load by default).
            domain (str): The required domain to load.
            table (str): The required table to load.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.

        Returns:
            NodeDefinition: The Dagster node.        
        """
        kwargs.update({'description': f"Starlake table '{domain}.{table}' loaded"})
        return super().sl_load(task_id=task_id, domain=domain, table=table, spark_config=spark_config, dataset=dataset, **kwargs)

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str = None, spark_config: StarlakeSparkConfig = None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_transform()
        Generate the Dagster node that will run the starlake `transform` command.

        Args:
            task_id (str): The optional task id ({transform_name} by default).
            transform_name (str): The required transform name.
            transform_options (str, optional): The optional transform options. Defaults to None.
            spark_config (StarlakeSparkConfig, optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.

        Returns:
            NodeDefinition: The Dagster node.
        """
        kwargs.update({'description': f"Starlake transform '{transform_name}' executed"})
        return super().sl_transform(task_id=task_id, transform_name=transform_name, transform_options=transform_options, spark_config=spark_config, dataset=dataset, **kwargs)

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]= None, task_type: Optional[TaskType]=None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_job()
        Generate the Dagster node that will run the starlake command.
        
        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.
            task_type (Optional[TaskType]): The optional task type to use.
        
        Returns:
            NodeDefinition: The Dagster node.
        """

    def dummy_op(self, task_id: str, events: Optional[List[AssetKey]] = None, task_type: Optional[TaskType] = TaskType.EMPTY, **kwargs) -> NodeDefinition:
        """Dummy op.
        Generate a Dagster dummy op.

        Args:
            task_id (str): The required task id.
            events (Optional[List[AssetKey]]): The optional events to materialize.
            task_type (Optional[TaskType]): The optional task type.

        Returns:
            NodeDefinition: The Dagster node.
        """

        out:str = kwargs.get("out", "result")

        assets: List[AssetKey] = kwargs.get("assets", [])
        if events:
            assets += events

        @op(
            name=task_id,
            required_resource_keys=set(),
            ins=kwargs.get("ins", {}),
            out={out: Out(dagster_type=str, is_required=True)}
        )
        def dummy(context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, **kwargs):
            yield Output(value=task_id, output_name=out)

            for asset in assets:
                yield AssetMaterialization(asset_key=asset.path, description=kwargs.get("description", f"Dummy op {task_id} execution succeeded"))

        return dummy

from dagster import Config

class DagsterLogicalDatetimeConfig(Config):
    logical_datetime: Optional[str]
    previous_logical_datetime: Optional[str] = None
    dry_run: bool = False
    # Runtime starlake --options carried by the triggering asset materializations
    # (StarlakeParameters.OPTIONS_PARAMETER), JSON-encoded as a dict of sections:
    # {"all": {key: value}, "<domain.task>": {key: value}} — the "all" section
    # applies to every node of the run, a task-keyed section only to that node
    # (precedence: static options < "all" < task-specific). Populated by the
    # pipeline sensor's RunRequest, or manually at launch (recovery escape hatch).
    sl_options: Optional[str] = None

class StarlakeDagsterUtils:

    @classmethod
    def quote_datetime(cls, date_str: Optional[str]) -> Optional[str]:
        """Quote the datetime string.
        Args:
            date_str (str): The datetime string to quote.
        Returns:
            str: The quoted datetime string.
        """
        if not date_str:
            return None
        return date_str.replace(' ', 'T').replace(':', '.').replace('+', '_')

    @classmethod
    def unquote_datetime(cls, date_str: Optional[str]) -> Optional[str]:
        """Unquote the datetime string.
        Args:
            date_str (str): The datetime string to unquote.
        Returns:
            str: The unquoted datetime string.
        """
        if not date_str:
            return None
        return date_str.replace('T', ' ').replace('.', ':').replace('_', '+')

    @classmethod
    def get_logical_datetime(cls, context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, **kwargs) -> datetime:
        """Get the logical datetime.
        Args:
            context (OpExecutionContext): The context.
            config (DagsterLogicalDatetimeConfig): The logical datetime config.

        If the partition key is set, it will be used to determine the logical datetime.
        If the partition key is not set, it will use the logical_datetime from the config.
        If neither is set, it will use the launch time of the run or the current time if the launch time is not available.

        Returns:
            datetime: The logical datetime.
        """
        context.log.info(f"config -> {config}")
        try:
            partition_key = context.partition_key
        except Exception as e:
            context.log.warning(e)
            partition_key = context.get_tag(PARTITION_NAME_TAG) or context.get_tag('logical_datetime')
        if partition_key:
            context.log.info(f"Partition key: {partition_key}")
            from dateutil import parser
            logical_datetime = parser.isoparse(partition_key).astimezone(pytz.timezone('UTC'))
        elif config.logical_datetime:
            from dateutil import parser
            logical_datetime = parser.isoparse(config.logical_datetime).astimezone(pytz.timezone('UTC'))
        else:
            run_stats = context.instance.get_run_stats(context.dagster_run.run_id)._asdict()
            launch_time = run_stats.get('launch_time')
            if not launch_time:
                logical_datetime = datetime.now().astimezone(pytz.timezone('UTC'))
            else:
                logical_datetime = datetime.fromtimestamp(run_stats.get('launch_time')).astimezone(pytz.timezone('UTC'))
        context.log.info(f"logical datetime : {logical_datetime}")
        return logical_datetime

    @classmethod
    def get_asset(cls, context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, dataset: Union[StarlakeDataset, str], **kwargs) -> AssetKey:
        """Get the asset.

        Args:
            context (OpExecutionContext): The context.
            config (DagsterLogicalDatetimeConfig): The logical datetime config.
            dataset (Union[StarlakeDataset, str): The dataset.

        Returns:
            AssetKey: The asset.
        """
        if isinstance(dataset, str):
            return AssetKey(dataset)
        return AssetKey(dataset.refresh(cls.get_logical_datetime(context, config, **kwargs)).url)

    @classmethod
    def get_materialization(cls, context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, dataset: Union[StarlakeDataset, str], **kwargs) -> AssetMaterialization:
        """Get the asset materialization.

        Args:
            context (OpExecutionContext): The context.
            config (DagsterLogicalDatetimeConfig): The logical datetime config.
            dataset (Union[StarlakeDataset, str]): The dataset.

        Returns:
            AssetMaterialization: The asset materialization.
        """
        from dagster._core.definitions.metadata import MetadataValue
        logical_datetime = cls.get_logical_datetime(context, config, **kwargs)
        partition_key = logical_datetime.strftime(sl_timestamp_format)
        if isinstance(dataset, str):
            metadata = {
                StarlakeParameters.URI_PARAMETER.value: dataset,
                StarlakeParameters.CRON_PARAMETER.value: None,
                StarlakeParameters.FRESHNESS_PARAMETER.value: MetadataValue.int(0),
            }
            asset_key = AssetKey(dataset)
        else:
            metadata = {
                StarlakeParameters.URI_PARAMETER.value: dataset.uri,
                StarlakeParameters.CRON_PARAMETER.value: dataset.cron,
                StarlakeParameters.FRESHNESS_PARAMETER.value: MetadataValue.int(dataset.freshness),
            }
            asset_key = AssetKey(dataset.uri)
        metadata.update({
            StarlakeParameters.SCHEDULED_DATE_PARAMETER.value: MetadataValue.timestamp(logical_datetime),
            StarlakeParameters.DRY_RUN_PARAMETER.value: MetadataValue.bool(config.dry_run),
        })
        # sl_options carried downstream: static sections passed by the caller via
        # the `extra` kwarg, overridden by the run-level sections (config.sl_options)
        # so a run relays the options it was itself triggered with.
        extra = kwargs.get("extra", None) or {}
        event_options: dict = {}
        if isinstance(extra, dict):
            static_options = extra.get(StarlakeParameters.OPTIONS_PARAMETER.value, None)
            if isinstance(static_options, dict):
                for section, opts in static_options.items():
                    if isinstance(opts, dict):
                        event_options.setdefault(section, {}).update(opts)
        if config.sl_options:
            import json
            try:
                run_options = json.loads(config.sl_options)
            except (TypeError, ValueError):
                run_options = None
            if isinstance(run_options, dict):
                for section, opts in run_options.items():
                    if isinstance(opts, dict):
                        event_options.setdefault(section, {}).update(opts)
        if event_options:
            metadata.update({
                StarlakeParameters.OPTIONS_PARAMETER.value: MetadataValue.json(event_options),
            })
        tags = kwargs.get("tags", {})
        partition = cls.quote_datetime(partition_key)
        tags.update({
            StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value: partition,
            PARTITION_NAME_TAG: partition,
        })
        if config.previous_logical_datetime:
            tags[StarlakeParameters.DATA_INTERVAL_START_PARAMETER.value] = cls.quote_datetime(config.previous_logical_datetime)
        return AssetMaterialization(
            asset_key=asset_key, 
            description=kwargs.get("description", f"Asset {asset_key.to_user_string()} materialized"),
            metadata=metadata,
            partition=partition_key,
            tags=tags
        )

    @classmethod
    def get_assets(cls, context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, datasets: List[StarlakeDataset], **kwargs) -> List[AssetKey]:
        """Get the assets.

        Args:
            context (OpExecutionContext): The context.
            config (DagsterLogicalDatetimeConfig): The logical datetime config.
            datasets (List[StarlakeDataset]): The datasets.

        Returns:
            List[AssetKey]: The assets.
        """
        return [cls.get_asset(context, config, dataset, **kwargs) for dataset in datasets]

    @classmethod
    def get_materializations(cls, context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, datasets: List[StarlakeDataset], **kwargs) -> List[AssetMaterialization]:
        """Get the asset materializations.

        Args:
            context (OpExecutionContext): The context.
            config (DagsterLogicalDatetimeConfig): The logical datetime config.
            datasets (List[StarlakeDataset]): The datasets.

        Returns:
            List[AssetMaterialization]: The asset materializations.
        """
        return [cls.get_materialization(context, config, dataset, **kwargs) for dataset in datasets]

    @classmethod
    def collect_sl_options(cls, materializations) -> dict:
        """Merge the sl_options sections carried by asset materializations
        (StarlakeParameters.OPTIONS_PARAMETER in their metadata).

        Merging is fail-loud: the same key carried with different values by two
        materializations raises (conflicting run variables must stop the run,
        not silently pick one).

        Args:
            materializations: An iterable of AssetMaterialization (None entries
                are skipped).

        Returns:
            dict: The merged sections {"all": {...}, "<domain.task>": {...}}.
        """
        import json
        sections: dict = {}
        for mat in materializations or []:
            if not mat:
                continue
            metadata = getattr(mat, "metadata", None) or {}
            value = metadata.get(StarlakeParameters.OPTIONS_PARAMETER.value, None)
            raw = getattr(value, "value", value)  # MetadataValue.json -> dict
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except (TypeError, ValueError):
                    continue
            if not isinstance(raw, dict):
                continue
            for section, opts in raw.items():
                if not isinstance(opts, dict):
                    continue
                merged = sections.setdefault(section, {})
                for key, v in opts.items():
                    if key in merged and str(merged[key]) != str(v):
                        raise ValueError(
                            f"Conflicting values for {StarlakeParameters.OPTIONS_PARAMETER.value}['{section}']['{key}'] across triggering materializations "
                            f"({getattr(mat, 'asset_key', None)}): '{merged[key]}' != '{v}'. Conflicting run variables must be run one by one, "
                            f"passing {StarlakeParameters.OPTIONS_PARAMETER.value} in the run config."
                        )
                    merged[key] = v
        return sections

    @classmethod
    def get_sl_options(cls, context: Optional[OpExecutionContext], config: DagsterLogicalDatetimeConfig, name: Optional[str] = None) -> dict:
        """Resolve the runtime starlake --options applying to a node.

        Reads the sections carried by the run (config.sl_options, set by the
        pipeline sensor or manually at launch — falling back to the run tag)
        and merges the 'all' section with the section keyed by the node name
        (precedence: 'all' < task-specific).

        Returns:
            dict: The options to append to the command's --options (appended
            last, they override the static ones — starlake keeps the last
            occurrence of a duplicate key).
        """
        import json
        raw = config.sl_options
        if not raw and context:
            try:
                raw = context.get_tag(StarlakeParameters.OPTIONS_PARAMETER.value)
            except Exception:
                raw = None
        if not raw:
            return {}
        sections = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(sections, dict):
            return {}
        options = dict(sections.get("all", {}))
        if name and isinstance(sections.get(name), dict):
            options.update(sections.get(name))
        return options

    @classmethod
    def get_transform_options(cls, context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, params: dict = dict(), **kwargs) -> str:
        """Get the transform options.

        Returns:
            str: The transform options.
        """
        previous_logical_datetime = config.previous_logical_datetime or context.get_tag('previous_logical_datetime')
        logical_datetime: datetime = cls.get_logical_datetime(context, config, **kwargs)
        if previous_logical_datetime and logical_datetime:
            # issue #118 — emit BOTH bounds in sl_timestamp_format (space-free
            # T-form, like the cron branch and the Airflow builder): on
            # cloud_run the argument vector is joined into gcloud's
            # space-separated --args fragment, which would split a
            # space-form value. Raw ISO first (the module's ingestion
            # convention — also keeps fractional seconds parseable, which the
            # tag decode would corrupt); the tag-encoded form
            # (quote_datetime: ' '->T, ':'->'.', '+'->'_') goes through
            # unquote_datetime + a STRICT parse, and anything else fails
            # loudly instead of shipping a corrupted SQL substitution value
            from dateutil import parser
            try:
                previous_datetime = parser.isoparse(previous_logical_datetime)
            except (ValueError, OverflowError):
                try:
                    previous_datetime = datetime.strptime(cls.unquote_datetime(previous_logical_datetime), '%Y-%m-%d %H:%M:%S%z')
                except ValueError as e:
                    raise ValueError(
                        f"get_transform_options: invalid previous_logical_datetime {previous_logical_datetime!r} — "
                        "expected an ISO datetime (e.g. 2026-07-17T00:00:00+0000) or its tag-encoded form"
                    ) from e
            # naive values follow the logical_datetime convention (local time
            # normalized to UTC) — and %z is thereby never empty
            previous_datetime = previous_datetime.astimezone(pytz.timezone('UTC'))
            return f"{StarlakeParameters.DATA_INTERVAL_START_PARAMETER.value}='{previous_datetime.strftime(sl_timestamp_format)}',{StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value}='{logical_datetime.strftime(sl_timestamp_format)}'"
        cron = params.get(StarlakeParameters.CRON_PARAMETER.value, params.get('cron', params.get('cron_expr', None)))
        if cron and (cron.lower().strip() == 'none' or not is_valid_cron(cron)):
            cron = None
        if cron:
            return sl_cron_start_end_dates(cron, logical_datetime)
        else:
            return ''
