from typing import List, Optional, Tuple, Union

from functools import partial, update_wrapper

from ai.starlake.common import MissingEnvironmentVariable, sanitize_id

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOptions, StarlakeOrchestrator, StarlakeExecutionEnvironment

from ai.starlake.dataset import StarlakeDataset, AbstractEvent

from snowflake.core.task import StoredProcedureCall
from snowflake.core.task.dagv1 import DAGTask

from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.row import Row

class SnowflakeEvent(AbstractEvent[StarlakeDataset]):
    @classmethod
    def to_event(cls, dataset: StarlakeDataset, source: Optional[str] = None) -> StarlakeDataset:
        return dataset

class StarlakeSnowflakeJob(IStarlakeJob[DAGTask, StarlakeDataset], StarlakeOptions, SnowflakeEvent):
    def __init__(self, filename: str, module_name: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self._stage_location = kwargs.get('stage_location', __class__.get_context_var(var_name='stage_location', options=self.options)) #stage_location is required
        try:
            self._warehouse = kwargs.get('warehouse', __class__.get_context_var(var_name='warehouse', options=self.options))
        except MissingEnvironmentVariable:
            self._warehouse = None
        self._packages=["croniter", "sqlalchemy", "python-dateutil"]

    @property
    def stage_location(self) -> Optional[str]:
        return self._stage_location

    @property
    def warehouse(self) -> Optional[str]:
        return self._warehouse

    @property
    def packages(self) -> List[str]:
        return self._packages

    @classmethod
    def sl_orchestrator(cls) -> Union[StarlakeOrchestrator, str]:
         return StarlakeOrchestrator.SNOWFLAKE

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.SQL

    def update_events(self, event: StarlakeDataset, **kwargs) -> Tuple[(str, List[StarlakeDataset])]:
        """Add the event to the list of Snowflake events that will be triggered.

        Args:
            event (StarlakeDataset): The event to add.

        Returns:
            Tuple[(str, List[StarlakeDataset]): The tuple containing the list of snowflake events to trigger.
        """
        events: List[StarlakeDataset] = kwargs.get('events', [])
        events.append(event)
        return 'events', events

    def start_op(self, task_id, scheduled: bool, not_scheduled_datasets: Optional[List[StarlakeDataset]], least_frequent_datasets: Optional[List[StarlakeDataset]], most_frequent_datasets: Optional[List[StarlakeDataset]], **kwargs) -> Optional[DAGTask]:
        """Overrides IStarlakeJob.start_op()
        It represents the first task of a pipeline, it will define the optional condition that may trigger the DAG.
        Args:
            task_id (str): The required task id.
            scheduled (bool): whether the dag is scheduled or not.
            not_scheduled_datasets (Optional[List[StarlakeDataset]]): The optional not scheduled datasets.
            least_frequent_datasets (Optional[List[StarlakeDataset]]): The optional least frequent datasets.
            most_frequent_datasets (Optional[List[StarlakeDataset]]): The optional most frequent datasets.
        Returns:
            Optional[DAGTask]: The optional Snowflake task.
        """
        comment = kwargs.get('comment', None)
        if not comment:
            comment = f"dummy task for {task_id}"
        kwargs.pop('comment', None)

        condition = None

        definition=f"select '{task_id}'"

        if not scheduled: # if the DAG is not scheduled we will rely on streams to trigger the underlying dag and check if the scheduled datasets without streams have data using CHANGES
            changes = dict() # tracks the datasets whose changes have to be checked

            if least_frequent_datasets:
                print(f"least frequent datasets: {','.join(list(map(lambda x: x.name, least_frequent_datasets)))}")
                for dataset in least_frequent_datasets:
                    changes.update({dataset.name: dataset.cron})

            not_scheduled_streams = set() # set of streams which underlying datasets are not scheduled
            not_scheduled_datasets_without_streams = []
            if not_scheduled_datasets:
                print(f"not scheduled datasets: {','.join(list(map(lambda x: x.name, not_scheduled_datasets)))}")
                for dataset in not_scheduled_datasets:
                    if dataset.stream:
                        not_scheduled_streams.add(f"SYSTEM$STREAM_HAS_DATA('{dataset.stream}')")
                    else:
                        not_scheduled_datasets_without_streams.append(dataset)
            if not_scheduled_datasets_without_streams:
                print(f"Warning: No streams found for {','.join(list(map(lambda x: x.name, not_scheduled_datasets_without_streams)))}")
                ... # nothing to do here

            if most_frequent_datasets:
                print(f"most frequent datasets: {','.join(list(map(lambda x: x.name, most_frequent_datasets)))}")
            streams = set()
            most_frequent_datasets_without_streams = []
            if most_frequent_datasets:
                for dataset in most_frequent_datasets:
                    if dataset.stream:
                        streams.add(f"SYSTEM$STREAM_HAS_DATA('{dataset.stream}')")
                    else:
                        most_frequent_datasets_without_streams.append(dataset)
                        changes.update({dataset.name: dataset.cron})
            if most_frequent_datasets_without_streams:
                print(f"Warning: No streams found for {','.join(list(map(lambda x: x.name, most_frequent_datasets_without_streams)))}")
                ...

            if streams:
                condition = ' OR '.join(streams)

            if not_scheduled_streams:
                if condition:
                    condition = f"({condition}) AND ({' AND '.join(not_scheduled_streams)})"
                else:
                    condition = ' AND '.join(not_scheduled_streams)

            if changes:
                format = '%Y-%m-%d %H:%M:%S%z'

                def fun(session: Session, changes: dict, format: str) -> None:
                    from croniter import croniter
                    from croniter.croniter import CroniterBadCronError
                    from datetime import datetime
                    from snowflake.core.task.context import TaskContext
                    context = TaskContext(session)
                    # get the original scheduled timestamp of the initial graph run in the current group
                    # For graphs that are retried, the returned value is the original scheduled timestamp of the initial graph run in the current group.
                    original_schedule = context.get_current_task_graph_original_schedule()
                    if original_schedule:
                        from dateutil import parser
                        start_time = parser.parse(original_schedule)
                    else:
                        start_time = datetime.fromtimestamp(datetime.now().timestamp())
                    for dataset, cron_expr in changes.items():
                        # enabling change tracking for the dataset
                        print(f"Enabling change tracking for dataset {dataset}")
                        session.sql(query=f"ALTER TABLE {dataset} SET CHANGE_TRACKING = TRUE").collect() # should be done once and when we create our datasets
                        try:
                            croniter(cron_expr)
                            iter = croniter(cron_expr, start_time)
                            # get the start and end date of the current cron iteration
                            curr = iter.get_current(datetime)
                            previous = iter.get_prev(datetime)
                            next = croniter(cron_expr, previous).get_next(datetime)
                            if curr == next :
                                sl_end_date = curr
                            else:
                                sl_end_date = previous
                            sl_start_date = croniter(cron_expr, sl_end_date).get_prev(datetime)
                            change = f"SELECT * FROM {dataset} WHERE CHANGES(INFORMATION => DEFAULT) AT(TIMESTAMP => '{sl_start_date.strftime(format)}') END (TIMESTAMP => '{sl_end_date.strftime(format)}')"
                            print(f"Checking changes for dataset {dataset} from {sl_start_date.strftime(format)} to {sl_end_date.strftime(format)} -> {change}")
                            df = session.sql(query=change)
                            rows = df.collect()
                            if rows.__len__() == 0:
                                raise ValueError(f"Dataset {dataset} has no changes from {sl_start_date.strftime(format)} to {sl_end_date.strftime(format)}")
                            print(f"Dataset {dataset} has data from {sl_start_date.strftime(format)} to {sl_end_date.strftime(format)}")
                        except CroniterBadCronError:
                            raise ValueError(f"Invalid cron expression: {cron_expr}")

                partial_fun = partial(
                        fun,
                        changes=changes,
                        format=format
                    )

                update_wrapper(partial_fun, fun)

                partial_fun.__name__ = f"fun_{sanitize_id(task_id)}"

                definition = StoredProcedureCall(
                    func = partial_fun, 
                    stage_location=self.stage_location,
                    packages=self.packages
                )

        if condition:
            print(f"condition: {condition}")

        return DAGTask(
            name=task_id, 
            definition=definition, 
            comment=comment, 
            condition=condition,
            **kwargs
        )

    def dummy_op(self, task_id: str, events: Optional[List[StarlakeDataset]] = None, **kwargs) -> DAGTask:
        """Dummy op.
        Generate a Snowflake dummy task.

        Args:
            task_id (str): The required task id.
            events (Optional[List[StarlakeDataset]]): The optional events to materialize.

        Returns:
            DAGTask: The Snowflake task.
        """
        comment = kwargs.get('comment', None)
        if not comment:
            comment = f"dummy task for {task_id}"
        kwargs.pop('comment', None)

        return DAGTask(
            name=task_id, 
            definition=f"select '{task_id}'", 
            comment=comment, 
            **kwargs
        )

    def skip_or_start_op(self, task_id: str, upstream_task: DAGTask, **kwargs) -> Optional[DAGTask]:
        """Overrides IStarlakeJob.skip_or_start_op()
        Generate a Snowflake task that will skip or start the pipeline.

        Args:
            task_id (str): The required task id.
            events (Optional[List[StarlakeDataset]]): The optional events to materialize.

        Returns:
            Optional[DAGTask]: The optional Snowflake task.
        """
        comment = kwargs.get('comment', None)
        if not comment:
            comment = f"skip or start task {task_id}"
        kwargs.pop('comment', None)

        def fun(session: Session, upstream_task_id: str) -> None:
            from snowflake.core.task.context import TaskContext
            context = TaskContext(session)
            return_value: str = context.get_predecessor_return_value(upstream_task_id)
            if return_value is None:
                print(f"upstream task {upstream_task_id} did not return any value")
                failed = True
            else:
                print(f"upstream task {upstream_task_id} returned {return_value}")
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
                        print(f"Parsed return value {parsed_return_value}[{type(parsed_return_value)}] is not a valid bool, integer or is empty.")
                except (ValueError, SyntaxError) as e:
                    failed = True
                    print(f"Error parsing return value: {e}")
            if failed:
                raise ValueError(f"upstream task {upstream_task_id} failed")

        partial_fun = partial(
            fun,
            upstream_task_id=upstream_task.name,
        )

        update_wrapper(partial_fun, fun)

        partial_fun.__name__ = f"fun_{sanitize_id(task_id)}"

        return DAGTask(
            name=task_id, 
            definition=partial_fun, 
            comment=comment, 
            **kwargs
        )

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str = None, spark_config: StarlakeSparkConfig = None, **kwargs) -> DAGTask:
        """Overrides IStarlakeJob.sl_transform()
        Generate the Snowflake task that will run the starlake `transform` command.

        Args:
            task_id (str): The optional task id ({transform_name} by default).
            transform_name (str): The required transform name.
            transform_options (str, optional): The optional transform options. Defaults to None.
            spark_config (StarlakeSparkConfig, optional): The optional spark configuration. Defaults to None.

        Returns:
            DAGTask: The Snowflake task.
        """
        kwargs.update({'transform': True})
        sink = kwargs.get('sink', transform_name)
        kwargs.update({'sink': sink})
        return super().sl_transform(task_id=task_id, transform_name=transform_name, transform_options=transform_options, spark_config=spark_config, **kwargs)

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[str]=None, **kwargs) -> DAGTask:
        """Overrides IStarlakeJob.sl_job()
        Generate the Snowflake task that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (StarlakeSparkConfig, optional): The optional spark configuration. Defaults to None.
            dataset (Optional[str], optional): The optional dataset name. Defaults to None.

        Returns:
            DAGTask: The Snowflake task.
        """
        if kwargs.get('transform', False):
            kwargs.pop('transform', None)
            sink = kwargs.get('sink', None)
            if sink:
                kwargs.pop('sink', None)
                statements = self.caller_globals.get('statements', dict()).get(sink, None)
                if statements:
                    comment = kwargs.get('comment', f'Starlake {sink} task')
                    kwargs.pop('comment', None)
                    options = self.sl_env_vars.copy() # Copy the current sl env variables
                    for index, arg in enumerate(arguments):
                        if arg == "--options" and arguments.__len__() > index + 1:
                            opts = arguments[index+1]
                            if opts.strip().__len__() > 0:
                                options.update({
                                    key: value
                                    for opt in opts.split(",")
                                    if "=" in opt  # Only process valid key=value pairs
                                    for key, value in [opt.split("=")]
                                })
                            break

                    cron_expr = kwargs.get('cron_expr', None)
                    kwargs.pop('cron_expr', None)

                    format = '%Y-%m-%d %H:%M:%S%z'

                    # create the function that will execute the transform
                    def fun(session: Session, sink: str, statements: dict, params: dict, cron_expr: Optional[str], format: str) -> None:
                        from sqlalchemy import text

                        if cron_expr:
                            from croniter import croniter
                            from croniter.croniter import CroniterBadCronError
                            from datetime import datetime
                            from snowflake.core.task.context import TaskContext
                            context = TaskContext(session)
                            # get the original scheduled timestamp of the initial graph run in the current group
                            # For graphs that are retried, the returned value is the original scheduled timestamp of the initial graph run in the current group.
                            original_schedule = context.get_current_task_graph_original_schedule()
                            if original_schedule:
                                from dateutil import parser
                                start_time = parser.parse(original_schedule)
                            else:
                                start_time = datetime.fromtimestamp(datetime.now().timestamp())
                            try:
                                croniter(cron_expr)
                                iter = croniter(cron_expr, start_time)
                                curr = iter.get_current(datetime)
                                previous = iter.get_prev(datetime)
                                next = croniter(cron_expr, previous).get_next(datetime)
                                if curr == next :
                                    sl_end_date = curr
                                else:
                                    sl_end_date = previous
                                sl_start_date = croniter(cron_expr, sl_end_date).get_prev(datetime)
                                params.update({'sl_start_date': sl_start_date.strftime(format), 'sl_end_date': sl_end_date.strftime(format)})
                            except CroniterBadCronError:
                                raise ValueError(f"Invalid cron expression: {cron_expr}")

                        schemaAndTable = sink.split('.')
                        schema = schemaAndTable[0]
                        table = schemaAndTable[1]

                        # create SQL schema
                        session.sql(query=f"CREATE SCHEMA IF NOT EXISTS {schema}").collect() # use templating

                        # execute preActions
                        preActions: List[str] = statements.get('preActions', [])
                        for action in preActions:
                            stmt: str = text(action).bindparams(**params)
                            session.sql(stmt).collect()

                        # execute preSqls
                        preSqls: List[str] = statements.get('preSqls', [])
                        for sql in preSqls:
                            stmt: str = text(sql).bindparams(**params)
                            session.sql(stmt).collect()

                        # check if the sink exists
                        df: DataFrame = session.sql(query=f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'") # use templating
                        rows: List[Row] = df.collect()
                        if rows.__len__() > 0:
                            # execute mainSqlIfExists
                            sqls: List[str] = statements.get('mainSqlIfExists', [])
                            for sql in sqls:
                                stmt: str = text(sql).bindparams(**params)
                                session.sql(stmt).collect()
                        else:
                            # execute mainSqlIfNotExists
                            sqls: List[str] = statements.get('mainSqlIfNotExists', [])
                            for sql in sqls:
                                stmt: str = text(sql).bindparams(**params)
                                session.sql(stmt).collect()

                        # execute postSqls
                        postSqls: List[str] = statements.get('postSqls', [])
                        for sql in postSqls:
                            stmt: str = text(sql).bindparams(**params)
                            session.sql(stmt).collect()

                        session.sql(query=f"ALTER TABLE {sink} SET CHANGE_TRACKING = TRUE").collect()

                    kwargs.pop('params', None)
                    kwargs.pop('events', None)

                    partial_fun = partial(
                        fun, 
                        sink=sink, 
                        statements=statements, 
                        params=options, 
                        cron_expr=cron_expr,
                        format=format
                    )

                    update_wrapper(partial_fun, fun)

                    partial_fun.__name__ = f"fun_{sanitize_id(task_id)}"

                    return DAGTask(
                        name=task_id, 
                        definition=StoredProcedureCall(
                            func = partial_fun, 
                            stage_location=self.stage_location,
                            packages=self.packages
                        ), 
                        comment=comment, 
                        **kwargs
                    )
                else:
                    # sink statements are required
                    raise ValueError(f"Transform '{sink}' statements not found")
            else:
                # sink is required
                raise ValueError("sink is required")
        else:
            # only sl_transform is implemented
            raise NotImplementedError("Not implemented")
