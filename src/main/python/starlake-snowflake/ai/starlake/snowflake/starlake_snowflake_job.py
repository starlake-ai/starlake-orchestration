from typing import List, Optional, Union

from ai.starlake.common import MissingEnvironmentVariable

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOptions, StarlakeOrchestrator, StarlakeExecutionEnvironment

from ai.starlake.dataset import StarlakeDataset, AbstractEvent

from snowflake.core.task import StoredProcedureCall
from snowflake.core.task.dagv1 import DAGTask

from snowflake.snowpark import Session

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
        packages = kwargs.get('packages', __class__.get_context_var(var_name='packages', default_value='croniter,python-dateutil', options=self.options)).split(',')
        packages = set([package.strip() for package in packages])
        packages.update(['croniter', 'python-dateutil'])
        self._packages = list(packages)
        timezone = kwargs.get('timezone', __class__.get_context_var(var_name='timezone', default_value='UTC', options=self.options))
        self._timezone = timezone

    @property
    def stage_location(self) -> Optional[str]:
        return self._stage_location

    @property
    def warehouse(self) -> Optional[str]:
        return self._warehouse

    @property
    def packages(self) -> List[str]:
        return self._packages

    @property
    def timezone(self) -> str:
        return self._timezone

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
        comment = kwargs.get('comment', f"dummy task for {task_id}")
        kwargs.update({'comment': comment})
        return super().start_op(task_id=task_id, scheduled=scheduled, not_scheduled_datasets=not_scheduled_datasets, least_frequent_datasets=least_frequent_datasets, most_frequent_datasets=most_frequent_datasets, **kwargs)

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

        return DAGTask(
            name=task_id, 
            definition=StoredProcedureCall(
                func = fun,
                args=[upstream_task.name],
                stage_location=self.stage_location,
                packages=self.packages
            ), 
            comment=comment, 
            **kwargs
        )

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str = None, spark_config: StarlakeSparkConfig = None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> DAGTask:
        """Overrides IStarlakeJob.sl_transform()
        Generate the Snowflake task that will run the starlake `transform` command.

        Args:
            task_id (str): The optional task id ({transform_name} by default).
            transform_name (str): The required transform name.
            transform_options (str, optional): The optional transform options. Defaults to None.
            spark_config (StarlakeSparkConfig, optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.

        Returns:
            DAGTask: The Snowflake task.
        """
        kwargs.update({'transform': True})
        if dataset:
            sink = dataset.sink
        else:
            params = kwargs.get('params', dict())
            sink = params.get('sink', kwargs.get('sink', transform_name))
        kwargs.update({'sink': sink})
        return super().sl_transform(task_id=task_id, transform_name=transform_name, transform_options=transform_options, spark_config=spark_config, dataset=dataset, **kwargs)

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> DAGTask:
        """Overrides IStarlakeJob.sl_job()
        Generate the Snowflake task that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (StarlakeSparkConfig, optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.

        Returns:
            DAGTask: The Snowflake task.
        """
        if kwargs.get('transform', False):
            kwargs.pop('transform', None)
            sink = kwargs.get('sink', None)
            if sink:
                kwargs.pop('sink', None)
                statements = self.caller_globals.get('statements', dict()).get(sink, None)
                expectations = self.caller_globals.get('expectations', dict())
                expectation_items = self.caller_globals.get('expectation_items', dict()).get(sink, None)
                if statements:
                    comment = kwargs.get('comment', f'Starlake {sink} task')
                    kwargs.pop('comment', None)
                    options = self.sl_env_vars.copy() # Copy the current sl env variables
                    from collections import defaultdict
                    safe_params = defaultdict(lambda: 'NULL', options)
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

                    audit = self.caller_globals.get('audit', dict())

                    # create the function that will execute the transform
                    def fun(session: Session, sink: str, statements: dict, audit: dict, expectations: dict, expectation_items: list, params: dict, cron_expr: Optional[str], format: str) -> None:
                        from datetime import datetime

                        if cron_expr:
                            from croniter import croniter
                            from croniter.croniter import CroniterBadCronError
                            # get the original scheduled timestamp of the initial graph run in the current group
                            # For graphs that are retried, the returned value is the original scheduled timestamp of the initial graph run in the current group.
                            original_schedule = session.sql(f"select to_timestamp(system$task_runtime_info('CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP'))").collect()[0][0]
                            if original_schedule:
                                if isinstance(original_schedule, str):
                                    from dateutil import parser
                                    start_time = parser.parse(original_schedule)
                                else:
                                    start_time = original_schedule
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

                        jobid = str(session.call("system$current_user_task_name"))

                        def bindParams(stmt: str) -> str:
                            return stmt.format_map(params)

                        def str_to_bool(value: str) -> bool:
                            truthy = ['yes', 'y', 'true', '1']
                            falsy = ['no', 'n', 'false', '0']

                            value = value.strip().lower()
                            if value in truthy:
                                return True
                            elif value in falsy:
                                return False
                            raise ValueError(f"Valeur invalide : {value}")

                        def check_if_schema_exists(domain: str, schema: str) -> bool:
                            df = session.sql(query=f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) ILIKE '{domain}.{schema}'")
                            rows = df.collect()
                            return rows.__len__() > 0

                        def check_if_audit_schema_exists() -> bool:
                            if audit:
                                try:
                                    # create SQL domain
                                    domain = audit.get('domain', ['audit'])[0]
                                    session.sql(query=f"CREATE SCHEMA IF NOT EXISTS {domain}").collect()
                                    # execute SQL preActions
                                    preActions: List[str] = audit.get('preActions', [])
                                    for sql in preActions:
                                        stmt: str = bindParams(sql)
                                        session.sql(stmt).collect()
                                    # check if the audit schema exists
                                    if not check_if_schema_exists(domain, 'audit'):
                                        # execute SQL createSchemaSql
                                        sqls: List[str] = audit.get('createSchemaSql', [])
                                        for sql in sqls:
                                            stmt: str = bindParams(sql)
                                            session.sql(stmt).collect()
                                        return True
                                    else:
                                        return True
                                except Exception as e:
                                    print(f"Error creating audit schema: {str(e)}")
                                    return False
                            else:
                                return False

                        def check_if_expectations_schema_exists() -> bool:
                            if expectations:
                                try:
                                    # create SQL domain
                                    domain = expectations.get('domain', ['audit'])[0]
                                    session.sql(query=f"CREATE SCHEMA IF NOT EXISTS {domain}").collect()
                                    # check if the expectations schema exists
                                    if not check_if_schema_exists(domain, 'expectations'):
                                        # execute SQL createSchemaSql
                                        sqls: List[str] = expectations.get('createSchemaSql', [])
                                        for sql in sqls:
                                            stmt: str = bindParams(sql)
                                            session.sql(stmt).collect()
                                        return True
                                    else:
                                        return True
                                except Exception as e:
                                    print(f"Error creating expectations schema: {str(e)}")
                                    return False
                            else:
                                return False

                        def log_audit(domain: str, schema: str, success: bool, duration: int, message: str, ts: datetime) -> bool :
                            if audit:
                                audit_domain = audit.get('domain', ['audit'])[0]
                                audit_sqls = audit.get('mainSqlIfExists', None)
                                if audit_sqls:
                                    try:
                                        audit_sql = audit_sqls[0]
                                        formatted_sql = audit_sql.format(
                                            jobid = jobid,
                                            paths = schema,
                                            domain = domain,
                                            schema = schema,
                                            success = str(success),
                                            count = "-1",
                                            countAccepted = "-1",
                                            countRejected = "-1",
                                            timestamp = ts.strftime("%Y-%m-%d %H:%M:%S"),
                                            duration = str(duration),
                                            message = message,
                                            step = "TRANSFORM",
                                            database = "",
                                            tenant = ""
                                        )
                                        insert_sql = f"INSERT INTO {audit_domain}.audit {formatted_sql}"
                                        session.sql(insert_sql).collect()
                                        return True
                                    except Exception as e:
                                        print(f"Error inserting audit record: {str(e)}")
                                        return False
                                else:
                                    return False
                            else:
                                return False

                        def log_expectation(domain: str, schema: str, success: bool, name: str, params: str, sql: str, count: int, exception: str, ts: datetime) -> bool :
                            if expectations:
                                expectation_domain = expectations.get('domain', ['audit'])[0]
                                expectation_sqls = expectations.get('mainSqlIfExists', None)
                                if expectation_sqls:
                                    try:
                                        expectation_sql = expectation_sqls[0]
                                        formatted_sql = expectation_sql.format(
                                            jobid = jobid,
                                            database = "",
                                            domain = domain,
                                            schema = schema,
                                            count = count,
                                            exception = exception,
                                            timestamp = ts.strftime("%Y-%m-%d %H:%M:%S"),
                                            success = str(success),
                                            name = name,
                                            params = params,
                                            sql = sql
                                        )
                                        insert_sql = f"INSERT INTO {expectation_domain}.expectations {formatted_sql}"
                                        session.sql(insert_sql).collect()
                                        return True
                                    except Exception as e:
                                        print(f"Error inserting expectations record: {str(e)}")
                                        return False
                                else:
                                    return False
                            else:
                                return False

                        def run_expectation(name: str, params: str, query: str, failOnError: bool = False) -> None:
                            count = 0
                            try:
                                if query:
                                    stmt: str = bindParams(query)
                                    df = session.sql(stmt)
                                    rows = df.collect()
                                    if rows.__len__() != 1:
                                        raise Exception(f'Expectation failed for {sink}: {query}. Expected 1 row but got {rows.__len__()}')
                                    count = rows[0][0]
                                    #  log expectations as audit in expectation table here
                                    if count != 0:
                                        raise Exception(f'Expectation failed for {sink}: {query}. Expected count to be equal to 0 but got {count}')
                                    log_expectation(domain, schema, True, name, params, query, count, "", datetime.now())
                                else:
                                    raise Exception(f'Expectation failed for {sink}: {name}. Query not found')
                            except Exception as e:
                                print(f"Error running expectation {name}: {str(e)}")
                                log_expectation(domain, schema, False, name, params, query, count, str(e), datetime.now())
                                if failOnError:
                                    raise e

                        start = datetime.now()

                        try:
                            # BEGIN transaction
                            session.sql("BEGIN").collect()

                            domainAndSchema = sink.split('.')
                            domain = domainAndSchema[0]
                            schema = domainAndSchema[-1]

                            # create SQL domain
                            session.sql(query=f"CREATE SCHEMA IF NOT EXISTS {domain}").collect() # use templating

                            # execute preActions
                            preActions: List[str] = statements.get('preActions', [])
                            for sql in preActions:
                                stmt: str = bindParams(sql)
                                session.sql(stmt).collect()

                            # execute preSqls
                            preSqls: List[str] = statements.get('preSqls', [])
                            for sql in preSqls:
                                stmt: str = bindParams(sql)
                                session.sql(stmt).collect()

                            if check_if_schema_exists(domain, schema):
                                # execute addSCD2ColumnsSqls
                                scd2_sqls: List[str] = statements.get('addSCD2ColumnsSqls', [])
                                for sql in scd2_sqls:
                                    stmt: str = bindParams(sql)
                                    session.sql(stmt).collect()
                                # execute mainSqlIfExists
                                sqls: List[str] = statements.get('mainSqlIfExists', [])
                                for sql in sqls:
                                    stmt: str = bindParams(sql)
                                    session.sql(stmt).collect()
                            else:
                                # execute mainSqlIfNotExists
                                sqls: List[str] = statements.get('mainSqlIfNotExists', [])
                                for sql in sqls:
                                    stmt: str = bindParams(sql)
                                    session.sql(stmt).collect()

                            # execute postSqls
                            postSqls: List[str] = statements.get('postSqls', [])
                            for sql in postSqls:
                                stmt: str = bindParams(sql)
                                session.sql(stmt).collect()

                            session.sql(query=f"ALTER TABLE {sink} SET CHANGE_TRACKING = TRUE").collect()

                            # run expectations
                            if expectation_items and check_if_expectations_schema_exists():
                                for expectation in expectation_items:
                                    run_expectation(expectation.get("name", None), expectation.get("params", None), expectation.get("query", None), str_to_bool(expectation.get('failOnError', 'no')))

                            # COMMIT transaction
                            session.sql("COMMIT").collect()
                            end = datetime.now()
                            duration = (end - start).total_seconds()
                            print(f"Duration in seconds: {duration}")
                            if audit and check_if_audit_schema_exists():
                                print("Audit schema exists")
                                # insert audit record
                                if log_audit(domain, schema, True, duration, 'Success', end):
                                    print("Audit record inserted")
                                else:
                                    print("Error inserting audit record")
                            else:
                                print("Audit schema does not exist")
                            
                        except Exception as e:
                            # ROLLBACK transaction
                            session.sql("ROLLBACK").collect()
                            end = datetime.now()
                            duration = (end - start).total_seconds()
                            error_message = str(e)
                            print(f"Duration in seconds: {duration}")
                            print(f"Error: {error_message}")
                            if audit and check_if_audit_schema_exists():
                                print("Audit schema exists")
                                # insert audit record
                                if log_audit(domain, schema, False, duration, error_message, end):
                                    print("Audit record inserted")
                                else:
                                    print("Error inserting audit record")
                            else:
                                print("Audit schema does not exist")
                            raise e

                    kwargs.pop('params', None)
                    kwargs.pop('events', None)

                    return DAGTask(
                        name=task_id, 
                        definition=StoredProcedureCall(
                            func = fun,
                            args=[sink, statements, audit, expectations, expectation_items, safe_params, cron_expr, format], 
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
