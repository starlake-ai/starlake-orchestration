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

    def end_op(self, task_id: str, events: Optional[List[StarlakeDataset]] = None, **kwargs) -> Optional[DAGTask]:
        """Overrides IStarlakeJob.end_op()
        Generate a Snowflake task that will end the pipeline.
        """
        comment = kwargs.get('comment', f"end task for {task_id}")
        kwargs.update({'comment': comment})
        # TODO: implement the definition to update SL_START_DATE and SL_END_DATE if the backfill is enabled and the DAG is not scheduled - maybe we will have to retrieve all the dag runs that didn't execute successfully ?
        return super().end_op(task_id=task_id, events=events, **kwargs)

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

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: Optional[StarlakeSparkConfig]=None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> DAGTask:
        """Overrides IStarlakeJob.sl_load()
        Generate the Snowflake task that will run the starlake `load` command.

        Args:
            task_id (str): The optional task id.
            domain (str): The required domain of the table to load.
            table (str): The required table to load.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
            dataset (Union[StarlakeDataset, str]): The optional dataset to materialize.

        Returns:
            DAGTask: The Snowflake task.
        """
        kwargs.update({'load': True})
        if dataset:
            if isinstance(dataset, str):
                sink = dataset
            else:
                sink = dataset.sink
        else:
            sink = f"{domain}.{table}"
        kwargs.update({'sink': sink})
        comment = kwargs.get('comment', None)
        if not comment:
            comment = f"Starlake load {sink}"
        kwargs.update({'comment': comment})
        return super().sl_load(task_id=task_id, domain=domain, table=table, spark_config=spark_config, dataset=dataset, **kwargs)

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
            if isinstance(dataset, str):
                sink = dataset
            else:
                sink = dataset.sink
        else:
            params = kwargs.get('params', dict())
            sink = params.get('sink', kwargs.get('sink', transform_name))
        kwargs.update({'sink': sink})
        comment = kwargs.get('comment', None)
        if not comment:
            comment = f"Starlake transform {sink}"
        kwargs.update({'comment': comment})
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
        sink = kwargs.get('sink', None)
        if sink:
            kwargs.pop('sink', None)
            domainAndSchema = sink.split('.')
            domain = domainAndSchema[0]
            schema = domainAndSchema[-1]
            statements = self.caller_globals.get('statements', dict()).get(sink, None)
            audit = self.caller_globals.get('audit', dict())
            expectations = self.caller_globals.get('expectations', dict())
            expectation_items = self.caller_globals.get('expectation_items', dict()).get(sink, None)
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

            def bindParams(stmt: str) -> str:
                """Bind parameters to the SQL statement.
                Args:
                    stmt (str): The SQL statement.
                Returns:
                    str: The SQL statement with the parameters bound
                """
                return stmt.format_map(safe_params)

            def str_to_bool(value: str) -> bool:
                """Convert a string to a boolean.
                Args:
                    value (str): The string to convert.
                Returns:
                    bool: The boolean value.
                """
                truthy = {'yes', 'y', 'true', '1'}
                falsy = {'no', 'n', 'false', '0'}

                value = value.strip().lower()
                if value in truthy:
                    return True
                elif value in falsy:
                    return False
                raise ValueError(f"Valeur invalide : {value}")

            def check_if_schema_exists(session: Session, domain: str, schema: str) -> bool:
                """Check if the schema exists.
                Args:
                    session (Session): The Snowflake session.
                    domain (str): The domain.
                    schema (str): The schema.
                    Returns:
                    bool: True if the schema exists, False otherwise.
                """
                query=f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) ILIKE '{domain}.{schema}'"
                print(f"#Check if table exists:\n{query};")
                df = session.sql(query=query)
                rows = df.collect()
                return rows.__len__() > 0

            def check_if_audit_schema_exists(session: Session) -> bool:
                """Check if the audit schema exists.
                Args:
                    session (Session): The Snowflake session.
                    Returns:
                    bool: True if the audit schema exists, False otherwise.
                """
                if audit:
                    try:
                        # create SQL domain
                        domain = audit.get('domain', ['audit'])[0]
                        query=f"CREATE SCHEMA IF NOT EXISTS {domain}"
                        print(f"#Create audit schema if not exists:\n{query};")
                        session.sql(query=query).collect()
                        # execute SQL preActions
                        preActions: List[str] = audit.get('preActions', [])
                        for sql in preActions:
                            stmt: str = bindParams(sql)
                            print(f"#Execute audit pre action:\n{stmt};")
                            session.sql(stmt).collect()
                        # check if the audit schema exists
                        if not check_if_schema_exists(session, domain, 'audit'):
                            # execute SQL createSchemaSql
                            sqls: List[str] = audit.get('createSchemaSql', [])
                            for sql in sqls:
                                stmt: str = bindParams(sql)
                                print(f"#Create audit table:\n{stmt};")
                                session.sql(stmt).collect()
                            return True
                        else:
                            return True
                    except Exception as e:
                        print(f"Error creating audit table: {str(e)}")
                        return False
                else:
                    return False

            def check_if_expectations_schema_exists(session: Session) -> bool:
                """Check if the expectations schema exists.
                Args:
                    session (Session): The Snowflake session.
                    Returns:
                    bool: True if the expectations schema exists, False otherwise.
                """
                if expectations:
                    try:
                        # create SQL domain
                        domain = expectations.get('domain', ['audit'])[0]
                        query=f"CREATE SCHEMA IF NOT EXISTS {domain}"
                        print(f"#Create expectations schema if not exists:\n{query};")
                        session.sql(query=query).collect()
                        # check if the expectations schema exists
                        if not check_if_schema_exists(session, domain, 'expectations'):
                            # execute SQL createSchemaSql
                            sqls: List[str] = expectations.get('createSchemaSql', [])
                            for sql in sqls:
                                stmt: str = bindParams(sql)
                                print(f"#Create expectations table:\n{stmt};")
                                session.sql(stmt).collect()
                            return True
                        else:
                            return True
                    except Exception as e:
                        print(f"Error creating expectations table: {str(e)}")
                        return False
                else:
                    return False

            from datetime import datetime

            def log_audit(session: Session, count: int, countAccepted: int, countRejected: int, success: bool, duration: int, message: str, ts: datetime, jobid: Optional[str] = None, step: Optional[str] = None) -> bool :
                """Log the audit record.
                Args:
                    session (Session): The Snowflake session.
                    count (int): The count.
                    countAccepted (int): The count accepted.
                    countRejected (int): The count rejected.
                    success (bool): The success.
                    duration (int): The duration.
                    message (str): The message.
                    ts (datetime): The timestamp.
                    jobid (Optional[str], optional): The optional job id. Defaults to None.
                    step (Optional[str], optional): The optional step. Defaults to None.
                Returns:
                    bool: True if the audit record was logged, False otherwise.
                """
                if audit and check_if_audit_schema_exists(session):
                    audit_domain = audit.get('domain', ['audit'])[0]
                    audit_sqls = audit.get('mainSqlIfExists', None)
                    if audit_sqls:
                        try:
                            audit_sql = audit_sqls[0]
                            formatted_sql = audit_sql.format(
                                jobid = jobid or f'{domain}.{schema}',
                                paths = schema,
                                domain = domain,
                                schema = schema,
                                success = str(success),
                                count = str(count),
                                countAccepted = str(countAccepted),
                                countRejected = str(countRejected),
                                timestamp = ts.strftime("%Y-%m-%d %H:%M:%S"),
                                duration = str(duration),
                                message = message,
                                step = step or "TRANSFORM",
                                database = "",
                                tenant = ""
                            )
                            insert_sql = f"INSERT INTO {audit_domain}.audit {formatted_sql}"
                            print(f"#Insert audit record:\n{insert_sql};")
                            session.sql(insert_sql).collect()
                            return True
                        except Exception as e:
                            print(f"Error inserting audit record: {str(e)}")
                            return False
                    else:
                        return False
                else:
                    return False

            def log_expectation(session: Session, success: bool, name: str, params: str, sql: str, count: int, exception: str, ts: datetime, jobid: Optional[str] = None) -> bool :
                """Log the expectation record.
                Args:
                    session (Session): The Snowflake session.
                    success (bool): whether the expectation has been successfully checked or not.
                    name (str): The name of the expectation.
                    params (str): The params for the expectation.
                    sql (str): The SQL.
                    count (int): The count.
                    exception (str): The exception.
                    ts (datetime): The timestamp.
                    jobid (Optional[str], optional): The optional job id. Defaults to None.
                Returns:
                    bool: True if the expectation record was logged, False otherwise.
                """
                if expectations and check_if_expectations_schema_exists(session):
                    expectation_domain = expectations.get('domain', ['audit'])[0]
                    expectation_sqls = expectations.get('mainSqlIfExists', None)
                    if expectation_sqls:
                        try:
                            expectation_sql = expectation_sqls[0]
                            formatted_sql = expectation_sql.format(
                                jobid = jobid or f'{domain}.{schema}',
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
                            print(f"#Insert expectations record:\n{insert_sql};")
                            session.sql(insert_sql).collect()
                            return True
                        except Exception as e:
                            print(f"Error inserting expectations record: {str(e)}")
                            return False
                    else:
                        return False
                else:
                    return False

            def run_expectation(session: Session, name: str, params: str, query: str, failOnError: bool = False, jobid: Optional[str] = None) -> None:
                """Run the expectation.
                Args:
                    session (Session): The Snowflake session.
                    name (str): The name of the expectation.
                    params (str): The params for the expectation.
                    query (str): The query.
                    failOnError (bool, optional): Whether to fail on error. Defaults to False.
                    jobid (Optional[str], optional): The optional job id. Defaults to None.
                """
                count = 0
                try:
                    if query:
                        stmt: str = bindParams(query)
                        print(f"#Run expectation {name}:\n{stmt};")
                        df = session.sql(stmt)
                        rows = df.collect()
                        if rows.__len__() != 1:
                            raise Exception(f'Expectation failed for {sink}: {query}. Expected 1 row but got {rows.__len__()}')
                        count = rows[0][0]
                        #  log expectations as audit in expectation table here
                        if count != 0:
                            raise Exception(f'Expectation failed for {sink}: {query}. Expected count to be equal to 0 but got {count}')
                        log_expectation(session, True, name, params, query, count, "", datetime.now(), jobid)
                    else:
                        raise Exception(f'Expectation failed for {sink}: {name}. Query not found')
                except Exception as e:
                    print(f"Error running expectation {name}: {str(e)}")
                    log_expectation(session, False, name, params, query, count, str(e), datetime.now(), jobid)
                    if failOnError:
                        raise e

            def run_expectations(session: Session, jobid: Optional[str] = None) -> None:
                """Run the expectations.
                Args:
                    session (Session): The Snowflake session.
                    jobid (Optional[str], optional): The optional job id. Defaults to None.
                """
                if expectation_items and check_if_expectations_schema_exists(session):
                    for expectation in expectation_items:
                        run_expectation(session, expectation.get("name", None), expectation.get("params", None), expectation.get("query", None), str_to_bool(expectation.get('failOnError', 'no')), jobid)

            if kwargs.get('transform', False):
                kwargs.pop('transform', None)
                if statements:

                    cron_expr = kwargs.get('cron_expr', None)
                    kwargs.pop('cron_expr', None)

                    format = '%Y-%m-%d %H:%M:%S%z'

                    # create the function that will execute the transform
                    def fun(session: Session) -> None:
                        from datetime import datetime

                        if cron_expr:
                            from croniter import croniter
                            from croniter.croniter import CroniterBadCronError
                            # get the original scheduled timestamp of the initial graph run in the current group
                            # For graphs that are retried, the returned value is the original scheduled timestamp of the initial graph run in the current group.
                            config = session.call("system$get_task_graph_config")
                            if config:
                                import json
                                config = json.loads(config)
                            else:
                                config = {}
                            original_schedule = config.get("SL_START_DATE", None)
                            if not original_schedule:
                                query = "SELECT to_timestamp(system$task_runtime_info('CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP'))"
                                print(f"#Get the original scheduled timestamp of the initial graph run:\n{query};")
                                original_schedule = session.sql(query=query).collect()[0][0]
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
                                safe_params.update({'sl_start_date': sl_start_date.strftime(format), 'sl_end_date': sl_end_date.strftime(format)})
                            except CroniterBadCronError:
                                raise ValueError(f"Invalid cron expression: {cron_expr}")

                        jobid = str(session.call("system$current_user_task_name"))

                        start = datetime.now()

                        try:
                            # BEGIN transaction
                            print("#BEGIN transaction:\nBEGIN;")
                            session.sql("BEGIN").collect()

                            # create SQL domain
                            query=f"CREATE SCHEMA IF NOT EXISTS {domain}"
                            print(f"#Create schema if not exists:\n{query};")
                            session.sql(query=query).collect()

                            # execute preActions
                            preActions: List[str] = statements.get('preActions', [])
                            for sql in preActions:
                                stmt: str = bindParams(sql)
                                print(f"#Execute pre action:\n{stmt};")
                                session.sql(stmt).collect()

                            # execute preSqls
                            preSqls: List[str] = statements.get('preSqls', [])
                            for sql in preSqls:
                                stmt: str = bindParams(sql)
                                print(f"#Execute pre sql:\n{stmt};")
                                session.sql(stmt).collect()

                            if check_if_schema_exists(session, domain, schema):
                                # execute addSCD2ColumnsSqls
                                scd2_sqls: List[str] = statements.get('addSCD2ColumnsSqls', [])
                                for sql in scd2_sqls:
                                    stmt: str = bindParams(sql)
                                    print(f"#Execute add SCD2 columns:\n{stmt};")
                                    session.sql(stmt).collect()
                                # execute mainSqlIfExists
                                sqls: List[str] = statements.get('mainSqlIfExists', [])
                                for sql in sqls:
                                    stmt: str = bindParams(sql)
                                    print(f"#Execute main sql if exists:\n{stmt};")
                                    session.sql(stmt).collect()
                            else:
                                # execute mainSqlIfNotExists
                                sqls: List[str] = statements.get('mainSqlIfNotExists', [])
                                for sql in sqls:
                                    stmt: str = bindParams(sql)
                                    print(f"#Execute main sql if not exists:\n{stmt};")
                                    session.sql(stmt).collect()

                            # execute postSqls
                            postSqls: List[str] = statements.get('postSqls', [])
                            for sql in postSqls:
                                stmt: str = bindParams(sql)
                                print(f"#Execute post sql:\n{stmt};")
                                session.sql(stmt).collect()

                            query = f"ALTER TABLE {sink} SET CHANGE_TRACKING = TRUE"
                            print(f"#Enable change tracking:\n{query};")
                            session.sql(query=query).collect()

                            # run expectations
                            run_expectations(session, jobid)

                            # COMMIT transaction
                            print("#COMMIT transaction:\nCOMMIT;")
                            session.sql("COMMIT").collect()
                            end = datetime.now()
                            duration = (end - start).total_seconds()
                            print(f"#Duration in seconds: {duration}")
                            log_audit(session, -1, -1, -1, True, duration, 'Success', end, jobid, "TRANSFORM")
                            
                        except Exception as e:
                            # ROLLBACK transaction
                            error_message = str(e)
                            print(f"Error executing transform for {sink}: {error_message}")
                            print("#ROLLBACK transaction:\nROLLBACK;")
                            session.sql("ROLLBACK").collect()
                            end = datetime.now()
                            duration = (end - start).total_seconds()
                            print(f"Duration in seconds: {duration}")
                            log_audit(session, -1, -1, -1, False, duration, error_message, end, jobid, "TRANSFORM")
                            raise e

                    kwargs.pop('params', None)
                    kwargs.pop('events', None)

                    return DAGTask(
                        name=task_id, 
                        definition=StoredProcedureCall(
                            func = fun,
                            args=[], 
                            stage_location=self.stage_location,
                            packages=self.packages,
                        ), 
                        comment=comment, 
                        **kwargs
                    )
                else:
                    # sink statements are required
                    raise ValueError(f"Transform '{sink}' statements not found")
            else:
                # only sl_transform is implemented
                raise NotImplementedError("Not implemented")
        else:
            # sink is required
            raise ValueError("sink is required")
