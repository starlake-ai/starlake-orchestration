from __future__ import annotations

from ai.starlake.job import StarlakeOrchestrator

from ai.starlake.dataset import StarlakeDataset
from ai.starlake.orchestration import AbstractOrchestration, StarlakeSchedule, StarlakeDependencies, AbstractPipeline, AbstractTaskGroup, AbstractTask, AbstractDependency

from ai.starlake.snowflake.starlake_snowflake_job import StarlakeSnowflakeJob

from snowflake.core.task import Cron, StoredProcedureCall, Task
from snowflake.core.task.dagv1 import DAG, DAGTask, _dag_context_stack
from snowflake.snowpark import Session

from typing import Any, List, Optional, Union

from types import ModuleType

from datetime import timedelta

class SnowflakeDag(DAG):
    def __init__(
        self,
        name: str,
        *,
        schedule: Optional[Union[Cron, timedelta]] = None,
        warehouse: Optional[str] = None,
        user_task_managed_initial_warehouse_size: Optional[str] = None,
        error_integration: Optional[str] = None,
        comment: Optional[str] = None,
        task_auto_retry_attempts: Optional[int] = None,
        allow_overlapping_execution: Optional[bool] = None,
        user_task_timeout_ms: Optional[int] = None,
        suspend_task_after_num_failures: Optional[int] = None,
        config: Optional[dict[str, Any]] = None,
        session_parameters: Optional[dict[str, Any]] = None,
        stage_location: Optional[str] = None,
        imports: Optional[list[Union[str, tuple[str, str]]]] = None,
        packages: Optional[list[Union[str, ModuleType]]] = None,
        use_func_return_value: bool = False,
        computed_cron: Optional[Cron] = None,
        not_scheduled_datasets: Optional[List[StarlakeDataset]] = None,
        least_frequent_datasets: Optional[List[StarlakeDataset]] = None,
        most_frequent_datasets: Optional[List[StarlakeDataset]] = None,
    ) -> None:
        condition = None

        definition=f"select '{name}'"

        if not schedule: # if the DAG is not scheduled we will rely on streams to trigger the underlying dag and check if the scheduled datasets without streams have data using CHANGES
            changes = dict() # tracks the datasets whose changes have to be checked

            if least_frequent_datasets:
                print(f"least frequent datasets: {','.join(list(map(lambda x: x.sink, least_frequent_datasets)))}")
                for dataset in least_frequent_datasets:
                    changes.update({dataset.sink: dataset.cron})

            not_scheduled_streams = set() # set of streams which underlying datasets are not scheduled
            not_scheduled_datasets_without_streams = []
            if not_scheduled_datasets:
                print(f"not scheduled datasets: {','.join(list(map(lambda x: x.sink, not_scheduled_datasets)))}")
                for dataset in not_scheduled_datasets:
                    if dataset.stream:
                        not_scheduled_streams.add(f"SYSTEM$STREAM_HAS_DATA('{dataset.stream}')")
                    else:
                        not_scheduled_datasets_without_streams.append(dataset)
            if not_scheduled_datasets_without_streams:
                print(f"Warning: No streams found for {','.join(list(map(lambda x: x.sink, not_scheduled_datasets_without_streams)))}")
                ... # nothing to do here

            if most_frequent_datasets:
                print(f"most frequent datasets: {','.join(list(map(lambda x: x.sink, most_frequent_datasets)))}")
            streams = set()
            most_frequent_datasets_without_streams = []
            if most_frequent_datasets:
                for dataset in most_frequent_datasets:
                    if dataset.stream:
                        streams.add(f"SYSTEM$STREAM_HAS_DATA('{dataset.stream}')")
                    else:
                        most_frequent_datasets_without_streams.append(dataset)
                        changes.update({dataset.sink: dataset.cron})
            if most_frequent_datasets_without_streams:
                print(f"Warning: No streams found for {','.join(list(map(lambda x: x.sink, most_frequent_datasets_without_streams)))}")
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

                    def check_if_dataset_exists(dataset: str) -> bool:
                        df = session.sql(query=f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) ILIKE '{dataset}'")
                        rows = df.collect()
                        return rows.__len__() > 0

                    for dataset, cron_expr in changes.items():
                        if not check_if_dataset_exists(dataset):
                            raise ValueError(f"Dataset {dataset} does not exist")
                        try:
                            # enabling change tracking for the dataset
                            print(f"Enabling change tracking for dataset {dataset}")
                            session.sql(query=f"ALTER TABLE {dataset} SET CHANGE_TRACKING = TRUE").collect() # should be done once and when we create our datasets
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
                            change = f"SELECT count(*) FROM {dataset} CHANGES(INFORMATION => DEFAULT) AT(TIMESTAMP => '{sl_start_date.strftime(format)}') END (TIMESTAMP => '{sl_end_date.strftime(format)}')"
                            print(f"Checking changes for dataset {dataset} from {sl_start_date.strftime(format)} to {sl_end_date.strftime(format)} -> {change}")
                            df = session.sql(query=change)
                            count = df.collect()[0][0]
                            if count == 0:
                                raise ValueError(f"Dataset {dataset} has no changes from {sl_start_date.strftime(format)} to {sl_end_date.strftime(format)}")
                            print(f"Dataset {dataset} has data from {sl_start_date.strftime(format)} to {sl_end_date.strftime(format)}")
                        except CroniterBadCronError:
                            raise ValueError(f"Invalid cron expression: {cron_expr}")
                        except Exception as e:
                            raise ValueError(f"Error checking changes for dataset {dataset}: {str(e)}")

                definition = StoredProcedureCall(
                    func = fun, 
                    args=[changes, format],
                    stage_location=stage_location,
                    packages=packages
                )

        if not schedule and not condition:
            if computed_cron:
                schedule = computed_cron
            else:
                raise ValueError("A DAG must be scheduled or have a condition")

        super().__init__(name, schedule=schedule, warehouse=warehouse, user_task_managed_initial_warehouse_size=user_task_managed_initial_warehouse_size, error_integration=error_integration, comment=comment, task_auto_retry_attempts=task_auto_retry_attempts, allow_overlapping_execution=allow_overlapping_execution, user_task_timeout_ms=user_task_timeout_ms, suspend_task_after_num_failures=suspend_task_after_num_failures, config=config, session_parameters=session_parameters, stage_location=stage_location, imports=imports, packages=packages, use_func_return_value=use_func_return_value)
        self.definition = definition
        self.condition = condition

    def _to_low_level_task(self) -> Task:
        return Task(
            name=f"{self.name}",
            definition=self.definition,
            condition=self.condition,
            schedule=self.schedule,
            warehouse=self.warehouse,
            user_task_managed_initial_warehouse_size=self.user_task_managed_initial_warehouse_size,
            error_integration=self.error_integration,
            comment=self.comment,
            task_auto_retry_attempts=self.task_auto_retry_attempts,
            allow_overlapping_execution=self.allow_overlapping_execution,
            user_task_timeout_ms=self.user_task_timeout_ms,
            suspend_task_after_num_failures=self.suspend_task_after_num_failures,
            session_parameters=self.session_parameters,
            config=self.config,
        )

class SnowflakePipeline(AbstractPipeline[DAG, DAGTask, List[DAGTask], StarlakeDataset], StarlakeDataset):
    def __init__(self, job: StarlakeSnowflakeJob, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, orchestration: Optional[AbstractOrchestration[DAG, DAGTask, List[DAGTask], StarlakeDataset]] = None, **kwargs) -> None:
        super().__init__(job, orchestration_cls=SnowflakeOrchestration, dag=None, schedule=schedule, dependencies=dependencies, orchestration=orchestration, **kwargs)

        snowflake_schedule: Union[Cron, None] = None
        computed_cron: Union[Cron, None] = None
        if self.cron is not None:
            snowflake_schedule = Cron(self.cron, job.timezone)
        elif self.datasets is not None:
            if self.computed_cron_expr is not None:
                computed_cron = Cron(self.computed_cron_expr, job.timezone)
            else:
                snowflake_schedule = timedelta(minutes=60) # FIXME should we keep a default value here?

        self.dag = SnowflakeDag(
            name=self.pipeline_id,
            schedule=snowflake_schedule,
            warehouse=job.warehouse,
            comment=job.caller_globals.get('description', f"Pipeline {self.pipeline_id}"),
            stage_location=job.stage_location,
            packages=job.packages,
            computed_cron=computed_cron,
            not_scheduled_datasets=self.not_scheduled_datasets,
            least_frequent_datasets=self.least_frequent_datasets,
            most_frequent_datasets=self.most_frequent_datasets,
            
        )

    def __enter__(self):
        _dag_context_stack.append(self.dag)
        return super().__enter__()
    
    def __exit__(self, exc_type, exc_value, traceback):
        _dag_context_stack.pop()

        # walk throw the dag to add snowflake dependencies

        def get_node(dependency: AbstractDependency) -> Union[DAGTask, SnowflakeTaskGroup, None]:
            if isinstance(dependency, SnowflakeTaskGroup):
                return dependency
            elif isinstance(dependency, AbstractTask):
                return dependency.task
            return None

        def update_group_dependencies(group: AbstractTaskGroup):
            def update_dependencies(upstream_dependencies, root_key):
                root = group.get_dependency(root_key)
                temp_root_node = get_node(root)
                if isinstance(temp_root_node, SnowflakeTaskGroup):
                    leaves = temp_root_node.group_leaves
                    if leaves.__len__() >= 1:
                        root_node = leaves[-1] # we may improve this
                    else:
                        root_node = None
                else:
                    root_node = temp_root_node
                if isinstance(root, AbstractTaskGroup) and root_key != group.group_id:
                    update_group_dependencies(root)
                if root_key in upstream_dependencies:
                    for key in upstream_dependencies[root_key]:
                        downstream = group.get_dependency(key)
                        temp_downstream_node = get_node(downstream)
                        if isinstance(downstream, AbstractTaskGroup) and key != group.group_id:
                            update_group_dependencies(downstream)
                        if root_node is not None:
                            if isinstance(temp_downstream_node, SnowflakeTaskGroup):
                                root_node.add_successors(temp_downstream_node.group_roots)
                            elif temp_downstream_node is not None:
                                root_node.add_successors(temp_downstream_node)
                        update_dependencies(upstream_dependencies, key)

            upstream_dependencies = group.upstream_dependencies
            upstream_keys = upstream_dependencies.keys()
            downstream_keys = group.downstream_dependencies.keys()
            root_keys = upstream_keys - downstream_keys

            if not root_keys and len(upstream_keys) == 0 and len(downstream_keys) == 0:
                root_keys = group.dependencies_dict.keys()

            for root_key in root_keys:
                update_dependencies(upstream_dependencies, root_key)

        update_group_dependencies(self)

        return super().__exit__(exc_type, exc_value, traceback)

class SnowflakeTaskGroup(AbstractTaskGroup[List[DAGTask]]):
    def __init__(self, group_id: str, group: List[DAGTask], dag: Optional[DAG] = None, **kwargs) -> None:
        super().__init__(group_id=group_id, orchestration_cls=SnowflakeOrchestration, group=group, **kwargs)
        self.dag = dag
        self._group_as_map = dict()

    def __exit__(self, exc_type, exc_value, traceback):
        for dep in self.dependencies:
            if isinstance(dep, SnowflakeTaskGroup):
                self._group_as_map.update({dep.id: dep})
            elif isinstance(dep, AbstractTask):
                self._group_as_map.update({dep.id: dep.task})
        return super().__exit__(exc_type, exc_value, traceback)

    @property
    def group_leaves(self) -> List[DAGTask]:
        leaves = []
        for leaf in self.leaves_keys:
            dep = self._group_as_map.get(leaf, None)
            if dep:
                if isinstance(dep, SnowflakeTaskGroup):
                    leaves.extend(dep.group_leaves)
                else:
                    leaves.append(dep)
        return leaves

    @property
    def group_roots(self) -> List[DAGTask]:
        roots = []
        for root in self.roots_keys:
            dep = self._group_as_map.get(root, None)
            if dep:
                if isinstance(dep, SnowflakeTaskGroup):
                    roots.extend(dep.group_roots)
                else:
                    roots.append(dep)
        return roots

class SnowflakeOrchestration(AbstractOrchestration[DAG, DAGTask, List[DAGTask], StarlakeDataset]):
    def __init__(self, job: StarlakeSnowflakeJob, **kwargs) -> None:
        """Overrides AbstractOrchestration.__init__()
        Args:
            job (StarlakeSnowflakeJob): The Snowflake job that will generate the tasks within the pipeline.
        """
        super().__init__(job, **kwargs) 

    @classmethod
    def sl_orchestrator(cls) -> str:
        return StarlakeOrchestrator.SNOWFLAKE

    def sl_create_pipeline(self, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, **kwargs) -> AbstractPipeline[DAG, DAGTask, List[DAGTask], StarlakeDataset]:
        """Create the Starlake pipeline to orchestrate.

        Args:
            schedule (Optional[StarlakeSchedule]): The optional schedule
            dependencies (Optional[StarlakeDependencies]): The optional dependencies
        
        Returns:
            AbstractPipeline[DAG, DAGTask, List[DAGTask], StarlakeDataset]: The pipeline to orchestrate.
        """
        return SnowflakePipeline(
            self.job, 
            schedule, 
            dependencies, 
            self
        )

    def sl_create_task(self, task_id: str, task: Optional[Union[DAGTask, List[DAGTask]]], pipeline: AbstractPipeline[DAG, DAGTask, List[DAGTask], StarlakeDataset]) -> Optional[Union[AbstractTask[DAGTask], AbstractTaskGroup[List[DAGTask]]]]:
        if task is None:
            return None

        elif isinstance(task, list):
            task_group = SnowflakeTaskGroup(
                group_id = task[0].name.split('.')[-1],
                group = task, 
                dag = pipeline.dag,
            )

            with task_group:

                tasks = task
                # sorted_tasks = []

                visited = {}

                def visit(t: Union[DAGTask, List[DAGTask]]) -> Optional[Union[AbstractTask[DAGTask], AbstractTaskGroup[List[DAGTask]]]]:
                    if isinstance(t, List[DAGTask]):
                        v_task_id = t[0].name.split('.')[-1]
                    else:
                        v_task_id = t.name
                    if v_task_id in visited.keys():
                        return visited.get(v_task_id)
                    v = self.sl_create_task(v_task_id.split('.')[-1], t, pipeline)
                    visited.update({v_task_id: v})
                    if isinstance(t, DAGTask):
                        for upstream in t.predecessors:  # Visite récursive des tâches en amont
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
            task._dag = pipeline.dag
            return AbstractTask(task_id, task)

    def sl_create_task_group(self, group_id: str, pipeline: AbstractPipeline[DAG, DAGTask, List[DAGTask], StarlakeDataset], **kwargs) -> AbstractTaskGroup[List[DAGTask]]:
        return SnowflakeTaskGroup(
            group_id, 
            group=[],
            dag=pipeline.dag, 
            **kwargs
        )

    @classmethod
    def from_native(cls, native: Any) -> Optional[Union[AbstractTask[DAGTask], AbstractTaskGroup[List[DAGTask]]]]:
        """Create a task or task group from a native object.
        Args:
            native (Any): the native object.
        Returns:
            Optional[Union[AbstractTask[DAGTask], AbstractTaskGroup[List[DAGTask]]]]: the task or task group.
        """
        if isinstance(native, list):
            return SnowflakeTaskGroup(native[0].name.split('.')[-1], native)
        elif isinstance(native, DAGTask):
            return AbstractTask(native.task_id, native)
        else:
            return None
