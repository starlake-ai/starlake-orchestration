# Starlake Orchestration

The core Python module for creating, scheduling, and managing data pipelines across multiple orchestration platforms.

## What is Starlake?

Starlake is a **configuration-driven** platform designed to simplify **Extract**, **Load**, and **Transform** (**ELT**) operations while supporting declarative **orchestration** of data pipelines. By minimizing coding requirements, it empowers users to create robust data workflows with YAML-based configurations.

### Typical Use Case

1. **Extract**: Gather data from sources such as Fixed Position files, DSV (Delimiter-Separated Values), JSON, or XML formats.
2. **Define or infer structure**: Use YAML to describe or infer the schema for each data source.
3. **Load**: Configure and execute the loading process to your data warehouse or other sinks.
4. **Transform**: Build aggregates and join datasets using SQL, Jinja, and YAML configurations.
5. **Output**: Observe your data becoming available as structured tables in your data warehouse.

### Flexibility Across Workflows

Starlake supports **any or all** steps in your data pipeline, allowing for seamless integration into existing workflows:

* **Extract**: Export selective data from SQL databases into CSV files.
* **Preload**: Evaluate whether the loading process should proceed, based on a configurable preload strategy.
* **Load**: Ingest FIXED-WIDTH, CSV, JSON, or XML files, converting them into strongly-typed records stored as Parquet files, data warehouse tables (e.g., Google BigQuery), or other configured sinks.
* **Transform**: Join loaded datasets and save them as Parquet files, data warehouse tables, or Elasticsearch indices.

## What is Starlake Orchestration?

Starlake Orchestration is a **Python-based API** for creating, scheduling, and managing data pipelines. It abstracts the complexities of various orchestration platforms, offering a unified interface for pipeline orchestration.

It is recommended to use it in combination with **[starlake dag generation](https://docs.starlake.ai/guides/orchestrate/customization)**, but it can also be used directly in your DAGs.

### Key Features

#### 1. Multi-Orchestrator Support

Starlake Orchestration integrates seamlessly with multiple orchestration frameworks, letting you select the best fit for your requirements.

#### 2. Write Once, Deploy Anywhere

Design your pipelines once and execute them seamlessly across diverse orchestrators and environments without rewriting code. Starlake ensures consistent pipeline definitions, whether you are using **Airflow**, **Dagster**, or **Snowflake** on **Google Cloud Platform** (GCP), **Amazon Web Services** (AWS), or on-premises.

Run Starlake jobs effortlessly using **GCP Cloud Run**, **GCP Dataproc**, **AWS Fargate**, or simple **shell scripts**.

This flexibility empowers teams to:

* Transition seamlessly between execution environments.
* Integrate with cloud-native or on-premises orchestration tools.
* Simplify deployments without compromising functionality or performance.

#### 3. Data Freshness and Scheduling

Starlake Orchestration supports **flexible scheduling mechanisms**, ensuring your data pipelines deliver up-to-date results:

* **Cron-based Scheduling**: Automate periodic pipeline runs (e.g., "Run at 2 AM daily").
* **Event-Driven Orchestration**: Dynamically trigger pipelines using **dataset-aware DAGs**, ensuring dependencies and lineage are respected.

By leveraging data lineage and dependencies, Starlake Orchestration aligns schedules automatically, ensuring the freshness of interconnected datasets.

#### 4. Simplified Management

With automated schedule alignment and dependency management, Starlake Orchestration eliminates manual adjustments and simplifies pipeline workflows while maintaining reliability.

## Prerequisites

* **Starlake CLI** 1.5.7+
* **Python** 3.8+

## Installation

```bash
pip install starlake-orchestration
```

## Package Structure

The core module provides the following packages under `ai.starlake`:

| Package | Description |
|---------|-------------|
| `common` | Utility functions, enums, cron helpers (`sanitize_id`, `is_valid_cron`, `sort_crons_by_frequency`, etc.) |
| `dataset` | Dataset identity, event abstraction, triggering strategies |
| `job` | Job execution, CLI invocation, pre-load strategies, Spark config |
| `orchestration` | Pipeline lifecycle, dependency graph, task grouping, factories, CLI entry point |
| `odbc` | SQL session abstraction (DuckDB, PostgreSQL, MySQL, Redshift, Snowflake, BigQuery) |
| `aws` | AWS-specific helpers (Fargate configuration) |
| `gcp` | GCP-specific helpers (Dataproc cluster configuration) |

## Main Components

### 1. IStarlakeJob[T, E]

`ai.starlake.job.IStarlakeJob` serves as the **generic factory interface** for creating orchestration tasks. These tasks execute the appropriate Starlake CLI commands, allowing seamless integration with orchestration platforms.

#### Classification Methods

```python
@classmethod
def sl_orchestrator(cls) -> Union[StarlakeOrchestrator, str, None]:
    """Returns the orchestrator type for this job implementation."""

@classmethod
def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str, None]:
    """Returns the execution environment type for this job implementation."""
```

#### Core Abstract Method

```python
@abstractmethod
def sl_job(
    self,
    task_id: str,
    arguments: list,
    spark_config: Optional[StarlakeSparkConfig] = None,
    dataset: Optional[Union[StarlakeDataset, str]] = None,
    task_type: Optional[TaskType] = None,
    **kwargs
) -> T:
    """Create an orchestrator-specific task that runs a Starlake CLI command."""
```

| Parameter | Type | Description |
|-----------|------|-------------|
| task_id | `str` | The required task id |
| arguments | `list` | The required arguments of the starlake command to run |
| spark_config | `StarlakeSparkConfig` | The optional spark configuration |
| dataset | `Union[StarlakeDataset, str]` | The optional dataset to publish |
| task_type | `TaskType` | The optional task type |

#### Factory Methods for Core Starlake Commands

##### Pre-load

Generates the task that will run the starlake [preload](https://docs.starlake.ai/cli/preload) command.

```python
def sl_pre_load(
    self,
    domain: str,
    tables: set = set(),
    pre_load_strategy: Union[StarlakePreLoadStrategy, str, None] = None,
    sensor: Optional[bool] = None,
    **kwargs
) -> Optional[T]:
```

| Parameter | Type | Description |
|-----------|------|-------------|
| domain | `str` | The required domain to pre-load |
| tables | `set` | The optional tables to pre-load |
| pre_load_strategy | `Union[StarlakePreLoadStrategy, str, None]` | The optional pre-load strategy (`self.pre_load_strategy` by default) |
| sensor | `Optional[bool]` | Optional sensor-mode override (the `pre_load_sensor` option by default): when enabled, the pre-load task pokes `starlake preload` every `pre_load_poke_interval` seconds within the `pre_load_timeout` wall-clock window instead of running once — shell execution environment only |

###### StarlakePreLoadStrategy

`ai.starlake.job.StarlakePreLoadStrategy` is an enumeration defining preload strategies for conditional domain loading.

1. **NONE** -- No condition applied; preload tasks are skipped.

   ![none strategy example](https://raw.githubusercontent.com/starlake-ai/starlake-orchestration/main/images/none.png)

2. **IMPORTED** -- Load only if files exist in the landing area (`SL_ROOT/datasets/importing/{domain}`).

   ![imported strategy example](https://raw.githubusercontent.com/starlake-ai/starlake-orchestration/main/images/imported.png)

3. **PENDING** -- Load only if files exist in the pending datasets area (`SL_ROOT/datasets/pending/{domain}`).

   ![pending strategy example](https://raw.githubusercontent.com/starlake-ai/starlake-orchestration/main/images/pending.png)

4. **ACK** -- Load only if an acknowledgment file exists at the configured path (`global_ack_file_path`).

   ![ack strategy example](https://raw.githubusercontent.com/starlake-ai/starlake-orchestration/main/images/ack.png)

**IMPORTED chain:** `sl_pre_load` >> `skip_or_start` >> `sl_import` >> `sl_load`

##### Import

Generates the task for the [import](https://docs.starlake.ai/cli/import) command.

```python
def sl_import(
    self,
    task_id: str,
    domain: str,
    tables: set = set(),
    **kwargs
) -> T:
```

| Parameter | Type | Description |
|-----------|------|-------------|
| task_id | `str` | The optional task id (`{domain}_import` by default) |
| domain | `str` | The required domain to import |
| tables | `set` | The optional tables to import |

##### Load

Generates the task for the [load](https://docs.starlake.ai/cli/load) command.

```python
def sl_load(
    self,
    task_id: str,
    domain: str,
    table: str,
    spark_config: Optional[StarlakeSparkConfig] = None,
    dataset: Optional[Union[StarlakeDataset, str]] = None,
    **kwargs
) -> T:
```

| Parameter | Type | Description |
|-----------|------|-------------|
| task_id | `str` | The optional task id (`load_{domain}_{table}` by default) |
| domain | `str` | The required domain of the table to load |
| table | `str` | The required table to load |
| spark_config | `StarlakeSparkConfig` | The optional spark configuration |
| dataset | `Union[StarlakeDataset, str]` | The optional dataset to materialize |

##### Transform

Generates the task for the [transform](https://docs.starlake.ai/cli/transform) command.

```python
def sl_transform(
    self,
    task_id: str,
    transform_name: str,
    transform_options: str = None,
    spark_config: Optional[StarlakeSparkConfig] = None,
    dataset: Optional[Union[StarlakeDataset, str]] = None,
    **kwargs
) -> T:
```

| Parameter | Type | Description |
|-----------|------|-------------|
| task_id | `str` | The optional task id (`{transform_name}` by default) |
| transform_name | `str` | The transform to run |
| transform_options | `str` | The optional transform options |
| spark_config | `StarlakeSparkConfig` | The optional spark configuration |
| dataset | `Union[StarlakeDataset, str]` | The optional dataset to materialize |

### 2. StarlakeDataset

`ai.starlake.dataset.StarlakeDataset` represents the metadata of a dataset produced by a task.

Starlake Orchestration collects all datasets produced by each task into a list of events to trigger per task. At runtime, the orchestrator triggers subsequent events only if their corresponding tasks succeed.

Key properties:

| Property | Type | Description |
|----------|------|-------------|
| `name` / `uri` | `str` | Dataset identifier (sanitized for orchestrator compatibility) |
| `cron` | `Optional[str]` | Optional cron expression |
| `sink` | `Optional[str]` | `domain.table` reference |
| `domain` / `table` | `str` | Computed from sink or name |
| `url` | `str` | Full URL with query parameters |
| `datasetType` | `StarlakeDatasetType` | `LOAD` or `TRANSFORM` |

### 3. StarlakeOptions

`ai.starlake.job.StarlakeOptions` provides methods to manage and retrieve configuration variables. Variables are resolved in order: **options dict -> default_value -> environment variable**.

The following options are available for all concrete factory classes derived from `IStarlakeJob`:

| Option | Type | Description |
|--------|------|-------------|
| **default_pool** | `str` | Pool of slots to use (`default_pool` by default) |
| **sl_env_var** | `str` | Optional starlake environment variables passed as an encoded JSON string |
| **retries** | `int` | Number of retries to attempt before failing a task (`1` by default) |
| **retry_delay** | `int` | Delay between retries in seconds (`300` by default) |
| **pre_load_strategy** | `str` | One of `none` (default), `imported`, `pending`, or `ack` |
| **global_ack_file_path** | `str` | Path to the ack file (`{SL_DATASETS}/pending/{domain}/{{ds}}.ack` by default) |
| **ack_wait_timeout** | `int` | Timeout in seconds to wait for the ack file (`1 hour` by default); ignored in sensor mode (`pre_load_timeout` is the wall-clock window there) |
| **pre_load_sensor** | `bool` | `true`/`false` (default `false`) — turn the pre-load task into a sensor that pokes `starlake preload` until files arrive. SHELL execution environment only: cloud engines (cloud_run, dataproc, fargate) reject it with a `ValueError` at DAG-definition time — use the retries-as-poke workaround there (`retries` / `retry_delay` options) |
| **pre_load_poke_interval** | `int` | Seconds between two pokes in sensor mode (`300` by default) |
| **pre_load_timeout** | `int` | Wall-clock timeout in seconds for the pre-load sensor (`3600` by default); must be >= `pre_load_poke_interval` |
| **pre_load_sensor_soft_fail** | `bool` | `true`/`false` (default `false`) — on sensor timeout, skip the downstream loads instead of failing the run |
| **dataset_triggering_strategy** | `str` | One of `any` (default) or `all` |
| **timezone** | `str` | Timezone for scheduling (`UTC` by default) |

### 4. Abstract Classes

#### AbstractDependency

Defines task dependencies, ensuring execution order in Directed Acyclic Graphs (DAGs). Operators `>>` and `<<` allow intuitive chaining and automatically register dependencies in the current `TaskGroupContext`.

#### AbstractTask[T]

Wraps concrete orchestration tasks into a unified interface. Auto-registers with the current `TaskGroupContext` on creation.

#### AbstractTaskGroup[GT]

Groups related tasks into cohesive units. Supports nested grouping via context managers (`with` blocks).

#### AbstractPipeline[U, T, GT, E]

Defines an entire pipeline, combining tasks and task groups. It handles:

* **Task management**: Adding and managing orchestrator-specific tasks via `@final` methods (`sl_load`, `sl_transform`, `sl_import`, `sl_pre_load`).
* **Dependency management**: Ensuring the correct execution order.
* **Lifecycle**: `start_task()`, `end_task()`, `pre_tasks()`, `post_tasks()`.
* **Execution**: Only `run()` is abstract -- all other lifecycle methods (`deploy()`, `delete()`, `dry_run()`, `backfill()`) are concrete.

Pipelines are constructed with either:

* **`StarlakeSchedule`** -- time-driven: cron expression + list of domains/tables.
* **`StarlakeDependencies`** -- data-driven: parsed from `starlake dag-generate` JSON, with dependency graphs and dataset events.

#### AbstractOrchestration[U, T, GT, E]

The central abstraction for creating pipelines, tasks, and task groups. Orchestrator-specific implementations extend this class.

Key methods:

* `sl_orchestrator()` -- Returns the orchestrator type.
* `sl_create_pipeline(schedule, dependencies, ...)` -- Creates a pipeline instance.
* `sl_create_task_group(group_id, pipeline, ...)` -- Creates task groups for organizing related tasks.

### 5. TaskGroupContext

A context manager responsible for:

* Tracking the current task group or pipeline context via a static `_context_stack`.
* Automatically adding tasks to the active group.
* Managing dependencies within the group (upstream/downstream relationships, roots, leaves).

### 6. StarlakeDependencies

Parses JSON dependency graphs (produced by `starlake dag-generate`) into traversable `StarlakeDependency` trees. Key capabilities:

* `graphs()` -- Returns traversable `TreeNodeMixin` trees.
* `get_schedule()` -- Computes scheduling from dependencies.
* `retrieve_datasets()` -- Extracts datasets for event-driven triggering.

### 7. Factories

#### OrchestrationFactory

Handles the dynamic registration and instantiation of concrete orchestration classes.

```python
class OrchestrationFactory:
    @classmethod
    def register_orchestrations_from_package(cls, package_name: str = "ai.starlake") -> None:
        """Dynamically load all AbstractOrchestration subclasses from the given package."""

    @classmethod
    def register_orchestration(cls, orchestration_class: Type[AbstractOrchestration]) -> None:
        """Manually register an orchestration class."""

    @classmethod
    def create_orchestration(cls, job: IStarlakeJob, **kwargs) -> AbstractOrchestration:
        """Create the correct AbstractOrchestration instance based on job.sl_orchestrator()."""
```

#### StarlakeJobFactory

Handles the dynamic registration and instantiation of concrete `IStarlakeJob` classes.

```python
class StarlakeJobFactory:
    @classmethod
    def register_jobs_from_package(cls, package_name: str = "ai.starlake") -> None:
        """Dynamically load all IStarlakeJob subclasses from the given package."""

    @classmethod
    def register_job(cls, job_class: Type[IStarlakeJob]) -> None:
        """Manually register a job class by (orchestrator, execution_environment)."""

    @classmethod
    def create_job(
        cls,
        filename: str,
        module_name: str,
        orchestrator: Union[StarlakeOrchestrator, str],
        execution_environment: Union[StarlakeExecutionEnvironment, str],
        options: dict,
        **kwargs
    ) -> IStarlakeJob:
        """Create the correct IStarlakeJob instance."""
```

#### SessionFactory

Creates database sessions for SQL-based orchestration and testing.

```python
class SessionFactory:
    @classmethod
    def session(
        cls,
        provider: SessionProvider = SessionProvider.DUCKDB,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        **kwargs
    ) -> Session:
        """Create a new session based on the provider."""
```

Supported providers via `SessionProvider`: `DUCKDB`, `POSTGRES`, `MYSQL`, `REDSHIFT`, `SNOWFLAKE`, `BIGQUERY`.

## Enums Reference

| Enum | Values | Purpose |
|------|--------|---------|
| `StarlakeOrchestrator` | `AIRFLOW`, `COMPOSER`, `DAGSTER`, `SNOWFLAKE`, `STARLAKE` | Orchestrator identity (`COMPOSER` aliases `AIRFLOW`) |
| `StarlakeExecutionEnvironment` | `CLOUD_RUN`, `DATAPROC`, `FARGATE`, `SHELL`, `SQL` | Where tasks execute |
| `StarlakeExecutionMode` | `DRY_RUN`, `RUN`, `BACKFILL` | Pipeline execution mode |
| `TaskType` | `START`, `PRELOAD`, `STAGE`, `LOAD`, `TRANSFORM`, `EMPTY`, `END` | Task classification (`IMPORT` deprecated, use `STAGE`) |
| `StarlakePreLoadStrategy` | `NONE`, `IMPORTED`, `ACK`, `PENDING` | Pre-load behavior |
| `DatasetTriggeringStrategy` | `ALL`, `ANY` | When to trigger downstream pipelines |
| `StarlakeDatasetType` | `LOAD`, `TRANSFORM` | Dataset origin type |
| `StarlakeDependencyType` | `TASK`, `TABLE` | Dependency node type |

## CLI

The module includes a command-line interface for executing pipelines directly:

```bash
python -m ai.starlake.orchestration <action> --file <path> [--options <options>]
```

Supported actions: `run`, `dry-run`, `deploy`, `delete`, `backfill`.

| Argument | Description |
|----------|-------------|
| `action` | The action to perform on the pipeline |
| `--file` | Path to the generated DAG file (or directory containing `.py` files) |
| `--options` | Additional options as JSON or `key=value` pairs |

Example:

```bash
python -m ai.starlake.orchestration run --file /path/to/my_dag.py
python -m ai.starlake.orchestration dry-run --file /path/to/dags/ --options '{"key": "value"}'
python -m ai.starlake.orchestration backfill --file /path/to/my_dag.py --options 'start_date=2024-01-01,end_date=2024-01-31'
```

## How to Extend Starlake Orchestration

### 1. Define a Starlake Job

Implement the `IStarlakeJob` interface to create a **concrete factory class** responsible for defining orchestrator-specific tasks.

```python
from ai.starlake.job import IStarlakeJob, StarlakeOrchestrator, StarlakeExecutionEnvironment

class MyStarlakeJob(IStarlakeJob):
    @classmethod
    def sl_orchestrator(cls) -> str:
        return "my_orchestrator"

    @classmethod
    def sl_execution_environment(cls) -> str:
        return "shell"

    def sl_job(self, task_id, arguments, spark_config=None, dataset=None, task_type=None, **kwargs):
        return MyOrchestratorTask(task_id=task_id, command_arguments=arguments, **kwargs)

    def dummy_op(self, task_id, events=None, task_type=None, **kwargs):
        return MyDummyTask(task_id=task_id, **kwargs)

    def skip_or_start_op(self, task_id, upstream_task, **kwargs):
        return None  # or a conditional task

    def to_event(self, dataset, source=None, **kwargs):
        return MyEvent(dataset=dataset, source=source)
```

### 2. Implement the Orchestration API

Extend `AbstractOrchestration` to integrate the new orchestrator's API.

```python
from ai.starlake.orchestration import AbstractPipeline, AbstractTaskGroup, AbstractOrchestration

class MyPipeline(AbstractPipeline):
    def run(self, **kwargs):
        # Execute the pipeline using the orchestrator's API
        ...

class MyTaskGroup(AbstractTaskGroup):
    ...

class MyOrchestration(AbstractOrchestration):
    @classmethod
    def sl_orchestrator(cls) -> str:
        return "my_orchestrator"

    def sl_create_pipeline(self, schedule=None, dependencies=None, **kwargs):
        return MyPipeline(self.job, schedule=schedule, dependencies=dependencies, orchestration=self, **kwargs)

    def sl_create_task_group(self, group_id, pipeline, **kwargs):
        return MyTaskGroup(name=group_id, pipeline=pipeline, **kwargs)
```

### 3. Register (Optional)

Registration happens automatically via `importlib` package discovery when using the factories. For explicit registration:

```python
from ai.starlake.orchestration import OrchestrationFactory
from ai.starlake.job import StarlakeJobFactory

OrchestrationFactory.register_orchestration(MyOrchestration)
StarlakeJobFactory.register_job(MyStarlakeJob)
```

### 4. Create and Run a Pipeline

```python
from ai.starlake.job import StarlakeJobFactory, StarlakeExecutionEnvironment
from ai.starlake.orchestration import StarlakeSchedule, StarlakeDomain, StarlakeTable, OrchestrationFactory
from ai.starlake.common import sanitize_id

import os

schedule = StarlakeSchedule(
    name='daily',
    cron='0 0 * * *',
    domains=[
        StarlakeDomain(
            name='starbake',
            final_name='starbake',
            tables=[
                StarlakeTable(name='Customers', final_name='Customers'),
                StarlakeTable(name='Products', final_name='Products'),
            ]
        )
    ]
)

sl_job = StarlakeJobFactory.create_job(
    filename=os.path.basename(__file__),
    module_name=f"{__name__}",
    orchestrator="my_orchestrator",
    execution_environment=StarlakeExecutionEnvironment.SHELL,
    options={}
)

with OrchestrationFactory.create_orchestration(job=sl_job) as orchestration:

    with orchestration.sl_create_pipeline(schedule=schedule) as pipeline:

        start = pipeline.start_task()

        def generate_load_domain(domain: StarlakeDomain):
            with orchestration.sl_create_task_group(group_id=sanitize_id(domain.name), pipeline=pipeline) as ld:
                with orchestration.sl_create_task_group(group_id=sanitize_id(f'load_{domain.name}'), pipeline=pipeline) as load_tables:
                    for table in domain.tables:
                        pipeline.sl_load(
                            task_id=sanitize_id(f'load_{domain.name}_{table.name}'),
                            domain=domain.name,
                            table=table.name,
                        )
                return load_tables
            return ld

        load_domains = [generate_load_domain(domain) for domain in schedule.domains]

        start >> load_domains

        end = pipeline.end_task()
        end << load_domains
```

## Integration Modules

The core `starlake-orchestration` module is extended by orchestrator-specific integration modules:

| Module | Package | Description |
|--------|---------|-------------|
| **[starlake-airflow](https://pypi.org/project/starlake-airflow/)** | `pip install starlake-orchestration[airflow]` | Apache Airflow integration (v2 and v3) |
| **[starlake-dagster](https://pypi.org/project/starlake-dagster/)** | `pip install starlake-orchestration[dagster]` | Dagster integration |
| **[starlake-snowflake](https://pypi.org/project/starlake-snowflake/)** | `pip install starlake-orchestration[snowflake]` | Snowflake Tasks integration |
| **SQL/ODBC** | (included in core) | SQL-based orchestration via `ai.starlake.odbc` |

Each integration module implements `IStarlakeJob`, `AbstractOrchestration`, `AbstractPipeline`, and `AbstractTaskGroup` for its target platform. See the individual module READMEs for platform-specific documentation and examples.

## Architecture

For detailed architectural documentation including the inheritance hierarchy, design patterns (double factory, context stack, `@final` invariants), and class-by-class reference, see [ARCHITECTURE.md](https://github.com/starlake-ai/starlake-orchestration/blob/main/starlake-orchestration/ARCHITECTURE.md).
