# Starlake Orchestration

## What is Starlake?

Starlake is a **configuration-only** toolfor **Extract**, **Load**, **Transform** (**ELT**) and **orchestration** of declarative data pipelines. It simplifies data workflows with minimal coding requirements. Below is a typical use case for Starlake:

1. **Extract**: Gather data from sources such as Fixed Position files, DSV (Delimiter-Separated Values), JSON, or XML formats.
2. **Define or infer structure**: Use YAML to describe or infer the schema for each data source.
3. **Load**: Configure and execute the loading process to your data warehouse or other sinks.
4. **Transform**: Build aggregates and join datasets using SQL, Jinja, and YAML configurations.
5. **Output**: Observe your data becoming available as structured tables in your data warehouse.

Starlake can be used for **any or all** of these steps, providing flexibility to adapt to your workflow.

* **Extract** : Export selective data from SQL databases into CSV files.
* **Preload**: Evaluate whether the loading process should proceed, based on a configurable preload strategy.
* **Load** : Ingest FIXED-WIDTH, CSV, JSON, or XML files, converting them into strongly-typed records stored as Parquet files, data warehouse tables (e.g., Google BigQuery), or other configured sinks.
* **Transform** : Join loaded datasets and save them as Parquet files, data warehouse tables, or Elasticsearch indices.

## What is Starlake orchestration?

Starlake Orchestration is the **Python-based API** for managing and orchestrating data pipelines in Starlake. It offers a simple yet powerful interface for creating, scheduling, and running pipelines while abstracting the complexity of underlying orchestrators.

### Key Features

#### 1. **Supports Multiple Orchestrators**

Starlake Orchestration integrates with popular orchestration frameworks such as **Apache Airflow** and  **Dagster** , enabling you to choose the platform that best fits your needs.

#### 2. **Write Once, Run Anywhere**

The API provides a simple and intuitive way to define pipelines with minimal boilerplate code. Its extensibility ensures pipelines can be reused across different orchestrators.

In alignment with Starlake's philosophy, you can define your data pipelines once and execute them seamlessly across platforms, without rework.

#### 3. **Ensures Data Freshness**

Starlake Orchestration supports  **flexible scheduling mechanisms** , ensuring your data pipelines deliver up-to-date results:

* **Cron-based scheduling** : Define static schedules, such as "run every day at 2 AM."
* **Event-driven orchestration** : Trigger workloads dynamically based on events, utilizing **dataset-aware DAGs** to track data lineage.

By leveraging data lineage and dependencies, Starlake Orchestration aligns schedules automatically, ensuring the freshness of interconnected datasets.

#### 4. **Simplifies Data Pipeline Management**

With automated schedule alignment and dependency management, Starlake Orchestration eliminates manual adjustments and simplifies pipeline workflows, while maintaining reliability.

### StarlakeOptions

The StarlakeOptions class defines methods to retrieve variables within the context of the DAG (options passed to the DAG as a dictionary, environment variables, etc).

## What are the main components of Starlake Orchestration?

### IStarlakeJob

`ai.starlake.job.IStarlakeJob` is the **generic factory interface** for creating orchestration tasks. These tasks invoke the appropriate Starlake CLI commands.

Each Starlake command is represented by a factory method. The `sl_job` abstract method must be implemented in all concrete factory classes to handle the creation of specific orchestration tasks (e.g., Dagster `OpDefinition`, Airflow `BaseOperator`, etc.).

#### Abstract Method: `sl_job`

```python
def sl_job(
    self, 
    task_id: str, 
    arguments: list, 
    spark_config: StarlakeSparkConfig=None, 
    **kwargs) -> T:
    #...
```

| name         | type                | description                                           |
| ------------ | ------------------- | ----------------------------------------------------- |
| task_id      | str                 | the required task id                                  |
| arguments    | list                | The required arguments of the starlake command to run |
| spark_config | StarlakeSparkConfig | the optional `ai.starlake.job.StarlakeSparkConfig`  |

#### Factory Methods for Key Starlake Commands

These methods generate tasks for the core Starlake commands. Each method corresponds to a specific operation in the pipeline.

##### Preload

Will generate the task that will run the starlake [preload](https://starlake.ai/starlake/docs/cli/preload) command.

```python
def sl_pre_load(
    self, 
    domain: str, 
    tables: set=set(), 
    pre_load_strategy: Union[StarlakePreLoadStrategy, str, None] = None, 
    **kwargs) -> Optional[T]:
    #...
```

| name              | type | description                                                       |
| ----------------- | ---- | ----------------------------------------------------------------- |
| domain            | str  | the domain to preload                                             |
| tables            | set  | the optional tables to preload                                    |
| pre_load_strategy | str  | the optional preload strategy (self.pre_load_strategy by default) |

###### StarlakePreLoadStrategy

`ai.starlake.job.StarlakePreLoadStrategy` is an enumeration defining preload strategies for conditional domain loading.

Strategies:

1. **NONE** No condition applied; preload tasks are skipped.![none strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/none.png)
2. **IMPORTED** Load only if files exist in the landing area (SL_ROOT/datasets/importing/{domain}).![imported strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/imported.png)
3. **PENDING** Load only if files exist in the pending datasets area (SL_ROOT/datasets/pending/{domain}).![pending strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/pending.png)
4. **ACK** Load only if an acknowledgment file exists at the configured path (global_ack_file_path).![ack strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/ack.png)

##### Import

Generates the task for the [import](https://starlake.ai/starlake/docs/cli/import) command.

```python
def sl_import(
    self, 
    task_id: str, 
    domain: str, 
    tables: set=set(),
    **kwargs) -> T:
    #...
```

| name    | type | description                                           |
| ------- | ---- | ----------------------------------------------------- |
| task_id | str  | the optional task id (`{domain}_import` by default) |
| domain  | str  | the required domain to import                         |
| tables  | set  | the optional tables to import                         |

##### Load

Generates the task for the [load](https://starlake.ai/starlake/docs/cli/load) command.

```python
def sl_load(
    self, 
    task_id: str, 
    domain: str, 
    table: str, 
    spark_config: StarlakeSparkConfig=None,
    **kwargs) -> BaseOperator:
    #...
```

| name         | type                | description                                                 |
| ------------ | ------------------- | ----------------------------------------------------------- |
| task_id      | str                 | the optional task id (`{domain}_{table}_load` by default) |
| domain       | str                 | the required domain of the table to load                    |
| table        | str                 | the required table to load                                  |
| spark_config | StarlakeSparkConfig | the optional `ai.starlake.job.StarlakeSparkConfig`        |

##### Transform

Generates the task for the [transform](https://starlake.ai/starlake/docs/cli/transform) command.

```python
def sl_transform(
    self, 
    task_id: str, 
    transform_name: str, 
    transform_options: str=None, 
    spark_config: StarlakeSparkConfig=None, **kwargs) -> BaseOperator:
    #...
```

| name              | type                | description                                            |
| ----------------- | ------------------- | ------------------------------------------------------ |
| task_id           | str                 | the optional task id (`{transform_name}` by default) |
| transform_name    | str                 | the transform to run                                   |
| transform_options | str                 | the optional transform options                         |
| spark_config      | StarlakeSparkConfig | the optional `ai.starlake.job.StarlakeSparkConfig`   |

### StarlakeDataset

When a task is added to the orchestration pipeline, a corresponding `ai.starlake.dataset.StarlakeDataset` object is created. This object encapsulates metadata about the dataset produced by the task.

These dataset objects are collected into a list of events. Each event will be triggered by the orchestrator **only if the corresponding task is completed successfully** during the pipeline's execution.

StarlakeDependencies defines all the dependencies between the tasks that will generate a pipeline.
StarlakeSchedules
AbstractDependency
AbstractTask - the abstract class for creating unit task.
IStarlakeJob - the interface for creating unit task jobs related to the load and transform of data.
