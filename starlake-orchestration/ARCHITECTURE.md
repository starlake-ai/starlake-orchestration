# Starlake Orchestration — Architecture Reference

This document describes the internal architecture of the `starlake-orchestration` core module. It is intended for contributors, AI agents, and anyone extending the framework with new orchestrators or execution environments.

For usage documentation, see [README.md](README.md).

## Design Philosophy

The core module is an **orchestrator-agnostic abstraction layer**. It defines contracts (abstract classes + generics) that orchestrator modules (Airflow, Dagster, Snowflake) implement. Four TypeVars flow through the entire hierarchy:

| TypeVar | Represents | Example |
|---------|-----------|---------|
| `U` | DAG/pipeline native type | Airflow `DAG`, Dagster `JobDefinition` |
| `T` | Task native type | Airflow `BaseOperator`, Dagster `OpDefinition` |
| `GT` | Task group native type | Airflow `TaskGroup`, Dagster `GraphDefinition` |
| `E` | Event native type | Airflow `Dataset`, Dagster `AssetKey` |

## Package Structure

```
ai.starlake/
├── orchestration/     Pipeline lifecycle, dependency graph, task grouping, factories
│   ├── starlake_orchestration.py    AbstractOrchestration, AbstractPipeline, AbstractTask,
│   │                                AbstractTaskGroup, AbstractDependency, TaskGroupContext,
│   │                                OrchestrationFactory
│   ├── starlake_dependencies.py     DependencyMixin, TreeNodeMixin, StarlakeDependency,
│   │                                StarlakeDependencies, StarlakeDependencyType
│   └── starlake_schedules.py        StarlakeSchedule, StarlakeSchedules, StarlakeDomain,
│                                    StarlakeTable
├── job/               Job execution, CLI invocation, pre-load strategies, Spark config
│   ├── starlake_job.py              IStarlakeJob, StarlakeJobFactory, StarlakeOrchestrator,
│   │                                StarlakeExecutionEnvironment, StarlakeExecutionMode, TaskType
│   ├── starlake_options.py          StarlakeOptions
│   ├── starlake_pre_load_strategy.py StarlakePreLoadStrategy
│   └── spark_config.py             StarlakeSparkConfig, StarlakeSparkExecutorConfig
├── dataset/           Dataset identity, event abstraction, triggering strategies
│   └── starlake_dataset.py          StarlakeDataset, StarlakeDatasetType,
│                                    DatasetTriggeringStrategy, AbstractEvent
└── common/            Utility functions, enums, cron helpers
    └── __init__.py                  StarlakeCronPeriod, StarlakeParameters,
                                     MissingEnvironmentVariable, sanitize_id, is_valid_cron,
                                     sort_crons_by_frequency, sl_schedule, ...
```

## Inheritance Hierarchy

```
DependencyMixin                         # Base graph node: id, upstreams, downstreams, >> / <<
├── StarlakeDependency                  # Concrete: name, cron, sink, dependency_type
└── AbstractDependency (ABC)            # Adds TaskGroupContext registration on >> / <<
    ├── AbstractTask[T]                 # Wraps native task (T), auto-registers with context
    └── TaskGroupContext                # Context manager with static _context_stack
        ├── AbstractTaskGroup[GT]       # Extends with native group (GT), print_group()
        │   └── AbstractPipeline[U,T,GT,E]  # Central orchestration: schedule/deps, @final methods
        └── (manages nested with-blocks)

TreeNodeMixin                           # Tree traversal wrapper for DependencyMixin

StarlakeOptions                         # Mixin: get_context_var(), get_sl_env_vars()
AbstractEvent[E]                        # Abstract: to_event() factory method
  └── IStarlakeJob[T,E]                # Job factory: sl_job(), sl_load(), sl_transform(), ...
      (also extends StarlakeOptions, AbstractEvent[E])

AbstractOrchestration[U,T,GT,E]         # Top-level container: creates pipelines, task groups
```

## Class-by-Class Reference

### Foundation Layer — Graph & Dependencies

#### `DependencyMixin`

The root building block for all dependency-aware nodes. Provides:

- `id` — unique identifier
- `upstreams` / `downstreams` — sets of connected nodes
- `>>` (`__rshift__`) / `<<` (`__lshift__`) — operators for chaining dependencies
- `node` — returns a `TreeNodeMixin` wrapper for tree traversal
- `all_dependencies` — flattened list of all reachable nodes

Every node in the pipeline graph inherits this mixin.

#### `TreeNodeMixin`

A tree traversal wrapper around `DependencyMixin`. Converts the flat bidirectional dependency graph into a parent/children tree structure for ordered walking. Used by `StarlakeDependencies.graphs()` to produce traversable trees.

#### `StarlakeDependency` (extends `DependencyMixin`)

A concrete dependency node representing a Starlake task (load or transform) with:

- `name` — task name
- `dependency_type` — `StarlakeDependencyType.TASK` or `TABLE`
- `cron` — optional cron expression
- `sink` — `domain.table` identifier
- `dependencies` — child dependencies (recursively linked via `<<`)
- `to_dataset()` — converts to a `StarlakeDataset` for event-driven scheduling

#### `StarlakeDependencies`

A collection that parses JSON dependency graphs (produced by `starlake dag-generate`) into `StarlakeDependency` trees. Key capabilities:

- `graphs()` — returns traversable `TreeNodeMixin` trees
- `get_schedule()` — computes scheduling from dependencies
- `retrieve_datasets()` — extracts datasets for event-driven triggering
- Iterable and indexable (supports `for dep in dependencies`, `dependencies[0]`, `len(dependencies)`)

**Important naming convention:** `upstream_dependencies[A] = [B]` means B is **downstream** of A (B runs after A). This is counter-intuitive — the map tracks "what follows A", not "what A depends on".

#### `StarlakeDependencyType` (Enum)

- `TASK` — a task dependency (transform)
- `TABLE` — a table dependency (load)

### Schedule Layer

#### `StarlakeTable` / `StarlakeDomain` / `StarlakeSchedule` / `StarlakeSchedules`

Simple data classes representing the schedule-based view of Starlake metadata:

- `StarlakeTable` — a table with `name` and `final_name`
- `StarlakeDomain` — a domain containing a list of tables
- `StarlakeSchedule` — groups domains under a `cron` expression with a `name` (e.g., "hourly", "daily")
- `StarlakeSchedules` — iterable collection of schedules

Used when pipelines are driven by **time** rather than **data dependencies**.

### Dataset & Event Layer

#### `StarlakeDataset`

Represents a dataset identity with:

- `name` / `uri` — identifier (sanitized for orchestrator compatibility)
- `cron` — optional cron expression
- `sink` — `domain.table` reference
- `domain` / `table` — computed from sink or name
- `url` — full URL with query parameters
- `parameters` — scheduling parameters (e.g., `sl_scheduled_date`)
- `datasetType` — `LOAD` or `TRANSFORM`
- `refresh()` — creates a time-aware copy for backfill scenarios

#### `StarlakeDatasetType` (Enum)

- `LOAD` — dataset produced by a load task
- `TRANSFORM` — dataset produced by a transform task

#### `DatasetTriggeringStrategy` (Enum)

- `ALL` — wait for all upstream datasets before triggering
- `ANY` — trigger on first available upstream dataset

#### `AbstractEvent[E]` (Generic, Abstract)

Abstract factory method that orchestrator modules implement:

- `to_event(dataset, source)` — converts a `StarlakeDataset` into an orchestrator-native event (e.g., Airflow `Dataset`, Dagster asset)

### Job Layer — Execution Engine Interface

#### `StarlakeOptions` (Mixin)

Configuration resolution mixin with resolution order: **options dict → default_value → environment variable**.

- `get_context_var(var_name, default_value, options)` — resolves a configuration variable
- `get_sl_env_vars(options)` — parses JSON from `sl_env_var` option
- `get_sl_root(options)` — returns `SL_ROOT`
- `get_sl_datasets(options)` — returns `SL_DATASETS`

#### `IStarlakeJob[T, E]` (Generic, extends `StarlakeOptions`, `AbstractEvent[E]`)

The abstract factory for creating orchestrator-native tasks. This is where Starlake CLI commands become tasks:

**Core task methods** (concrete, call `sl_job()` internally):

- `sl_load(task_id, domain, table, ...)` → creates a LOAD task
- `sl_transform(task_id, transform_name, ...)` → creates a TRANSFORM task
- `sl_import(task_id, domain, tables, ...)` → creates a STAGE task
- `sl_pre_load(domain, tables, pre_load_strategy, ...)` → creates a PRELOAD task (handles ACK, IMPORTED, PENDING strategies)
- `start_op(task_id, ...)` / `end_op(task_id, events, ...)` → pipeline bookend tasks
- `pre_tasks()` / `post_tasks()` → optional pre/post pipeline tasks (default: no-op)

**Abstract methods** (must be implemented by each orchestrator × execution environment):

- `sl_job(task_id, arguments, spark_config, dataset, task_type, ...)` → creates the actual CLI-invoking task **specific to the execution environment** (Cloud Run, Dataproc, Fargate, Shell, SQL, etc.)
- `dummy_op(task_id, events, task_type, ...)` → creates a no-op task
- `skip_or_start_op(task_id, upstream_task, ...)` → creates a conditional task

**Classification methods** (must be implemented by each orchestrator):

- `sl_orchestrator()` → returns the orchestrator type (e.g., `StarlakeOrchestrator.AIRFLOW`)
- `sl_execution_environment()` → returns the execution environment (e.g., `StarlakeExecutionEnvironment.SHELL`)

**Resolution chain:**

```
StarlakeJobFactory resolves by (orchestrator x execution_environment)
  → IStarlakeJob subclass (e.g., StarlakeAirflowShellJob)
    → sl_job() builds the CLI invocation specific to that execution environment
      → wrapped in an orchestrator-native task type (T)
```

#### `StarlakeJobFactory`

Registry-based factory:

- `register_jobs_from_package(package_name)` — discovers all `IStarlakeJob` subclasses via `importlib`
- `register_job(job_class)` — registers by `(orchestrator, execution_environment)`
- `create_job(filename, module_name, orchestrator, execution_environment, options)` — instantiates the correct job class

#### `StarlakePreLoadStrategy` (Enum)

Controls what happens before data loading:

- `NONE` — no pre-load, skip directly to load
- `IMPORTED` — check if files exist in landing area, then `sl_import` → `sl_load`
- `PENDING` — check if files exist in pending area
- `ACK` — wait for an acknowledgment file

**IMPORTED chain:** `sl_pre_load` >> `skip_or_start` >> `sl_import` >> `sl_load`

#### `StarlakeSparkConfig` / `StarlakeSparkExecutorConfig`

Spark executor configuration (memory, cores, instances) plus arbitrary spark properties passed as kwargs.

### Orchestration Layer — Pipeline Lifecycle

#### `AbstractDependency` (extends `DependencyMixin`, ABC)

Extends `DependencyMixin` with **context-awareness**: the `>>` and `<<` operators not only link nodes but also **register the dependency in the current `TaskGroupContext`** via `ctx.set_dependency()`.

#### `AbstractTask[T]` (extends `AbstractDependency`)

Wraps an orchestrator-native task (`T`) with a `task_id`. **Auto-registers** with the current `TaskGroupContext` on creation.

#### `TaskGroupContext` (extends `AbstractDependency`)

The **context manager** that tracks nested task groups via a static `_context_stack`. This is the mechanism that makes `with pipeline:` and `with task_group:` work.

Key state:

- `dependencies` — all tasks/groups registered within this context
- `dependencies_dict` — lookup by ID
- `upstream_dependencies` / `downstream_dependencies` — the dependency graph
- `roots` — entry points (nodes with no upstream within the group)
- `leaves` — exit points (nodes with no downstream within the group)

Class-level state:

- `_context_stack` — static list enabling nested `with` blocks

Methods:

- `set_dependency(upstream, downstream)` — registers a bidirectional dependency; converts non-`AbstractDependency` objects via `orchestration_cls.from_native()`
- `add_dependency(dependency)` — `@final`, adds to the dependency list (raises if ID already exists)
- `get_dependency(id)` — looks up by ID

#### `AbstractTaskGroup[GT]` (extends `TaskGroupContext`)

Extends `TaskGroupContext` with a native group reference (`GT`). Supports recursive `print_group()` for tree visualization.

#### `AbstractPipeline[U, T, GT, E]` (extends `AbstractTaskGroup[U]`, `AbstractEvent[E]`)

The **central orchestration class** where everything comes together.

**Construction:** Takes a `job` (the `IStarlakeJob` instance) plus either a `schedule` (time-driven) or `dependencies` (data-driven), but not both. Wires up cron expressions, datasets, and dependency graphs.

**Context manager `__exit__`:** When the `with pipeline:` block exits:

1. Registers the pipeline with its parent `AbstractOrchestration`
2. Wires native dependencies via the `add_dag_dependency` callback
3. Walks the dependency tree to build ordered task and task name lists
4. Prints the task sequence for debugging

**`@final` methods** (cannot be overridden by subclasses):

- `sl_load()`, `sl_transform()`, `sl_import()`, `sl_pre_load()` — create tasks and register dataset assets
- `start_task()`, `end_task()`, `pre_tasks()`, `post_tasks()` — pipeline lifecycle bookends
- `dummy_task()` — creates a no-op task
- `dry_run()` — calls `run(mode=DRY_RUN)`
- `print_pipeline()` — displays the task tree
- `pipeline_id`, `schedule`, `cron`, `datasets`, `events`, `job`, ... — read-only properties

**Abstract method:** Only `run()` must be implemented by subclasses — this is the single extension point for pipeline execution.

**Concrete optional overrides:**

- `deploy()` — default no-op (override for deployment logic)
- `delete()` — default no-op (override for cleanup logic)
- `backfill(timeout, start_date, end_date)` — concrete implementation that validates cron/dates and loops via `croniter`, calling `run()` for each interval

**Scheduling intelligence:**

- `set_cron_expr(datasets)` — analyzes cron frequencies via `sort_crons_by_frequency()`
- `least_frequent_datasets` / `most_frequent_datasets` — separates datasets by frequency for multi-frequency pipelines
- `scheduled_datasets` — filters to datasets with cron expressions
- `not_scheduled_datasets` — datasets without cron (event-driven only)

#### `AbstractOrchestration[U, T, GT, E]`

The top-level container. A context manager (`with orchestration:`) that holds a list of pipelines.

**Abstract methods** (must be implemented per orchestrator):

- `sl_create_pipeline(schedule, dependencies, ...)` → factory for creating `AbstractPipeline` subclasses
- `sl_create_task_group(group_id, pipeline, ...)` → factory for creating `AbstractTaskGroup` subclasses

**Concrete methods:**

- `sl_create_task(task_id, task, pipeline)` → wraps a native task in `AbstractTask` (default implementation; can be overridden)
- `from_native(native)` → converts orchestrator-native tasks to `AbstractTask` (default returns `None`; subclasses override for native task/group conversion)
- `sl_orchestrator()` → returns orchestrator name (subclasses override)

#### `OrchestrationFactory`

Registry that discovers `AbstractOrchestration` subclasses via `importlib` and instantiates the correct one based on `job.sl_orchestrator()`.

- `register_orchestrations_from_package(package_name)` — auto-discovery
- `register_orchestration(orchestration_class)` — manual registration
- `create_orchestration(job)` — factory method (lazy-initializes the registry on first call)

### Enums & Constants

| Enum | Values | Purpose |
|------|--------|---------|
| `StarlakeOrchestrator` | `AIRFLOW`, `COMPOSER`, `DAGSTER`, `SNOWFLAKE`, `STARLAKE` | Orchestrator identity (`COMPOSER` aliases `AIRFLOW`) |
| `StarlakeExecutionEnvironment` | `CLOUD_RUN`, `DATAPROC`, `FARGATE`, `SHELL`, `SQL` | Where tasks execute |
| `StarlakeExecutionMode` | `DRY_RUN`, `RUN`, `BACKFILL` | Pipeline execution mode |
| `TaskType` | `START`, `PRELOAD`, `IMPORT`/`STAGE`, `LOAD`, `TRANSFORM`, `EMPTY`, `END` | Task classification (`IMPORT` deprecated, use `STAGE`) |
| `StarlakeCronPeriod` | `DAY`, `WEEK`, `MONTH`, `YEAR` | Cron frequency bucketing |
| `StarlakeParameters` | `SCHEDULED_DATE_PARAMETER`, `URI_PARAMETER`, `SINK_PARAMETER`, `CRON_PARAMETER`, `FRESHNESS_PARAMETER`, `DATA_INTERVAL_START_PARAMETER`, `DATA_INTERVAL_END_PARAMETER`, `DRY_RUN_PARAMETER` | Standard `sl_`-prefixed parameter names |
| `StarlakeDependencyType` | `TASK`, `TABLE` | Dependency node type |
| `StarlakeDatasetType` | `LOAD`, `TRANSFORM` | Dataset origin type |
| `DatasetTriggeringStrategy` | `ALL`, `ANY` | When to trigger downstream |
| `StarlakePreLoadStrategy` | `NONE`, `IMPORTED`, `ACK`, `PENDING` | Pre-load behavior |

### Utility Functions (`ai.starlake.common`)

| Function | Purpose |
|----------|---------|
| `sanitize_id(id)` | ASCII-only, replaces special chars with underscores |
| `keep_ascii_only(text)` | Strips non-ASCII characters |
| `is_valid_cron(cron_expr)` | Validates cron expression via croniter |
| `get_cron_frequency(cron_expr)` | Returns `timedelta` between consecutive executions |
| `sort_crons_by_frequency(cron_expressions)` | Groups and sorts crons by frequency |
| `most_frequent_crons(all_crons)` | Returns crons with smallest interval |
| `sl_schedule(cron, start_time, format)` | Returns previous cron tick formatted |
| `sl_cron_start_end_dates(cron_expr, start_time)` | Returns `sl_data_interval_start` and `sl_data_interval_end` |
| `sl_scheduled_date(cron, ts)` | Returns end of cron interval for timestamp |
| `sl_scheduled_dataset(dataset, cron, ts)` | Returns dataset URI with schedule parameter |
| `scheduled_dates_range(cron, scheduled_date)` | Returns `(start, end)` of cron interval |
| `cron_start_time(timezone)` | Returns current time in timezone |
| `asQueryParameters(parameters)` | Converts dict to URL query string |

## Key Architectural Patterns

### 1. Double Factory

Two independent factory registries enable the `(orchestrator x execution_environment)` matrix:

- `StarlakeJobFactory` resolves by `(orchestrator, execution_environment)` → correct `IStarlakeJob` subclass
- `OrchestrationFactory` resolves by `orchestrator` → correct `AbstractOrchestration` subclass

Both use `importlib` for auto-discovery of subclasses across installed packages.

### 2. Context Stack

`TaskGroupContext._context_stack` is a static list that enables nested `with` blocks:

```python
with orchestration.sl_create_pipeline(...) as pipeline:       # pushes pipeline
    with orchestration.sl_create_task_group(...) as group:     # pushes group
        pipeline.sl_load(...)                                  # registers in group
    # group pops, tasks wire to group's roots/leaves
# pipeline pops, registers with orchestration
```

Tasks auto-register with the current (top-of-stack) context on creation.

### 3. `@final` for Invariants

Critical pipeline methods are locked with `@final` to prevent subclasses from breaking the contract:

- `sl_load()`, `sl_transform()`, `sl_import()`, `sl_pre_load()` — ensure consistent task creation and asset registration
- `dry_run()` — always delegates to `run(mode=DRY_RUN)`
- `add_dependency()`, `get_dependency()` — protect the dependency graph

Only `run()` is abstract — this is the **single extension point** for pipeline execution.

### 4. Operator Overloading is Load-Bearing

`>>` and `<<` are not syntactic sugar. In `AbstractDependency`, they:

1. Link the nodes (upstream/downstream sets via `DependencyMixin`)
2. Register the dependency in the current `TaskGroupContext` via `ctx.set_dependency()`

Replacing `>>` with method calls would break context registration.

### 5. Schedule vs Dependencies — Two Pipeline Construction Paths

Pipelines are constructed with either:

- **`StarlakeSchedule`** — time-driven: cron expression + list of domains/tables
- **`StarlakeDependencies`** — data-driven: parsed from `starlake dag-generate` JSON, with dependency graphs and dataset events

Both cannot be used simultaneously. The constructor raises `ValueError` if neither is provided.

### 6. Execution Environment Abstraction

The `sl_job()` abstract method is where each execution environment defines **how** to invoke the Starlake CLI:

| Environment | How `sl_job()` invokes CLI |
|-------------|---------------------------|
| `SHELL` | Shell command / `BashOperator` |
| `CLOUD_RUN` | GCP Cloud Run API call |
| `DATAPROC` | GCP Dataproc job submission |
| `FARGATE` | AWS Fargate task launch |
| `SQL` | SQL stored procedure / Snowflake Task |

The orchestrator module provides the task wrapper (Airflow operator, Dagster op, Snowflake Task), while `sl_job()` determines the execution mechanics.
