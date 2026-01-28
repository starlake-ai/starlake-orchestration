# starlake-snowflake

**starlake-snowflake** is the **[Starlake](https://starlake.ai)** Python Distribution for **Snowflake**.

It is recommended to use it in combinaison with **[starlake dag generation](https://docs.starlake.ai/guides/orchestrate/customization)**, but can be used directly as is in your **Snowflake Tasks**.

## Prerequisites

Before installing starlake-snowflake, ensure the following minimum versions are installed on your system:

- starlake: 1.3.1 or higher
- python: 3.8 or higher
- Snowflake account

## Installation

```bash
pip install starlake-orchestration[snowflake] --upgrade
```

## StarlakeSnowflakeJob

`ai.starlake.snowflake.StarlakeSnowflakeJob` is an **abstract factory class** that extends the generic factory interface `ai.starlake.job.IStarlakeJob` and is responsible for **generating** the **Snowflake tasks** that will run the [import](https://docs.starlake.ai/cli/import), [load](https://docs.starlake.ai/category/load) and [transform](https://docs.starlake.ai/category/transform) starlake commands.

### Init

To initialize this class, you may specify the optional **pre load strategy** and **options** to use.

```python
    def __init__(self, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None], options: dict=None, **kwargs) -> None:
        """Overrides IStarlakeJob.__init__()
        Args:
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The pre-load strategy to use.
            options (dict): The options to use.
        """
        super().__init__(pre_load_strategy, options, **kwargs)
        #...
```

#### Options

The following options can be specified:

| name                            | type | description                                                                  |
| ------------------------------- | ---- | ---------------------------------------------------------------------------- |
| **stage_location**              | str  | the required stage location to use for the stored procedures                 |
| **warehouse**                   | str  | the optional warehouse to use                                                |
| **packages**                    | str  | the optional list of packages to use (`croniter,python-dateutil` by default) |
| **sl_incoming_file_stage**      | str  | the optional stage where incoming files are located                          |
| **allow_overlapping_execution** | bool | whether to allow overlapping execution of the tasks (`False` by default)     |

### sl_load

It generates the Snowflake task that will run the starlake [load](https://docs.starlake.ai/cli/load) command.

```python
def sl_load(
    self,
    task_id: str,
    domain: str,
    table: str,
    spark_config: StarlakeSparkConfig=None,
    **kwargs) -> DAGTask:
    #...
```

### sl_transform

It generates the Snowflake task that will run the starlake [transform](https://docs.starlake.ai/cli/transform) command.

```python
def sl_transform(
    self,
    task_id: str,
    transform_name: str,
    transform_options: str=None,
    spark_config: StarlakeSparkConfig=None,
    **kwargs) -> DAGTask:
    #...
```
