# **Orchestrate Declaratively with Starlake AI: Simplifying Data Workflows**

In the age of modern data stacks, the bottleneck is no longer access to tools but managing the growing complexity of data pipelines. From ingestion to transformation to orchestration, teams are burdened with stitching together disparate technologies and writing boilerplate code. **Starlake AI** addresses this challenge by offering a **declarative data stack** that removes friction from data ingestion and transformation — but it goes a step further. It also **streamlines orchestration**, the often overlooked but critical component of scalable data operations.

**Starlake AI radically simplifies orchestration**, helping data teams move faster and focus on what really matters: delivering reliable data products.

## **Declarative Orchestration: From Configuration to Execution**

At the heart of Starlake’s orchestration solution lies a simple idea: **define workflows declaratively**, and let the system take care of the rest.

Instead of writing imperative DAGs with fragile dependency chains, users define in simple YAML configurations **datasets, their transformations, schedulings and DAG generation configuration** which includes:

* The **template** to use (Airflow, Dagster, Snowflake Tasks, etc.)
* the **relative path** to the DAG(s) that will be generated
* DAG-specific settings like:

  * `start_date` - the start date of the DAG
  * `retries` - the number of retries to attempt before failing the task
  * `retry_delay` - the delay between retries in seconds
  * `catchup` - whether to catch up on missed runs

![dag configuration](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/scheduled_tasks_dag_configuration.png)

Starlake reads these definitions and infers **execution order, dependencies, and orchestration logic**, generating production-ready DAGs without a single line of orchestration code.

![dag generation](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/airflow_dag_generated.png)

This declarative model shifts orchestration from code to config — and from ad hoc to **repeatable and governed**.

This approach accelerates onboarding and eliminates the risk of introducing bugs through custom orchestration scripts.

## **Built-in Dependency Management via Data Lineage**

One of the standout features of Starlake AI is its **automatic dependency management**.

Starlake parses your transformation (SQL) and builds a **lineage graph** of dataset dependencies.
This graph is then used to:

* **Determine task execution order** automatically while avoiding cycles or race conditions
* **Define the required datasets that will trigger the non scheduled DAGs**

![sql](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/starbake_customer_purchase_history_sql.png)

![lineage graph](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/starbake_customer_purchase_history_lineage.png)

No need to define upstream/downstream relationships manually — **Starlake infers them from the logic you've already written**.

![handling dependencies](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/airflow_starbake_analytics_graph.png)

## **Pluggable Orchestration: Use the Tools You Already Know**

Whether your team is using **Apache Airflow, Google Cloud Composer, Dagster, or Snowflake Tasks**, Starlake has you covered.

* For **Airflow**, it generates native Python DAGs with task dependencies derived from your dataset lineage.
* On **Dagster**, it generates jobs and graphs that follow your project structure and allow rich observability.
* With **Snowflake**, it produces orchestration DAGs using native **Snowflake Tasks and Streams** — no external scheduler needed.

![orchestrators](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/starlake_orchestrators.png)

You retain control over how and where your workflows run, while benefitting from **automatic DAG generation** that is infrastructure-agnostic.

## **Customizable Templates: Declarative, Yet Flexible**

Starlake comes with a rich set of **predefined orchestration templates**. These can be used as-is or extended with your own logic.

You can easily override or extend the default templates. This ensures you don’t sacrifice **flexibility for simplicity** — you get both.

## **Event-Driven Workflows: React to Your Data**

In a modern data ecosystem, batch schedules aren't enough. That’s why **Starlake also supports event-driven orchestration out of the box** by publishing events based on dataset changes.

![publish events](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/airflow_starbake_publish_events.png)

This means that DAGs can be triggered not just by time schedules, but by the **availability of data**.

![consume events](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/airflow_starbake_analytics_consuming_hourly.png)

This allows for **asynchronous, reactive pipelines** that automatically respond to data availability — no need to guess fixed execution times.

## **Summary: Declarative, Composable, and Intelligent Orchestration**

| Feature                               | Benefit                                                  |
| ------------------------------------- | -------------------------------------------------------- |
| Per-task scheduling and orchestration | Fine-grained control over workflow execution             |
| Templates for DAG generation          | Avoid boilerplate, ensure consistency                    |
| Lineage-based dependency inference    | Automatically orchestrate based on actual data logic     |
| Event-driven task triggering          | More reactive pipelines, less idle time                  |
| Multi-orchestrator support            | Plug into Airflow, Dagster, or Snowflake with one config |

## **Conclusion: Orchestration Without the Overhead**

With Starlake AI, orchestration is no longer a burden. By combining **declarative definitions**, **automatic dependency inference**, and **plug-and-play support** for leading orchestrators, it helps data teams:

* **Ship faster** with fewer errors
* **Maintain less orchestration code**
* **Respond to change** with confidence

If you're building or maintaining data pipelines and feel like orchestration is slowing you down, it's time to try a smarter approach.

**Let Starlake AI orchestrate your data — so you can focus on your insights.**
