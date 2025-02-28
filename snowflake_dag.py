from ai.starlake.job import StarlakeOrchestrator
orchestrator = StarlakeOrchestrator.SNOWFLAKE

from ai.starlake.job import StarlakeExecutionEnvironment
execution_environment = StarlakeExecutionEnvironment.SQL

description="""sample dag configuration"""

template="transform/snowflake__scheduled_task__sql.py.j2"

access_control = None

options={
    'sl_env_var':'{"SL_ROOT": ".", "SL_ENV": "SNOWFLAKE"}', 
    'tags':'starlake', 
    'load_dependencies':'false', 
    'retries':'1',
    'retry_delay':'30',
    'stage_location':'staging',
    'schema': 'starbake',
    'warehouse':'COMPUTE_WH',
}

from ai.starlake.job import StarlakeJobFactory

import os

import sys

sl_job = StarlakeJobFactory.create_job(
    filename=os.path.basename(__file__), 
    module_name=f"{__name__}", 
    orchestrator=orchestrator,
    execution_environment=execution_environment,
    options=dict(options, **sys.modules[__name__].__dict__.get('jobs', {}))
    #optional variable jobs as a dict of all options to apply by job
    #eg jobs = {"task1 domain.task1 name": {"options": "task1 transform options"}, "task2 domain.task2 name": {"options": "task2 transform options"}}
)

cron = "None"

from ai.starlake.orchestration import StarlakeDependencies, StarlakeDependency, StarlakeDependencyType, OrchestrationFactory, AbstractTaskGroup, AbstractTask

from typing import List, Optional, Set, Union

dependencies=StarlakeDependencies(dependencies="""[ {
  "data" : {
    "name" : "kpi.order_items_analysis0",
    "typ" : "task",
    "parent" : "starbake.order_line",
    "parentTyp" : "table",
    "parentRef" : "starbake.order_line",
    "writeStrategy" : {
      "type" : "OVERWRITE"
    },
    "sink" : "kpi.order_items_analysis",
    "cron" : "None"
  },
  "children" : [ {
    "data" : {
      "name" : "starbake.product",
      "typ" : "table",
      "parentTyp" : "unknown",
      "writeStrategy" : {
        "type" : "OVERWRITE"
      },
      "cron" : "0 0 * * *"
    },
    "task" : false
  }, {
    "data" : {
      "name" : "starbake.order_line",
      "typ" : "table",
      "parentTyp" : "unknown",
      "writeStrategy" : {
        "type" : "APPEND"
      },
      "cron" : "0 * * * *",
      "stream" : ""
    },
    "task" : false
  } ],
  "task" : true
}, {
  "data" : {
    "name" : "kpi.order_summary",
    "typ" : "task",
    "parent" : "kpi.product_summary",
    "parentTyp" : "task",
    "parentRef" : "kpi.product_summary",
    "writeStrategy" : {
      "type" : "OVERWRITE"
    },
    "sink" : "kpi.order_summary",
    "cron" : "None"
  },
  "children" : [ {
    "data" : {
      "name" : "kpi.revenue_summary",
      "typ" : "task",
      "parent" : "starbake.order",
      "parentTyp" : "table",
      "parentRef" : "starbake.order",
      "writeStrategy" : {
        "type" : "OVERWRITE"
      },
      "sink" : "kpi.revenue_summary",
      "cron" : "None"
    },
    "children" : [ {
      "data" : {
        "name" : "starbake.order",
        "typ" : "table",
        "parentTyp" : "unknown",
        "writeStrategy" : {
          "type" : "APPEND"
        },
        "cron" : "0 0 * * *"
      },
      "task" : false
    }, {
      "data" : {
        "name" : "starbake.order_line",
        "typ" : "table",
        "parentTyp" : "unknown",
        "writeStrategy" : {
          "type" : "APPEND"
        },
        "cron" : "0 * * * *",
        "stream" : ""
      },
      "task" : false
    } ],
    "task" : true
  }, {
    "data" : {
      "name" : "kpi.product_summary",
      "typ" : "task",
      "parent" : "starbake.product",
      "parentTyp" : "table",
      "parentRef" : "starbake.product",
      "writeStrategy" : {
        "type" : "OVERWRITE"
      },
      "sink" : "kpi.product_summary",
      "cron" : "None"
    },
    "children" : [ {
      "data" : {
        "name" : "starbake.order",
        "typ" : "table",
        "parentTyp" : "unknown",
        "writeStrategy" : {
          "type" : "APPEND"
        },
        "cron" : "0 0 * * *"
      },
      "task" : false
    }, {
      "data" : {
        "name" : "starbake.order_line",
        "typ" : "table",
        "parentTyp" : "unknown",
        "writeStrategy" : {
          "type" : "APPEND"
        },
        "cron" : "0 * * * *",
        "stream" : ""
      },
      "task" : false
    }, {
      "data" : {
        "name" : "starbake.product",
        "typ" : "table",
        "parentTyp" : "unknown",
        "writeStrategy" : {
          "type" : "OVERWRITE"
        },
        "cron" : "0 0 * * *"
      },
      "task" : false
    } ],
    "task" : true
  } ],
  "task" : true
}, {
  "data" : {
    "name" : "kpi.product_summary",
    "typ" : "task",
    "parent" : "starbake.product",
    "parentTyp" : "table",
    "parentRef" : "starbake.product",
    "writeStrategy" : {
      "type" : "OVERWRITE"
    },
    "sink" : "kpi.product_summary",
    "cron" : "None"
  },
  "children" : [ {
    "data" : {
      "name" : "starbake.order",
      "typ" : "table",
      "parentTyp" : "unknown",
      "writeStrategy" : {
        "type" : "APPEND"
      },
      "cron" : "0 0 * * *"
    },
    "task" : false
  }, {
    "data" : {
      "name" : "starbake.order_line",
      "typ" : "table",
      "parentTyp" : "unknown",
      "writeStrategy" : {
        "type" : "APPEND"
      },
      "cron" : "0 * * * *",
      "stream" : ""
    },
    "task" : false
  }, {
    "data" : {
      "name" : "starbake.product",
      "typ" : "table",
      "parentTyp" : "unknown",
      "writeStrategy" : {
        "type" : "OVERWRITE"
      },
      "cron" : "0 0 * * *"
    },
    "task" : false
  } ],
  "task" : true
}, {
  "data" : {
    "name" : "kpi.revenue_summary",
    "typ" : "task",
    "parent" : "starbake.order",
    "parentTyp" : "table",
    "parentRef" : "starbake.order",
    "writeStrategy" : {
      "type" : "OVERWRITE"
    },
    "sink" : "kpi.revenue_summary",
    "cron" : "None"
  },
  "children" : [ {
    "data" : {
      "name" : "starbake.order",
      "typ" : "table",
      "parentTyp" : "unknown",
      "writeStrategy" : {
        "type" : "APPEND"
      },
      "cron" : "0 0 * * *"
    },
    "task" : false
  }, {
    "data" : {
      "name" : "starbake.order_line",
      "typ" : "table",
      "parentTyp" : "unknown",
      "writeStrategy" : {
        "type" : "APPEND"
      },
      "cron" : "0 * * * *",
      "stream" : ""
    },
    "task" : false
  } ],
  "task" : true
} ]""")

statements = {
  "kpi.order_items_analysis0" : {
    "preActions" : [ "USE kpi" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE kpi.order_items_analysis AS WITH order_details AS (\nSELECT  o.order_id\n, o.customer_id\n, List( p.name || ' (' || o.quantity || ')' ) AS purchased_items\n, Sum( o.quantity * p.price ) AS total_order_value\nFROM starbake.order_line o\nJOIN starbake.product p\nON o.product_id = p.product_id\nGROUP BY    o.order_id\n, o.customer_id )\nSELECT  order_id\n, customer_id\n, purchased_items\n, total_order_value\nFROM order_details\nORDER BY order_id;" ],
    "mainSqlIfExists" : [ "TRUNCATE TABLE kpi.order_items_analysis", "INSERT INTO kpi.order_items_analysis WITH order_details AS (\nSELECT  o.order_id\n, o.customer_id\n, List( p.name || ' (' || o.quantity || ')' ) AS purchased_items\n, Sum( o.quantity * p.price ) AS total_order_value\nFROM starbake.order_line o\nJOIN starbake.product p\nON o.product_id = p.product_id\nGROUP BY    o.order_id\n, o.customer_id )\nSELECT  order_id\n, customer_id\n, purchased_items\n, total_order_value\nFROM order_details\nORDER BY order_id" ]
  },
  "kpi.order_summary" : {
    "preActions" : [ "USE kpi" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE kpi.order_summary AS SELECT\nps.order_id,\nps.order_date,\nrs.total_revenue,\nps.profit,\nps.total_units_sold\nFROM\nkpi.product_summary ps\nJOIN kpi.revenue_summary rs ON ps.order_id = rs.order_id;" ],
    "mainSqlIfExists" : [ "TRUNCATE TABLE kpi.order_summary", "INSERT INTO kpi.order_summary SELECT\nps.order_id,\nps.order_date,\nrs.total_revenue,\nps.profit,\nps.total_units_sold\nFROM\nkpi.product_summary ps\nJOIN kpi.revenue_summary rs ON ps.order_id = rs.order_id" ]
  },
  "kpi.product_summary" : {
    "preActions" : [ "USE kpi" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE kpi.product_summary AS SELECT\np.product_id,\np.name AS product_name,\nSUM(ol.quantity) AS total_units_sold,\n(SUM(ol.sale_price) - Sum(ol.quantity * p.cost)) AS profit,\no.order_id,\no.timestamp AS order_date\nFROM\nstarbake.product p\nJOIN starbake.order_line ol ON p.product_id = ol.product_id\nJOIN starbake.order o ON ol.order_id = o.order_id\nGROUP BY\np.product_id,\no.order_id, p.name, o.timestamp;" ],
    "mainSqlIfExists" : [ "TRUNCATE TABLE kpi.product_summary", "INSERT INTO kpi.product_summary SELECT\np.product_id,\np.name AS product_name,\nSUM(ol.quantity) AS total_units_sold,\n(SUM(ol.sale_price) - Sum(ol.quantity * p.cost)) AS profit,\no.order_id,\no.timestamp AS order_date\nFROM\nstarbake.product p\nJOIN starbake.order_line ol ON p.product_id = ol.product_id\nJOIN starbake.order o ON ol.order_id = o.order_id\nGROUP BY\np.product_id,\no.order_id, p.name, o.timestamp" ]
  },
  "kpi.revenue_summary" : {
    "preActions" : [ "USE kpi" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE kpi.revenue_summary AS SELECT\no.order_id,\no.timestamp AS order_date,\nSUM(ol.quantity * ol.sale_price) AS total_revenue\nFROM\nstarbake.order o\nJOIN starbake.order_line ol ON o.order_id = ol.order_id\nGROUP BY\no.order_id, o.timestamp;" ],
    "mainSqlIfExists" : [ "TRUNCATE TABLE kpi.revenue_summary", "INSERT INTO kpi.revenue_summary SELECT\no.order_id,\no.timestamp AS order_date,\nSUM(ol.quantity * ol.sale_price) AS total_revenue\nFROM\nstarbake.order o\nJOIN starbake.order_line ol ON o.order_id = ol.order_id\nGROUP BY\no.order_id, o.timestamp" ]
  }
}

expectations = { }

audit = {
  "preActions" : [ "USE audit" ],
  "mainSqlIfNotExists" : [ "\n          SELECT\n            '{{jobid}}' AS JOBID,\n            '{{paths}}' AS PATHS,\n            '{{domain}}' AS DOMAIN,\n            '{{schema}}' AS SCHEMA,\n            {{success}} AS SUCCESS,\n            {{count}} AS COUNT,\n            {{countAccepted}} AS COUNTACCEPTED,\n            {{countRejected}} AS COUNTREJECTED,\n            TIMESTAMP '{{timestamp}}' AS TIMESTAMP,\n            {{duration}} AS DURATION,\n            '{{message}}' AS MESSAGE,\n            '{{step}}' AS STEP,\n            '{{database}}' AS DATABASE,\n            '{{tenant}}' AS TENANT\n        " ],
  "createSchemaSql" : [ "CREATE SCHEMA IF NOT EXISTS audit", "CREATE TABLE IF NOT EXISTS audit.audit (\n                              JOBID VARCHAR NOT NULL,\n                              PATHS TEXT NOT NULL,\n                              DOMAIN VARCHAR NOT NULL,\n                              SCHEMA VARCHAR NOT NULL,\n                              SUCCESS BOOLEAN NOT NULL,\n                              COUNT BIGINT NOT NULL,\n                              COUNTACCEPTED BIGINT NOT NULL,\n                              COUNTREJECTED BIGINT NOT NULL,\n                              TIMESTAMP TIMESTAMP NOT NULL,\n                              DURATION LONG NOT NULL,\n                              MESSAGE VARCHAR NOT NULL,\n                              STEP VARCHAR NOT NULL,\n                              DATABASE VARCHAR,\n                              TENANT VARCHAR\n                             )\n    " ],
  "mainSqlIfExists" : [ "\n          SELECT\n            '{{jobid}}' AS JOBID,\n            '{{paths}}' AS PATHS,\n            '{{domain}}' AS DOMAIN,\n            '{{schema}}' AS SCHEMA,\n            {{success}} AS SUCCESS,\n            {{count}} AS COUNT,\n            {{countAccepted}} AS COUNTACCEPTED,\n            {{countRejected}} AS COUNTREJECTED,\n            TIMESTAMP '{{timestamp}}' AS TIMESTAMP,\n            {{duration}} AS DURATION,\n            '{{message}}' AS MESSAGE,\n            '{{step}}' AS STEP,\n            '{{database}}' AS DATABASE,\n            '{{tenant}}' AS TENANT\n        " ]
}

with OrchestrationFactory.create_orchestration(job=sl_job) as orchestration:
    with orchestration.sl_create_pipeline(dependencies=dependencies) as pipeline:

        first_level_tasks: Set[str] = dependencies.first_level_tasks

        all_dependencies: Set[str] = dependencies.all_dependencies

        load_dependencies: Optional[bool] = pipeline.load_dependencies

        start = pipeline.start_task()

        pre_tasks = pipeline.pre_tasks()

        # create a task
        def create_task(task_id: str, task_name: str, task_type: StarlakeDependencyType, task_sink:str) -> Union[AbstractTask, AbstractTaskGroup]:
            if (task_type == StarlakeDependencyType.TASK):
                return pipeline.sl_transform(
                    task_id=task_id, 
                    transform_name=task_name,
                    params={'sink': task_sink},
                )
            else:
                load_domain_and_table = task_name.split(".", 1)
                domain = load_domain_and_table[0]
                table = load_domain_and_table[-1]
                return pipeline.sl_load(
                    task_id=task_id, 
                    domain=domain, 
                    table=table,
                )

        # build group of tasks recursively
        def generate_task_group_for_task(task: StarlakeDependency, parent_group_id: Optional[str] = None) -> Union[AbstractTaskGroup, AbstractTask]:
            task_name = task.name
            task_group_id = task.uri
            task_type = task.dependency_type
            task_id = f"{task_group_id}_{task_type}"
            task_sink = task.sink
            
            if load_dependencies and parent_group_id:
                task_id = parent_group_id + "_" + task_id # to ensure task_id uniqueness

            children: List[StarlakeDependency] = []
            if load_dependencies and len(task.dependencies) > 0: 
                children = task.dependencies
            else:
                for child in task.dependencies:
                    if child.name in first_level_tasks:
                        children.append(child)

            if children.__len__() > 0:
                with orchestration.sl_create_task_group(group_id=task_group_id, pipeline=pipeline) as task_group:
                    upstream_tasks = [generate_task_group_for_task(child, parent_group_id=task_group_id) for child in children]
                    task = create_task(task_id, task_name, task_type, task_sink)
                    task << upstream_tasks
                return task_group
            else:
                task = create_task(task_id=task_id, task_name=task_name, task_type=task_type, task_sink=task_sink)
                return task

        all_transform_tasks = [generate_task_group_for_task(task) for task in dependencies if task.name not in all_dependencies]

        if pre_tasks:
            start >> pre_tasks >> all_transform_tasks
        else:
            start >> all_transform_tasks

        end = pipeline.end_task()

        end << all_transform_tasks

        post_tasks = pipeline.post_tasks()

        if post_tasks:
            all_done = pipeline.sl_dummy_op(task_id="all_done")
            all_done << all_transform_tasks
            all_done >> post_tasks >> end

    print(f"Pipeline {pipeline.pipeline_id} created") 

from snowflake.core.task.dagv1 import DAG
dag: DAG = pipeline.dag
for task in dag.tasks:
    for pre in task.predecessors:
        print(f"Task {task.name} depends on {pre.name}")
for dep in dependencies.dependencies:
    print(f"Dependency {dep.name} with sink {dep.sink} has {len(dep.dependencies)} children")

from ai.starlake.orchestration import AbstractPipeline
from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.task.dagv1 import DAGOperation
from snowflake.snowpark import Session

def get_dag_operation(session: Session, database: str, schema: str, options: dict = dict()) -> DAGOperation:
  session.sql(f"USE DATABASE {database}").collect()
  session.sql(f"USE SCHEMA {schema}").collect()
  session.sql(f"USE WAREHOUSE {options.get('warehouse', 'COMPUTE_WH').upper()}").collect()
  root = Root(session)
  schema = root.databases[database].schemas[schema]
  return DAGOperation(schema)

def deploy_dag(session: Session, pipeline: AbstractPipeline, database: str, schema: str) -> None:
  stage_name = f"{database}.{schema}.{pipeline.options.get('stage_location', 'staging')}".upper()
  result = session.sql(f"SHOW STAGES LIKE '{stage_name.split('.')[-1]}'").collect()
  if not result:
      session.sql(f"CREATE STAGE {stage_name}").collect()

  session.custom_package_usage_config = {"enabled": True, "force_push": True}
  op = get_dag_operation(session, database, schema, pipeline.options)
  # op.delete(pipeline_id)
  op.deploy(pipeline.dag, mode = CreateMode.or_replace)
  print(f"Pipeline {pipeline.pipeline_id} deployed")

def run_dag(session: Session, pipeline: AbstractPipeline, database: str, schema: str) -> None:
  op = get_dag_operation(session, database, schema, pipeline.options)
  op.run(pipeline.dag)
  print(f"Pipeline {pipeline.pipeline_id} run")
