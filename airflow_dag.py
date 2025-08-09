from ai.starlake.job import StarlakeOrchestrator
orchestrator = StarlakeOrchestrator.AIRFLOW

from ai.starlake.job import StarlakeExecutionEnvironment
execution_environment = StarlakeExecutionEnvironment.SHELL

description="""sample dag configuration"""

template="transform/snowflake__scheduled_task__sql.py.j2"

access_control = None

options={
    'sl_env_var':'{"SL_ROOT": ".", "SL_ENV": "SNOWFLAKE"}', 
    'tags':'starlake', 
    'run_dependencies':'true', 
    'retries':'1',
    'retry_delay':'30',
    'stage_location':'staging',
    'schema': 'starbake',
    'warehouse':'COMPUTE_WH',
    'SL_STARLAKE_PATH': '/Users/smanciot/starlake/starlake'
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

from ai.starlake.orchestration import StarlakeDependencies, StarlakeDependency, StarlakeDependencyType, OrchestrationFactory, AbstractTaskGroup, AbstractTask, TreeNodeMixin as TreeNode

from typing import Dict, List, Optional, Set, Union

dependencies=StarlakeDependencies(dependencies="""[ {
  "data" : {
    "name" : "kpi.order_items_analysis",
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
      "cron" : "None",
      "stream": "starbake.order_line_stream"
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
      "parent" : "starbake.orders",
      "parentTyp" : "table",
      "parentRef" : "starbake.orders",
      "writeStrategy" : {
        "type" : "OVERWRITE"
      },
      "sink" : "kpi.revenue_summary",
      "cron" : "None"
    },
    "children" : [ {
      "data" : {
        "name" : "starbake.orders",
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
        "cron" : "None"
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
        "name" : "starbake.orders",
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
        "cron" : "None"
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
      "name" : "starbake.orders",
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
      "cron" : "None"
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
    "parent" : "starbake.orders",
    "parentTyp" : "table",
    "parentRef" : "starbake.orders",
    "writeStrategy" : {
      "type" : "OVERWRITE"
    },
    "sink" : "kpi.revenue_summary",
    "cron" : "None"
  },
  "children" : [ {
    "data" : {
      "name" : "starbake.orders",
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
      "cron" : "None"
    },
    "task" : false
  } ],
  "task" : true
} ]""")

statements = {
  "kpi.order_items_analysis" : {
    "preActions" : [ "USE SCHEMA kpi" ],
    "domain" : [ "kpi" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE kpi.order_items_analysis  AS WITH order_details AS (\nSELECT  o.order_id\n, o.customer_id\n, ARRAY_AGG( p.name || ' (' || ol.quantity || ')' ) AS purchased_items\n, Sum( ol.quantity * p.price ) AS total_order_value\nFROM starbake.orders o\nJOIN starbake.order_line ol\nON o.order_id=ol.order_id\nJOIN starbake.product p\nON o.product_id = p.product_id\nGROUP BY    o.order_id\n, o.customer_id )\nSELECT  order_id\n, customer_id\n, purchased_items\n, total_order_value\nFROM order_details\nORDER BY order_id;" ],
    "mainSqlIfExists" : [ "TRUNCATE TABLE kpi.order_items_analysis", "INSERT INTO kpi.order_items_analysis WITH order_details AS (\nSELECT  o.order_id\n, o.customer_id\n, ARRAY_AGG( p.name || ' (' || ol.quantity || ')' ) AS purchased_items\n, Sum( ol.quantity * p.price ) AS total_order_value\nFROM starbake.orders o\nJOIN starbake.order_line ol\nON o.order_id=ol.order_id\nJOIN starbake.product p\nON o.product_id = p.product_id\nGROUP BY    o.order_id\n, o.customer_id )\nSELECT  order_id\n, customer_id\n, purchased_items\n, total_order_value\nFROM order_details\nORDER BY order_id" ],
    "table" : [ "order_items_analysis" ],
    "connectionType" : [ "JDBC" ]
  },
  "kpi.order_summary" : {
    "preActions" : [ "USE SCHEMA kpi" ],
    "domain" : [ "kpi" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE kpi.order_summary  AS SELECT\nps.order_id,\nps.order_date,\nrs.total_revenue,\nps.profit,\nps.total_units_sold\nFROM\nkpi.product_summary ps\nJOIN kpi.revenue_summary rs ON ps.order_id = rs.order_id;" ],
    "mainSqlIfExists" : [ "TRUNCATE TABLE kpi.order_summary", "INSERT INTO kpi.order_summary SELECT\nps.order_id,\nps.order_date,\nrs.total_revenue,\nps.profit,\nps.total_units_sold\nFROM\nkpi.product_summary ps\nJOIN kpi.revenue_summary rs ON ps.order_id = rs.order_id" ],
    "table" : [ "order_summary" ],
    "connectionType" : [ "JDBC" ]
  },
  "kpi.product_summary" : {
    "preActions" : [ "USE SCHEMA kpi" ],
    "domain" : [ "kpi" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE kpi.product_summary  AS SELECT\np.product_id,\np.name AS product_name,\nSUM(ol.quantity) AS total_units_sold,\n(SUM(ol.sale_price) - Sum(ol.quantity * p.cost)) AS profit,\no.order_id,\no.order_date\nFROM\nstarbake.product p\nJOIN starbake.order_line ol ON p.product_id = ol.product_id\nJOIN starbake.orders o ON ol.order_id = o.order_id\nGROUP BY\np.product_id,\no.order_id, p.name, o.order_date;" ],
    "mainSqlIfExists" : [ "TRUNCATE TABLE kpi.product_summary", "INSERT INTO kpi.product_summary SELECT\np.product_id,\np.name AS product_name,\nSUM(ol.quantity) AS total_units_sold,\n(SUM(ol.sale_price) - Sum(ol.quantity * p.cost)) AS profit,\no.order_id,\no.order_date\nFROM\nstarbake.product p\nJOIN starbake.order_line ol ON p.product_id = ol.product_id\nJOIN starbake.orders o ON ol.order_id = o.order_id\nGROUP BY\np.product_id,\no.order_id, p.name, o.order_date" ],
    "table" : [ "product_summary" ],
    "connectionType" : [ "JDBC" ]
  },
  "kpi.revenue_summary" : {
    "preActions" : [ "USE SCHEMA kpi" ],
    "domain" : [ "kpi" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE kpi.revenue_summary  AS SELECT\no.order_id,\no.order_date,\nSUM(ol.quantity * ol.sale_price) AS total_revenue\nFROM\nstarbake.orders o\nJOIN starbake.order_line ol ON o.order_id = ol.order_id\nGROUP BY\no.order_id, o.order_date;" ],
    "mainSqlIfExists" : [ "TRUNCATE TABLE kpi.revenue_summary", "INSERT INTO kpi.revenue_summary SELECT\no.order_id,\no.order_date,\nSUM(ol.quantity * ol.sale_price) AS total_revenue\nFROM\nstarbake.orders o\nJOIN starbake.order_line ol ON o.order_id = ol.order_id\nGROUP BY\no.order_id, o.order_date" ],
    "table" : [ "revenue_summary" ],
    "connectionType" : [ "JDBC" ]
  }
}

expectation_items = {
  "kpi.order_items_analysis" : [ {
    "name" : "is_col_value_not_unique",
    "params" : "order_id",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.order_items_analysis)\nSELECT COALESCE(max(cnt), 0)\n    FROM (SELECT order_id, count(*) as cnt FROM sl_this GROUP BY order_id) AS COL_COUNT",
    "failOnError" : "no"
  }, {
    "name" : "is_row_count_to_be_between",
    "params" : "1, 2",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.order_items_analysis)\nselect\n    case\n    when count(*) between 1 and 2 then 1\n    else 0\n    end\n    from SL_THIS",
    "failOnError" : "no"
  } ],
  "kpi.order_summary" : [ {
    "name" : "is_col_value_not_unique",
    "params" : "order_id",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.order_summary)\nSELECT COALESCE(max(cnt), 0)\n    FROM (SELECT order_id, count(*) as cnt FROM sl_this GROUP BY order_id) AS COL_COUNT",
    "failOnError" : "no"
  }, {
    "name" : "is_row_count_to_be_between",
    "params" : "1, 2",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.order_summary)\nselect\n    case\n    when count(*) between 1 and 2 then 1\n    else 0\n    end\n    from SL_THIS",
    "failOnError" : "no"
  } ],
  "kpi.product_summary" : [ {
    "name" : "is_col_value_not_unique",
    "params" : "order_id",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.product_summary)\nSELECT COALESCE(max(cnt), 0)\n    FROM (SELECT order_id, count(*) as cnt FROM sl_this GROUP BY order_id) AS COL_COUNT",
    "failOnError" : "no"
  }, {
    "name" : "is_row_count_to_be_between",
    "params" : "1, 2",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.product_summary)\nselect\n    case\n    when count(*) between 1 and 2 then 1\n    else 0\n    end\n    from SL_THIS",
    "failOnError" : "no"
  } ],
  "kpi.revenue_summary" : [ {
    "name" : "is_col_value_not_unique",
    "params" : "order_id",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.revenue_summary)\nSELECT COALESCE(max(cnt), 0)\n    FROM (SELECT order_id, count(*) as cnt FROM sl_this GROUP BY order_id) AS COL_COUNT",
    "failOnError" : "no"
  }, {
    "name" : "is_row_count_to_be_between",
    "params" : "1, 2",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.revenue_summary)\nselect\n    case\n    when count(*) between 1 and 2 then 1\n    else 0\n    end\n    from SL_THIS",
    "failOnError" : "no"
  } ]
}

audit = {
  "preActions" : [ "USE SCHEMA audit" ],
  "domain" : [ "audit" ],
  "createSchemaSql" : [ "CREATE SCHEMA IF NOT EXISTS audit", "CREATE TABLE IF NOT EXISTS audit.audit (\n                              JOBID VARCHAR NOT NULL,\n                              PATHS TEXT NOT NULL,\n                              DOMAIN VARCHAR NOT NULL,\n                              SCHEMA VARCHAR NOT NULL,\n                              SUCCESS BOOLEAN NOT NULL,\n                              COUNT BIGINT NOT NULL,\n                              COUNTACCEPTED BIGINT NOT NULL,\n                              COUNTREJECTED BIGINT NOT NULL,\n                              TIMESTAMP TIMESTAMP NOT NULL,\n                              DURATION BIGINT NOT NULL,\n                              MESSAGE VARCHAR NOT NULL,\n                              STEP VARCHAR NOT NULL,\n                              DATABASE VARCHAR,\n                              TENANT VARCHAR\n                             )\n    " ],
  "mainSqlIfExists" : [ "\n          SELECT\n            '{jobid}' AS JOBID,\n            '{paths}' AS PATHS,\n            '{domain}' AS DOMAIN,\n            '{schema}' AS SCHEMA,\n            {success} AS SUCCESS,\n            {count} AS COUNT,\n            {countAccepted} AS COUNTACCEPTED,\n            {countRejected} AS COUNTREJECTED,\n            TO_TIMESTAMP('{timestamp}') AS TIMESTAMP,\n            {duration} AS DURATION,\n            '{message}' AS MESSAGE,\n            '{step}' AS STEP,\n            '{database}' AS DATABASE,\n            '{tenant}' AS TENANT\n        " ],
  "table" : [ "audit" ],
  "connectionType" : [ "JDBC" ]
}

expectations = {
  "domain" : [ "audit" ],
  "createSchemaSql" : [ "CREATE TABLE IF NOT EXISTS audit.expectations (\n                            JOBID VARCHAR NOT NULL,\n                            DATABASE VARCHAR,\n                            DOMAIN VARCHAR NOT NULL,\n                            SCHEMA VARCHAR NOT NULL,\n                            TIMESTAMP TIMESTAMP NOT NULL,\n                            NAME VARCHAR NOT NULL,\n                            PARAMS VARCHAR NOT NULL,\n                            SQL VARCHAR NOT NULL,\n                            COUNT BIGINT NOT NULL,\n                            EXCEPTION VARCHAR NOT NULL,\n                            SUCCESS BOOLEAN NOT NULL\n                          )\n        " ],
  "mainSqlIfExists" : [ "\n          SELECT\n            '{jobid}' AS JOBID,\n            '{database}' AS DATABASE,\n            '{domain}' AS DOMAIN,\n            '{schema}' AS SCHEMA,\n            TO_TIMESTAMP('{timestamp}') AS TIMESTAMP,\n            '{name}' AS NAME,\n            '{params}' AS PARAMS,\n            '{sql}' AS SQL,\n            {count} AS COUNT,\n            '{exception}' AS EXCEPTION,\n            {success} AS SUCCESS\n        " ],
  "table" : [ "expectations" ],
  "connectionType" : [ "JDBC" ]
}

with OrchestrationFactory.create_orchestration(job=sl_job) as orchestration:
    with orchestration.sl_create_pipeline(dependencies=dependencies) as pipeline:

        graphs: Set[TreeNode] = pipeline.graphs

        start = pipeline.start_task()
        pre_tasks = pipeline.pre_tasks()
        if pre_tasks:
            start >> pre_tasks

        end = pipeline.end_task()
        post_tasks = pipeline.post_tasks()
        all_done = None
        if post_tasks:
            all_done = pipeline.sl_dummy_op(task_id="all_done")
            all_done >> post_tasks >> end

        def generate_tasks_for_graph(graph: TreeNode):
          parent_tasks: List[AbstractTask] = list()
          for parent in graph.parents:
              parent_task = pipeline.dependency_to_task(parent.node)
              parent_tasks.append(parent_task)
          task = pipeline.dependency_to_task(graph.node)
          if pre_tasks:
              if len(parent_tasks) > 0:
                  task << parent_tasks
                  pre_tasks >> parent_tasks
              else:
                  pre_tasks >> task
          else:
              if len(parent_tasks) > 0:
                  start >> parent_tasks
                  task << parent_tasks
              else:
                  start >> task
          if all_done:
              task >> all_done
          else:
              task >> end
        
        for graph in graphs:
            generate_tasks_for_graph(graph)

pipelines = [pipeline]
