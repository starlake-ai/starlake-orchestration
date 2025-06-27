from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark.row import Row
from typing import List, Tuple
import os
import datetime



#
# au deploiement de dags, l'utilisateur rajoute le nom de la connexion 
# et le nom du schema de déploiement
connection_parameters = {
   "account": os.environ['SNOWFLAKE_ACCOUNT'],
   "user": os.environ['SNOWFLAKE_USER'],
   "password": os.environ['SNOWFLAKE_PASSWORD'],
#   "role": "<your snowflake role>",  # optional
   "warehouse": os.environ['SNOWFLAKE_WAREHOUSE'],  # optional
#   "schema": "<your snowflake schema>",  # optional
   "database": os.environ['SNOWFLAKE_DB'],  # optional
}


# deploy.scala
# SNOWFLAKE_ACCOUNT=A SNOWFLAKE_USER=U SNOWFLAKE_PASSWORD=P SNOWFLAKE_WAREHOUSE=W python deploy.py

expectation_items = {
  "kpi.order_items_analysis" : [ {
    "name" : "is_col_value_not_unique",
    "params" : "id",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.order_items_analysis)\nSELECT COALESCE(max(cnt), 0)\n    FROM (SELECT , count(*) as cnt FROM sl_this GROUP BY ) AS COL_COUNT",
    "failOnError" : "no"
  }, {
    "name" : "is_row_count_to_be_between",
    "params" : "1, 2",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.order_items_analysis)\nselect\n    case\n    when count(*) between 1 and 2 then 1\n    else 0\n    end\n    from SL_THIS",
    "failOnError" : "no"
  }, {
    "name" : "is_col_value_not_unique",
    "params" : "id",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.order_items_analysis)\nSELECT COALESCE(max(cnt), 0)\n    FROM (SELECT , count(*) as cnt FROM sl_this GROUP BY ) AS COL_COUNT",
    "failOnError" : "no"
  }, {
    "name" : "is_row_count_to_be_between",
    "params" : "1, 2",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.order_items_analysis)\nselect\n    case\n    when count(*) between 1 and 2 then 1\n    else 0\n    end\n    from SL_THIS",
    "failOnError" : "no"
  } ],
  "kpi.order_summary" : [ {
    "name" : "is_col_value_not_unique",
    "params" : "id",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.order_summary)\nSELECT COALESCE(max(cnt), 0)\n    FROM (SELECT , count(*) as cnt FROM sl_this GROUP BY ) AS COL_COUNT",
    "failOnError" : "no"
  }, {
    "name" : "is_row_count_to_be_between",
    "params" : "1, 2",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.order_summary)\nselect\n    case\n    when count(*) between 1 and 2 then 1\n    else 0\n    end\n    from SL_THIS",
    "failOnError" : "no"
  }, {
    "name" : "is_col_value_not_unique",
    "params" : "id",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.order_summary)\nSELECT COALESCE(max(cnt), 0)\n    FROM (SELECT , count(*) as cnt FROM sl_this GROUP BY ) AS COL_COUNT",
    "failOnError" : "no"
  }, {
    "name" : "is_row_count_to_be_between",
    "params" : "1, 2",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.order_summary)\nselect\n    case\n    when count(*) between 1 and 2 then 1\n    else 0\n    end\n    from SL_THIS",
    "failOnError" : "no"
  } ],
  "kpi.product_summary" : [ {
    "name" : "is_col_value_not_unique",
    "params" : "id",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.product_summary)\nSELECT COALESCE(max(cnt), 0)\n    FROM (SELECT , count(*) as cnt FROM sl_this GROUP BY ) AS COL_COUNT",
    "failOnError" : "no"
  }, {
    "name" : "is_row_count_to_be_between",
    "params" : "1, 2",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.product_summary)\nselect\n    case\n    when count(*) between 1 and 2 then 1\n    else 0\n    end\n    from SL_THIS",
    "failOnError" : "no"
  }, {
    "name" : "is_col_value_not_unique",
    "params" : "id",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.product_summary)\nSELECT COALESCE(max(cnt), 0)\n    FROM (SELECT , count(*) as cnt FROM sl_this GROUP BY ) AS COL_COUNT",
    "failOnError" : "no"
  }, {
    "name" : "is_row_count_to_be_between",
    "params" : "1, 2",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.product_summary)\nselect\n    case\n    when count(*) between 1 and 2 then 1\n    else 0\n    end\n    from SL_THIS",
    "failOnError" : "no"
  } ],
  "kpi.revenue_summary" : [ {
    "name" : "is_col_value_not_unique",
    "params" : "id",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.revenue_summary)\nSELECT COALESCE(max(cnt), 0)\n    FROM (SELECT , count(*) as cnt FROM sl_this GROUP BY ) AS COL_COUNT",
    "failOnError" : "no"
  }, {
    "name" : "is_row_count_to_be_between",
    "params" : "1, 2",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.revenue_summary)\nselect\n    case\n    when count(*) between 1 and 2 then 1\n    else 0\n    end\n    from SL_THIS",
    "failOnError" : "no"
  }, {
    "name" : "is_col_value_not_unique",
    "params" : "id",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.revenue_summary)\nSELECT COALESCE(max(cnt), 0)\n    FROM (SELECT , count(*) as cnt FROM sl_this GROUP BY ) AS COL_COUNT",
    "failOnError" : "no"
  }, {
    "name" : "is_row_count_to_be_between",
    "params" : "1, 2",
    "query" : "WITH SL_THIS AS (SELECT * FROM kpi.revenue_summary)\nselect\n    case\n    when count(*) between 1 and 2 then 1\n    else 0\n    end\n    from SL_THIS",
    "failOnError" : "no"
  } ]
}

#statements = {{ context.statements }}

#expectation_items = {{ context.expectationItems }}

#audit = {{ context.audit }}

#expectations = {{ context.expectations }}

#acl = {{ context.acl }}


statements = {
  "sales_kpi.byseller_kpi0" : {
    "preActions" : [ "USE SCHEMA sales_kpi" ],
    "domain" : [ "sales_kpi" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE sales_kpi.byseller_kpi  AS with mycte as (\nselect o.amount, c.id, CURRENT_TIMESTAMP() as timestamp\nfrom sales.orders o, sales.customers c\nwhere o.customer_id = c.id\n)\nselect id, sum(amount) as sum, timestamp\nfrom mycte\ngroup by mycte.id, mycte.timestamp;" ],
    "mainSqlIfExists" : [ "INSERT INTO sales_kpi.byseller_kpi with mycte as (\nselect o.amount, c.id, CURRENT_TIMESTAMP() as timestamp\nfrom sales.orders o, sales.customers c\nwhere o.customer_id = c.id\n)\nselect id, sum(amount) as sum, timestamp\nfrom mycte\ngroup by mycte.id, mycte.timestamp" ],
    "table" : [ "byseller_kpi" ],
    "connectionType" : [ "JDBC" ]
  },
  "sales_kpi.byseller_kpi1" : {
    "preActions" : [ "USE SCHEMA sales_kpi" ],
    "domain" : [ "sales_kpi" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE sales_kpi.byseller_kpi1  AS select count(*) as cnt from sales.orders;" ],
    "mainSqlIfExists" : [ "INSERT INTO sales_kpi.byseller_kpi1 select count(*) as cnt from sales.orders" ],
    "table" : [ "byseller_kpi1" ],
    "connectionType" : [ "JDBC" ]
  },
  "bqtest.table1" : {
    "preActions" : [ "USE SCHEMA bqtest" ],
    "domain" : [ "bqtest" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE bqtest.table1111  AS select 'a' as id, date('2020-01-01') as due, 'newname' as name;" ],
    "mainSqlIfExists" : [ "DELETE FROM bqtest.table1111 \nWHERE due IN (SELECT DISTINCT due FROM (select 'a' as id, date('2020-01-01') as due, 'newname' as name))", "\nINSERT INTO bqtest.table1111(id,due,name) select 'a' as id, date('2020-01-01') as due, 'newname' as name;" ],
    "table" : [ "table1111" ],
    "connectionType" : [ "JDBC" ]
  }
}

expectation_items = { }

audit = {
  "preActions" : [ "USE SCHEMA audit" ],
  "domain" : [ "audit" ],
  "createSchemaSql" : [ "CREATE SCHEMA IF NOT EXISTS audit", "CREATE TABLE IF NOT EXISTS audit.audit (\n                              JOBID VARCHAR NOT NULL,\n                              PATHS TEXT NOT NULL,\n                              DOMAIN VARCHAR NOT NULL,\n                              SCHEMA VARCHAR NOT NULL,\n                              SUCCESS BOOLEAN NOT NULL,\n                              COUNT BIGINT NOT NULL,\n                              COUNTACCEPTED BIGINT NOT NULL,\n                              COUNTREJECTED BIGINT NOT NULL,\n                              TIMESTAMP TIMESTAMP NOT NULL,\n                              DURATION BIGINT NOT NULL,\n                              MESSAGE VARCHAR NOT NULL,\n                              STEP VARCHAR NOT NULL,\n                              DATABASE VARCHAR,\n                              TENANT VARCHAR\n                             )\n    " ],
  "mainSqlIfExists" : [ "\n          SELECT\n            '{jobid}' AS JOBID,\n            '{paths}' AS PATHS,\n            '{domain}' AS DOMAIN,\n            '{schema}' AS SCHEMA,\n            {success} AS SUCCESS,\n            {count} AS COUNT,\n            {countAccepted} AS COUNTACCEPTED,\n            {countRejected} AS COUNTREJECTED,\n            TO_TIMESTAMP('{timestamp}') AS TIMESTAMP,\n            {duration} AS DURATION,\n            '{message}' AS MESSAGE,\n            '{step}' AS STEP,\n            '{database}' AS DATABASE,\n            '{tenant}' AS TENANT\n        " ],
  "connectionType" : [ "JDBC" ]
}

expectations = {
  "domain" : [ "audit" ],
  "createSchemaSql" : [ "CREATE TABLE IF NOT EXISTS audit.expectations (\n                            JOBID VARCHAR NOT NULL,\n                            DATABASE VARCHAR,\n                            DOMAIN VARCHAR NOT NULL,\n                            SCHEMA VARCHAR NOT NULL,\n                            TIMESTAMP TIMESTAMP NOT NULL,\n                            NAME VARCHAR NOT NULL,\n                            PARAMS VARCHAR NOT NULL,\n                            SQL VARCHAR NOT NULL,\n                            COUNT BIGINT NOT NULL,\n                            EXCEPTION VARCHAR NOT NULL,\n                            SUCCESS BOOLEAN NOT NULL\n                          )\n        " ],
  "mainSqlIfExists" : [ "\n          SELECT\n            '{jobid}' AS JOBID,\n            '{database}' AS DATABASE,\n            '{domain}' AS DOMAIN,\n            '{schema}' AS SCHEMA,\n            TO_TIMESTAMP('{timestamp}') AS TIMESTAMP,\n            '{name}' AS NAME,\n            '{params}' AS PARAMS,\n            '{sql}' AS SQL,\n            {count} AS COUNT,\n            '{exception}' AS EXCEPTION,\n            {success} AS SUCCESS\n        " ],
  "connectionType" : [ "JDBC" ]
}

acl = { }

def sl_plit_domain_task(task: str) -> Tuple[str, str]:
   domain, task = task.split('.')
   return domain, task


def sl_log_expectation(session: Session, task: str, success: bool, name: str, params: str, sql: str, count: int, exception: str, ts: datetime):
   from snowflake.core.task.context import TaskContext
   try:
      jobid = TaskContext(session).get_current_task_name()
      domain, task = sl_plit_domain_task(task)
      expectation_sql = expectations.get("mainSqlIfExists", [])[0]
      expectation_domain = expectations.get("domain", [])[0]
      formatted_sql = expectation_sql.format(
         jobid = jobid, # should be the full name of the snowflake task including the id of the dag
         database = "",
         domain = domain,
         schema = task,
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
   except Exception as e:
      error_message = str(e)
      print(error_message)

def sl_log_audit(session: Session, task: str, success: bool, duration: int, message: str, ts: datetime):
   from snowflake.core.task.context import TaskContext
   try:
      jobid = TaskContext(session).get_current_task_name()
      domain, task = sl_plit_domain_task(task)
      audit_sql = audit.get("mainSqlIfExists", [])[0]
      audit_domain = audit.get("domain", [])[0]
      formatted_sql = audit_sql.format(
         jobid = jobid, # should be the full name of the snowflake task including the id of the dag
         paths = task,
         domain = domain,
         schema = task,
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
   except Exception as e:
      error_message = str(e)
      print(error_message)



def sl_run_task(session: Session, task: str):
   start = datetime.datetime.now()
   try:
      session.sql("BEGIN").collect()
      taskDomain = statements.get(task).get("domain")[0]
      taskTable = statements.get(task).get("table")[0]
      sl_create_domain(session, taskDomain)
      for statement in statements.get(task, {}).get("preActions", []):
         print(statement)
         session.sql(statement).collect()
      table_exists = sl_table_exists(session, f"{taskDomain}.{taskTable}")

      pre_sql = statements.get(task, {}).get('preSqls', [])
      for sql in pre_sql:
         session.sql(sql).collect()

      if  not table_exists:
         print(f'{task} table does not exist')
         sqls: List[str] = statements.get(task, {}).get('mainSqlIfNotExists', [])
         for sql in sqls:
               print(sql)
               session.sql(sql).collect()
      else:
         print(f'{task} table exists')
         scd2_sql = statements.get(task, {}).get('addSCD2ColumnsSqls', [])
         for sql in scd2_sql:
            print(sql)
            session.sql(sql).collect()
         sqls: List[str] = statements.get(task, {}).get('mainSqlIfExists', [])
         for sql in sqls:
            print(sql)
            session.sql(sql).collect()

      post_sql = statements.get(task, {}).get('postsql', [])
      for sql in post_sql:
         session.sql(sql).collect()
      session.sql("COMMIT").collect()

      end = datetime.datetime.now()
      duration = (end - start).total_seconds()
      sl_log_audit(session, task, True, duration, "success", end)
   except Exception as e:
      session.sql("ROLLBACK").collect()
      end = datetime.datetime.now()
      duration = (end - start).total_seconds()
      error_message = str(e)
      print(error_message)
      sl_log_audit(session, task, False, duration, error_message, end)
      raise e

def sl_table_exists(session: Session, table_name: str) -> bool:
    df: DataFrame = session.sql(query=f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) ILIKE '{table_name}'")
    rows: List[Row] = df.collect()
    return rows.__len__() > 0


def sl_create_audit_table(session: Session):
   for statement in audit.get("preActions", []):
      session.sql(statement).collect()
   sl_audit_table_exists = sl_table_exists(session, 'audit.audit')
   if  not sl_audit_table_exists:
      print('AUDIT table does not exist')
      sqls: List[str] = audit.get('createSchemaSql', [])
      for sql in sqls:
            session.sql(sql).collect()

def sl_create_expectations_table(session: Session):
   sl_expectation_table_exists = sl_table_exists(session, 'audit.expectations')
   if  not sl_expectation_table_exists:
      print('EXPECTATIONS table does not exist')
      sqls: List[str] = expectations.get('createSchemaSql', [])
      for sql in sqls:
            session.sql(sql).collect()


def sl_create_domain(session: Session, domain: str):
   sql = f"CREATE SCHEMA IF NOT EXISTS {domain}"
   session.sql(sql).collect()

def sl_create_acl(session: Session, task: str):
    if acl.get(task, None) is not None:
      sqls: List[str] = acl.get(task)
      for sql in sqls:
         session.sql(sql).collect()




def sl_run_expectations(session: Session, task: str):
   for expectation in expectations.get(task, []):
      print(expectation)
      name = expectation.get("name")
      params = expectation.get("params")
      query = expectation.get("query")
      failOnError = expectation.get("failOnError")
      count = 0
      try:
         df: DataFrame = session.sql(query).collect()
         rows: List[Row] = df.collect()
         if rows.__len__ != 1:
            raise Exception(f'Expectation failed for {task}: {query}. Expected 1 row but got {rows.__len__()}')
         count = rows.collect()[0][0]
         #  log expectations as audit in expectation table here
         if count != 0:
            raise Exception(f'Expectation failed for {task}: {query}. Expected count to be equal to 0 but got {count}')
         sl_log_expectation(session, task, True, name, params, query, count, "", datetime.datetime.now())
      except Exception as e:
         error_message = str(e)
         print(error_message)
         sl_log_expectation(session, task, False, name, params, query, count, error_message, datetime.datetime.now())
         if failOnError == "yes":
            raise e



def sl_main(session: Session):
   sl_create_audit_table(session)
   sl_create_expectations_table(session)
   for task in statements.keys():
      sl_run_task(session, task)
      sl_run_expectations(session, task)
      sl_create_acl(session, task)



session = Session.builder.configs(connection_parameters).create()
try:
   sl_main(session)
finally:
   session.close()