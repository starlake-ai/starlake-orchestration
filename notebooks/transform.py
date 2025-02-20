from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark.row import Row
import pandas as pd
from typing import List, Optional, Tuple, Union

connection_parameters = {
   "account": os.environ['SNOWFLAKE_ACCOUNT'],
   "user": os.environ['SNOWFLAKE_USER'],
   "password": os.environ['SNOWFLAKE_PASSWORD'],
#   "role": "<your snowflake role>",  # optional
   "warehouse": os.environ['SNOWFLAKE_WAREHOUSE'],  # optional
#   "schema": "<your snowflake schema>",  # optional
   "database": os.environ['SNOWFLAKE_DB'],  # optional
}

audit = {
  "preActions" : [ "USE SCHEMA audit" ],
  "mainSqlIfNotExists" : [ "\n          SELECT\n            '{{jobid}}' AS JOBID,\n            '{{paths}}' AS PATHS,\n            '{{domain}}' AS DOMAIN,\n            '{{schema}}' AS SCHEMA,\n            {{success}} AS SUCCESS,\n            {{count}} AS COUNT,\n            {{countAccepted}} AS COUNTACCEPTED,\n            {{countRejected}} AS COUNTREJECTED,\n            TO_TIMESTAMP('{{timestamp}}') AS TIMESTAMP,\n            {{duration}} AS DURATION,\n            '{{message}}' AS MESSAGE,\n            '{{step}}' AS STEP,\n            '{{database}}' AS DATABASE,\n            '{{tenant}}' AS TENANT\n        " ],
  "createSchemaSql" : [ "CREATE SCHEMA IF NOT EXISTS audit", "CREATE TABLE audit.audit (\n                              JOBID VARCHAR NOT NULL,\n                              PATHS TEXT NOT NULL,\n                              DOMAIN VARCHAR NOT NULL,\n                              SCHEMA VARCHAR NOT NULL,\n                              SUCCESS BOOLEAN NOT NULL,\n                              COUNT BIGINT NOT NULL,\n                              COUNTACCEPTED BIGINT NOT NULL,\n                              COUNTREJECTED BIGINT NOT NULL,\n                              TIMESTAMP TIMESTAMP NOT NULL,\n                              DURATION BIGINT NOT NULL,\n                              MESSAGE VARCHAR NOT NULL,\n                              STEP VARCHAR NOT NULL,\n                              DATABASE VARCHAR,\n                              TENANT VARCHAR\n                             )\n    " ],
  "mainSqlIfExists" : [ "\n          SELECT\n            '{{jobid}}' AS JOBID,\n            '{{paths}}' AS PATHS,\n            '{{domain}}' AS DOMAIN,\n            '{{schema}}' AS SCHEMA,\n            {{success}} AS SUCCESS,\n            {{count}} AS COUNT,\n            {{countAccepted}} AS COUNTACCEPTED,\n            {{countRejected}} AS COUNTREJECTED,\n            TO_TIMESTAMP('{{timestamp}}') AS TIMESTAMP,\n            {{duration}} AS DURATION,\n            '{{message}}' AS MESSAGE,\n            '{{step}}' AS STEP,\n            '{{database}}' AS DATABASE,\n            '{{tenant}}' AS TENANT\n        " ]
}


statements = {
  "kpi.order_items_analysis" : {
    "preActions" : [ "USE SCHEMA kpi" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE kpi.order_items_analysis  AS WITH order_details AS (\nSELECT  o.order_id\n, o.customer_id\n, List( p.name || ' (' || o.quantity || ')' ) AS purchased_items\n, Sum( o.quantity * p.price ) AS total_order_value\nFROM starbake.order_line o\nJOIN starbake.product p\nON o.product_id = p.product_id\nGROUP BY    o.order_id\n, o.customer_id )\nSELECT  order_id\n, customer_id\n, purchased_items\n, total_order_value\nFROM order_details\nORDER BY order_id;" ],
    "mainSqlIfExists" : [ "TRUNCATE TABLE kpi.order_items_analysis", "INSERT INTO kpi.order_items_analysis WITH order_details AS (\nSELECT  o.order_id\n, o.customer_id\n, List( p.name || ' (' || o.quantity || ')' ) AS purchased_items\n, Sum( o.quantity * p.price ) AS total_order_value\nFROM starbake.order_line o\nJOIN starbake.product p\nON o.product_id = p.product_id\nGROUP BY    o.order_id\n, o.customer_id )\nSELECT  order_id\n, customer_id\n, purchased_items\n, total_order_value\nFROM order_details\nORDER BY order_id" ]
  },
  "kpi.order_summary" : {
    "preActions" : [ "USE SCHEMA kpi" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE kpi.order_summary  AS SELECT\nps.order_id,\nps.order_date,\nrs.total_revenue,\nps.profit,\nps.total_units_sold\nFROM\nkpi.product_summary ps\nJOIN kpi.revenue_summary rs ON ps.order_id = rs.order_id;" ],
    "mainSqlIfExists" : [ "TRUNCATE TABLE kpi.order_summary", "INSERT INTO kpi.order_summary SELECT\nps.order_id,\nps.order_date,\nrs.total_revenue,\nps.profit,\nps.total_units_sold\nFROM\nkpi.product_summary ps\nJOIN kpi.revenue_summary rs ON ps.order_id = rs.order_id" ]
  },
  "kpi.product_summary" : {
    "preActions" : [ "USE SCHEMA kpi" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE kpi.product_summary  AS SELECT\np.product_id,\np.name AS product_name,\nSUM(ol.quantity) AS total_units_sold,\n(SUM(ol.sale_price) - Sum(ol.quantity * p.cost)) AS profit,\no.order_id,\no.timestamp AS order_date\nFROM\nstarbake.product p\nJOIN starbake.order_line ol ON p.product_id = ol.product_id\nJOIN starbake.order o ON ol.order_id = o.order_id\nGROUP BY\np.product_id,\no.order_id, p.name, o.timestamp;" ],
    "mainSqlIfExists" : [ "TRUNCATE TABLE kpi.product_summary", "INSERT INTO kpi.product_summary SELECT\np.product_id,\np.name AS product_name,\nSUM(ol.quantity) AS total_units_sold,\n(SUM(ol.sale_price) - Sum(ol.quantity * p.cost)) AS profit,\no.order_id,\no.timestamp AS order_date\nFROM\nstarbake.product p\nJOIN starbake.order_line ol ON p.product_id = ol.product_id\nJOIN starbake.order o ON ol.order_id = o.order_id\nGROUP BY\np.product_id,\no.order_id, p.name, o.timestamp" ]
  },
  "kpi.revenue_summary" : {
    "preActions" : [ "USE SCHEMA kpi" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE kpi.revenue_summary  AS SELECT\no.order_id,\no.timestamp AS order_date,\nSUM(ol.quantity * ol.sale_price) AS total_revenue\nFROM\nstarbake.order o\nJOIN starbake.order_line ol ON o.order_id = ol.order_id\nGROUP BY\no.order_id, o.timestamp;" ],
    "mainSqlIfExists" : [ "TRUNCATE TABLE kpi.revenue_summary", "INSERT INTO kpi.revenue_summary SELECT\no.order_id,\no.timestamp AS order_date,\nSUM(ol.quantity * ol.sale_price) AS total_revenue\nFROM\nstarbake.order o\nJOIN starbake.order_line ol ON o.order_id = ol.order_id\nGROUP BY\no.order_id, o.timestamp" ]
  }
}


def sl_main(session: Session, task: str): 
   for statement in statements.get(task, {}).get("preActions", []):
      session.sql(statement)
   sl_table_exists = sl_table_exists(session, task)
   if  not sl_table_exists:
      print(f'{task} table does not exist')
      sqls: List[str] = statements.get(task, {}).get('mainSqlIfNotExists', [])
      for sql in sqls:
            session.sql(sql).show()
   else:
      print(f'{task} table exists')
      sqls: List[str] = statements.get(task, {}).get('mainSqlIfExists', [])
      for sql in sqls:
            session.sql(sql).show()

def sl_table_exists(session: Session, table_name: str) -> bool:
    df: DataFrame = session.sql(query=f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) ILIKE '{table_name}'") 
    rows: List[Row] = df.collect()
    return rows.__len__() > 0


def sl_create_audit_table(session: Session):
   for statement in audit.get("preActions", []):
      session.sql(statement)
   sl_audit_table_exists = sl_table_exists(session, 'audit.audit')
   if  not sl_audit_table_exists:
      print('AUDIT table does not exist')
      sqls: List[str] = audit.get('createSchemaSql', [])
      for sql in sqls:
            session.sql(sql).show()



session = Session.builder.configs(connection_parameters).create()

def sl_run_task(session: Session, task: str):
   try:
      sl_create_audit_table(session)
      sl_main(session, task)
   except Exception as e:
      error_message = str(e)
      print(error_message)
   finally:
      session.close()
