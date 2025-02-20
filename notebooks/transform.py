from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark.row import Row
import pandas as pd
from typing import List, Optional, Tuple, Union

connection_parameters = {
   "account": "qbuqrgc-or28005",
   "user": "unit_test_service",
   "password": "Azizam7157Azizam7157!!",
#   "role": "<your snowflake role>",  # optional
   "warehouse": "COMPUTE_WH",  # optional
#   "schema": "<your snowflake schema>",  # optional
   "database": "starlake1",  # optional
}

audit = {
  "preActions" : [ "USE SCHEMA audit" ],
  "mainSqlIfNotExists" : [ "\n          SELECT\n            '{{jobid}}' AS JOBID,\n            '{{paths}}' AS PATHS,\n            '{{domain}}' AS DOMAIN,\n            '{{schema}}' AS SCHEMA,\n            {{success}} AS SUCCESS,\n            {{count}} AS COUNT,\n            {{countAccepted}} AS COUNTACCEPTED,\n            {{countRejected}} AS COUNTREJECTED,\n            TO_TIMESTAMP('{{timestamp}}') AS TIMESTAMP,\n            {{duration}} AS DURATION,\n            '{{message}}' AS MESSAGE,\n            '{{step}}' AS STEP,\n            '{{database}}' AS DATABASE,\n            '{{tenant}}' AS TENANT\n        " ],
  "createSchemaSql" : [ "CREATE SCHEMA IF NOT EXISTS audit", "CREATE TABLE IF NOT EXISTS audit.audit (\n                              JOBID VARCHAR NOT NULL,\n                              PATHS TEXT NOT NULL,\n                              DOMAIN VARCHAR NOT NULL,\n                              SCHEMA VARCHAR NOT NULL,\n                              SUCCESS BOOLEAN NOT NULL,\n                              COUNT BIGINT NOT NULL,\n                              COUNTACCEPTED BIGINT NOT NULL,\n                              COUNTREJECTED BIGINT NOT NULL,\n                              TIMESTAMP TIMESTAMP NOT NULL,\n                              DURATION BIGINT NOT NULL,\n                              MESSAGE VARCHAR NOT NULL,\n                              STEP VARCHAR NOT NULL,\n                              DATABASE VARCHAR,\n                              TENANT VARCHAR\n                             )\n    " ],
  "mainSqlIfExists" : [ "\n          SELECT\n            '{{jobid}}' AS JOBID,\n            '{{paths}}' AS PATHS,\n            '{{domain}}' AS DOMAIN,\n            '{{schema}}' AS SCHEMA,\n            {{success}} AS SUCCESS,\n            {{count}} AS COUNT,\n            {{countAccepted}} AS COUNTACCEPTED,\n            {{countRejected}} AS COUNTREJECTED,\n            TO_TIMESTAMP('{{timestamp}}') AS TIMESTAMP,\n            {{duration}} AS DURATION,\n            '{{message}}' AS MESSAGE,\n            '{{step}}' AS STEP,\n            '{{database}}' AS DATABASE,\n            '{{tenant}}' AS TENANT\n        " ]
}




session = Session.builder.configs(connection_parameters).create()

for statement in audit.get("preActions", []):
    session.sql(statement)
    df: DataFrame = session.sql(query=f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'audit2' AND TABLE_NAME = 'audit2'") 
    rows: List[Row] = df.collect()
    print(rows.__len__())
    if rows.__len__() == 0:
        # execute mainSqlIfNotExists
        sqls: List[str] = audit.get('createSchemaSql', [])
        for sql in sqls:
            print(sql)
            session.sql(sql).show()
session.close()
