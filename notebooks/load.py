from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark.row import Row
from typing import List, Tuple
import os
import datetime
import json



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
json_context = '''{
  "schema" : {
    "finalName" : "flat_locations",
    "name" : "flat_locations",
    "attributes" : [ {
      "name" : "id",
      "rename" : "id",
      "array" : false,
      "privacy" : "NONE",
      "type" : "string",
      "required" : true
    }, {
      "name" : "city",
      "rename" : "city",
      "array" : false,
      "privacy" : "NONE",
      "type" : "string",
      "required" : true
    }, {
      "name" : "country",
      "rename" : "country",
      "array" : false,
      "privacy" : "NONE",
      "type" : "string",
      "required" : true
    } ],
    "expectations" : [ {
      "expectQuery" : "is_col_value_not_unique('id')",
      "expectFailOnError" : false
    } ],
    "pattern" : "flat_locations-.*.json",
    "primaryKey" : [ "id" ],
    "acl" : [ {
      "aceRole" : "viewer",
      "aceGrants" : "user:me@me.com,user:you@me.com"
    }, {
      "aceRole" : "owner",
      "aceGrants" : "user:me@you.com,user:you@you.com"
    } ],
    "metadata" : {
      "format" : "JSON",
      "multiline" : false,
      "separator" : ";",
      "encoding" : "UTF-8",
      "quote" : "\\"",
      "escape" : "\\\\",
      "emptyIsNull" : true,
      "withHeader" : true,
      "directory" : "/Users/hayssams/git/public/starlake/samples/spark/incoming/hr",
      "array" : false
    }
  },
  "fileSystem" : "file://",
  "sink" : {
    "sinkFormat" : "parquet",
    "sinkConnectionRef" : "snowflake"
  },
  "sl_project_id" : "-1",
  "sl_project_name" : "[noname]",
  "statements" : {
    "preActions" : [ "USE SCHEMA hr" ],
    "domain" : [ "hr" ],
    "mainSqlIfNotExists" : [ "CREATE TABLE hr.flat_locations  AS SELECT id, city, country\\n  FROM (\\n    SELECT id, city, country\\n    FROM hr.flat_locations\\n  ) AS SL_INTERNAL_FROM_SELECT;" ],
    "mainSqlIfExists" : [ "TRUNCATE TABLE hr.flat_locations", "INSERT INTO hr.flat_locations SELECT id, city, country\\n  FROM (\\n    SELECT id, city, country\\n    FROM hr.flat_locations\\n  ) AS SL_INTERNAL_FROM_SELECT" ],
    "table" : [ "flat_locations" ],
    "connectionType" : [ "JDBC" ]
  },
  "acl" : [ "GRANT viewer ON TABLE hr.flat_locations TO USER me@me.com", "GRANT viewer ON TABLE hr.flat_locations TO USER you@me.com", "GRANT owner ON TABLE hr.flat_locations TO USER me@you.com", "GRANT owner ON TABLE hr.flat_locations TO USER you@you.com" ],
  "tempStage" : "starlake_load_stage_p3j7BgCn6Y",
  "expectationItems" : [ {
    "name" : "is_col_value_not_unique",
    "params" : "'id'",
    "query" : "WITH SL_THIS AS (SELECT * FROM hr.flat_locations)\\nSELECT COALESCE(max(cnt), 0)\\n    FROM (SELECT id, count(*) as cnt FROM sl_this GROUP BY id) AS COL_COUNT",
    "failOnError" : "no"
  } ],
  "task" : {
    "format" : "JSON",
    "pattern" : "flat_locations-.*.json",
    "domain" : "hr",
    "writeStrategy" : "WRITE_TRUNCATE",
    "createTable" : [ "CREATE SCHEMA IF NOT EXISTS hr", "CREATE TABLE IF NOT EXISTS hr.flat_locations (id STRING, city STRING, country STRING) " ],
    "incomingDir" : "/Users/hayssams/git/public/starlake/samples/spark/incoming/hr",
    "steps" : "1",
    "targetTableName" : "hr.flat_locations",
    "table" : "flat_locations"
  },
  "audit" : {
    "preActions" : [ "USE SCHEMA audit" ],
    "domain" : [ "audit" ],
    "createSchemaSql" : [ "CREATE SCHEMA IF NOT EXISTS audit", "CREATE TABLE IF NOT EXISTS audit.audit (\\n                              JOBID VARCHAR NOT NULL,\\n                              PATHS TEXT NOT NULL,\\n                              DOMAIN VARCHAR NOT NULL,\\n                              SCHEMA VARCHAR NOT NULL,\\n                              SUCCESS BOOLEAN NOT NULL,\\n                              COUNT BIGINT NOT NULL,\\n                              COUNTACCEPTED BIGINT NOT NULL,\\n                              COUNTREJECTED BIGINT NOT NULL,\\n                              TIMESTAMP TIMESTAMP NOT NULL,\\n                              DURATION BIGINT NOT NULL,\\n                              MESSAGE VARCHAR NOT NULL,\\n                              STEP VARCHAR NOT NULL,\\n                              DATABASE VARCHAR,\\n                              TENANT VARCHAR\\n                             )\\n    " ],
    "mainSqlIfExists" : [ "\\n          SELECT\\n            '{jobid}' AS JOBID,\\n            '{paths}' AS PATHS,\\n            '{domain}' AS DOMAIN,\\n            '{schema}' AS SCHEMA,\\n            {success} AS SUCCESS,\\n            {count} AS COUNT,\\n            {countAccepted} AS COUNTACCEPTED,\\n            {countRejected} AS COUNTREJECTED,\\n            TO_TIMESTAMP('{timestamp}') AS TIMESTAMP,\\n            {duration} AS DURATION,\\n            '{message}' AS MESSAGE,\\n            '{step}' AS STEP,\\n            '{database}' AS DATABASE,\\n            '{tenant}' AS TENANT\\n        " ],
    "table" : [ "audit" ],
    "connectionType" : [ "JDBC" ]
  },
  "schedules" : [ {
    "schedule" : "0 0 * * *",
    "cron" : "cron1",
    "domains" : [ {
      "name" : "hr",
      "finalName" : "hr",
      "tables" : [ {
        "name" : "flat_locations",
        "finalName" : "flat_locations"
      } ]
    } ]
  } ],
  "expectations" : {
    "domain" : [ "audit" ],
    "createSchemaSql" : [ "CREATE TABLE IF NOT EXISTS audit.expectations (\\n                            JOBID VARCHAR NOT NULL,\\n                            DATABASE VARCHAR,\\n                            DOMAIN VARCHAR NOT NULL,\\n                            SCHEMA VARCHAR NOT NULL,\\n                            TIMESTAMP TIMESTAMP NOT NULL,\\n                            NAME VARCHAR NOT NULL,\\n                            PARAMS VARCHAR NOT NULL,\\n                            SQL VARCHAR NOT NULL,\\n                            COUNT BIGINT NOT NULL,\\n                            EXCEPTION VARCHAR NOT NULL,\\n                            SUCCESS BOOLEAN NOT NULL\\n                          )\\n        " ],
    "mainSqlIfExists" : [ "\\n          SELECT\\n            '{jobid}' AS JOBID,\\n            '{database}' AS DATABASE,\\n            '{domain}' AS DOMAIN,\\n            '{schema}' AS SCHEMA,\\n            TO_TIMESTAMP('{timestamp}') AS TIMESTAMP,\\n            '{name}' AS NAME,\\n            '{params}' AS PARAMS,\\n            '{sql}' AS SQL,\\n            {count} AS COUNT,\\n            '{exception}' AS EXCEPTION,\\n            {success} AS SUCCESS\\n        " ],
    "table" : [ "expectations" ],
    "connectionType" : [ "JDBC" ]
  },
  "config" : {
    "template" : "load/airflow__scheduled_table__shell.py.j2",
    "options" : [ {
      "name" : "load_dependencies",
      "value" : "true"
    }, {
      "name" : "SL_TIMEZONE",
      "value" : "Europe/Paris"
    } ],
    "comment" : "sample dag configuration"
  },
  "sl_airflow_access_control" : "None"
}'''

def sl_is_true(value: str, default: bool) -> bool:
   if value is None:
      return default
   return value.lower() == "true"

def sl_get_option(metadata: dict, key: str, metadata_key: str) -> str:
  if "options" in metadata:
    options = metadata["options"]
    if key in options:
        return options[key.lower()]
  if metadata_key is not None and metadata[metadata_key] is not None:
    return metadata[metadata_key]
  return None

context = json.loads(json_context)
schema = context["schema"]
metadata = schema["metadata"]

if "options" in metadata:
  options = metadata["options"]
else:
  options = {}

task = context["task"]
compression = sl_is_true(sl_get_option(metadata, "compression", None), True)
if compression:
  compression_format = "COMPRESSION = GZIP" 
else:
  compression_format = "COMPRESSION = NONE"


null_if = sl_get_option(metadata, 'NULL_IF', None)
if null_if is None and metadata['emptyIsNull'] is not None and metadata['emptyIsNull']:
  null_if = f"('')"
if null_if is not None:
  null_if = f"NULL_IF = {null_if}"
else:
  null_if = ""


def sl_put_to_stage(session: Session):
  if context["fileSystem"] == 'file://':
    sql = f"CREATE TEMPORARY STAGE IF NOT EXISTS {context['tempStage']}"
    print(sql)
    #session.sql(sql).collect()
    if (sl_is_true(sl_get_option(metadata, "compression", None), True)):
        auto_compress = "TRUE"
    else:
        auto_compress = "FALSE"
    files=context["schema"]["metadata"]["directory"] + '/' + context["schema"]["pattern"].replace(".*", "*")
    sql = f"PUT {files} @{context['tempStage']}/{context['task']['domain']} AUTO_COMPRESS = {auto_compress}"
    print(sql)
    #session.sql(sql).collect()

def sl_copy(session: Session):
   if (task['steps'] == '1'):
      sl_one_step(session)
   else:
      sl_two_steps(session)


   
def sl_extra_copy_options(metadata: dict, common_options: list[str]) -> str:
  copy_extra_options = ""
  if options is not None:
    for k, v in options.items():
      if v in common_options:
        options.remove(k)

  copy_extra_options = ""
  if options is not None:
    for k, v in options.items():
      copy_extra_options += f"{k} = {v}\n"
  return copy_extra_options


def sl_build_copy_csv(targetTable: str) -> str:
  skipCount = sl_get_option(metadata, "SKIP_HEADER", None)
  if skipCount is None and metadata['withHeader']:
    skipCount = '1'
    common_options = [
      'SKIP_HEADER', 
      'NULL_IF', 
      'FIELD_OPTIONALLY_ENCLOSED_BY', 
      'FIELD_DELIMITER',
      'ESCAPE_UNENCLOSED_FIELD', 
      'ENCODING'
  ]
  copy_extra_options = sl_extra_copy_options(metadata, common_options)
  sql = f'''
    COPY INTO {targetTable} 
    FROM @{context['tempStage']}/{context['task']['domain']} 
    PATTERN = '{schema['pattern']}'
    FILE_FORMAT = (
      TYPE = CSV
      SKIP_HEADER = {skipCount} 
      FIELD_OPTIONALLY_ENCLOSED_BY = '{sl_get_option(metadata, 'FIELD_OPTIONALLY_ENCLOSED_BY', 'quote')}' 
      FIELD_DELIMITER = '{sl_get_option(metadata, 'FIELD_DELIMITER', 'separator')}' 
      ESCAPE_UNENCLOSED_FIELD = '{sl_get_option(metadata, 'ESCAPE_UNENCLOSED_FIELD', 'escape')}' 
      ENCODING = '{sl_get_option(metadata, 'ENCODING', 'encoding')}'
      {null_if}
      {copy_extra_options}
      {compression_format}
    )
  '''
  return sql


def sl_build_copy_json(targetTable: str) -> str:
  strip_outer_array = sl_get_option(metadata, "STRIP_OUTER_ARRAY", 'array')

  common_options = [
      'STRIP_OUTER_ARRAY', 
      'NULL_IF'
  ]
  copy_extra_options = sl_extra_copy_options(metadata, common_options)
  sql = f'''
    COPY INTO {targetTable} 
    FROM @{context['tempStage']}/{context['task']['domain']} 
    PATTERN = '{schema['pattern']}'
    FILE_FORMAT = (
      TYPE = JSON
      STRIP_OUTER_ARRAY = {strip_outer_array}
      {null_if}
      {copy_extra_options}
      {compression_format}
    )
  '''
  return sql
   
def sl_build_copy_other(targetTable: str, format: str) -> str:
  common_options = [
      'NULL_IF'
  ]
  copy_extra_options = sl_extra_copy_options(metadata, common_options)
  sql = f'''
    COPY INTO {targetTable} 
    FROM @{context['tempStage']}/{context['task']['domain']} 
    PATTERN = '{schema['pattern']}'
    FILE_FORMAT = (
      TYPE = {format}
      {null_if}
      {copy_extra_options}
      {compression_format}
    )
  '''
  return sql


def sl_build_copy(targetTable: str) -> str:
  if metadata['format'] == 'CSV':
    return sl_build_copy_csv(targetTable)
  elif metadata['format'] == 'JSON':
    return sl_build_copy_json(targetTable)
  elif metadata['format'] == 'PARQUET':
    return sl_build_copy_other(targetTable)
  elif metadata['format'] == 'XML':
    return sl_build_copy_other(targetTable)
  else:
    raise ValueError(f"Unsupported format {metadata['format']}")
  
def sl_one_step(session: Session):
    sql = sl_build_copy(task['targetTableName'])
    print(sql)
    #session.sql(sql).collect()

def sl_two_steps(session: Session):
   pass

session = Session.builder.configs(connection_parameters).create()
try:
#   sl_main(session)
   print("start")
   sl_put_to_stage(session)
   sl_copy(session)
   print("end")
finally:
   session.close()



