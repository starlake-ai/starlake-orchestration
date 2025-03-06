from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark.row import Row
from typing import List, Tuple
import os
from datetime import datetime
import json
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType



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

sl_debug = True
def run_sql(session: Session, sql: str) -> DataFrame:
  my_schema = StructType([StructField("a", IntegerType())])
  if sl_debug:
    print(sql)
    return session.create_dataframe([], schema=my_schema)
  else:
    return session.sql(sql).collect()

def str_to_bool(value: str) -> bool:
    truthy = {'yes', 'y', 'true', '1'}
    falsy = {'no', 'n', 'false', '0'}

    value = value.strip().lower()
    if value in truthy:
        return True
    elif value in falsy:
        return False
    raise ValueError(f"Valeur invalide : {value}")

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
audit = context["audit"]
expectations = context["expectations"]
task = context["task"]
domain = task["domain"]
table = task["table"]
expectation_items = context["expectationItems"]
jobid = task["domain"] + "." + task["table"]

if "options" in metadata:
  options = metadata["options"]
else:
  options = {}

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
    run_sql(session, sql)
    if (sl_is_true(sl_get_option(metadata, "compression", None), True)):
        auto_compress = "TRUE"
    else:
        auto_compress = "FALSE"
    files=context["schema"]["metadata"]["directory"] + '/' + context["schema"]["pattern"].replace(".*", "*")
    sql = f"PUT {files} @{context['tempStage']}/{context['task']['domain']} AUTO_COMPRESS = {auto_compress}"
    run_sql(session, sql)



   
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

def sl_purge_option(metadata: dict) -> str:
  purge = sl_get_option(metadata, "PURGE", None)
  if purge is None:
    purge = "FALSE"
  return purge.upper()

def sl_build_copy_csv(targetTable: str) -> str:
  skipCount = sl_get_option(metadata, "SKIP_HEADER", None)
  purge = sl_purge_option(metadata)

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
    PURGE = {purge}
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
  purge = sl_purge_option(metadata)
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
    PURGE = {purge}
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
  purge = sl_purge_option(metadata)
  common_options = [
      'NULL_IF'
  ]
  copy_extra_options = sl_extra_copy_options(metadata, common_options)
  sql = f'''
    COPY INTO {targetTable} 
    FROM @{context['tempStage']}/{context['task']['domain']} 
    PATTERN = '{schema['pattern']}'
    PURGE = {purge}
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
  
def sl_copy_step(session: Session, targetTable: str):
    sql = sl_build_copy(targetTable)
    run_sql(session, sql)

def sl_copy_two_steps(session: Session):
   pass


###################################################################
###################################################################
###################################################################
###################################################################
###################################################################
###################################################################
###################################################################
###################################################################
###################################################################
###################################################################
###################################################################
###################################################################

params = dict()
def bindParams(stmt: str) -> str:
    return stmt.format_map(params)



def check_if_table_exists(domain: str, schema: str) -> bool:
    sql = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) ILIKE '{domain}.{schema}'"
    df = run_sql(session, sql)
    rows = df.collect()
    return rows.__len__() > 0

def check_if_audit_schema_exists() -> bool:
    if audit:
        try:
            # create SQL domain
            domain = audit.get('domain', ['audit'])[0]
            query=f"CREATE SCHEMA IF NOT EXISTS {domain}"
            run_sql(session, query)
            # execute SQL preActions
            preActions: List[str] = audit.get('preActions', [])
            for sql in preActions:
                stmt: str = bindParams(sql)
                run_sql(session, stmt)
            # check if the audit schema exists
            if not check_if_table_exists(domain, 'audit'):
                # execute SQL createSchemaSql
                sqls: List[str] = audit.get('createSchemaSql', [])
                for sql in sqls:
                    stmt: str = bindParams(sql)
                    run_sql(session, stmt)
                return True
            else:
                return True
        except Exception as e:
            print(f"Error creating audit schema: {str(e)}")
            return False
    else:
        return False

def check_if_expectations_schema_exists() -> bool:
    if expectations:
        try:
            # create SQL domain
            domain = expectations.get('domain', ['audit'])[0]
            query=f"CREATE SCHEMA IF NOT EXISTS {domain}"
            run_sql(session, query)
            # check if the expectations schema exists
            if not check_if_table_exists(domain, 'expectations'):
                # execute SQL createSchemaSql
                sqls: List[str] = expectations.get('createSchemaSql', [])
                for sql in sqls:
                    stmt: str = bindParams(sql)
                    run_sql(session, stmt)
                return True
            else:
                return True
        except Exception as e:
            print(f"Error creating expectations schema: {str(e)}")
            return False
    else:
        return False

def log_audit(domain: str, schema: str, success: bool, duration: int, message: str, ts: datetime, step: str) -> bool :
    if audit:
        audit_domain = audit.get('domain', ['audit'])[0]
        audit_sqls = audit.get('mainSqlIfExists', None)
        if audit_sqls:
            try:
                audit_sql = audit_sqls[0]
                formatted_sql = audit_sql.format(
                    jobid = jobid,
                    paths = schema,
                    domain = domain,
                    schema = schema,
                    success = str(success),
                    count = "-1",
                    countAccepted = "-1",
                    countRejected = "-1",
                    timestamp = ts.strftime("%Y-%m-%d %H:%M:%S"),
                    duration = str(duration),
                    message = message,
                    step = step,
                    database = "",
                    tenant = ""
                )
                insert_sql = f"INSERT INTO {audit_domain}.audit {formatted_sql}"
                run_sql(session, insert_sql)
                return True
            except Exception as e:
                print(f"Error inserting audit record: {str(e)}")
                return False
        else:
            return False
    else:
        return False

def log_expectation(domain: str, schema: str, success: bool, name: str, params: str, sql: str, count: int, exception: str, ts: datetime) -> bool :
    if expectations:
        expectation_domain = expectations.get('domain', ['audit'])[0]
        expectation_sqls = expectations.get('mainSqlIfExists', None)
        if expectation_sqls:
            try:
                expectation_sql = expectation_sqls[0]
                formatted_sql = expectation_sql.format(
                    jobid = jobid,
                    database = "",
                    domain = domain,
                    schema = schema,
                    count = count,
                    exception = exception,
                    timestamp = ts.strftime("%Y-%m-%d %H:%M:%S"),
                    success = str(success),
                    name = name,
                    params = params,
                    sql = sql
                )
                insert_sql = f"INSERT INTO {expectation_domain}.expectations {formatted_sql}"
                run_sql(session, insert_sql)
                return True
            except Exception as e:
                print(f"Error inserting expectations record: {str(e)}")
                return False
        else:
            return False
    else:
        return False

def run_expectation(name: str, params: str, query: str, failOnError: bool = False) -> None:
    count = 0
    try:
        if query:
            stmt: str = bindParams(query)
            df = run_sql(session, stmt)
            rows = df.collect()
            if rows.__len__() != 1:
                raise Exception(f'Expectation failed for {sink}: {query}. Expected 1 row but got {rows.__len__()}')
            count = rows[0][0]
            #  log expectations as audit in expectation table here
            if count != 0:
                raise Exception(f'Expectation failed for {sink}: {query}. Expected count to be equal to 0 but got {count}')
            log_expectation(domain, schema, True, name, params, query, count, "", datetime.now())
        else:
            raise Exception(f'Expectation failed for {sink}: {name}. Query not found')
    except Exception as e:
        print(f"Error running expectation {name}: {str(e)}")
        log_expectation(domain, schema, False, name, params, query, count, str(e), datetime.now())
        if failOnError:
            raise e

session = Session.builder.configs(connection_parameters).create()


try:
    # BEGIN transaction
    run_sql(session, "BEGIN")
    start = datetime.now()
    sl_put_to_stage(session)
    if task["steps"] == "1":
      for sql in task["createTable"]:
        stmt: str = bindParams(sql)
        run_sql(session, stmt)
      if task["writeStrategy"] == "WRITE_TRUNCATE":
        sql = f"TRUNCATE TABLE {task['targetTableName']}"
        run_sql(session, sql)
      sl_copy_step(session, task['targetTableName'])
    elif task["steps"] == "2":
      for sql in task["firstStep"]:
        stmt: str = bindParams(sql)
        run_sql(session, stmt)
      if task["writeStrategy"] == "WRITE_TRUNCATE":
        sql = f"TRUNCATE TABLE {task['targetTableName']}"
        run_sql(session, sql)
      sl_copy_step(session, task['tempTableName'])
      second_step = task.get("secondStep", None)
      # execute preSqls
      preSqls: List[str] = schema.get('presql', [])
      for sql in preSqls:
          stmt: str = bindParams(sql)
          run_sql(session, stmt)
      if check_if_table_exists(domain, table):
        # execute addSCD2ColumnsSqls only if table exists
        scd2_sqls: List[str] = schema.get('addSCD2ColumnsSqls', [])
        for sql in scd2_sqls:
            stmt: str = bindParams(sql)
            run_sql(session, stmt)
        sqls = second_step["mainSqlIfExists"]
      else:
        # addSCD2ColumnsSqls is in the mainSqlIfNotExists sql list if required
        sqls = second_step["mainSqlIfNotExists"]
      for sql in sqls:
          stmt: str = bindParams(sql)
          run_sql(session, stmt)
      drop_first_step = task["dropFirstStep"]
      stmt: str = bindParams(drop_first_step)
      run_sql(session, stmt)
    else:
        raise ValueError(f"Unsupported steps value: {task['steps']}")
    # execute postSqls
    postSqls: List[str] = schema.get('postsql', [])
    for sql in postSqls:
        stmt: str = bindParams(sql)
        run_sql(session, stmt)

    # run expectations
    if expectation_items and check_if_expectations_schema_exists():
        for expectation in expectation_items:
            run_expectation(expectation.get("name", None), expectation.get("params", None), expectation.get("query", None), str_to_bool(expectation.get('failOnError', 'no')))

    # COMMIT transaction
    run_sql(session, "COMMIT")
    end = datetime.now()
    duration = (end - start).total_seconds()
    print(f"Duration in seconds: {duration}")
    if audit and check_if_audit_schema_exists():
        print("Audit schema exists")
        # insert audit record
        if log_audit(domain, schema, True, duration, 'Success', end, "LOAD"):
            print("Audit record inserted")
        else:
            print("Error inserting audit record")
    else:
        print("Audit schema does not exist")
    
except Exception as e:
    # ROLLBACK transaction
    run_sql(session, "ROLLBACK")
    end = datetime.now()
    duration = (end - start).total_seconds()
    error_message = str(e)
    print(f"Duration in seconds: {duration}")
    print(f"Error: {error_message}")
    if audit and check_if_audit_schema_exists():
        print("Audit schema exists")
        # insert audit record
        if log_audit(domain, schema, False, duration, error_message, end, "LOAD"):
            print("Audit record inserted")
        else:
            print("Error inserting audit record")
    else:
        print("Audit schema does not exist")
    raise e
