from ai.starlake.job import StarlakeOrchestrator
orchestrator = StarlakeOrchestrator.SNOWFLAKE

from ai.starlake.job import StarlakeExecutionEnvironment
execution_environment = StarlakeExecutionEnvironment.SQL

description="""sample dag configuration"""

template="load/snowflake__scheduled_table__sql.py.j2"

access_control = None

options={
    'sl_env_var':'{"SL_ROOT": ".", "SL_ENV": "SNOWFLAKE"}', 
    'tags':'starlake', 
    'pre_load_strategy':'none', 
    'retries':'1',
    'retry_delay':'30',
    'stage_location':'staging',
    'schema': 'starbake',
    'warehouse':'COMPUTE_WH',
}

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeJobFactory

import os

sl_job = StarlakeJobFactory.create_job(
    filename=os.path.basename(__file__), 
    module_name=f"{__name__}", 
    orchestrator=orchestrator,
    execution_environment=execution_environment,
    options=options
)

from ai.starlake.orchestration import StarlakeSchedule, StarlakeDomain, StarlakeTable, OrchestrationFactory

schedules= [
    StarlakeSchedule(
        name='daily', 
        cron='0 0 * * *', 
        domains=[
            StarlakeDomain(
                name='sales', 
                final_name='sales', 
                tables=[
                    StarlakeTable(
                        name='customers', 
                        final_name='customers'
                    ),
                ]
            )
    ]),
]

json_context = '''{
  "sales.customers" : {
    "schema" : {
      "finalName" : "customers",
      "name" : "customers",
      "attributes" : [ {
        "name" : "id",
        "rename" : "id",
        "ignore" : "false",
        "array" : "false",
        "privacy" : "NONE",
        "type" : "string",
        "required" : "false"
      }, {
        "name" : "signup",
        "rename" : "signup",
        "ignore" : "false",
        "array" : "false",
        "privacy" : "NONE",
        "type" : "timestamp",
        "required" : "false"
      }, {
        "name" : "contact",
        "rename" : "contact",
        "ignore" : "false",
        "array" : "false",
        "privacy" : "NONE",
        "type" : "string",
        "required" : "false"
      }, {
        "name" : "birthdate",
        "rename" : "birthdate",
        "ignore" : "false",
        "array" : "false",
        "privacy" : "NONE",
        "type" : "date",
        "required" : "false"
      }, {
        "name" : "name1",
        "rename" : "name1",
        "ignore" : "false",
        "array" : "false",
        "privacy" : "NONE",
        "type" : "string",
        "required" : "false"
      }, {
        "name" : "name2",
        "rename" : "name2",
        "ignore" : "false",
        "array" : "false",
        "privacy" : "NONE",
        "type" : "string",
        "required" : "false"
      }, {
        "name" : "id1",
        "rename" : "id1",
        "array" : "false",
        "privacy" : "NONE",
        "script" : "substring(id, 1, 1)",
        "type" : "string",
        "required" : "false"
      } ],
      "pattern" : "customers.*.psv",
      "metadata" : {
        "format" : "DSV",
        "multiline" : "false",
        "separator" : "|",
        "encoding" : "UTF-8",
        "quote" : "\\"",
        "escape" : "\\\\",
        "emptyIsNull" : "true",
        "withHeader" : "true",
        "directory" : "/Users/hayssams/git/public/starlake/samples/spark/incoming/sales",
        "array" : "false"
      }
    },
    "fileSystem" : "file://",
    "sink" : {
      "sinkFormat" : "parquet",
      "sinkConnectionRef" : "snowflake"
    },
    "tempStage" : "starlake_load_stage_e7QLl5UBYs"
  },
  "sl_project_id" : "-1",
  "sl_project_name" : "[noname]",
  "config" : {
    "template" : "load/airflow__scheduled_table__shell.py.j2",
    "options" : [ {
      "name" : "run_dependencies",
      "value" : "true"
    }, {
      "name" : "SL_TIMEZONE",
      "value" : "Europe/Paris"
    } ],
    "comment" : "sample dag configuration"
  },
  "sl_airflow_access_control" : "None"
}'''

statements = {
  "sales.customers" : {
    "format" : "DSV",
    "secondStep" : {
      "name" : "sales.customers",
      "preActions" : [ "USE SCHEMA sales" ],
      "domain" : [ "sales" ],
      "mainSqlIfNotExists" : [ "CREATE TABLE sales.customers  AS\nSELECT  id,signup,contact,birthdate,name1,name2,id1\nFROM (SELECT id, signup, contact, birthdate, name1, name2, id1\n  FROM (\n    SELECT id, signup, contact, birthdate, name1, name2, substring(id, 1, 1) AS id1\n    FROM sales.zztmp_customers_6a68ab9ac35d4e1592df5bc1cc7a9f35\n  ) AS SL_INTERNAL_FROM_SELECT)\nQUALIFY ROW_NUMBER() OVER (PARTITION BY  id ORDER BY signup DESC) = 1;" ],
      "mainSqlIfExists" : [ "MERGE INTO  sales.customers SL_EXISTING \nUSING (\n    SELECT  id,signup,contact,birthdate,name1,name2,id1\n    FROM (SELECT id, signup, contact, birthdate, name1, name2, id1\n  FROM (\n    SELECT id, signup, contact, birthdate, name1, name2, substring(id, 1, 1) AS id1\n    FROM sales.zztmp_customers_6a68ab9ac35d4e1592df5bc1cc7a9f35\n  ) AS SL_INTERNAL_FROM_SELECT) SL_TMP1\n    QUALIFY ROW_NUMBER() OVER (PARTITION BY id  ORDER BY signup DESC) = 1\n) SL_INCOMING \nON ( SL_INCOMING.id = SL_EXISTING.id)\nWHEN MATCHED AND SL_INCOMING.signup > SL_EXISTING.signup THEN  UPDATE SET id = SL_INCOMING.id,signup = SL_INCOMING.signup,contact = SL_INCOMING.contact,birthdate = SL_INCOMING.birthdate,name1 = SL_INCOMING.name1,name2 = SL_INCOMING.name2,id1 = SL_INCOMING.id1\nWHEN NOT MATCHED THEN INSERT (id,signup,contact,birthdate,name1,name2,id1) VALUES (SL_INCOMING.id,SL_INCOMING.signup,SL_INCOMING.contact,SL_INCOMING.birthdate,SL_INCOMING.name1,SL_INCOMING.name2,SL_INCOMING.id1)" ],
      "table" : [ "customers" ],
      "connectionType" : [ "JDBC" ]
    },
    "pattern" : "customers.*.psv",
    "schemaString" : "\"id\" STRING, \"signup\" TIMESTAMP, \"contact\" STRING, \"birthdate\" DATE, \"name1\" STRING, \"name2\" STRING, \"id1\" STRING",
    "domain" : "sales",
    "firstStep" : [ "CREATE SCHEMA IF NOT EXISTS sales", "CREATE TABLE IF NOT EXISTS sales.zztmp_customers_6a68ab9ac35d4e1592df5bc1cc7a9f35 (id STRING, signup TIMESTAMP, contact STRING, birthdate DATE, name1 STRING, name2 STRING, id1 STRING) " ],
    "writeStrategy" : "WRITE_APPEND",
    "dropFirstStep" : "DROP TABLE IF EXISTS sales.zztmp_customers_6a68ab9ac35d4e1592df5bc1cc7a9f35;",
    "tempTableName" : "sales.zztmp_customers_6a68ab9ac35d4e1592df5bc1cc7a9f35",
    "incomingDir" : "/Users/hayssams/git/public/starlake/samples/spark/incoming/sales",
    "steps" : "2",
    "table" : "customers",
    "extraFileNameColumn" : [ "ALTER TABLE sales.zztmp_customers_6a68ab9ac35d4e1592df5bc1cc7a9f35 ADD COLUMN sl_input_file_name STRING DEFAULT '{{sl_input_file_name}}';" ],
    "targetTableName" : "sales.customers"
  }
}

expectation_items = { }

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

    def generate_pipeline(schedule: StarlakeSchedule):
        # generate the load pipeline
        with orchestration.sl_create_pipeline(
            schedule=schedule,
        ) as pipeline:

            schedule       = pipeline.schedule
            schedule_name  = pipeline.schedule_name

            start = pipeline.start_task()
            if not start:
                raise Exception("Start task not defined")

            pre_tasks = pipeline.pre_tasks()

            if pre_tasks:
                start >> pre_tasks

            def generate_load_domain(domain: StarlakeDomain):

                from ai.starlake.common import sanitize_id

                if schedule_name:
                    name = f"{domain.name}_{schedule_name}"
                else:
                    name = domain.name

                with orchestration.sl_create_task_group(group_id=sanitize_id(name), pipeline=pipeline) as ld:

                    pre_load_strategy=pipeline.pre_load_strategy

                    def pre_load(pre_load_strategy: StarlakePreLoadStrategy):
                        if pre_load_strategy != StarlakePreLoadStrategy.NONE:
                            with orchestration.sl_create_task_group(group_id=sanitize_id(f'pre_load_{name}'), pipeline=pipeline) as pre_load_tasks:
                                pre_load = pipeline.sl_pre_load(
                                        domain=domain.name, 
                                        tables=set([table.name for table in domain.tables]), 
                                    )
                                skip_or_start = pipeline.skip_or_start(
                                    task_id=f'skip_or_start_loading_{name}', 
                                    upstream_task=pre_load
                                )
                                if pre_load_strategy == StarlakePreLoadStrategy.IMPORTED:
                                    sl_import = pipeline.sl_import(
                                            task_id=f"import_{name}",
                                            domain=domain.name, 
                                            tables=set([table.name for table in domain.tables]), 
                                        )
                                else:
                                    sl_import = None

                                if skip_or_start:
                                    pre_load >> skip_or_start
                                    if sl_import:
                                        skip_or_start >> sl_import
                                elif sl_import:
                                    pre_load >> sl_import

                            return pre_load_tasks
                        else:
                            return None

                    pld = pre_load(pre_load_strategy)                              

                    def load_domain_tables():
                        with orchestration.sl_create_task_group(group_id=sanitize_id(f'load_{name}'), pipeline=pipeline) as load_domain_tables:
                            for table in domain.tables:
                                pipeline.sl_load(
                                    task_id=sanitize_id(f'load_{domain.name}_{table.name}'), 
                                    domain=domain.name, 
                                    table=table.name,
                                )

                        return load_domain_tables

                    ld_tables=load_domain_tables()

                    if pld:
                        pld >> ld_tables

                return ld

            load_domains = [generate_load_domain(domain) for domain in schedule.domains]

            end = pipeline.end_task()

            if pre_tasks:
                start >> pre_tasks >> load_domains
            else:
                start >> load_domains

            end << load_domains

            post_tasks = pipeline.post_tasks()
        
            if post_tasks:
                all_done = pipeline.sl_dummy_op(task_id="all_done")
                all_done << load_domains
                all_done >> post_tasks >> end

        return pipeline

    pipelines = [generate_pipeline(schedule) for schedule in schedules]
