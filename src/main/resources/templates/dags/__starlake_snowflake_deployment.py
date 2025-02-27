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
