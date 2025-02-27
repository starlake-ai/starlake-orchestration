from snowflake_dag import deploy_dag
from snowflake.snowpark import Session

import os

options = {
    "account": os.environ['SNOWFLAKE_ACCOUNT'],
    "user": os.environ['SNOWFLAKE_USER'],
    "password": os.environ['SNOWFLAKE_PASSWORD'],
#    "database": os.environ['SNOWFLAKE_DATABASE'],
#    "schema": "os.environ['SNOWFLAKE_SCHEMA']",
#    "warehouse": os.environ['SNOWFLAKE_WAREHOUSE'],
}
session = Session.builder.configs(options).create()
deploy_dag(session, os.environ['SNOWFLAKE_DATABASE'], os.environ['SNOWFLAKE_SCHEMA'])
