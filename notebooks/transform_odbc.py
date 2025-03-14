from typing import List, Optional, Tuple, Union
from collections import defaultdict
import os
import psycopg2
######################

#parameters


options = {}


######################


connection_parameters = {
   "account": os.environ['SNOWFLAKE_ACCOUNT'],
   "user": os.environ['SNOWFLAKE_USER'],
   "password": os.environ['SNOWFLAKE_PASSWORD'],
#   "role": "<your snowflake role>",  # optional
   "warehouse": os.environ['SNOWFLAKE_WAREHOUSE'],  # optional
#   "schema": "<your snowflake schema>",  # optional
   "database": os.environ['SNOWFLAKE_DB'],  # optional
   "port": 5432,
}



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

safe_params = defaultdict(lambda: 'NULL', options)


class Session:
    def __init__(self, connection_parameters):
        self.conn = psycopg2.connect(database = connection_parameters['database'], 
                        user = connection_parameters['user'], 
                        host= connection_parameters['host'],
                        password = connection_parameters['password'],
                        port = connection_parameters['port'])

    def close(self):
        self.conn.close()

    def sql(self, stmt: str) -> List[Tuple]:
        cur = self.conn.cursor()
        cur.execute(stmt)
        if (stmt.lower().startswith("select")) or (stmt.lower().startswith("with")):
            result = self.cur.fetchall()
        else:
            result = []
        cur.close()
        return result
    
    def commit(self):
        self.conn.commit()
        return []
    
    def rollback(self):
        self.conn.rollback()
        return []
    



def execute_sql(session: Session, sql: Optional[str], message: Optional[str] = None, dry_run: bool = False):
    """Execute the SQL.
    Args:
        session (Session): The Snowflake session.
        sql (str): The SQL query to execute.
        message (Optional[str], optional): The optional message. Defaults to None.
        dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
    Returns:
        List[Row]: The rows.
    """
    if sql:
        if dry_run and message:
            print(f"# {message}")
        stmt: str = bindParams(sql)
        if dry_run:
            print(f"{stmt};")
            return []
        else:
            try:
                if (stmt.lower().startswith("rollback")):
                    session.rollback()
                elif (stmt.lower().startswith("commit")):
                    session.commit()
                else:
                    rows = session.sql(stmt)
                return rows
            except Exception as e:
                raise Exception(f"Error executing SQL {stmt}: {str(e)}")
    else:
        return []

def execute_sqls(session: Session, sqls: List[str], message: Optional[str] = None, dry_run: bool = False) -> None:
    """Execute the SQLs.
    Args:
        session (Session): The Snowflake session.
        sqls (List[str]): The SQLs.
        message (Optional[str], optional): The optional message. Defaults to None.
        dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
    """
    if sqls:
        if dry_run and message:
            print(f"# {message}")
        for sql in sqls:
            execute_sql(session, sql, None, dry_run)

def bindParams(stmt: str) -> str:
    """Bind parameters to the SQL statement.
    Args:
        stmt (str): The SQL statement.
    Returns:
        str: The SQL statement with the parameters bound
    """
    return stmt.format_map(safe_params)

def str_to_bool(value: str) -> bool:
    """Convert a string to a boolean.
    Args:
        value (str): The string to convert.
    Returns:
        bool: The boolean value.
    """
    truthy = {'yes', 'y', 'true', '1'}
    falsy = {'no', 'n', 'false', '0'}

    value = value.strip().lower()
    if value in truthy:
        return True
    elif value in falsy:
        return False
    raise ValueError(f"Valeur invalide : {value}")


def check_if_table_exists(session: Session, domain: str, table: str) -> bool:
    """Check if the table exists.
    Args:
        session (Session): The Snowflake session.
        domain (str): The domain.
        table (str): The table.
        Returns:
        bool: True if the table exists, False otherwise.
    """
    query=f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) ILIKE '{domain}.{table}'"
    return execute_sql(session, query, f"Check if table {domain}.{table} exists:", False).__len__() > 0

def check_if_audit_table_exists(session: Session, dry_run: bool = False) -> bool:
    """Check if the audit table exists.
    Args:
        session (Session): The Snowflake session.
        Returns:
        bool: True if the audit table exists, False otherwise.
        dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
    """
    if audit:
        try:
            # create SQL domain
            domain = audit.get('domain', ['audit'])[0]
            create_domain_if_not_exists(session, domain, dry_run)
            # execute SQL preActions
            execute_sqls(session, audit.get('preActions', []), "Execute audit pre action:", dry_run)
            # check if the audit table exists
            if not check_if_table_exists(session, domain, 'audit'):
                # execute SQL createSchemaSql
                sqls: List[str] = audit.get('createSchemaSql', [])
                if sqls:
                    execute_sqls(session, sqls, "Create audit table", dry_run)
                return True
            else:
                return True
        except Exception as e:
            print(f"Error creating audit table: {str(e)}")
            return False
    else:
        return False

def check_if_expectations_table_exists(session: Session, dry_run: bool = False) -> bool:
    """Check if the expectations table exists.
    Args:
        session (Session): The Snowflake session.
        Returns:
        bool: True if the expectations table exists, False otherwise.
        dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
    """
    if expectations:
        try:
            # create SQL domain
            domain = expectations.get('domain', ['audit'])[0]
            create_domain_if_not_exists(session, domain, dry_run)
            # check if the expectations table exists
            if not check_if_table_exists(session, domain, 'expectations'):
                # execute SQL createSchemaSql
                execute_sqls(session, expectations.get('createSchemaSql', []), "Create expectations table", dry_run)
                return True
            else:
                return True
        except Exception as e:
            print(f"Error creating expectations table: {str(e)}")
            return False
    else:
        return False

from datetime import datetime

def log_audit(session: Session, model_name: str, paths: Optional[str], count: int, countAccepted: int, countRejected: int, success: bool, duration: int, message: str, ts: datetime, jobid: Optional[str] = None, step: Optional[str] = None, dry_run: bool = False) -> bool :
    """Log the audit record.
    Args:
        session (Session): The Snowflake session.
        count (int): The count.
        countAccepted (int): The count accepted.
        countRejected (int): The count rejected.
        success (bool): The success.
        duration (int): The duration.
        message (str): The message.
        ts (datetime): The timestamp.
        jobid (Optional[str], optional): The optional job id. Defaults to None.
        step (Optional[str], optional): The optional step. Defaults to None.
        dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
    Returns:
        bool: True if the audit record was logged, False otherwise.
    """
    if audit and check_if_audit_table_exists(session, dry_run):
        audit_domain = audit.get('domain', ['audit'])[0]
        audit_sqls = audit.get('mainSqlIfExists', None)
        if audit_sqls:
            try:
                domain_and_table = model_name.split('.')
                domain = domain_and_table[0]
                table = domain_and_table[1]
                audit_sql = audit_sqls[0]
                formatted_sql = audit_sql.format(
                    jobid = jobid or f'{domain}.{table}',
                    paths = paths or table,
                    domain = domain,
                    schema = table,
                    success = str(success),
                    count = str(count),
                    countAccepted = str(countAccepted),
                    countRejected = str(countRejected),
                    timestamp = ts.strftime("%Y-%m-%d %H:%M:%S"),
                    duration = str(duration),
                    message = message,
                    step = step or "TRANSFORM",
                    database = "",
                    tenant = ""
                )
                insert_sql = f"INSERT INTO {audit_domain}.audit {formatted_sql}"
                execute_sql(session, insert_sql, "Insert audit record:", dry_run)
                return True
            except Exception as e:
                print(f"Error inserting audit record: {str(e)}")
                return False
        else:
            return False
    else:
        return False

def log_expectation(session: Session, model_name: str, success: bool, name: str, params: str, sql: str, count: int, exception: str, ts: datetime, jobid: Optional[str] = None, dry_run: bool = False) -> bool :
    """Log the expectation record.
    Args:
        session (Session): The Snowflake session.
        success (bool): whether the expectation has been successfully checked or not.
        name (str): The name of the expectation.
        params (str): The params for the expectation.
        sql (str): The SQL.
        count (int): The count.
        exception (str): The exception.
        ts (datetime): The timestamp.
        jobid (Optional[str], optional): The optional job id. Defaults to None.
        dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
    Returns:
        bool: True if the expectation record was logged, False otherwise.
    """
    if expectations and check_if_expectations_table_exists(session, dry_run):
        expectation_domain = expectations.get('domain', ['audit'])[0]
        expectation_sqls = expectations.get('mainSqlIfExists', None)
        if expectation_sqls:
            try:
                domain_and_table = model_name.split('.')
                domain = domain_and_table[0]
                table = domain_and_table[1]
                expectation_sql = expectation_sqls[0]
                formatted_sql = expectation_sql.format(
                    jobid = jobid or f'{domain}.{table}',
                    database = "",
                    domain = domain,
                    schema = table,
                    count = count,
                    exception = exception,
                    timestamp = ts.strftime("%Y-%m-%d %H:%M:%S"),
                    success = str(success),
                    name = name,
                    params = params,
                    sql = sql
                )
                insert_sql = f"INSERT INTO {expectation_domain}.expectations {formatted_sql}"
                execute_sql(session, insert_sql, "Insert expectations record:", dry_run)
                return True
            except Exception as e:
                print(f"Error inserting expectations record: {str(e)}")
                return False
        else:
            return False
    else:
        return False

def run_expectation(session: Session,  model_name: str, name: str, params: str, query: str, failOnError: bool = False, jobid: Optional[str] = None, dry_run: bool = False) -> None:
    """Run the expectation.
    Args:
        session (Session): The Snowflake session.
        name (str): The name of the expectation.
        params (str): The params for the expectation.
        query (str): The query.
        failOnError (bool, optional): Whether to fail on error. Defaults to False.
        jobid (Optional[str], optional): The optional job id. Defaults to None.
        dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
    """
    count = 0
    try:
        if query:
            rows = execute_sql(session, query, f"Run expectation {name}:", dry_run)
            if rows.__len__() != 1:
                if not dry_run:
                    raise Exception(f'Expectation failed for {model_name}: {query}. Expected 1 row but got {rows.__len__()}')
            else:
                count = rows[0][0]
            #  log expectations as audit in expectation table here
            if count != 0:
                raise Exception(f'Expectation failed for {model_name}: {query}. Expected count to be equal to 0 but got {count}')
            log_expectation(session, True, name, params, query, count, "", datetime.now(), jobid, dry_run)
        else:
            raise Exception(f'Expectation failed for {model_name}: {name}. Query not found')
    except Exception as e:
        print(f"Error running expectation {name}: {str(e)}")
        log_expectation(session, False, name, params, query, count, str(e), datetime.now(), jobid, dry_run)
        if failOnError and not dry_run:
            raise e

def run_expectations(session: Session, jobid: Optional[str] = None, dry_run: bool = False) -> None:
    """Run the expectations.
    Args:
        session (Session): The Snowflake session.
        jobid (Optional[str], optional): The optional job id. Defaults to None.
        dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
    """
    if expectation_items and check_if_expectations_table_exists(session, dry_run):
        for expectation in expectation_items:
            run_expectation(session, expectation.get("name", None), expectation.get("params", None), expectation.get("query", None), str_to_bool(expectation.get('failOnError', 'no')), jobid, dry_run)

def create_domain_if_not_exists(session: Session, domain: str, dry_run: bool = False) -> None:
    """Create the schema if it does not exist.
    Args:
        session (Session): The Snowflake session.
        domain (str): The domain.
        dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
    """
    execute_sql(session, f"CREATE SCHEMA IF NOT EXISTS {domain}", f"Create schema {domain} if not exists:", dry_run)

def enable_change_tracking(session: Session, sink: str, dry_run: bool = False) -> None:
    """Enable change tracking.
    Args:
        session (Session): The Snowflake session.
        sink (str): The sink.
        dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
    """
    execute_sql(session, f"ALTER TABLE {sink} SET CHANGE_TRACKING = TRUE", "Enable change tracking:", dry_run)

def commit_transaction(session: Session, dry_run: bool = False) -> None:
    """Commit the transaction.
    Args:
        session (Session): The Snowflake session.
        dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
    """
    execute_sql(session, "COMMIT", "COMMIT transaction:", dry_run)

def rollback_transaction(session: Session, dry_run: bool = False) -> None:
    """Rollback the transaction.
    Args:
        session (Session): The Snowflake session.
        dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
    """
    execute_sql(session, "ROLLBACK", "ROLLBACK transaction:", dry_run)

def fun(session: Session, model_name: str, dry_run: bool, ) -> None:
    from datetime import datetime
    start = datetime.now()

    try:
        # BEGIN transaction
        begin_transaction(session, dry_run)
        domainAndTable = model_name.split('.')
        domain = domainAndTable[0]
        table = domainAndTable[-1]

        # create SQL domain
        create_domain_if_not_exists(session, domain, dry_run)

        # execute preActions
        execute_sqls(session, statements.get('preActions', []), "Pre actions", dry_run)

        # execute preSqls
        execute_sqls(session, statements.get('preSqls', []), "Pre sqls", dry_run)

        if check_if_table_exists(session, domain, table):
            # enable change tracking
            enable_change_tracking(session, sink, dry_run)
            # execute addSCD2ColumnsSqls
            execute_sqls(session, statements.get('addSCD2ColumnsSqls', []), "Add SCD2 columns", dry_run)
            # execute mainSqlIfExists
            execute_sqls(session, statements.get('mainSqlIfExists', []), "Main sql if exists", dry_run)
        else:
            # execute mainSqlIfNotExists
            execute_sqls(session, statements.get('mainSqlIfNotExists', []), "Main sql if not exists", dry_run)
            # enable change tracking
            enable_change_tracking(session, sink, dry_run)

        # execute postSqls
        execute_sqls(session, statements.get('postSqls', []) , "Post sqls", dry_run)

        # run expectations
        run_expectations(session, jobid, dry_run)

        # COMMIT transaction
        commit_transaction(session, dry_run)
        end = datetime.now()
        duration = (end - start).total_seconds()
        print(f"#Duration in seconds: {duration}")
        log_audit(session, None, -1, -1, -1, True, duration, 'Success', end, jobid, "TRANSFORM", dry_run)
        
    except Exception as e:
        # ROLLBACK transaction
        error_message = str(e)
        print(f"Error executing transform for {model_name}: {error_message}")
        rollback_transaction(session, dry_run)
        end = datetime.now()
        duration = (end - start).total_seconds()
        print(f"Duration in seconds: {duration}")
        log_audit(session, None, -1, -1, -1, False, duration, error_message, end, jobid, "TRANSFORM", dry_run)
        raise e

