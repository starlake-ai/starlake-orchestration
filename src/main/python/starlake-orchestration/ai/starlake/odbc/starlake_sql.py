from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import List, Optional, Tuple

from ai.starlake.odbc import Session

class SQLTask(ABC):

    def __init__(self, caller_globals: dict = dict(), options: dict = dict(), **kwargs):
        """Initialize the SQL task.
        Args:
            sink (str): The sink.
            caller_globals (dict, optional): The caller globals. Defaults to dict().
            options (dict, optional): The options. Defaults to dict().
        """
        super().__init__()
        self.caller_globals = caller_globals
        self.statements: dict = self.caller_globals.get('statements', dict()).get(sink, None)
        self.audit: dict = self.caller_globals.get('audit', dict())
        self.expectations: dict = self.caller_globals.get('expectations', dict())
        self.expectation_items: dict = self.caller_globals.get('expectation_items', dict()).get(sink, None)
        self.safe_params = defaultdict(lambda: 'NULL', options)

    def bindParams(self, stmt: str) -> str:
        """Bind parameters to the SQL statement.
        Args:
            stmt (str): The SQL statement.
        Returns:
            str: The SQL statement with the parameters bound
        """
        return stmt.format_map(self.safe_params)

    def str_to_bool(self, value: str) -> bool:
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

    def execute_sql(self, session: Session, sql: Optional[str], message: Optional[str] = None, dry_run: bool = False) -> List[Row]:
        """Execute the SQL.
        Args:
            session (Session): The Starlake session.
            sql (str): The SQL query to execute.
            message (Optional[str], optional): The optional message. Defaults to None.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        Returns:
            List[Row]: The rows.
        """
        if sql:
            if dry_run and message:
                print(f"# {message}")
            stmt: str = self.bindParams(sql)
            if dry_run:
                print(f"{stmt};")
                return []
            else:
                try:
                    rows = session.sql(stmt)
                    return rows
                except Exception as e:
                    raise Exception(f"Error executing SQL {stmt}: {str(e)}")
        else:
            return []

    def execute_sqls(self, session: Session, sqls: List[str], message: Optional[str] = None, dry_run: bool = False) -> None:
        """Execute the SQLs.
        Args:
            session (Session): The Starlake session.
            sqls (List[str]): The SQLs.
            message (Optional[str], optional): The optional message. Defaults to None.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if sqls:
            if dry_run and message:
                print(f"# {message}")
            for sql in sqls:
                self.execute_sql(session, sql, None, dry_run)

    def check_if_table_exists(self, session: Session, domain: str, table: str) -> bool:
        """Check if the table exists.
        Args:
            session (Session): The Starlake session.
            domain (str): The domain.
            table (str): The table.
            Returns:
            bool: True if the table exists, False otherwise.
        """
        query=f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) ILIKE '{domain}.{table}'"
        return self.execute_sql(session, query, f"Check if table {domain}.{table} exists:", False).__len__() > 0

    def check_if_audit_table_exists(self, session: Session, dry_run: bool = False) -> bool:
        """Check if the audit table exists.
        Args:
            session (Session): The Starlake session.
            Returns:
            bool: True if the audit table exists, False otherwise.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if self.audit:
            try:
                # create SQL domain
                domain = self.audit.get('domain', ['audit'])[0]
                self.create_domain_if_not_exists(session, domain, dry_run)
                # execute SQL preActions
                self.execute_sqls(session, self.audit.get('preActions', []), "Execute audit pre action:", dry_run)
                # check if the audit table exists
                if not self.check_if_table_exists(session, domain, 'audit'):
                    # execute SQL createSchemaSql
                    sqls: List[str] = self.audit.get('createSchemaSql', [])
                    if sqls:
                        self.execute_sqls(session, sqls, "Create audit table", dry_run)
                    return True
                else:
                    return True
            except Exception as e:
                print(f"Error creating audit table: {str(e)}")
                return False
        else:
            return False

    def check_if_expectations_table_exists(self, session: Session, dry_run: bool = False) -> bool:
        """Check if the expectations table exists.
        Args:
            session (Session): The Starlake session.
            Returns:
            bool: True if the expectations table exists, False otherwise.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if self.expectations:
            try:
                # create SQL domain
                domain = self.expectations.get('domain', ['audit'])[0]
                self.create_domain_if_not_exists(session, domain, dry_run)
                # check if the expectations table exists
                if not self.check_if_table_exists(session, domain, 'expectations'):
                    # execute SQL createSchemaSql
                    self.execute_sqls(session, self.expectations.get('createSchemaSql', []), "Create expectations table", dry_run)
                    return True
                else:
                    return True
            except Exception as e:
                print(f"Error creating expectations table: {str(e)}")
                return False
        else:
            return False

    def log_audit(self, session: Session, domain: str, table: str, paths: Optional[str], count: int, countAccepted: int, countRejected: int, success: bool, duration: int, message: str, ts: datetime, jobid: Optional[str] = None, step: Optional[str] = None, dry_run: bool = False) -> bool :
        """Log the audit record.
        Args:
            session (Session): The Starlake session.
            domain (str): The domain.
            table (str): The table.
            paths (Optional[str], optional): The optional paths. Defaults to None.
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
        if self.audit and self.check_if_audit_table_exists(session, dry_run):
            audit_domain = self.audit.get('domain', ['audit'])[0]
            audit_sqls = self.audit.get('mainSqlIfExists', None)
            if audit_sqls:
                try:
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
                    self.execute_sql(session, insert_sql, "Insert audit record:", dry_run)
                    return True
                except Exception as e:
                    print(f"Error inserting audit record: {str(e)}")
                    return False
            else:
                return False
        else:
            return False

    def log_expectation(self, session: Session, domain:str, table: str, success: bool, name: str, params: str, sql: str, count: int, exception: str, ts: datetime, jobid: Optional[str] = None, dry_run: bool = False) -> bool :
        """Log the expectation record.
        Args:
            session (Session): The Starlake session.
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
        if self.expectations and self.check_if_expectations_table_exists(session, dry_run):
            expectation_domain = self.expectations.get('domain', ['audit'])[0]
            expectation_sqls = self.expectations.get('mainSqlIfExists', None)
            if expectation_sqls:
                try:
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
                    self.execute_sql(session, insert_sql, "Insert expectations record:", dry_run)
                    return True
                except Exception as e:
                    print(f"Error inserting expectations record: {str(e)}")
                    return False
            else:
                return False
        else:
            return False

    def run_expectation(self, session: Session, name: str, params: str, query: str, failOnError: bool = False, jobid: Optional[str] = None, dry_run: bool = False) -> None:
        """Run the expectation.
        Args:
            session (Session): The Starlake session.
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
                rows = self.execute_sql(session, query, f"Run expectation {name}:", dry_run)
                if rows.__len__() != 1:
                    if not dry_run:
                        raise Exception(f'Expectation failed for {sink}: {query}. Expected 1 row but got {rows.__len__()}')
                else:
                    count = rows[0][0]
                #  log expectations as audit in expectation table here
                if count != 0:
                    raise Exception(f'Expectation failed for {sink}: {query}. Expected count to be equal to 0 but got {count}')
                self.log_expectation(session, True, name, params, query, count, "", datetime.now(), jobid, dry_run)
            else:
                raise Exception(f'Expectation failed for {sink}: {name}. Query not found')
        except Exception as e:
            print(f"Error running expectation {name}: {str(e)}")
            self.log_expectation(session, False, name, params, query, count, str(e), datetime.now(), jobid, dry_run)
            if failOnError and not dry_run:
                raise e

    def run_expectations(self, session: Session, jobid: Optional[str] = None, dry_run: bool = False) -> None:
        """Run the expectations.
        Args:
            session (Session): The Starlake session.
            jobid (Optional[str], optional): The optional job id. Defaults to None.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if self.expectation_items and self.check_if_expectations_table_exists(session, dry_run):
            for expectation in expectation_items:
                self.run_expectation(session, expectation.get("name", None), expectation.get("params", None), expectation.get("query", None), self.str_to_bool(expectation.get('failOnError', 'no')), jobid, dry_run)

    def begin_transaction(self, session: Session, dry_run: bool = False) -> None:
        """Begin the transaction.
        Args:
            session (Session): The Starlake session.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, "BEGIN", "BEGIN transaction:", dry_run)

    def create_domain_if_not_exists(self, session: Session, domain: str, dry_run: bool = False) -> None:
        """Create the schema if it does not exist.
        Args:
            session (Session): The Starlake session.
            domain (str): The domain.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, f"CREATE SCHEMA IF NOT EXISTS {domain}", f"Create schema {domain} if not exists:", dry_run)

    def enable_change_tracking(self, session: Session, sink: str, dry_run: bool = False) -> None:
        """Enable change tracking.
        Args:
            session (Session): The Starlake session.
            sink (str): The sink.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, f"ALTER TABLE {sink} SET CHANGE_TRACKING = TRUE", "Enable change tracking:", dry_run)

    def commit_transaction(self, session: Session, dry_run: bool = False) -> None:
        """Commit the transaction.
        Args:
            session (Session): The Starlake session.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, "COMMIT", "COMMIT transaction:", dry_run)

    def rollback_transaction(self, session: Session, dry_run: bool = False) -> None:
        """Rollback the transaction.
        Args:
            session (Session): The Starlake session.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, "ROLLBACK", "ROLLBACK transaction:", dry_run)
