#
# Copyright © 2025 Starlake AI (https://starlake.ai)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union

class Cursor(ABC):

    @abstractmethod
    def execute(self, stmt: str) -> None: ...

    @abstractmethod
    def fetchall(self) -> List[Tuple]: ...

    @abstractmethod
    def close(self) -> None: ...

class Connection(ABC):

    @abstractmethod
    def cursor(self) -> Cursor: ...

    @abstractmethod
    def commit(self) -> None: ...

    @abstractmethod
    def rollback(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...

from enum import Enum

class SessionProvider(str, Enum):
    DUCKDB = "duckdb"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    REDSHIFT = "redshift"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"

    def __str__(self):
        return self.value

import inspect

class Session(ABC):
    def __init__(self, database: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, **kwargs):
        """
        Create a new session
        Args:
            database (Optional[str]): The database name
            user (Optional[str]): The user name
            password (Optional[str]): The password
            host (Optional[str]): The host
            port (Optional[int]): The port
            kwargs: Additional keyword arguments
        """
        self.__database = database
        self.__user = user
        self.__password = password
        self.__host = host
        self.__port = port
        self.__conn: Optional[Connection] = None

    @property
    def database(self) -> Optional[str]:
        return self.__database

    @property
    def user(self) -> Optional[str]:
        if inspect.stack()[1].function == '_new_connection':
            return self.__user
        else:
            raise AttributeError("User name is not accessible")

    @property
    def password(self) -> Optional[str]:
        if inspect.stack()[1].function == '_new_connection':
            return self.__password
        else:
            raise AttributeError("Password is not accessible")

    @property
    def host(self) -> Optional[str]:
        if inspect.stack()[1].function == '_new_connection':
            return self.__host
        else:
            raise AttributeError("Host is not accessible")

    @property
    def port(self) -> Optional[int]:
        if inspect.stack()[1].function == '_new_connection':            
            return self.__port
        else:
            raise AttributeError("Port is not accessible")

    @abstractmethod
    def provider(self) -> SessionProvider: ...

    @abstractmethod
    def _new_connection(self) -> Connection: 
        """
        Create a new connection
        Returns:
            Connection: The connection
        """
        ...

    @property
    def conn(self) -> Connection: 
        """
        Get the current connection, if not available create a new one
        Returns:
            Connection: The current connection or a new one if not available
        """
        if not self.__conn:
            self.__conn = self._new_connection()
        return self.__conn

    def sql(self, stmt: str) -> List[Any]:
        """
        Execute the SQL statement
        Args:
            stmt (str): The SQL statement
        Returns:
            List[Any]: The result
        """
        if stmt.strip().upper().startswith("USE SCHEMA ") and self.provider() != SessionProvider.SNOWFLAKE:
            schema = stmt.strip().split()[-1]
            if self.provider() in [SessionProvider.REDSHIFT, SessionProvider.POSTGRES]:
                stmt = f"SET search_path TO {schema}"
            else:
                stmt = f"USE `{self.database}.{schema}`"
        cur = self.conn.cursor()
        cur.execute(stmt)
        if (stmt.lower().startswith("select")) or (stmt.lower().startswith("with")):
            result = cur.fetchall()
        else:
            result = []
        cur.close()
        return result

    def commit(self) -> None:
        """
        Commit the transaction
        """
        if self.__conn:
            self.conn.commit()

    def rollback(self) -> None:
        """
        Rollback the transaction
        """
        if self.__conn:
            self.conn.rollback()

    def close(self) -> None:
        """
        Close the connection
        """
        if self.__conn:
            self.conn.close()
        self.__conn = None

import os

class DuckDBSession(Session):
    def __init__(self, database: Optional[str] = None, schema: Optional[str] = None, **kwargs):
        """
        Create a new DuckDB session
        Args:
            database (Optional[str]): The database name
            kwargs: Additional keyword arguments
        """
        env = os.environ.copy() # Copy the current environment variables
        options = {
            "database": database or kwargs.get('DUCKDB_DB', env.get('DUCKDB_DB', None)),
            "schema": schema or kwargs.get('DUCKDB_SCHEMA', env.get('DUCKDB_SCHEMA', None))
        }
        super().__init__(database=options.get('database', None), **kwargs)

    def provider(self) -> SessionProvider:
        return SessionProvider.DUCKDB
    
    def _new_connection(self) -> Connection:
        """
        Creates a new connection
        Returns:
            Connection: The new connection
        """
        if not self.database:
            raise ValueError("Database name is required")
        import duckdb
        return duckdb.connect(database=self.database)

class PostgresSession(Session):
    def __init__(self, database: Optional[str] = None, schema: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, **kwargs):
        """
        Create a new Postgres session
        Args:
            database (Optional[str]): The database name
            user (Optional[str]): The user name
            password (Optional[str]): The password
            host (Optional[str]): The host
            port (Optional[int]): The port
            kwargs: Additional keyword arguments
        """
        env = os.environ.copy() # Copy the current environment variables
        options = {
            "database": database or kwargs.get('POSTGRES_DB', env.get('POSTGRES_DB', None)),
            "currentSchema": schema or kwargs.get('POSTGRES_SCHEMA', env.get('POSTGRES_SCHEMA', None)),
            "user": user or kwargs.get('POSTGRES_USER', env.get('POSTGRES_USER', None)),
            "password": password or kwargs.get('POSTGRES_PASSWORD', env.get('POSTGRES_PASSWORD', None)),
            "host": host or kwargs.get('POSTGRES_HOST', env.get('POSTGRES_HOST', None)),
            "port": port or kwargs.get('POSTGRES_PORT', env.get('POSTGRES_PORT', None)),
        }
        super().__init__(database=options.get('database', None), user=options.get('user', None), password=options.get('password', None), host=options.get('host', '127.0.0.1'), port=options.get('port', 5432), **kwargs)

    def provider(self) -> SessionProvider:
        return SessionProvider.POSTGRES

    def _new_connection(self) -> Connection:
        """
        Creates a new connection
        Returns:
            Connection: The new connection
        """
        import psycopg2
        if not self.database:
            raise ValueError("Database name is required")
        if not self.user:
            raise ValueError("User name is required")
        if not self.password:
            raise ValueError("Password is required")
        return psycopg2.connect(database=self.database, user=self.user, host=self.host or '127.0.0.1', password=self.password, port=self.port or 5432)

class MySQLSession(Session):
    def __init__(self, database: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, **kwargs):
        """
        Create a new MySQL session
        Args:
            database (Optional[str]): The database name
            user (Optional[str]): The user name
            password (Optional[str]): The password
            host (Optional[str]): The host
            port (Optional[int]): The port
            kwargs: Additional keyword arguments
        """
        env = os.environ.copy() # Copy the current environment variables
        options = {
            "database": database or kwargs.get('MYSQL_DB', env.get('MYSQL_DB', None)),
            "user": user or kwargs.get('MYSQL_USER', env.get('MYSQL_USER', None)),
            "password": password or kwargs.get('MYSQL_PASSWORD', env.get('MYSQL_PASSWORD', None)),
            "host": host or kwargs.get('MYSQL_HOST', env.get('MYSQL_HOST', None)),
            "port": port or kwargs.get('MYSQL_PORT', env.get('MYSQL_PORT', None)),
        }
        super().__init__(database=options.get('database', None), user=options.get('user', None), password=options.get('password', None), host=options.get('host', '127.0.0.1'), port=options.get('port', 3306), **kwargs)

    def provider(self) -> SessionProvider:
        return SessionProvider.MYSQL

    def _new_connection(self) -> Connection:
        """
        Creates a new connection
        Returns:
            Connection: The new connection
        """
        import mysql.connector
        if not self.database:
            raise ValueError("Database name is required")
        if not self.user:
            raise ValueError("User name is required")
        if not self.password:
            raise ValueError("Password is required")
        return mysql.connector.connect(database=self.database, user=self.user, host=self.host or '127.0.0.1', password=self.password, port=self.port or 3306)

class RedshiftSession(Session):
    def __init__(self, database: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, **kwargs):
        """
        Create a new Redshift session
        Args:
            database (Optional[str]): The database name
            user (Optional[str]): The user name
            password (Optional[str]): The password
            host (Optional[str]): The host
            port (Optional[int]): The port
            kwargs: Additional keyword arguments
        """
        env = os.environ.copy() # Copy the current environment variables
        options = {
            "database": database or kwargs.get('REDSHIFT_DB', env.get('REDSHIFT_DB', None)),
            "user": user or kwargs.get('REDSHIFT_USER', env.get('REDSHIFT_USER', None)),
            "password": password or kwargs.get('REDSHIFT_PASSWORD', env.get('REDSHIFT_PASSWORD', None)),
            "host": host or kwargs.get('REDSHIFT_HOST', env.get('REDSHIFT_HOST', None)),
            "port": port or kwargs.get('REDSHIFT_PORT', env.get('REDSHIFT_PORT', None)),
        }
        super().__init__(database=options.get('database', None), user=options.get('user', None), password=options.get('password', None), host=options.get('host', '127.0.0.1'), port=options.get('port', 5439), **kwargs)

    def provider(self) -> SessionProvider:
        return SessionProvider.REDSHIFT
    
    def _new_connection(self) -> Connection:
        """
        Creates a new connection
        Returns:
            Connection: The new connection
        """
        import redshift_connector
        if not self.database:
            raise ValueError("Database name is required")
        if not self.user:
            raise ValueError("User name is required")
        if not self.password:
            raise ValueError("Password is required")
        return redshift_connector.connect(database=self.database, user=self.user, host=self.host, password=self.password, port=self.port)

class SnowflakeSession(Session):
    def __init__(self, database: Optional[str] = None, schema: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, **kwargs):
        """
        Create a new Snowflake session
        Args:
            database (Optional[str]): The database name
            user (Optional[str]): The user name
            password (Optional[str]): The password
            host (Optional[str]): The host
            port (Optional[int]): The port
            kwargs: Additional keyword arguments
        """
        env = os.environ.copy() # Copy the current environment variables
        options = {
            "database": database or kwargs.get('SNOWFLAKE_DB', env.get('SNOWFLAKE_DB', None)),
            "schema": schema or kwargs.get('SNOWFLAKE_SCHEMA', env.get('SNOWFLAKE_SCHEMA', None)),
            "user": user or kwargs.get('SNOWFLAKE_USER', env.get('SNOWFLAKE_USER', None)),
            "password": password or kwargs.get('SNOWFLAKE_PASSWORD', env.get('SNOWFLAKE_PASSWORD', None)),
            "host": host or kwargs.get('SNOWFLAKE_HOST', env.get('SNOWFLAKE_HOST', None)),
            "port": port or kwargs.get('SNOWFLAKE_PORT', env.get('SNOWFLAKE_PORT', None)),
            "account": kwargs.get('SNOWFLAKE_ACCOUNT', env.get('SNOWFLAKE_ACCOUNT', None)),
            "warehouse": kwargs.get('SNOWFLAKE_WAREHOUSE', env.get('SNOWFLAKE_WAREHOUSE', None)),
            "role": kwargs.get('SNOWFLAKE_ROLE', env.get('SNOWFLAKE_ROLE', None)),
        }
        super().__init__(database=options.get('database', None), user=options.get('user', None), password=options.get('password', None), host=options.get('host', None), port=options.get('port', 443), **kwargs)
        self.__account = options.get('account', None)
        self.__warehouse = options.get('warehouse', None)
        self.__role = options.get('role', None)

    def provider(self) -> SessionProvider:
        return SessionProvider.SNOWFLAKE

    @property
    def account(self) -> Optional[str]:
        if inspect.stack()[1].function == '_new_connection':
            return self.__account
        else:
            raise AttributeError("Account is not accessible")

    @property
    def warehouse(self) -> Optional[str]:
        if inspect.stack()[1].function == '_new_connection':
            return self.__warehouse
        else:
            raise AttributeError("Warehouse is not accessible")

    @property
    def role(self) -> Optional[str]:
        if inspect.stack()[1].function == '_new_connection':
            return self.__role
        else:
            raise AttributeError("Role is not accessible")

    def _new_connection(self) -> Connection:
        """
        Creates a new connection
        Returns:
            Connection: The new connection
        """
        import snowflake.connector.connection
        if not self.database:
            raise ValueError("Database name is required")
        if not self.user:
            raise ValueError("User name is required")
        if not self.password:
            raise ValueError("Password is required")
        if not self.account:
            raise ValueError("Account is required")
        if not self.warehouse:
            raise ValueError("Warehouse is required")
        return snowflake.connector.connect(database=self.database, user=self.user, host=self.host or f'{self.account}.snowflakecomputing.com', password=self.password, port=self.port or 443, account=self.account, warehouse=self.warehouse, role=self.role)

class SessionFactory:
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def session(cls, provider: SessionProvider = SessionProvider.DUCKDB, database: Optional[str] = None,  schema: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, **kwargs) -> Session: 
        """
        Create a new session based on the provider
        Args:
            provider (SessionProvider): The provider to use
            database (Optional[str]): The database name
            schema (Optional[str]): The schema name
            user (Optional[str]): The user name
            password (Optional[str]): The password
            host (Optional[str]): The host
            port (Optional[int]): The port
            kwargs: Additional keyword arguments
        Returns:
            Session: The session
        Example:
            session = SessionFactory.session(SessionProvider.POSTGRES, database="starlake", user="starlake")
        """
        if provider == SessionProvider.DUCKDB:
            return DuckDBSession(database=database, schema=schema, **kwargs)
        elif provider == SessionProvider.POSTGRES:
            return PostgresSession(database=database, schema=schema, user=user, password=password, host=host, port=port, **kwargs)
        elif provider == SessionProvider.MYSQL:
            return MySQLSession(database=database, user=user, password=password, host=host, port=port, **kwargs)
        elif provider == SessionProvider.REDSHIFT:
            return RedshiftSession(database=database, user=user, password=password, host=host, port=port, **kwargs)
        elif provider == SessionProvider.SNOWFLAKE:
            return SnowflakeSession(database=database, schema=schema, user=user, password=password, host=host, port=port, **kwargs)
        elif provider == SessionProvider.BIGQUERY:
            from .starlake_bigquery_session import BigQuerySession
            return BigQuerySession(database=database, **kwargs)
        else:
            raise ValueError(f"Unsupported provider: {provider}")

# Example usage
# session = SessionFactory.session(SessionProvider.POSTGRES, database="starlake", user="starlake")
# rows = session.sql("select * from public.slk_member")
# rows2 = session.sql("insert into public.slk_whitelist(email_or_domain) values('gmail.com')")
# session.commit()
# for row in rows:
#     print(row)
# session.close()