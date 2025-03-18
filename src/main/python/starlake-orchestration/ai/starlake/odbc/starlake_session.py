from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

class Cursor(ABC):

    @abstractmethod
    def execute(self, stmt: str) -> None: ...

    @abstractmethod
    def fetchall(self) -> List[Tuple]: ...

    @abstractmethod
    def close(self) -> None: ...

class Connection(ABC):
    def __init__(self, database: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, **kwargs):
        super().__init__()
        self._database = database
        self._user = user
        self._password = password
        self._host = host
        self._port = port

    @property
    def database(self) -> Optional[str]:
        return self._database

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

    def __str__(self):
        return self.value

class Session(Connection):
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
        super().__init__(database=database, user=user, password=password, host=host, port=port, **kwargs)
        self._conn: Connection = None

    @abstractmethod
    def provider(self) -> SessionProvider: ...

    @property
    @abstractmethod
    def conn(self) -> Connection: 
        """
        Get the connection
        Returns:
            Connection: The connection
        """
        ...

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

    def cursor(self) -> Cursor:
        """
        Get a new cursor
        Returns:
            Cursor: The cursor
        """
        return self.conn.cursor()

    def commit(self) -> None:
        """
        Commit the transaction
        """
        self.conn.commit()

    def rollback(self) -> None:
        """
        Rollback the transaction
        """
        self.conn.rollback()

    def close(self) -> None:
        """
        Close the connection
        """
        self.conn.close()
        self._conn = None

import os

class DuckDBSession(Session):
    def __init__(self, database: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, **kwargs):
        """
        Create a new DuckDB session
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
            "database": database or kwargs.get('DUCKDB_DB', env.get('DUCKDB_DB', None)),
        }
        super().__init__(database=options.get('database', None), **kwargs)

    def provider(self) -> SessionProvider:
        return SessionProvider.DUCKDB
    
    @property
    def conn(self) -> Connection:
        """
        Get the connection
        Returns:
            Connection: The connection
        """
        if not self._conn:
            if not self._database:
                raise ValueError("Database name is required")
            import duckdb
            self._conn = duckdb.connect(database=self._database)
        return self._conn

class PostgresSession(Session):
    def __init__(self, database: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, **kwargs):
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
            "user": user or kwargs.get('POSTGRES_USER', env.get('POSTGRES_USER', None)),
            "password": password or kwargs.get('POSTGRES_PASSWORD', env.get('POSTGRES_PASSWORD', None)),
            "host": host or kwargs.get('POSTGRES_HOST', env.get('POSTGRES_HOST', None)),
            "port": port or kwargs.get('POSTGRES_PORT', env.get('POSTGRES_PORT', None)),
        }
        super().__init__(database=options.get('database', None), user=options.get('user', None), password=options.get('password', None), host=options.get('host', '127.0.0.1'), port=options.get('port', 5432), **kwargs)

    def provider(self) -> SessionProvider:
        return SessionProvider.POSTGRES

    @property
    def conn(self) -> Connection:
        """
        Get the connection
        Returns:
            Connection: The connection
        """
        if not self._conn:
            import psycopg2
            if not self._database:
                raise ValueError("Database name is required")
            if not self._user:
                raise ValueError("User name is required")
            if not self._password:
                raise ValueError("Password is required")
            self._conn = psycopg2.connect(database=self._database, user=self._user, host=self._host or '127.0.0.1', password=self._password, port=self._port or 5432)
        return self._conn

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

    @property
    def conn(self) -> Connection:
        """
        Get the connection
        Returns:
            Connection: The connection
        """
        if not self._conn:
            import mysql.connector
            if not self._database:
                raise ValueError("Database name is required")
            if not self._user:
                raise ValueError("User name is required")
            if not self._password:
                raise ValueError("Password is required")
            self._conn = mysql.connector.connect(database=self._database, user=self._user, host=self._host or '127.0.0.1', password=self._password, port=self._port or 3306)
        return self._conn

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
    
    @property
    def conn(self) -> Connection:
        """
        Get the connection
        Returns:
            Connection: The connection
        """
        if not self._conn:
            import redshift_connector
            if not self._database:
                raise ValueError("Database name is required")
            if not self._user:
                raise ValueError("User name is required")
            if not self._password:
                raise ValueError("Password is required")
            self._conn = redshift_connector.connect(database=self._database, user=self._user, host=self._host, password=self._password, port=self._port)
        return self._conn

class SnowflakeSession(Session):
    def __init__(self, database: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, **kwargs):
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
            "user": user or kwargs.get('SNOWFLAKE_USER', env.get('SNOWFLAKE_USER', None)),
            "password": password or kwargs.get('SNOWFLAKE_PASSWORD', env.get('SNOWFLAKE_PASSWORD', None)),
            "host": host or kwargs.get('SNOWFLAKE_HOST', env.get('SNOWFLAKE_HOST', None)),
            "port": port or kwargs.get('SNOWFLAKE_PORT', env.get('SNOWFLAKE_PORT', None)),
            "account": kwargs.get('SNOWFLAKE_ACCOUNT', env.get('SNOWFLAKE_ACCOUNT', None)),
            "warehouse": kwargs.get('SNOWFLAKE_WAREHOUSE', env.get('SNOWFLAKE_WAREHOUSE', None)),
            "role": kwargs.get('SNOWFLAKE_ROLE', env.get('SNOWFLAKE_ROLE', None)),
        }
        super().__init__(database=options.get('database', None), user=options.get('user', None), password=options.get('password', None), host=options.get('host', None), port=options.get('port', 443), **kwargs)
        self._account = options.get('account', None)
        self._warehouse = options.get('warehouse', None)
        self._role = options.get('role', None)

    def provider(self) -> SessionProvider:
        return SessionProvider.SNOWFLAKE

    @property
    def conn(self) -> Connection:
        """
        Get the connection
        Returns:
            Connection: The connection
        """
        if not self._conn:
            import snowflake.connector.connection
            if not self._database:
                raise ValueError("Database name is required")
            if not self._user:
                raise ValueError("User name is required")
            if not self._password:
                raise ValueError("Password is required")
            if not self._account:
                raise ValueError("Account is required")
            if not self._warehouse:
                raise ValueError("Warehouse is required")
            self._conn = snowflake.connector.connect(database=self._database, user=self._user, host=self._host or f'{self._account}.snowflakecomputing.com', password=self._password, port=self._port or 443, account=self._account, warehouse=self._warehouse, role=self._role)
        return self._conn

class SessionFactory:
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def session(cls, provider: SessionProvider = SessionProvider.DUCKDB, database: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None, **kwargs) -> Session: 
        """
        Create a new session based on the provider
        Args:
            provider (SessionProvider): The provider to use
            database (Optional[str]): The database name
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
            return DuckDBSession(database=database, user=user, password=password, host=host, port=port, **kwargs)
        elif provider == SessionProvider.POSTGRES:
            return PostgresSession(database=database, user=user, password=password, host=host, port=port, **kwargs)
        elif provider == SessionProvider.MYSQL:
            return MySQLSession(database=database, user=user, password=password, host=host, port=port, **kwargs)
        elif provider == SessionProvider.REDSHIFT:
            return RedshiftSession(database=database, user=user, password=password, host=host, port=port, **kwargs)
        elif provider == SessionProvider.SNOWFLAKE:
            return SnowflakeSession(database=database, user=user, password=password, host=host, port=port, **kwargs)
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