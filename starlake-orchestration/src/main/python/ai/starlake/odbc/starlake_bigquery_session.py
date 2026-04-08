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

import inspect
import os
from typing import Any, Dict, List, Optional, Tuple, Union

from google.cloud import bigquery
import google.auth.credentials
import google.api_core.client_info
import google.api_core.client_options

from .starlake_session import Connection, Cursor, Session, SessionProvider

class BigQueryConnection(bigquery.Client, Connection, Cursor):
    def __init__(self,
                 project: Optional[str] = None,
                 credentials: Optional[google.auth.credentials.Credentials] = None,
                 location: Optional[str] = None,
                 client_info: Optional[google.api_core.client_info.ClientInfo] = None,
                 client_options: Optional[Union[google.api_core.client_options.ClientOptions, Dict[str, Any]]] = None,
                 **kwargs):
        super().__init__(project=project, credentials=credentials, location=location, client_info=client_info, client_options=client_options, **kwargs)

    def cursor(self) -> Cursor:
        return self

    def execute(self, stmt: str) -> None:
        query_job = self.query(stmt)
        self.__iterator = query_job.result()

    def fetchall(self) -> List[Tuple]:
        if self.__iterator:
            from google.api_core.page_iterator import Page
            from google.cloud.bigquery.table import Row
            page: Page = next(self.__iterator.pages)
            rows: List[Row] = list(page)
            return list(map(lambda row: tuple(row.items()), rows or []))
        return []

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

class BigQuerySession(Session):

    def __init__(self, database: Optional[str] = None, **kwargs):
        """
        Create a new BigQuery session
        Args:
            database (Optional[str]): The database name
            kwargs: Additional keyword arguments
        """
        env = os.environ.copy() # Copy the current environment variables
        scopes = kwargs.get('authScopes', 'https://www.googleapis.com/auth/cloud-platform').split(',')
        auth_type = kwargs.get('authType', 'APPLICATION_DEFAULT')
        if auth_type == 'APPLICATION_DEFAULT':
            import google.auth
            creds, _ = google.auth.default(scopes)
        elif auth_type == 'SERVICE_ACCOUNT_JSON_KEYFILE':
            filename = kwargs.get('jsonKeyfile', env.get('GOOGLE_APPLICATION_CREDENTIALS', None))
            if not filename:
                raise ValueError("JSON keyfile is required")
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(filename=filename, scopes=scopes)
        elif auth_type == 'USER_CREDENTIALS':
            from google.oauth2 import credentials
            creds = credentials.Credentials(
                token=kwargs.get('accessToken', env.get('accessToken', None)),
                refresh_token=kwargs.get('refreshToken', env.get('refreshToken', None)),
                client_id=kwargs.get('clientId', env.get('clientId', None)),
                client_secret=kwargs.get('clientSecret', env.get('clientSecret', None)),
                scopes=scopes,
            )
        else:
            raise ValueError(f"Invalid authType: {auth_type}")
        project_id = database or kwargs.get('project_id', env.get('GOOGLE_CLOUD_PROJECT', None))
        super().__init__(database=project_id, **kwargs)

        impersonated_service_account = kwargs.get('impersonatedServiceAccount', None)
        if impersonated_service_account:
            import google.auth.impersonated_credentials
            creds = google.auth.impersonated_credentials.Credentials(
                source_credentials=creds,
                target_principal=impersonated_service_account,
                target_scopes=scopes,
            )

        self.__credentials = creds
        self.__project_id = project_id
        self.__location = kwargs.get('location', env.get('location', None))
        from google.api_core import client_info
        self.__client_info = client_info.ClientInfo(user_agent="starlake")
        self.__client_options = kwargs.get('client_options', env.get('client_options', None))

    @property
    def credentials(self) -> Optional[google.auth.credentials.Credentials]:
        if inspect.stack()[1].function == '_new_connection':
            return self.__credentials
        else:
            raise AttributeError("Credentials are not accessible")

    @property
    def project_id(self) -> Optional[str]:
        if inspect.stack()[1].function == '_new_connection':
            return self.__project_id
        else:
            raise AttributeError("Project ID is not accessible")

    @property
    def location(self) -> Optional[str]:
        if inspect.stack()[1].function == '_new_connection':
            return self.__location
        else:
            raise AttributeError("Location is not accessible")

    @property
    def client_info(self) -> Optional[google.api_core.client_info.ClientInfo]:
        if inspect.stack()[1].function == '_new_connection':
            return self.__client_info
        else:
            raise AttributeError("Client info is not accessible")

    @property
    def client_options(self) -> Optional[
            Union[google.api_core.client_options.ClientOptions, Dict[str, Any]]
        ]:
        if inspect.stack()[1].function == '_new_connection':
            return self.__client_options
        else:
            raise AttributeError("Client options are not accessible")

    def provider(self) -> SessionProvider:
        return SessionProvider.BIGQUERY

    def _new_connection(self) -> Connection:
        """
        Creates a new connection
        Returns:
            Connection: The new connection
        """
        return BigQueryConnection(
            project=self.project_id,
            credentials=self.credentials,
            location=self.location,
            client_info=self.client_info,
            client_options=self.client_options
        )