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

import logging
import os
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.configuration import conf


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------


def api_prefix() -> str:
    """
    Airflow 2.x → /api/v1
    Airflow 3.x → /api/v2
    """
    return "/api/v2"


# ---------------------------------------------------------------------------
# DotDict utility for dot-notation access
# ---------------------------------------------------------------------------

class DotDict(dict):
    """Dictionary allowing attribute-style access (obj.key)."""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def to_dotdict(obj: Any) -> Any:
    """Recursively convert dicts/lists to DotDict."""
    if isinstance(obj, dict):
        return DotDict({k: to_dotdict(v) for k, v in obj.items()})
    if isinstance(obj, list):
        return [to_dotdict(v) for v in obj]
    return obj


# ---------------------------------------------------------------------------
# Airflow API Client (HTTP REST API only - Airflow 3.0 blocks direct ORM access)
# ---------------------------------------------------------------------------

class StarlakeAirflowApiClient:
    """
    Client for interacting with the Airflow REST API (v2).
    
    NOTE: Airflow 3.0 blocks direct ORM access from task code with the error:
    "Direct database access via the ORM is not allowed in Airflow 3.0"
    
    Therefore, this client ONLY uses the HTTP REST API for all operations.
    
    This client handles:
    - HTTP REST API requests with retry logic
    - Authentication via JWT Bearer tokens
    - Querying DagRuns, TaskInstances, Assets/Datasets, and AssetEvents
    """

    def __init__(
            self,
            conn_id: str = "airflow_api",
            timeout: int = 30,
            max_retries: int = 3,
    ) -> None:
        """
        Initialize the Airflow API client.
        
        Args:
            conn_id: Airflow connection ID for HTTP authentication (optional)
            timeout: HTTP request timeout in seconds
            max_retries: Number of HTTP retry attempts
        """
        log.info("Initializing StarlakeAirflowApiClient (conn_id=%s, timeout=%s, max_retries=%s)", 
                 conn_id, timeout, max_retries)
        
        self.timeout = timeout
        self._init_http_client(conn_id, max_retries)
        log.info("StarlakeAirflowApiClient initialization complete - using HTTP REST API")

    def _init_http_client(self, conn_id: str, max_retries: int) -> None:
        """Initialize HTTP client components for REST API access."""
        log.info("Initializing HTTP client for REST API access...")
        
        # Base URL from environment or airflow.cfg
        base = os.environ.get("AIRFLOW_API_BASE_URL")
        if base:
            log.info("Using AIRFLOW_API_BASE_URL from environment: %s", base)
        else:
            log.info("AIRFLOW_API_BASE_URL not set, checking airflow.cfg...")
            try:
                base = conf.get("api", "base_url")
                log.info("Using base_url from [api] config: %s", base)
            except Exception:
                try:
                    base = conf.get("webserver", "base_url")
                    log.info("Using base_url from [webserver] config: %s", base)
                except Exception:
                    base = "http://localhost:8080"
                    log.info("No base_url found in config, using default: %s", base)
                    
        self.base_url = base.rstrip("/")
        self.base_url = "http://starlake-airflow:8080/airflow"
        self.api_base_url = f"{self.base_url}{api_prefix()}"
        log.info("API base URL set to: %s", self.api_base_url)

        # Airflow connection (username/password)
        log.info("Looking up Airflow connection: %s", conn_id)
        try:
            from airflow.sdk.bases.hook import BaseHook
            self.conn = BaseHook.get_connection(conn_id)
            log.info("Found Airflow connection '%s' with login: %s", conn_id, self.conn.login)
        except Exception as e:
            log.info("Could not find Airflow connection '%s' (%s), using default credentials", conn_id, e)
            self.conn = DotDict({"login": "airflow", "password": "airflow"})

        # HTTP session with retry strategy
        log.info("Creating HTTP session with retry strategy (max_retries=%s)", max_retries)
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

        retry = Retry(
            total=max_retries,
            backoff_factor=0.5,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST", "PUT", "PATCH", "DELETE"]),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Authentication mode: Airflow 3.x → JWT Bearer
        log.info("Configuring JWT Bearer authentication...")
        self._configure_bearer_auth()

    # -----------------------------------------------------------------------
    # Authentication for HTTP mode (Airflow 3.x)
    # -----------------------------------------------------------------------

    def _configure_bearer_auth(self) -> None:
        """
        Obtain a JWT token via POST /auth/token and configure Authorization header.
        """
        token_url = self.base_url + "/auth/token"
        log.info("Requesting JWT token from: %s", token_url)

        payload = {}
        if self.conn.login and self.conn.password:
            payload = {"username": self.conn.login, "password": self.conn.password}
            log.info("Using credentials for user: %s", self.conn.login)
        else:
            log.info("No credentials provided, requesting token without authentication")

        resp = requests.post(
            token_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=self.timeout,
        )

        if not resp.ok:
            log.error("Failed to obtain JWT token: HTTP %s - %s", resp.status_code, resp.text[:500])
            raise RuntimeError(
                f"Failed to obtain JWT token ({resp.status_code}): {resp.text}"
            )

        data = resp.json()
        token = (
                data.get("access_token")
                or data.get("token")
                or data.get("jwt")
                or data.get("clientToken")
        )

        if not token:
            log.error("JWT token not found in response keys: %s", list(data.keys()))
            raise RuntimeError(f"JWT token not found in response: {data}")

        log.info("JWT token obtained successfully (token length: %d chars)", len(token))
        self.session.headers["Authorization"] = f"Bearer {token}"

    # -----------------------------------------------------------------------
    # HTTP helpers
    # -----------------------------------------------------------------------

    def _url(self, path: str) -> str:
        return f"{self.api_base_url}/{path.lstrip('/')}"

    def _request(
            self,
            method: str,
            path: str,
            params: Optional[Dict[str, Any]] = None,
            json: Optional[Dict[str, Any]] = None,
    ) -> Any:

        url = self._url(path)
        log.info("HTTP %s %s (params=%s)", method, url, params)

        resp = self.session.request(
            method=method,
            url=url,
            params=params,
            json=json,
            timeout=self.timeout,
        )

        # Resource not found
        if resp.status_code == 404:
            log.info("HTTP 404 Not Found for %s", url)
            return None

        # No content
        if resp.status_code == 204:
            log.info("HTTP 204 No Content for %s", url)
            return None

        # Other errors
        if not (200 <= resp.status_code < 300):
            log.error("HTTP error %s for %s: %s", resp.status_code, url, resp.text[:1000])
            raise RuntimeError(
                f"Airflow API error {resp.status_code} for {url}: {resp.text}"
            )

        # Normal JSON response
        result = to_dotdict(resp.json())
        log.info("HTTP %s %s completed successfully", method, url)
        return result

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", path, params=params)

    # -----------------------------------------------------------------------
    # DAG Runs
    # -----------------------------------------------------------------------

    def list_dag_runs(self, dag_id: str, **params) -> List[DotDict]:
        """
        List DagRuns for a given DAG.
        """
        log.info("list_dag_runs called for dag_id=%s with params=%s", dag_id, params)
        resp = self._get(f"dags/{dag_id}/dagRuns", params=params)
        runs = resp.dag_runs if resp else []
        log.info("list_dag_runs returned %d runs for dag_id=%s", len(runs), dag_id)
        return runs

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> Optional[DotDict]:
        """Get a specific DagRun."""
        log.info("get_dag_run called for dag_id=%s, dag_run_id=%s", dag_id, dag_run_id)
        result = self._get(f"dags/{dag_id}/dagRuns/{dag_run_id}")
        log.info("get_dag_run result: %s", "found" if result else "not found")
        return result

    # -----------------------------------------------------------------------
    # Task Instances
    # -----------------------------------------------------------------------

    def list_task_instances(self, dag_id: str, dag_run_id: str, **params) -> List[DotDict]:
        """List task instances for a specific DagRun."""
        log.info("list_task_instances called for dag_id=%s, dag_run_id=%s with params=%s", dag_id, dag_run_id, params)
        resp = self._get(f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances", params=params)
        instances = resp.task_instances if resp else []
        log.info("list_task_instances returned %d task instances", len(instances))
        return instances

    def list_dag_task_instances(self, dag_id: str, **params) -> List[DotDict]:
        """
        List task instances for a given DAG across all runs.
        """
        log.info("list_dag_task_instances called for dag_id=%s with params=%s", dag_id, params)
        resp = self._get(f"dags/{dag_id}/dagRuns/~/taskInstances", params=params)
        instances = resp.task_instances if resp else []
        log.info("list_dag_task_instances returned %d task instances", len(instances))
        return instances

    # -----------------------------------------------------------------------
    # Assets (Airflow 3.x)
    # -----------------------------------------------------------------------

    def get_dataset_by_uri(self, uri: str) -> Optional[DotDict]:
        """
        Get dataset/asset by URI.
        """
        log.info("get_dataset_by_uri called for uri=%s", uri)
        # Try finding by URI via list endpoint
        resp = self._get("assets", params={"uri_pattern": uri, "limit": 1})
        if resp and resp.assets:
            result = resp.assets[0]
            log.info("Found asset: id=%s, uri=%s", result.id, result.uri)
            return result
        log.info("No asset found for uri=%s", uri)
        return None

    def list_events(self, **params) -> List[DotDict]:
        """
        List asset events.
        """
        log.info("list_events called with params=%s", str(params))
        resp = self._get("assets/events", params=params)
        events = resp.asset_events if resp else []
        log.info("list_events returned %d events", len(events))
        return events
