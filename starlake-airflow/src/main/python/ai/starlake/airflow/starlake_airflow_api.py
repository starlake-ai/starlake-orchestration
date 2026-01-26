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
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.configuration import conf
from airflow.sdk.bases.hook import BaseHook


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------

def supports_datasets() -> bool:
    """Datasets introduced in Airflow 2.4."""
    return False


def supports_inlet_events() -> bool:
    """Inlet events introduced in Airflow 2.10."""
    return True


def supports_assets() -> bool:
    """Assets replace datasets starting in Airflow 3.0."""
    return True


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
# Airflow API Client (supports Airflow 3)
# ---------------------------------------------------------------------------

class StarlakeAirflowApiClient(BaseHook):
    """
    Client for interacting with the Airflow API (v2).
    
    This client handles:
    - Authentication via JWT (if configured).
    - Querying dataset events (AssetEvents).
    - Triggering DAGs via the REST API.
    
    It abstracts away the differences between Airflow versions (now focused on v2/Airflow 3).
    """

    def __init__(
            self,
            conn_id: str = "airflow_api",
            timeout: int = 30,
            max_retries: int = 3,
    ) -> None:

        self.timeout = timeout
        self._supports_datasets = False
        self._supports_assets = True

        # Base URL from airflow.cfg
        base = conf.get("webserver", "base_url").rstrip("/")
        self.base_url = base
        self.api_base_url = f"{base}{api_prefix()}"

        # Airflow connection (username/password)
        self.conn = BaseHook.get_connection(conn_id)

        # HTTP session with retry strategy
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

        # Authentication mode
        # Airflow 3.x → JWT Bearer
        self._configure_bearer_auth()

    # -----------------------------------------------------------------------
    # Authentication for Airflow 3.x
    # -----------------------------------------------------------------------

    def _configure_bearer_auth(self) -> None:
        """
        Obtain a JWT token via POST /auth/token and configure Authorization header.
        """
        token_url = self.base_url + "/auth/token"

        payload = {}
        if self.conn.login and self.conn.password:
            payload = {"username": self.conn.login, "password": self.conn.password}

        log.debug("Requesting JWT token from %s", token_url)
        resp = requests.post(
            token_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=self.timeout,
        )

        if not resp.ok:
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
            raise RuntimeError(f"JWT token not found in response: {data}")

        self.session.headers["Authorization"] = f"Bearer {token}"

    # -----------------------------------------------------------------------
    # Internal helpers
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
        log.debug("Airflow API %s %s params=%s json=%s", method, url, params, json)

        resp = self.session.request(
            method=method,
            url=url,
            params=params,
            json=json,
            timeout=self.timeout,
        )

        # Resource not found
        if resp.status_code == 404:
            log.debug("Airflow API 404 Not Found for %s", url)
            return None

        # No content
        if resp.status_code == 204:
            return None

        # Other errors
        if not (200 <= resp.status_code < 300):
            log.error(
                "Airflow API error %s %s: %s",
                resp.status_code,
                url,
                resp.text[:1000],
            )
            raise RuntimeError(
                f"Airflow API error {resp.status_code} for {url}: {resp.text}"
            )

        # Normal JSON response
        return to_dotdict(resp.json())

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", path, params=params)

    # -----------------------------------------------------------------------
    # DAG Runs
    # -----------------------------------------------------------------------

    def list_dag_runs(self, dag_id: str, **params) -> List[DotDict]:
        """
        List DagRuns for a given DAG.
        """
        resp = self._get(f"dags/{dag_id}/dagRuns", params=params)
        return resp.dag_runs

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> DotDict:
        return self._get(f"dags/{dag_id}/dagRuns/{dag_run_id}")

    # -----------------------------------------------------------------------
    # Task Instances
    # -----------------------------------------------------------------------

    def list_task_instances(self, dag_id: str, dag_run_id: str, **params) -> List[DotDict]:
        resp = self._get(f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances", params=params)
        return resp.task_instances

    # -----------------------------------------------------------------------
    # Assets (Airflow 3.x)
    # -----------------------------------------------------------------------
    def get_dataset_by_uri(self, uri: str) -> Optional[DotDict]:
        """
        Unified interface for dataset (Airflow 2.4+) and asset (Airflow 3.x) by URI.
        """
        return self._get(f"assets/{uri}")

    def list_events(self, **params) -> List[DotDict]:
        """
        Unified interface for dataset events (Airflow 2.4+) and asset events (Airflow 3.x).
        """
        resp = self._get("assets/events", params=params)
        return resp.events

