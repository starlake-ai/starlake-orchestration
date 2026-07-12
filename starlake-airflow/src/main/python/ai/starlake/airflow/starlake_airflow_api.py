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
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.configuration import conf

from ai.starlake.airflow.compat import (
    BaseHook,
    airflow_version,
    api_prefix,
    supports_assets,
    supports_datasets,
    supports_inlet_events,
)

log = logging.getLogger(__name__)


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


def _as_datetime(value: Any) -> datetime:
    """Parse an API timestamp (ISO 8601, possibly 'Z'-suffixed) into an aware datetime."""
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# ---------------------------------------------------------------------------
# Airflow API Client (supports Airflow 2 & 3)
# ---------------------------------------------------------------------------

class StarlakeAirflowApiClient(BaseHook):
    """
    Client for interacting with the Airflow's public API (v1/v2).

    - Airflow 2.x:
        * Basic Auth (username/password from Airflow connection);
          requires ``airflow.api.auth.backend.basic_auth`` in ``[api] auth_backends``
        * Datasets
        * API prefix: /api/v1

    - Airflow 3.x:
        * Bearer JWT token (POST /auth/token)
        * Assets instead of datasets
        * API prefix: /api/v2

    Features:
        * Automatic version detection
        * Automatic authentication mode
        * Automatic endpoint selection (datasets vs assets)
        * Retry logic
        * DotDict responses
    """

    def __init__(
            self,
            conn_id: str = "airflow_api",
            timeout: int = 30,
            max_retries: int = 3,
    ) -> None:
        super().__init__()
        self.timeout = timeout
        self._supports_datasets = supports_datasets()
        self._supports_assets = supports_assets()


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
        if self._supports_assets:
            # Airflow 3.x → JWT Bearer
            self._configure_bearer_auth()
        else:
            # Airflow 2.x → Basic Auth
            self.session.auth = (self.conn.login, self.conn.password)

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

        ``data_interval_end_gt/gte/lte`` filters are passed through to the
        Airflow 3.x API and applied client-side on Airflow 2.x (the v1 API
        rejects unknown query parameters).
        """
        params = dict(params)
        di_gt = di_gte = di_lte = None
        if not self._supports_assets:
            di_gt = params.pop("data_interval_end_gt", None)
            di_gte = params.pop("data_interval_end_gte", None)
            di_lte = params.pop("data_interval_end_lte", None)
        resp = self._get(f"dags/{dag_id}/dagRuns", params=params)
        runs = (resp.dag_runs if resp else None) or []
        if di_gt is not None:
            gt = _as_datetime(di_gt)
            runs = [r for r in runs if r.data_interval_end and _as_datetime(r.data_interval_end) > gt]
        if di_gte is not None:
            gte = _as_datetime(di_gte)
            runs = [r for r in runs if r.data_interval_end and _as_datetime(r.data_interval_end) >= gte]
        if di_lte is not None:
            lte = _as_datetime(di_lte)
            runs = [r for r in runs if r.data_interval_end and _as_datetime(r.data_interval_end) <= lte]
        return runs

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> DotDict:
        return self._get(f"dags/{dag_id}/dagRuns/{dag_run_id}")

    # -----------------------------------------------------------------------
    # Task Instances
    # -----------------------------------------------------------------------

    def list_task_instances(self, dag_id: str, dag_run_id: str, **params) -> List[DotDict]:
        resp = self._get(f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances", params=params)
        return (resp.task_instances if resp else None) or []

    def list_dag_task_instances(self, dag_id: str, **params) -> List[DotDict]:
        """
        List IDs of task instances for a given DAG across all runs.
        Ideally uses /dags/{dag_id}/dagRuns/~/taskInstances if available.
        """
        resp = self._get(f"dags/{dag_id}/dagRuns/~/taskInstances", params=params)
        return (resp.task_instances if resp else None) or []

    # -----------------------------------------------------------------------
    # Datasets (Airflow 2.4+) / Assets (Airflow 3.x)
    # -----------------------------------------------------------------------
    def get_dataset_by_uri(self, uri: str) -> Optional[DotDict]:
        """
        Unified interface for dataset (Airflow 2.4+) and asset (Airflow 3.x) by URI.
        """
        if self._supports_assets:
            # Airflow 3.x → assets are fetched by numeric id; resolve the URI
            # through the uri_pattern filter and match exactly
            resp = self._get("assets", params={"uri_pattern": uri})
            assets = (resp.assets if resp else None) or []
            return next((asset for asset in assets if asset.uri == uri), None)

        if self._supports_datasets:
            # Airflow 2.4+ → datasets are fetched by their percent-encoded URI
            return self._get(f"datasets/{quote(uri, safe='')}")

        raise RuntimeError("Datasets are not supported on this Airflow version.")

    def list_events(self, **params) -> List[DotDict]:
        """
        Unified interface for dataset events (Airflow 2.4+) and asset events (Airflow 3.x).

        Accepts either ``asset_id`` or ``dataset_id`` and translates it to the
        parameter expected by the underlying API. ``timestamp_gte``/``timestamp_lte``
        are supported natively by the Airflow 3.x API and applied client-side on
        Airflow 2.x (the v1 API has no timestamp filters).
        """
        params = dict(params)
        if self._supports_assets:
            # Airflow 3.x → assets
            if "dataset_id" in params:
                params["asset_id"] = params.pop("dataset_id")
            resp = self._get("assets/events", params=params)
            return ((resp.asset_events if resp else None) or [])

        if self._supports_datasets:
            # Airflow 2.4+ → datasets
            if "asset_id" in params:
                params["dataset_id"] = params.pop("asset_id")
            timestamp_gte = params.pop("timestamp_gte", None)
            timestamp_lte = params.pop("timestamp_lte", None)
            resp = self._get("datasets/events", params=params)
            events = (resp.dataset_events if resp else None) or []
            if timestamp_gte is not None:
                gte = _as_datetime(timestamp_gte)
                events = [e for e in events if e.timestamp and _as_datetime(e.timestamp) >= gte]
            if timestamp_lte is not None:
                lte = _as_datetime(timestamp_lte)
                events = [e for e in events if e.timestamp and _as_datetime(e.timestamp) <= lte]
            return events

        raise RuntimeError("Datasets are not supported on this Airflow version.")
