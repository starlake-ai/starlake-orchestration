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
    Client for querying Airflow metadata (datasets/assets, events, DAG runs,
    task instances).

    - Airflow 2.x:
        * Metadata database queried directly by default (server-side filters,
          no page-size cap, no webserver dependency)
        * REST /api/v1 as fallback when the database is unreachable:
          Basic Auth (username/password from the optional ``airflow_api``
          connection); requires ``airflow.api.auth.backend.basic_auth``
          in ``[api] auth_backends``
        * Datasets

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
        self._db_available: Optional[bool] = None

        # Base URL from airflow.cfg
        base = conf.get("webserver", "base_url").rstrip("/")
        self.base_url = base
        self.api_base_url = f"{base}{api_prefix()}"

        # Airflow connection (username/password)
        try:
            self.conn = BaseHook.get_connection(conn_id)
        except Exception as e:
            if self._supports_assets:
                raise
            # Airflow 2.x works database-first: the connection is only
            # needed by the REST fallback
            log.info(
                "Airflow connection '%s' not found (%s); the REST API fallback will be unauthenticated",
                conn_id,
                e,
            )
            self.conn = None

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
        elif self.conn:
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
    # Airflow 2.x metadata database access
    # -----------------------------------------------------------------------
    # On Airflow 2 tasks run with direct access to the metadata database (the
    # platform's own execution model). The database supports the server-side
    # filters the v1 REST API lacks (timestamp, data_interval_end, end_date_lt,
    # order_by/task_id on task instances) and is not subject to the API
    # page-size cap ([api] maximum_page_limit, default 100). It is therefore
    # used by default, with the REST API kept as fallback. On Airflow 3 the
    # Task SDK has no database access and the /api/v2 REST API is always used.

    @property
    def db_available(self) -> bool:
        """Whether the Airflow 2 metadata database is reachable (probed once)."""
        if self._supports_assets:
            return False
        if self._db_available is None:
            try:
                from sqlalchemy import text

                from airflow.utils.session import create_session
                with create_session() as session:
                    session.execute(text("SELECT 1"))
                self._db_available = True
            except Exception as e:
                log.warning(
                    "Airflow metadata database not reachable (%s); falling back to the REST API",
                    e,
                )
                self._db_available = False
        return self._db_available

    @staticmethod
    def _iso(value: Any) -> Optional[str]:
        if value is None or isinstance(value, str):
            return value
        return value.isoformat()

    @staticmethod
    def _order_query(query, model, order_by) -> Any:
        """Apply one or more '[-]column' order clauses to a SQLAlchemy query."""
        fields = order_by if isinstance(order_by, (list, tuple)) else [order_by] if order_by else []
        for field in fields:
            field = str(field)
            column = getattr(model, field.lstrip("-"), None)
            if column is not None:
                query = query.order_by(column.desc() if field.startswith("-") else column.asc())
        return query

    def _dag_run_to_dotdict(self, run) -> DotDict:
        return DotDict({
            "dag_id": run.dag_id,
            "run_id": run.run_id,
            "dag_run_id": run.run_id,
            "state": str(run.state) if run.state is not None else None,
            "execution_date": self._iso(run.execution_date),
            "logical_date": self._iso(run.execution_date),
            "start_date": self._iso(run.start_date),
            "end_date": self._iso(run.end_date),
            "data_interval_start": self._iso(run.data_interval_start),
            "data_interval_end": self._iso(run.data_interval_end),
        })

    def _event_to_dotdict(self, event) -> DotDict:
        dataset = getattr(event, "dataset", None)
        return DotDict({
            "id": event.id,
            "dataset_id": event.dataset_id,
            "dataset_uri": dataset.uri if dataset is not None else None,
            "extra": to_dotdict(event.extra) if event.extra else {},
            "source_dag_id": event.source_dag_id,
            "source_task_id": event.source_task_id,
            "source_run_id": event.source_run_id,
            "source_map_index": event.source_map_index,
            "timestamp": self._iso(event.timestamp),
        })

    def _task_instance_to_dotdict(self, ti) -> DotDict:
        return DotDict({
            "dag_id": ti.dag_id,
            "dag_run_id": ti.run_id,
            "task_id": ti.task_id,
            "state": str(ti.state) if ti.state is not None else None,
            "start_date": self._iso(ti.start_date),
            "end_date": self._iso(ti.end_date),
        })

    def _get_dataset_by_uri_db(self, uri: str) -> Optional[DotDict]:
        from airflow.models.dataset import DatasetModel
        from airflow.utils.session import create_session
        with create_session() as session:
            row = session.query(DatasetModel).filter(DatasetModel.uri == uri).one_or_none()
            if not row:
                return None
            return DotDict({
                "id": row.id,
                "uri": row.uri,
                "extra": to_dotdict(row.extra) if row.extra else {},
            })

    def _list_events_db(self, **params) -> List[DotDict]:
        from sqlalchemy.orm import joinedload

        from airflow.models.dataset import DatasetEvent
        from airflow.utils.session import create_session
        with create_session() as session:
            query = session.query(DatasetEvent).options(joinedload(DatasetEvent.dataset))
            if params.get("dataset_id") is not None:
                query = query.filter(DatasetEvent.dataset_id == params["dataset_id"])
            if params.get("timestamp_gte") is not None:
                query = query.filter(DatasetEvent.timestamp >= _as_datetime(params["timestamp_gte"]))
            if params.get("timestamp_lte") is not None:
                query = query.filter(DatasetEvent.timestamp <= _as_datetime(params["timestamp_lte"]))
            query = self._order_query(query, DatasetEvent, params.get("order_by", "timestamp"))
            if params.get("limit"):
                query = query.limit(int(params["limit"]))
            return [self._event_to_dotdict(row) for row in query.all()]

    def _list_dag_runs_db(self, dag_id: str, **params) -> List[DotDict]:
        from airflow.models import DagRun
        from airflow.utils.session import create_session
        with create_session() as session:
            query = session.query(DagRun).filter(DagRun.dag_id == dag_id)
            state = params.get("state")
            if state is not None:
                states = [str(s) for s in (state if isinstance(state, (list, tuple)) else [state])]
                query = query.filter(DagRun.state.in_(states))
            if params.get("end_date_lt") is not None:
                query = query.filter(DagRun.end_date < _as_datetime(params["end_date_lt"]))
            if params.get("end_date_lte") is not None:
                query = query.filter(DagRun.end_date <= _as_datetime(params["end_date_lte"]))
            if params.get("data_interval_end_gt") is not None:
                query = query.filter(DagRun.data_interval_end > _as_datetime(params["data_interval_end_gt"]))
            if params.get("data_interval_end_gte") is not None:
                query = query.filter(DagRun.data_interval_end >= _as_datetime(params["data_interval_end_gte"]))
            if params.get("data_interval_end_lte") is not None:
                query = query.filter(DagRun.data_interval_end <= _as_datetime(params["data_interval_end_lte"]))
            query = self._order_query(query, DagRun, params.get("order_by"))
            if params.get("limit"):
                query = query.limit(int(params["limit"]))
            return [self._dag_run_to_dotdict(row) for row in query.all()]

    def _list_task_instances_db(self, dag_id: str, dag_run_id: Optional[str] = None, **params) -> List[DotDict]:
        from airflow.models import TaskInstance
        from airflow.utils.session import create_session
        with create_session() as session:
            query = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id)
            if dag_run_id is not None:
                query = query.filter(TaskInstance.run_id == dag_run_id)
            if params.get("task_id") is not None:
                query = query.filter(TaskInstance.task_id == params["task_id"])
            state = params.get("state")
            if state is not None:
                states = [str(s) for s in (state if isinstance(state, (list, tuple)) else [state])]
                query = query.filter(TaskInstance.state.in_(states))
            if params.get("end_date_lte") is not None:
                query = query.filter(TaskInstance.end_date <= _as_datetime(params["end_date_lte"]))
            query = self._order_query(query, TaskInstance, params.get("order_by"))
            if params.get("limit"):
                query = query.limit(int(params["limit"]))
            return [self._task_instance_to_dotdict(row) for row in query.all()]

    # -----------------------------------------------------------------------
    # DAG Runs
    # -----------------------------------------------------------------------

    @staticmethod
    def _first_order_by(order_by) -> Optional[str]:
        """The REST APIs accept a single order_by field; keep the first one."""
        if isinstance(order_by, (list, tuple)):
            return str(order_by[0]) if order_by else None
        return order_by

    @staticmethod
    def _alias_run_ids(run: DotDict) -> DotDict:
        """Expose both run_id (v2/database naming) and dag_run_id (v1 naming)."""
        if run.get("run_id") is None and run.get("dag_run_id") is not None:
            run["run_id"] = run["dag_run_id"]
        elif run.get("dag_run_id") is None and run.get("run_id") is not None:
            run["dag_run_id"] = run["run_id"]
        return run

    def list_dag_runs(self, dag_id: str, **params) -> List[DotDict]:
        """
        List DagRuns for a given DAG.

        On Airflow 2 the metadata database is queried by default. The REST
        fallback applies ``data_interval_end_gt/gte/lte`` and ``end_date_lt``
        filters client-side on the v1 API (which rejects unknown query
        parameters).
        """
        params = dict(params)
        if self.db_available:
            try:
                return self._list_dag_runs_db(dag_id, **params)
            except Exception as e:
                log.warning("Metadata database query failed (%s); falling back to the REST API", e)
        order_by = self._first_order_by(params.pop("order_by", None))
        if order_by is not None:
            params["order_by"] = order_by
        di_gt = di_gte = di_lte = ed_lt = None
        if not self._supports_assets:
            di_gt = params.pop("data_interval_end_gt", None)
            di_gte = params.pop("data_interval_end_gte", None)
            di_lte = params.pop("data_interval_end_lte", None)
            ed_lt = params.pop("end_date_lt", None)
        resp = self._get(f"dags/{dag_id}/dagRuns", params=params)
        runs = [self._alias_run_ids(r) for r in ((resp.dag_runs if resp else None) or [])]
        if di_gt is not None:
            gt = _as_datetime(di_gt)
            runs = [r for r in runs if r.data_interval_end and _as_datetime(r.data_interval_end) > gt]
        if di_gte is not None:
            gte = _as_datetime(di_gte)
            runs = [r for r in runs if r.data_interval_end and _as_datetime(r.data_interval_end) >= gte]
        if di_lte is not None:
            lte = _as_datetime(di_lte)
            runs = [r for r in runs if r.data_interval_end and _as_datetime(r.data_interval_end) <= lte]
        if ed_lt is not None:
            lt = _as_datetime(ed_lt)
            runs = [r for r in runs if r.end_date and _as_datetime(r.end_date) < lt]
        return runs

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> DotDict:
        return self._get(f"dags/{dag_id}/dagRuns/{dag_run_id}")

    # -----------------------------------------------------------------------
    # Task Instances
    # -----------------------------------------------------------------------

    def list_task_instances(self, dag_id: str, dag_run_id: str, **params) -> List[DotDict]:
        if self.db_available:
            try:
                return self._list_task_instances_db(dag_id, dag_run_id, **params)
            except Exception as e:
                log.warning("Metadata database query failed (%s); falling back to the REST API", e)
        params = self._rest_task_instance_params(params)
        task_id = params.pop("task_id", None)
        resp = self._get(f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances", params=params)
        instances = (resp.task_instances if resp else None) or []
        if task_id is not None:
            instances = [ti for ti in instances if ti.task_id == task_id]
        return instances

    def list_dag_task_instances(self, dag_id: str, **params) -> List[DotDict]:
        """
        List task instances for a given DAG across all runs.
        """
        if self.db_available:
            try:
                return self._list_task_instances_db(dag_id, **params)
            except Exception as e:
                log.warning("Metadata database query failed (%s); falling back to the REST API", e)
        params = self._rest_task_instance_params(params)
        task_id = params.pop("task_id", None)
        resp = self._get(f"dags/{dag_id}/dagRuns/~/taskInstances", params=params)
        instances = (resp.task_instances if resp else None) or []
        if task_id is not None:
            instances = [ti for ti in instances if ti.task_id == task_id]
        return instances

    def _rest_task_instance_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Adapt task-instance filters for the REST APIs.

        The v1 API has no ``order_by`` and no ``task_id`` filter on the
        taskInstances endpoints: ``order_by`` is dropped (callers only
        membership-test the results) and ``task_id`` is filtered client-side.
        """
        params = dict(params)
        if self._supports_assets:
            order_by = self._first_order_by(params.pop("order_by", None))
            if order_by is not None:
                params["order_by"] = order_by
        else:
            params.pop("order_by", None)
        state = params.get("state")
        if state is not None:
            params["state"] = [str(s) for s in (state if isinstance(state, (list, tuple)) else [state])]
        return params

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
            if self.db_available:
                try:
                    return self._get_dataset_by_uri_db(uri)
                except Exception as e:
                    log.warning("Metadata database query failed (%s); falling back to the REST API", e)
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
            if self.db_available:
                try:
                    return self._list_events_db(**params)
                except Exception as e:
                    log.warning("Metadata database query failed (%s); falling back to the REST API", e)
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
