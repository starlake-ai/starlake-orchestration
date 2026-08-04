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

"""Unit tests for the Airflow 2/3 compatibility layer and StarlakeAirflowApiClient.

The HTTP layer is fully mocked so both the Airflow 2.x (datasets, /api/v1,
basic auth) and Airflow 3.x (assets, /api/v2, JWT) code paths can be exercised
regardless of the Airflow version installed in the test environment.
"""

from __future__ import annotations

import json

import pytest

airflow = pytest.importorskip("airflow")

from ai.starlake.airflow import compat
from ai.starlake.airflow import starlake_airflow_api as api_module
from ai.starlake.airflow.starlake_airflow_api import StarlakeAirflowApiClient


BASE_URL = "http://localhost:8080"


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)
        self.ok = 200 <= status_code < 300

    def json(self):
        return self._payload


class FakeConnection:
    login = "user"
    password = "pass"


class FakeConf:
    @staticmethod
    def get(section, key):
        assert (section, key) == ("webserver", "base_url")
        return BASE_URL


@pytest.fixture
def make_client(monkeypatch):
    """Build a StarlakeAirflowApiClient for a given Airflow version with a mocked HTTP layer.

    ``responses`` maps a URL substring to the JSON payload returned for it;
    unmatched requests get a 404. Requests are recorded on ``client.calls``.
    """

    def _make(version: str, responses=None):
        monkeypatch.setattr(airflow, "__version__", version)
        monkeypatch.setattr(api_module, "conf", FakeConf())
        monkeypatch.setattr(
            api_module.BaseHook, "get_connection", staticmethod(lambda conn_id: FakeConnection())
        )
        monkeypatch.setattr(
            api_module.requests, "post", lambda *args, **kwargs: FakeResponse(200, {"access_token": "tok"})
        )

        client = StarlakeAirflowApiClient()
        # force the REST transport: these tests exercise the HTTP code paths
        client._db_available = False

        calls = []

        def fake_request(method, url, params=None, json=None, timeout=None):
            calls.append({"method": method, "url": url, "params": params, "json": json})
            for fragment, payload in (responses or {}).items():
                if fragment in url:
                    return FakeResponse(200, payload)
            return FakeResponse(404, {})

        monkeypatch.setattr(client.session, "request", fake_request)
        client.calls = calls
        return client

    return _make


# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "version, datasets, inlet_events, assets, prefix",
    [
        ("2.3.4", False, False, False, "/api/v1"),
        ("2.4.0", True, False, False, "/api/v1"),
        ("2.10.5", True, True, False, "/api/v1"),
        ("3.0.2", False, True, True, "/api/v2"),
    ],
)
def test_version_helpers(monkeypatch, version, datasets, inlet_events, assets, prefix):
    monkeypatch.setattr(airflow, "__version__", version)
    assert compat.supports_datasets() is datasets
    assert compat.supports_inlet_events() is inlet_events
    assert compat.supports_assets() is assets
    assert compat.api_prefix() == prefix


# ---------------------------------------------------------------------------
# Authentication and API prefix selection
# ---------------------------------------------------------------------------

def test_basic_auth_and_v1_prefix_on_airflow_2(make_client):
    client = make_client("2.10.5")
    assert client.session.auth == ("user", "pass")
    assert "Authorization" not in client.session.headers
    assert client.api_base_url == f"{BASE_URL}/api/v1"


def test_bearer_auth_and_v2_prefix_on_airflow_3(make_client):
    client = make_client("3.0.2")
    assert client.session.auth is None
    assert client.session.headers["Authorization"] == "Bearer tok"
    assert client.api_base_url == f"{BASE_URL}/api/v2"


# ---------------------------------------------------------------------------
# get_dataset_by_uri
# ---------------------------------------------------------------------------

def test_get_dataset_by_uri_percent_encodes_on_airflow_2(make_client):
    uri = "s3://bucket/table"
    client = make_client("2.10.5", responses={"/datasets/": {"id": 42, "uri": uri}})

    dataset = client.get_dataset_by_uri(uri)

    assert dataset.id == 42
    assert client.calls[0]["url"] == f"{BASE_URL}/api/v1/datasets/s3%3A%2F%2Fbucket%2Ftable"


def test_get_dataset_by_uri_resolves_uri_pattern_on_airflow_3(make_client):
    uri = "s3://bucket/table"
    client = make_client(
        "3.0.2",
        responses={
            "/assets": {
                "assets": [
                    {"id": 1, "uri": uri},
                    {"id": 2, "uri": f"{uri}_2"},
                ],
                "total_entries": 2,
            }
        },
    )

    asset = client.get_dataset_by_uri(uri)

    assert asset.id == 1
    call = client.calls[0]
    assert call["url"] == f"{BASE_URL}/api/v2/assets"
    assert call["params"] == {"uri_pattern": uri}


def test_get_dataset_by_uri_returns_none_without_exact_match_on_airflow_3(make_client):
    client = make_client(
        "3.0.2",
        responses={"/assets": {"assets": [{"id": 2, "uri": "s3://bucket/other"}], "total_entries": 1}},
    )
    assert client.get_dataset_by_uri("s3://bucket/table") is None


def test_get_dataset_by_uri_returns_none_on_404(make_client):
    client = make_client("2.10.5")
    assert client.get_dataset_by_uri("s3://bucket/missing") is None


def test_get_dataset_by_uri_raises_below_airflow_2_4(make_client):
    client = make_client("2.3.4")
    with pytest.raises(RuntimeError):
        client.get_dataset_by_uri("s3://bucket/table")


# ---------------------------------------------------------------------------
# list_events
# ---------------------------------------------------------------------------

def test_list_events_reads_asset_events_and_translates_dataset_id_on_airflow_3(make_client):
    client = make_client(
        "3.0.2",
        responses={"/assets/events": {"asset_events": [{"id": 1}], "total_entries": 1}},
    )

    events = client.list_events(dataset_id=5, limit=10, timestamp_lte="2026-07-02T00:00:00+00:00")

    assert [event.id for event in events] == [1]
    params = client.calls[0]["params"]
    assert params["asset_id"] == 5
    assert "dataset_id" not in params
    assert params["timestamp_lte"] == "2026-07-02T00:00:00+00:00"


def test_list_events_reads_dataset_events_and_filters_timestamp_client_side_on_airflow_2(make_client):
    client = make_client(
        "2.10.5",
        responses={
            "/datasets/events": {
                "dataset_events": [
                    {"id": 1, "timestamp": "2026-07-01T00:00:00+00:00"},
                    {"id": 2, "timestamp": "2026-07-02T00:00:00Z"},
                    {"id": 3, "timestamp": "2026-07-03T00:00:00+00:00"},
                ],
                "total_entries": 3,
            }
        },
    )

    events = client.list_events(asset_id=5, timestamp_lte="2026-07-02T00:00:00+00:00")

    assert [event.id for event in events] == [1, 2]
    params = client.calls[0]["params"]
    assert params["dataset_id"] == 5
    assert "asset_id" not in params
    # the v1 API has no timestamp filters: they must not be sent
    assert "timestamp_lte" not in params


def test_list_events_raises_below_airflow_2_4(make_client):
    client = make_client("2.3.4")
    with pytest.raises(RuntimeError):
        client.list_events(asset_id=5)


# ---------------------------------------------------------------------------
# list_dag_runs
# ---------------------------------------------------------------------------

def test_list_dag_runs_filters_data_interval_end_client_side_on_airflow_2(make_client):
    client = make_client(
        "2.10.5",
        responses={
            "/dagRuns": {
                "dag_runs": [
                    {"run_id": "a", "data_interval_end": "2026-07-01T00:00:00+00:00"},
                    {"run_id": "b", "data_interval_end": "2026-07-02T00:00:00+00:00"},
                    {"run_id": "c", "data_interval_end": "2026-07-03T00:00:00+00:00"},
                ],
                "total_entries": 3,
            }
        },
    )

    runs = client.list_dag_runs(
        "my_dag",
        data_interval_end_gt="2026-07-01T00:00:00+00:00",
        data_interval_end_lte="2026-07-02T00:00:00+00:00",
        limit=1000,
    )

    assert [run.run_id for run in runs] == ["b"]
    params = client.calls[0]["params"]
    # the v1 API rejects unknown query parameters: filters must not be sent
    assert "data_interval_end_gt" not in params
    assert "data_interval_end_lte" not in params
    # requests are paged: the page size is capped at the API maximum (100)
    assert params["limit"] == 100


def test_list_dag_runs_passes_data_interval_end_filters_through_on_airflow_3(make_client):
    client = make_client(
        "3.0.2",
        responses={
            "/dagRuns": {
                "dag_runs": [{"run_id": "a", "data_interval_end": "2026-07-01T00:00:00+00:00"}],
                "total_entries": 1,
            }
        },
    )

    runs = client.list_dag_runs(
        "my_dag",
        data_interval_end_lte="2026-07-02T00:00:00+00:00",
        order_by=["-data_interval_end", "-start_date"],
    )

    assert [run.run_id for run in runs] == ["a"]
    params = client.calls[0]["params"]
    # the v2 API has no data_interval_end filter/sort: run_after is its
    # documented Airflow 3 equivalent (multi-criteria sort supported)
    assert params["run_after_lte"] == "2026-07-02T00:00:00+00:00"
    assert "data_interval_end_lte" not in params
    assert params["order_by"] == ["-run_after", "-start_date"]


def test_list_dag_runs_backfills_data_interval_end_from_run_after_on_airflow_3(make_client):
    """
    An Airflow 3 run without a data interval (asset-triggered, manually
    triggered) has a NULL data_interval_end; run_after carries its
    schedule-equivalent timestamp and must be exposed under both names, or
    every consumer of data_interval_end reads None (issue #145).
    """
    client = make_client(
        "3.0.2",
        responses={
            "/dagRuns": {
                # the v2 listing serializes the id as dag_run_id, like v1
                "dag_runs": [
                    {
                        "dag_run_id": "asset_triggered__2026-07-25T07:45:27+00:00_LYIrISJQ",
                        "data_interval_end": None,
                        "run_after": "2026-07-25T07:45:27+00:00",
                    },
                    {
                        "dag_run_id": "scheduled__2026-07-24T06:00:00+00:00",
                        "data_interval_end": "2026-07-25T06:00:00+00:00",
                        "run_after": "2026-07-25T06:00:00+00:00",
                    },
                ],
                "total_entries": 2,
            }
        },
    )

    runs = client.list_dag_runs("my_dag")

    # the asset-triggered run reports run_after as its data_interval_end...
    assert runs[0].data_interval_end == "2026-07-25T07:45:27+00:00"
    assert runs[0].run_after == "2026-07-25T07:45:27+00:00"
    assert runs[0].run_id == "asset_triggered__2026-07-25T07:45:27+00:00_LYIrISJQ"
    # ...while a scheduled run keeps its own interval end untouched
    assert runs[1].data_interval_end == "2026-07-25T06:00:00+00:00"


def test_get_dag_run_backfills_data_interval_end_from_run_after_on_airflow_3(make_client):
    """Single-run reads go through the same normalization as listings."""
    client = make_client(
        "3.0.2",
        responses={
            "/dagRuns/asset_triggered__x": {
                "dag_run_id": "asset_triggered__x",
                "data_interval_end": None,
                "run_after": "2026-07-25T07:45:27+00:00",
            }
        },
    )

    run = client.get_dag_run("my_dag", "asset_triggered__x")

    assert run.data_interval_end == "2026-07-25T07:45:27+00:00"
    assert run.run_id == "asset_triggered__x"


def test_list_dag_runs_aliases_v1_dag_run_id(make_client):
    """The v1 API names the field dag_run_id; both spellings must be exposed."""
    client = make_client(
        "2.10.5",
        responses={
            "/dagRuns": {
                "dag_runs": [{"dag_run_id": "a", "end_date": "2026-07-01T01:00:00+00:00"}],
                "total_entries": 1,
            }
        },
    )
    runs = client.list_dag_runs("my_dag", order_by=["-data_interval_end", "-start_date"])
    assert runs[0].run_id == "a" and runs[0].dag_run_id == "a"
    # v1 accepts a single sort field and cannot sort by data_interval_end:
    # execution_date is its schedule-equivalent proxy
    assert client.calls[0]["params"]["order_by"] == "-execution_date"


def test_list_task_instances_v1_drops_order_by_and_filters_task_id(make_client):
    client = make_client(
        "2.10.5",
        responses={
            "/taskInstances": {
                "task_instances": [
                    {"task_id": "leaf", "dag_run_id": "a", "state": "skipped"},
                    {"task_id": "other", "dag_run_id": "a", "state": "skipped"},
                ],
                "total_entries": 2,
            }
        },
    )
    instances = client.list_dag_task_instances(
        "my_dag", task_id="leaf", state="skipped", order_by=["-end_date"], limit=10
    )
    assert [ti.task_id for ti in instances] == ["leaf"]
    params = client.calls[0]["params"]
    # the v1 taskInstances endpoints support neither order_by nor task_id
    assert "order_by" not in params and "task_id" not in params
    assert params["state"] == ["skipped"]


# ---------------------------------------------------------------------------
# DAG run mutations (trigger / delete) — pipeline lifecycle
# ---------------------------------------------------------------------------

def test_trigger_dag_run_on_airflow_2(make_client):
    client = make_client(
        "2.10.5",
        responses={"/dagRuns": {"dag_run_id": "manual_run_x", "state": "queued"}},
    )

    run = client.trigger_dag_run(
        "my_dag",
        dag_run_id="manual_run_x",
        logical_date="2026-07-13T00:00:00Z",
        conf={"backfill": True},
    )

    call = client.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == f"{BASE_URL}/api/v1/dags/my_dag/dagRuns"
    assert call["json"] == {
        "dag_run_id": "manual_run_x",
        "conf": {"backfill": True},
        "logical_date": "2026-07-13T00:00:00Z",
    }
    assert run.run_id == "manual_run_x"  # aliased like every dag-run object


def test_trigger_dag_run_on_airflow_3_sends_nullable_logical_date(make_client):
    client = make_client(
        "3.0.2",
        responses={"/dagRuns": {"dag_run_id": "manual_run_y", "state": "queued"}},
    )

    client.trigger_dag_run("my_dag", dag_run_id="manual_run_y")

    call = client.calls[0]
    assert call["url"] == f"{BASE_URL}/api/v2/dags/my_dag/dagRuns"
    # the v2 API requires the (nullable) logical_date key to be present
    assert call["json"] == {"dag_run_id": "manual_run_y", "logical_date": None}


def test_delete_dag(make_client):
    client = make_client("2.10.5", responses={"/dags/my_dag": {}})
    client.delete_dag("my_dag")
    call = client.calls[0]
    assert call["method"] == "DELETE"
    assert call["url"] == f"{BASE_URL}/api/v1/dags/my_dag"


def test_explicit_base_url_targets_remote_instance(monkeypatch):
    """An explicitly targeted instance uses its REST API with the given
    credentials and never the local metadata database."""
    monkeypatch.setattr(airflow, "__version__", "2.10.5")

    client = StarlakeAirflowApiClient(
        base_url="http://remote:8080", username="u", password="p"
    )

    assert client.api_base_url == "http://remote:8080/api/v1"
    assert client.session.auth == ("u", "p")
    assert client.db_available is False


# ---------------------------------------------------------------------------
# Transport selection: database first on Airflow 2, REST as fallback
# ---------------------------------------------------------------------------

def test_list_events_uses_database_when_available(make_client, monkeypatch):
    client = make_client("2.10.5", responses={"/datasets/events": {"dataset_events": []}})
    client._db_available = True
    sentinel = [{"id": 99}]
    monkeypatch.setattr(client, "_list_events_db", lambda **params: sentinel)

    events = client.list_events(asset_id=5, timestamp_lte="2026-07-02T00:00:00+00:00")

    assert events == sentinel
    assert client.calls == []  # no HTTP call


def test_list_events_falls_back_to_rest_on_database_error(make_client, monkeypatch):
    client = make_client(
        "2.10.5",
        responses={"/datasets/events": {"dataset_events": [{"id": 1, "timestamp": "2026-07-01T00:00:00+00:00"}]}},
    )
    client._db_available = True

    def boom(**params):
        raise RuntimeError("database gone")

    monkeypatch.setattr(client, "_list_events_db", boom)

    events = client.list_events(dataset_id=5)

    assert [event.id for event in events] == [1]
    assert len(client.calls) == 1  # REST fallback used


def test_client_tolerates_missing_connection_on_airflow_2(monkeypatch):
    monkeypatch.setattr(airflow, "__version__", "2.10.5")
    monkeypatch.setattr(api_module, "conf", FakeConf())

    def not_found(conn_id):
        raise RuntimeError(f"connection {conn_id} not defined")

    monkeypatch.setattr(api_module.BaseHook, "get_connection", staticmethod(not_found))

    client = StarlakeAirflowApiClient()

    assert client.conn is None
    assert client.session.auth is None


def test_db_available_is_false_on_airflow_3(make_client):
    client = make_client("3.0.2")
    client._db_available = None  # reset the forced value; the probe must not run
    assert client.db_available is False


def test_rest_requests_paginate_until_total_entries(make_client, monkeypatch):
    """A collection larger than the API page cap is fetched page by page."""
    client = make_client("2.10.5")
    pages = {
        0: [{"dag_run_id": f"run_{i}"} for i in range(100)],
        100: [{"dag_run_id": f"run_{i}"} for i in range(100, 150)],
    }

    def paged_request(method, url, params=None, json=None, timeout=None):
        batch = pages.get(params["offset"], [])
        return FakeResponse(200, {"dag_runs": batch, "total_entries": 150})

    monkeypatch.setattr(client.session, "request", paged_request)

    runs = client.list_dag_runs("my_dag")

    assert len(runs) == 150
    assert runs[0].run_id == "run_0" and runs[-1].run_id == "run_149"


def test_find_dataset_events_rest_composition_on_airflow_3(make_client):
    """Events are crossed with their producing runs and sorted by the producing
    run's data_interval_end; the dataset is attached to each event."""
    uri = "s3://bucket/table"
    client = make_client(
        "3.0.2",
        responses={
            "/assets/events": {
                "asset_events": [
                    {"id": 1, "source_dag_id": "producer", "source_run_id": "new", "timestamp": "2026-07-02T00:00:00+00:00"},
                    {"id": 2, "source_dag_id": "producer", "source_run_id": "old", "timestamp": "2026-07-01T00:00:00+00:00"},
                    {"id": 3, "source_dag_id": "producer", "source_run_id": "outside", "timestamp": "2026-06-01T00:00:00+00:00"},
                ],
                "total_entries": 3,
            },
            "/assets": {"assets": [{"id": 7, "uri": uri}], "total_entries": 1},
            "/dagRuns": {
                "dag_runs": [
                    {"dag_id": "producer", "run_id": "old", "data_interval_end": "2026-07-01T00:00:00+00:00"},
                    {"dag_id": "producer", "run_id": "new", "data_interval_end": "2026-07-02T00:00:00+00:00"},
                ],
                "total_entries": 2,
            },
        },
    )

    events = client.find_dataset_events(
        uri,
        "2026-07-03T00:00:00+00:00",
        data_interval_end_gt="2026-06-30T00:00:00+00:00",
        data_interval_end_lte="2026-07-02T00:00:00+00:00",
    )

    # 'outside' has no producing run in the window; the rest sorted by the
    # producing run's data_interval_end ascending
    assert [event.id for event in events] == [2, 1]
    assert all(event.dataset.id == 7 for event in events)
    assert events[0].data_interval_end == "2026-07-01T00:00:00+00:00"


# ---------------------------------------------------------------------------
# Database transport against a real (isolated) sqlite metadata database
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    compat.supports_assets(),
    reason="Metadata database transport is Airflow 2 only (the client never "
    "touches the database on Airflow 3, and DatasetModel/DatasetEvent are gone)",
)
class TestDatabaseTransport:
    """Exercise the session-backed queries end-to-end: SQL filters, ordering,
    limits and the normalized DotDict shape shared with the REST transport."""

    @pytest.fixture(scope="class")
    def db_client(self, tmp_path_factory):
        import os
        from datetime import datetime, timedelta, timezone

        db_dir = tmp_path_factory.mktemp("api_client_db")
        previous = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
        os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{db_dir}/airflow.db"
        from airflow import settings
        try:
            settings.configure_vars()
            settings.configure_orm()
            from airflow.utils.db import initdb
            initdb(load_connections=False)

            from airflow.models import DagRun, TaskInstance
            from airflow.models.dataset import DatasetEvent, DatasetModel
            from airflow.utils.session import create_session
            from airflow.utils.state import DagRunState
            from airflow.utils.types import DagRunType

            base = datetime(2026, 7, 1, tzinfo=timezone.utc)
            with create_session() as session:
                dataset = DatasetModel(uri="s3://bucket/table")
                session.add(dataset)
                session.flush()
                dataset_id = dataset.id
                for i in range(3):
                    event = DatasetEvent(
                        dataset_id=dataset_id,
                        extra={},
                        source_dag_id="producer",
                        source_run_id=f"run_{i}",
                        source_task_id="task",
                        source_map_index=-1,
                    )
                    event.timestamp = base + timedelta(days=i)
                    session.add(event)
                for i in range(3):
                    run = DagRun(
                        dag_id="producer",
                        run_id=f"run_{i}",
                        execution_date=base + timedelta(days=i),
                        start_date=base + timedelta(days=i),
                        data_interval=(base + timedelta(days=i - 1), base + timedelta(days=i)),
                        run_type=DagRunType.MANUAL,
                        state=DagRunState.SUCCESS,
                    )
                    run.end_date = base + timedelta(days=i, hours=1)
                    session.add(run)
                # the raw insert below is a Core statement and does not autoflush
                # the pending DagRun rows its foreign key points to
                session.flush()
                # a SKIPPED leaf task instance on run_2, for the anti-join query
                session.execute(
                    TaskInstance.__table__.insert().values(
                        dag_id="producer",
                        task_id="leaf",
                        run_id="run_2",
                        map_index=-1,
                        state="skipped",
                        try_number=0,
                        max_tries=-1,
                        pool="default_pool",
                        pool_slots=1,
                        priority_weight=1,
                    )
                )

            client = StarlakeAirflowApiClient()

            def no_http(*args, **kwargs):
                raise AssertionError("HTTP must not be used when the database is available")

            client.session.request = no_http
            client.base = base
            client.dataset_id = dataset_id
            yield client
        finally:
            if previous is None:
                os.environ.pop("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", None)
            else:
                os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = previous
            settings.configure_vars()
            settings.configure_orm()

    def test_database_is_probed_available(self, db_client):
        assert db_client.db_available is True
        assert db_client.conn is None  # airflow_api connection not needed

    def test_get_dataset_by_uri_via_database(self, db_client):
        dataset = db_client.get_dataset_by_uri("s3://bucket/table")
        assert dataset.id == db_client.dataset_id
        assert dataset.uri == "s3://bucket/table"
        assert db_client.get_dataset_by_uri("s3://bucket/missing") is None

    def test_list_events_filters_timestamp_in_sql(self, db_client):
        from datetime import timedelta
        events = db_client.list_events(
            asset_id=db_client.dataset_id,  # translated to dataset_id
            timestamp_lte=(db_client.base + timedelta(days=1)).isoformat(),
            order_by="timestamp",
            limit=1000,
        )
        assert [event.source_run_id for event in events] == ["run_0", "run_1"]
        assert all(isinstance(event.timestamp, str) for event in events)
        assert events[0].dataset_uri == "s3://bucket/table"

    def test_list_dag_runs_filters_and_orders_in_sql(self, db_client):
        from datetime import timedelta
        runs = db_client.list_dag_runs(
            "producer",
            state="success",
            data_interval_end_gt=db_client.base.isoformat(),
            data_interval_end_lte=(db_client.base + timedelta(days=2)).isoformat(),
            order_by=["-data_interval_end", "-start_date"],
            limit=100,
        )
        assert [run.run_id for run in runs] == ["run_2", "run_1"]
        # both spellings exposed, like the normalized REST responses
        assert [run.dag_run_id for run in runs] == ["run_2", "run_1"]
        assert all(isinstance(run.data_interval_end, str) for run in runs)

    def test_list_dag_runs_end_date_lt_in_sql(self, db_client):
        from datetime import timedelta
        runs = db_client.list_dag_runs(
            "producer",
            end_date_lt=(db_client.base + timedelta(days=1)).isoformat(),
        )
        assert [run.run_id for run in runs] == ["run_0"]

    def test_find_dataset_events_joined_window(self, db_client):
        """The window filter applies to the producing run inside the join."""
        from datetime import timedelta
        events = db_client.find_dataset_events(
            "s3://bucket/table",
            db_client.base + timedelta(days=10),
            data_interval_end_gt=db_client.base.isoformat(),
            data_interval_end_lte=(db_client.base + timedelta(days=2)).isoformat(),
        )
        assert [event.source_run_id for event in events] == ["run_1", "run_2"]
        assert all(event.dataset.uri == "s3://bucket/table" for event in events)
        assert all(isinstance(event.data_interval_end, str) for event in events)

    def test_find_dataset_events_replay_past_window(self, db_client):
        """Replaying an arbitrarily old window finds its events regardless of recency."""
        from datetime import timedelta
        events = db_client.find_dataset_events(
            "s3://bucket/table",
            db_client.base + timedelta(days=10),
            data_interval_end_gte=(db_client.base - timedelta(days=1)).isoformat(),
            data_interval_end_lte=db_client.base.isoformat(),
        )
        assert [event.source_run_id for event in events] == ["run_0"]

    def test_find_previous_dag_runs_anti_join(self, db_client):
        from datetime import timedelta
        runs = db_client.find_previous_dag_runs(
            "producer",
            db_client.base + timedelta(days=10),
            ["leaf"],
            at_scheduled_date=True,
        )
        # run_2 is excluded: its leaf task instance is SKIPPED
        assert [run.run_id for run in runs] == ["run_1", "run_0"]

    def test_find_previous_dag_runs_before_scheduled_date(self, db_client):
        from datetime import timedelta
        runs = db_client.find_previous_dag_runs(
            "producer",
            db_client.base + timedelta(days=1, hours=12),
            [],
            at_scheduled_date=False,
        )
        assert [run.run_id for run in runs] == ["run_1", "run_0"]
