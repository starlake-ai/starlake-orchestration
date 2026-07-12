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
    assert params["limit"] == 1000


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

    runs = client.list_dag_runs("my_dag", data_interval_end_lte="2026-07-02T00:00:00+00:00")

    assert [run.run_id for run in runs] == ["a"]
    assert client.calls[0]["params"]["data_interval_end_lte"] == "2026-07-02T00:00:00+00:00"
