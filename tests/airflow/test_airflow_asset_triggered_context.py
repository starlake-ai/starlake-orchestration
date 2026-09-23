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
"""Issue #139 — asset-triggered transform DAGs on Airflow 3.

Two version-portability gaps, pinned here:

1. The Airflow 3 task SDK hands ``triggering_asset_events`` as a mapping of
   ``Asset``/``AssetAlias`` OBJECTS to ``AssetEventDagRunReference(Result)``
   lists — the previous recognition only accepted URI-string keys and
   ``AssetEvent``/``DatasetEvent`` class names, so every asset-triggered run
   fell back to "No triggering datasets found. Manually triggered.".
2. An Airflow 3 asset-triggered run has NO data interval and the SDK omits
   ``data_interval_end`` from the Jinja context entirely — templates
   referencing it crashed at render time (``UndefinedError``) before the
   ``ts_as_datetime``/``sl_dates`` macros could apply their XCom fallback.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import jinja2
import pytest

from ai.starlake.airflow.starlake_airflow_job import triggering_datasets_from_events
from ai.starlake.common import StarlakeParameters


def _event(cls_name: str, extra: dict, ts: datetime):
    """Build an event object whose class NAME mimics the orchestrator model."""
    cls = type(cls_name, (), {})
    event = cls()
    event.extra = extra
    event.timestamp = ts
    return event


TS_1 = datetime(2026, 7, 25, 6, 0, tzinfo=timezone.utc)
TS_2 = datetime(2026, 7, 25, 7, 0, tzinfo=timezone.utc)
DAG_START_DATE = datetime(2026, 7, 1, tzinfo=timezone.utc)


class TestTriggeringDatasetsFromEvents:

    def test_airflow2_shape_uri_keys_and_dataset_events(self):
        events = {"starbake_customers": [_event("DatasetEvent", {"k": "v"}, TS_1)]}
        datasets = triggering_datasets_from_events(events)
        assert [d.uri for d in datasets] == ["starbake_customers"]
        assert datasets[0].extra["k"] == "v"
        assert datasets[0].extra["ts"] == TS_1  # ts injected from the event

    def test_airflow3_shape_asset_keys_and_reference_events(self):
        """The Airflow 3 accessor maps Asset OBJECTS to
        AssetEventDagRunReferenceResult lists — both must be recognized."""
        asset_key = type("Asset", (), {"uri": "starbake_customers", "name": "starbake_customers"})()
        events = {
            asset_key: [
                _event("AssetEventDagRunReferenceResult", {"k": "v"}, TS_1)
            ]
        }
        datasets = triggering_datasets_from_events(events)
        assert [d.uri for d in datasets] == ["starbake_customers"]
        assert datasets[0].extra["k"] == "v"

    def test_unknown_event_types_are_ignored(self):
        events = {"starbake_customers": [_event("SomethingElse", {}, TS_1)]}
        assert triggering_datasets_from_events(events) == []

    def test_latest_event_wins_per_uri(self):
        events = {
            "starbake_customers": [
                _event("AssetEventDagRunReference", {"which": "old"}, TS_1),
                _event("AssetEventDagRunReference", {"which": "new"}, TS_2),
            ]
        }
        datasets = triggering_datasets_from_events(events)
        assert len(datasets) == 1
        assert datasets[0].extra["which"] == "new"

    def test_event_extra_is_not_mutated(self):
        extra = {"k": "v"}
        events = {"uri": [_event("DatasetEvent", extra, TS_1)]}
        triggering_datasets_from_events(events)
        assert extra == {"k": "v"}  # the injected "ts" lands on a COPY

    def test_empty_or_none_mapping(self):
        assert triggering_datasets_from_events(None) == []
        assert triggering_datasets_from_events({}) == []


class TestDataIntervalEndFallback:

    def _transform_command(self):
        from ai.starlake.airflow.bash.starlake_airflow_bash_job import StarlakeAirflowBashJob
        job = StarlakeAirflowBashJob(
            filename="test_airflow.py",
            module_name="tests.airflow.test_airflow_asset_triggered_context",
            options={},
        )
        task = job.sl_transform(
            task_id="kpi_order_summary",
            transform_name="kpi.order_summary",
        )
        return task.bash_command

    def test_scheduled_date_template_is_undefined_safe(self):
        command = self._transform_command()
        assert "data_interval_end | default(dag_run.run_after, true)" in command

    def test_render_without_data_interval_end(self):
        """Asset-triggered Airflow 3 context: NO data_interval_end at all —
        the template must render (the macro receives dag_run.run_after)."""
        command = self._transform_command()
        received = {}

        def sl_scheduled_date(cron, value):
            received["value"] = value
            return datetime(2026, 7, 25, 8, 0, tzinfo=timezone.utc)

        env = jinja2.Environment(undefined=jinja2.StrictUndefined)
        env.filters["ts"] = lambda value: value  # Airflow's | ts filter stand-in
        run_after = datetime(2026, 7, 25, 7, 30, tzinfo=timezone.utc)
        rendered = env.from_string(command).render(
            params={"cron": None, "cron_expr": None},
            dag_run=SimpleNamespace(run_after=run_after),
            sl_scheduled_date=sl_scheduled_date,
            ts_as_datetime=lambda value: value,
            sl_dates=lambda *args: "",
            sl_options_from_events=lambda *args: "sl_options_applied=0",
            triggering_asset_events={},
        )
        assert rendered  # no UndefinedError
        assert received["value"] == run_after

    def test_pipeline_transform_options_are_undefined_safe(self):
        from ai.starlake.airflow.starlake_airflow_orchestration import AirflowPipeline
        # the method only consults cron_expr — a bare self keeps the test light
        options = AirflowPipeline.sl_transform_options(SimpleNamespace(), "0 6 * * *")
        assert options is not None
        assert "data_interval_end | default(dag_run.run_after, true)" in options


class TestStartTaskWithoutPreviousDataInterval:
    """
    Issue #145 — the start task read the previous successful DagRun's
    data_interval_end and fed it straight to an ISO parser. An Airflow 3
    asset-triggered run has none, so from the SECOND run on the whole DAG died
    with ``invalid literal for int() with base 10: b'None'`` (str(None) reaching
    dateutil). The run must instead be treated as having no known interval and
    fall back to the DAG start date.

    The triggering event here carries no ``sl_cron``: since issue #150 it is
    retained by its presence, with no lookup. The fallback therefore shows in
    two places: the pushed interval start (the previous run's
    data_interval_end, else the DAG start date — unchanged by #150), and the
    publication window of a non-triggering cron-less dependency, which opens at
    the previous run's trigger time, else its start date, else that same
    fallback.
    """

    URI = "starbake_orders"
    OTHER_URI = "starbake_customers"

    def _start_callable(self, monkeypatch, previous_runs, windows, options=None, built=None, extra_datasets=()):
        from ai.starlake.airflow import starlake_airflow_job as job_module
        from ai.starlake.airflow.bash.starlake_airflow_bash_job import StarlakeAirflowBashJob
        from ai.starlake.dataset import StarlakeDataset

        class FakeClient:
            def __init__(self, **kwargs):
                if built is not None:
                    built.append(kwargs)

            def find_previous_dag_runs(self, dag_id, scheduled_date, leaf_task_ids, at_scheduled_date=False):
                return previous_runs

            def find_dataset_events(self, uri, timestamp_lte, **window):
                # the window start is what the previous run's interval end
                # decides — recording it is what makes these tests diagnostic
                windows.append(window)
                return []

            def find_dataset_events_published(self, uri, timestamp_lte, timestamp_gt=None, timestamp_gte=None):
                # the publication window's lower bound is what the previous
                # run decides for a cron-less dependency (issue #150)
                windows.append({"uri": uri, "timestamp_gt": timestamp_gt, "timestamp_gte": timestamp_gte})
                return []

        monkeypatch.setattr(job_module, "StarlakeAirflowApiClient", FakeClient)

        job = StarlakeAirflowBashJob(
            filename="test_airflow.py",
            module_name="tests.airflow.test_airflow_asset_triggered_context",
            options=options or {},
        )
        dataset = StarlakeDataset(name=self.URI, cron="0 6 * * *")
        start = job.start_op(
            task_id="start",
            scheduled=False,
            not_scheduled_datasets=[StarlakeDataset(name=name) for name in extra_datasets],
            least_frequent_datasets=[],
            most_frequent_datasets=[dataset],
        )
        return start.python_callable

    def _context(self, monkeypatch):
        """A task context whose triggering event carries the asset URI."""
        from unittest.mock import MagicMock

        asset = type("Asset", (), {"uri": self.URI})()
        event = _event("AssetEventDagRunReferenceResult", {}, TS_2)
        ti = MagicMock()
        ti.get_template_context.return_value = {"triggering_asset_events": {asset: [event]}}
        dag = SimpleNamespace(
            dag_id="ing_starlake_transform",
            leaves=[SimpleNamespace(task_id="end")],
            start_date=DAG_START_DATE,
        )
        return {"task_instance": ti, "dag": dag}

    @staticmethod
    def _pushed(context):
        return {
            call.kwargs["key"]: call.kwargs["value"]
            for call in context["task_instance"].xcom_push.call_args_list
        }

    NO_INTERVAL = {"dag_id": "ing_starlake_transform", "data_interval_end": None, "start_date": None}
    WITH_INTERVAL = {
        "dag_id": "ing_starlake_transform",
        "data_interval_end": TS_1.isoformat(),
        "start_date": TS_1.isoformat(),
    }

    def test_previous_asset_triggered_run_without_data_interval_does_not_crash(self, monkeypatch):
        windows = []
        should_continue = self._start_callable(monkeypatch, [SimpleNamespace(**self.NO_INTERVAL)], windows)
        context = self._context(monkeypatch)

        # returns instead of raising on str(None) reaching the ISO parser...
        assert should_continue(start_date=TS_2.isoformat(), **context) is True
        # ...and the run with no known interval is treated as no run at all:
        # the interval starts at the DAG start date
        assert self._pushed(context)[StarlakeParameters.DATA_INTERVAL_START_PARAMETER.value] == DAG_START_DATE
        # the cron-less triggering event was retained by its presence
        assert windows == []

    def test_previous_run_with_data_interval_is_still_honoured(self, monkeypatch):
        windows = []
        should_continue = self._start_callable(monkeypatch, [SimpleNamespace(**self.WITH_INTERVAL)], windows)
        context = self._context(monkeypatch)

        assert should_continue(start_date=TS_2.isoformat(), **context) is True
        # a known interval end still starts the interval — no fallback applied
        assert self._pushed(context)[StarlakeParameters.DATA_INTERVAL_START_PARAMETER.value] == TS_1
        assert windows == []

    def test_previous_run_without_data_interval_opens_the_publication_window_at_the_dag_start(self, monkeypatch):
        windows = []
        should_continue = self._start_callable(
            monkeypatch, [SimpleNamespace(**self.NO_INTERVAL)], windows, extra_datasets=[self.OTHER_URI]
        )

        # the non-triggering cron-less dependency has no event: a legitimate wait
        assert should_continue(start_date=TS_2.isoformat(), **self._context(monkeypatch)) is False
        # the run exposes neither a trigger time nor a start date: the window
        # opens at the DAG start date
        assert windows == [{"uri": self.OTHER_URI, "timestamp_gt": DAG_START_DATE, "timestamp_gte": None}]

    def test_previous_run_start_date_opens_the_publication_window(self, monkeypatch):
        windows = []
        should_continue = self._start_callable(
            monkeypatch, [SimpleNamespace(**self.WITH_INTERVAL)], windows, extra_datasets=[self.OTHER_URI]
        )

        assert should_continue(start_date=TS_2.isoformat(), **self._context(monkeypatch)) is False
        # the run exposes no trigger time: its start date opens the window
        assert windows == [{"uri": self.OTHER_URI, "timestamp_gt": TS_1, "timestamp_gte": None}]


class TestApiClientOptions:
    """The instance to query, its connection and its authentication mode are
    DAG options like any other — a managed Airflow needs no code change, and no
    connection it cannot have."""

    def test_options_reach_the_api_client(self, monkeypatch):
        harness = TestStartTaskWithoutPreviousDataInterval()
        built = []
        harness._start_callable(
            monkeypatch,
            previous_runs=[],
            windows=[],
            options={
                "airflow_api_auth": "google",
                "airflow_api_base_url": "https://x-dot-europe-west1.composer.googleusercontent.com",
                "airflow_api_conn_id": "another_airflow_api",
            },
            built=built,
        )(start_date=TS_2.isoformat(), **harness._context(monkeypatch))

        assert built == [
            {
                "conn_id": "another_airflow_api",
                "base_url": "https://x-dot-europe-west1.composer.googleusercontent.com",
                "auth": "google",
            }
        ]

    def test_building_the_job_asks_the_variable_store_nothing(self, monkeypatch):
        """These three are read when the client is built, in the worker — a DAG
        parse must not pay a round-trip per option per file."""
        from ai.starlake.airflow import starlake_airflow_options as options_module
        from ai.starlake.airflow.bash.starlake_airflow_bash_job import StarlakeAirflowBashJob

        asked = []
        monkeypatch.setattr(
            options_module,
            "get_variable",
            lambda var_name, default=None, **kwargs: asked.append(var_name),
        )

        StarlakeAirflowBashJob(
            filename="test_airflow.py",
            module_name="tests.airflow.test_airflow_asset_triggered_context",
            options={},
        )

        assert [name for name in asked if name.startswith("airflow_api")] == []

    def test_an_airflow_variable_names_the_mode(self, monkeypatch):
        """Settable on a deployed instance without regenerating a DAG: a
        default value would have shadowed the variable store."""
        from ai.starlake.airflow import starlake_airflow_options as options_module

        monkeypatch.setattr(
            options_module,
            "get_variable",
            lambda var_name, default=None, **kwargs: "google" if var_name == "airflow_api_auth" else default,
        )

        harness = TestStartTaskWithoutPreviousDataInterval()
        built = []
        harness._start_callable(monkeypatch, previous_runs=[], windows=[], built=built)(
            start_date=TS_2.isoformat(), **harness._context(monkeypatch)
        )

        assert built == [{"conn_id": "airflow_api", "base_url": None, "auth": "google"}]

    def test_no_option_leaves_every_choice_to_the_client(self, monkeypatch):
        harness = TestStartTaskWithoutPreviousDataInterval()
        built = []
        harness._start_callable(monkeypatch, previous_runs=[], windows=[], built=built)(
            start_date=TS_2.isoformat(), **harness._context(monkeypatch)
        )

        assert built == [{"conn_id": "airflow_api", "base_url": None, "auth": None}]
