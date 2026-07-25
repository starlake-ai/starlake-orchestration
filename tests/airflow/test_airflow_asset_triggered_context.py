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


def _event(cls_name: str, extra: dict, ts: datetime):
    """Build an event object whose class NAME mimics the orchestrator model."""
    cls = type(cls_name, (), {})
    event = cls()
    event.extra = extra
    event.timestamp = ts
    return event


TS_1 = datetime(2026, 7, 25, 6, 0, tzinfo=timezone.utc)
TS_2 = datetime(2026, 7, 25, 7, 0, tzinfo=timezone.utc)


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
