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
"""Issue #150 — the start task of a transform DAG consuming a table loaded by a
NON-scheduled load DAG (a manual, one-shot, bootstrap or replay load).

On Airflow 3 such a producer runs without a data interval, yet the start task
correlated every cron-less dependency on the PRODUCING run's data_interval_end
— even the very event that triggered the run. The check failed, the
ShortCircuitOperator skipped everything and the run ended green in seconds
having executed nothing.

Pinned here:

- a triggering event whose dataset declares no schedule is retained by its
  presence, whatever ``data_cycle`` says, and no lookup is issued for it;
- a cron-less dependency that did not trigger the run is found by
  publication time: the most recently published event in
  ``(previous successful run's trigger time, this run's trigger time]``;
- nothing changes for datasets produced by scheduled DAGs (issue #151), for
  the previous run's data interval, nor for a run that fails its check (no
  guard rail: it keeps being skipped silently).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional
from unittest.mock import MagicMock

import pytest

from ai.starlake.airflow.starlake_airflow_api import DotDict
from ai.starlake.common import StarlakeParameters

MODULE_NAME = "tests.airflow.test_airflow_dataset_check_event_first"

UTC = timezone.utc

DATA_INTERVAL_START = StarlakeParameters.DATA_INTERVAL_START_PARAMETER.value
DATA_INTERVAL_END = StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value

DAG_START_DATE = datetime(2026, 7, 1, tzinfo=UTC)
# a manual load triggered without a logical date: Airflow 3 gives its run no
# data interval, the event's sl_scheduled_date is the run's trigger instant
PRODUCED_AT = datetime(2026, 7, 25, 6, 0, tzinfo=UTC)
# the consumer run: its trigger time (run_after on Airflow 3 — the queue time
# of an asset-triggered run, microsecond precision) and its start date (the
# "{{ dag_run.start_date }}" argument of the start task)
TRIGGERED_AT = datetime(2026, 7, 25, 6, 5, 0, 123456, tzinfo=UTC)
STARTED_AT = datetime(2026, 7, 25, 6, 5, 30, tzinfo=UTC)
# the previous successful consumer run's trigger time
PREVIOUS_TRIGGERED_AT = datetime(2026, 7, 24, 6, 5, tzinfo=UTC)

SINK_A, URI_A = "my_domain.a", "my_domain_a"
SINK_B, URI_B = "my_domain.b", "my_domain_b"
SINK_C, URI_C = "my_domain.c", "my_domain_c"
# a table also written by a non-Starlake producer, whose events carry no
# Starlake keys at all
SINK_FOREIGN, URI_FOREIGN = "landing.foreign", "landing_foreign"


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------

def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


class FakeLookups:
    """A recording stand-in for StarlakeAirflowApiClient.

    - ``find_previous_dag_runs`` answers both of the start task's calls with
      the seeded previous runs;
    - ``find_dataset_events_published`` FILTERS the seeded per-URI events by
      the requested publication window — so every verdict depends on the
      bounds the start task asks for — and returns them sorted by
      ``(timestamp, id)``, like the real client;
    - ``find_dataset_events`` (the producing-run-interval join, the scheduled
      branch's lookup) records its window and returns the seeded events, and
      REFUSES the URIs that must never be correlated on a producing run's
      data interval any more.
    """

    def __init__(
            self,
            previous_runs: Iterable[Any] = (),
            published: Optional[Dict[str, List[DotDict]]] = None,
            joined: Optional[Dict[str, List[DotDict]]] = None,
            forbid_interval_lookup: Iterable[str] = (),
    ) -> None:
        self.previous_runs = list(previous_runs)
        self.published = published or {}
        self.joined = joined or {}
        self.forbid_interval_lookup = set(forbid_interval_lookup)
        self.calls: List[tuple] = []

    def install(self, monkeypatch) -> "FakeLookups":
        from ai.starlake.airflow import starlake_airflow_job as job_module

        lookups = self

        class Client:
            def __init__(self, **kwargs):
                pass

            def find_previous_dag_runs(self, dag_id, scheduled_date, leaf_task_ids, at_scheduled_date=False):
                lookups.calls.append(("find_previous_dag_runs", {"at_scheduled_date": at_scheduled_date}))
                return list(lookups.previous_runs)

            def find_dataset_events_published(self, uri, timestamp_lte, timestamp_gt=None, timestamp_gte=None):
                lookups.calls.append((
                    "find_dataset_events_published",
                    {"uri": uri, "timestamp_lte": timestamp_lte, "timestamp_gt": timestamp_gt, "timestamp_gte": timestamp_gte},
                ))
                kept = []
                for event in lookups.published.get(uri, []):
                    published = _as_datetime(event.timestamp)
                    if published > _as_datetime(timestamp_lte):
                        continue
                    if timestamp_gt is not None and published <= _as_datetime(timestamp_gt):
                        continue
                    if timestamp_gte is not None and published < _as_datetime(timestamp_gte):
                        continue
                    kept.append(event)
                return sorted(kept, key=lambda event: (_as_datetime(event.timestamp), event.id))

            def find_dataset_events(self, uri, timestamp_lte, **window):
                # recorded first: were the refusal below ever swallowed, the
                # call would still show in event_lookups()
                lookups.calls.append(("find_dataset_events", dict(uri=uri, timestamp_lte=timestamp_lte, **window)))
                if uri in lookups.forbid_interval_lookup:
                    raise AssertionError(
                        f"{uri} declares no schedule: it must not be correlated on a producing "
                        f"run's data interval (find_dataset_events called with {window})"
                    )
                return list(lookups.joined.get(uri, []))

        monkeypatch.setattr(job_module, "StarlakeAirflowApiClient", Client)
        return self

    def lookups(self, method: str) -> List[dict]:
        return [kwargs for name, kwargs in self.calls if name == method]

    def event_lookups(self) -> List[tuple]:
        return [(name, kwargs) for name, kwargs in self.calls if name != "find_previous_dag_runs"]


def declared(name: str, cron: Optional[str] = None):
    from ai.starlake.dataset import StarlakeDataset
    return StarlakeDataset(name=name, cron=cron)


def start_callable(not_scheduled=(), most_frequent=(), least_frequent=(), options=None):
    """The start task's python_callable, built through the job's start_op seam."""
    from ai.starlake.airflow.bash.starlake_airflow_bash_job import StarlakeAirflowBashJob

    job = StarlakeAirflowBashJob(filename="test_airflow.py", module_name=MODULE_NAME, options=options or {})
    start = job.start_op(
        task_id="start",
        scheduled=False,
        not_scheduled_datasets=list(not_scheduled),
        least_frequent_datasets=list(least_frequent),
        most_frequent_datasets=list(most_frequent),
    )
    return start.python_callable


def _event(cls_name: str, extra: dict, timestamp: datetime):
    """An event object whose class NAME mimics the orchestrator model."""
    event = type(cls_name, (), {})()
    event.extra = extra
    event.timestamp = timestamp
    return event


def starlake_extra(uri: str, sink: str, cron: Optional[str] = None, scheduled_date: Any = None) -> dict:
    """The extra a Starlake load task publishes (StarlakeDatasetMixin)."""
    extra = {
        StarlakeParameters.URI_PARAMETER.value: uri,
        StarlakeParameters.SINK_PARAMETER.value: sink,
        StarlakeParameters.CRON_PARAMETER.value: cron,
        StarlakeParameters.FRESHNESS_PARAMETER.value: 0,
        "ts": "2026-07-25T06:00:05+0000",
    }
    if scheduled_date is not None:
        extra[StarlakeParameters.SCHEDULED_DATE_PARAMETER.value] = (
            scheduled_date.isoformat() if isinstance(scheduled_date, datetime) else scheduled_date
        )
    return extra


def asset_triggered_context(dag, extras_by_uri: Dict[str, dict], dag_run=None):
    """Airflow 3 shape: triggering_asset_events maps Asset OBJECTS to
    AssetEventDagRunReferenceResult lists."""
    triggering = {
        type("Asset", (), {"uri": uri, "name": uri})(): [_event("AssetEventDagRunReferenceResult", extra, TRIGGERED_AT)]
        for uri, extra in extras_by_uri.items()
    }
    ti = MagicMock()
    ti.get_template_context.return_value = {"triggering_asset_events": triggering}
    return {
        "task_instance": ti,
        "dag": dag,
        "dag_run": dag_run if dag_run is not None else SimpleNamespace(run_after=TRIGGERED_AT),
    }


def dataset_triggered_context(dag, extras_by_uri: Dict[str, dict], dag_run):
    """Airflow 2 shape: triggering_dataset_events maps URI strings to
    DatasetEvent lists."""
    triggering = {uri: [_event("DatasetEvent", extra, TRIGGERED_AT)] for uri, extra in extras_by_uri.items()}
    ti = MagicMock()
    ti.get_template_context.return_value = {"triggering_dataset_events": triggering}
    return {"task_instance": ti, "dag": dag, "dag_run": dag_run}


def transform_dag(start_date: datetime = DAG_START_DATE):
    return SimpleNamespace(
        dag_id="my_transform",
        leaves=[SimpleNamespace(task_id="end")],
        start_date=start_date,
    )


def pushed(context) -> Dict[str, Any]:
    """The XComs the start task pushed, by key."""
    return {
        call.kwargs["key"]: call.kwargs["value"]
        for call in context["task_instance"].xcom_push.call_args_list
    }


def published_event(event_id: int, uri: str, published_at: datetime, scheduled_date: Optional[datetime] = None) -> DotDict:
    """A dataset event in the client's normalized shape."""
    extra = DotDict()
    if scheduled_date is not None:
        extra[StarlakeParameters.SCHEDULED_DATE_PARAMETER.value] = scheduled_date.isoformat()
    return DotDict({
        "id": event_id,
        "dataset_id": 1,
        "dataset_uri": uri,
        "extra": extra,
        "source_dag_id": "load_my_domain",
        "source_task_id": "load",
        "source_run_id": f"manual__{published_at.isoformat()}",
        "source_map_index": -1,
        "timestamp": published_at.isoformat(),
        "dataset": DotDict({"id": 1, "uri": uri, "extra": DotDict()}),
    })


# ---------------------------------------------------------------------------
# AC 1-2 — a non-scheduled triggering event is retained by its presence
# ---------------------------------------------------------------------------

class TestNonScheduledTriggeringEvent:
    """The run was triggered by an event whose dataset declares no schedule:
    it correlates with no cron boundary, its presence is the whole test.
    RED on main: the non-scheduled branch went straight to the
    producing-run-interval lookup (refused by the fake here)."""

    def _triggered_by_a_manual_load(self, dataset_extra=None):
        return {URI_A: dataset_extra or starlake_extra(URI_A, SINK_A, scheduled_date=PRODUCED_AT)}

    def test_start_task_runs_when_triggered_by_a_non_scheduled_load(self, monkeypatch):
        """The issue #150 reproduction: one cron-less dependency, no previous
        successful run, and lookups that would find nothing."""
        lookups = FakeLookups(forbid_interval_lookup={URI_A}).install(monkeypatch)
        should_continue = start_callable(not_scheduled=[declared(SINK_A)])
        context = asset_triggered_context(transform_dag(), self._triggered_by_a_manual_load())

        assert should_continue(start_date=STARTED_AT.isoformat(), **context) is True
        assert pushed(context) == {DATA_INTERVAL_START: DAG_START_DATE, DATA_INTERVAL_END: PRODUCED_AT}
        assert lookups.event_lookups() == []

    def test_pipeline_start_task_runs_when_triggered_by_a_non_scheduled_load(self, monkeypatch):
        """The same reproduction through the production path: a dependencies
        pipeline whose only upstream is a cron-less table."""
        from ai.starlake.airflow import AirflowOrchestration
        from ai.starlake.airflow.bash.starlake_airflow_bash_job import StarlakeAirflowBashJob
        from ai.starlake.orchestration import StarlakeDependencies, StarlakeDependency, StarlakeDependencyType

        job = StarlakeAirflowBashJob(filename="test_airflow.py", module_name=MODULE_NAME, options={})
        pipeline = AirflowOrchestration(job=job).sl_create_pipeline(
            dependencies=StarlakeDependencies([
                StarlakeDependency(
                    name="my_transform",
                    dependency_type=StarlakeDependencyType.TASK,
                    dependencies=[
                        StarlakeDependency(name=SINK_A, dependency_type=StarlakeDependencyType.TABLE, cron=None),
                    ],
                ),
            ])
        )
        with pipeline:
            start = pipeline.start_task()
            end = pipeline.end_task()
            start >> end

        assert [dataset.uri for dataset in pipeline.not_scheduled_datasets] == [URI_A]
        lookups = FakeLookups(forbid_interval_lookup={URI_A}).install(monkeypatch)
        should_continue = pipeline.dag.get_task("start").python_callable
        context = asset_triggered_context(pipeline.dag, self._triggered_by_a_manual_load())

        assert should_continue(start_date=STARTED_AT.isoformat(), **context) is True
        assert pipeline.dag.start_date is not None
        assert pushed(context) == {DATA_INTERVAL_START: pipeline.dag.start_date, DATA_INTERVAL_END: PRODUCED_AT}
        assert lookups.event_lookups() == []

    def test_data_cycle_does_not_gate_a_non_scheduled_triggering_event(self, monkeypatch):
        """A data cycle turned the event into a 'scheduled' one on main, and a
        manual producer's date — never on the cycle's boundary — failed the
        cycle range: bootstrap and replay loads must not be blocked by the
        consumer's business cycle."""
        lookups = FakeLookups(forbid_interval_lookup={URI_A}).install(monkeypatch)
        should_continue = start_callable(
            not_scheduled=[declared(SINK_A)],
            options={"data_cycle_enabled": "true", "data_cycle": "0 0 * * *"},
        )
        context = asset_triggered_context(transform_dag(), self._triggered_by_a_manual_load())

        assert should_continue(start_date=STARTED_AT.isoformat(), **context) is True
        assert pushed(context) == {DATA_INTERVAL_START: DAG_START_DATE, DATA_INTERVAL_END: PRODUCED_AT}
        assert lookups.event_lookups() == []

    def test_a_foreign_event_without_scheduled_date_triggering_too_does_not_crash(self, monkeypatch):
        """The anchor was chosen with a NAIVE datetime.min against aware dates:
        a foreign event (no sl_scheduled_date) triggering together with a
        Starlake event raised TypeError before any check ran."""
        lookups = FakeLookups(forbid_interval_lookup={URI_A, URI_FOREIGN}).install(monkeypatch)
        should_continue = start_callable(not_scheduled=[declared(SINK_A), declared(SINK_FOREIGN)])
        context = asset_triggered_context(
            transform_dag(),
            {URI_A: starlake_extra(URI_A, SINK_A, scheduled_date=PRODUCED_AT), URI_FOREIGN: {}},
        )

        assert should_continue(start_date=STARTED_AT.isoformat(), **context) is True
        # the only readable scheduled date anchors the interval end
        assert pushed(context) == {DATA_INTERVAL_START: DAG_START_DATE, DATA_INTERVAL_END: PRODUCED_AT}
        assert lookups.event_lookups() == []

    @pytest.mark.parametrize("unreadable", ["not-a-date", 1753423200])
    def test_an_unreadable_scheduled_date_does_not_prevent_retention(self, monkeypatch, unreadable):
        """A foreign producer's unreadable date is treated as absent: the event
        is still retained, the interval end falls back to the run's start."""
        lookups = FakeLookups(forbid_interval_lookup={URI_A}).install(monkeypatch)
        should_continue = start_callable(not_scheduled=[declared(SINK_A)])
        context = asset_triggered_context(
            transform_dag(), {URI_A: starlake_extra(URI_A, SINK_A, scheduled_date=unreadable)}
        )

        assert should_continue(start_date=STARTED_AT.isoformat(), **context) is True
        assert pushed(context) == {DATA_INTERVAL_START: DAG_START_DATE, DATA_INTERVAL_END: STARTED_AT}
        assert lookups.event_lookups() == []


# ---------------------------------------------------------------------------
# AC 3 + AC 11 — a non-triggering non-scheduled dependency, by publication time
# ---------------------------------------------------------------------------

def _previous_run_airflow3():
    # a manual run triggered for a PAST logical date: its data interval ends
    # at that date, its run_after is the instant it was triggered
    return SimpleNamespace(
        dag_id="my_transform",
        run_id="manual__2026-07-20T00:00:00+00:00",
        run_after=PREVIOUS_TRIGGERED_AT.isoformat(),
        data_interval_end=datetime(2026, 7, 20, tzinfo=UTC).isoformat(),
        start_date=(PREVIOUS_TRIGGERED_AT + timedelta(minutes=1)).isoformat(),
    )


def _previous_run_airflow2():
    # a dataset-triggered run: its logical date is its queue time, its data
    # interval spans its source runs' intervals
    return SimpleNamespace(
        dag_id="my_transform",
        run_id="dataset_triggered__2026-07-24T06:05:00+00:00",
        execution_date=PREVIOUS_TRIGGERED_AT.isoformat(),
        data_interval_end=datetime(2026, 7, 24, 6, 0, tzinfo=UTC).isoformat(),
        start_date=(PREVIOUS_TRIGGERED_AT + timedelta(minutes=1)).isoformat(),
    )


def _previous_run_with_logical_date_only():
    return SimpleNamespace(
        dag_id="my_transform",
        logical_date=PREVIOUS_TRIGGERED_AT.isoformat(),
        data_interval_end=datetime(2026, 7, 24, 6, 0, tzinfo=UTC).isoformat(),
        start_date=(PREVIOUS_TRIGGERED_AT + timedelta(minutes=1)).isoformat(),
    )


def _previous_run_without_trigger_time():
    return SimpleNamespace(
        dag_id="my_transform",
        data_interval_end=datetime(2026, 7, 24, 6, 0, tzinfo=UTC).isoformat(),
        start_date=PREVIOUS_TRIGGERED_AT.isoformat(),
    )


class TestNonTriggeringPublicationLookup:
    """The DAG depends on A and B, both cron-less; the run was triggered by A
    only. B is looked up by publication time over (P_prev, U] =
    (previous successful run's trigger time, this run's trigger time] — never
    through the producing-run-interval join (refused by the fake).
    RED on main: B went through find_dataset_events."""

    def _check(self, monkeypatch, b_events, previous_runs=(), started_at=STARTED_AT, context=None):
        lookups = FakeLookups(
            previous_runs=previous_runs,
            published={URI_B: b_events},
            forbid_interval_lookup={URI_A, URI_B},
        ).install(monkeypatch)
        should_continue = start_callable(not_scheduled=[declared(SINK_A), declared(SINK_B)])
        if context is None:
            context = asset_triggered_context(
                transform_dag(), {URI_A: starlake_extra(URI_A, SINK_A, scheduled_date=PRODUCED_AT)}
            )
        verdict = should_continue(start_date=started_at.isoformat(), **context)
        return verdict, lookups, context

    def test_b_published_since_the_dag_start_is_found(self, monkeypatch):
        """No previous successful run: the window opens at the DAG start date."""
        verdict, lookups, context = self._check(
            monkeypatch, [published_event(1, URI_B, TRIGGERED_AT - timedelta(minutes=30))]
        )

        assert verdict is True
        assert lookups.event_lookups() == [(
            "find_dataset_events_published",
            {"uri": URI_B, "timestamp_lte": TRIGGERED_AT, "timestamp_gt": DAG_START_DATE, "timestamp_gte": None},
        )]
        assert pushed(context) == {DATA_INTERVAL_START: DAG_START_DATE, DATA_INTERVAL_END: PRODUCED_AT}

    @pytest.mark.parametrize(
        "previous_run",
        [
            pytest.param(_previous_run_airflow3(), id="airflow3-run_after"),
            pytest.param(_previous_run_airflow2(), id="airflow2-execution_date"),
            pytest.param(_previous_run_with_logical_date_only(), id="logical_date"),
            pytest.param(_previous_run_without_trigger_time(), id="no-trigger-time-start_date"),
        ],
    )
    def test_window_opens_at_the_previous_successful_run_trigger_time(self, monkeypatch, previous_run):
        """P_prev is the previous successful run's trigger time — never its
        data_interval_end (which would re-accept what that run consumed) —
        else its start date. The pushed interval start (the previous run's
        data_interval_end) is unchanged."""
        verdict, lookups, context = self._check(
            monkeypatch,
            [published_event(1, URI_B, PREVIOUS_TRIGGERED_AT + timedelta(hours=1))],
            previous_runs=[previous_run],
        )

        assert verdict is True
        assert lookups.lookups("find_dataset_events_published") == [
            {"uri": URI_B, "timestamp_lte": TRIGGERED_AT, "timestamp_gt": PREVIOUS_TRIGGERED_AT, "timestamp_gte": None}
        ]
        assert pushed(context)[DATA_INTERVAL_START] == _as_datetime(previous_run.data_interval_end)

    def test_airflow2_trigger_times_are_the_logical_dates(self, monkeypatch):
        """Airflow 2 shape end to end: URI-keyed DatasetEvent lists, a DagRun
        exposing execution_date (its queue time) and no run_after."""
        context = dataset_triggered_context(
            transform_dag(),
            {URI_A: starlake_extra(URI_A, SINK_A, scheduled_date=PRODUCED_AT)},
            dag_run=SimpleNamespace(execution_date=TRIGGERED_AT),
        )
        verdict, lookups, context = self._check(
            monkeypatch,
            [published_event(1, URI_B, PREVIOUS_TRIGGERED_AT + timedelta(hours=1))],
            previous_runs=[_previous_run_airflow2()],
            context=context,
        )

        assert verdict is True
        assert lookups.lookups("find_dataset_events_published") == [
            {"uri": URI_B, "timestamp_lte": TRIGGERED_AT, "timestamp_gt": PREVIOUS_TRIGGERED_AT, "timestamp_gte": None}
        ]

    def test_no_b_event_published_is_a_legitimate_wait(self, monkeypatch):
        verdict, lookups, context = self._check(monkeypatch, [])

        assert verdict is False
        assert len(lookups.lookups("find_dataset_events_published")) == 1
        assert pushed(context) == {}

    def test_a_b_event_without_scheduled_date_counts_as_present(self, monkeypatch):
        """Publication is the only meaningful ordering of a non-scheduled event."""
        verdict, _, context = self._check(
            monkeypatch, [published_event(1, URI_B, TRIGGERED_AT - timedelta(minutes=1))]
        )

        assert verdict is True
        assert pushed(context)[DATA_INTERVAL_END] == PRODUCED_AT

    def test_a_b_event_later_than_the_anchor_is_accepted_and_raises_the_interval_end(self, monkeypatch):
        """freshness no longer caps a non-scheduled event's date at the anchor."""
        later = PRODUCED_AT + timedelta(minutes=2)
        verdict, _, context = self._check(
            monkeypatch, [published_event(1, URI_B, TRIGGERED_AT - timedelta(minutes=1), scheduled_date=later)]
        )

        assert verdict is True
        assert pushed(context)[DATA_INTERVAL_END] == later

    def test_the_most_recently_published_b_event_is_retained(self, monkeypatch):
        """Among B's events in the window, the most recently PUBLISHED one is
        retained — not the one carrying the greatest scheduled date."""
        verdict, _, context = self._check(
            monkeypatch,
            [
                published_event(1, URI_B, TRIGGERED_AT - timedelta(minutes=3), scheduled_date=PRODUCED_AT + timedelta(minutes=2)),
                published_event(2, URI_B, TRIGGERED_AT - timedelta(minutes=1), scheduled_date=PRODUCED_AT + timedelta(minutes=1)),
            ],
        )

        assert verdict is True
        assert pushed(context)[DATA_INTERVAL_END] == PRODUCED_AT + timedelta(minutes=1)

    @pytest.mark.parametrize(
        "published_at, expected",
        [
            pytest.param(PREVIOUS_TRIGGERED_AT, False, id="exactly-at-P_prev-excluded"),
            pytest.param(PREVIOUS_TRIGGERED_AT + timedelta(microseconds=1), True, id="just-after-P_prev-included"),
            pytest.param(TRIGGERED_AT, True, id="exactly-at-U-included"),
            pytest.param(TRIGGERED_AT + timedelta(microseconds=1), False, id="after-U-excluded"),
        ],
    )
    def test_window_bounds(self, monkeypatch, published_at, expected):
        verdict, _, _ = self._check(
            monkeypatch, [published_event(1, URI_B, published_at)], previous_runs=[_previous_run_airflow3()]
        )

        assert verdict is expected

    def test_re_execution_after_a_clear_reproduces_the_verdict(self, monkeypatch):
        """A clear resets dag_run.start_date past events published since: the
        window's upper bound is the run's trigger time, so the re-executed
        start task retains what the original run retained, not the later
        event (AC 11)."""
        b_events = [
            published_event(1, URI_B, TRIGGERED_AT - timedelta(minutes=5), scheduled_date=PRODUCED_AT + timedelta(minutes=1)),
            published_event(2, URI_B, TRIGGERED_AT + timedelta(days=1), scheduled_date=PRODUCED_AT + timedelta(days=1)),
        ]
        first, first_lookups, first_context = self._check(monkeypatch, b_events)
        replay, replay_lookups, replay_context = self._check(
            monkeypatch, b_events, started_at=TRIGGERED_AT + timedelta(days=2)
        )

        assert first is True and replay is True
        assert pushed(replay_context) == pushed(first_context)
        assert pushed(replay_context)[DATA_INTERVAL_END] == PRODUCED_AT + timedelta(minutes=1)
        assert replay_lookups.lookups("find_dataset_events_published")[0]["timestamp_lte"] == TRIGGERED_AT

    def test_an_event_published_after_the_trigger_is_left_to_the_next_run(self, monkeypatch):
        """An event published after this run's trigger has queued the next
        run, which retains it event-first: re-executing this run's start task
        later must not consume it too (AC 11)."""
        b_events = [published_event(1, URI_B, TRIGGERED_AT + timedelta(days=1))]
        first, _, _ = self._check(monkeypatch, b_events)
        replay, _, replay_context = self._check(monkeypatch, b_events, started_at=TRIGGERED_AT + timedelta(days=2))

        assert first is False and replay is False
        assert pushed(replay_context) == {}


# ---------------------------------------------------------------------------
# AC 4 — scheduled datasets: regression pins (GREEN on main)
# ---------------------------------------------------------------------------

def _scheduled_extra(uri: str, sink: str, cron: str, scheduled_date: datetime) -> dict:
    return starlake_extra(uri, sink, cron=cron, scheduled_date=scheduled_date)


def joined_event(event_id: int, uri: str, scheduled_date: datetime) -> DotDict:
    event = published_event(event_id, uri, scheduled_date + timedelta(minutes=5), scheduled_date=scheduled_date)
    event["data_interval_end"] = scheduled_date.isoformat()
    return event


class TestScheduledDatasetsUnchanged:
    """Datasets produced by scheduled DAGs keep today's check, window for
    window (issue #151 owns their redesign). Regression pins: GREEN on main."""

    SLOT = datetime(2026, 7, 25, 6, 0, tzinfo=UTC)

    def test_triggering_scheduled_dataset_on_its_boundary_is_retained_directly(self, monkeypatch):
        lookups = FakeLookups().install(monkeypatch)
        should_continue = start_callable(most_frequent=[declared(SINK_A, cron="0 6 * * *")])
        context = asset_triggered_context(
            transform_dag(), {URI_A: _scheduled_extra(URI_A, SINK_A, "0 6 * * *", self.SLOT)}
        )

        assert should_continue(start_date=STARTED_AT.isoformat(), **context) is True
        assert pushed(context) == {DATA_INTERVAL_START: DAG_START_DATE, DATA_INTERVAL_END: self.SLOT}
        assert lookups.event_lookups() == []

    @pytest.mark.parametrize(
        "c_cron, window, c_scheduled_date, expected",
        [
            pytest.param(
                "0 6 * * *",
                {"data_interval_end_gt": datetime(2026, 7, 24, 6, 0, tzinfo=UTC),
                 "data_interval_end_lte": datetime(2026, 7, 25, 6, 0, tzinfo=UTC)},
                datetime(2026, 7, 25, 6, 0, tzinfo=UTC),
                True,
                id="same-cron",
            ),
            pytest.param(
                # same frequency, later slot: the window is capped at the anchor
                "0 18 * * *",
                {"data_interval_end_gte": datetime(2026, 7, 24, 18, 0, tzinfo=UTC),
                 "data_interval_end_lte": datetime(2026, 7, 25, 6, 0, tzinfo=UTC)},
                datetime(2026, 7, 24, 18, 0, tzinfo=UTC),
                False,
                id="later-slot-capped-at-the-anchor",
            ),
        ],
    )
    def test_non_triggering_scheduled_dataset_keeps_the_interval_lookup(self, monkeypatch, c_cron, window, c_scheduled_date, expected):
        lookups = FakeLookups(joined={URI_C: [joined_event(7, URI_C, c_scheduled_date)]}).install(monkeypatch)
        should_continue = start_callable(most_frequent=[declared(SINK_A, cron="0 6 * * *"), declared(SINK_C, cron=c_cron)])
        context = asset_triggered_context(
            transform_dag(), {URI_A: _scheduled_extra(URI_A, SINK_A, "0 6 * * *", self.SLOT)}
        )

        assert should_continue(start_date=STARTED_AT.isoformat(), **context) is expected
        assert lookups.event_lookups() == [
            ("find_dataset_events", dict(uri=URI_C, timestamp_lte=STARTED_AT, **window))
        ]

    def test_non_triggering_cron_less_dataset_under_a_data_cycle_keeps_the_interval_lookup(self, monkeypatch):
        """A data cycle makes a cron-less dependency 'scheduled' by the cycle:
        it keeps the cycle-range lookup."""
        lookups = FakeLookups(
            joined={URI_B: [joined_event(7, URI_B, datetime(2026, 7, 25, 0, 0, tzinfo=UTC))]}
        ).install(monkeypatch)
        should_continue = start_callable(
            most_frequent=[declared(SINK_A, cron="0 6 * * *")],
            not_scheduled=[declared(SINK_B)],
            options={"data_cycle_enabled": "true", "data_cycle": "0 0 * * *"},
        )
        context = asset_triggered_context(
            transform_dag(), {URI_A: _scheduled_extra(URI_A, SINK_A, "0 6 * * *", self.SLOT)}
        )

        assert should_continue(start_date=STARTED_AT.isoformat(), **context) is True
        assert lookups.event_lookups() == [(
            "find_dataset_events",
            {
                "uri": URI_B,
                "timestamp_lte": STARTED_AT,
                "data_interval_end_gt": datetime(2026, 7, 24, 0, 0, tzinfo=UTC),
                "data_interval_end_lte": datetime(2026, 7, 25, 0, 0, tzinfo=UTC),
            },
        )]


# ---------------------------------------------------------------------------
# AC 5 — the previous run's data interval: regression pins (GREEN on main)
# ---------------------------------------------------------------------------

class TestPreviousDagCheckedUnchanged:
    """The pushed interval start still comes from the previous successful
    run's data_interval_end, with the DAG start date as fallback (issue #145).
    A scheduled triggering dataset keeps these pins on a path main shares.
    Regression pins: GREEN on main."""

    SLOT = datetime(2026, 7, 25, 6, 0, tzinfo=UTC)
    TS_1 = datetime(2026, 7, 24, 6, 0, tzinfo=UTC)

    def _check(self, monkeypatch, previous_runs):
        FakeLookups(previous_runs=previous_runs).install(monkeypatch)
        should_continue = start_callable(most_frequent=[declared(SINK_A, cron="0 6 * * *")])
        context = asset_triggered_context(
            transform_dag(), {URI_A: _scheduled_extra(URI_A, SINK_A, "0 6 * * *", self.SLOT)}
        )
        return should_continue(start_date=STARTED_AT.isoformat(), **context), context

    def test_previous_run_without_data_interval_falls_back_to_the_dag_start_date(self, monkeypatch):
        previous = SimpleNamespace(dag_id="my_transform", data_interval_end=None, start_date=None)
        verdict, context = self._check(monkeypatch, [previous])

        assert verdict is True
        assert pushed(context)[DATA_INTERVAL_START] == DAG_START_DATE

    def test_previous_run_data_interval_end_starts_the_interval(self, monkeypatch):
        previous = SimpleNamespace(dag_id="my_transform", data_interval_end=self.TS_1.isoformat(), start_date=self.TS_1.isoformat())
        verdict, context = self._check(monkeypatch, [previous])

        assert verdict is True
        assert pushed(context)[DATA_INTERVAL_START] == self.TS_1


# ---------------------------------------------------------------------------
# AC 6 — no guard rail: a failed check is still skipped silently
# ---------------------------------------------------------------------------

class TestNoGuardRail:
    """A run that fails its check keeps returning False without raising
    (maintainer decision). Regression pins: GREEN on main."""

    def test_rejected_scheduled_triggering_event_is_skipped_silently(self, monkeypatch):
        """A cron-declaring event off its own boundary (a foreign or partial
        event) with no match in the lookup."""
        lookups = FakeLookups().install(monkeypatch)
        should_continue = start_callable(most_frequent=[declared(SINK_A, cron="0 6 * * *")])
        context = asset_triggered_context(
            transform_dag(),
            {URI_A: _scheduled_extra(URI_A, SINK_A, "0 6 * * *", datetime(2026, 7, 25, 7, 30, tzinfo=UTC))},
        )

        assert should_continue(start_date=STARTED_AT.isoformat(), **context) is False
        assert lookups.event_lookups() == [(
            "find_dataset_events",
            {
                "uri": URI_A,
                "timestamp_lte": STARTED_AT,
                "data_interval_end_gt": datetime(2026, 7, 24, 6, 0, tzinfo=UTC),
                "data_interval_end_lte": datetime(2026, 7, 25, 6, 0, tzinfo=UTC),
            },
        )]
        assert pushed(context) == {}

    def test_min_timedelta_between_runs_still_skips_before_any_check(self, monkeypatch):
        """The last successful run processed the same date moments ago: the
        early skip precedes even the event-first retention."""
        just_run = SimpleNamespace(
            dag_id="my_transform",
            data_interval_end=PRODUCED_AT.isoformat(),
            start_date=(STARTED_AT - timedelta(seconds=60)).isoformat(),
        )
        lookups = FakeLookups(previous_runs=[just_run], forbid_interval_lookup={URI_A}).install(monkeypatch)
        should_continue = start_callable(not_scheduled=[declared(SINK_A)])
        context = asset_triggered_context(
            transform_dag(), {URI_A: starlake_extra(URI_A, SINK_A, scheduled_date=PRODUCED_AT)}
        )

        assert should_continue(start_date=STARTED_AT.isoformat(), **context) is False
        assert lookups.event_lookups() == []
        assert pushed(context) == {}
