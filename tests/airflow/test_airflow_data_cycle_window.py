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
"""The data-cycle window of the start task.

Two tables of one monthly delivery, whose files arrive on any of the first
three days of the month, are loaded with the cron "0 6 1-3 * *". A transform
DAG reads both, with the ANY strategy. Without a data cycle, the start task
checks each dependency tick by tick, (previous tick, current tick]: the run
triggered by B on 2 December refuses A's event of 1 December. Widening that
window by a freshness of two days accepts it, but also accepts B's previous
delivery (2 November) in the run triggered by A on 1 December.

With a declared data cycle (issue #167), a dependency that fires in every
period of the data cycle is checked in data-cycle mode: its events are admitted
from the start of the cycle period that holds the anchor (the run's scheduled
date) up to the anchor. Any other dependency keeps its own schedule window
(dataset mode), and nothing changes without a data cycle or with the default
one (the pipeline's computed cron).
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from ai.starlake.common import StarlakeParameters

from tests.airflow.test_airflow_dataset_check_event_first import (
    DATA_INTERVAL_END,
    OPTIONS,
    SINK_A,
    SINK_B,
    SINK_C,
    URI_A,
    URI_B,
    URI_C,
    UTC,
    FakeLookups,
    asset_triggered_context,
    declared,
    found_options_key,
    joined_event,
    pulling,
    pushed,
    starlake_extra,
    start_callable,
    transform_dag,
    triggering_events,
    with_options,
)

MONTHLY_DELIVERY = "0 6 1-3 * *"
MONTHLY = {"data_cycle_enabled": "true", "data_cycle": "monthly"}
TWO_DAYS = 2 * 24 * 3600


def at(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def check(monkeypatch, triggering, joined=None, declared_datasets=None, options=None, started_at=None, freshness=None, default_data_cycle=None, event_freshness=None):
    """The start task's verdict for a run triggered by `triggering`
    ({uri: scheduled date}), the dependencies A and B being declared with the
    monthly-delivery cron unless `declared_datasets` says otherwise, and
    `freshness` ({sink: seconds}) declared by the transform DAG, and
    `event_freshness` ({uri: seconds}) published by the triggering events."""
    freshness = freshness or {}
    event_freshness = event_freshness or {}
    sinks = {URI_A: SINK_A, URI_B: SINK_B, URI_C: SINK_C}
    lookups = FakeLookups(joined=joined or {}).install(monkeypatch)
    if declared_datasets is None:
        declared_datasets = [
            declared(SINK_A, cron=MONTHLY_DELIVERY, freshness=freshness.get(SINK_A, 0)),
            declared(SINK_B, cron=MONTHLY_DELIVERY, freshness=freshness.get(SINK_B, 0)),
        ]
    should_continue = start_callable(most_frequent=declared_datasets, options=options, default_data_cycle=default_data_cycle)
    extras = {uri: starlake_extra(uri, sinks[uri], cron=crons_of(declared_datasets)[uri], scheduled_date=date) for uri, date in triggering.items()}
    for uri, seconds in event_freshness.items():
        extras[uri][StarlakeParameters.FRESHNESS_PARAMETER.value] = seconds
    context = asset_triggered_context(transform_dag(), extras)
    started_at = started_at or max(triggering.values()).replace(hour=7)
    verdict = should_continue(start_date=started_at.isoformat(), **context)
    return verdict, context, lookups


def crons_of(declared_datasets) -> dict:
    return {dataset.uri: dataset.cron for dataset in declared_datasets}


def interval_lookup(uri, started_at, gt, lte) -> tuple:
    return ("find_dataset_events", {"uri": uri, "timestamp_lte": started_at, "data_interval_end_gt": gt, "data_interval_end_lte": lte})


class TestDataCycleWindow:

    def test_an_earlier_attempt_of_the_same_delivery_is_accepted(self, monkeypatch):
        """B's file arrived on 2 December and triggered the run; A's arrived on
        1 December. A fires in every period of the monthly cycle: its events
        are admitted within (1 December 00:00, 2 December 06:00]."""
        verdict, context, lookups = check(
            monkeypatch, {URI_B: at(2026, 12, 2, 6)}, joined={URI_A: [joined_event(7, URI_A, at(2026, 12, 1, 6))]}, options=MONTHLY
        )

        assert verdict is True
        assert lookups.event_lookups() == [interval_lookup(URI_A, at(2026, 12, 2, 7), at(2026, 12, 1), at(2026, 12, 2, 6))]
        assert pushed(context)[DATA_INTERVAL_END] == at(2026, 12, 2, 6)

    @pytest.mark.parametrize("b_freshness", [0, TWO_DAYS, 31 * 24 * 3600])
    def test_the_previous_delivery_is_refused_at_the_start_of_a_cycle(self, monkeypatch, b_freshness):
        """A's file arrived on 1 December and triggered the run; B's latest
        event is the one of the November delivery (2 November). The window
        (1 December 00:00, 1 December 06:00] refuses it, whatever B's freshness:
        the run waits for B's December file instead of running on last month's
        data."""
        verdict, context, lookups = check(
            monkeypatch,
            {URI_A: at(2026, 12, 1, 6)},
            joined={URI_B: [joined_event(7, URI_B, at(2026, 11, 2, 6))]},
            options=MONTHLY,
            freshness={SINK_B: b_freshness},
            event_freshness={URI_A: b_freshness},
        )

        assert verdict is False
        assert lookups.event_lookups() == [interval_lookup(URI_B, at(2026, 12, 1, 7), at(2026, 12, 1), at(2026, 12, 1, 6))]
        assert pushed(context) == {}

    def test_a_late_file_of_a_previous_cycle_catches_up_with_the_other_table(self, monkeypatch):
        """A file of the October delivery of B is reloaded in December: its
        event anchors the run on 2 October 06:00, and A's October event is
        accepted in (1 October 00:00, 2 October 06:00], so the transformation
        catches up on October for this table alone."""
        started_at = at(2026, 12, 10, 9)
        verdict, context, lookups = check(
            monkeypatch,
            {URI_B: at(2026, 10, 2, 6)},
            joined={URI_A: [joined_event(7, URI_A, at(2026, 10, 1, 6))]},
            options=MONTHLY,
            started_at=started_at,
        )

        assert verdict is True
        assert lookups.event_lookups() == [interval_lookup(URI_A, started_at, at(2026, 10, 1), at(2026, 10, 2, 6))]
        assert pushed(context)[DATA_INTERVAL_END] == at(2026, 10, 2, 6)

    def test_both_attempts_coalesced_in_one_run_are_accepted(self, monkeypatch):
        """Both events triggered the same run: the anchor is 2 December, and
        A's triggering event of 1 December lies in the same cycle period."""
        verdict, _, lookups = check(monkeypatch, {URI_A: at(2026, 12, 1, 6), URI_B: at(2026, 12, 2, 6)}, options=MONTHLY)

        assert verdict is True
        assert lookups.event_lookups() == []

    def test_a_dependency_less_frequent_than_the_cycle_keeps_its_own_window(self, monkeypatch):
        """C is a yearly table (2 January) in the same monthly DAG. It misses
        most months, so it keeps its own schedule window, (2 January 2025 06:00,
        2 January 2026 06:00], and the DAG still runs in December."""
        declared_datasets = [declared(SINK_A, cron=MONTHLY_DELIVERY), declared(SINK_C, cron="0 6 2 1 *")]
        verdict, _, lookups = check(
            monkeypatch,
            {URI_A: at(2026, 12, 2, 6)},
            joined={URI_C: [joined_event(7, URI_C, at(2026, 1, 2, 6))]},
            declared_datasets=declared_datasets,
            options=MONTHLY,
        )

        assert verdict is True
        assert lookups.event_lookups() == [interval_lookup(URI_C, at(2026, 12, 2, 7), at(2025, 1, 2, 6), at(2026, 1, 2, 6))]

    @pytest.mark.parametrize(
        "anchor, c_event, expected",
        [
            pytest.param(at(2026, 12, 1, 6), at(2026, 11, 2, 6), False, id="november-refused-on-1-december"),
            pytest.param(at(2026, 12, 2, 6), at(2026, 12, 2, 6), True, id="december-accepted-on-2-december"),
        ],
    )
    def test_a_dependency_firing_once_per_period_is_checked_in_the_cycle_window(self, monkeypatch, anchor, c_event, expected):
        """C is a monthly table (the 2nd) read with the monthly-delivery table
        A. It fires once in every period of the declared monthly cycle, so it
        is checked in the cycle window: the run anchored on 1 December refuses
        its November event, which its own window (2 October 06:00, 2 November
        06:00] would have accepted, and the run anchored on 2 December accepts
        its December event."""
        declared_datasets = [declared(SINK_A, cron=MONTHLY_DELIVERY), declared(SINK_C, cron="0 6 2 * *")]
        verdict, _, lookups = check(
            monkeypatch,
            {URI_A: anchor},
            joined={URI_C: [joined_event(7, URI_C, c_event)]},
            declared_datasets=declared_datasets,
            options=MONTHLY,
        )

        assert verdict is expected
        assert lookups.event_lookups() == [interval_lookup(URI_C, anchor.replace(hour=7), at(2026, 12, 1), anchor)]

    def test_a_dependency_firing_only_on_the_cycle_ticks_keeps_its_own_window(self, monkeypatch):
        """A daily load at midnight (C) and one at 06:00 (A), with a declared
        daily cycle. C's events are dated on the cycle ticks, which close the
        period that ends there: in the cycle window (2 December 00:00,
        2 December 06:00] of the run triggered by A, C would never be found.
        C keeps its own window (1 December 00:00, 2 December 00:00], and its
        event of midnight is accepted, as before."""
        declared_datasets = [declared(SINK_A, cron="0 6 * * *"), declared(SINK_C, cron="0 0 * * *")]
        verdict, _, lookups = check(
            monkeypatch,
            {URI_A: at(2026, 12, 2, 6)},
            joined={URI_C: [joined_event(7, URI_C, at(2026, 12, 2))]},
            declared_datasets=declared_datasets,
            options={"data_cycle_enabled": "true", "data_cycle": "daily"},
        )

        assert verdict is True
        assert lookups.event_lookups() == [interval_lookup(URI_C, at(2026, 12, 2, 7), at(2026, 12, 1), at(2026, 12, 2))]

    def test_the_default_data_cycle_keeps_the_dataset_windows(self, monkeypatch):
        """Daily loads at 06:00 (A) and 07:00 (B), with the data cycle enabled
        but not declared: it defaults to the pipeline's computed cron, which
        carries A's hour and delimits no delivery. Both keep their own window,
        and A's event of 06:00 is accepted in the run triggered by B at 07:00,
        as before. In data-cycle mode, (2 December 06:00, 2 December 07:00]
        would refuse it."""
        declared_datasets = [declared(SINK_A, cron="0 6 * * *"), declared(SINK_B, cron="0 7 * * *")]
        verdict, _, lookups = check(
            monkeypatch,
            {URI_B: at(2026, 12, 2, 7)},
            joined={URI_A: [joined_event(7, URI_A, at(2026, 12, 2, 6))]},
            declared_datasets=declared_datasets,
            options={"data_cycle_enabled": "true"},
            default_data_cycle="0 6 * * *",
            started_at=at(2026, 12, 2, 8),
        )

        assert verdict is True
        assert lookups.event_lookups() == [interval_lookup(URI_A, at(2026, 12, 2, 8), at(2026, 12, 1, 6), at(2026, 12, 2, 6))]

    @pytest.mark.parametrize(
        "options, default_data_cycle",
        [
            # the reproduction of issue #166 declares the delivery's cron: a
            # dependency firing only on the cycle ticks keeps its dataset window
            pytest.param({"data_cycle_enabled": "true", "data_cycle": MONTHLY_DELIVERY}, None, id="declared-delivery-cron"),
            pytest.param({"data_cycle_enabled": "true"}, MONTHLY_DELIVERY, id="default-data-cycle"),
        ],
    )
    @pytest.mark.parametrize("published_freshness, expected", [(TWO_DAYS, True), (0, False)])
    def test_the_freshness_published_by_a_triggering_event_widens_its_window(self, monkeypatch, options, default_data_cycle, published_freshness, expected):
        """Issue #166: both events triggered the run (strategy ALL), B's of 3
        December anchors it, and A's dates from 1 December. A triggering
        dataset is checked with the freshness its event carries: with the two
        days that A's load now publishes, its window (2 December 06:00,
        3 December 06:00] widens to (30 November 06:00, 5 December 06:00] and
        the run processes the delivery; with the 0 published before, it is
        skipped."""
        verdict, _, lookups = check(
            monkeypatch,
            {URI_A: at(2026, 12, 1, 6), URI_B: at(2026, 12, 3, 6)},
            options=options,
            default_data_cycle=default_data_cycle,
            event_freshness={URI_A: published_freshness, URI_B: published_freshness},
            started_at=at(2026, 12, 3, 6, 20),
        )

        assert verdict is expected

    @pytest.mark.parametrize(
        "options",
        [
            pytest.param({}, id="no-data-cycle"),
            pytest.param({"data_cycle": "monthly"}, id="data-cycle-not-enabled"),
        ],
    )
    def test_without_data_cycle_nothing_changes(self, monkeypatch, options):
        """The tick-by-tick window (1 December 06:00, 2 December 06:00] still
        refuses A's event of 1 December."""
        verdict, _, lookups = check(
            monkeypatch, {URI_B: at(2026, 12, 2, 6)}, joined={URI_A: [joined_event(7, URI_A, at(2026, 12, 1, 6))]}, options=options
        )

        assert verdict is False
        assert lookups.event_lookups() == [interval_lookup(URI_A, at(2026, 12, 2, 7), at(2026, 12, 1, 6), at(2026, 12, 2, 6))]

    @pytest.mark.parametrize("month", range(1, 13))
    def test_the_verdicts_do_not_depend_on_the_month_the_start_task_runs_in(self, monkeypatch, month):
        """get_cron_frequency measures the gap between the next two ticks from
        the wall clock: on the 2nd of a month, 29 days plus a freshness of two
        days for the monthly delivery against 31 days for the monthly cycle in
        December, against 28 days in January. The beyond-data-cycle widening
        therefore depended on the month; the data-cycle window does not. The
        wall clock is emulated on the 2nd of each month."""
        from croniter import croniter
        from ai.starlake.airflow import starlake_airflow_job as job_module

        now = at(2026, month, 2, 7)

        def get_cron_frequency(cron):
            ticks = croniter(cron, now)
            first = ticks.get_next(datetime)
            return ticks.get_next(datetime) - first

        monkeypatch.setattr(job_module, "get_cron_frequency", get_cron_frequency)

        accepted, _, _ = check(
            monkeypatch,
            {URI_B: at(2026, 12, 2, 6)},
            joined={URI_A: [joined_event(7, URI_A, at(2026, 12, 1, 6))]},
            options=MONTHLY,
            freshness={SINK_A: TWO_DAYS},
        )
        refused, _, _ = check(
            monkeypatch,
            {URI_A: at(2026, 12, 1, 6)},
            joined={URI_B: [joined_event(7, URI_B, at(2026, 11, 2, 6))]},
            options=MONTHLY,
            freshness={SINK_B: TWO_DAYS},
        )

        assert (accepted, refused) == (True, False)

    def test_the_run_variables_of_the_earlier_attempt_reach_the_tasks(self, monkeypatch):
        """With the event of 1 December accepted, the tasks of the run
        triggered on 2 December receive the run variables of both loads."""
        from ai.starlake.airflow import sl_options_from_events
        a_event = with_options(joined_event(7, URI_A, at(2026, 12, 1, 6)), {"all": {"a_file_date": "2026-12-01"}})
        FakeLookups(joined={URI_A: [a_event]}).install(monkeypatch)
        should_continue = start_callable(
            most_frequent=[declared(SINK_A, cron=MONTHLY_DELIVERY), declared(SINK_B, cron=MONTHLY_DELIVERY)], options=MONTHLY
        )
        b_extra = starlake_extra(URI_B, SINK_B, cron=MONTHLY_DELIVERY, scheduled_date=at(2026, 12, 2, 6))
        b_extra[OPTIONS] = {"all": {"b_file_date": "2026-12-02"}}
        context = asset_triggered_context(transform_dag(), {URI_B: b_extra})

        assert should_continue(start_date=at(2026, 12, 2, 7).isoformat(), **context) is True
        assert pushed(context)[found_options_key()][0]["uri"] == URI_A
        rendered = sl_options_from_events(triggering_events(context), SimpleNamespace(conf={}), "my_domain.my_transform", pulling(pushed(context)))
        assert set(rendered.split(",")) == {"a_file_date=2026-12-01", "b_file_date=2026-12-02"}
