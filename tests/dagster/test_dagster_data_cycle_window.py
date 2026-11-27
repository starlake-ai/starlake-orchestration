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

"""Issue #167 — the sensor's start check admits a dependency's
materializations from the start of the current period of a declared data cycle.

Two tables of one monthly delivery, whose files arrive on any of the first
three days of the month, are loaded with the cron "0 6 1-3 * *". A transform
pipeline reads both, with the ANY strategy. Without a data cycle, its sensor
checks each dependency tick by tick, (previous tick, current tick] around the
anchor (the most recent scheduled date among the unconsumed materializations),
and looks the event log up on the two boundary partitions of that window only:
the run anchored on 2 December refuses the materialization of 1 December.

With a declared data cycle, a dependency that fires in every period of the data
cycle is checked in data-cycle mode: its materializations are admitted from the
start of the cycle period that holds the anchor up to the anchor, looked up on
every tick of the window (and without partition filter). Any other dependency
keeps its own schedule window (dataset mode), and nothing changes without a
data cycle or with the default one (the pipeline's computed cron).
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.dagster.dagster_check_harness import (  # noqa: F401 (harness is a fixture)
    BALANCES,
    CONTRACTS,
    MONTHLY,
    MONTHLY_DELIVERY,
    POSITIONS,
    RATES,
    TWO_DAYS,
    at,
    harness,
    relayed,
)

DELIVERY = {BALANCES: MONTHLY_DELIVERY, CONTRACTS: MONTHLY_DELIVERY}


def accepted_ids(result) -> list:
    return [record.storage_id for record in result.accepted]


class TestDataCycleWindow:

    def test_an_earlier_attempt_of_the_same_delivery_is_accepted(self, harness):
        """The contracts file arrived on 2 December, the balances file on
        1 December. Balances fires in every period of the monthly cycle: its
        materializations are admitted within (1 December 00:00, 2 December
        06:00], although 1 December 06:00 is neither a boundary of its own
        window nor of the cycle's."""
        pipeline = harness(DELIVERY, MONTHLY)
        balances = pipeline.materialize(BALANCES, at(2026, 12, 1, 6))

        result = pipeline.check(at(2026, 12, 2, 6), [BALANCES], {CONTRACTS: at(2026, 12, 2, 6)})

        assert result.checked is True
        assert result.max_scheduled_date == at(2026, 12, 2, 6)
        assert accepted_ids(result) == [balances]

    @pytest.mark.parametrize("freshness", [0, TWO_DAYS])
    def test_the_previous_delivery_is_refused_at_the_start_of_a_cycle(self, harness, freshness):
        """The balances file arrived on 1 December; the latest contracts file
        is the one of the November delivery (2 November). The window
        (1 December 00:00, 1 December 06:00] refuses it, whatever the freshness
        of contracts: the run waits for December's file instead of running on
        last month's data."""
        pipeline = harness(DELIVERY, MONTHLY)
        pipeline.materialize(CONTRACTS, at(2026, 11, 2, 6), freshness=freshness)

        result = pipeline.check(at(2026, 12, 1, 6), [CONTRACTS], {BALANCES: at(2026, 12, 1, 6)}, freshness={CONTRACTS: freshness})

        assert result.checked is False
        assert [dataset.uri for dataset in result.missing] == [CONTRACTS]
        assert result.accepted == []

    def test_a_late_file_of_a_previous_cycle_catches_up_with_the_other_table(self, harness):
        """Both tables were loaded in October, November and December. A file of
        the October delivery of contracts is reloaded in December: its
        materialization anchors the run on 2 October 06:00, and the October
        materialization of balances, consumed long ago and older than the
        November and December ones, is accepted in (1 October 00:00, 2 October
        06:00]: the transformation catches up on October for this table alone,
        with the run variables of both October files."""
        pipeline = harness(DELIVERY, MONTHLY)
        for month in (10, 11, 12):
            pipeline.materialize(BALANCES, at(2026, month, 1, 6), options={"all": {"balances_file_date": f"2026-{month}-01"}})
        for month in (11, 12):
            pipeline.materialize(CONTRACTS, at(2026, month, 2, 6), options={"all": {"contracts_file_date": f"2026-{month}-02"}})
        first = pipeline.tick()
        assert len(first.run_requests) == 1
        assert first.run_requests[0].partition_key == "2026-12-02T06:00:00+0000"

        pipeline.materialize(CONTRACTS, at(2026, 10, 2, 6), options={"all": {"contracts_file_date": "2026-10-02"}})
        catch_up = pipeline.tick()

        assert len(catch_up.run_requests) == 1
        assert catch_up.run_requests[0].partition_key == "2026-10-02T06:00:00+0000"
        assert relayed(catch_up) == {"all": {"balances_file_date": "2026-10-01", "contracts_file_date": "2026-10-02"}}

    def test_both_attempts_consumed_by_one_run_are_accepted(self, harness):
        """The balances file of 1 December does not run the pipeline (the
        contracts one is missing), so it stays unconsumed; the contracts file
        of 2 December anchors the next tick, and the unconsumed balances
        materialization lies in the same cycle period."""
        pipeline = harness(DELIVERY, MONTHLY)
        pipeline.materialize(BALANCES, at(2026, 12, 1, 6))
        assert pipeline.tick().run_requests == []

        pipeline.materialize(CONTRACTS, at(2026, 12, 2, 6))
        result = pipeline.tick()

        assert len(result.run_requests) == 1
        assert result.run_requests[0].partition_key == "2026-12-02T06:00:00+0000"

    def test_a_dependency_less_frequent_than_the_cycle_keeps_its_own_window(self, harness):
        """Rates is a yearly table (2 January) of the same monthly pipeline. It
        misses most months, so it keeps its own schedule window, (2 January
        2025 06:00, 2 January 2026 06:00] up to the anchor, and does not block
        the run of December."""
        pipeline = harness({BALANCES: MONTHLY_DELIVERY, RATES: "0 6 2 1 *"}, MONTHLY)
        rates = pipeline.materialize(RATES, at(2026, 1, 2, 6))

        result = pipeline.check(at(2026, 12, 2, 6), [RATES], {BALANCES: at(2026, 12, 2, 6)})

        assert result.checked is True
        assert accepted_ids(result) == [rates]

    @pytest.mark.parametrize(
        "anchor, positions_date, expected",
        [
            pytest.param(at(2026, 12, 1, 6), at(2026, 11, 2, 6), False, id="november-refused-on-1-december"),
            pytest.param(at(2026, 12, 2, 6), at(2026, 12, 2, 6), True, id="december-accepted-on-2-december"),
        ],
    )
    def test_a_dependency_firing_once_per_period_is_checked_in_the_cycle_window(self, harness, anchor, positions_date, expected):
        """Positions is a monthly table (the 2nd) read with the monthly-delivery
        table balances. It fires once in every period of the declared monthly
        cycle, so it is checked in the cycle window: the run anchored on
        1 December refuses its November materialization, which its own window
        (2 October 06:00, 2 November 06:00] would have accepted, and the run
        anchored on 2 December accepts its December one."""
        pipeline = harness({BALANCES: MONTHLY_DELIVERY, POSITIONS: "0 6 2 * *"}, MONTHLY)
        pipeline.materialize(POSITIONS, positions_date)

        result = pipeline.check(anchor, [POSITIONS], {BALANCES: anchor})

        assert result.checked is expected

    def test_the_default_data_cycle_keeps_the_dataset_windows(self, harness):
        """Daily loads at 06:00 (balances) and 07:00 (contracts), with the data
        cycle enabled but not declared: it defaults to the pipeline's computed
        cron, which carries the hour of one dependency and delimits no
        delivery. Both keep their own window, and the balances materialization
        of 06:00 is accepted in the run anchored by contracts at 07:00, as
        before. In data-cycle mode, (2 December 06:00, 2 December 07:00] would
        refuse it."""
        pipeline = harness({BALANCES: "0 6 * * *", CONTRACTS: "0 7 * * *"}, {"data_cycle_enabled": "true"})
        assert pipeline.job.data_cycle is not None
        assert pipeline.job.data_cycle_declared is False
        # the default cycle that would refuse the 06:00 materialization in data-cycle mode
        pipeline.job.data_cycle = "0 6 * * *"
        balances = pipeline.materialize(BALANCES, at(2026, 12, 2, 6))

        result = pipeline.check(at(2026, 12, 2, 7), [BALANCES], {CONTRACTS: at(2026, 12, 2, 7)})

        assert result.checked is True
        assert accepted_ids(result) == [balances]

    @pytest.mark.parametrize(
        "options",
        [
            pytest.param({}, id="no-data-cycle"),
            pytest.param({"data_cycle": "monthly"}, id="data-cycle-not-enabled"),
        ],
    )
    def test_without_data_cycle_nothing_changes(self, harness, options):
        """The tick-by-tick window (1 December 06:00, 2 December 06:00] still
        refuses the balances materialization of 1 December."""
        pipeline = harness(DELIVERY, options)
        pipeline.materialize(BALANCES, at(2026, 12, 1, 6))

        result = pipeline.check(at(2026, 12, 2, 6), [BALANCES], {CONTRACTS: at(2026, 12, 2, 6)})

        assert result.checked is False
        assert [dataset.uri for dataset in result.missing] == [BALANCES]

    @pytest.mark.parametrize("month", range(1, 13))
    def test_the_verdicts_do_not_depend_on_the_month_the_check_runs_in(self, harness, monkeypatch, month):
        """get_cron_frequency measures the gap between the next two ticks from
        the wall clock: on the 2nd of a month, 29 days plus a freshness of two
        days for the monthly delivery against 31 days for the monthly cycle in
        December, against 28 days in January. The beyond-data-cycle widening
        therefore depended on the month; the data-cycle window does not, and
        the mode choice never asks get_cron_frequency about the dependency. The
        wall clock is emulated on the 2nd of each month."""
        from croniter import croniter
        from ai.starlake.dagster import starlake_dagster_orchestration as orchestration_module

        now = at(2026, month, 2, 7)
        asked = []

        def get_cron_frequency(cron):
            asked.append(cron)
            ticks = croniter(cron, now)
            first = ticks.get_next(datetime)
            return ticks.get_next(datetime) - first

        monkeypatch.setattr(orchestration_module, "get_cron_frequency", get_cron_frequency)
        pipeline = harness(DELIVERY, MONTHLY)
        pipeline.materialize(BALANCES, at(2026, 12, 1, 6), freshness=TWO_DAYS)
        pipeline.materialize(CONTRACTS, at(2026, 11, 2, 6), freshness=TWO_DAYS)

        accepted = pipeline.check(at(2026, 12, 2, 6), [BALANCES], {CONTRACTS: at(2026, 12, 2, 6)}, freshness={BALANCES: TWO_DAYS})
        refused = pipeline.check(at(2026, 12, 1, 6), [CONTRACTS], {BALANCES: at(2026, 12, 1, 6)}, freshness={CONTRACTS: TWO_DAYS})

        assert (accepted.checked, refused.checked) == (True, False)
        assert MONTHLY_DELIVERY not in asked


class TestDataCycleLookup:
    """The event-log lookup of a dependency in data-cycle mode is not
    restricted to the two boundary partitions of a tick-by-tick window."""

    def test_a_materialization_partitioned_off_the_ticks_is_found(self, harness):
        """A manual load of balances on 1 December 09:13 (its partition is not
        a tick of its cron) is found by the lookup without partition filter."""
        pipeline = harness(DELIVERY, MONTHLY)
        manual = pipeline.materialize(BALANCES, at(2026, 12, 1, 9, 13), partition="2026-12-01T09:13:00+0000")

        result = pipeline.check(at(2026, 12, 2, 6), [BALANCES], {CONTRACTS: at(2026, 12, 2, 6)})

        assert result.checked is True
        assert accepted_ids(result) == [manual]

    def test_a_materialization_recorded_before_many_others_is_found_on_its_tick(self, harness):
        """The October materialization of balances is followed by more
        materializations than the lookup fetches without partition filter: the
        lookup on the ticks of the window still finds it."""
        pipeline = harness(DELIVERY, MONTHLY)
        october = pipeline.materialize(BALANCES, at(2026, 10, 1, 6))
        for month in (11, 12):
            for day in (1, 2, 3):
                pipeline.materialize(BALANCES, at(2026, month, day, 6))

        result = pipeline.check(at(2026, 10, 2, 6), [BALANCES], {CONTRACTS: at(2026, 10, 2, 6)}, limit=2)

        assert result.checked is True
        assert accepted_ids(result) == [october]

    def test_the_most_recently_recorded_materialization_of_the_window_is_accepted(self, harness):
        """Balances was loaded on 1 December, then reloaded for the same date,
        and a later materialization lies outside the window: the reload is
        accepted."""
        pipeline = harness(DELIVERY, MONTHLY)
        pipeline.materialize(BALANCES, at(2026, 12, 1, 6), options={"all": {"balances_file_date": "first"}})
        reload = pipeline.materialize(BALANCES, at(2026, 12, 1, 6), options={"all": {"balances_file_date": "reload"}})
        pipeline.materialize(BALANCES, at(2026, 12, 3, 6))

        result = pipeline.check(at(2026, 12, 2, 6), [BALANCES], {CONTRACTS: at(2026, 12, 2, 6)})

        assert result.checked is True
        assert accepted_ids(result) == [reload]
