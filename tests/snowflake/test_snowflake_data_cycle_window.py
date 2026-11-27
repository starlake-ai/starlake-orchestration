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
"""The data-cycle window of the root task (issue #167).

Two tables of one monthly delivery, whose files arrive on any of the first
three days of the month, are loaded with the cron "0 6 1-3 * *". A transform
pipeline without schedule reads both: its root task polls, anchors the run on
the scheduled date (the latest tick of the pipeline's computed cron at or
before the logical date) and looks each dependency up in the Starlake audit
table, by scheduled date in (min, max]. Without a data cycle, the window of a
dependency is computed tick by tick from its own cron: the run anchored on
2 December refuses the event of 1 December.

With a declared data cycle, a dependency that fires in every period of it is
checked in data-cycle mode: its events are admitted from the start of the cycle
period that holds the anchor up to the anchor. The mode is decided at DAG build
time, by the job's rule, and reaches the stored procedure as a set of sinks
serialized with its closure. Any other dependency keeps its own schedule window
(dataset mode), and nothing changes without a data cycle or with the default one
(the pipeline's computed cron).

The root task runs here against a fake Snowpark session that answers its
queries: the real audit query is filtered on its own bounds, so the exclusive
lower bound and the inclusive upper bound are the ones Snowflake applies.
"""

from __future__ import annotations

import json
import pickle
import re
import sys
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple

import pytest
import pytz

from tests.snowflake.snowflake_test_mixin import SnowflakeTestMixin

UTC = pytz.UTC

MONTHLY_DELIVERY = "0 6 1-3 * *"
YEARLY = "0 6 2 1 *"
MONTHLY_ON_THE_2ND = "0 6 2 * *"
MONTHLY = {"data_cycle_enabled": "true", "data_cycle": "monthly"}
TWO_DAYS = 2 * 24 * 3600

BALANCES = "finance.balances"
CONTRACTS = "finance.contracts"
RATES = "finance.rates"
POSITIONS = "finance.positions"

AUDIT_FORMAT = "%Y-%m-%d %H:%M:%S %z"


def at(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


class FakeSession:
    """Answers the queries of the root task. `audit` holds the scheduled dates
    of the successful runs of each table; the audit lookups are recorded as
    (sink, min, max) and the return value set by the root task tells the
    verdict (an empty string skips the run)."""

    def __init__(self, audit: Dict[str, List[datetime]], ts: datetime, logical_date: Optional[datetime] = None, previous_run: Optional[Tuple[datetime, datetime]] = None) -> None:
        self.audit = audit
        # the previous successful run of the DAG, as (scheduled time, start time)
        self.previous_run = previous_run
        self.ts = ts
        self.config = {"logical_date": logical_date.isoformat()} if logical_date else {}
        self.lookups: List[Tuple[str, datetime, datetime]] = []
        self.return_values: List[str] = []

    def call(self, name: str, *args):
        assert name == "system$get_task_graph_config"
        return json.dumps(self.config) if self.config else None

    def sql(self, query: str):
        rows = self._answer(query)
        return SimpleNamespace(collect=lambda: rows)

    def _answer(self, query: str) -> list:
        if "CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP" in query:
            return [(self.ts,)]
        if "SYSTEM$SET_RETURN_VALUE" in query:
            self.return_values.append(re.search(r"SYSTEM\$SET_RETURN_VALUE\('(.*)'\)", query).group(1))
            return []
        if "SCHEDULED_TIME, QUERY_START_TIME" in query:
            return [self.previous_run] if self.previous_run else []
        if "TASK_HISTORY" in query:
            return []
        if "INFORMATION_SCHEMA.TABLES" in query:
            return [("exists",)]
        if "FROM audit.audit" in query:
            domain = re.search(r"DOMAIN ilike '([^']+)'", query).group(1)
            schema = re.search(r"SCHEMA ilike '([^']+)'", query).group(1)
            lower = datetime.strptime(re.search(r"SCHEDULED_DATE > TO_TIMESTAMP\('([^']+)'\)", query).group(1), AUDIT_FORMAT)
            upper = datetime.strptime(re.search(r"SCHEDULED_DATE <= TO_TIMESTAMP\('([^']+)'\)", query).group(1), AUDIT_FORMAT)
            sink = f"{domain}.{schema}"
            self.lookups.append((sink, lower, upper))
            # an entry is a scheduled date, recorded ten minutes later, or a
            # (scheduled date, record time) pair; the latest scheduled date
            # comes first, as the query orders them
            records = [entry if isinstance(entry, tuple) else (entry, entry + timedelta(minutes=10)) for entry in self.audit.get(sink, [])]
            return sorted(((recorded, date) for date, recorded in records if lower < date <= upper), key=lambda row: (row[1], row[0]), reverse=True)
        # ALTER SESSION, CURRENT_TASK_GRAPH_RUN_GROUP_ID (none: the run is
        # considered scheduled)
        return []

    @property
    def verdict(self) -> bool:
        assert len(self.return_values) == 1
        return self.return_values[0] != ""

    def window(self, sink: str) -> Tuple[datetime, datetime]:
        windows = [(lower, upper) for looked_up, lower, upper in self.lookups if looked_up == sink]
        assert len(windows) == 1, f"{sink} looked up {len(windows)} times: {self.lookups}"
        return windows[0]


def pin_wall_clock(monkeypatch, now: datetime) -> None:
    """get_cron_frequency measures the gap between the next two ticks of a
    cron from the wall clock: the DAG build (the computed cron, the most
    frequent crons) and the root task (optional, beyond_data_cycle) read it.
    It is emulated at `now`; the root task's closure captures the emulation,
    since SnowflakeDag imports get_cron_frequency when it is built."""
    from croniter import croniter
    from ai.starlake import common

    def get_cron_frequency(cron):
        if not common.is_valid_cron(cron):
            raise ValueError(f"Invalid cron expression: {cron}")
        ticks = croniter(cron, now)
        first = ticks.get_next(datetime)
        return ticks.get_next(datetime) - first

    monkeypatch.setattr(common, "get_cron_frequency", get_cron_frequency)


def transform_pipeline(options: dict, tables: List[Tuple[str, str, int]]):
    """A transform pipeline without schedule, reading `tables`, a list of
    (sink, cron, freshness in seconds)."""
    from ai.starlake.orchestration import StarlakeDependencies, StarlakeDependency, StarlakeDependencyType

    dependencies = StarlakeDependencies([
        StarlakeDependency(
            name="finance.monthly_position",
            dependency_type=StarlakeDependencyType.TASK,
            dependencies=[
                StarlakeDependency(name=sink, dependency_type=StarlakeDependencyType.TABLE, cron=cron, freshness=freshness)
                for sink, cron, freshness in tables
            ],
        ),
    ])
    pipeline = SnowflakeTestMixin().create_orchestration(options=options).sl_create_pipeline(dependencies=dependencies)
    with pipeline:
        pass
    assert pipeline.dag.schedule == timedelta(seconds=900), "only a pipeline without schedule checks its dependencies"
    return pipeline


def delivery(freshness: Optional[Dict[str, int]] = None) -> List[Tuple[str, str, int]]:
    freshness = freshness or {}
    return [(BALANCES, MONTHLY_DELIVERY, freshness.get(BALANCES, 0)), (CONTRACTS, MONTHLY_DELIVERY, freshness.get(CONTRACTS, 0))]


def run_root_task(pipeline, audit, ts: datetime, logical_date: Optional[datetime] = None, func=None, previous_run=None) -> FakeSession:
    """Runs the root task as the deployed task graph calls it (args=[False,
    None]): the logical date comes from the task graph configuration, or is
    the execution date of the polling run."""
    session = FakeSession(audit, ts, logical_date, previous_run)
    (func or pipeline.dag.definition.func)(session, False, None)
    return session


def check(monkeypatch, audit, logical_date: datetime, options=MONTHLY, tables=None, ts=None, wall_clock=None):
    ts = ts or logical_date + timedelta(minutes=15)
    pin_wall_clock(monkeypatch, wall_clock or ts)
    pipeline = transform_pipeline(options, tables or delivery())
    return pipeline, run_root_task(pipeline, audit, ts, logical_date)


class TestSnowflakeDataCycleWindow:

    def test_an_earlier_attempt_of_the_same_delivery_is_accepted(self, monkeypatch):
        """The contracts file arrived on 2 December, the balances file on
        1 December. The polling run of 2 December 06:15 is anchored on
        2 December 06:00, and both tables fire in every period of the monthly
        cycle: their events are admitted within (1 December 00:00, 2 December
        06:00], so the run accepts the balances event of 1 December."""
        pin_wall_clock(monkeypatch, at(2026, 12, 2, 6, 15))
        pipeline = transform_pipeline(MONTHLY, delivery())
        session = run_root_task(pipeline, {BALANCES: [at(2026, 12, 1, 6)], CONTRACTS: [at(2026, 12, 2, 6)]}, at(2026, 12, 2, 6, 15))

        assert session.verdict is True
        assert session.return_values == ["2026-12-02 06:00:00 +0000"]
        assert session.window(BALANCES) == (at(2026, 12, 1), at(2026, 12, 2, 6))
        assert session.window(CONTRACTS) == (at(2026, 12, 1), at(2026, 12, 2, 6))
        assert pipeline.dag.data_cycle_mode_datasets == {BALANCES, CONTRACTS}

    @pytest.mark.parametrize("freshness", [0, TWO_DAYS, 31 * 24 * 3600])
    def test_the_previous_delivery_is_refused_at_the_start_of_a_cycle(self, monkeypatch, freshness):
        """The balances file arrived on 1 December; the latest contracts event
        is the one of the November delivery (2 November). The window
        (1 December 00:00, 1 December 06:00] refuses it, whatever the freshness
        of contracts: the run waits for the December file instead of running
        on last month's data."""
        _, session = check(
            monkeypatch,
            {BALANCES: [at(2026, 11, 1, 6), at(2026, 12, 1, 6)], CONTRACTS: [at(2026, 11, 2, 6)]},
            at(2026, 12, 1, 6),
            tables=delivery({CONTRACTS: freshness}),
        )

        assert session.verdict is False
        assert session.window(CONTRACTS) == (at(2026, 12, 1), at(2026, 12, 1, 6))

    def test_a_logical_date_in_october_checks_the_october_period(self, monkeypatch):
        """A contracts file of the October delivery is reloaded in December;
        the run for October is requested with a logical date in October (the
        task graph configuration), anchored on 2 October 06:00. The balances
        event of 1 October is accepted in (1 October 00:00, 2 October 06:00],
        and the December events lie beyond the anchor."""
        _, session = check(
            monkeypatch,
            {BALANCES: [at(2026, 10, 1, 6), at(2026, 12, 1, 6)], CONTRACTS: [at(2026, 10, 2, 6), at(2026, 12, 2, 6)]},
            at(2026, 10, 2, 6),
            ts=at(2026, 12, 10, 9),
        )

        assert session.verdict is True
        assert session.return_values == ["2026-10-02 06:00:00 +0000"]
        assert session.window(BALANCES) == (at(2026, 10, 1), at(2026, 10, 2, 6))

    def test_a_dependency_less_frequent_than_the_cycle_keeps_its_own_window(self, monkeypatch):
        """Rates is a yearly table (2 January) in the same monthly pipeline. It
        misses most months, so it is checked in dataset mode, with its own
        schedule window raised to the anchor, (2 January 2026 06:00,
        2 December 2026 06:00]: the pipeline still runs in December, while the
        balances event of 1 December is accepted in the cycle window."""
        pipeline, session = check(
            monkeypatch,
            {BALANCES: [at(2026, 12, 1, 6)], RATES: [at(2026, 1, 2, 6)]},
            at(2026, 12, 2, 6),
            tables=[(BALANCES, MONTHLY_DELIVERY, 0), (RATES, YEARLY, 0)],
        )

        assert session.verdict is True
        assert session.window(RATES) == (at(2025, 1, 2, 6), at(2026, 12, 2, 6))
        assert session.window(BALANCES) == (at(2026, 12, 1), at(2026, 12, 2, 6))
        assert pipeline.dag.data_cycle_mode_datasets == {BALANCES}

    def test_an_optional_dependency_less_frequent_than_the_cycle_is_skipped(self, monkeypatch):
        """With optional_dataset_enabled, the yearly table refreshes less often
        than the monthly cycle: it stays optional (dataset mode) and is not
        looked up, while the monthly delivery is never optional."""
        _, session = check(
            monkeypatch,
            {BALANCES: [at(2026, 12, 1, 6)]},
            at(2026, 12, 2, 6),
            options={**MONTHLY, "optional_dataset_enabled": "true"},
            tables=[(BALANCES, MONTHLY_DELIVERY, 0), (RATES, YEARLY, 0)],
        )

        assert session.verdict is True
        assert [sink for sink, _, _ in session.lookups] == [BALANCES]

    @pytest.mark.parametrize(
        "logical_date, positions_event, expected",
        [
            pytest.param(at(2026, 12, 1, 6), at(2026, 11, 2, 6), False, id="november-refused-on-1-december"),
            pytest.param(at(2026, 12, 2, 6), at(2026, 12, 2, 6), True, id="december-accepted-on-2-december"),
        ],
    )
    def test_a_dependency_firing_once_per_period_is_checked_in_the_cycle_window(self, monkeypatch, logical_date, positions_event, expected):
        """Positions is a monthly table (the 2nd) read with the monthly-delivery
        table balances. It fires once in every period of the declared monthly
        cycle, so it is checked in the cycle window: the run anchored on
        1 December refuses its November event, which its own window raised to
        the anchor, (2 October 06:00, 1 December 06:00], would have accepted,
        and the run anchored on 2 December accepts its December event."""
        pipeline, session = check(
            monkeypatch,
            {BALANCES: [at(2026, 12, 1, 6)], POSITIONS: [positions_event]},
            logical_date,
            tables=[(BALANCES, MONTHLY_DELIVERY, 0), (POSITIONS, MONTHLY_ON_THE_2ND, 0)],
        )

        assert session.verdict is expected
        assert session.window(POSITIONS) == (at(2026, 12, 1), logical_date)
        assert pipeline.dag.data_cycle_mode_datasets == {BALANCES, POSITIONS}

    def test_the_default_data_cycle_keeps_the_dataset_windows(self, monkeypatch):
        """Daily loads at 06:00 (balances) and 07:00 (contracts), with the data
        cycle enabled but not declared: it defaults to the pipeline's computed
        cron, which carries the hour of one load and delimits no delivery. Both
        keep their own window: the run anchored on 2 December 07:00 looks the
        06:00 load up in (1 December 06:00, 2 December 07:00], as before, not
        in the (1 December 07:00, 2 December 07:00] of the data-cycle mode."""
        pipeline, session = check(
            monkeypatch,
            {BALANCES: [at(2026, 12, 2, 6)], CONTRACTS: [at(2026, 12, 2, 7)]},
            at(2026, 12, 2, 7),
            options={"data_cycle_enabled": "true"},
            tables=[(BALANCES, "0 6 * * *", 0), (CONTRACTS, "0 7 * * *", 0)],
        )

        assert pipeline.job.data_cycle in ("0 6 * * *", "0 7 * * *")
        assert pipeline.job.data_cycle_declared is False
        assert session.verdict is True
        assert session.window(BALANCES) == (at(2026, 12, 1, 6), at(2026, 12, 2, 7))
        assert session.window(CONTRACTS) == (at(2026, 12, 1, 7), at(2026, 12, 2, 7))
        assert pipeline.dag.data_cycle_mode_datasets == set()

    @pytest.mark.parametrize(
        "options",
        [
            pytest.param({}, id="no-data-cycle"),
            pytest.param({"data_cycle": "monthly"}, id="data-cycle-not-enabled"),
        ],
    )
    def test_without_data_cycle_nothing_changes(self, monkeypatch, options):
        """The tick-by-tick window (1 December 06:00, 2 December 06:00] still
        refuses the balances event of 1 December."""
        pipeline, session = check(
            monkeypatch, {BALANCES: [at(2026, 12, 1, 6)], CONTRACTS: [at(2026, 12, 2, 6)]}, at(2026, 12, 2, 6), options=options
        )

        assert session.verdict is False
        assert session.window(BALANCES) == (at(2026, 12, 1, 6), at(2026, 12, 2, 6))
        assert pipeline.dag.data_cycle_mode_datasets == set()

    @pytest.mark.parametrize("month", range(1, 13))
    def test_the_verdicts_do_not_depend_on_the_month_the_check_runs_in(self, monkeypatch, month):
        """get_cron_frequency measures the gap between the next two ticks from
        the wall clock, so the beyond-data-cycle widening of the monthly
        delivery, with a freshness of two days, depended on the month; the
        data-cycle window does not. The wall clock is emulated on the 2nd of
        each month, for the DAG build and the root task."""
        wall_clock = at(2027 if month == 1 else 2026, month, 2, 7)
        _, accepting = check(
            monkeypatch,
            {BALANCES: [at(2026, 12, 1, 6)], CONTRACTS: [at(2026, 12, 2, 6)]},
            at(2026, 12, 2, 6),
            tables=delivery({BALANCES: TWO_DAYS, CONTRACTS: TWO_DAYS}),
            wall_clock=wall_clock,
        )
        _, refusing = check(
            monkeypatch,
            {BALANCES: [at(2026, 12, 1, 6)], CONTRACTS: [at(2026, 11, 2, 6)]},
            at(2026, 12, 1, 6),
            tables=delivery({BALANCES: TWO_DAYS, CONTRACTS: TWO_DAYS}),
            wall_clock=wall_clock,
        )

        assert (accepting.verdict, refusing.verdict) == (True, False)


class TestSnowflakeRunsOncePerDelivery:
    """The root task polls instead of consuming events: in the cycle window,
    the events that ran the DAG earlier in the period satisfy every later tick
    of it. With a dataset in data-cycle mode, the DAG runs again only when a
    dataset has an event scheduled after the scheduled date of the previous
    successful run."""

    # both tables loaded on 1 December, and the DAG run that processed them
    DELIVERED = {BALANCES: [at(2026, 12, 1, 6)], CONTRACTS: [at(2026, 12, 1, 6)]}
    RUN_OF_1_DECEMBER = (at(2026, 12, 1, 6), at(2026, 12, 1, 6, 15))

    def _poll(self, monkeypatch, audit, previous_run, options=MONTHLY, tables=None):
        ts = at(2026, 12, 2, 6, 15)
        pin_wall_clock(monkeypatch, ts)
        pipeline = transform_pipeline(options, tables or delivery())
        return run_root_task(pipeline, audit, ts, previous_run=previous_run)

    def test_the_next_tick_of_the_period_does_not_run_the_same_delivery_again(self, monkeypatch):
        assert self._poll(monkeypatch, self.DELIVERED, self.RUN_OF_1_DECEMBER).verdict is False

    def test_a_load_since_the_previous_run_runs_the_dag_again(self, monkeypatch):
        # contracts' file is reloaded on 2 December: new data for the period
        audit = {BALANCES: [at(2026, 12, 1, 6)], CONTRACTS: [at(2026, 12, 1, 6), at(2026, 12, 2, 6)]}
        assert self._poll(monkeypatch, audit, self.RUN_OF_1_DECEMBER).verdict is True

    def test_a_reload_of_an_attempt_already_processed_does_not_run_again(self, monkeypatch):
        """The attempt of 1 December recorded again after the previous run:
        its scheduled date is not after the previous run's. The record times
        are not compared: the audit TIMESTAMP is the start of a load written
        by Starlake core, stored without time zone, and the start time that
        TASK_HISTORY gives is not the run's own."""
        audit = {BALANCES: [at(2026, 12, 1, 6)], CONTRACTS: [(at(2026, 12, 1, 6), at(2026, 12, 2, 5))]}
        assert self._poll(monkeypatch, audit, self.RUN_OF_1_DECEMBER).verdict is False

    def test_a_previous_run_of_another_period_does_not_hide_the_delivery(self, monkeypatch):
        # a backfill of October succeeded after the December delivery was
        # recorded: its scheduled date is October's
        october_backfill = (at(2026, 10, 2, 6), at(2026, 12, 1, 10))
        assert self._poll(monkeypatch, self.DELIVERED, october_backfill).verdict is True

    def test_the_first_run_of_the_period_is_not_guarded(self, monkeypatch):
        previous_month = (at(2026, 11, 3, 6), at(2026, 11, 3, 6, 15))
        assert self._poll(monkeypatch, self.DELIVERED, previous_month).verdict is True

    def test_dataset_mode_is_not_guarded(self, monkeypatch):
        """With the default data cycle, the datasets keep their own windows,
        here widened by a freshness of two days: the run is unchanged."""
        verdict = self._poll(
            monkeypatch,
            self.DELIVERED,
            self.RUN_OF_1_DECEMBER,
            options={"data_cycle_enabled": "true"},
            tables=delivery({BALANCES: TWO_DAYS, CONTRACTS: TWO_DAYS}),
        ).verdict
        assert verdict is True


class TestSnowflakeDataCycleModeSerialization:
    """The root task runs as an anonymous stored procedure: snowpark pickles
    its function with cloudpickle when the DAG is deployed, and the procedure
    imports what the pickle references by name from its packages and from the
    `ai` zip of zip_selected_packages (ai.starlake.common and ai.starlake.helper,
    not ai.starlake.job, ai.starlake.orchestration nor ai.starlake.snowflake)."""

    def test_the_closure_carries_the_decision_not_the_job(self):
        pipeline = transform_pipeline(MONTHLY, delivery())
        func = pipeline.dag.definition.func
        closure = dict(zip(func.__code__.co_freevars, (cell.cell_contents for cell in func.__closure__)))

        assert closure["data_cycle_mode_datasets"] == {BALANCES, CONTRACTS}
        assert closure["data_cycle"] == "0 0 1 * *"
        assert "data_cycle_mode" not in closure
        assert closure["data_cycle_window"].__module__ == "ai.starlake.common"

    def test_the_pickled_root_task_only_references_the_shipped_modules(self):
        # the releases that pickle the functions of this Python version
        # (snowpark excludes 2.1.0 and 2.2.0, broken on Python 3.11)
        minimum = "3.0.0" if sys.version_info >= (3, 12) else "2.2.1" if sys.version_info >= (3, 11) else "1.6.0"
        cloudpickle = pytest.importorskip("cloudpickle", minversion=minimum)
        import inspect
        from ai.starlake.helper import zip_selected_packages

        pipeline = transform_pipeline(MONTHLY, delivery())
        payload = cloudpickle.dumps(pipeline.dag.definition.func)

        references = set()

        class RecordingUnpickler(pickle.Unpickler):
            def find_class(self, module, name):
                references.add((module, name))
                return super().find_class(module, name)

        import io
        func = RecordingUnpickler(io.BytesIO(payload)).load()

        parameters = inspect.signature(zip_selected_packages).parameters
        excluded = parameters["exclude"].default
        ai_modules = {module for module, _ in references if module == "ai" or module.startswith("ai.")}
        assert ("ai.starlake.common", "data_cycle_window") in references
        assert ai_modules, references
        assert not [module for module in ai_modules if any(module == prefix or module.startswith(prefix + ".") for prefix in excluded)]

        # the unpickled root task decides as the original one
        session = run_root_task(pipeline, {BALANCES: [at(2026, 12, 1, 6)], CONTRACTS: [at(2026, 12, 2, 6)]}, at(2026, 12, 2, 6, 15), func=func)
        assert session.verdict is True
        assert session.window(BALANCES) == (at(2026, 12, 1), at(2026, 12, 2, 6))
