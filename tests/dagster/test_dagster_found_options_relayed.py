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

"""Issue #168 — the run variables of the materializations that the sensor's
check accepted from the event log reach the tasks.

The pipeline sensor relays the sl_options of the materializations it has not
consumed yet. A dependency without an unconsumed materialization is checked in
the event log, and the materialization accepted there, already consumed by an
earlier tick, used not to be relayed: under the ANY strategy, the run of the
last load of a delivery to arrive passes the check thanks to the earlier loads'
materializations, and its tasks missed their variables.
"""

from __future__ import annotations

import json
import types

import pytest

from ai.starlake.dagster import DagsterLogicalDatetimeConfig, StarlakeDagsterUtils

from tests.dagster.dagster_check_harness import (  # noqa: F401 (harness is a fixture)
    BALANCES,
    CONTRACTS,
    OPTIONS,
    OPTIONS_MERGE,
    RATES,
    UNION,
    at,
    harness,
    relayed,
    relayed_merge,
)

DAILY = {BALANCES: "0 6 * * *", CONTRACTS: "0 7 * * *"}
MANUAL_RUN = "launch the run manually"
ONE_BY_ONE = "must be run one by one"


def _mat(uri: str, sections=None, merge=None):
    from dagster import AssetKey, AssetMaterialization, MetadataValue
    metadata = {}
    if sections is not None:
        metadata[OPTIONS] = MetadataValue.json(sections)
    if merge is not None:
        metadata[OPTIONS_MERGE] = MetadataValue.json(merge) if isinstance(merge, (dict, list)) else merge
    return AssetMaterialization(asset_key=AssetKey(uri), metadata=metadata)


def _accepted(uri: str, storage_id: int, sections=None, merge=None):
    """An EventLogRecord-like materialization accepted by the check."""
    from dagster import AssetKey
    return types.SimpleNamespace(asset_key=AssetKey(uri), storage_id=storage_id, asset_materialization=_mat(uri, sections, merge))


def _tick_error():
    from dagster._core.errors import DagsterUserCodeExecutionError
    return (ValueError, DagsterUserCodeExecutionError)


def _delivered(pipeline, balances=None, contracts=None):
    """Both loads of 1 December, consumed by the run of that day."""
    pipeline.materialize(BALANCES, at(2026, 12, 1, 6), options=balances)
    pipeline.materialize(CONTRACTS, at(2026, 12, 1, 7), options=contracts)
    assert len(pipeline.tick().run_requests) == 1


# ------------------------------------------------------------------
# the pipeline sensor — the accepted materializations reach the run config
# ------------------------------------------------------------------

class TestSensorRelaysAcceptedMaterializations:

    def test_the_run_that_completes_the_delivery_receives_both_variables(self, harness):
        """The reproduction: daily loads of balances at 06:00 and contracts at
        07:00, each publishing the business date it loaded. The tick after
        balances' load of 2 December accepts contracts' materialization of
        1 December from the event log; the tick after contracts' load of
        2 December accepts balances' materialization of 06:00, consumed by the
        previous tick: each run receives the variables of every materialization
        its check accepted."""
        pipeline = harness(DAILY)
        _delivered(pipeline, {"all": {"balances_file_date": "2026-12-01"}}, {"all": {"contracts_file_date": "2026-12-01"}})

        pipeline.materialize(BALANCES, at(2026, 12, 2, 6), options={"all": {"balances_file_date": "2026-12-02"}})
        morning = pipeline.tick()
        pipeline.materialize(CONTRACTS, at(2026, 12, 2, 7), options={"all": {"contracts_file_date": "2026-12-02"}})
        completing = pipeline.tick()

        assert relayed(morning) == {"all": {"balances_file_date": "2026-12-02", "contracts_file_date": "2026-12-01"}}
        assert completing.run_requests[0].partition_key == "2026-12-02T07:00:00+0000"
        assert relayed(completing) == {"all": {"balances_file_date": "2026-12-02", "contracts_file_date": "2026-12-02"}}

    def test_a_reloaded_partition_relays_its_latest_materialization(self, harness):
        """Balances' partition of 2 December is loaded twice (a reload), each
        load consumed by a run of its own; contracts' load of 07:00 then
        completes the delivery. The lookup of balances finds both
        materializations in its window and accepts the most recent one, as
        Airflow does: the run receives the reloaded value."""
        pipeline = harness(DAILY)
        _delivered(pipeline)
        pipeline.materialize(BALANCES, at(2026, 12, 2, 6), options={"all": {"balances_file_date": "first load"}})
        assert len(pipeline.tick().run_requests) == 1
        pipeline.materialize(BALANCES, at(2026, 12, 2, 6), options={"all": {"balances_file_date": "reload"}})
        assert len(pipeline.tick().run_requests) == 1
        pipeline.materialize(CONTRACTS, at(2026, 12, 2, 7), options={"all": {"contracts_file_date": "2026-12-02"}})

        assert relayed(pipeline.tick()) == {"all": {"balances_file_date": "reload", "contracts_file_date": "2026-12-02"}}

    def test_a_conflict_with_an_accepted_materialization_fails_naming_both(self, harness):
        pipeline = harness(DAILY)
        _delivered(pipeline)
        balances = pipeline.materialize(BALANCES, at(2026, 12, 2, 6), options={"all": {"file_date": "2026-12-01"}})
        assert len(pipeline.tick().run_requests) == 1
        pipeline.materialize(CONTRACTS, at(2026, 12, 2, 7), options={"all": {"file_date": "2026-12-02"}})

        with pytest.raises(_tick_error()) as error:
            pipeline.tick()

        message = str(error.value)
        assert "Conflicting values for sl_options['all']['file_date']" in message
        assert "finance_contracts" in message
        assert f"finance_balances (materialization {balances} accepted by the sensor)" in message
        assert MANUAL_RUN in message
        assert ONE_BY_ONE not in message
        assert "policy 'union'" in message

    def test_a_union_declared_by_the_accepted_materialization_merges_both_values(self, harness):
        """Contracts ran twice since the previous run (both unconsumed) without
        declaring its key; the balances materialization accepted from the event
        log declares it union: every value joins the union, the earlier
        unconsumed one of contracts included."""
        pipeline = harness(DAILY)
        _delivered(pipeline)
        pipeline.materialize(BALANCES, at(2026, 12, 2, 6), options={"all": {"file_dates": "2026-12-01"}}, merge={"all": {"file_dates": UNION}})
        assert len(pipeline.tick().run_requests) == 1
        pipeline.materialize(CONTRACTS, at(2026, 12, 2, 7), options={"all": {"file_dates": "2026-11-30"}})
        pipeline.materialize(CONTRACTS, at(2026, 12, 2, 7), options={"all": {"file_dates": "2026-12-02"}})

        result = pipeline.tick()

        assert relayed(result) == {"all": {"file_dates": "2026-11-30|2026-12-01|2026-12-02"}}
        assert relayed_merge(result) == {"all": {"file_dates": UNION}}

    @pytest.mark.parametrize("declared", [True, False])
    def test_a_conflict_among_the_unconsumed_materializations(self, harness, declared):
        """Balances and contracts, both unconsumed, carry the same key with
        different values without declaring it. A union declaration carried by
        the rates materialization that the check accepts applies to them too
        (the same merge as on Airflow); without it, the tick fails with the
        message of a conflict among the triggering materializations."""
        pipeline = harness({RATES: "0 5 * * *", BALANCES: "0 6 * * *", CONTRACTS: "0 7 * * *"})
        pipeline.materialize(RATES, at(2026, 12, 1, 5))
        pipeline.materialize(BALANCES, at(2026, 12, 1, 6))
        pipeline.materialize(CONTRACTS, at(2026, 12, 1, 7))
        assert len(pipeline.tick().run_requests) == 1
        pipeline.materialize(RATES, at(2026, 12, 2, 5), options={"all": {"fd": "R"}}, merge={"all": {"fd": UNION}} if declared else None)
        assert len(pipeline.tick().run_requests) == 1
        pipeline.materialize(BALANCES, at(2026, 12, 2, 6), options={"all": {"fd": "B"}})
        pipeline.materialize(CONTRACTS, at(2026, 12, 2, 7), options={"all": {"fd": "C"}})

        if declared:
            assert relayed(pipeline.tick()) == {"all": {"fd": "B|C|R"}}
        else:
            with pytest.raises(_tick_error()) as error:
                pipeline.tick()
            message = str(error.value)
            assert "across triggering materializations (" in message
            assert ONE_BY_ONE in message
            assert "accepted by the sensor" not in message

    def test_a_malformed_declaration_on_an_accepted_materialization_fails_naming_it(self, harness):
        pipeline = harness(DAILY)
        _delivered(pipeline)
        balances = pipeline.materialize(BALANCES, at(2026, 12, 2, 6), options={"all": {"k": "v"}}, merge="{not json")
        with pytest.raises(_tick_error(), match="Invalid sl_options_merge"):
            # the materialization fails the tick that relays it as a triggering one
            pipeline.tick()
        pipeline.materialize(CONTRACTS, at(2026, 12, 1, 7))
        pipeline.materialize(BALANCES, at(2026, 12, 1, 6))
        assert len(pipeline.tick().run_requests) == 1, "the malformed materialization is consumed by this run"
        pipeline.materialize(CONTRACTS, at(2026, 12, 2, 7), options={"all": {"contracts_file_date": "2026-12-02"}})

        with pytest.raises(_tick_error()) as error:
            pipeline.tick()

        assert f"Invalid sl_options_merge carried by finance_balances (materialization {balances} accepted by the sensor)" in str(error.value)

    def test_the_run_config_overrides_the_accepted_values(self, harness):
        """The ops of the requested run resolve the accepted materialization's
        value; a run launched manually with sl_options in its config (the
        recovery the conflict message names) resolves the operator's value."""
        pipeline = harness(DAILY)
        _delivered(pipeline)
        pipeline.materialize(BALANCES, at(2026, 12, 2, 6), options={"all": {"balances_file_date": "2026-12-02"}})
        assert len(pipeline.tick().run_requests) == 1
        pipeline.materialize(CONTRACTS, at(2026, 12, 2, 7))
        op_config = next(iter(pipeline.tick().run_requests[0].run_config["ops"].values()))["config"]

        relayed_config = DagsterLogicalDatetimeConfig(**op_config)
        manual_config = DagsterLogicalDatetimeConfig(**dict(op_config, sl_options=json.dumps({"all": {"balances_file_date": "2026-12-03"}})))

        assert StarlakeDagsterUtils.get_sl_options(None, relayed_config, "finance.position") == {"balances_file_date": "2026-12-02"}
        assert StarlakeDagsterUtils.get_sl_options(None, manual_config, "finance.position") == {"balances_file_date": "2026-12-03"}

    def test_without_run_variables_on_the_accepted_materialization_the_run_config_is_unchanged(self, harness):
        """The accepted balances materialization carries no run variables: the
        run config holds the unconsumed contracts' sections only, as before,
        and no sl_options at all when contracts carries none either."""
        pipeline = harness(DAILY)
        _delivered(pipeline)
        # contracts' materialization of 1 December, accepted, carries nothing either
        pipeline.materialize(BALANCES, at(2026, 12, 2, 6), options={"all": {}})
        without_any = pipeline.tick()
        pipeline.materialize(CONTRACTS, at(2026, 12, 2, 7), options={"all": {"contracts_file_date": "2026-12-02"}})
        with_contracts = pipeline.tick()

        assert relayed(without_any) is None
        for op in without_any.run_requests[0].run_config["ops"].values():
            assert "sl_options" not in op["config"]
            assert "sl_options_merge" not in op["config"]
        assert relayed(with_contracts) == {"all": {"contracts_file_date": "2026-12-02"}}
        assert relayed_merge(with_contracts) is None


# ------------------------------------------------------------------
# check_datasets_freshness — which materializations it returns as accepted
# ------------------------------------------------------------------

class TestCheckReturnsAcceptedMaterializations:

    def test_only_the_dependencies_without_an_unconsumed_materialization(self, harness):
        """Balances' unconsumed materialization (a replay of 30 November) falls
        outside its window, and the lookup accepts its materialization of
        2 December from the event log: the sensor relays the unconsumed one
        only, so the accepted one is not returned."""
        pipeline = harness(DAILY)
        regular = pipeline.materialize(BALANCES, at(2026, 12, 2, 6), options={"all": {"balances_file_date": "2026-12-02"}})
        anchor = at(2026, 12, 2, 7)

        triggering = pipeline.check(anchor, [BALANCES], {CONTRACTS: anchor, BALANCES: at(2026, 11, 30, 6)})
        explicit = pipeline.check(anchor, [BALANCES], {CONTRACTS: anchor}, triggering_uris=[CONTRACTS, BALANCES])
        missing = pipeline.check(anchor, [BALANCES], {CONTRACTS: anchor})

        assert (triggering.checked, triggering.accepted) == (True, [])
        assert (explicit.checked, explicit.accepted) == (True, [])
        assert missing.checked is True
        assert [record.storage_id for record in missing.accepted] == [regular]

    def test_only_the_in_window_materialization_of_the_lookup(self, harness):
        """The lookup returns both materializations of the boundary partitions;
        the replay of 1 December 06:00, recorded last, lies outside the window
        (1 December 06:00, 2 December 07:00]: only the materialization of
        2 December is accepted."""
        pipeline = harness(DAILY)
        in_window = pipeline.materialize(BALANCES, at(2026, 12, 2, 6), options={"all": {"balances_file_date": "2026-12-02"}})
        pipeline.materialize(BALANCES, at(2026, 12, 1, 6), options={"all": {"balances_file_date": "2026-12-01"}})

        result = pipeline.check(at(2026, 12, 2, 7), [BALANCES], {CONTRACTS: at(2026, 12, 2, 7)})

        assert result.checked is True
        assert [record.storage_id for record in result.accepted] == [in_window]

    def test_a_cron_less_dependency_found_in_the_event_log_is_returned(self, harness):
        pipeline = harness({BALANCES: None, CONTRACTS: "0 7 * * *"})
        found = pipeline.materialize(BALANCES, at(2026, 12, 2, 7))

        result = pipeline.check(at(2026, 12, 2, 7), [BALANCES], {CONTRACTS: at(2026, 12, 2, 7)})

        assert result.checked is True
        assert [record.storage_id for record in result.accepted] == [found]

    def test_nothing_is_returned_when_the_check_fails(self, harness):
        pipeline = harness({BALANCES: "0 6 * * *", CONTRACTS: "0 7 * * *"})
        pipeline.materialize(BALANCES, at(2026, 12, 2, 6))

        result = pipeline.check(at(2026, 12, 2, 7), [BALANCES, CONTRACTS], {})

        assert result.checked is False
        assert result.accepted == []
        assert [dataset.uri for dataset in result.missing] == [CONTRACTS]

    def test_a_pipeline_without_previous_run_nor_start_date_is_not_checked(self, harness):
        """No previous run and a start date after the anchor: the check returns
        a verdict instead of False, which the sensor could not read."""
        pipeline = harness(DAILY, {"start_date": "2027-01-01"})

        result = pipeline.check(at(2026, 12, 2, 7), [BALANCES], {CONTRACTS: at(2026, 12, 2, 7)})

        assert (result.checked, result.previous_partition, result.max_scheduled_date, result.accepted) == (False, None, None, [])
        assert [dataset.uri for dataset in result.missing] == [BALANCES]


# ------------------------------------------------------------------
# collect_sl_options_with_merge — the accepted materializations' carriers
# ------------------------------------------------------------------

class TestCollectAcceptedMaterializations:

    def test_the_accepted_variables_are_merged_with_the_triggering_ones(self):
        merged, merge = StarlakeDagsterUtils.collect_sl_options_with_merge(
            [_mat(CONTRACTS, {"all": {"contracts_file_date": "2026-12-02"}})],
            accepted=[_accepted(BALANCES, 42, {"all": {"balances_file_date": "2026-12-02"}, "d.t": {"k": "v"}})],
        )
        assert merged == {"all": {"contracts_file_date": "2026-12-02", "balances_file_date": "2026-12-02"}, "d.t": {"k": "v"}}
        assert merge == {}

    def test_a_conflict_with_an_accepted_materialization_names_it_and_asks_for_a_manual_run(self):
        with pytest.raises(ValueError) as error:
            StarlakeDagsterUtils.collect_sl_options_with_merge(
                [_mat(CONTRACTS, {"all": {"fd": "2026-12-02"}})],
                accepted=[_accepted(BALANCES, 42, {"all": {"fd": "2026-12-01"}})],
            )
        message = str(error.value)
        assert "Conflicting values" in message
        assert "across triggering materializations and the materializations accepted by the sensor" in message
        assert "finance_balances (materialization 42 accepted by the sensor)" in message
        assert MANUAL_RUN in message

    def test_a_conflict_among_the_triggering_materializations_keeps_its_message(self):
        """The accepted materialization carries run variables, but the
        conflict lies between the two triggering ones."""
        with pytest.raises(ValueError) as error:
            StarlakeDagsterUtils.collect_sl_options_with_merge(
                [_mat(CONTRACTS, {"all": {"fd": "A"}}), _mat(BALANCES, {"all": {"fd": "B"}})],
                accepted=[_accepted("finance_rates", 42, {"all": {"other": "x"}})],
            )
        message = str(error.value)
        assert "across triggering materializations (" in message
        assert ONE_BY_ONE in message
        assert MANUAL_RUN not in message
        assert "accepted by the sensor" not in message

    @pytest.mark.parametrize("declared_by", ["triggering", "accepted"])
    def test_a_union_declared_by_either_side_merges_both_values(self, declared_by):
        declaration = {"all": {"fd": UNION}}
        merged, merge = StarlakeDagsterUtils.collect_sl_options_with_merge(
            [_mat(CONTRACTS, {"all": {"fd": "B|C"}}, declaration if declared_by == "triggering" else None)],
            accepted=[_accepted(BALANCES, 42, {"all": {"fd": "A|B"}}, declaration if declared_by == "accepted" else None)],
        )
        assert merged == {"all": {"fd": "A|B|C"}}
        assert merge == declaration

    def test_different_declarations_on_both_sides_fail(self):
        with pytest.raises(ValueError, match="Conflicting merge declarations"):
            StarlakeDagsterUtils.collect_sl_options_with_merge(
                [_mat(CONTRACTS, {"all": {"fd": "B"}}, {"all": {"fd": UNION}})],
                accepted=[_accepted(BALANCES, 42, {"all": {"fd": "A"}}, {"all": {"fd": {"policy": "strict"}}})],
            )

    @pytest.mark.parametrize("merge", ["{not json", ["union"], {"all": "union"}])
    def test_a_malformed_declaration_on_an_accepted_materialization_fails_naming_it(self, merge):
        with pytest.raises(ValueError, match=r"Invalid sl_options_merge.* carried by finance_balances \(materialization 42 accepted by the sensor\)"):
            StarlakeDagsterUtils.collect_sl_options_with_merge(
                [_mat(CONTRACTS, {"all": {"fd": "B"}})],
                accepted=[_accepted(BALANCES, 42, None, merge)],
            )

    def test_accepted_materializations_without_run_variables_change_nothing(self):
        triggering = [_mat(CONTRACTS, {"all": {"fd": "B"}}, {"all": {"fd": UNION}})]
        nothing = [
            _accepted(BALANCES, 42),
            _accepted(BALANCES, 43, {"all": {}}, {"all": {}}),
            _accepted(BALANCES, 44, {"all": "not-a-dict"}),
            types.SimpleNamespace(asset_key=None, storage_id=45, asset_materialization=None),
        ]
        assert StarlakeDagsterUtils.collect_sl_options_with_merge(triggering, accepted=nothing) == \
            StarlakeDagsterUtils.collect_sl_options_with_merge(triggering)
        with pytest.raises(ValueError) as error:
            StarlakeDagsterUtils.collect_sl_options_with_merge(
                [_mat(CONTRACTS, {"all": {"fd": "A"}}), _mat(BALANCES, {"all": {"fd": "B"}})],
                accepted=nothing,
            )
        assert ONE_BY_ONE in str(error.value)
