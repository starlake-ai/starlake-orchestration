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

from __future__ import annotations

import json
import types

import pytest

from ai.starlake.common import StarlakeParameters
from ai.starlake.dagster import DagsterLogicalDatetimeConfig, DagsterOrchestration, StarlakeDagsterUtils
from ai.starlake.dagster.shell import StarlakeDagsterShellJob
from ai.starlake.orchestration import StarlakeSchedule

from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME

OPTIONS = StarlakeParameters.OPTIONS_PARAMETER.value
OPTIONS_MERGE = StarlakeParameters.OPTIONS_MERGE_PARAMETER.value
UNION = {"policy": "union", "separator": "|"}


def _mat(uri: str, sections: dict, merge: dict = None):
    from dagster import AssetMaterialization, MetadataValue
    metadata = {OPTIONS: MetadataValue.json(sections)}
    if merge is not None:
        metadata[OPTIONS_MERGE] = MetadataValue.json(merge)
    return AssetMaterialization(asset_key=uri, metadata=metadata)


# ------------------------------------------------------------------
# collect_sl_options — merge across materializations, fail-loud
# ------------------------------------------------------------------

class TestCollectSlOptions:

    def test_empty_and_none_materializations(self):
        assert StarlakeDagsterUtils.collect_sl_options([]) == {}
        assert StarlakeDagsterUtils.collect_sl_options([None]) == {}
        assert StarlakeDagsterUtils.collect_sl_options(None) == {}

    def test_materialization_without_options_metadata(self):
        from dagster import AssetMaterialization
        assert StarlakeDagsterUtils.collect_sl_options([AssetMaterialization(asset_key="a")]) == {}

    def test_merges_sections_across_materializations(self):
        merged = StarlakeDagsterUtils.collect_sl_options([
            _mat("a", {"all": {"a_file_date": "2026-07-09"}}),
            _mat("b", {"all": {"b_file_date": "2026-07-10"}, "d.t": {"k": "v"}}),
        ])
        assert merged == {
            "all": {"a_file_date": "2026-07-09", "b_file_date": "2026-07-10"},
            "d.t": {"k": "v"},
        }

    def test_same_key_same_value_is_not_a_conflict(self):
        merged = StarlakeDagsterUtils.collect_sl_options([
            _mat("a", {"all": {"fd": "2026-07-09"}}),
            _mat("b", {"all": {"fd": "2026-07-09"}}),
        ])
        assert merged == {"all": {"fd": "2026-07-09"}}

    def test_conflicting_values_fail_loud(self):
        with pytest.raises(ValueError, match="Conflicting values"):
            StarlakeDagsterUtils.collect_sl_options([
                _mat("a", {"all": {"fd": "2026-07-08"}}),
                _mat("b", {"all": {"fd": "2026-07-09"}}),
            ])

    def test_non_dict_sections_are_ignored(self):
        merged = StarlakeDagsterUtils.collect_sl_options([
            _mat("a", {"all": "not-a-dict", "d.t": {"k": "v"}}),
        ])
        assert merged == {"d.t": {"k": "v"}}


# ------------------------------------------------------------------
# collect_sl_options — keys declared union by their producer (issue #158)
# ------------------------------------------------------------------

class TestCollectSlOptionsUnionMerge:

    def _union_mat(self, uri: str, value: str, declaration: dict = UNION):
        return _mat(uri, {"all": {"fd": value}}, {"all": {"fd": declaration}})

    def test_union_values_merge(self):
        merged = StarlakeDagsterUtils.collect_sl_options([
            self._union_mat("a", "A|B"),
            self._union_mat("b", "B|C"),
        ])
        assert merged == {"all": {"fd": "A|B|C"}}

    def test_the_order_of_materializations_does_not_change_the_result(self):
        a = self._union_mat("a", "2026-11-01|2026-11-02")
        b = self._union_mat("b", "2026-11-02|2026-11-03")
        expected = {"all": {"fd": "2026-11-01|2026-11-02|2026-11-03"}}
        assert StarlakeDagsterUtils.collect_sl_options([a, b]) == expected
        assert StarlakeDagsterUtils.collect_sl_options([b, a]) == expected

    def test_a_declaration_applies_to_materializations_that_do_not_declare(self):
        merged = StarlakeDagsterUtils.collect_sl_options([
            _mat("a", {"all": {"fd": "B|C"}}),
            self._union_mat("b", "A|B"),
        ])
        assert merged == {"all": {"fd": "A|B|C"}}

    def test_the_same_materializations_without_declaration_still_fail_loud(self):
        with pytest.raises(ValueError, match="Conflicting values"):
            StarlakeDagsterUtils.collect_sl_options([
                _mat("a", {"all": {"fd": "A|B"}}),
                _mat("b", {"all": {"fd": "B|C"}}),
            ])

    def test_different_separators_fail_loud_naming_the_key(self):
        with pytest.raises(ValueError, match=r"Conflicting merge declarations for sl_options_merge\['all'\]\['fd'\]"):
            StarlakeDagsterUtils.collect_sl_options([
                self._union_mat("a", "A|B"),
                self._union_mat("b", "B;C", {"policy": "union", "separator": ";"}),
            ])

    def test_different_policies_fail_loud_naming_the_key(self):
        with pytest.raises(ValueError, match=r"Conflicting merge declarations for sl_options_merge\['all'\]\['fd'\]"):
            StarlakeDagsterUtils.collect_sl_options([
                self._union_mat("a", "A|B"),
                self._union_mat("b", "A|B", {"policy": "strict"}),
            ])

    def test_the_declarations_are_returned_for_the_relay(self):
        merged, merge = StarlakeDagsterUtils.collect_sl_options_with_merge([
            self._union_mat("a", "A|B"),
            _mat("b", {"all": {"fd": "C", "k": "v"}}),
        ])
        assert merged == {"all": {"fd": "A|B|C", "k": "v"}}
        assert merge == {"all": {"fd": UNION}}

    def test_undecodable_merge_metadata_raises(self):
        from dagster import AssetMaterialization
        mat = AssetMaterialization(asset_key="a", metadata={
            OPTIONS: json.dumps({"all": {"fd": "A"}}),
            OPTIONS_MERGE: "{not json",
        })
        with pytest.raises(ValueError, match="Invalid sl_options_merge"):
            StarlakeDagsterUtils.collect_sl_options([mat])

    def test_json_string_metadata_is_read(self):
        from dagster import AssetMaterialization
        mat = AssetMaterialization(asset_key="a", metadata={
            OPTIONS: json.dumps({"all": {"fd": "B|A"}}),
            OPTIONS_MERGE: json.dumps({"all": {"fd": UNION}}),
        })
        assert StarlakeDagsterUtils.collect_sl_options([mat]) == {"all": {"fd": "A|B"}}

    def test_superseded_materializations_join_the_union_only(self):
        merged, merge = StarlakeDagsterUtils.collect_sl_options_with_merge(
            [_mat("orders", {"all": {"fd": "C", "k": "2"}}, {"all": {"fd": UNION}})],
            superseded=[_mat("orders", {"all": {"fd": "A|B", "k": "1"}}), None],
        )
        assert merged == {"all": {"fd": "A|B|C", "k": "2"}}
        assert merge == {"all": {"fd": UNION}}

    def test_the_run_config_is_the_recovery_escape_hatch(self):
        """A run launched by hand with sl_options in its config bypasses the
        sensor: the ops resolve those options, not the materializations'."""
        config = DagsterLogicalDatetimeConfig(logical_datetime=None, sl_options=json.dumps({"all": {"fd": "D"}}))
        assert StarlakeDagsterUtils.get_sl_options(None, config, "d.t") == {"fd": "D"}


# ------------------------------------------------------------------
# pipeline sensor — union merged and relayed through the run config
# ------------------------------------------------------------------

class TestSensorRelaysUnion:

    def _tick(self, materializations: dict, tmp_path):
        """Evaluate one tick after reporting the materializations: {uri: metadata},
        or {uri: [metadata, ...]} for several materializations of an asset, oldest first."""
        from dagster import AssetKey, AssetMaterialization, DagsterInstance, MetadataValue, build_multi_asset_sensor_context
        from tests.shared.triggering_scenarios import TWO_UPSTREAMS, make_dependencies

        job = StarlakeDagsterShellJob(
            filename="test_sl_options_sensor.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={"dataset_triggering_strategy": "all"},
        )
        with DagsterOrchestration(job=job) as orch:
            pipeline = orch.sl_create_pipeline(dependencies=make_dependencies(TWO_UPSTREAMS))
            with pipeline:
                start = pipeline.start_task()
                end = pipeline.end_task()
                start >> end
        sensors = orch.definitions.sensors
        assert len(sensors) == 1
        instance = DagsterInstance.local_temp(tempdir=str(tmp_path))
        try:
            for uri, metadatas in materializations.items():
                for metadata in metadatas if isinstance(metadatas, list) else [metadatas]:
                    instance.report_runless_asset_event(AssetMaterialization(
                        asset_key=AssetKey(uri),
                        metadata={key: MetadataValue.json(value) for key, value in metadata.items()},
                    ))
            context = build_multi_asset_sensor_context(
                monitored_assets=[AssetKey(uri) for uri in materializations],
                instance=instance,
                definitions=orch.definitions,
            )
            return sensors[0].evaluate_tick(context)
        finally:
            instance.dispose()

    def test_union_and_declaration_reach_every_op(self, tmp_path):
        result = self._tick({
            "starbake_orders": {OPTIONS: {"all": {"fd": "A|B"}}, OPTIONS_MERGE: {"all": {"fd": UNION}}},
            "starbake_customers": {OPTIONS: {"all": {"fd": "B|C"}}},
        }, tmp_path)
        assert len(result.run_requests) == 1
        ops = result.run_requests[0].run_config["ops"]
        assert len(ops) > 0
        for op_config in ops.values():
            assert json.loads(op_config["config"]["sl_options"]) == {"all": {"fd": "A|B|C"}}
            assert json.loads(op_config["config"]["sl_options_merge"]) == {"all": {"fd": UNION}}

    def test_earlier_materializations_of_an_asset_join_the_union(self, tmp_path):
        """A producer that ran several times since the consumer's last run:
        the sensor reads the latest materialization of each asset, the earlier
        runs' dates must not be lost."""
        declared = {"all": {"fd": UNION}}
        result = self._tick({
            "starbake_orders": [
                {OPTIONS: {"all": {"fd": "A|B", "k": "1"}}, OPTIONS_MERGE: declared},
                {OPTIONS: {"all": {"fd": "C", "k": "2"}}, OPTIONS_MERGE: declared},
            ],
            "starbake_customers": {OPTIONS: {"all": {"fd": "D"}}},
        }, tmp_path)
        assert len(result.run_requests) == 1
        for op_config in result.run_requests[0].run_config["ops"].values():
            # the union gathers every run; any other key keeps the latest run's value
            assert json.loads(op_config["config"]["sl_options"]) == {"all": {"fd": "A|B|C|D", "k": "2"}}

    def test_without_declaration_the_latest_materialization_of_an_asset_wins(self, tmp_path):
        result = self._tick({
            "starbake_orders": [{OPTIONS: {"all": {"k": "1"}}}, {OPTIONS: {"all": {"k": "2"}}}],
            "starbake_customers": {OPTIONS: {"all": {"other": "x"}}},
        }, tmp_path)
        assert len(result.run_requests) == 1
        for op_config in result.run_requests[0].run_config["ops"].values():
            assert json.loads(op_config["config"]["sl_options"]) == {"all": {"k": "2", "other": "x"}}
            assert "sl_options_merge" not in op_config["config"]

    def test_undeclared_conflict_fails_the_tick(self, tmp_path):
        from dagster._core.errors import DagsterUserCodeExecutionError
        with pytest.raises((ValueError, DagsterUserCodeExecutionError), match="Conflicting values"):
            self._tick({
                "starbake_orders": {OPTIONS: {"all": {"fd": "A|B"}}},
                "starbake_customers": {OPTIONS: {"all": {"fd": "B|C"}}},
            }, tmp_path)


# ------------------------------------------------------------------
# get_sl_options — node-level resolution from the run config
# ------------------------------------------------------------------

class TestGetSlOptions:

    def _config(self, sections: dict = None):
        return DagsterLogicalDatetimeConfig(
            logical_datetime=None,
            sl_options=json.dumps(sections) if sections else None,
        )

    def test_no_options(self):
        assert StarlakeDagsterUtils.get_sl_options(None, self._config()) == {}

    def test_all_section_applies_to_any_node(self):
        config = self._config({"all": {"fd": "2026-07-09"}})
        assert StarlakeDagsterUtils.get_sl_options(None, config, "any_node") == {"fd": "2026-07-09"}

    def test_task_specific_section_overrides_all(self):
        config = self._config({"all": {"k": "1"}, "my_transform": {"k": "2"}})
        assert StarlakeDagsterUtils.get_sl_options(None, config, "my_transform") == {"k": "2"}
        assert StarlakeDagsterUtils.get_sl_options(None, config, "other") == {"k": "1"}

    def test_tag_fallback_when_config_unset(self):
        context = types.SimpleNamespace(get_tag=lambda key: json.dumps({"all": {"fd": "x"}}) if key == OPTIONS else None)
        assert StarlakeDagsterUtils.get_sl_options(context, self._config()) == {"fd": "x"}


# ------------------------------------------------------------------
# get_materialization — options published in the event metadata
# ------------------------------------------------------------------

class TestMaterializationCarriesOptions:

    def _context(self):
        log = types.SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)
        return types.SimpleNamespace(log=log, partition_key=None)

    def test_static_extra_published(self):
        config = DagsterLogicalDatetimeConfig(logical_datetime="2026-07-09 06:00:00+00:00")
        mat = StarlakeDagsterUtils.get_materialization(
            self._context(), config, "my_dataset",
            extra={OPTIONS: {"all": {"fd": "2026-07-09"}}},
        )
        value = mat.metadata[OPTIONS].value
        assert value == {"all": {"fd": "2026-07-09"}}

    def test_run_options_relayed_and_override_static(self):
        config = DagsterLogicalDatetimeConfig(
            logical_datetime="2026-07-09 06:00:00+00:00",
            sl_options=json.dumps({"all": {"fd": "2026-07-10"}, "d.t": {"k": "v"}}),
        )
        mat = StarlakeDagsterUtils.get_materialization(
            self._context(), config, "my_dataset",
            extra={OPTIONS: {"all": {"fd": "2026-07-09", "static": "1"}}},
        )
        value = mat.metadata[OPTIONS].value
        assert value == {"all": {"fd": "2026-07-10", "static": "1"}, "d.t": {"k": "v"}}

    def test_no_options_no_metadata_entry(self):
        config = DagsterLogicalDatetimeConfig(logical_datetime="2026-07-09 06:00:00+00:00")
        mat = StarlakeDagsterUtils.get_materialization(self._context(), config, "my_dataset")
        assert OPTIONS not in mat.metadata
        assert OPTIONS_MERGE not in mat.metadata

    def test_static_merge_declarations_published(self):
        config = DagsterLogicalDatetimeConfig(logical_datetime="2026-07-09 06:00:00+00:00")
        mat = StarlakeDagsterUtils.get_materialization(
            self._context(), config, "my_dataset",
            extra={OPTIONS: {"all": {"fd": "A|B"}}, OPTIONS_MERGE: {"all": {"fd": UNION}}},
        )
        assert mat.metadata[OPTIONS].value == {"all": {"fd": "A|B"}}
        assert mat.metadata[OPTIONS_MERGE].value == {"all": {"fd": UNION}}

    def test_run_merge_declarations_relayed_and_override_static(self):
        semicolon = {"policy": "union", "separator": ";"}
        config = DagsterLogicalDatetimeConfig(
            logical_datetime="2026-07-09 06:00:00+00:00",
            sl_options=json.dumps({"all": {"fd": "A|B|C"}}),
            sl_options_merge=json.dumps({"all": {"fd": UNION}}),
        )
        mat = StarlakeDagsterUtils.get_materialization(
            self._context(), config, "my_dataset",
            extra={OPTIONS: {"all": {"other": "X;Y"}}, OPTIONS_MERGE: {"all": {"fd": semicolon, "other": semicolon}}},
        )
        assert mat.metadata[OPTIONS_MERGE].value == {"all": {"fd": UNION, "other": semicolon}}

    def test_declarations_of_keys_not_carried_are_not_published(self):
        config = DagsterLogicalDatetimeConfig(
            logical_datetime="2026-07-09 06:00:00+00:00",
            sl_options_merge=json.dumps({"all": {"fd": UNION}}),
        )
        mat = StarlakeDagsterUtils.get_materialization(
            self._context(), config, "my_dataset",
            extra={OPTIONS: {"all": {"k": "v"}}, OPTIONS_MERGE: {"d.t": {"x": UNION}}},
        )
        assert mat.metadata[OPTIONS].value == {"all": {"k": "v"}}
        assert OPTIONS_MERGE not in mat.metadata

    def test_relayed_materializations_merge_downstream_by_union(self):
        """Multi-hop: two runs relaying different date sets reach the next
        sensor with their declarations, which merges them by union."""
        mats = []
        for dates in ("A|B", "B|C"):
            config = DagsterLogicalDatetimeConfig(
                logical_datetime="2026-07-09 06:00:00+00:00",
                sl_options=json.dumps({"all": {"fd": dates}}),
                sl_options_merge=json.dumps({"all": {"fd": UNION}}),
            )
            mats.append(StarlakeDagsterUtils.get_materialization(self._context(), config, f"dataset_{len(mats)}"))
        assert StarlakeDagsterUtils.collect_sl_options(mats) == {"all": {"fd": "A|B|C"}}


# ------------------------------------------------------------------
# _ops_config — sl_options propagated to every op config
# ------------------------------------------------------------------

class TestOpsConfigCarriesOptions:

    def _pipeline(self):
        job = StarlakeDagsterShellJob(
            filename="test_sl_options.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={},
        )
        orch = DagsterOrchestration(job=job)
        schedule = StarlakeSchedule(name=None, cron="0 0 * * *", domains=[])
        pipeline = orch.sl_create_pipeline(schedule=schedule)
        with pipeline:
            start = pipeline.start_task()
            end = pipeline.end_task()
            start >> end
        return pipeline

    def test_sl_options_in_every_op_config(self):
        pipeline = self._pipeline()
        sections = json.dumps({"all": {"fd": "2026-07-09"}})
        run_config = pipeline._ops_config(logical_datetime=None, sl_options=sections)
        ops = run_config["ops"]
        assert len(ops) > 0
        for op_config in ops.values():
            assert op_config["config"]["sl_options"] == sections

    def test_sl_options_absent_when_not_provided(self):
        pipeline = self._pipeline()
        run_config = pipeline._ops_config(logical_datetime="2026-07-09 06:00:00+00:00")
        for op_config in run_config["ops"].values():
            assert "sl_options" not in op_config["config"]
            assert "sl_options_merge" not in op_config["config"]

    def test_sl_options_merge_in_every_op_config(self):
        pipeline = self._pipeline()
        sections = json.dumps({"all": {"fd": "A|B"}})
        merge = json.dumps({"all": {"fd": UNION}})
        run_config = pipeline._ops_config(logical_datetime=None, sl_options=sections, sl_options_merge=merge)
        for op_config in run_config["ops"].values():
            assert op_config["config"]["sl_options"] == sections
            assert op_config["config"]["sl_options_merge"] == merge
