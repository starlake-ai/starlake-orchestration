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


def _mat(uri: str, sections: dict):
    from dagster import AssetMaterialization, MetadataValue
    return AssetMaterialization(asset_key=uri, metadata={OPTIONS: MetadataValue.json(sections)})


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
