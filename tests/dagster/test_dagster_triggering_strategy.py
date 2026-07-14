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

"""Dagster triggering-strategy validation.

Sensor-tick semantics with real (runless) asset materializations —
pattern per tests/dagster/test_dagster_runtime.py::
TestDagsterDatasetTriggering (persistent DagsterInstance.local_temp,
build_multi_asset_sensor_context, evaluate_tick).

Dagster ANY semantics are maintainer-confirmed DESIGN (2026-07-14,
issue #78): ANY governs the sensor's trigger gate only; before a run,
the post-gate consistency check verifies ALL non-optional monitored
datasets — see test_any_partial_materialization_skips_by_design.

The positive trigger paths (RunRequest for runless materializations)
depend on the sensor advancing its cursor — issue #80, fixed alongside
these tests.
"""

from __future__ import annotations

import pytest

from tests.dagster.dagster_test_mixin import DagsterTestMixin
from tests.shared.base_test_triggering_strategy import BaseTestTriggeringStrategy
from tests.shared.triggering_scenarios import (
    NO_UPSTREAM,
    SINGLE_UPSTREAM,
    TWO_UPSTREAMS,
    make_dependencies,
)


class TestDagsterTriggeringStrategy(DagsterTestMixin, BaseTestTriggeringStrategy):
    """Dagster concrete implementation of the shared triggering tests."""


class TestDagsterTriggeringSemantics:
    """Sensor-tick semantics with real (runless) asset materializations."""

    def _build(self, strategy, upstreams, filename):
        from ai.starlake.dagster import DagsterOrchestration
        from ai.starlake.dagster.shell import StarlakeDagsterShellJob
        from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME

        job = StarlakeDagsterShellJob(
            filename=filename,
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={"dataset_triggering_strategy": strategy},
        )
        with DagsterOrchestration(job=job) as orch:
            pipeline = orch.sl_create_pipeline(
                dependencies=make_dependencies(upstreams)
            )
            with pipeline:
                pass
        return pipeline, orch

    def _evaluate_tick(self, orch, monitored_uris, materialized_uris, tmp_path):
        from dagster import (
            AssetKey,
            AssetMaterialization,
            DagsterInstance,
            build_multi_asset_sensor_context,
        )

        sensors = orch.definitions.sensors
        assert len(sensors) == 1, "Expected 1 sensor, got {}".format(
            len(sensors)
        )
        instance = DagsterInstance.local_temp(tempdir=str(tmp_path))
        try:
            for uri in materialized_uris:
                instance.report_runless_asset_event(
                    AssetMaterialization(asset_key=AssetKey(uri))
                )
            context = build_multi_asset_sensor_context(
                monitored_assets=[AssetKey(uri) for uri in monitored_uris],
                instance=instance,
                definitions=orch.definitions,
            )
            return sensors[0].evaluate_tick(context)
        finally:
            instance.dispose()

    @pytest.mark.parametrize("strategy", ["any", "all"])
    def test_single_upstream_materialization_triggers(
        self, tmp_path, strategy
    ):
        """Both strategies fire once the only monitored asset materializes."""
        _, orch = self._build(
            strategy, SINGLE_UPSTREAM, "test_trig_single_{}.py".format(strategy)
        )
        result = self._evaluate_tick(
            orch, ["starbake_orders"], ["starbake_orders"], tmp_path
        )
        assert len(result.run_requests) == 1, (
            "Expected 1 RunRequest for {} with its single upstream "
            "materialized, got {}".format(strategy, len(result.run_requests))
        )

    def test_all_partial_materialization_skips(self, tmp_path):
        """ALL waits: 1 of 2 monitored assets materialized -> no run.

        Under ALL the partial materialization is rejected at the TRIGGER
        GATE itself (``all()`` over the latest materialization records),
        so the skip message is the gate-level one.
        """
        _, orch = self._build(
            "all", TWO_UPSTREAMS, "test_trig_all_partial.py"
        )
        result = self._evaluate_tick(
            orch,
            ["starbake_orders", "starbake_customers"],
            ["starbake_orders"],
            tmp_path,
        )
        assert len(result.run_requests) == 0
        assert result.skip_message == "No materializations observed"

    def test_all_complete_materialization_triggers(self, tmp_path):
        """ALL fires once every monitored asset has materialized."""
        _, orch = self._build(
            "all", TWO_UPSTREAMS, "test_trig_all_complete.py"
        )
        result = self._evaluate_tick(
            orch,
            ["starbake_orders", "starbake_customers"],
            ["starbake_orders", "starbake_customers"],
            tmp_path,
        )
        assert len(result.run_requests) == 1

    def test_any_partial_materialization_skips_by_design(self, tmp_path):
        """BEHAVIOR PIN (issue #78 — maintainer-confirmed design, 2026-07-14).

        With ``dataset_triggering_strategy=any`` and 1 of 2 monitored
        assets materialized, the sensor passes the ``any()`` trigger
        gate but the POST-GATE consistency check returns a SkipReason:
        ANY governs the trigger gate only; before running, the pipeline
        must verify the freshness of ALL the datasets it depends on
        within the window frame.  This intentionally diverges from
        Airflow's ``DatasetAny`` (which fires on the first available
        upstream).

        The skip message is asserted to be the POST-GATE one ("but not
        for ...") — NOT the gate-level "No materializations observed" —
        so this pin cannot pass for the wrong reason (canary rule: the
        positive tests above prove runless materializations are visible
        to the sensor context).  If the design ruling is ever revisited,
        this pin flips loudly.
        """
        _, orch = self._build(
            "any", TWO_UPSTREAMS, "test_trig_any_partial.py"
        )
        result = self._evaluate_tick(
            orch,
            ["starbake_orders", "starbake_customers"],
            ["starbake_orders"],
            tmp_path,
        )
        assert len(result.run_requests) == 0
        skip_message = result.skip_message or ""
        assert "but not for" in skip_message, (
            "Expected the POST-GATE consistency SkipReason "
            "('Observed materializations for ..., but not for ...'), "
            "got: {!r}".format(result.skip_message)
        )
        assert "starbake_customers" in skip_message

    @pytest.mark.parametrize("strategy", ["any", "all"])
    def test_no_upstream_produces_no_sensor(self, strategy):
        """No datasets and no cron -> neither sensor nor schedule."""
        _, orch = self._build(
            strategy, NO_UPSTREAM, "test_trig_none_{}.py".format(strategy)
        )
        assert len(orch.definitions.sensors) == 0
        assert len(orch.definitions.schedules) == 0
