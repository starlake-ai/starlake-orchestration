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

"""A transform pipeline triggered by its dependencies, with a real (local,
temporary) Dagster instance: the dependencies' loads are reported as runless
materializations shaped like StarlakeDagsterUtils.get_materialization's
(partitioned on their scheduled date, with sl_cron, sl_freshness,
sl_scheduled_date and the run variables in their metadata). The pipeline's
start check (DagsterPipeline.check_datasets_freshness) is either called
directly or reached through the pipeline sensor, whose cursor is carried from
one tick to the next, so that the materializations of a run requested by an
earlier tick are consumed, as in a deployment (issues #167 and #168).
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Dict, List, Optional

import pytest
import pytz

from ai.starlake.common import StarlakeParameters, sl_timestamp_format
from ai.starlake.dataset import StarlakeDataset

from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME
from tests.shared.triggering_scenarios import make_dependencies

UTC = pytz.UTC
OPTIONS = StarlakeParameters.OPTIONS_PARAMETER.value
OPTIONS_MERGE = StarlakeParameters.OPTIONS_MERGE_PARAMETER.value
UNION = {"policy": "union", "separator": "|"}

BALANCES = "finance_balances"
CONTRACTS = "finance_contracts"
RATES = "finance_rates"
POSITIONS = "finance_positions"

MONTHLY_DELIVERY = "0 6 1-3 * *"
MONTHLY = {"data_cycle_enabled": "true", "data_cycle": "monthly"}
TWO_DAYS = 2 * 24 * 3600


def at(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


class PipelineHarness:
    """A transform pipeline reading the tables of `crons` ({uri: cron}), with
    the ANY strategy unless `options` says otherwise."""

    def __init__(self, tmp_path, crons: Dict[str, Optional[str]], options: Optional[dict] = None):
        from dagster import DagsterInstance
        from ai.starlake.dagster import DagsterOrchestration
        from ai.starlake.dagster.shell import StarlakeDagsterShellJob

        self.crons = dict(crons)
        job_options = {"dataset_triggering_strategy": "any", "start_date": "2025-01-01"}
        job_options.update(options or {})
        self.job = StarlakeDagsterShellJob(
            filename="test_dagster_check_harness.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options=job_options,
        )
        # finance_balances -> finance.balances: the dependency's uri is its sanitized name
        upstreams = tuple((uri.replace("_", ".", 1), cron) for uri, cron in self.crons.items())
        with DagsterOrchestration(job=self.job) as orch:
            pipeline = orch.sl_create_pipeline(dependencies=make_dependencies(upstreams, task_name="finance.position"))
            with pipeline:
                start = pipeline.start_task()
                end = pipeline.end_task()
                start >> end
        assert sorted(dataset.uri for dataset in pipeline.datasets) == sorted(self.crons)
        self.orch = orch
        self.pipeline = pipeline
        tmp_path.mkdir(parents=True, exist_ok=True)
        self.instance = DagsterInstance.local_temp(tempdir=str(tmp_path))
        self.cursor = None

    def dispose(self) -> None:
        self.instance.dispose()

    def materialize(self, uri: str, scheduled: datetime, options: Optional[dict] = None, merge=None, freshness: int = 0, partition: Optional[str] = None) -> int:
        """Report a load of `uri` scheduled on `scheduled`; returns the storage
        id of its materialization."""
        from dagster import AssetKey, AssetMaterialization, MetadataValue

        metadata = {
            StarlakeParameters.URI_PARAMETER.value: uri,
            StarlakeParameters.CRON_PARAMETER.value: self.crons[uri],
            StarlakeParameters.FRESHNESS_PARAMETER.value: MetadataValue.int(freshness),
            StarlakeParameters.SCHEDULED_DATE_PARAMETER.value: MetadataValue.timestamp(scheduled),
        }
        if options is not None:
            metadata[OPTIONS] = MetadataValue.json(options)
        if merge is not None:
            metadata[OPTIONS_MERGE] = MetadataValue.json(merge) if isinstance(merge, (dict, list)) else merge
        self.instance.report_runless_asset_event(AssetMaterialization(
            asset_key=AssetKey(uri),
            partition=partition or scheduled.strftime(sl_timestamp_format),
            metadata=metadata,
        ))
        return self.instance.fetch_materializations(AssetKey(uri), limit=1).records[0].storage_id

    def dataset(self, uri: str, freshness: int = 0) -> StarlakeDataset:
        return StarlakeDataset(name=uri, sink=uri, cron=self.crons[uri], freshness=freshness)

    def check(self, anchor: datetime, uris: List[str], schedules: Optional[Dict[str, datetime]] = None, freshness: Optional[Dict[str, int]] = None, **kwargs):
        """check_datasets_freshness on the datasets `uris`, anchored on
        `anchor`, the unconsumed materializations being scheduled on
        `schedules` ({uri: scheduled date})."""
        freshness = freshness or {}
        datasets = [self.dataset(uri, freshness.get(uri, 0)) for uri in uris]
        return self.pipeline.check_datasets_freshness(anchor, datasets, schedules or {}, self.instance, **kwargs)

    def tick(self):
        """Evaluate one tick of the pipeline sensor, from the cursor left by the
        previous one."""
        from dagster import AssetKey, build_multi_asset_sensor_context

        sensors = self.orch.definitions.sensors
        assert len(sensors) == 1
        context = build_multi_asset_sensor_context(
            monitored_assets=[AssetKey(uri) for uri in self.crons],
            instance=self.instance,
            definitions=self.orch.definitions,
            cursor=self.cursor,
        )
        result = sensors[0].evaluate_tick(context)
        if result.cursor is not None:
            self.cursor = result.cursor
        return result


def relayed(result) -> Optional[dict]:
    """The sl_options sections of the single run requested by a tick (the same
    for every op), None when its run config carries none."""
    assert len(result.run_requests) == 1, result.skip_message
    run_config = result.run_requests[0].run_config or {}
    configs = [op["config"] for op in run_config.get("ops", {}).values()]
    values = {op_config.get("sl_options") for op_config in configs}
    assert len(values) <= 1
    value = values.pop() if values else None
    return json.loads(value) if value else None


def relayed_merge(result) -> Optional[dict]:
    run_config = result.run_requests[0].run_config or {}
    configs = [op["config"] for op in run_config.get("ops", {}).values()]
    values = {op_config.get("sl_options_merge") for op_config in configs}
    assert len(values) <= 1
    value = values.pop() if values else None
    return json.loads(value) if value else None


@pytest.fixture
def harness(tmp_path):
    """A factory of PipelineHarness, whose instances are disposed of after
    the test."""
    created: List[PipelineHarness] = []

    def make(crons: Dict[str, Optional[str]], options: Optional[dict] = None) -> PipelineHarness:
        pipeline = PipelineHarness(tmp_path / f"instance-{len(created)}", crons, options)
        created.append(pipeline)
        return pipeline

    yield make
    for pipeline in created:
        pipeline.dispose()
