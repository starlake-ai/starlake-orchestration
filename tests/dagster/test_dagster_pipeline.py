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

from ai.starlake.dagster import DagsterOrchestration
from ai.starlake.dagster.shell import StarlakeDagsterShellJob
from ai.starlake.orchestration import StarlakeSchedule

from tests.dagster.conftest import _DAGSTER_TEST_MODULE_NAME


# ------------------------------------------------------------------
# 4.5  DagsterPipeline with cron schedule produces valid JobDefinition
# ------------------------------------------------------------------

class TestDagsterPipelineCron:

    def test_pipeline_with_cron_schedule(self):
        from dagster import JobDefinition

        job = StarlakeDagsterShellJob(
            filename="test_cron.py",
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
        assert isinstance(pipeline.dag, JobDefinition)
        assert pipeline.computed_cron_expr == "0 0 * * *"


# ------------------------------------------------------------------
# 4.6  DagsterPipeline with dataset triggers produces valid sensor
# ------------------------------------------------------------------

class TestDagsterPipelineSensor:

    def _make_dataset_pipeline(self, strategy):
        from ai.starlake.orchestration import (
            StarlakeDependencies,
            StarlakeDependency,
            StarlakeDependencyType,
        )

        job = StarlakeDagsterShellJob(
            filename="test_dataset.py",
            module_name=_DAGSTER_TEST_MODULE_NAME,
            options={"dataset_triggering_strategy": strategy},
        )
        with DagsterOrchestration(job=job) as orch:
            dependencies = StarlakeDependencies([
                StarlakeDependency(
                    name="overall_kpis",
                    dependency_type=StarlakeDependencyType.TASK,
                    dependencies=[
                        StarlakeDependency(
                            name="starbake.orders",
                            dependency_type=StarlakeDependencyType.TABLE,
                            cron="0 * * * *",
                        ),
                        StarlakeDependency(
                            name="starbake.customers",
                            dependency_type=StarlakeDependencyType.TABLE,
                            cron="0 0 * * *",
                        ),
                    ],
                ),
            ])
            pipeline = orch.sl_create_pipeline(dependencies=dependencies)
            with pipeline:
                pass
        return pipeline, orch

    def test_any_strategy_produces_sensor(self):
        from ai.starlake.dataset import DatasetTriggeringStrategy

        pipeline, orch = self._make_dataset_pipeline("any")
        assert pipeline.dag is not None
        assert len(pipeline.events) == 2
        event_paths = {e.path[0] for e in pipeline.events}
        assert "starbake_orders" in event_paths, (
            f"Expected 'starbake_orders' in event paths, got {event_paths}"
        )
        assert "starbake_customers" in event_paths, (
            f"Expected 'starbake_customers' in event paths, got {event_paths}"
        )
        assert pipeline.job.dataset_triggering_strategy == DatasetTriggeringStrategy.ANY

        # Verify exactly one sensor was created in definitions
        assert hasattr(orch, "definitions")
        sensors = orch.definitions.sensors
        assert len(sensors) == 1, f"Expected 1 sensor, got {len(sensors)}"

        # Verify sensor is named after the pipeline
        sensor = sensors[0]
        assert pipeline.pipeline_id in sensor.name, (
            f"Sensor name '{sensor.name}' should contain pipeline id "
            f"'{pipeline.pipeline_id}'"
        )

    def test_all_strategy_produces_sensor(self):
        from ai.starlake.dataset import DatasetTriggeringStrategy

        pipeline, orch = self._make_dataset_pipeline("all")
        assert pipeline.dag is not None
        assert len(pipeline.events) == 2
        event_paths = {e.path[0] for e in pipeline.events}
        assert "starbake_orders" in event_paths, (
            f"Expected 'starbake_orders' in event paths, got {event_paths}"
        )
        assert "starbake_customers" in event_paths, (
            f"Expected 'starbake_customers' in event paths, got {event_paths}"
        )
        assert pipeline.job.dataset_triggering_strategy == DatasetTriggeringStrategy.ALL

        sensors = orch.definitions.sensors
        assert len(sensors) == 1, f"Expected 1 sensor, got {len(sensors)}"

        # Verify sensor is named after the pipeline
        sensor = sensors[0]
        assert pipeline.pipeline_id in sensor.name, (
            f"Sensor name '{sensor.name}' should contain pipeline id "
            f"'{pipeline.pipeline_id}'"
        )
