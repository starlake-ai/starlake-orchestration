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

"""The load task of an Airflow load DAG publishes the table's freshness (issue #166).

The load template hands the freshness to the StarlakeTable of the schedule;
the pipeline gives it to the dataset of the load event, which the operator
publishes as ``sl_freshness`` in its outlet. Before, every load event carried
``sl_freshness: 0``.
"""

from __future__ import annotations

from ai.starlake.common import StarlakeParameters

from tests.airflow.conftest import _AIRFLOW_TEST_MODULE_NAME


def _load_task(freshness):
    from ai.starlake.airflow import AirflowOrchestration
    from ai.starlake.airflow.bash import StarlakeAirflowBashJob
    from ai.starlake.orchestration import StarlakeDomain, StarlakeSchedule, StarlakeTable

    job = StarlakeAirflowBashJob(filename="test_airflow.py", module_name=_AIRFLOW_TEST_MODULE_NAME, options={})
    schedule = StarlakeSchedule(
        name=None,
        cron="0 6 1-3 * *",
        domains=[StarlakeDomain(name="finance", final_name="finance", tables=[StarlakeTable(name="balances", freshness=freshness)])],
    )
    pipeline = AirflowOrchestration(job=job).sl_create_pipeline(schedule=schedule)
    with pipeline:
        return pipeline.sl_load(task_id="load_finance_balances", domain="finance", table="balances").task


def test_the_load_event_publishes_the_table_freshness():
    outlets = _load_task("2 days").outlets
    assert [outlet.extra[StarlakeParameters.FRESHNESS_PARAMETER.value] for outlet in outlets] == [172800]


def test_a_table_without_freshness_publishes_zero():
    outlets = _load_task(None).outlets
    assert [outlet.extra[StarlakeParameters.FRESHNESS_PARAMETER.value] for outlet in outlets] == [0]
