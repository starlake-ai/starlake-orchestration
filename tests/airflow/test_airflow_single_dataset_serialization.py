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

"""Scheduler-serialization guard for dataset-triggered DAGs (issue #130).

Airflow 2.9 serializes a *bare* single-``Dataset`` schedule as a dict, but the
``dataset_triggers`` schema requires an array — so the scheduler rejects the
DAG at serialization ("... is not of type 'array'") even though DAG *import*
succeeds. ``reduce(| / &)`` over one event returned that bare ``Dataset``; the
fix wraps a single event in a one-element flat list (a ``DATASET_ALL`` array).

This module is the guard on the **Airflow 2.9** CI leg, where the
condition-introspection structure tests in
``test_airflow_triggering_strategy.py`` are skipped (timetable
``.dataset_condition`` introspection needs Airflow >= 2.10). On 2.5.x the
single-event schedule was already a flat list (conditions did not exist), so
this passes there too; the bug window is 2.9.x only.

Airflow 3 changed the serialization API (no ``SerializedDAG.to_dict``) and its
single-event ``AssetAll`` shape is covered by the introspection structure
tests, so this module is Airflow-2-only.
"""

from __future__ import annotations

import pytest

from tests.airflow.dataset_compat import AIRFLOW_AVAILABLE, SUPPORTS_ASSETS
from tests.shared.triggering_scenarios import (
    SINGLE_UPSTREAM,
    TWO_UPSTREAMS,
    make_dependencies,
)

pytestmark = pytest.mark.skipif(
    not AIRFLOW_AVAILABLE or SUPPORTS_ASSETS,
    reason=(
        "SerializedDAG.to_dict/validate_schema is the Airflow-2 serialization "
        "API; Airflow 3 single-event AssetAll shape is covered by the "
        "condition-introspection structure tests"
    ),
)


def _make_dataset_triggered_dag(strategy, upstreams, filename):
    """Build a dataset-triggered DAG through the real orchestration path."""
    from ai.starlake.airflow import AirflowOrchestration
    from ai.starlake.airflow.bash import StarlakeAirflowBashJob
    from tests.airflow.conftest import _AIRFLOW_TEST_MODULE_NAME

    job = StarlakeAirflowBashJob(
        filename=filename,
        module_name=_AIRFLOW_TEST_MODULE_NAME,
        options={"dataset_triggering_strategy": strategy},
    )
    pipeline = AirflowOrchestration(job=job).sl_create_pipeline(
        dependencies=make_dependencies(upstreams)
    )
    with pipeline:
        pass
    return pipeline.dag


def _serialize_and_validate(dag):
    """Run the scheduler's serialize + schema-validate path.

    Raises ``jsonschema.ValidationError`` on a bare-``Dataset`` schedule under
    Airflow 2.9 (the ``dataset_triggers`` value is a dict, not the required
    array) — the exact failure of issue #130.
    """
    from airflow.serialization.serialized_objects import SerializedDAG

    SerializedDAG.validate_schema(SerializedDAG.to_dict(dag))


@pytest.mark.parametrize("strategy", ["any", "all"])
def test_single_upstream_dag_serializes(strategy):
    """A single-upstream dataset-triggered DAG must pass scheduler schema
    validation on every Airflow 2.x — regression guard for issue #130 (2.9
    rejected the bare single-``Dataset`` schedule)."""
    dag = _make_dataset_triggered_dag(
        strategy, SINGLE_UPSTREAM, f"test_ser_single_{strategy}.py"
    )
    _serialize_and_validate(dag)  # must not raise


@pytest.mark.parametrize("strategy", ["any", "all"])
def test_two_upstreams_dag_serializes(strategy):
    """Multi-upstream conditions already serialized as arrays on 2.9 — kept
    covered so the single-event fix does not regress the >1 path."""
    dag = _make_dataset_triggered_dag(
        strategy, TWO_UPSTREAMS, f"test_ser_two_{strategy}.py"
    )
    _serialize_and_validate(dag)  # must not raise
